"""Restartable staging batches, isolated from the Airflow bind mount."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import tempfile

from .contracts import (
    ContractError, ENRICHED_GAME_COLUMNS, RAW_COLUMNS, appid, encode_csv, enrich_games,
    utc_observation,
)


REPOSITORY = Path(__file__).resolve().parents[1]
AIRFLOW_SOURCE = "/opt/airflow/source_data"


def staging_root(path, *, repository=REPOSITORY):
    """Resolve the configured bind mount and fail closed on an unknown layout."""
    root = Path(path).resolve()
    repository = Path(repository).resolve()
    compose = repository / "LakeHouse_Project" / "docker-compose.yml"
    protected = [repository, Path(AIRFLOW_SOURCE).resolve()]
    mounted = []
    for line in compose.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line.startswith("#") or AIRFLOW_SOURCE not in line:
            continue
        item = line.removeprefix("- ").strip().strip("\"'")
        host, separator, options = item.rpartition(":" + AIRFLOW_SOURCE)
        if (not line.startswith("- ") or not separator or not host
                or "$" in host or options not in {"", ":ro", ":rw"}):
            raise ContractError("Cannot verify the Airflow source_data bind mount")
        source = Path(host)
        if not source.is_absolute():
            if PureWindowsPath(host).is_absolute() or not host.startswith(("./", "../")):
                raise ContractError("Cannot resolve the Airflow host source_data path")
            source = compose.parent / source
        # Protect the configured thesis workspace even if data is a link.
        if source.name.casefold() == "data" and source.parent != Path(source.anchor):
            protected.append(source.parent.resolve())
        mounted.append(source.resolve())
    if not mounted:
        raise ContractError("Airflow source_data mount is missing")
    for location in (*protected, *mounted):
        if root.is_relative_to(location) or location.is_relative_to(root):
            raise ContractError("Staging must be outside repositories and Airflow source_data")
    return root


def inside(parent, *names):
    """Reject symlinks and junctions in managed staging paths."""
    path = parent.joinpath(*names)
    if path.resolve() != path or not path.is_relative_to(parent):
        raise ContractError("Unsafe managed staging path")
    return path


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def digest(payload):
    return hashlib.sha256(payload).hexdigest()


def read_json(path):
    try:
        data = json.loads(path.read_bytes())
    except (UnicodeError, json.JSONDecodeError):
        raise ContractError("Unreadable staging metadata") from None
    if not isinstance(data, dict):
        raise ContractError("Invalid staging metadata")
    return data


def atomic_write(path, payload):
    """Persist one checkpoint atomically on its own filesystem."""
    stream = tempfile.NamedTemporaryFile(dir=path.parent, prefix=".pending-", delete=False)
    temp = Path(stream.name)
    try:
        with stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


@contextmanager
def batch_lock(root, kind, batch_id):
    if (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", batch_id)
            or PureWindowsPath(batch_id).is_reserved()):
        raise ContractError("Invalid batch ID")
    locks = inside(root, ".locks")
    locks.mkdir(parents=True, exist_ok=True)
    lock = inside(locks, f"{kind}-{batch_id}.lock")
    try:
        lock.touch(exist_ok=False)
    except FileExistsError:
        raise ContractError("Batch is locked by another writer or an interrupted process") from None
    try:
        yield
    finally:
        lock.unlink()


def completed(directory, identity, filenames):
    """Only an absent directory permits a new batch. Corrupt output fails closed."""
    try:
        directory.stat()
    except FileNotFoundError:
        return False
    manifest = read_json(inside(directory, "manifest.json"))
    files = manifest.get("files")
    if (manifest.get("status") != "complete" or manifest.get("version") != 1
            or manifest.get("identity") != identity or not isinstance(files, dict)
            or set(files) != set(filenames)):
        raise ContractError("Existing batch does not match the requested inputs")
    for name, metadata in files.items():
        if (not isinstance(metadata, dict)
                or digest(inside(directory, name).read_bytes()) != metadata.get("sha256")):
            raise ContractError("Existing completed output failed integrity verification")
    return True


def publish(directory, identity, outputs, validation):
    """Expose a complete staging directory with a same-parent rename."""
    directory.parent.mkdir(parents=True, exist_ok=True)
    pending = Path(tempfile.mkdtemp(prefix=".pending-", dir=directory.parent)).resolve()
    if pending.parent != directory.parent:
        raise ContractError("Unsafe pending staging path")
    files = {}
    for name, (columns, rows) in outputs.items():
        payload = encode_csv(columns, rows)
        atomic_write(inside(pending, name), payload)
        files[name] = {"sha256": digest(payload), "rows": len(rows)}
    atomic_write(inside(pending, "manifest.json"), json_bytes({
        "version": 1, "status": "complete", "identity": identity,
        "files": files, "validation": validation,
    }))
    try:
        directory.stat()
    except FileNotFoundError:
        os.rename(pending, directory)
    else:
        raise ContractError("Refusing to replace a completed staging directory")
    return directory


def validate_observation(rows, key):
    if not isinstance(rows, dict) or set(rows) != set(RAW_COLUMNS):
        raise ContractError("Observation has an invalid set of files")
    for name, columns in RAW_COLUMNS.items():
        row = rows[name]
        if not isinstance(row, dict) or set(row) != set(columns) or appid(row["steam_appid"]) != key:
            raise ContractError("Observation does not match the CSV contract")
    stamps = (rows["games.csv"]["ts_ingested"], rows["reviews.csv"]["ts_ingested"], rows["popularity.csv"]["ts"])
    for stamp in stamps:
        utc_observation(stamp)
    if len(set(stamps)) != 1:
        raise ContractError("Observation timestamps disagree")


def ensure_plan(work, identity):
    work.mkdir(parents=True, exist_ok=True)
    plan = inside(work, "plan.json")
    expected = {"version": 1, "identity": identity}
    try:
        saved = read_json(plan)
    except FileNotFoundError:
        # A process can stop before a temporary plan file becomes durable.
        if any(not entry.name.startswith(".pending-") for entry in work.iterdir()):
            raise ContractError("Staging checkpoint exists without its batch plan") from None
        atomic_write(plan, json_bytes(expected))
    else:
        if saved != expected:
            raise ContractError("Resume inputs differ from the saved batch plan")


def collect_batch(stage, batch_id, appids, collector, *, language="all", repository=REPOSITORY):
    root = staging_root(stage, repository=repository)
    keys = sorted({appid(value) for value in appids})
    if not keys or not re.fullmatch(r"[a-zA-Z0-9_]{1,32}", language):
        raise ContractError("Provide App IDs and a valid review language")
    identity = digest(json_bytes({"kind": "raw", "batch": batch_id, "ids": keys, "language": language}))
    with batch_lock(root, "raw", batch_id):
        destination = inside(root, "raw", batch_id)
        if completed(destination, identity, RAW_COLUMNS):
            return destination
        work = inside(root, ".work", "raw", batch_id)
        ensure_plan(work, identity)

        outputs = {name: (columns, []) for name, columns in RAW_COLUMNS.items()}
        for key in keys:
            checkpoint = inside(work, f"{key}.json")
            try:
                record = read_json(checkpoint)
            except FileNotFoundError:
                rows = collector.capture(key, language)
                validate_observation(rows, key)
                atomic_write(checkpoint, json_bytes({
                    "identity": identity, "rows": rows, "sha256": digest(json_bytes(rows)),
                }))
            else:
                rows = record.get("rows")
                if record.get("identity") != identity or record.get("sha256") != digest(json_bytes(rows)):
                    raise ContractError("Observation checkpoint failed integrity verification")
                validate_observation(rows, key)
            for name, row in rows.items():
                outputs[name][1].append(row)
        return publish(destination, identity, outputs, {"observations": len(keys)})


def read_staging_input(root, path):
    source = Path(path).resolve(strict=True)
    if not source.is_relative_to(root):
        raise ContractError("Enrichment inputs must be inside the isolated staging directory")
    return source.read_bytes()


def enrich_batch(stage, batch_id, games, tags, *, repository=REPOSITORY):
    root = staging_root(stage, repository=repository)
    with batch_lock(root, "enriched", batch_id):
        games_payload = read_staging_input(root, games)
        tags_payload = read_staging_input(root, tags)
        identity = digest(json_bytes({
            "kind": "enriched", "batch": batch_id,
            "games_sha256": digest(games_payload), "tags_sha256": digest(tags_payload),
        }))
        destination = inside(root, "enriched", batch_id)
        if completed(destination, identity, ("games.csv",)):
            return destination
        work = inside(root, ".work", "enriched", batch_id)
        ensure_plan(work, identity)
        try:
            rows, validation = enrich_games(games_payload, tags_payload)
        except ContractError as error:
            atomic_write(inside(work, "validation.json"), json_bytes({"status": "rejected", "reason": str(error)}))
            raise
        return publish(destination, identity, {"games.csv": (ENRICHED_GAME_COLUMNS, rows)}, validation)
