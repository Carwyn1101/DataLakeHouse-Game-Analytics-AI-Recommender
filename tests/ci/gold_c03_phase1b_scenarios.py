"""TP-19 and TP-25 for the three remaining Gold game dimensions."""

from collections import Counter
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
from typing import NamedTuple
from unittest.mock import patch


SCENARIO = "gold-c03-phase-1b"
BASE = "a815d6bd9aaa09faf3b8e65ad4ec643abfed062d"
FLAGS = ("__dimension_delete_marker", "__dimension_changed")
VARIANTS = ("exact_existing", "exact_new", "conflicting_existing", "conflicting_new")


class Dimension(NamedTuple):
    name: str
    silver: str
    key: str
    input_fields: tuple
    output_fields: tuple

    @property
    def table(self):
        return f"hive.gold.dim_{self.name}_info"

    @property
    def source(self):
        return f"hive.silver.{self.silver}"

    @property
    def case(self):
        return f"C03B-{self.name.upper()}"

    @property
    def columns(self):
        return (self.key, "steam_appid", *(name for name, _ in self.output_fields))

    @property
    def schema(self):
        return ((self.key, "int"), ("steam_appid", "bigint"), *self.output_fields)


DETAIL = (
    ("steam_metacritic_score", "double"), ("steam_price_initial", "double"),
    ("steam_price_final", "double"),
)
DETAIL_REST = (
    ("recommendations_total", "double"), ("age_category", "string"),
    ("price_category", "string"), ("price_vnd", "bigint"), ("price_category_vnd", "string"),
)
REVIEW = (
    ("steam_review_total_positive", "bigint"), ("steam_review_total_negative", "bigint"),
    ("steam_review_total", "bigint"), ("steam_review_positive_ratio", "double"),
    ("steam_review_score_desc", "string"), ("review_volume_category", "string"),
    ("review_quality_score", "double"),
)
POPULARITY = (
    ("steam_concurrent_players", "bigint"), ("owners_low", "bigint"), ("owners_high", "bigint"),
    ("owners_midpoint", "bigint"), ("steamspy_players_2weeks", "bigint"),
    ("playtime_avg_hours", "double"), ("playtime_median_hours", "double"),
    ("popularity_category", "string"), ("engagement_category", "string"),
)
DIMENSIONS = (
    Dimension("detail", "games", "game_detail_key", DETAIL + DETAIL_REST,
              DETAIL + (("steam_currency", "string"), ("discount_pct", "double")) + DETAIL_REST),
    Dimension("review", "reviews", "review_key", REVIEW, REVIEW),
    Dimension("popularity", "popularity", "popularity_key", POPULARITY, POPULARITY),
)


class ApplicationFault(RuntimeError):
    """Fault injected at the application boundary."""


class State(NamedTuple):
    rows: Counter
    snapshots: dict


def require(condition, stage):
    if not condition:
        raise AssertionError(stage)


def schema(fields):
    return ", ".join(f"{name} {kind}" for name, kind in fields)


def signature(frame):
    return tuple(frame.dtypes)


def values(dimension, revision):
    """Supply deterministic values and exact fractional fixtures in all Spark schemas."""
    if dimension.name == "detail":
        return (float(revision), 8.0, 4.0, 11.0, "Any", "Paid", 100_000, "VND")
    if dimension.name == "review":
        return (revision, 8, revision + 8, 0.75, "Positive", "Small", 7.25)
    return (revision, 1000, 2000, 1500, 7, 1.25, 0.5, "Small", "Active")


def row(dimension, key, appid, revision):
    payload = values(dimension, revision)
    if dimension.name == "detail":
        payload = (*payload[:3], "USD", 50.0, *payload[3:])
    return (key, appid, *payload)


def seed(run, spark, dimension):
    # Use the expected nullable target schema and only synthetic rows.
    initial = [
        (2, 101, 1), (4, 202, 2), (7, 202, 3), (7, 202, 3),
        (8, 404, 4), (9, 404, 5), (20, 909, 6),
    ]
    spark.createDataFrame([row(dimension, *fields) for fields in initial], schema(dimension.schema)) \
        .writeTo(dimension.table).using("iceberg").tableProperty("format-version", "2").create()
    silver = (("steam_appid", "bigint"), *dimension.input_fields,
              ("ts_ingested", "timestamp"), ("is_current", "boolean"))
    spark.sql(f"CREATE TABLE {dimension.source} ({schema(silver)}) USING iceberg "
              "TBLPROPERTIES ('format-version'='2')").collect()


def freeze(spark, dimension, records):
    ordered = (("steam_appid", "bigint"), *dimension.input_fields,
               ("ts_ingested", "timestamp"), ("is_current", "boolean"))
    source = [
        (appid, *values(dimension, revision), datetime(2026, 1, day), active)
        for appid, revision, day, active in records
    ]
    spark.createDataFrame(source, schema(ordered)).writeTo(dimension.source).overwritePartitions()


def observe(spark, dimension):
    reader = spark.newSession()
    data = reader.table(dimension.table)
    require(signature(data) == dimension.schema, "target schema")
    snapshots = reader.sql(f"SELECT snapshot_id, parent_id, operation FROM {dimension.table}.snapshots")
    return State(Counter(tuple(item) for item in data.collect()),
                 {entry[0]: tuple(entry[1:]) for entry in snapshots.collect()})


def child_snapshot(before, after):
    existing, actual = before.snapshots, after.snapshots
    tips = existing.keys() - {item[0] for item in existing.values()}
    additions = actual.keys() - existing.keys()
    require(len(tips) == len(additions) == 1 and len(actual) == len(existing) + 1,
            "expected exactly one descendant snapshot")
    require(all(actual.get(key) == entry for key, entry in existing.items()), "snapshot history changed")
    require(actual[next(iter(additions))][0] in tips, "new snapshot parent")


def replacement(before, replacements):
    affected = {payload[1] for payload in replacements}
    return Counter({payload: count for payload, count in before.rows.items()
                    if payload[1] not in affected}) + Counter(replacements)


def check_stage(frame, dimension, expected):
    require(signature(frame) == (*dimension.schema, (FLAGS[0], "boolean"), (FLAGS[1], "boolean")),
            "staged Parquet schema names and types")
    require(all(field.nullable for field in frame.schema.fields), "staged Parquet nullability")
    staged_rows = [tuple(entry) for entry in frame.collect()]
    markers = Counter(item[1] for item in staged_rows if item[-2])
    changed = {item[1] for item in staged_rows if item[-1]}
    require(all(type(value) is bool for item in staged_rows for value in item[-2:]) and
            all(not item[-2] or item[-1] for item in staged_rows), "staged flag values")
    require(markers == Counter(dict.fromkeys(changed, 1)), "one delete marker per changed AppID")
    if expected is not None:
        require(Counter((*item[:-2], item[-1]) for item in staged_rows) == Counter(expected),
                "staged payload rows and assigned keys")


def refresh(run, spark, dimension, expected, *, fault=None, before=None):
    writer = spark.newSession()
    original_sql = writer.sql
    stage_paths = []
    source_paths = []
    attempted = []
    committed = []
    completed = []

    @contextmanager
    def stage_directory(*, prefix):
        final_prefix = f"gold-dim_{dimension.name}_info-"
        source_prefix = f"{final_prefix}source-"
        require(prefix == final_prefix or (dimension.name != "detail" and prefix == source_prefix),
                "scratch identity")
        with tempfile.TemporaryDirectory(prefix=prefix) as location:
            path = Path(location).resolve()
            if prefix == source_prefix:
                source_paths.append(path)
                yield location
                return
            stage_paths.append(path)
            yield location
            if not attempted:
                require(expected is None or expected == [], "nonempty stage skipped the MERGE")
                check_stage(writer.read.parquet((path / "rows").as_uri()), dimension, [])

    def sql(statement, *args, **kwargs):
        flattened = " ".join(statement.split())
        if not flattened.startswith("MERGE "):
            return original_sql(statement, *args, **kwargs)
        require(flattened.startswith(f"MERGE INTO {dimension.table} AS t "), "unexpected MERGE target")
        require(len(stage_paths) == 1 and not attempted, "expected a single MERGE")
        attempted.append(True)
        match = re.search(r"\bFROM parquet\.\x60(file:///[^ \x60]+)\x60", statement)
        path = stage_paths[0] / "rows"
        require(match is not None and match[1] == path.as_uri() and any(path.glob("*.parquet")),
                "scoped physical Parquet source")
        check_stage(writer.read.parquet(match[1]), dimension, expected)
        if fault == "before":
            raise ApplicationFault("pre-commit")
        result = original_sql(statement, *args, **kwargs)
        result.collect()
        committed.append(True)
        if fault == "lost-ack":
            child_snapshot(before, observe(spark, dimension))
            raise ApplicationFault("post-commit")
        return result

    failure = None
    try:
        with patch.object(run.gold, "get_spark_session", return_value=writer), \
                patch.object(run.gold, "tempfile", SimpleNamespace(TemporaryDirectory=stage_directory)), \
                patch.object(writer, "stop") as stop, patch.object(writer, "sql", side_effect=sql):
            try:
                getattr(run.gold, f"create_dim_{dimension.name}_info")()
                completed.append(True)
            except ApplicationFault as error:
                failure = error
            stop.assert_called_once_with()
    finally:
        require(all(not path.exists() for path in (*source_paths, *stage_paths)), "scratch cleanup")

    require(len(source_paths) == int(dimension.name != "detail") and len(stage_paths) == 1,
            "expected scoped input and MERGE scratch directories")
    if expected is not None:
        require(len(attempted) == int(bool(expected)), "MERGE attempt count")
        require(len(committed) == int(bool(expected) and fault != "before"), "commit call count")
    require((failure is not None) == (fault is not None) and bool(completed) == (fault is None),
            "unexpected application fault")


def join_counts(spark, dimension, ids, old_refs):
    """Join a left fact-shaped probe by AppID and a retained FK probe by both keys."""
    reader = spark.newSession()
    data = reader.table(dimension.table).select("steam_appid", dimension.key)
    probe = reader.createDataFrame([(appid,) for appid in ids], "steam_appid bigint")
    linked = probe.join(data, "steam_appid", "left").collect()
    app_counts = Counter(item[0] for item in linked if item[1] is not None)
    if not old_refs:
        return app_counts, Counter()
    ref = reader.createDataFrame(old_refs, f"steam_appid bigint, {dimension.key} int")
    retained = Counter(tuple(item) for item in ref.join(data, ["steam_appid", dimension.key], "inner").collect())
    return app_counts, retained


def canonical(run, spark, dimension):
    seed(run, spark, dimension)
    initial = observe(spark, dimension)
    records = [(101, 10, 2, True), (101, 90, 3, False),
               (202, 11, 2, True), (303, 12, 2, True), (404, 5, 2, True)]
    if dimension.name == "detail":
        records.append((101, 91, 1, True))
    freeze(spark, dimension, records)
    rows = [row(dimension, *item) for item in ((2, 101, 10), (7, 202, 11), (21, 303, 12))]
    stage = [(*item, item[1] != 303) for item in rows]
    refresh(run, spark, dimension, stage)
    first = observe(spark, dimension)
    require(first.rows == replacement(initial, rows), "changed/new/unchanged rows")
    child_snapshot(initial, first)
    refresh(run, spark, dimension, [])
    require(observe(spark, dimension) == first, "unchanged rerun")

    records.extend([(505, 13, 2, True)])
    records[0] = (101, 14, 2, True)
    freeze(spark, dimension, records)
    rows = [row(dimension, 2, 101, 14), row(dimension, 22, 505, 13)]
    stage = [(*item, item[1] == 101) for item in rows]
    refresh(run, spark, dimension, stage, fault="before")
    require(observe(spark, dimension) == first, "pre-commit failure altered rows or snapshots")
    refresh(run, spark, dimension, stage)
    second = observe(spark, dimension)
    require(second.rows == replacement(first, rows), "frozen pre-commit retry")
    child_snapshot(first, second)

    records.append((606, 15, 2, True))
    records[0] = (101, 16, 2, True)
    freeze(spark, dimension, records)
    rows = [row(dimension, 2, 101, 16), row(dimension, 23, 606, 15)]
    stage = [(*item, item[1] == 101) for item in rows]
    refresh(run, spark, dimension, stage, fault="lost-ack", before=second)
    third = observe(spark, dimension)
    require(third.rows == replacement(second, rows), "state after lost acknowledgement")
    child_snapshot(second, third)
    refresh(run, spark, dimension, [])
    require(observe(spark, dimension) == third, "frozen lost-ack retry")

    refs = [(101, 2), (202, 7), (303, 21), (404, 9), (505, 22), (606, 23), (909, 20)]
    by_appid, by_key = join_counts(spark, dimension, [appid for appid, _ in refs], refs)
    # The untouched old duplicate for 404 is expected to remain. Retained fact keys still join once.
    require(by_appid == Counter({appid: 2 if appid == 404 else 1 for appid, _ in refs})
            and by_key == Counter(refs), "retained fact references")
    return {"rows": sum(third.rows.values()), "retained_fact_references": len(refs),
            "successful_snapshots": 3, "readback_verified": True, "scratch_cleaned": True}


def keys_for(state, appid):
    keys = Counter()
    for payload, count in state.rows.items():
        if payload[1] == appid:
            keys[payload[0]] += count
    return keys


def duplicate_findings(first_keys, replay_keys, fact_matches, *, snapshot_delta):
    """Measure duplicate integrity independently of write atomicity."""
    return {
        "collision": max(sum(count - 1 for count in keys.values()) for keys in (first_keys, replay_keys)),
        "repeated_commit": int(snapshot_delta > 0),
        "fanout": max(0, fact_matches - 1),
        "key_drift": int(first_keys.keys() != replay_keys.keys()),
    }


def load_baseline(run):
    # Populated only by deterministic bundle preparation from the sanitized pinned blob.
    source = globals().get("PINNED_BASELINE_SOURCE")
    manifest = json.loads((run.root / "run.json").read_bytes())
    require(type(source) is bytes and manifest.get("baseline_revision") == BASE
            and manifest.get("baseline_sha256") == hashlib.sha256(source).hexdigest(), "baseline provenance")
    scope = {"__name__": "pinned_baseline_gold"}
    exec(compile(source, "pinned-baseline-gold.py", "exec"), scope)
    return scope


def baseline_refresh(baseline, spark, dimension):
    writer = spark.newSession()
    # Bind only the session. Preserve historical SQL, lazy plans, and both writes.
    with patch.dict(baseline, {"get_spark_session": lambda *args: writer}), patch.object(writer, "stop") as stop:
        baseline[f"create_dim_{dimension.name}_info"]()
        stop.assert_called_once_with()


def reset_duplicate_target(spark, dimension, seed_rows):
    spark.createDataFrame(list(seed_rows.elements()), schema(dimension.schema)) \
        .writeTo(dimension.table).overwritePartitions()
    before = observe(spark, dimension)
    require(before.rows == seed_rows, "identical duplicate fixture target seed")
    return before


def appid_payloads(state, appid):
    counts = Counter()
    for payload, count in state.rows.items():
        if payload[1] == appid:
            counts[payload[2:]] += count
    return counts


def measure_baseline(spark, dimension, appid, revisions, seed_rows, run_refresh):
    before = reset_duplicate_target(spark, dimension, seed_rows)
    run_refresh()
    first = observe(spark, dimension)
    first_links, _ = join_counts(spark, dimension, [appid], [])
    first_delta = len(first.snapshots) - len(before.snapshots)
    first_keys = keys_for(first, appid)
    first_collision = sum(count - 1 for count in first_keys.values())
    first_fanout = max(0, first_links[appid] - 1)
    try:
        run_refresh()
        replay = observe(spark, dimension)
        replay_links, _ = join_counts(spark, dimension, [appid], [])
    except Exception:
        partial = {"first_snapshots": first_delta, "first_rows": sum(first_keys.values()),
                   "first_collision": first_collision, "first_fanout": first_fanout}
        return partial, "EXECUTION_ERROR"

    delta = len(replay.snapshots) - len(first.snapshots)
    before_keys = keys_for(before, appid)
    replay_keys = keys_for(replay, appid)
    expected_keys = (Counter({max(before_keys): len(revisions)}) if before_keys else
                     Counter(max(payload[0] for payload in before.rows) + offset
                             for offset in range(1, len(revisions) + 1)))
    expected_payloads = Counter(row(dimension, 0, appid, revision)[2:] for revision in revisions)
    away = Counter({payload: count for payload, count in before.rows.items() if payload[1] != appid})
    preserved = all(Counter({payload: count for payload, count in state.rows.items() if payload[1] != appid}) == away
                    for state in (first, replay))
    complete = (first_delta > 0 and delta >= 0 and (delta > 0 or first == replay)
                and first_keys == expected_keys and replay_keys and preserved
                and all(appid_payloads(state, appid) == expected_payloads for state in (first, replay))
                and first_links[appid] == sum(first_keys.values()) and replay_links[appid] == sum(replay_keys.values()))

    measurements = duplicate_findings(first_keys, replay_keys, max(first_links[appid], replay_links[appid]),
                                     snapshot_delta=delta)
    measurements.update(first_snapshots=first_delta, replay_snapshots=delta,
                        first_rows=sum(first_keys.values()), replay_rows=sum(replay_keys.values()),
                        first_collision=first_collision, first_fanout=first_fanout,
                        replay_collision=sum(count - 1 for count in replay_keys.values()),
                        replay_fanout=max(0, replay_links[appid] - 1))
    return measurements, "MEASURED" if complete else "INCONCLUSIVE"


def reject_duplicate_input(run, spark, dimension, before):
    writer = spark.newSession()
    target_rows = before.rows
    paths = []
    write_attempts = []
    source_prefix = f"gold-dim_{dimension.name}_info-source-"
    gold = run.gold

    @contextmanager
    def temporary_directory(*, prefix):
        require(prefix == source_prefix, "unexpected write staging for rejected input")
        with tempfile.TemporaryDirectory(prefix=prefix) as location:
            path = Path(location).resolve()
            paths.append(path)
            try:
                yield location
            finally:
                require(any((path / "input").glob("*.parquet")), "missing frozen duplicate input")

    def unexpected_write(*args, **kwargs):
        write_attempts.append(True)
        raise AssertionError("duplicate input attempted a Gold write")

    # Check every existing key, including any pre-existing duplicate multiplicity.
    refs = sorted({(entry[1], entry[0]) for entry in target_rows})
    expected_links = Counter()
    for entry, count in target_rows.items():
        expected_links[(entry[1], entry[0])] += count
    frame_type = type(writer.table(dimension.table))
    for _ in range(2):
        try:
            with patch.object(gold, "get_spark_session", return_value=writer), \
                    patch.object(gold, "tempfile", SimpleNamespace(TemporaryDirectory=temporary_directory)), \
                    patch.object(writer, "stop") as stop, patch.object(writer, "sql", side_effect=unexpected_write), \
                    patch.object(frame_type, "writeTo", side_effect=unexpected_write):
                try:
                    getattr(gold, f"create_dim_{dimension.name}_info")()
                except gold.DuplicateCurrentInputError as error:
                    require(str(error).startswith(f"dim_{dimension.name}_info: multiple current Silver rows"),
                            "unexpected controlled rejection")
                else:
                    raise AssertionError("duplicate input was accepted")
                stop.assert_called_once_with()
        finally:
            require(all(not path.exists() for path in paths), "duplicate input scratch cleanup")
        require(not write_attempts and observe(spark, dimension) == before, "target changed after rejection")
    require(len(set(paths)) == 2, "expected one frozen input per rejection attempt")
    _, retained = join_counts(spark, dimension, sorted({appid for appid, _ in refs}), refs)
    require(retained == expected_links, "rejected input changed existing fact references")
    return {"outcome": "REJECTED_DUPLICATE_INPUT", "attempts": 2, "target_write_attempts": 0,
            "rows_unchanged": True, "snapshots_unchanged": True, "references_preserved": True,
            "scratch_cleaned": True}


def duplicates(run, spark, dimension, baseline):
    evidence = {}
    inputs = zip(VARIANTS, (101, 701, 202, 702), ((30, 30), (31, 31), (32, 33), (34, 35)))
    seed_rows = observe(spark, dimension).rows
    for variant, appid, revisions in inputs:
        freeze(spark, dimension, [(appid, revision, 2, True) for revision in revisions])
        before = reset_duplicate_target(spark, dimension, seed_rows)
        try:
            candidate = reject_duplicate_input(run, spark, dimension, before)
        except Exception as error:
            candidate = {"outcome": "FAIL", "error": type(error).__name__}
        historical, status = None, "EXECUTION_ERROR"
        if baseline is not None:
            try:
                historical, status = measure_baseline(spark, dimension, appid, revisions, seed_rows,
                                                      lambda: baseline_refresh(baseline, spark, dimension))
            except Exception:
                # A native baseline error leaves the historical result unverified.
                pass
        evidence[variant] = {"candidate": candidate, "baseline": historical, "baseline_status": status}
    return evidence


def emit(case, record, *, status="PASS"):
    print(f"{case} {status} " + json.dumps(record, separators=(",", ":")), flush=True)


def run_phase_1b(run):
    case = "C03B-SETUP"
    failed = False
    try:
        spark = run.new_spark()
        run.session = spark
        require(re.fullmatch(r"s3a://native-[0-9a-f]{16}", run.storage), "disposable warehouse")
        for namespace in ("silver", "gold"):
            spark.sql(f"CREATE NAMESPACE hive.{namespace} LOCATION '{run.storage}/{namespace}'").collect()
        for dimension in DIMENSIONS:
            case = dimension.case
            print(f"{case} START", flush=True)
            emit(case, canonical(run, spark, dimension))
        try:
            baseline = load_baseline(run)
        except Exception:
            baseline = None
        for dimension in DIMENSIONS[1:]:
            case = dimension.case + "-DUPS"
            print(f"{case} START", flush=True)
            record = duplicates(run, spark, dimension, baseline)
            passed = all(entry["candidate"]["outcome"] == "REJECTED_DUPLICATE_INPUT" for entry in record.values())
            emit(case, record, status="PASS" if passed else "FAIL")
            failed = failed or not passed
    except Exception as error:
        print(f"{case} DIAGNOSTIC {type(error).__name__}", flush=True)
        raise
    require(not failed, "duplicate input was not safely rejected")


SCENARIOS = {SCENARIO: run_phase_1b}
