"""Assemble and statically verify a disposable native run without invoking Docker."""

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import tempfile

# The approved CLI can be streamed directly from its pinned Git object.
REPO = Path.cwd().resolve() if __file__ == "<stdin>" else Path(__file__).absolute().parents[2].resolve()
HARNESS_DIR = "tests/native"
HERE = REPO / HARNESS_DIR
BASE = "5fd358ab12a637b06b175d4da77d339026eac3bf"
BRONZE = "bcea0e946bb7b199e797943a82cfe82b65a89883"
GOLD = "b0f733580c75b561893580a1dd15b7f344602d36"
CANDIDATES = {BASE, BRONZE, GOLD}
SCENARIOS = (
    "bronze", "silver", "silver-catalog", "silver-create-collision",
    "silver-before-update", "silver-append-failure", "silver-lost-ack",
    "gold", "gold-helpers", "gold-partial", "gold-create-collision",
)
MODULES = ("rawData_to_bronze.py", "bronze_to_silver_etl.py", "silver_to_gold_etl.py")
CONFIG = {
    "MINIO_ENDPOINT": "blocked.invalid:9000",
    "MINIO_ACCESS_KEY": "UNCONFIGURED",
    "MINIO_SECRET_KEY": "UNCONFIGURED",
    "BUCKET": "unconfigured",
    "JARS": "",
    "HIVE_METASTORE_URI": "thrift://blocked.invalid:9083",
}
STORAGE_PATHS = {
    MODULES[1]: {"BRONZE_PATH": "bronze", "SILVER_PATH": "silver", "WAREHOUSE_PATH": "warehouse"},
    MODULES[2]: {"SILVER_PATH": "silver", "GOLD_PATH": "gold", "WAREHOUSE_PATH": "warehouse"},
}
BUSINESS_CONFIG = {MODULES[1]: {"USD_TO_VND", "CURRENT_YEAR", "SCD2_CHANGE_COLS"}, MODULES[2]: set()}
RUNNER_FILES = ("runner.Dockerfile", "runtime.py", "scenarios.py", "fixtures.py")
HMS_FILES = ("metastore.Dockerfile", "hive-site.xml", "core-site.xml")
HARNESS_FILES = set(RUNNER_FILES + HMS_FILES + ("prepare.py", "test_harness.py"))
SUBNET = "172.30.233.0/28"


def read_git(*args):
    try:
        return subprocess.run(
            ["git", "--no-replace-objects", *args], cwd=REPO, check=True, capture_output=True,
        ).stdout
    except subprocess.CalledProcessError:
        raise ValueError("Required Git object is unavailable") from None


def git_blob(revision, name):
    return read_git("show", f"{revision}:LakeHouse_Project/scripts/{name}").decode("utf-8-sig")


def reviewed_harness(revision):
    """Read immutable build inputs and reject working-copy drift from the approval."""
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Expected a full approved harness commit SHA")
    if read_git("cat-file", "-t", revision).strip() != b"commit":
        raise ValueError("Approved harness revision must identify a commit")
    files = {}
    entries = read_git("ls-tree", "-rz", revision, "--", HARNESS_DIR).split(b"\0")
    for entry in filter(None, entries):
        metadata, path = entry.split(b"\t", 1)
        mode, kind, oid = metadata.split()
        relative = path.decode()
        name = relative.removeprefix(HARNESS_DIR + "/")
        if (name not in HARNESS_FILES or relative != f"{HARNESS_DIR}/{name}"
                or mode != b"100644" or kind != b"blob"):
            raise ValueError("Unexpected file in approved harness tree")
        files[name] = read_git("cat-file", "blob", oid.decode())
    if set(files) != HARNESS_FILES:
        raise ValueError("Approved harness tree is incomplete")

    if not HERE.is_dir() or is_link(HERE.parent) or is_link(HERE):
        raise ValueError("Expected an ordinary harness directory")
    if {path.name for path in HERE.iterdir()} != HARNESS_FILES:
        raise ValueError("Working harness file set differs from the approved revision")
    for name, blob in files.items():
        path = HERE / name
        # Allow Windows checkout line endings. The build still receives the exact Git bytes.
        if is_link(path) or not path.is_file() or path.read_bytes().replace(b"\r\n", b"\n") != blob:
            raise ValueError(f"Working harness file differs from the approved revision: {name}")
    return files


def sanitized(name, source):
    """Strip production connection literals before any source enters a build context."""
    tree = ast.parse(source)
    removed = set()

    def replace(node, value):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            raise ValueError(f"Unexpected connection binding in {name}")
        removed.add(node.value)
        return ast.Constant(value)

    if name == MODULES[0]:
        calls = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "Minio"
        ]
        if len(calls) != 3:
            raise ValueError("Unreviewed Bronze client shape")
        for call in calls:
            if len(call.args) != 1:
                raise ValueError("Unreviewed Bronze endpoint shape")
            call.args[0] = replace(call.args[0], CONFIG["MINIO_ENDPOINT"])
            fields = {item.arg: item for item in call.keywords}
            if set(fields) != {"access_key", "secret_key", "secure"}:
                raise ValueError("Unreviewed Bronze credential shape")
            for field in ("access_key", "secret_key"):
                fields[field].value = replace(fields[field].value, "UNCONFIGURED")
        loaders = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                   and node.name == "detect_and_upload"]
        if len(loaders) != 1:
            raise ValueError("Unreviewed Airflow source loader shape")
        tree.body.remove(loaders[0])
    elif name in STORAGE_PATHS:
        classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Config"]
        if len(classes) != 1 or classes[0].bases or classes[0].keywords or classes[0].decorator_list:
            raise ValueError(f"Unreviewed Config shape in {name}")
        paths = STORAGE_PATHS[name]
        pending = CONFIG.keys() | paths.keys() | BUSINESS_CONFIG[name]
        config = classes[0]
        bindings = config.body[1:] if ast.get_docstring(config, clean=False) is not None else config.body
        for node in bindings:
            if (not isinstance(node, ast.Assign) or len(node.targets) != 1
                    or not isinstance(node.targets[0], ast.Name) or node.targets[0].id not in pending):
                raise ValueError(f"Unreviewed Config binding in {name}")
            key = node.targets[0].id
            pending.remove(key)
            if key in CONFIG:
                node.value = replace(node.value, CONFIG[key])
            elif key in paths:
                expected = ast.parse(f'f"s3a://{{BUCKET}}/{paths[key]}"', mode="eval").body
                if "BUCKET" in pending or ast.dump(node.value) != ast.dump(expected):
                    raise ValueError(f"Unexpected storage path binding in {name}: {key}")
                # This expression can resolve only through the sanitized BUCKET binding.
        if pending:
            raise ValueError(f"Missing reviewed Config bindings in {name}")

    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if any(value and value in node.value for value in removed):
                raise ValueError(f"Production binding still present in {name}")
            if re.search(r"(?i)(/opt/airflow/source_data|[a-z]:\\|thrift://(?!blocked.invalid:))", node.value):
                if node.value != CONFIG["HIVE_METASTORE_URI"]:
                    raise ValueError(f"Unexpected absolute path or endpoint in {name}")
    result = ast.unparse(tree) + "\n"
    compile(result, name, "exec")
    return result.encode()


def compose(project):
    """Generate a closed topology with project-scoped volumes and no host mounts."""
    def network(address):
        return {"isolated": {"ipv4_address": address}}

    return {
        "name": project,
        "services": {
            "fixture-minio": {
                "image": "quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z",
                "command": ["server", "/data", "--address", ":9000"],
                "env_file": ["test.env"],
                "volumes": ["objects:/data"],
                "networks": network("172.30.233.2"),
            },
            "fixture-hms": {
                "build": {"context": "./metastore", "dockerfile": "metastore.Dockerfile"},
                "environment": {"SERVICE_NAME": "metastore", "DB_DRIVER": "derby",
                                "HIVE_CUSTOM_CONF_DIR": "/opt/native-hive"},
                "env_file": ["test.env"],
                "volumes": ["metastore:/native-meta"],
                "networks": network("172.30.233.3"),
            },
            "runner": {
                "build": {"context": "./runner", "dockerfile": "runner.Dockerfile"},
                "env_file": ["test.env"],
                "environment": {
                    "NATIVE_ALLOW_WRITES": "0",
                    "HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": "",
                    "http_proxy": "", "https_proxy": "", "all_proxy": "",
                },
                "profiles": ["native"],
                "networks": network("172.30.233.4"),
            },
        },
        "volumes": {"objects": {}, "metastore": {}},
        "networks": {"isolated": {"internal": True, "enable_ipv6": False,
                                  "ipam": {"config": [{"subnet": SUBNET}]}}},
    }


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def expected_files(revision, scenario, project, password, harness_revision):
    if revision not in CANDIDATES or scenario not in SCENARIOS:
        raise ValueError("Unreviewed candidate SHA or scenario")
    if not re.fullmatch(r"native-[0-9a-f]{16}", project) or not re.fullmatch(r"[0-9a-f]{48}", password):
        raise ValueError("Invalid disposable resource identity")

    approved = reviewed_harness(harness_revision)
    files = {"compose.json": json_bytes(compose(project))}
    for name in RUNNER_FILES:
        files[f"runner/{name}"] = approved[name]
    for name in HMS_FILES:
        files[f"metastore/{name}"] = approved[name]
    modules = MODULES + (("bronze_appid.py",) if revision == BRONZE else ())
    scripts = {
        name: sanitized(name, git_blob(revision, name)) for name in modules
    }
    files.update({f"runner/scripts/{name}": content for name, content in scripts.items()})
    files["runner/scripts/__init__.py"] = b""
    files["runner/run.json"] = json_bytes({
        "candidate": revision, "harness_revision": harness_revision, "scenario": scenario, "project": project,
        "scripts": {name: hashlib.sha256(content).hexdigest() for name, content in scripts.items()},
    })
    files["test.env"] = (
        f"MINIO_ROOT_USER={project}-test\nMINIO_ROOT_PASSWORD={password}\n"
    ).encode()
    return files


def prepare(revision, scenario, harness_revision):
    project = "native-" + secrets.token_hex(8)
    files = expected_files(revision, scenario, project, secrets.token_hex(24), harness_revision)
    folder = Path(tempfile.mkdtemp(prefix=project + "-", dir=temp_root()))
    for relative, content in files.items():
        path = folder / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    verify(folder, harness_revision, candidate=revision, scenario=scenario)
    return folder


def temp_root():
    # Avoid probing arbitrary TMP overrides, which can themselves write into a worktree.
    folder = (Path(os.environ["LOCALAPPDATA"]) / "Temp" if os.name == "nt" else Path("/tmp")).resolve()
    if folder == REPO or REPO in folder.parents:
        raise ValueError("System temp directory must be outside the recovery worktree")
    if any((parent / ".git").exists() for parent in (folder, *folder.parents)):
        raise ValueError("System temp directory must be outside Git worktrees")
    if not folder.is_dir():
        raise ValueError("Local system temp directory is unavailable")
    return folder


def is_link(path):
    attributes = getattr(path.lstat(), "st_file_attributes", 0)
    return path.is_symlink() or bool(attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT)


def verify(folder, harness_revision, *, candidate, scenario):
    if (not re.fullmatch(r"native-[0-9a-f]{16}-[a-z0-9_]+", folder.name)
            or folder.parent.resolve() != temp_root() or is_link(folder)):
        raise ValueError("Expected a generated run directory directly under system temp")
    files, directories = set(), set()
    for path in folder.rglob("*"):
        if is_link(path):
            raise ValueError("Run bundle contains a filesystem link")
        relative = path.relative_to(folder).as_posix()
        if path.is_dir():
            directories.add(relative)
        elif path.is_file():
            files.add(relative)
        else:
            raise ValueError("Run bundle contains an unexpected filesystem entry")
    manifest = json.loads((folder / "runner/run.json").read_bytes())
    if manifest.get("harness_revision") != harness_revision:
        raise ValueError("Run bundle harness SHA differs from the approved revision")
    if manifest.get("candidate") != candidate or manifest.get("scenario") != scenario:
        raise ValueError("Run bundle candidate or scenario differs from the approved run")
    if not folder.name.startswith(manifest["project"] + "-"):
        raise ValueError("Run directory and project identity differ")
    env = (folder / "test.env").read_text(encoding="ascii").splitlines()
    if len(env) != 2 or not env[1].startswith("MINIO_ROOT_PASSWORD="):
        raise ValueError("Unexpected fixture credential file")
    expected = expected_files(
        candidate, scenario, manifest["project"], env[1].partition("=")[2],
        harness_revision,
    )

    expected_directories = {parent.as_posix() for name in expected for parent in Path(name).parents
                            if parent != Path(".")}
    if files != set(expected) or directories != expected_directories:
        raise ValueError("Run bundle file set differs from the reviewed context")
    for name, content in expected.items():
        if (folder / name).read_bytes() != content:
            raise ValueError(f"Run bundle differs from the reviewed context: {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    stage = commands.add_parser("prepare")
    check = commands.add_parser("verify")
    check.add_argument("folder", type=Path)
    for command in (stage, check):
        command.add_argument("--candidate", required=True, choices=sorted(CANDIDATES))
        command.add_argument("--scenario", required=True, choices=SCENARIOS)
        command.add_argument("--harness-revision", required=True, help="Full commit SHA approved by the reviewer")
    args = parser.parse_args()
    if args.action == "prepare":
        print(prepare(args.candidate, args.scenario, args.harness_revision))
    else:
        verify(args.folder, args.harness_revision, candidate=args.candidate, scenario=args.scenario)
        print(f"PASS: harness {args.harness_revision}, exact candidate staging and isolated build context")


if __name__ == "__main__":
    if __file__ != "<stdin>":
        raise SystemExit("Stream the approved Git blob into Python as described in the runbook")
    main()
