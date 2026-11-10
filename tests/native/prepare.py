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

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
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
RUNNER_FILES = ("runner.Dockerfile", "runtime.py", "scenarios.py", "fixtures.py")
HMS_FILES = ("metastore.Dockerfile", "hive-site.xml", "core-site.xml")
SUBNET = "172.30.233.0/28"


def git_blob(revision, name):
    try:
        return subprocess.run(
            ["git", "show", f"{revision}:LakeHouse_Project/scripts/{name}"],
            cwd=REPO, check=True, capture_output=True,
        ).stdout.decode("utf-8-sig")
    except subprocess.CalledProcessError:
        raise ValueError(f"Reviewed Git object is unavailable: {revision}:{name}") from None


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
    elif name in MODULES:
        classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Config"]
        if len(classes) != 1:
            raise ValueError(f"Unreviewed Config shape in {name}")
        pending = CONFIG.copy()
        for node in classes[0].body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name) and target.id in pending:
                    node.value = replace(node.value, pending.pop(target.id))
        if pending:
            raise ValueError(f"Unreviewed Config bindings in {name}")

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


def expected_files(revision, scenario, project, password):
    if revision not in CANDIDATES or scenario not in SCENARIOS:
        raise ValueError("Unreviewed candidate SHA or scenario")
    if not re.fullmatch(r"native-[0-9a-f]{16}", project) or not re.fullmatch(r"[0-9a-f]{48}", password):
        raise ValueError("Invalid disposable resource identity")

    files = {"compose.json": json_bytes(compose(project))}
    for name in RUNNER_FILES:
        files[f"runner/{name}"] = (HERE / name).read_text(encoding="utf-8").encode()
    for name in HMS_FILES:
        files[f"metastore/{name}"] = (HERE / name).read_text(encoding="utf-8").encode()
    modules = MODULES + (("bronze_appid.py",) if revision == BRONZE else ())
    scripts = {
        name: sanitized(name, git_blob(revision, name)) for name in modules
    }
    files.update({f"runner/scripts/{name}": content for name, content in scripts.items()})
    files["runner/scripts/__init__.py"] = b""
    files["runner/run.json"] = json_bytes({
        "candidate": revision, "scenario": scenario, "project": project,
        "scripts": {name: hashlib.sha256(content).hexdigest() for name, content in scripts.items()},
    })
    files["test.env"] = (
        f"MINIO_ROOT_USER={project}-test\nMINIO_ROOT_PASSWORD={password}\n"
    ).encode()
    return files


def prepare(revision, scenario):
    project = "native-" + secrets.token_hex(8)
    files = expected_files(revision, scenario, project, secrets.token_hex(24))
    folder = Path(tempfile.mkdtemp(prefix=project + "-", dir=temp_root()))
    for relative, content in files.items():
        path = folder / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    verify(folder)
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


def verify(folder):
    if (not re.fullmatch(r"native-[0-9a-f]{16}-[a-z0-9_]+", folder.name)
            or folder.parent.resolve() != temp_root() or is_link(folder)):
        raise ValueError("Expected a generated run directory directly under system temp")
    for path in folder.rglob("*"):
        if is_link(path):
            raise ValueError("Run bundle contains a filesystem link")
    manifest = json.loads((folder / "runner/run.json").read_bytes())
    if not folder.name.startswith(manifest["project"] + "-"):
        raise ValueError("Run directory and project identity differ")
    env = (folder / "test.env").read_text(encoding="ascii").splitlines()
    if len(env) != 2 or not env[1].startswith("MINIO_ROOT_PASSWORD="):
        raise ValueError("Unexpected fixture credential file")
    expected = expected_files(
        manifest["candidate"], manifest["scenario"], manifest["project"], env[1].partition("=")[2],
    )

    actual = {path.relative_to(folder).as_posix() for path in folder.rglob("*") if path.is_file()}
    if actual != set(expected):
        raise ValueError("Run bundle file set differs from the reviewed context")
    for name, content in expected.items():
        if (folder / name).read_bytes() != content:
            raise ValueError(f"Run bundle differs from the reviewed context: {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    stage = commands.add_parser("prepare")
    stage.add_argument("--candidate", required=True, choices=sorted(CANDIDATES))
    stage.add_argument("--scenario", required=True, choices=SCENARIOS)
    check = commands.add_parser("verify")
    check.add_argument("folder", type=Path)
    args = parser.parse_args()
    if args.action == "prepare":
        print(prepare(args.candidate, args.scenario))
    else:
        verify(args.folder)
        print("PASS: exact SHA staging, isolated Compose configuration and build context")


if __name__ == "__main__":
    main()
