"""Run only the reviewed Silver C02 scenario on an Owner-approved Actions VM."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace

REPOSITORY = "Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender"
ORIGIN = f"https://github.com/{REPOSITORY}.git"
OWNER = REPOSITORY.split("/")[0]
BRANCH = "codex/silver-c02-native-validation"
APPROVAL = "native-silver-c02-approved"
HARNESS = "adc2d56803b9493c2e366f9a31cc3b85ad4e9816"
REVIEWED_PILOT = "ed0937882e505fac6bb658d00cfc858d73fa6332"
CANDIDATE = "724cf1554111951065120ea64a9ee5f7abe6f038"
SCENARIO = "silver-c02"
SCENARIO_PATH = "tests/ci/silver_c02_scenarios.py"
CASE_IDS = ("C02-01", "C02-02", "C02-03", "C02-04")
GIB = 1024**3


class C02Failure(RuntimeError):
    """A gate, provenance, budget or scenario evidence check failed."""


def require(condition, message):
    if not condition:
        raise C02Failure(message)


def approved_event(environment, event):
    """Bind native execution to the Owner's first labeled event and its exact head."""
    head = environment.get("PILOT_HEAD", "")
    pull = event["pull_request"]
    require(
        re.fullmatch(r"[0-9a-f]{40}", head)
        and environment.get("GITHUB_ACTIONS") == "true"
        and environment.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and environment.get("RUNNER_OS") == "Linux"
        and environment.get("GITHUB_EVENT_NAME") == "pull_request"
        and environment.get("GITHUB_REPOSITORY") == REPOSITORY
        and environment.get("GITHUB_ACTOR") == OWNER
        and environment.get("GITHUB_TRIGGERING_ACTOR") == OWNER
        and environment.get("GITHUB_RUN_ATTEMPT") == "1"
        and environment.get("NATIVE_ALLOW_WRITES") == "0"
        and event["repository"]["full_name"] == REPOSITORY
        and event["sender"]["login"] == OWNER
        and event["action"] == "labeled"
        and event["label"]["name"] == APPROVAL
        and pull["state"] == "open"
        and pull["draft"] is False
        and pull["base"]["ref"] == "main"
        and pull["base"]["repo"]["full_name"] == REPOSITORY
        and pull["head"]["repo"]["full_name"] == REPOSITORY
        and pull["head"]["ref"] == BRANCH
        and pull["head"]["sha"] == head,
        "Expected the first Owner-approved Silver C02 event on the dedicated branch",
    )
    return head


def git(repository, *args):
    result = subprocess.run(
        ["git", "--no-replace-objects", "-c", "credential.helper=", "-C", str(repository), *args],
        capture_output=True, check=False, timeout=60,
    )
    require(result.returncode == 0, "Pinned Git input unavailable")
    return result.stdout


def checked_blob(repository, revision, path):
    require(git(repository, "rev-parse", "HEAD").decode().strip() == revision,
            "Unexpected checkout revision")
    return git(repository, "show", f"{revision}:{path}")


def bind_preparation(source, scenario_source, harness_dir):
    """Extend exact PR #14 bundle generation for just the reviewed candidate/scenario pair."""
    scope = {"__file__": "<stdin>", "__name__": "pinned_native_prepare"}
    exec(compile(source, "pinned-native-prepare.py", "exec"), scope)
    scope["REPO"] = harness_dir
    scope["HERE"] = harness_dir / scope["HARNESS_DIR"]
    scope["CANDIDATES"] = {CANDIDATE}
    scope["SCENARIOS"] = (SCENARIO,)
    original = scope["expected_files"]

    def expected_files(candidate, scenario, project, password, harness_revision):
        require((candidate, scenario, harness_revision) == (CANDIDATE, SCENARIO, HARNESS),
                "Unexpected Silver candidate, scenario or harness revision")
        files = original(candidate, scenario, project, password, harness_revision)
        # The reviewed runtime imports Bronze even for Silver. The PR #16 tree has this helper.
        helper = "bronze_appid.py"
        helper_bytes = scope["sanitized"](helper, scope["git_blob"](candidate, helper))
        files[f"runner/scripts/{helper}"] = helper_bytes
        manifest = json.loads(files["runner/run.json"])
        manifest["scripts"][helper] = hashlib.sha256(helper_bytes).hexdigest()
        files["runner/run.json"] = scope["json_bytes"](manifest)
        files["runner/scenarios.py"] = scenario_source
        return files

    scope["expected_files"] = expected_files
    return scope


def evidence_lines(output, redactions):
    """Require all four reviewed cases and forward only bounded synthetic JSON."""
    lines = {}
    for line in output.splitlines():
        if not line.startswith(b"C02-"):
            continue
        case, separator, payload = line.partition(b" PASS ")
        identifier = case.decode("ascii")
        require(separator and identifier in CASE_IDS and identifier not in lines and len(payload) <= 6000,
                "Unexpected or duplicate Silver evidence record")
        record = json.loads(payload)
        require(isinstance(record, dict) and record, "Expected structured Silver evidence")
        encoded = json.dumps(record, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
        require(len(encoded) <= 6000 and not any(secret and secret in encoded for secret in redactions),
                "Silver evidence exceeded its budget or included fixture credentials")
        lines[identifier] = f"{identifier} PASS {encoded}"
    require(tuple(lines) == CASE_IDS and b"Scenario completed: silver-c02" in output.splitlines(),
            "Expected all four Silver cases and the scenario completion marker")
    return list(lines.values())


C02_FAILURE_STAGES = {
    "fixture-transform", "fixture-parquet-write", "fixture-spark-open", "fixture-namespace-create",
    "fixture-parquet-read", "fixture-iceberg-seed", "fixture-source-collect", "fixture-spark-close",
    "fixture-temp-cleanup", "seed-reader-open", "seed-reader-rows", "seed-reader-snapshots",
    "seed-reader-close", "seed-assertions", "writer-open", "writer-create-input", "upsert-plan",
    "merge-sql", "upsert-cleanup", "writer-close", "transition-count", "committed-reader-open",
    "committed-reader-rows", "committed-reader-snapshots", "committed-reader-close",
    "transition-assertions", "transition-evidence",
}
SAFE_NATIVE_ERRORS = {
    "AnalysisException", "AssertionError", "FileNotFoundError", "IllegalArgumentException",
    "ImportError", "ModuleNotFoundError", "OSError", "ParseException", "PermissionError",
    "Py4JJavaError", "RuntimeError", "SparkRuntimeException", "TimeoutError", "TypeError", "ValueError",
}


SAFE_MERGE_ERROR_CATEGORIES = {
    "spark-view-metadata-columns", "iceberg-write-schema", "iceberg-commit", "unclassified",
}


def native_failure_line(content):
    """Forward only reviewed C02-01 diagnostic fields."""
    prefix = "C02-01 DIAGNOSTIC "
    markers = [line for line in content.splitlines() if line.startswith(prefix)]
    if len(markers) == 1:
        match = re.fullmatch(
            r"C02-01 DIAGNOSTIC ([a-z-]+) ([A-Za-z][A-Za-z0-9]*)(?: ([a-z-]+))?", markers[0]
        )
        if match:
            stage, error, category = match.groups()
            if stage in C02_FAILURE_STAGES and (
                    category is None or (stage == "merge-sql" and category in SAFE_MERGE_ERROR_CATEGORIES)):
                error = error if error in SAFE_NATIVE_ERRORS else "OtherError"
                suffix = f" [{category}]" if category else ""
                return f"C02-01 failed at {stage} ({error}){suffix}"
    return "Native failure: no validated C02-01 substep diagnostic"


def silver_pilot(baseline):
    class SilverPilot(baseline.BronzePilot):
        def diagnostic(self, content):
            if self.invocation == "FAIL":
                print(native_failure_line(content), flush=True)
            else:
                super().diagnostic(content)

    return SilverPilot()


class DiskBudget:
    """Check disk use at stage boundaries on the disposable runner."""

    def __init__(self, docker_root):
        require(docker_root.is_absolute(), "Expected an absolute Docker storage path")
        self.initial = {path: shutil.disk_usage(path).used for path in (docker_root, Path("/tmp"))}

    def check(self):
        for path, used in self.initial.items():
            usage = shutil.disk_usage(path)
            require(usage.free >= 4 * GIB and usage.used - used <= 12 * GIB,
                    "Docker or temporary storage exceeded the Silver pilot budget")


def stage_bundle(pilot, baseline, preparation, recipe, minio_context):
    folder = preparation["prepare"](CANDIDATE, SCENARIO, HARNESS)
    require(folder.parent.resolve() == Path("/tmp") and not folder.is_symlink()
            and re.fullmatch(r"native-[0-9a-f]{16}-[a-z0-9_]+", folder.name),
            "Unexpected disposable bundle path")
    pilot.folder = folder
    pilot.project = folder.name.rsplit("-", 1)[0]
    preparation["verify"](folder, HARNESS, candidate=CANDIDATE, scenario=SCENARIO)
    baseline.compile_staged_python(folder)
    pilot.redactions = [line.partition("=")[2] for line in (folder / "test.env").read_text().splitlines()]
    minio_context.mkdir()
    (minio_context / baseline.MINIO_DOCKERFILE).write_bytes(recipe)
    print("PASS: verified exact candidate, scenario, Python grammar and isolated bundle", flush=True)


def run(pilot, baseline, preparation, recipe, work):
    pilot.check_host()
    disk = DiskBudget(Path(pilot.docker("read local Docker storage path", "info",
                                        "--format", "{{.DockerRootDir}}").decode().strip()))
    disk.check()
    minio_context = work / "minio-fixture"
    stage_bundle(pilot, baseline, preparation, recipe, minio_context)
    disk.check()

    pilot.overlay = work / "fixture-overlay.json"
    services = {name: {"cpus": cpu, "mem_limit": memory, "logging": baseline.LOGGING}
                for name, (cpu, memory) in baseline.LIMITS.items()}
    services["fixture-minio"].update(baseline.minio_build(pilot.project, minio_context))
    pilot.overlay.write_text(json.dumps({"services": services}))
    config = ["config", "--no-env-resolution", "--format", "json"]
    base = json.loads(pilot.compose("resolve reviewed Compose model", *config, limited=False))
    effective = json.loads(pilot.compose("resolve bounded Compose model", *config))
    baseline.verify_models(base, effective, pilot.project, minio_context)
    pilot.verify_fresh()

    disk.check()
    pilot.compose("build runner and Hive images serially", "build", "--builder", "default",
                  "fixture-hms", "runner", timeout=900, diagnostics=True)
    disk.check()
    runner_image = pilot.docker("read built runner image ID", "image", "inspect", f"{pilot.project}-runner",
                                "--format", "{{.Id}}").decode().strip()
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", runner_image), "Unexpected runner image ID")
    print(f"Verified runner image {runner_image}", flush=True)
    pilot.compose("build checksum-pinned MinIO fixture", "build", "--builder", "default", "fixture-minio",
                  timeout=180, diagnostics=True)
    disk.check()
    pilot.minio_image_id = pilot.docker(
        "read built MinIO image ID", "image", "inspect",
        baseline.minio_build(pilot.project, minio_context)["image"], "--format", "{{.Id}}",
    ).decode().strip()
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", pilot.minio_image_id), "Unexpected MinIO image ID")

    pilot.started = True
    pilot.compose("start only MinIO and Hive fixtures", "up", "--wait", "--wait-timeout", "90",
                  "--no-build", "--pull", "never", *baseline.FIXTURES, timeout=120, diagnostics=True)
    disk.check()
    pilot.verify_running()
    # Recheck the tag and storage budget at the only write-enabled invocation boundary.
    current_image = pilot.docker("recheck runner image ID", "image", "inspect", f"{pilot.project}-runner",
                                 "--format", "{{.Id}}").decode().strip()
    require(current_image == runner_image, "Runner image changed after its build")
    disk.check()
    pilot.invocation = "FAIL"
    output = pilot.compose("run bounded native Silver C02", "run", "--pull", "never", "--rm", "--no-deps",
                           "-T", "--interactive=false", "-e", "NATIVE_ALLOW_WRITES=1", "runner",
                           timeout=420, diagnostics=True)
    disk.check()
    for line in evidence_lines(output, pilot.redactions):
        print(line, flush=True)
    pilot.invocation = "PASS"


def main():
    environment = os.environ
    head = "unavailable"
    pilot = None
    failure = None
    cleanup_failure = None
    baseline_error = C02Failure
    try:
        event = json.loads(Path(environment["GITHUB_EVENT_PATH"]).read_bytes())
        head = approved_event(environment, event)
        require(sys.implementation.name == "cpython" and sys.version_info[:3] == (3, 10, 22),
                "Expected the pinned CPython 3.10.22 compiler")
        workspace = Path(environment["GITHUB_WORKSPACE"]).resolve()
        harness_dir = workspace / "reviewed-harness"
        require(Path.cwd().resolve() == harness_dir, "Unexpected reviewed harness checkout path")

        scenario_source = checked_blob(workspace / "pilot", head, SCENARIO_PATH)
        prepare_source = checked_blob(harness_dir, HARNESS, "tests/native/prepare.py")
        pilot_dir = workspace / "reviewed-pilot"
        recipe = checked_blob(pilot_dir, REVIEWED_PILOT, "tests/ci/minio-fixture.Dockerfile")
        driver = checked_blob(pilot_dir, REVIEWED_PILOT, "tests/ci/run_native_bronze.py")
        scope = {"__file__": "<reviewed-bronze-pilot>", "__name__": "reviewed_bronze_pilot"}
        exec(compile(driver, "pinned-bronze-pilot.py", "exec"), scope)
        baseline = SimpleNamespace(**scope)
        baseline_error = baseline.PilotFailure
        require(baseline.HARNESS == HARNESS and baseline.ORIGIN == ORIGIN,
                "Reviewed pilot disagrees with the pinned harness or repository")
        git(harness_dir, "fetch", "--no-tags", "--depth=1", ORIGIN, CANDIDATE)
        require(git(harness_dir, "cat-file", "-t", CANDIDATE).strip() == b"commit",
                "Candidate SHA is not a commit")
        preparation = bind_preparation(prepare_source, scenario_source, harness_dir)
        print(f"Review head {head} | harness {HARNESS} | pilot {REVIEWED_PILOT} | candidate {CANDIDATE}",
              flush=True)
        print(f"Reviewed scenario SHA-256 {hashlib.sha256(scenario_source).hexdigest()}", flush=True)
        pilot = silver_pilot(baseline)
        with tempfile.TemporaryDirectory(prefix="silver-c02-pilot-") as work:
            try:
                run(pilot, baseline, preparation, recipe, Path(work))
            finally:
                try:
                    pilot.cleanup()
                except (C02Failure, baseline_error, OSError, ValueError, TypeError, KeyError) as error:
                    cleanup_failure = type(error).__name__
                    print(f"FAIL: scoped cleanup ({cleanup_failure})", flush=True)
    except (C02Failure, baseline_error) as error:
        failure = str(error)
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired, SyntaxError) as error:
        failure = f"Unexpected {type(error).__name__}; see the preceding stage"
    if failure:
        print(f"FAIL: {failure}", flush=True)
    failed = bool(failure or cleanup_failure)
    if "GITHUB_STEP_SUMMARY" in environment:
        with Path(environment["GITHUB_STEP_SUMMARY"]).open("a") as summary:
            summary.write(f"## Bounded Silver C02 validation\n\n- Review head: {head}\n"
                          f"- Harness: {HARNESS}\n- Reviewed pilot: {REVIEWED_PILOT}\n"
                          f"- Candidate: {CANDIDATE}\n"
                          f"- Native invocation: {pilot.invocation if pilot else 'NOT EXECUTED'}\n"
                          f"- Result: {'FAIL' if failed else 'PASS'}\n")
            if failure:
                summary.write(f"- First failure: {failure}\n")
            if cleanup_failure:
                summary.write(f"- Cleanup failure: {cleanup_failure}\n")
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
