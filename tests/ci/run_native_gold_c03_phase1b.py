"""Bind reviewed Gold C03 infrastructure to the Owner-approved Phase 1B run."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from types import SimpleNamespace

REPOSITORY = "Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender"
ORIGIN = f"https://github.com/{REPOSITORY}.git"
OWNER = REPOSITORY.split("/")[0]
BASE = "a815d6bd9aaa09faf3b8e65ad4ec643abfed062d"
BRANCH = "codex/gold-c03-phase-1b-native-validation"
APPROVAL = "native-gold-c03-phase-1b-approved"
HARNESS = "adc2d56803b9493c2e366f9a31cc3b85ad4e9816"
REVIEWED_PILOT = "ed0937882e505fac6bb658d00cfc858d73fa6332"
REVIEWED_GOLD = "42e45f441c97522bdd6afbbc55cd2a9f5dc27bad"
REVIEWED_GOLD_DRIVER = "tests/ci/run_native_gold_c03.py"
CANDIDATE = "d06e1d69f82465bba9a8a72b7edda62bb2e23adf"
SCENARIO = "gold-c03-phase-1b"
SCENARIO_PATH = "tests/ci/gold_c03_phase1b_scenarios.py"
CASE_IDS = (
    "C03B-DETAIL", "C03B-REVIEW", "C03B-POPULARITY",
    "C03B-REVIEW-DUPS", "C03B-POPULARITY-DUPS",
)
CANONICAL_CHECKS = {
    "rows": int,
    "retained_fact_references": int,
    "successful_snapshots": int,
    "readback_verified": bool,
    "scratch_cleaned": bool,
}
VARIANTS = ("exact_existing", "exact_new", "conflicting_existing", "conflicting_new")
ISSUES = ("collision", "repeated_commit", "fanout", "key_drift")
DUPLICATE_CHECKS = {f"{variant}_{issue}": int for variant in VARIANTS for issue in ISSUES}
SAFE_NATIVE_ERRORS = {
    "AnalysisException", "AssertionError", "FileNotFoundError", "IllegalArgumentException",
    "ImportError", "ModuleNotFoundError", "OSError", "ParseException", "PermissionError",
    "Py4JJavaError", "RuntimeError", "SparkRuntimeException", "TimeoutError", "TypeError", "ValueError",
}


class GateFailure(RuntimeError):
    """A gate, provenance, or evidence check failed."""


def require(condition, message):
    if not condition:
        raise GateFailure(message)


def approved_event(environment, event):
    """Accept only the Owner's first label event on the exact reviewed branch and base."""
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
        and pull["base"]["sha"] == BASE
        and pull["base"]["repo"]["full_name"] == REPOSITORY
        and pull["head"]["repo"]["full_name"] == REPOSITORY
        and pull["head"]["ref"] == BRANCH
        and pull["head"]["sha"] == head,
        "Expected the first Owner-approved Phase 1B event on the pinned base and validation HEAD",
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


def checked_case(line):
    match = re.fullmatch(r"(C03B-[A-Z-]+) (PASS|FAIL) (\{[^}\r\n]{1,1000}\})", line)
    require(match, "Malformed Phase 1B case evidence")
    case, status, payload = match.groups()
    require(case in CASE_IDS, "Unexpected Phase 1B case ID")
    fields = CANONICAL_CHECKS if case in CASE_IDS[:3] else DUPLICATE_CHECKS
    items = json.loads(payload, object_pairs_hook=list)
    record = dict(items)
    require(len(items) == len(record) and record.keys() == fields.keys(),
            "Unexpected Phase 1B evidence fields")
    require(all(type(record[key]) is kind and (kind is bool or 0 <= record[key] <= 1000)
                for key, kind in fields.items()), "Unexpected Phase 1B evidence value")
    if case in CASE_IDS[:3]:
        require(status == "PASS" and record["readback_verified"] and record["scratch_cleaned"]
                and record["rows"] > 0 and record["retained_fact_references"] > 0
                and record["successful_snapshots"] == 3, "Incomplete canonical evidence")
    else:
        require((status == "FAIL") == any(record.values()), "Duplicate result contradicts issue counters")
    return case, status, json.dumps(record, sort_keys=True, separators=(",", ":"))


def evidence_records(content, redactions, *, successful):
    """Rebuild only typed, allowlisted case or diagnostic records from captured logs."""
    require(isinstance(content, str), "Unexpected native output format")
    records = {}
    diagnostic = None
    completion = 0
    for line in content.splitlines():
        if line == f"Scenario completed: {SCENARIO}":
            completion += 1
        if not line.startswith("C03B-"):
            continue
        if line in {f"{case} START" for case in CASE_IDS}:
            continue
        if " DIAGNOSTIC " in line:
            match = re.fullmatch(r"(C03B-[A-Z-]+) DIAGNOSTIC ([A-Za-z][A-Za-z0-9]{0,60})", line)
            require(match and diagnostic is None, "Unexpected native diagnostic")
            case, error = match.groups()
            require(case in (*CASE_IDS, "C03B-SETUP"), "Unexpected native diagnostic case")
            diagnostic = f"{case} failed ({error if error in SAFE_NATIVE_ERRORS else 'OtherError'})"
            continue
        case, status, encoded = checked_case(line)
        require(case not in records, "Duplicate Phase 1B case evidence")
        records[case] = (status, encoded)
    require(tuple(records) == tuple(case for case in CASE_IDS if case in records),
            "Out-of-order Phase 1B case evidence")
    if successful:
        require(tuple(records) == CASE_IDS and completion == 1 and diagnostic is None
                and all(status == "PASS" for status, _ in records.values()),
                "Native success requires five clean cases and scenario completion")
    else:
        require(completion == 0, "Native failed after a contradictory completion marker")
    lines = [f"{case} {status} {encoded}" for case, (status, encoded) in records.items()]
    if diagnostic:
        lines.append(diagnostic)
    require(not any(secret and secret in line for secret in redactions for line in lines),
            "Phase 1B evidence included fixture credentials")
    return lines


def evidence_lines(output, redactions):
    return evidence_records(output.decode("ascii", errors="replace"), redactions, successful=True)


def gold_pilot(baseline):
    class Phase1BPilot(baseline.BronzePilot):
        def diagnostic(self, content):
            if self.invocation != "FAIL":
                super().diagnostic(content)
                return
            try:
                lines = evidence_records(content, self.redactions, successful=False)
            except (GateFailure, ValueError, TypeError, OverflowError):
                lines = []
            for line in lines or ["Native Gold Phase 1B failure: no validated evidence"]:
                print(line, flush=True)

    return Phase1BPilot()


def bind_gold(source):
    """Reuse PR #19's verified stage, topology, budgets, write boundary and cleanup."""
    scope = {"__file__": "<reviewed-gold-pilot>", "__name__": "reviewed_gold_pilot"}
    exec(compile(source, "pinned-gold-pilot.py", "exec"), scope)
    require(scope["HARNESS"] == HARNESS and scope["REVIEWED_PILOT"] == REVIEWED_PILOT
            and scope["ORIGIN"] == ORIGIN, "Reviewed Gold pilot pins differ")
    scope.update(CANDIDATE=CANDIDATE, SCENARIO=SCENARIO, evidence_lines=evidence_lines)
    return SimpleNamespace(**scope)


def main():
    environment = os.environ
    head = "unavailable"
    pilot = None
    result = "FAIL"
    cleanup_failed = False
    try:
        event = json.loads(Path(environment["GITHUB_EVENT_PATH"]).read_bytes())
        head = approved_event(environment, event)
        require(sys.implementation.name == "cpython" and sys.version_info[:3] == (3, 10, 22),
                "Expected the pinned CPython 3.10.22 compiler")
        workspace = Path(environment["GITHUB_WORKSPACE"]).resolve()
        harness_dir = workspace / "reviewed-harness"
        require(Path.cwd().resolve() == harness_dir, "Unexpected reviewed harness checkout path")

        scenario = checked_blob(workspace / "pilot", head, SCENARIO_PATH)
        prepare = checked_blob(harness_dir, HARNESS, "tests/native/prepare.py")
        pilot_dir = workspace / "reviewed-pilot"
        recipe = checked_blob(pilot_dir, REVIEWED_PILOT, "tests/ci/minio-fixture.Dockerfile")
        driver = checked_blob(pilot_dir, REVIEWED_PILOT, "tests/ci/run_native_bronze.py")
        baseline_scope = {"__file__": "<reviewed-bronze-pilot>", "__name__": "reviewed_bronze_pilot"}
        exec(compile(driver, "pinned-bronze-pilot.py", "exec"), baseline_scope)
        baseline = SimpleNamespace(**baseline_scope)
        require(baseline.HARNESS == HARNESS and baseline.ORIGIN == ORIGIN, "Reviewed isolation pins differ")
        for revision in (REVIEWED_GOLD, CANDIDATE):
            git(harness_dir, "fetch", "--no-tags", "--depth=1", ORIGIN, revision)
            require(git(harness_dir, "cat-file", "-t", revision).strip() == b"commit", "Expected a pinned commit")
        gold = bind_gold(git(harness_dir, "show", f"{REVIEWED_GOLD}:{REVIEWED_GOLD_DRIVER}"))
        preparation = gold.bind_preparation(prepare, scenario, harness_dir)
        print(f"Validation HEAD {head} | base {BASE} | candidate {CANDIDATE}\n"
              f"Harness {HARNESS} | isolation pilot {REVIEWED_PILOT} | Gold pilot {REVIEWED_GOLD}", flush=True)
        pilot = gold_pilot(baseline)
        with tempfile.TemporaryDirectory(prefix="gold-c03-phase-1b-pilot-") as work:
            try:
                gold.run(pilot, baseline, preparation, recipe, Path(work))
                result = "PASS"
            except (gold.C03Failure, baseline.PilotFailure) as error:
                # These errors contain controlled stage labels, not Spark or Docker output.
                print(f"FAIL: {error}", flush=True)
            finally:
                try:
                    pilot.cleanup()
                except (gold.C03Failure, baseline.PilotFailure, OSError, ValueError, TypeError, KeyError) as error:
                    cleanup_failed = True
                    print(f"FAIL: scoped cleanup ({type(error).__name__})", flush=True)
    except GateFailure as error:
        print(f"FAIL: {error}", flush=True)
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired, SyntaxError) as error:
        print(f"FAIL: unexpected {type(error).__name__}; see the preceding stage", flush=True)
    if cleanup_failed:
        result = "FAIL"
    if "GITHUB_STEP_SUMMARY" in environment:
        with Path(environment["GITHUB_STEP_SUMMARY"]).open("a") as summary:
            summary.write(f"## Bounded Gold C03 Phase 1B validation\n\n- Validation HEAD: {head}\n"
                          f"- Candidate: {CANDIDATE}\n- Harness: {HARNESS}\n- Isolation pilot: {REVIEWED_PILOT}\n"
                          f"- Gold pilot: {REVIEWED_GOLD}\n"
                          f"- Native invocation: {pilot.invocation if pilot else 'NOT EXECUTED'}\n"
                          f"- Result: {result}\n")
    return int(result != "PASS")


if __name__ == "__main__":
    sys.exit(main())
