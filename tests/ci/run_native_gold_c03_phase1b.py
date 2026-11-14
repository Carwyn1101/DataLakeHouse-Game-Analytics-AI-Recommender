"""Bind reviewed Gold C03 infrastructure to the Owner-approved Phase 1B run."""

import hashlib
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
CANDIDATE = "90fcdce6a78caa7b0c74b7d9708eb92ffce80275"
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
FIRST_MEASUREMENTS = ("first_snapshots", "first_rows", "first_collision", "first_fanout")
MEASUREMENTS = (*ISSUES, *FIRST_MEASUREMENTS, "replay_snapshots", "replay_rows", "replay_collision", "replay_fanout")
BASELINE_MODULE = "silver_to_gold_etl.py"
REJECTION = {
    "outcome": "REJECTED_DUPLICATE_INPUT", "attempts": 2, "target_write_attempts": 0,
    "rows_unchanged": True, "snapshots_unchanged": True, "references_preserved": True, "scratch_cleaned": True,
}
NATIVE_STAGE = "run bounded native Gold C03"
MAX_EVIDENCE_LINE = 6100
MAX_EVIDENCE_BYTES = 32 * 1024
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


def json_object(pairs):
    result = dict(pairs)
    require(len(result) == len(pairs), "Duplicate Phase 1B evidence field")
    return result


def validate_measurements(record, *, partial=False):
    fields = FIRST_MEASUREMENTS if partial else MEASUREMENTS
    require(type(record) is dict and set(record) == set(fields), "Unexpected measurement fields")
    require(all(type(count) is int and 0 <= count <= 1000 for count in record.values()),
            "Unexpected measurement value")
    if not partial:
        require(record["key_drift"] in (0, 1)
                and record["repeated_commit"] == int(record["replay_snapshots"] > 0)
                and all(record[name] == max(record[f"first_{name}"], record[f"replay_{name}"])
                        for name in ("collision", "fanout")), "Contradictory measurement value")


def validate_duplicates(record):
    require(set(record) == set(VARIANTS), "Missing or unexpected duplicate fixture")
    first_failure = None
    for variant in VARIANTS:
        entry = record[variant]
        require(type(entry) is dict and set(entry) == {"candidate", "baseline", "baseline_status"},
                "Unexpected duplicate evidence fields")
        candidate, historical = entry["candidate"], entry["baseline"]
        require(type(candidate) is dict, "Unexpected candidate evidence")
        if candidate.get("outcome") == "FAIL":
            require(set(candidate) == {"outcome", "error"} and type(candidate["error"]) is str
                    and re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,60}", candidate["error"]),
                    "Invalid duplicate diagnostic")
            if candidate["error"] not in SAFE_NATIVE_ERRORS:
                candidate["error"] = "OtherError"
            if first_failure is None:
                first_failure = f"{variant} ({candidate['error']})"
        else:
            require(candidate.keys() == REJECTION.keys()
                    and all(type(candidate[key]) is type(expected) and candidate[key] == expected
                            for key, expected in REJECTION.items()), "Missing safe duplicate-input rejection")
        baseline_status = entry["baseline_status"]
        require(baseline_status in ("MEASURED", "EXECUTION_ERROR", "INCONCLUSIVE"),
                "Unexpected baseline evidence status")
        if baseline_status == "EXECUTION_ERROR":
            if historical is not None:
                validate_measurements(historical, partial=True)
        else:
            validate_measurements(historical)
            if baseline_status == "MEASURED":
                require(historical["first_snapshots"] > 0 and historical["first_rows"] > 0
                        and historical["replay_rows"] > 0, "Measured baseline did not complete")
    return first_failure


def checked_case(line):
    match = re.fullmatch(r"(C03B-[A-Z-]+) (PASS|FAIL) (\{[^\r\n]{1,5998}\})",
                         line)
    require(match, "Malformed Phase 1B case evidence")
    case, status, payload = match.groups()
    require(case in CASE_IDS, "Unexpected Phase 1B case ID")
    try:
        record = json.loads(payload, object_pairs_hook=json_object)
    except (ValueError, RecursionError):
        raise GateFailure("Invalid Phase 1B evidence JSON") from None
    if case in CASE_IDS[:3]:
        require(record.keys() == CANONICAL_CHECKS.keys()
                and all(type(record[key]) is kind and (kind is bool or 0 < record[key] <= 1000)
                        for key, kind in CANONICAL_CHECKS.items()), "Unexpected Gate A evidence fields or values")
        require(status == "PASS" and record["readback_verified"] and record["scratch_cleaned"]
                and record["successful_snapshots"] == 3, "Incomplete canonical evidence")
    else:
        rejection_failure = validate_duplicates(record)
        require(status == ("FAIL" if rejection_failure else "PASS"), "Contradictory duplicate outcome")
    return case, status, json.dumps(record, sort_keys=True, separators=(",", ":"))


def evidence_records(content, redactions, *, successful):
    """Rebuild only validated case records and bounded exception types."""
    require(type(content) is str and len(content) <= MAX_EVIDENCE_BYTES, "Unexpected native output format")
    records = {}
    completion = 0
    order = ("C03B-SETUP", *CASE_IDS)
    for line in content.splitlines():
        if line == f"Scenario completed: {SCENARIO}":
            completion += 1
            continue
        if not line.startswith("C03B-") or line in {f"{case} START" for case in CASE_IDS}:
            continue
        if " DIAGNOSTIC " in line:
            match = re.fullmatch(r"(C03B-[A-Z-]+) DIAGNOSTIC ([A-Za-z][A-Za-z0-9]{0,60})", line)
            require(match, "Invalid native diagnostic")
            case, error = match.groups()
            status, payload = "failed", f"({error if error in SAFE_NATIVE_ERRORS else 'OtherError'})"
        else:
            case, status, payload = checked_case(line)
        require(case in order and case not in records and completion == 0,
                "Duplicate or contradictory Phase 1B case evidence")
        records[case] = status, payload
    require(tuple(records) == tuple(case for case in order if case in records),
            "Out-of-order Phase 1B case evidence")
    if successful:
        require(tuple(records) == CASE_IDS and completion == 1
                and all(status == "PASS" for status, _ in records.values()),
                "Native completion requires all Gate A and Gate B evidence")
    else:
        require(completion == 0, "Native failed after a contradictory completion marker")
    lines = [f"{case} {status} {payload}" for case, (status, payload) in records.items()]
    require(not any(secret and secret in line for secret in redactions for line in lines),
            "Phase 1B evidence included fixture credentials")
    return lines


def collect_evidence(output):
    """Scan stdout from its beginning without retaining arbitrary native output."""
    output.seek(0)
    evidence = bytearray()
    start_of_line = True
    while chunk := output.readline(MAX_EVIDENCE_LINE + 1):
        if start_of_line and chunk.startswith((b"C03B-", b"Scenario completed:")):
            require(len(chunk) <= MAX_EVIDENCE_LINE, "Native evidence line exceeded its budget")
            require(len(evidence) + len(chunk) <= MAX_EVIDENCE_BYTES, "Native evidence exceeded its budget")
            evidence.extend(chunk)
        start_of_line = chunk.endswith(b"\n")
    return bytes(evidence)


def case_outcomes(lines):
    statuses = dict(line.split(" ", 2)[:2] for line in lines)
    return {case: {"PASS": "PASS", "failed": "FAIL", "FAIL": "FAIL"}.get(statuses.get(case), "NOT OBSERVED") for case in CASE_IDS}


def gate_states(lines):
    cases = case_outcomes(lines)
    return tuple("FAIL" if "FAIL" in results else "PASS" if set(results) == {"PASS"} else "INCOMPLETE"
                 for results in ([cases[case] for case in CASE_IDS[:3]], [cases[case] for case in CASE_IDS[3:]]))


def baseline_state(lines):
    for line in lines:
        case, status, encoded = line.split(" ", 2)
        if case in CASE_IDS[3:] and status in ("PASS", "FAIL"):
            if any(entry["baseline_status"] != "MEASURED" for entry in json.loads(encoded).values()):
                return "UNVERIFIED"
    complete = {line.split(" ", 2)[0] for line in lines if line.split(" ", 2)[1] in ("PASS", "FAIL")}
    return "CHARACTERIZED" if complete.issuperset(CASE_IDS[3:]) else "UNVERIFIED"


def evidence_lines(output, redactions):
    return evidence_records(output.decode("ascii", errors="replace"), redactions, successful=True)


def gold_pilot(baseline):
    class Phase1BPilot(baseline.BronzePilot):
        gate_a = "NOT EXECUTED"
        gate_b = "NOT EXECUTED"
        historical = "NOT EXECUTED"
        cases = None
        first_failure = None

        def record(self, lines):
            self.gate_a, self.gate_b = gate_states(lines)
            self.historical = baseline_state(lines)
            self.cases = case_outcomes(lines)
            self.first_failure = None
            for line in lines:
                case, status, payload = line.split(" ", 2)
                if status == "failed":
                    self.first_failure = line
                elif status == "FAIL":
                    finding = validate_duplicates(json.loads(payload))
                    self.first_failure = f"{case} {finding}"
                if self.first_failure:
                    break

        def accept(self, output, redactions):
            lines = evidence_lines(output, redactions)
            self.record(lines)
            return lines

        def report_failure(self, output):
            try:
                lines = evidence_records(output.decode("ascii", errors="replace"), self.redactions, successful=False)
            except (GateFailure, ValueError, TypeError):
                lines = []
            self.record(lines)
            for line in lines or ["Native Gold Phase 1B failure: no validated evidence"]:
                print(line, flush=True)

        def command(self, stage, args, *, timeout=30, source=None, diagnostics=False):
            if stage != NATIVE_STAGE:
                return super().command(stage, args, timeout=timeout, source=source, diagnostics=diagnostics)
            print(f"Stage: {stage}", flush=True)
            # PR #15's shared command reports a 16 KiB tail. This invocation
            # instead retains allowlisted records across the entire bounded run.
            with tempfile.TemporaryFile() as output:
                try:
                    result = subprocess.run(args, input=source, stdout=output, stderr=subprocess.DEVNULL,
                                            timeout=timeout, check=False)
                except subprocess.TimeoutExpired:
                    self.report_failure(collect_evidence(output))
                    raise baseline.PilotFailure(f"{stage} timed out after {timeout}s") from None
                evidence = collect_evidence(output)
                if result.returncode:
                    self.report_failure(evidence)
                    raise baseline.PilotFailure(f"{stage} failed (exit {result.returncode})")
                return evidence

    return Phase1BPilot()


def bind_baseline(preparation):
    """Add exact sanitized historical bytes to a build input already covered by verify()."""
    baseline = preparation["sanitized"](BASELINE_MODULE, preparation["git_blob"](BASE, BASELINE_MODULE))
    digest = hashlib.sha256(baseline).hexdigest()
    expected_files = preparation["expected_files"]

    def include_baseline(*args):
        files = expected_files(*args)
        manifest = json.loads(files["runner/run.json"])
        manifest.update(baseline_revision=BASE, baseline_sha256=digest)
        files["runner/run.json"] = preparation["json_bytes"](manifest)
        files["runner/scenarios.py"] += b"\nPINNED_BASELINE_SOURCE = " + repr(baseline).encode("ascii") + b"\n"
        return files

    preparation["expected_files"] = include_baseline
    return preparation


def bind_gold(source, accept=evidence_lines):
    """Reuse PR #19's verified stage, topology, budgets, write boundary and cleanup."""
    scope = {"__file__": "<reviewed-gold-pilot>", "__name__": "reviewed_gold_pilot"}
    exec(compile(source, "pinned-gold-pilot.py", "exec"), scope)
    require(scope["HARNESS"] == HARNESS and scope["REVIEWED_PILOT"] == REVIEWED_PILOT
            and scope["ORIGIN"] == ORIGIN, "Reviewed Gold pilot pins differ")
    scope.update(CANDIDATE=CANDIDATE, SCENARIO=SCENARIO, evidence_lines=accept)
    return SimpleNamespace(**scope)


def main():
    environment = os.environ
    head = "unavailable"
    pilot = None
    result = "FAIL"
    cleanup = "NOT EXECUTED"
    first_failure = None
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
        for revision in (REVIEWED_GOLD, CANDIDATE, BASE):
            git(harness_dir, "fetch", "--no-tags", "--depth=1", ORIGIN, revision)
            require(git(harness_dir, "cat-file", "-t", revision).strip() == b"commit", "Expected a pinned commit")
        pilot = gold_pilot(baseline)
        gold = bind_gold(git(harness_dir, "show", f"{REVIEWED_GOLD}:{REVIEWED_GOLD_DRIVER}"), pilot.accept)
        preparation = bind_baseline(gold.bind_preparation(prepare, scenario, harness_dir))
        print(f"Validation HEAD {head} | base {BASE} | candidate {CANDIDATE}\n"
              f"Harness {HARNESS} | isolation pilot {REVIEWED_PILOT} | Gold pilot {REVIEWED_GOLD}", flush=True)
        with tempfile.TemporaryDirectory(prefix="gold-c03-phase-1b-pilot-") as work:
            try:
                gold.run(pilot, baseline, preparation, recipe, Path(work))
                result = "PASS" if pilot.historical == "CHARACTERIZED" else "HOLD"
            except (gold.C03Failure, baseline.PilotFailure) as error:
                # These errors contain controlled stage labels, not Spark or Docker output.
                first_failure = str(error)
                print(f"FAIL: {error}", flush=True)
            finally:
                try:
                    pilot.cleanup()
                    cleanup = "PASS" if pilot.folder else "NOT EXECUTED"
                except (gold.C03Failure, baseline.PilotFailure, OSError, ValueError, TypeError, KeyError) as error:
                    cleanup = "FAIL"
                    cleanup_error = f"scoped cleanup ({type(error).__name__})"
                    first_failure = first_failure or cleanup_error
                    print(f"FAIL: {cleanup_error}", flush=True)
    except GateFailure as error:
        first_failure = str(error)
        print(f"FAIL: {first_failure}", flush=True)
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired, SyntaxError) as error:
        first_failure = f"Unexpected {type(error).__name__}; see the preceding stage"
        print(f"FAIL: {first_failure}", flush=True)
    if cleanup == "FAIL":
        result = "FAIL"
    if pilot and pilot.invocation == "FAIL" and pilot.gate_a == "NOT EXECUTED":
        pilot.record([])
    gate_a = pilot.gate_a if pilot else "NOT EXECUTED"
    gate_b = pilot.gate_b if pilot else "NOT EXECUTED"
    historical = pilot.historical if pilot else "NOT EXECUTED"
    if pilot and pilot.first_failure:
        first_failure = pilot.first_failure
    print(f"Gate A atomicity: {gate_a} | Gate B safe rejection: {gate_b} | Baseline: {historical} | "
          f"Cleanup: {cleanup} | Result: {result}", flush=True)
    if "GITHUB_STEP_SUMMARY" in environment:
        with Path(environment["GITHUB_STEP_SUMMARY"]).open("a") as summary:
            summary.write(f"## Bounded Gold C03 Phase 1B validation\n\n- Validation HEAD: {head}\n"
                          f"- Candidate: {CANDIDATE}\n- Harness: {HARNESS}\n- Isolation pilot: {REVIEWED_PILOT}\n"
                          f"- Gold pilot: {REVIEWED_GOLD}\n"
                          f"- Native invocation: {pilot.invocation if pilot else 'NOT EXECUTED'}\n"
                          f"- Gate A atomicity: {gate_a}\n- Gate B safe rejection: {gate_b}\n"
                          f"- Baseline: {historical}\n- Cleanup: {cleanup}\n- Result: {result}\n"
                          "- Merge readiness: HOLD pending independent review\n")
            if pilot and pilot.cases:
                for case, status in pilot.cases.items():
                    summary.write(f"- {case}: {status}\n")
            if first_failure:
                summary.write(f"- First failure: {first_failure}\n")
    return int(result != "PASS")


if __name__ == "__main__":
    sys.exit(main())
