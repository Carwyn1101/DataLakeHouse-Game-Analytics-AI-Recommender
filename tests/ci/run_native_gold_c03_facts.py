"""Run the pinned Gold fact validation only after the Owner's exact-HEAD approval."""

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
BASE = "d9fb3fd06a1e04ab28f7f70bcb288d7eaba1d533"
CANDIDATE = "f96212899e9f188524bcea6d2cd1a019f5a1aca6"
HARNESS = "adc2d56803b9493c2e366f9a31cc3b85ad4e9816"
ISOLATION = "ed0937882e505fac6bb658d00cfc858d73fa6332"
GOLD_PILOT = "42e45f441c97522bdd6afbbc55cd2a9f5dc27bad"
EVIDENCE_PILOT = "213a537e60d30a9ab8d2e3b7157d2cb294f9f9de"
BRANCH = "codex/gold-c03-phase-1c-a-native-validation"
APPROVAL = "native-gold-c03-phase-1c-a-approved"
SCENARIO = "gold-c03-phase-1c-a"
SCENARIO_PATH = "tests/ci/gold_c03_fact_scenarios.py"
CASE_IDS = tuple(f"C03F-{gate}-{fact}" for gate in "ABC" for fact in ("REVIEW", "POPULARITY"))
LEGACY_NULL = "LEGACY_BEHAVIOR_PRESERVED"
COMMON_FIELDS = ("schema_verified", "scratch_cleaned", "independent_readback", "parquet_verified")
GATE_FIELDS = {
    "A": ("initial_rows", "refreshed_rows", "duplicate_rows", "fanout_rows", "nullable_fk_rows",
          "refresh_snapshots", "unchanged_snapshots", "empty_snapshots"),
    "B": ("reference_rows", "retry_rows", "lost_ack_rows", "staging_failure_snapshots",
          "precommit_snapshots", "retry_snapshots", "lost_ack_snapshots", "replay_snapshots"),
    "C": ("initial_null_rows", "first_null_rows", "replay_null_rows", "null_payload_rows",
          "refresh_snapshots", "replay_snapshots", "legacy_behavior"),
}
MAX_LINE_BYTES = 6100
MAX_EVIDENCE_BYTES = 32 * 1024
SAFE_ERRORS = frozenset({
    "AnalysisException", "AssertionError", "FileNotFoundError", "IllegalArgumentException",
    "ImportError", "KeyError", "ModuleNotFoundError", "OSError", "ParseException", "PermissionError",
    "Py4JJavaError", "RuntimeError", "SparkRuntimeException", "TimeoutError", "TypeError", "ValueError",
})


class FactValidationError(RuntimeError):
    """A reviewed validation boundary or evidence check failed."""


def require(condition, message):
    if not condition:
        raise FactValidationError(message)


def approved_event(environment, event):
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
        "Expected first Owner label event for the exact ready-for-review validation HEAD and base",
    )
    return head


def git(repository, *args):
    result = subprocess.run(
        ["git", "--no-replace-objects", "-c", "credential.helper=", "-C", str(repository), *args],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False, timeout=60,
    )
    require(result.returncode == 0, "Pinned Git input unavailable")
    return result.stdout


def checked_blob(repository, revision, path):
    require(git(repository, "rev-parse", "HEAD").strip() == revision.encode(), "Unexpected checkout revision")
    return git(repository, "show", f"{revision}:{path}")


def pinned_revision(repository, revision):
    git(repository, "fetch", "--no-tags", "--depth=1", ORIGIN, revision)
    require(git(repository, "cat-file", "-t", revision).strip() == b"commit", "Expected pinned commit")


def load_blob(source, name):
    scope = {"__name__": name, "__file__": f"<{name}>"}
    exec(compile(source, name, "exec"), scope)
    return scope


def json_object(pairs):
    record = dict(pairs)
    require(len(record) == len(pairs), "Duplicate evidence field")
    return record


def validate_values(gate, record):
    numbers = set(GATE_FIELDS[gate]) - {"legacy_behavior"}
    require(type(record) is dict and record.keys() == set(COMMON_FIELDS) | set(GATE_FIELDS[gate]),
            "Missing or unexpected case evidence fields")
    require(all(record[name] is True for name in COMMON_FIELDS)
            and all(type(record[name]) is int and 0 <= record[name] <= 1000 for name in numbers),
            "Unexpected case evidence value")
    if gate == "A":
        require(all(record[name] > 0 for name in ("initial_rows", "refreshed_rows", "duplicate_rows",
                                                   "fanout_rows", "nullable_fk_rows"))
                and record["refresh_snapshots"] == record["unchanged_snapshots"] == 1
                and record["empty_snapshots"] == 0, "Incomplete Gate A evidence")
    elif gate == "B":
        require(record["reference_rows"] == record["retry_rows"] == record["lost_ack_rows"] > 0
                and record["staging_failure_snapshots"] == record["precommit_snapshots"] == 0
                and record["retry_snapshots"] == record["lost_ack_snapshots"] == record["replay_snapshots"] == 1,
                "Incomplete Gate B evidence")
    else:
        require(record["legacy_behavior"] == LEGACY_NULL and record["initial_null_rows"] > 0
                and record["null_payload_rows"] > 0
                and record["first_null_rows"] == record["initial_null_rows"] + record["null_payload_rows"]
                and record["replay_null_rows"] == record["first_null_rows"] + record["null_payload_rows"]
                and record["refresh_snapshots"] == record["replay_snapshots"] == 1,
                "Incomplete Gate C evidence")


def evidence_records(content, redactions, *, successful):
    """Rebuild a bounded allowlist of observations; reject malformed or contradictory output."""
    require(type(content) is str and len(content) <= MAX_EVIDENCE_BYTES, "Unexpected native evidence size")
    records = {}
    completion = False
    for line in content.splitlines():
        if line.startswith("Scenario completed:"):
            require(line == f"Scenario completed: {SCENARIO}" and not completion
                    and tuple(records) == CASE_IDS, "Unexpected native completion marker")
            completion = True
            continue
        if not line.startswith("C03F-") or line in {f"{case} START" for case in CASE_IDS}:
            continue
        require(not completion, "Native case evidence follows completion")
        diagnostic = re.fullmatch(r"(C03F-[A-Z-]+) DIAGNOSTIC ([A-Za-z][A-Za-z0-9]{0,60})", line)
        if diagnostic:
            case, kind = diagnostic.groups()
            expected = CASE_IDS[len(records)] if len(records) < len(CASE_IDS) else None
            require(case == expected or (case == "C03F-SETUP" and not records)
                    or (case == "C03F-TEARDOWN" and tuple(records) == CASE_IDS),
                    "Unexpected native diagnostic order")
            records[case] = f"{case} FAIL ({kind if kind in SAFE_ERRORS else 'OtherError'})"
            continue
        match = re.fullmatch(r"(C03F-[A-Z-]+) PASS (\{.{1,5998}\})", line)
        require(match, "Malformed native case evidence")
        case, value = match.groups()
        require(len(records) < len(CASE_IDS) and case == CASE_IDS[len(records)]
                and not any(" FAIL " in record for record in records.values()), "Unexpected native case order")
        try:
            record = json.loads(value, object_pairs_hook=json_object)
        except (ValueError, RecursionError):
            raise FactValidationError("Malformed native case JSON") from None
        validate_values(case.split("-")[1], record)
        records[case] = f"{case} PASS " + json.dumps(record, sort_keys=True, separators=(",", ":"))
    lines = list(records.values())
    if successful:
        require(tuple(records) == CASE_IDS and completion and all(" PASS " in line for line in lines),
                "Successful native process lacks complete fact evidence")
    else:
        require(not completion, "Failed native process claimed completion")
    require(not any(secret and secret in line for secret in redactions for line in lines),
            "Native evidence included fixture credentials")
    return lines


def collect_evidence(output):
    """Select reviewed fact markers across the entire native stdout spool."""
    output.seek(0)
    evidence = bytearray()
    start = True
    while chunk := output.readline(MAX_LINE_BYTES + 1):
        if start and chunk.startswith((b"C03F-", b"Scenario completed:")):
            require(len(chunk) <= MAX_LINE_BYTES, "Native evidence line exceeded its budget")
            require(len(evidence) + len(chunk) <= MAX_EVIDENCE_BYTES, "Native evidence exceeded its budget")
            evidence.extend(chunk)
        start = chunk.endswith(b"\n")
    return bytes(evidence)


def evidence_lines(output, redactions):
    return evidence_records(output.decode("ascii", errors="replace"), redactions, successful=True)


def bind_capture(reference_source, isolation):
    """Reuse the reviewed full-span native command with fact-only marker parsing."""
    reference = load_blob(reference_source, "reviewed_gold_evidence")
    require((reference["HARNESS"], reference["REVIEWED_PILOT"], reference["REVIEWED_GOLD"], reference["ORIGIN"])
            == (HARNESS, ISOLATION, GOLD_PILOT, ORIGIN), "Reviewed evidence pins differ")
    reference.update(collect_evidence=collect_evidence, evidence_lines=evidence_lines)
    adapter = type(reference["gold_pilot"](isolation))

    class FactPilot(adapter):
        isolation_result = "NOT OBSERVED"
        cases = None
        first_failure = None

        def verify_running(self):
            self.isolation_result = "FAIL"
            super().verify_running()
            self.isolation_result = "PASS"

        def record(self, lines):
            statuses = dict(line.split(" ", 2)[:2] for line in lines)
            self.cases = {case: statuses.get(case, "NOT OBSERVED") for case in CASE_IDS}
            self.first_failure = next((line for line in lines if " FAIL " in line), None)

        def report_failure(self, output):
            try:
                lines = evidence_records(output.decode("ascii", errors="replace"), self.redactions, successful=False)
            except FactValidationError:
                lines = []
            self.record(lines)
            for line in lines or ["Native fact invocation failed without validated evidence"]:
                print(line, flush=True)

    return FactPilot()


def bind_gold(source, accept):
    scope = load_blob(source, "reviewed_gold_pilot")
    require((scope["HARNESS"], scope["REVIEWED_PILOT"], scope["ORIGIN"]) == (HARNESS, ISOLATION, ORIGIN),
            "Reviewed Gold pilot pins differ")
    scope.update(CANDIDATE=CANDIDATE, SCENARIO=SCENARIO, evidence_lines=accept)
    return SimpleNamespace(**scope)


def results(pilot, cleanup):
    cases = pilot.cases if pilot and pilot.cases else dict.fromkeys(CASE_IDS, "NOT OBSERVED")
    gates = {}
    for gate in "ABC":
        observed = {cases[case] for case in CASE_IDS if case.startswith(f"C03F-{gate}-")}
        gates[gate] = "FAIL" if "FAIL" in observed else "PASS" if observed == {"PASS"} else "NOT OBSERVED"
    invocation = pilot.invocation if pilot else "NOT EXECUTED"
    compatibility = "FAIL" if invocation == "FAIL" else "PASS" if invocation == "PASS" else "NOT OBSERVED"
    isolation = pilot.isolation_result if pilot else "NOT OBSERVED"
    overall = ("PASS" if all(state == "PASS" for state in (*gates.values(), compatibility, isolation, cleanup))
               else "FAIL")
    return gates, compatibility, isolation, overall, cases


def main():
    environment = os.environ
    head, cleanup, failure = "unavailable", "NOT OBSERVED", None
    pilot = None
    try:
        event = json.loads(Path(environment["GITHUB_EVENT_PATH"]).read_bytes())
        head = approved_event(environment, event)
        require(sys.implementation.name == "cpython" and sys.version_info[:3] == (3, 10, 22),
                "Expected pinned CPython 3.10.22")
        workspace = Path(environment["GITHUB_WORKSPACE"]).resolve()
        harness_dir, isolation_dir = workspace / "reviewed-harness", workspace / "reviewed-pilot"
        require(Path.cwd().resolve() == harness_dir, "Unexpected harness working directory")

        scenario = checked_blob(workspace / "pilot", head, SCENARIO_PATH)
        prepare = checked_blob(harness_dir, HARNESS, "tests/native/prepare.py")
        recipe = checked_blob(isolation_dir, ISOLATION, "tests/ci/minio-fixture.Dockerfile")
        source = checked_blob(isolation_dir, ISOLATION, "tests/ci/run_native_bronze.py")
        isolation = SimpleNamespace(**load_blob(source, "reviewed_isolation"))
        require((isolation.HARNESS, isolation.ORIGIN) == (HARNESS, ORIGIN), "Reviewed isolation pins differ")
        for revision in (GOLD_PILOT, EVIDENCE_PILOT, CANDIDATE):
            pinned_revision(harness_dir, revision)
        metadata = git(harness_dir, "cat-file", "-p", CANDIDATE).split(b"\n\n", 1)[0].splitlines()
        require([line for line in metadata if line.startswith(b"parent ")] == [b"parent " + BASE.encode()],
                "Candidate is not based on the approved main")
        pilot = bind_capture(git(harness_dir, "show", f"{EVIDENCE_PILOT}:tests/ci/run_native_gold_c03_phase1b.py"),
                             isolation)
        gold = bind_gold(git(harness_dir, "show", f"{GOLD_PILOT}:tests/ci/run_native_gold_c03.py"), pilot.accept)
        preparation = gold.bind_preparation(prepare, scenario, harness_dir)
        print(f"Validation HEAD {head} | base {BASE} | candidate {CANDIDATE}\n"
              f"Harness {HARNESS} | isolation {ISOLATION} | Gold {GOLD_PILOT} | evidence {EVIDENCE_PILOT}", flush=True)
        with tempfile.TemporaryDirectory(prefix="gold-c03-facts-pilot-") as work:
            try:
                gold.run(pilot, isolation, preparation, recipe, Path(work))
            except (gold.C03Failure, isolation.PilotFailure, FactValidationError) as error:
                failure = str(error)
            finally:
                try:
                    pilot.cleanup()
                    cleanup = "PASS" if pilot.started and pilot.folder else "NOT OBSERVED"
                except (gold.C03Failure, isolation.PilotFailure, OSError, ValueError, TypeError, KeyError) as error:
                    cleanup = "FAIL"
                    failure = failure or f"Scoped cleanup ({type(error).__name__})"
    except FactValidationError as error:
        failure = str(error)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.TimeoutExpired, SyntaxError) as error:
        failure = f"Unexpected {type(error).__name__}; see the preceding stage"

    gates, compatibility, isolation, overall, cases = results(pilot, cleanup)
    if failure:
        overall = "FAIL"
    failure = (pilot.first_failure if pilot else None) or failure
    report = [
        f"Validation HEAD: {head}", f"Production candidate: {CANDIDATE}",
        f"Gate A normal replacement: {gates['A']}", f"Gate B fault and replay: {gates['B']}",
        f"Gate C legacy NULL semantics: {gates['C']}", f"Native compatibility: {compatibility}",
        f"Isolation: {isolation}", f"Cleanup: {cleanup}", f"Overall: {overall}",
        *(f"{case}: {status}" for case, status in cases.items()),
    ]
    if failure:
        report.append(f"First failure: {failure}")
    for line in report:
        print(line, flush=True)
    if "GITHUB_STEP_SUMMARY" in environment:
        with Path(environment["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as summary:
            summary.write("## Gold fact native validation\n\n" + "".join(f"- {line}\n" for line in report))
    return int(overall != "PASS")


if __name__ == "__main__":
    sys.exit(main())
