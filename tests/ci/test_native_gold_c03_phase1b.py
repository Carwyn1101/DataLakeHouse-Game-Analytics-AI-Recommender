"""Offline checks of Phase 1B's approval and evidence boundaries."""

from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import run_native_gold_c03_phase1b as gold


def owner_event():
    head = "a" * 40
    repository = {"full_name": gold.REPOSITORY}
    environment = {
        "PILOT_HEAD": head,
        "GITHUB_ACTIONS": "true",
        "RUNNER_ENVIRONMENT": "github-hosted",
        "RUNNER_OS": "Linux",
        "GITHUB_EVENT_NAME": "pull_request",
        "GITHUB_REPOSITORY": gold.REPOSITORY,
        "GITHUB_ACTOR": gold.OWNER,
        "GITHUB_TRIGGERING_ACTOR": gold.OWNER,
        "GITHUB_RUN_ATTEMPT": "1",
        "NATIVE_ALLOW_WRITES": "0",
    }
    event = {
        "action": "labeled",
        "label": {"name": gold.APPROVAL},
        "repository": repository,
        "sender": {"login": gold.OWNER},
        "pull_request": {
            "state": "open",
            "draft": False,
            "base": {"ref": "main", "repo": repository, "sha": gold.BASE},
            "head": {"ref": gold.BRANCH, "sha": head, "repo": repository},
        },
    }
    return environment, event


def clean_record(case):
    if case in gold.CASE_IDS[:3]:
        return {"rows": 6, "retained_fact_references": 4, "successful_snapshots": 3,
                "readback_verified": True, "scratch_cleaned": True}
    measures = dict.fromkeys(gold.MEASUREMENTS, 0)
    measures.update(first_rows=2, replay_rows=2, first_snapshots=1)
    return {
        variant: {"candidate": dict(gold.REJECTION), "baseline": {**measures, "first_snapshots": 2},
                  "baseline_status": "MEASURED"}
        for variant in gold.VARIANTS
    }


def case_line(case, status="PASS", record=None):
    return f"{case} {status} {json.dumps(clean_record(case) if record is None else record)}"


def completed_output():
    return [*(case_line(case) for case in gold.CASE_IDS), f"Scenario completed: {gold.SCENARIO}"]


class Phase1BPilotTests(unittest.TestCase):
    def test_owner_gate_rejects_mismatched_actor_sha_event_and_runner(self):
        environment, event = owner_event()
        self.assertEqual(gold.approved_event(environment, event), "a" * 40)
        for field, value in {
            "GITHUB_ACTIONS": "false",
            "RUNNER_ENVIRONMENT": "self-hosted",
            "RUNNER_OS": "Windows",
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_REPOSITORY": "someone/else",
            "GITHUB_ACTOR": "someone",
            "GITHUB_TRIGGERING_ACTOR": "someone",
            "GITHUB_RUN_ATTEMPT": "2",
            "NATIVE_ALLOW_WRITES": "1",
            "PILOT_HEAD": "b" * 40,
        }.items():
            with self.subTest(field=field), self.assertRaises(gold.GateFailure):
                gold.approved_event(dict(environment, **{field: value}), event)

    def test_owner_gate_rejects_wrong_branch_base_label_and_draft(self):
        environment, event = owner_event()
        for path, value in (
            (("action",), "synchronize"),
            (("label", "name"), "native-gold-c03-approved"),
            (("sender", "login"), "someone"),
            (("pull_request", "draft"), True),
            (("pull_request", "state"), "closed"),
            (("pull_request", "base", "sha"), "f" * 40),
            (("pull_request", "head", "ref"), "another-branch"),
            (("pull_request", "head", "repo"), {"full_name": "someone/fork"}),
        ):
            altered = deepcopy(event)
            parent = altered
            for key in path[:-1]:
                parent = parent[key]
            parent[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(gold.GateFailure):
                gold.approved_event(environment, altered)

    def test_checked_blob_verifies_checkout_before_reading(self):
        repository = Path("reviewed")
        with patch.object(gold, "git", return_value=b"wrong\n") as git:
            with self.assertRaisesRegex(gold.GateFailure, "checkout revision"):
                gold.checked_blob(repository, gold.HARNESS, "tests/native/prepare.py")
            git.assert_called_once_with(repository, "rev-parse", "HEAD")

    def test_reviewed_adapter_requires_matching_harness_and_pilot_pins(self):
        source = f"HARNESS={gold.HARNESS!r}\nREVIEWED_PILOT={gold.REVIEWED_PILOT!r}\nORIGIN={gold.ORIGIN!r}"
        adapter = gold.bind_gold(source.encode())
        self.assertEqual((adapter.CANDIDATE, adapter.SCENARIO), (gold.CANDIDATE, gold.SCENARIO))
        self.assertIs(adapter.evidence_lines, gold.evidence_lines)
        with self.assertRaisesRegex(gold.GateFailure, "pins differ"):
            gold.bind_gold(source.replace(gold.HARNESS, "f" * 40).encode())

    def test_success_requires_all_ordered_pass_cases_and_completion(self):
        lines = completed_output()
        self.assertEqual(len(gold.evidence_lines("\n".join(lines).encode(), [])), len(gold.CASE_IDS))
        invalid = (lines[1:], [lines[1], lines[0], *lines[2:]], lines[:-1], [lines[0], *lines],
                   [*lines, lines[-1]])
        for payload in invalid:
            with self.subTest(payload=payload), self.assertRaises(gold.GateFailure):
                gold.evidence_lines("\n".join(payload).encode(), [])

    def test_gate_a_failure_is_fatal_and_never_a_pass(self):
        log = f"{gold.CASE_IDS[0]} DIAGNOSTIC AssertionError"
        self.assertEqual(gold.gate_states(gold.evidence_records(log, [], successful=False)),
                         ("FAIL", "INCOMPLETE"))
        with self.assertRaisesRegex(gold.GateFailure, "completion requires"):
            gold.evidence_records(log, [], successful=True)
        record = clean_record(gold.CASE_IDS[0])
        record["successful_snapshots"] = 2
        with self.assertRaisesRegex(gold.GateFailure, "Incomplete canonical"):
            gold.checked_case(case_line(gold.CASE_IDS[0], record=record))

    def test_safe_rejection_accepts_different_historical_outcome(self):
        case = gold.CASE_IDS[-2]
        record = clean_record(case)
        variant = record["conflicting_new"]
        variant["baseline"].update(collision=1, replay_collision=1, repeated_commit=1, replay_snapshots=2)
        lines = completed_output()
        lines[-3] = case_line(case, record=record)
        evidence = gold.evidence_records("\n".join(lines), [], successful=True)
        self.assertEqual(gold.gate_states(evidence), ("PASS", "PASS"))
        self.assertEqual(gold.baseline_state(evidence), "CHARACTERIZED")
        self.assertIn('"replay_snapshots":2', evidence[-2])
        self.assertIn('"outcome":"REJECTED_DUPLICATE_INPUT"', evidence[-2])

    def test_missing_rejection_or_target_mutation_is_fatal(self):
        case = gold.CASE_IDS[-2]
        for field, value in (("outcome", "INGESTED"), ("attempts", 1), ("target_write_attempts", 1),
                             ("rows_unchanged", False), ("snapshots_unchanged", False),
                             ("references_preserved", False), ("scratch_cleaned", False)):
            record = clean_record(case)
            record["conflicting_existing"]["candidate"][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(gold.GateFailure, "Missing safe"):
                gold.checked_case(case_line(case, record=record))
        record = clean_record(case)
        del record["exact_new"]
        with self.assertRaisesRegex(gold.GateFailure, "Missing or unexpected duplicate fixture"):
            gold.checked_case(case_line(case, record=record))

    def test_unavailable_baseline_remains_unverified_after_candidate_pass(self):
        case = gold.CASE_IDS[-1]
        record = clean_record(case)
        for variant, present in zip(record.values(), (True, False, False, False)):
            historical = {name: variant["baseline"][name] for name in gold.FIRST_MEASUREMENTS}
            variant.update(baseline=historical if present else None, baseline_status="EXECUTION_ERROR")
        lines = completed_output()
        lines[-2] = case_line(case, record=record)
        evidence = gold.evidence_records("\n".join(lines), [], successful=True)
        self.assertEqual(gold.gate_states(evidence), ("PASS", "PASS"))
        self.assertEqual(gold.baseline_state(evidence), "UNVERIFIED")
        self.assertIn('"first_snapshots":2', evidence[-1])

    def test_failure_evidence_rejects_raw_values_duplicate_fields_and_credentials(self):
        case = gold.CASE_IDS[-1]
        for value in ("sensitive", True, -1, 1001, 1.0):
            record = clean_record(case)
            record["exact_new"]["baseline"]["fanout"] = value
            with self.subTest(value=value), self.assertRaises(gold.GateFailure):
                gold.checked_case(case_line(case, record=record))
        duplicated_key = case_line(case).replace('"fanout": 0', '"fanout":0,"fanout":0', 1)
        with self.assertRaisesRegex(gold.GateFailure, "Duplicate"):
            gold.checked_case(duplicated_key)
        with self.assertRaisesRegex(gold.GateFailure, "fixture credentials"):
            gold.evidence_records(case_line(case), ["exact_new"], successful=False)
        record = clean_record(case)
        record["exact_new"]["baseline"]["replay_snapshots"] = 1
        with self.assertRaisesRegex(gold.GateFailure, "Contradictory measurement"):
            gold.checked_case(case_line(case, record=record))

    def test_workflow_result_holds_unverified_but_completes_characterization(self):
        for gate_b, historical_state, expected_exit, result in (
            ("PASS", "CHARACTERIZED", 0, "PASS"), ("PASS", "UNVERIFIED", 1, "HOLD"),
            ("FAIL", "UNVERIFIED", 1, "FAIL"),
        ):
            with self.subTest(gate_b=gate_b, baseline=historical_state), tempfile.TemporaryDirectory() as folder:
                workspace = Path(folder)
                harness = workspace / "reviewed-harness"
                event_file = workspace / "event.json"
                environment, event = owner_event()
                event_file.write_text(json.dumps(event), encoding="utf-8")
                summary = workspace / "summary"
                environment.update(GITHUB_EVENT_PATH=str(event_file), GITHUB_WORKSPACE=str(workspace),
                                   GITHUB_STEP_SUMMARY=str(summary))
                historical = f"HARNESS={gold.HARNESS!r}\nORIGIN={gold.ORIGIN!r}\nPilotFailure=RuntimeError".encode()
                pilot = Mock(gate_a="PASS", gate_b=gate_b, invocation="PASS", historical=historical_state,
                             cases=None, first_failure=None, folder=workspace)
                reviewed = Mock(C03Failure=ValueError)
                if gate_b == "FAIL":
                    pilot.invocation = "FAIL"
                    reviewed.run.side_effect = RuntimeError("reviewed native command failed")
                with patch.dict(gold.os.environ, environment, clear=True), \
                        patch.object(gold.sys, "version_info", (3, 10, 22)), \
                        patch.object(gold.Path, "cwd", return_value=harness), \
                        patch.object(gold, "checked_blob", side_effect=[b"scenario", b"prepare", b"recipe", historical]), \
                        patch.object(gold, "git", return_value=b"commit"), \
                        patch.object(gold, "gold_pilot", return_value=pilot), \
                        patch.object(gold, "bind_gold", return_value=reviewed), \
                        patch.object(gold, "bind_baseline"), redirect_stdout(io.StringIO()):
                    self.assertEqual(gold.main(), expected_exit)
                report = summary.read_text()
                self.assertIn("- Gate A atomicity: PASS", report)
                self.assertIn(f"- Gate B safe rejection: {gate_b}", report)
                self.assertIn(f"- Baseline: {historical_state}", report)
                self.assertIn("- Cleanup: PASS", report)
                self.assertIn(f"- Result: {result}", report)
                self.assertIn("- Merge readiness: HOLD", report)
                pilot.cleanup.assert_called_once_with()

    def test_late_native_failure_preserves_early_outcomes_and_both_duplicate_cases(self):
        class BronzePilot:
            redactions = ["private token"]

        pilot = gold.gold_pilot(Mock(BronzePilot=BronzePilot, PilotFailure=RuntimeError))
        lines = completed_output()[:-1]
        record = clean_record(gold.CASE_IDS[-1])
        record["conflicting_new"]["candidate"] = {"outcome": "FAIL", "error": "IllegalArgumentException"}
        lines[-1] = case_line(gold.CASE_IDS[-1], "FAIL", record)
        lines.insert(1, "private token\n" + "s" * 20000)
        lines.insert(-1, "private SQL\n" + "j" * 30000)
        raw = "\n".join(lines).encode()
        self.assertNotIn(lines[0].encode(), raw[-16384:])

        def fail(*args, stdout, stderr, **kwargs):
            self.assertEqual(stderr, gold.subprocess.DEVNULL)
            stdout.write(raw)
            return Mock(returncode=1)

        with patch.object(gold.subprocess, "run", side_effect=fail), redirect_stdout(io.StringIO()) as output:
            with self.assertRaisesRegex(RuntimeError, "failed"):
                pilot.command(gold.NATIVE_STAGE, ["synthetic"])
        self.assertEqual((pilot.gate_a, pilot.gate_b), ("PASS", "FAIL"))
        self.assertEqual(pilot.first_failure, f"{gold.CASE_IDS[-1]} conflicting_new (IllegalArgumentException)")
        self.assertEqual(pilot.cases[gold.CASE_IDS[-2]], "PASS")
        self.assertIn(gold.CASE_IDS[-2] + " PASS", output.getvalue())
        self.assertIn(gold.CASE_IDS[-1] + " FAIL", output.getvalue())
        self.assertIn('"baseline_status":"MEASURED"', output.getvalue())
        self.assertIn("REJECTED_DUPLICATE_INPUT", output.getvalue())
        self.assertNotIn("private", output.getvalue())

    def test_failure_preserves_multiple_case_diagnostics_in_order(self):
        lines = completed_output()[:3] + [
            f"{gold.CASE_IDS[-2]} DIAGNOSTIC AssertionError",
            f"{gold.CASE_IDS[-1]} DIAGNOSTIC ArbitraryJVMError",
        ]
        evidence = gold.evidence_records("\n".join(lines), [], successful=False)
        self.assertEqual(gold.gate_states(evidence), ("PASS", "FAIL"))
        self.assertTrue(evidence[-1].endswith("(OtherError)"))
        with self.assertRaisesRegex(gold.GateFailure, "Duplicate or contradictory"):
            gold.evidence_records("\n".join([*lines, lines[-1]]), [], successful=False)

    def test_collection_rejects_oversized_marker_without_copying_native_output(self):
        marker = b"C03B-DETAIL " + b"x" * gold.MAX_EVIDENCE_LINE
        with self.assertRaisesRegex(gold.GateFailure, "line exceeded"):
            gold.collect_evidence(io.BytesIO(marker))
        with self.assertRaisesRegex(gold.GateFailure, "evidence exceeded"):
            gold.collect_evidence(io.BytesIO(b"C03B-DETAIL START\n" * gold.MAX_EVIDENCE_BYTES))
