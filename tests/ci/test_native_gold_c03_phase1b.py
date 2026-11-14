"""Offline checks of Phase 1B's approval and evidence boundaries."""

from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
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
    return dict.fromkeys(gold.DUPLICATE_CHECKS, 0)


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

    def test_duplicate_findings_remain_fail_even_after_other_cases_pass(self):
        case = gold.CASE_IDS[-2]
        counters = clean_record(case)
        counters["conflicting_new_collision"] = 1
        counters["conflicting_new_repeated_commit"] = 1
        counters["conflicting_new_fanout"] = 1
        records = [*completed_output()[:3], case_line(case, "FAIL", counters), case_line(gold.CASE_IDS[-1])]
        lines = gold.evidence_records("\n".join(records), [], successful=False)
        self.assertTrue(lines[-2].startswith(f"{case} FAIL "))
        self.assertIn('"conflicting_new_collision":1', lines[-2])
        with self.assertRaisesRegex(gold.GateFailure, "requires five clean"):
            gold.evidence_lines("\n".join([*records, completed_output()[-1]]).encode(), [])
        with self.assertRaisesRegex(gold.GateFailure, "contradicts"):
            gold.checked_case(case_line(case, "PASS", counters))
        with self.assertRaisesRegex(gold.GateFailure, "contradicts"):
            gold.checked_case(case_line(case, "FAIL"))

    def test_failure_evidence_rejects_raw_values_duplicate_fields_and_credentials(self):
        case = gold.CASE_IDS[-1]
        for value in ("sensitive", True, -1, 1001, 1.0):
            record = clean_record(case)
            record["exact_new_fanout"] = value
            with self.subTest(value=value), self.assertRaises(gold.GateFailure):
                gold.checked_case(case_line(case, "FAIL", record))
        duplicated_key = case_line(case).replace("{", '{"exact_new_fanout":0,', 1)
        with self.assertRaisesRegex(gold.GateFailure, "evidence fields"):
            gold.checked_case(duplicated_key)
        with self.assertRaisesRegex(gold.GateFailure, "fixture credentials"):
            gold.evidence_records(case_line(case), ["exact_new_fanout"], successful=False)

    def test_native_diagnostics_forward_valid_case_and_type_only(self):
        class BronzePilot:
            invocation = "FAIL"
            redactions = []

        pilot = gold.gold_pilot(Mock(BronzePilot=BronzePilot))
        for log, expected in (
            ("raw credentials and SQL\nC03B-DETAIL DIAGNOSTIC IllegalArgumentException",
             "C03B-DETAIL failed (IllegalArgumentException)"),
            ("::warning::private\nC03B-REVIEW DIAGNOSTIC ArbitraryJVMError",
             "C03B-REVIEW failed (OtherError)"),
            ("C03B-SETUP DIAGNOSTIC RuntimeError\nC03B-DETAIL DIAGNOSTIC ValueError",
             "Native Gold Phase 1B failure: no validated evidence"),
        ):
            with self.subTest(log=log), redirect_stdout(io.StringIO()) as output:
                pilot.diagnostic(log)
            self.assertEqual(output.getvalue().strip(), expected)
