"""Exercise approval, provenance and bounded result handling without native services."""

from copy import deepcopy
import io
import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import run_native_gold_c03_facts as gold


def approval():
    head = "c" * 40
    repo = {"full_name": gold.REPOSITORY}
    event = {
        "repository": repo, "sender": {"login": gold.OWNER},
        "action": "labeled", "label": {"name": gold.APPROVAL},
        "pull_request": {
            "state": "open", "draft": False,
            "base": {"sha": gold.BASE, "ref": "main", "repo": repo},
            "head": {"sha": head, "ref": gold.BRANCH, "repo": repo},
        },
    }
    environment = {
        "PILOT_HEAD": head, "GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted", "RUNNER_OS": "Linux",
        "GITHUB_EVENT_NAME": "pull_request", "GITHUB_REPOSITORY": gold.REPOSITORY, "GITHUB_ACTOR": gold.OWNER,
        "GITHUB_TRIGGERING_ACTOR": gold.OWNER, "GITHUB_RUN_ATTEMPT": "1", "NATIVE_ALLOW_WRITES": "0",
    }
    return environment, event


def evidence(gate):
    values = {
        "A": dict(initial_rows=4, refreshed_rows=8, duplicate_rows=1, fanout_rows=2, nullable_fk_rows=1,
                  refresh_snapshots=1, unchanged_snapshots=1, empty_snapshots=0),
        "B": dict(reference_rows=4, retry_rows=4, lost_ack_rows=4, staging_failure_snapshots=0,
                  precommit_snapshots=0, retry_snapshots=1, lost_ack_snapshots=1, replay_snapshots=1),
        "C": dict(initial_null_rows=1, first_null_rows=2, replay_null_rows=3, null_payload_rows=1,
                  refresh_snapshots=1, replay_snapshots=1, legacy_behavior=gold.LEGACY_NULL),
    }
    return {**dict.fromkeys(gold.COMMON_FIELDS, True), **values[gate]}


def complete_lines():
    return [*(f"{case} PASS {json.dumps(evidence(case.split('-')[1]))}" for case in gold.CASE_IDS),
            f"Scenario completed: {gold.SCENARIO}"]


class GoldFactDriverTests(unittest.TestCase):
    def test_only_owner_first_labeled_event_at_exact_head_is_allowed(self):
        environment, event = approval()
        self.assertEqual(gold.approved_event(environment, event), "c" * 40)
        for field, value in (
            ("PILOT_HEAD", "d" * 40), ("GITHUB_ACTOR", "other"), ("GITHUB_TRIGGERING_ACTOR", "other"),
            ("GITHUB_RUN_ATTEMPT", "2"), ("GITHUB_EVENT_NAME", "push"), ("NATIVE_ALLOW_WRITES", "1"),
            ("RUNNER_ENVIRONMENT", "self-hosted"),
        ):
            with self.subTest(field=field), self.assertRaises(gold.FactValidationError):
                gold.approved_event({**environment, field: value}, event)
        for path, value in (
            (("action",), "opened"), (("action",), "synchronize"), (("label", "name"), "old-native-approval"),
            (("sender", "login"), "other"), (("pull_request", "draft"), True), (("pull_request", "state"), "closed"),
            (("pull_request", "base", "sha"), "0" * 40), (("pull_request", "head", "ref"), "another-branch"),
            (("pull_request", "head", "repo", "full_name"), "other/fork"),
        ):
            altered = deepcopy(event)
            parent = altered
            for key in path[:-1]:
                parent = parent[key]
            parent[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(gold.FactValidationError):
                gold.approved_event(environment, altered)

    def test_provenance_rejects_mismatches_and_reads_immutable_blob(self):
        repository = Path("synthetic-review")
        with patch.object(gold, "git", return_value=b"wrong\n") as git:
            with self.assertRaisesRegex(gold.FactValidationError, "checkout revision"):
                gold.checked_blob(repository, gold.HARNESS, "tests/native/prepare.py")
            git.assert_called_once_with(repository, "rev-parse", "HEAD")
        with patch.object(gold, "git", side_effect=[gold.HARNESS.encode(), b"pinned blob"]) as git:
            self.assertEqual(gold.checked_blob(repository, gold.HARNESS, "tests/native/prepare.py"), b"pinned blob")
            git.assert_called_with(repository, "show", f"{gold.HARNESS}:tests/native/prepare.py")
        with patch.object(gold, "git", side_effect=[b"", b"tree\n"]) as git:
            with self.assertRaisesRegex(gold.FactValidationError, "pinned commit"):
                gold.pinned_revision(repository, gold.CANDIDATE)
            self.assertEqual(git.call_args_list[0].args[-1], gold.CANDIDATE)

    def test_reviewed_bundle_adapter_is_bound_to_exact_pins(self):
        source = f"HARNESS={gold.HARNESS!r}\nREVIEWED_PILOT={gold.ISOLATION!r}\nORIGIN={gold.ORIGIN!r}".encode()
        adapter = gold.bind_gold(source, gold.evidence_lines)
        self.assertEqual((adapter.CANDIDATE, adapter.SCENARIO), (gold.CANDIDATE, gold.SCENARIO))
        self.assertIs(adapter.evidence_lines, gold.evidence_lines)
        for pin in (gold.HARNESS, gold.ISOLATION, gold.ORIGIN):
            changed = source.replace(pin.encode(), b"mismatch")
            with self.subTest(pin=pin), self.assertRaisesRegex(gold.FactValidationError, "pins differ"):
                gold.bind_gold(changed, gold.evidence_lines)

    def test_success_requires_all_case_records_and_valid_completion(self):
        lines = complete_lines()
        self.assertEqual(len(gold.evidence_lines("\n".join(lines).encode(), [])), 6)
        for altered in (lines[1:], lines[:-1], [lines[1], lines[0], *lines[2:]], [lines[0], *lines],
                        [*lines, lines[0]], [*lines, lines[-1]]):
            with self.subTest(altered=altered), self.assertRaises(gold.FactValidationError):
                gold.evidence_lines("\n".join(altered).encode(), [])
        with self.assertRaisesRegex(gold.FactValidationError, "claimed completion"):
            gold.evidence_records("\n".join(lines), [], successful=False)

    def test_contradictory_fields_and_nonpositive_observations_are_rejected(self):
        for gate, field, value in (
            ("A", "refresh_snapshots", 2), ("A", "fanout_rows", 0), ("B", "precommit_snapshots", 1),
            ("B", "retry_rows", 3), ("C", "replay_null_rows", 2), ("C", "legacy_behavior", "PASS"),
            ("C", "schema_verified", 1),
        ):
            with self.subTest(gate=gate, field=field), self.assertRaises(gold.FactValidationError):
                gold.validate_values(gate, {**evidence(gate), field: value})
        bad = complete_lines()[0].replace("{", '{"initial_rows":4,', 1)
        with self.assertRaisesRegex(gold.FactValidationError, "Duplicate evidence"):
            gold.evidence_records(bad, [], successful=False)
        with self.assertRaisesRegex(gold.FactValidationError, "fixture credentials"):
            gold.evidence_records(complete_lines()[0], ["nullable_fk_rows"], successful=False)

    def test_early_completed_evidence_survives_large_native_output_and_late_error(self):
        early = complete_lines()[:5]
        record = f"{gold.CASE_IDS[-1]} DIAGNOSTIC IllegalArgumentException"
        raw = "\n".join([early[0], "raw JVM " + "x" * 50000, *early[1:], "secret", record]).encode()
        self.assertNotIn(early[0].encode(), raw[-16384:])
        selected = gold.collect_evidence(io.BytesIO(raw))
        self.assertNotIn(b"raw", selected)
        self.assertNotIn(b"secret", selected)
        evidence_lines = gold.evidence_records(selected.decode(), [], successful=False)
        self.assertEqual(evidence_lines[-1], f"{gold.CASE_IDS[-1]} FAIL (IllegalArgumentException)")
        self.assertEqual(len(evidence_lines), 6)
        changed = record.replace("IllegalArgumentException", "ArbitrarySensitiveClass")
        self.assertTrue(gold.evidence_records("\n".join([*early, changed]), [], successful=False)[-1].endswith("(OtherError)"))

    def test_excess_allowlisted_output_fails_closed(self):
        with self.assertRaisesRegex(gold.FactValidationError, "line exceeded"):
            gold.collect_evidence(io.BytesIO(b"C03F-A-REVIEW " + b"x" * gold.MAX_LINE_BYTES))
        lines = b"C03F-A-REVIEW START\n" * gold.MAX_EVIDENCE_BYTES
        with self.assertRaisesRegex(gold.FactValidationError, "evidence exceeded"):
            gold.collect_evidence(io.BytesIO(lines))

    def test_overall_requires_all_cases_native_success_isolation_and_cleanup(self):
        pilot = SimpleNamespace(cases=dict.fromkeys(gold.CASE_IDS, "PASS"), invocation="PASS", isolation_result="PASS")
        self.assertEqual(gold.results(pilot, "PASS")[3], "PASS")
        for cleanup in ("FAIL", "NOT OBSERVED"):
            self.assertEqual(gold.results(pilot, cleanup)[3], "FAIL")
        pilot.invocation = "FAIL"
        self.assertEqual(gold.results(pilot, "PASS")[1:4], ("FAIL", "PASS", "FAIL"))
        pilot.invocation = "PASS"
        pilot.isolation_result = "FAIL"
        self.assertEqual(gold.results(pilot, "PASS")[3], "FAIL")
        pilot.isolation_result = "PASS"
        pilot.cases[gold.CASE_IDS[-1]] = "NOT OBSERVED"
        self.assertEqual(gold.results(pilot, "PASS")[0]["C"], "NOT OBSERVED")
        self.assertEqual(gold.results(pilot, "PASS")[3], "FAIL")
