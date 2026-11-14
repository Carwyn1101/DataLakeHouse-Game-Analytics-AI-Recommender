"""Offline assertions for snapshot lineage and duplicate findings."""

from collections import Counter
from contextlib import redirect_stdout
import io
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import gold_c03_phase1b_scenarios as scenario
from gold_c03_phase1b_scenarios import DIMENSIONS, State, check_stage, child_snapshot, duplicate_findings, row


class Phase1BScenarioTests(unittest.TestCase):
    def test_only_one_child_of_the_current_snapshot_is_accepted(self):
        before = State(Counter(), {1: (None, "append"), 2: (1, "overwrite")})
        after = State(Counter(), {**before.snapshots, 3: (2, "overwrite")})
        child_snapshot(before, after)
        for invalid in (
            before,
            State(Counter(), {**before.snapshots, 3: (1, "overwrite")}),
            State(Counter(), {**after.snapshots, 4: (3, "overwrite")}),
            State(Counter(), {1: (None, "append"), 3: (2, "overwrite")}),
        ):
            with self.subTest(invalid=invalid), self.assertRaises(AssertionError):
                child_snapshot(before, invalid)

    def test_parquet_stage_requires_schema_payload_and_one_delete_marker(self):
        for dimension in DIMENSIONS:
            frame = Mock()
            frame.dtypes = (*dimension.schema, ("__dimension_delete_marker", "boolean"),
                            ("__dimension_changed", "boolean"))
            frame.schema.fields = [Mock(nullable=True) for _ in frame.dtypes]
            changed = row(dimension, 2, 101, 10)
            duplicate = row(dimension, 2, 101, 11)
            new = row(dimension, 21, 303, 12)
            expected = [(*changed, True), (*duplicate, True), (*new, False)]
            rows = [(*changed, True, True), (*duplicate, False, True), (*new, False, False)]
            frame.collect.return_value = rows
            check_stage(frame, dimension, expected)
            for malformed in (
                [(*changed, False, True), *rows[1:]],
                [rows[0], (*duplicate, True, True), rows[2]],
                [*rows[:2], (*new, True, False)],
                [*rows[:2], (*new, False, True)],
                [*rows[:2], (*row(dimension, 20, 303, 12), False, False)],
            ):
                with self.subTest(dimension=dimension.name, malformed=malformed), self.assertRaises(AssertionError):
                    frame.collect.return_value = malformed
                    check_stage(frame, dimension, expected)
            frame.dtypes = frame.dtypes[:-1]
            with self.subTest(dimension=dimension.name), self.assertRaisesRegex(AssertionError, "schema"):
                check_stage(frame, dimension, expected)

    def test_healed_or_preexisting_defects_remain_visible(self):
        self.assertEqual(duplicate_findings(Counter({7: 2}), Counter({7: 1}), 2, snapshot_delta=1),
                         {"collision": 1, "repeated_commit": 1, "fanout": 1, "key_drift": 0})
        self.assertEqual(duplicate_findings(Counter({23: 1, 24: 1}), Counter({24: 2}), 2, snapshot_delta=1),
                         {"collision": 1, "repeated_commit": 1, "fanout": 1, "key_drift": 1})
        self.assertEqual(duplicate_findings(Counter({7: 1}), Counter({7: 1}), 1, snapshot_delta=0),
                         {"collision": 0, "repeated_commit": 0, "fanout": 0, "key_drift": 0})

    def test_gate_a_failure_stops_before_baseline_or_duplicate_execution(self):
        run = SimpleNamespace(new_spark=Mock(), storage="s3a://native-0123456789abcdef")
        with patch.object(scenario, "canonical", side_effect=AssertionError("synthetic Gate A")) as canonical, \
                patch.object(scenario, "load_baseline") as load, \
                patch.object(scenario, "duplicates") as duplicates, redirect_stdout(io.StringIO()) as log:
            with self.assertRaisesRegex(AssertionError, "synthetic Gate A"):
                scenario.run_phase_1b(run)
        canonical.assert_called_once_with(run, run.session, DIMENSIONS[0])
        load.assert_not_called()
        duplicates.assert_not_called()
        self.assertIn("C03B-DETAIL DIAGNOSTIC AssertionError", log.getvalue())
        self.assertNotIn("C03B-DETAIL PASS", log.getvalue())

    def test_duplicate_cases_keep_baseline_evidence_after_candidate_failure(self):
        run, spark, baseline = object(), Mock(), object()
        before = State(Counter(), {})
        rejection = {"outcome": "REJECTED_DUPLICATE_INPUT"}
        historical = {"first_rows": 2}
        with patch.object(scenario, "observe", return_value=before), \
                patch.object(scenario, "freeze") as freeze, \
                patch.object(scenario, "reset_duplicate_target", return_value=before), \
                patch.object(scenario, "reject_duplicate_input", side_effect=[
                    rejection, AssertionError("synthetic mutation"), rejection, rejection,
                ]) as candidate, \
                patch.object(scenario, "measure_baseline", return_value=(historical, "EXECUTION_ERROR")) as measure:
            result = scenario.duplicates(run, spark, DIMENSIONS[1], baseline)
        self.assertEqual(candidate.call_count, 4)
        self.assertEqual(freeze.call_count, 4)
        self.assertEqual(measure.call_count, 4)
        self.assertEqual(result["exact_new"], {
            "candidate": {"outcome": "FAIL", "error": "AssertionError"},
            "baseline": historical, "baseline_status": "EXECUTION_ERROR",
        })
        self.assertEqual(result["conflicting_new"]["candidate"], rejection)

    def test_gate_b_reports_both_cases_before_failing(self):
        run = SimpleNamespace(new_spark=Mock(), storage="s3a://native-0123456789abcdef")
        failure = {"exact_new": {"candidate": {"outcome": "FAIL", "error": "AssertionError"}}}
        rejected = {"exact_new": {"candidate": {"outcome": "REJECTED_DUPLICATE_INPUT"}}}
        with patch.object(scenario, "canonical", return_value={"snapshots": 3}), \
                patch.object(scenario, "load_baseline"), \
                patch.object(scenario, "duplicates", side_effect=[failure, rejected]), redirect_stdout(io.StringIO()) as log:
            with self.assertRaisesRegex(AssertionError, "not safely rejected"):
                scenario.run_phase_1b(run)
        self.assertIn("C03B-REVIEW-DUPS FAIL", log.getvalue())
        self.assertIn("C03B-POPULARITY-DUPS PASS", log.getvalue())
