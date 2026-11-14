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

    def test_comparison_keeps_partial_baseline_and_classifies_phase_regressions(self):
        candidate = dict.fromkeys(scenario.ISSUES, 0)
        candidate.update(collision=1, first_collision=1, replay_collision=0, first_fanout=0, replay_fanout=0)
        baseline = {**candidate, "first_collision": 0, "replay_collision": 1}
        result = scenario.comparison(candidate, baseline, "MEASURED")
        self.assertEqual(result["observations"]["collision"], "REGRESSION")
        self.assertEqual(scenario.duplicate_status(result["observations"].values()), "REGRESSION")

        matched = scenario.comparison(candidate, candidate, "MEASURED")
        self.assertEqual(scenario.duplicate_status(matched["observations"].values()), "CHARACTERIZED")
        self.assertEqual(matched["observations"]["fanout"], "NO ISSUE")

        partial = {"first_rows": 2, "first_collision": 1}
        incomplete = scenario.comparison(candidate, partial, "EXECUTION_ERROR")
        self.assertEqual(incomplete["baseline"], partial)
        self.assertEqual(set(incomplete["observations"].values()), {"UNVERIFIED"})
        self.assertEqual(scenario.duplicate_status(incomplete["observations"].values()), "UNVERIFIED")
