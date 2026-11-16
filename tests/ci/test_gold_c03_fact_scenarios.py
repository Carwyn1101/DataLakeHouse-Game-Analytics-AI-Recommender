"""Check the native fixture oracles and case sequencing without Spark."""

from collections import Counter
from contextlib import nullcontext, redirect_stdout
import io
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import gold_c03_fact_scenarios as cases
import run_native_gold_c03_facts as driver
from test_native_gold_c03_facts import evidence


class FactScenarioTests(unittest.TestCase):
    def test_multiset_oracle_keeps_target_only_duplicates_and_nullable_payload(self):
        target = Counter({(1, 11, 21, cases.STAMP1): 2, (9, 11, 29, cases.STAMP1): 1,
                          (None, 11, None, cases.STAMP1): 1})
        new = (1, 12, 21, cases.STAMP2)
        fanout = (1, 12, 22, cases.STAMP2)
        nullable = (3, None, None, cases.OUTSIDE)
        payload = [new, new, fanout, nullable]
        expected = Counter({new: 2, fanout: 1, nullable: 1, (9, 11, 29, cases.STAMP1): 1,
                            (None, 11, None, cases.STAMP1): 1})
        self.assertEqual(cases.replacement(target, payload), expected)
        self.assertEqual(cases.replacement(expected, payload), expected)
        self.assertEqual(cases.replacement(target, []), target)
        incoming = (None, 12, None, cases.STAMP2)
        first = cases.replacement(expected, [incoming])
        self.assertEqual(cases.replacement(first, [incoming]), expected + Counter({incoming: 2}))

    def test_snapshot_check_rejects_missing_extra_and_off_lineage_commits(self):
        before = cases.State(Counter(), {3: (None, "append"), 4: (3, "overwrite")})
        self.assertEqual(cases.snapshot_child(before, cases.State(Counter(),
                         {**before.snapshots, 5: (4, "overwrite")})), 1)
        for actual in ({**before.snapshots}, {**before.snapshots, 5: (3, "overwrite")},
                       {**before.snapshots, 5: (4, "overwrite"), 6: (5, "overwrite")},
                       {4: (3, "overwrite"), 5: (4, "overwrite")}):
            with self.subTest(actual=actual), self.assertRaises(AssertionError):
                cases.snapshot_child(before, cases.State(Counter(), actual))

    def test_fault_gate_distinguishes_precommit_and_committed_readback(self):
        fact = cases.FACTS[0]
        before = cases.State(Counter({(1, 12, 21, cases.STAMP2): 2, (9, 11, 29, cases.STAMP1): 1}),
                             {1: (None, "append")})
        reference_rows = Counter({(1, 11, 21, cases.STAMP1): 1, (4, 12, 25, cases.STAMP2): 1,
                                  (9, 11, 29, cases.STAMP1): 1})
        seed = cases.State(before.rows, {7: (None, "append")})
        lost_seed = cases.State(before.rows, {10: (None, "append")})
        committed = cases.State(reference_rows, {**lost_seed.snapshots, 11: (10, "overwrite")})
        spark, run = Mock(), Mock()
        with patch.object(cases, "observe", side_effect=[before, seed, seed, committed]), \
                patch.object(cases, "freeze") as freeze, \
                patch.object(cases, "restore", side_effect=[seed, lost_seed]), \
                patch.object(cases, "run_attempt", side_effect=[None, None, committed]) as attempt, \
                patch.object(cases, "check_commit", return_value=(cases.State(reference_rows, {}), 1)):
            record = cases.gate_b(run, spark, fact)
        self.assertEqual([item.kwargs["fault"] for item in attempt.call_args_list], ["staging", "before", "lost-ack"])
        freeze.assert_called_once()
        self.assertEqual(record["lost_ack_snapshots"], 1)
        self.assertEqual(record["precommit_snapshots"], 0)
        self.assertEqual(record["reference_rows"], record["lost_ack_rows"])

    def test_scenario_emits_all_case_ids_and_stops_on_first_native_failure(self):
        spark = Mock()
        run = SimpleNamespace(session=lambda: nullcontext(spark))
        self.assertEqual(list(cases.SCENARIOS), [driver.SCENARIO])
        for fault in (None, RuntimeError("synthetic failure")):
            with self.subTest(fault=fault), redirect_stdout(io.StringIO()) as output, \
                    patch.object(cases, "seed"), patch.object(cases, "gate_a", return_value=evidence("A")), \
                    patch.object(cases, "gate_b", return_value=evidence("B"), side_effect=fault), \
                    patch.object(cases, "gate_c", return_value=evidence("C")) as null_gate:
                if fault:
                    with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
                        cases.validation(run)
                    null_gate.assert_not_called()
                else:
                    cases.validation(run)
            content = output.getvalue()
            if fault:
                records = driver.evidence_records(content, [], successful=False)
                self.assertEqual([line.split()[0] for line in records], list(driver.CASE_IDS[:3]))
                self.assertTrue(records[-1].endswith("FAIL (RuntimeError)"))
            else:
                content += f"Scenario completed: {driver.SCENARIO}\n"
                self.assertEqual(len(driver.evidence_records(content, [], successful=True)), 6)
                self.assertEqual(json.loads(content.splitlines()[-2].split(" ", 2)[-1])["legacy_behavior"],
                                 driver.LEGACY_NULL)
