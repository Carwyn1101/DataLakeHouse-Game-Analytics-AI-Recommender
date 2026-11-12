"""Exercise C02 assertions and fault timing offline using small synthetic states."""

from copy import deepcopy
from datetime import datetime
import unittest
from unittest.mock import Mock, call

import silver_c02_scenarios as c02


MERGE = "MERGE INTO `hive`.`silver`.`games` AS t USING source AS s"


class SilverScenarioTests(unittest.TestCase):
    def states(self):
        start = datetime(2026, 1, 1)
        end = datetime(2026, 1, 2)
        old = {"steam_appid": 101, "name_cleaned": "Original", "price": 50,
               "effective_date": start, "expiry_date": None, "is_current": True, "scd_version": 1}
        before = c02.State((old,), ({"snapshot_id": 3, "parent_id": None, "operation": "append"},))
        incoming = {"steam_appid": 101, "name_cleaned": "Changed", "price": 50}
        successor = dict(incoming, effective_date=end, expiry_date=None, is_current=True, scd_version=2)
        after = c02.State((dict(old, expiry_date=end, is_current=False), successor),
                          (*before.snapshots, {"snapshot_id": 8, "parent_id": 3, "operation": "overwrite"}))
        return before, after, incoming

    def test_transition_preserves_history_and_has_one_snapshot_with_same_validity_boundary(self):
        before, after, incoming = self.states()
        c02.assert_transition(before, after, incoming, 2)

    def test_transition_rejects_history_change_invalid_boundary_or_extra_current(self):
        for column, value in (("price", 0), ("expiry_date", None), ("is_current", True)):
            with self.subTest(column=column):
                before, after, incoming = self.states()
                after.rows[0][column] = value
                with self.assertRaises(AssertionError):
                    c02.assert_transition(before, after, incoming, 2)

    def test_snapshot_assertion_rejects_missing_extra_or_unrelated_commit(self):
        before, after, _ = self.states()
        unrelated = deepcopy(after.snapshots)
        unrelated[-1]["parent_id"] = 5
        for snapshots in (before.snapshots, unrelated,
                          (*after.snapshots, {"snapshot_id": 9, "parent_id": 8, "operation": "overwrite"})):
            with self.subTest(snapshots=snapshots), self.assertRaises(AssertionError):
                c02.one_snapshot(before.snapshots, snapshots)

    def test_pre_merge_fault_does_not_call_native_sql(self):
        sql = Mock()
        with self.assertRaisesRegex(c02.InjectedFault, "before native SQL"):
            c02.intercept_merge(sql)(MERGE)
        sql.assert_not_called()

    def test_lost_ack_occurs_only_after_native_sql_and_confirmation(self):
        sql, confirmed, steps = Mock(), Mock(), Mock()
        steps.attach_mock(sql, "sql")
        steps.attach_mock(sql.return_value.collect, "execute")
        steps.attach_mock(confirmed, "confirm")
        patched = c02.intercept_merge(sql, confirmed)
        with self.assertRaisesRegex(c02.InjectedFault, "after native commit"):
            patched(MERGE)
        self.assertEqual(steps.mock_calls, [call.sql(MERGE), call.execute(), call.confirm()])

    def test_fault_wrapper_passes_reads_through_and_preserves_confirmation_errors(self):
        sql = Mock()
        error = RuntimeError("synthetic snapshot failure")
        patched = c02.intercept_merge(sql, Mock(side_effect=error))
        self.assertIs(patched("SELECT 1"), sql.return_value)
        with self.assertRaises(RuntimeError) as raised:
            patched(MERGE)
        self.assertIs(raised.exception, error)


if __name__ == "__main__":
    unittest.main()
