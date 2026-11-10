"""Characterize Silver SCD2 call order with scripted Spark results.

Counts represent supplied fixture states. These tests do not execute Spark plans
or verify Iceberg snapshots, version uniqueness, or transaction behavior.
"""

import unittest
from unittest.mock import MagicMock, Mock, call

from test_characterization import load_functions


class SilverSCD2Characterization(unittest.TestCase):
    table_name = "hive.silver.games"
    key = "steam_appid"
    tracked_column = "name_cleaned"

    def setUp(self):
        self.functions = MagicMock()
        self.columns = {
            name: MagicMock(name=name)
            for name in ("is_current", "s.steam_appid", "s.name_cleaned", "e.name_cleaned", "max_ver")
        }
        self.functions.col.side_effect = self.columns.__getitem__
        self.spark = Mock()
        self.table = self.spark.table.return_value
        self.current = self.table.filter.return_value
        self.source = Mock(columns=[self.key, self.tracked_column])
        self.matched = self.source.alias.return_value.join.return_value
        changed = self.matched.filter.return_value.select.return_value.distinct.return_value
        self.changed_keys = changed.cache.return_value
        joined = self.source.join.return_value
        self.new_keys = joined.select.return_value.cache.return_value
        self.selected_keys = self.changed_keys.union.return_value.distinct.return_value.cache.return_value
        self.selected_keys.count.return_value = 2

        self.successors = joined.join.return_value
        self.successors.withColumn.return_value = self.successors
        self.records = self.successors.drop.return_value
        self.writer = self.records.writeTo.return_value

        self.writes = Mock()
        self.writes.attach_mock(self.changed_keys.count, "materialize")
        self.writes.attach_mock(self.changed_keys.createOrReplaceTempView, "register")
        self.writes.attach_mock(self.spark.sql, "expire")
        self.writes.attach_mock(self.records.writeTo, "write_to")
        self.writes.attach_mock(self.writer.append, "append")

        self.production = load_functions("bronze_to_silver_etl.py", {
            "_add_scd2_columns", "_upsert_scd2",
        }, {"F": self.functions})

    def upsert(self):
        return self.production["_upsert_scd2"](
            self.spark, self.source, self.table_name, self.key, [self.tracked_column]
        )

    def write_steps(self):
        return [entry[0] for entry in self.writes.mock_calls]

    def test_initial_load_sets_version_one_and_current_metadata(self):
        self.source.withColumn.return_value = self.source
        self.assertIs(self.production["_add_scd2_columns"](self.source), self.source)
        self.assertEqual(self.functions.lit.call_args_list, [call(None), call(True), call(1)])
        self.assertEqual(self.functions.lit.return_value.cast.call_args_list,
                         [call("timestamp"), call("int")])
        self.assertEqual([args[0] for args, _ in self.source.withColumn.call_args_list],
                         ["effective_date", "expiry_date", "is_current", "scd_version"])

    def test_new_and_changed_keys_plan_versioned_successors_and_order_writes(self):
        self.assertEqual(self.upsert(), 2)
        self.source.alias.return_value.join.assert_called_once_with(
            self.current.alias.return_value, self.key, "inner"
        )
        self.columns["s.name_cleaned"].eqNullSafe.assert_called_once_with(self.columns["e.name_cleaned"])
        self.assertEqual(self.source.join.call_args_list, [
            call(self.current.select.return_value, self.key, "left_anti"),
            call(self.selected_keys, self.key, "inner"),
        ])
        self.changed_keys.union.assert_called_once_with(self.new_keys)
        self.table.groupBy.assert_called_once_with(self.key)
        self.functions.max.assert_called_once_with("scd_version")
        self.functions.max.return_value.alias.assert_called_once_with("max_ver")
        self.functions.coalesce.assert_called_once_with(self.columns["max_ver"], self.functions.lit.return_value)
        self.functions.lit.assert_any_call(0)
        next_version = self.functions.coalesce.return_value.__add__
        next_version.assert_called_once_with(1)
        next_version.return_value.cast.assert_called_once_with("int")
        self.successors.withColumn.assert_any_call("scd_version", next_version.return_value.cast.return_value)
        self.assertEqual(self.write_steps(), ["materialize", "register", "expire", "write_to", "append"])
        self.changed_keys.createOrReplaceTempView.assert_called_once_with("__scd2_expire_keys__")
        self.records.writeTo.assert_called_once_with(self.table_name)
        statement = " ".join(self.spark.sql.call_args.args[0].split())
        self.assertEqual(statement, (
            "UPDATE hive.silver.games SET is_current = false, expiry_date = current_timestamp() "
            "WHERE is_current = true AND steam_appid IN (SELECT steam_appid FROM __scd2_expire_keys__)"
        ))

    def test_repeated_unchanged_plan_skips_all_table_writes(self):
        self.selected_keys.count.return_value = 0
        self.assertEqual(self.upsert(), 0)
        self.assertEqual(self.upsert(), 0)
        self.assertEqual(self.write_steps(), [])
        self.table.groupBy.assert_not_called()
        for plan in (self.changed_keys, self.new_keys, self.selected_keys):
            self.assertEqual(plan.unpersist.call_count, 2)

    def test_materialization_failure_before_update_propagates(self):
        error = RuntimeError("synthetic materialization failure")
        self.changed_keys.count.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.write_steps(), ["materialize"])

    def test_update_failure_propagates_and_does_not_start_append(self):
        error = RuntimeError("synthetic UPDATE failure")
        self.spark.sql.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.write_steps(), ["materialize", "register", "expire"])

    def test_failure_after_update_before_append_propagates(self):
        error = RuntimeError("synthetic writer setup failure")
        self.records.writeTo.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.write_steps(), ["materialize", "register", "expire", "write_to"])

    def test_append_failure_then_retry_plans_from_current_set_and_history(self):
        # The retry fixture supplies one selected key with history and no current row.
        self.selected_keys.count.return_value = 1
        error = RuntimeError("synthetic APPEND failure")
        self.writer.append.side_effect = [error, None]
        retry_table = Mock()
        self.spark.table.side_effect = [self.table, self.table, retry_table, retry_table]
        with self.assertRaises(RuntimeError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.write_steps(), ["materialize", "register", "expire", "write_to", "append"])
        self.assertEqual(self.upsert(), 1)
        retry_current = retry_table.filter.return_value
        self.source.join.assert_any_call(retry_current.select.return_value, self.key, "left_anti")
        retry_table.groupBy.assert_called_once_with(self.key)
        retry_versions = retry_table.groupBy.return_value.agg.return_value
        self.source.join.return_value.join.assert_called_with(retry_versions, self.key, "left")
        self.assertEqual(self.writer.append.call_count, 2)

    def test_lost_acknowledgement_then_visible_successor_is_a_noop(self):
        # The retry count assumes the previous APPEND committed and is now visible.
        self.selected_keys.count.side_effect = [1, 0]
        error = TimeoutError("synthetic lost APPEND acknowledgement")
        self.writer.append.side_effect = error
        with self.assertRaises(TimeoutError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.upsert(), 0)
        self.assertEqual(self.write_steps(), ["materialize", "register", "expire", "write_to", "append"])


if __name__ == "__main__":
    unittest.main()
