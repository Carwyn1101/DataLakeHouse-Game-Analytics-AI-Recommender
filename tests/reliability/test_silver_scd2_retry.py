"""Verify Silver SCD2 planning and retry control flow with scripted Spark results.

These tests do not execute Spark joins, evaluate SQL, or verify Iceberg commits.
"""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, call

from test_characterization import load_functions


class SilverSCD2RecoveryTests(unittest.TestCase):
    table_name = "hive.silver.games"
    key = "steam_appid"
    expire = "__scd2_expire"
    max_version = "__scd2_max_version"

    def setUp(self):
        self.functions = MagicMock()
        self.columns = {
            name: MagicMock(name=name)
            for name in ("is_current", "steam_appid", "count", "e.is_current",
                         "s.name_cleaned", "e.name_cleaned", self.expire)
        }
        self.columns["count"].__gt__.return_value = MagicMock()
        self.columns["is_current"].__eq__.return_value = MagicMock()
        self.functions.col.side_effect = self.columns.__getitem__
        self.spark = Mock()
        self.table = self.spark.table.return_value
        self.source = Mock(columns=[self.key, "name_cleaned", "ts_ingested"])
        self.table.columns = ["ts_ingested", self.key, "name_cleaned",
                              "effective_date", "expiry_date", "is_current", "scd_version"]
        self.current = self.table.filter.return_value
        self.joined = self.source.alias.return_value.join.return_value
        self.invalid = self.joined.groupBy.return_value.count.return_value.filter.return_value
        self.invalid.limit.return_value.count.return_value = 0
        self.selected = self.joined.filter.return_value.select.return_value.cache.return_value
        self.selected.count.return_value = 2
        self.successors = self.selected.join.return_value
        self.inserts = self.successors.withColumn.return_value
        self.staged = self.inserts.unionByName.return_value
        self.steps = Mock()
        self.steps.attach_mock(self.selected.count, "count")
        self.steps.attach_mock(self.staged.write.parquet, "stage")
        self.steps.attach_mock(self.spark.sql, "merge")
        self.steps.attach_mock(self.selected.unpersist, "unpersist")

        self.production = load_functions("bronze_to_silver_etl.py", {
            "_add_scd2_columns", "_upsert_scd2",
        }, {"F": self.functions, "Path": Path, "tempfile": tempfile})

    def upsert(self):
        return self.production["_upsert_scd2"](
            self.spark, self.source, self.table_name, self.key, ["name_cleaned"]
        )

    def step_names(self):
        return [entry[0] for entry in self.steps.mock_calls]

    def test_initial_load_sets_version_one_and_current_metadata(self):
        self.source.withColumn.return_value = self.source
        self.assertIs(self.production["_add_scd2_columns"](self.source), self.source)
        self.assertEqual(self.functions.lit.call_args_list, [call(None), call(True), call(1)])
        self.assertEqual(self.functions.lit.return_value.cast.call_args_list,
                         [call("timestamp"), call("int")])
        self.assertEqual([args[0] for args, _ in self.source.withColumn.call_args_list],
                         ["effective_date", "expiry_date", "is_current", "scd_version"])

    def test_normal_changes_use_one_merge_with_versioned_successors(self):
        self.assertEqual(self.upsert(), 2)
        self.table.filter.assert_called_once_with(self.columns["is_current"].__eq__.return_value)
        self.source.alias.return_value.join.assert_called_once_with(
            self.current.alias.return_value, self.key, "left"
        )
        self.columns["s.name_cleaned"].eqNullSafe.assert_called_once_with(self.columns["e.name_cleaned"])
        has_current = self.columns["e.is_current"].isNotNull.return_value
        self.joined.filter.return_value.select.assert_called_once_with(
            "s.*", has_current.alias.return_value
        )
        has_current.alias.assert_called_once_with(self.expire)
        self.table.groupBy.assert_called_once_with(self.key)
        self.functions.max.assert_called_once_with("scd_version")
        self.functions.max.return_value.alias.assert_called_once_with(self.max_version)
        self.selected.join.assert_called_once_with(
            self.table.groupBy.return_value.agg.return_value, self.key, "left"
        )
        self.assertEqual(self.functions.lit.call_args_list, [call(False), call(False)])
        self.successors.withColumn.assert_called_once_with(self.expire, self.functions.lit.return_value)
        self.successors.filter.assert_called_once_with(self.columns[self.expire])
        self.inserts.unionByName.assert_called_once_with(self.successors.filter.return_value)

        self.assertEqual(self.step_names(), ["count", "stage", "merge", "unpersist"])
        self.staged.createTempView.assert_not_called()
        self.spark.catalog.dropTempView.assert_not_called()
        uri = self.staged.write.parquet.call_args.args[0]
        self.assertRegex(uri, r"^file:///.*/silver-scd2-[^/]+/actions$")
        self.assertEqual(" ".join(self.spark.sql.call_args.args[0].split()), (
            f"MERGE INTO `hive`.`silver`.`games` AS t USING parquet.`{uri}` AS s "
            "ON s.__scd2_expire AND t.is_current = true AND t.`steam_appid` = s.`steam_appid` "
            "WHEN MATCHED THEN UPDATE SET is_current = false, expiry_date = current_timestamp() "
            "WHEN NOT MATCHED AND NOT s.__scd2_expire THEN "
            "INSERT (`steam_appid`, `name_cleaned`, `ts_ingested`, `effective_date`, `expiry_date`, "
            "`is_current`, `scd_version`) "
            "VALUES (s.`steam_appid`, s.`name_cleaned`, s.`ts_ingested`, current_timestamp(), "
            "CAST(NULL AS TIMESTAMP), true, CAST(COALESCE(s.__scd2_max_version, 0) + 1 AS INT))"
        ))

    def test_expired_only_key_uses_history_and_is_eligible_without_current(self):
        self.selected.count.return_value = 1
        self.assertEqual(self.upsert(), 1)
        has_current = self.columns["e.is_current"].isNotNull.return_value
        has_current.__invert__.assert_called_once_with()
        self.joined.filter.assert_called_once_with(has_current.__invert__.return_value.__or__.return_value)
        self.table.groupBy.assert_called_once_with(self.key)
        self.functions.max.assert_called_once_with("scd_version")
        self.assertEqual(self.spark.sql.call_count, 1)

    def test_repeated_unchanged_input_issues_no_merge(self):
        self.selected.count.return_value = 0
        self.assertEqual(self.upsert(), 0)
        self.assertEqual(self.upsert(), 0)
        self.assertEqual(self.step_names(), ["count", "unpersist", "count", "unpersist"])
        self.table.groupBy.assert_not_called()

    def test_invalid_key_cardinality_fails_before_any_write(self):
        self.invalid.limit.return_value.count.return_value = 1
        with self.assertRaisesRegex(ValueError, "unique source keys"):
            self.upsert()
        self.joined.groupBy.assert_called_once_with(self.key)
        self.columns[self.key].isNull.assert_called_once_with()
        self.columns["count"].__gt__.assert_called_once_with(1)
        self.invalid.limit.assert_called_once_with(1)
        self.assertEqual(self.step_names(), [])

    def test_missing_source_column_fails_before_any_write(self):
        self.source.columns.remove("ts_ingested")
        with self.assertRaisesRegex(ValueError, "target schema"):
            self.upsert()
        self.assertEqual(self.step_names(), [])

    def test_planning_failure_propagates_and_releases_cache(self):
        error = RuntimeError("synthetic staging failure")
        self.selected.count.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.step_names(), ["count", "unpersist"])

    def test_failed_scratch_write_does_not_merge_and_cleans_directory(self):
        error = OSError("synthetic scratch write failure")
        directories = []

        def fail_write(uri):
            path = self.scratch_path(uri)
            directories.append(path.parent)
            path.mkdir()
            (path / "partial").touch()
            raise error

        self.staged.write.parquet.side_effect = fail_write
        with self.assertRaises(OSError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.step_names(), ["count", "stage", "unpersist"])
        self.assertFalse(directories[0].exists())

    @staticmethod
    def scratch_path(uri):
        path = uri.removeprefix("file://")
        return Path(path[1:] if len(path) > 2 and path[2] == ":" else path)

    def test_interrupted_merge_then_retry_replans_against_previous_state(self):
        # Script a failure before commit, followed by a retry still seeing the old current row.
        error = RuntimeError("synthetic pre-commit failure")
        directories = []

        def write(uri):
            path = self.scratch_path(uri)
            directories.append(path.parent)
            path.mkdir()
            (path / "part-00000.parquet").touch()

        def merge(_statement):
            self.assertTrue((directories[-1] / "actions" / "part-00000.parquet").is_file())
            if len(directories) == 1:
                raise error

        self.staged.write.parquet.side_effect = write
        self.spark.sql.side_effect = merge
        self.selected.count.side_effect = [1, 1]
        with self.assertRaises(RuntimeError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertFalse(directories[0].exists())
        self.assertEqual(self.upsert(), 1)
        self.assertEqual(len(set(directories)), 2)
        self.assertTrue(all(not path.exists() for path in directories))
        self.assertEqual(self.spark.table.call_args_list, [call(self.table_name)] * 2)
        self.assertEqual(self.step_names(), ["count", "stage", "merge", "unpersist"] * 2)

    def test_lost_acknowledgement_with_visible_successor_is_noop_on_retry(self):
        # Script a timeout after commit, then a fresh comparison finding identical tracked fields.
        error = TimeoutError("synthetic lost MERGE acknowledgement")
        self.spark.sql.side_effect = error
        self.selected.count.side_effect = [1, 0]
        with self.assertRaises(TimeoutError) as raised:
            self.upsert()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.upsert(), 0)
        self.assertEqual(self.step_names(),
                         ["count", "stage", "merge", "unpersist", "count", "unpersist"])


if __name__ == "__main__":
    unittest.main()
