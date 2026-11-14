"""Check dim_game incremental staging and retry control flow with Spark mocks."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, call

from test_characterization import load_functions


class GoldDimGameRecoveryTests(unittest.TestCase):
    table = "hive.gold.dim_game"
    business = ["steam_appid", "name", "release_date", "release_year",
                "game_age_year", "required_age", "is_free"]

    def setUp(self):
        self.spark = Mock()
        self.silver = Mock()
        self.target = Mock()
        self.spark.table.side_effect = {
            "hive.silver.games": self.silver, self.table: self.target,
        }.__getitem__
        self.columns = MagicMock()
        self.window = Mock()
        self.row_number = MagicMock()
        self.literal = MagicMock()
        self.max_key = Mock(return_value=20)
        self.changed_keys = Mock()
        self.incoming = (
            self.silver.filter.return_value.withColumn.return_value
            .filter.return_value.drop.return_value.select.return_value
        )
        self.latest = self.target.withColumn.return_value.filter.return_value.drop.return_value
        self.key_map = self.latest.select.return_value
        self.new_rows = self.incoming.join.return_value.withColumn.return_value.select.return_value
        self.changed_rows = self.incoming.join.return_value.join.return_value.select.return_value
        self.staged = self.changed_rows.withColumn.return_value.unionByName.return_value
        self.counts = self.spark.read.parquet.return_value.groupBy.return_value.count.return_value.collect
        self.counts.return_value = [(True, 1), (False, 1)]
        self.logger = Mock()
        self.steps = Mock()
        self.steps.attach_mock(self.staged.write.parquet, "stage")
        self.steps.attach_mock(self.spark.read.parquet, "read_stage")
        self.steps.attach_mock(self.counts, "count_stage")
        self.steps.attach_mock(self.spark.sql, "merge")
        self.steps.attach_mock(self.spark.stop, "stop")
        self.directories = []
        self.staged.write.parquet.side_effect = self.write_stage
        self.spark.sql.side_effect = self.check_stage

        self.create_dim_game = load_functions("silver_to_gold_etl.py", {"create_dim_game"}, {
            "F": self.columns, "col": self.columns.col, "Window": self.window,
            "row_number": self.row_number, "lit": self.literal,
            "logger": self.logger, "get_spark_session": Mock(return_value=self.spark),
            "table_exists": Mock(return_value=True), "get_max_key": self.max_key,
            "get_changed_keys": self.changed_keys, "Path": Path, "tempfile": tempfile,
        })["create_dim_game"]

    def write_stage(self, uri):
        self.assertRegex(uri, r"^file:///.*/gold-dim-game-[^/]+/rows$")
        path = uri.removeprefix("file://")
        destination = Path(path[1:] if len(path) > 2 and path[2] == ":" else path)
        self.directories.append(destination.parent)
        destination.mkdir()
        (destination / "part-00000.parquet").touch()

    def check_stage(self, sql):
        self.assertTrue((self.directories[-1] / "rows" / "part-00000.parquet").is_file())
        uri = self.staged.write.parquet.call_args.args[0]
        self.assertEqual(self.spark.read.parquet.call_args, call(uri))
        self.assertEqual(" ".join(sql.split()), (
            "MERGE INTO hive.gold.dim_game AS t "
            "USING ( SELECT *, explode(IF(__dim_game_changed, array(false, true), array(false))) "
            f"AS __dim_game_delete FROM parquet.`{uri}` ) AS s "
            "ON s.__dim_game_delete AND t.steam_appid = s.steam_appid "
            "WHEN MATCHED THEN DELETE "
            "WHEN NOT MATCHED AND NOT s.__dim_game_delete THEN "
            "INSERT (game_key, steam_appid, name, release_date, release_year, game_age_year, required_age, is_free) "
            "VALUES (s.game_key, s.steam_appid, s.name, s.release_date, s.release_year, "
            "s.game_age_year, s.required_age, s.is_free)"
        ))

    def test_changed_and_new_rows_keep_legacy_key_and_duplicate_selection(self):
        self.create_dim_game()

        self.max_key.assert_called_once_with(self.spark, self.table, "game_key")
        self.assertEqual(self.window.partitionBy.call_args_list, [call("steam_appid")] * 2)
        self.columns.col.assert_any_call("ts_ingested")
        self.columns.col.assert_any_call("game_key")
        order = self.window.partitionBy.return_value.orderBy
        order.assert_has_calls([call(self.columns.col.return_value.desc_nulls_last.return_value),
                                call(self.columns.col.return_value.desc.return_value)])
        self.latest.select.assert_has_calls([call("game_key", "steam_appid"), call(*self.business)])
        self.changed_keys.assert_called_once_with(
            self.incoming, self.latest.select.return_value, "steam_appid", self.business[1:]
        )
        self.assertEqual(self.incoming.join.call_args_list, [
            call(self.key_map.select.return_value, "steam_appid", "left_anti"),
            call(self.changed_keys.return_value, "steam_appid", "inner"),
        ])
        self.incoming.join.return_value.join.assert_called_once_with(self.key_map, "steam_appid")
        self.incoming.join.return_value.join.return_value.select.assert_called_once_with("game_key", *self.business)
        self.window.orderBy.assert_called_once_with("steam_appid")
        self.assertEqual(self.literal.call_args_list, [call(20), call(True), call(False)])
        allocated = self.literal.return_value.__add__
        allocated.assert_called_once_with(self.row_number.return_value.over.return_value)
        allocated.return_value.cast.assert_called_once_with("int")
        self.incoming.join.return_value.withColumn.assert_called_once_with(
            "game_key", allocated.return_value.cast.return_value,
        )
        self.changed_rows.withColumn.assert_called_once_with("__dim_game_changed", self.literal.return_value)
        self.new_rows.withColumn.assert_called_once_with("__dim_game_changed", self.literal.return_value)
        self.changed_rows.withColumn.return_value.unionByName.assert_called_once_with(
            self.new_rows.withColumn.return_value
        )
        self.assertEqual([step[0] for step in self.steps.method_calls],
                         ["stage", "read_stage", "count_stage", "merge", "stop"])
        self.staged.createTempView.assert_not_called()
        self.staged.createOrReplaceTempView.assert_not_called()
        self.staged.writeTo.assert_not_called()
        self.assertFalse(self.directories[0].exists())

    def test_unchanged_input_issues_no_target_write(self):
        self.counts.return_value = []
        self.create_dim_game()
        self.spark.sql.assert_not_called()
        self.logger.info.assert_any_call("dim_game: no changes detected, skipped")
        self.spark.stop.assert_called_once_with()
        self.assertFalse(self.directories[0].exists())

    def test_failure_before_commit_preserves_retry_plan_and_cleans_scratch(self):
        error = RuntimeError("synthetic pre-commit failure")

        def fail(sql):
            self.check_stage(sql)
            raise error

        self.spark.sql.side_effect = fail
        with self.assertRaises(RuntimeError) as raised:
            self.create_dim_game()
        self.assertIs(raised.exception, error)
        self.assertFalse(self.directories[0].exists())

        # The mock target still has the same key mapping and the retry sees the same work.
        self.spark.sql.side_effect = self.check_stage
        self.create_dim_game()
        self.assertEqual(self.max_key.call_args_list, [call(self.spark, self.table, "game_key")] * 2)
        self.assertEqual(self.spark.sql.call_count, 2)
        self.assertEqual(self.spark.stop.call_count, 2)
        self.assertEqual(len(set(self.directories)), 2)
        self.assertTrue(all(not path.exists() for path in self.directories))

    def test_lost_acknowledgement_and_visible_target_make_replay_noop(self):
        error = TimeoutError("synthetic lost acknowledgement")

        def lost_ack(sql):
            self.check_stage(sql)
            # Script the post-commit target state that the next attempt will see.
            self.max_key.return_value = 21
            self.counts.return_value = []
            raise error

        self.spark.sql.side_effect = lost_ack
        with self.assertRaises(TimeoutError) as raised:
            self.create_dim_game()
        self.assertIs(raised.exception, error)
        self.create_dim_game()
        self.assertEqual(self.spark.sql.call_count, 1)
        self.assertEqual(self.spark.stop.call_count, 2)
        self.assertTrue(all(not path.exists() for path in self.directories))

    def test_scratch_write_failure_prevents_target_merge(self):
        error = OSError("synthetic scratch failure")

        def fail_write(uri):
            self.write_stage(uri)
            raise error

        self.staged.write.parquet.side_effect = fail_write
        with self.assertRaises(OSError) as raised:
            self.create_dim_game()
        self.assertIs(raised.exception, error)
        self.spark.sql.assert_not_called()
        self.spark.stop.assert_called_once_with()
        self.assertFalse(self.directories[0].exists())


if __name__ == "__main__":
    unittest.main()
