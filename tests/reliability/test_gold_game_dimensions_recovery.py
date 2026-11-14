"""Exercise the three Gold dimension refreshes with scripted Spark state."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, call

from test_characterization import load_functions


DIMENSIONS = (
    ("detail", "games", "game_detail_key", (
        "steam_metacritic_score", "steam_price_initial", "steam_price_final",
        "steam_currency", "discount_pct", "recommendations_total",
        "age_category", "price_category", "price_vnd", "price_category_vnd",
    )),
    ("review", "reviews", "review_key", (
        "steam_review_total_positive", "steam_review_total_negative", "steam_review_total",
        "steam_review_positive_ratio", "steam_review_score_desc", "review_volume_category",
        "review_quality_score",
    )),
    ("popularity", "popularity", "popularity_key", (
        "steam_concurrent_players", "owners_low", "owners_high", "owners_midpoint",
        "steamspy_players_2weeks", "playtime_avg_hours", "playtime_median_hours",
        "popularity_category", "engagement_category",
    )),
)


class GoldDimensionRecoveryTests(unittest.TestCase):
    def setup_dimension(self, name, silver_table, surrogate, tracked):
        self.name = f"dim_{name}_info"
        self.table = f"hive.gold.{self.name}"
        self.surrogate = surrogate
        self.columns = (surrogate, "steam_appid", *tracked)
        self.tracked = tracked
        self.spark = Mock()
        self.silver = Mock()
        target = Mock()
        self.spark.table.side_effect = {
            f"hive.silver.{silver_table}": self.silver, self.table: target,
        }.__getitem__
        self.column = MagicMock()
        self.column.return_value.__eq__.return_value = Mock(name="column comparison")
        self.column.return_value.__gt__.return_value = Mock(name="price comparison")
        self.window = Mock()
        self.row_number = MagicMock()
        self.row_number.return_value.over.return_value.__eq__.return_value = Mock(name="first row")
        self.literal = Mock()
        self.literals = {value: MagicMock(name=f"literal({value!r})") for value in ("USD", 0, 20, 21, True)}
        self.literal.side_effect = self.literals.__getitem__
        self.max_key = Mock(return_value=20)
        self.changed_keys = Mock()
        active = self.silver.filter.return_value
        if name == "detail":
            active = active.withColumn.return_value.filter.return_value.drop.return_value
        self.active = active
        self.incoming = active.select.return_value
        self.latest = target.withColumn.return_value.filter.return_value.drop.return_value
        self.key_map = self.latest.select.return_value
        self.new_rows = self.incoming.join.return_value.withColumn.return_value.select.return_value
        self.changed_rows = self.incoming.join.return_value.join.return_value.select.return_value
        self.marked_changes = self.changed_rows.withColumn.return_value
        self.marked_new = self.new_rows.withColumn.return_value
        self.staged = self.marked_changes.withColumn.return_value.unionByName.return_value
        self.group_stage = self.spark.read.parquet.return_value.groupBy
        self.counts = self.group_stage.return_value.count.return_value.collect
        self.counts.return_value = [(True, 1 if name == "detail" else 2), (False, 1)]
        self.logger = Mock()
        self.steps = Mock()
        self.steps.attach_mock(self.staged.write.parquet, "stage")
        self.steps.attach_mock(self.spark.read.parquet, "read_stage")
        self.steps.attach_mock(self.counts, "count_stage")
        self.steps.attach_mock(self.spark.sql, "merge")
        self.steps.attach_mock(self.spark.stop, "stop")
        self.directories = []
        self.staged.write.parquet.side_effect = self.write_stage
        self.spark.sql.side_effect = self.check_merge
        function = f"create_{self.name}"
        return load_functions("silver_to_gold_etl.py", {function, "_merge_dimension_refresh"}, {
            "col": self.column, "Window": self.window, "row_number": self.row_number,
            "when": Mock(), "lit": self.literal, "logger": self.logger, "Path": Path,
            "tempfile": tempfile, "get_spark_session": Mock(return_value=self.spark),
            "table_exists": Mock(return_value=True), "get_max_key": self.max_key,
            "get_changed_keys": self.changed_keys,
        })[function]

    def write_stage(self, uri):
        self.assertRegex(uri, rf"^file:///.*/gold-{self.name}-[^/]+/rows$")
        path = uri.removeprefix("file://")
        destination = Path(path[1:] if path[2:3] == ":" else path)
        self.directories.append(destination.parent)
        destination.mkdir()
        (destination / "part-00000.parquet").touch()

    def check_merge(self, sql):
        self.assertTrue((self.directories[-1] / "rows" / "part-00000.parquet").is_file())
        uri = self.staged.write.parquet.call_args.args[0]
        self.assertEqual(self.spark.read.parquet.call_args, call(uri))
        columns = ", ".join(self.columns)
        values = ", ".join(f"s.{column}" for column in self.columns)
        self.assertEqual(" ".join(sql.split()), (
            f"MERGE INTO {self.table} AS t "
            "USING ( SELECT *, explode(IF(__dimension_delete_marker, array(false, true), array(false))) "
            f"AS __dimension_delete FROM parquet.`{uri}` ) AS s "
            "ON s.__dimension_delete AND t.steam_appid = s.steam_appid "
            "WHEN MATCHED THEN DELETE "
            "WHEN NOT MATCHED AND NOT s.__dimension_delete THEN "
            f"INSERT ({columns}) VALUES ({values})"
        ))

    def test_changed_and_new_rows_keep_selection_and_stage_all_payloads(self):
        for dimension in DIMENSIONS:
            with self.subTest(dimension=dimension[0]):
                run = self.setup_dimension(*dimension)
                run()

                self.max_key.assert_called_once_with(self.spark, self.table, self.surrogate)
                self.silver.filter.assert_called_once_with(self.column.return_value.__eq__.return_value)
                self.column.assert_any_call("is_current")
                source_order = self.window.partitionBy.return_value.orderBy
                expected_orders = [call(self.column.return_value.desc.return_value)]
                if dimension[0] == "detail":
                    self.column.assert_any_call("ts_ingested")
                    expected_orders *= 2
                else:
                    self.silver.filter.return_value.withColumn.assert_not_called()
                self.column.assert_any_call(self.surrogate)
                self.assertEqual(self.window.partitionBy.call_args_list,
                                 [call("steam_appid")] * (len(expected_orders) + 1))
                self.assertEqual(source_order.call_args_list, [*expected_orders, call("steam_appid")])
                self.latest.select.assert_has_calls([
                    call(self.surrogate, "steam_appid"), call("steam_appid", *self.tracked)
                ])
                self.changed_keys.assert_called_once_with(
                    self.incoming, self.latest.select.return_value, "steam_appid", list(self.tracked)
                )
                self.assertEqual(self.incoming.join.call_args_list, [
                    call(self.key_map.select.return_value, "steam_appid", "left_anti"),
                    call(self.changed_keys.return_value, "steam_appid", "inner"),
                ])
                self.incoming.join.return_value.join.assert_called_once_with(self.key_map, "steam_appid")
                self.incoming.join.return_value.join.return_value.select.assert_called_once_with(*self.columns)
                self.window.orderBy.assert_called_once_with("steam_appid")
                rank = self.row_number.return_value.over.return_value
                allocated = self.literals[20].__add__
                allocated.assert_called_once_with(rank)
                allocated.return_value.cast.assert_called_once_with("int")
                self.incoming.join.return_value.withColumn.assert_called_once_with(
                    self.surrogate, allocated.return_value.cast.return_value
                )

                rank.__eq__.assert_called_once_with(1)
                self.changed_rows.withColumn.assert_called_once_with(
                    "__dimension_delete_marker", rank.__eq__.return_value
                )
                self.new_rows.withColumn.assert_called_once_with("__dimension_delete_marker", self.literals[False])
                self.marked_changes.withColumn.assert_called_once_with("__dimension_changed", self.literals[True])
                self.marked_new.withColumn.assert_called_once_with("__dimension_changed", self.literals[False])
                self.marked_changes.withColumn.return_value.unionByName.assert_called_once_with(
                    self.marked_new.withColumn.return_value
                )
                self.group_stage.assert_called_once_with("__dimension_changed")
                self.assertEqual([step[0] for step in self.steps.method_calls],
                                 ["stage", "read_stage", "count_stage", "merge", "stop"])
                self.spark.sql.assert_called_once()
                self.assertFalse(self.directories[0].exists())

    def test_unchanged_rerun_does_not_issue_target_write(self):
        for dimension in DIMENSIONS:
            with self.subTest(dimension=dimension[0]):
                run = self.setup_dimension(*dimension)
                self.counts.return_value = []
                run()
                self.spark.sql.assert_not_called()
                self.logger.info.assert_any_call(f"{self.name}: no changes detected, skipped")
                self.spark.stop.assert_called_once_with()
                self.assertFalse(self.directories[0].exists())

    def test_precommit_failure_propagates_and_leaves_retry_work(self):
        for dimension in DIMENSIONS:
            with self.subTest(dimension=dimension[0]):
                run = self.setup_dimension(*dimension)
                error = RuntimeError("synthetic pre-commit failure")

                def fail(sql):
                    self.check_merge(sql)
                    raise error

                self.spark.sql.side_effect = fail
                with self.assertRaises(RuntimeError) as raised:
                    run()
                self.assertIs(raised.exception, error)
                self.assertFalse(self.directories[0].exists())

                self.spark.sql.side_effect = self.check_merge
                run()
                self.assertEqual(self.max_key.call_args_list, [call(self.spark, self.table, self.surrogate)] * 2)
                self.assertEqual(self.spark.sql.call_count, 2)
                self.assertEqual(self.spark.stop.call_count, 2)
                self.assertTrue(all(not path.exists() for path in self.directories))

    def test_lost_acknowledgement_with_visible_commit_makes_clean_replay_noop(self):
        for dimension in DIMENSIONS:
            with self.subTest(dimension=dimension[0]):
                run = self.setup_dimension(*dimension)
                error = TimeoutError("synthetic lost acknowledgement")

                def lost_ack(sql):
                    self.check_merge(sql)
                    # Script the post-commit state for an unambiguous source batch.
                    self.max_key.return_value = 21
                    self.counts.return_value = []
                    raise error

                self.spark.sql.side_effect = lost_ack
                with self.assertRaises(TimeoutError) as raised:
                    run()
                self.assertIs(raised.exception, error)
                run()
                self.assertEqual(self.spark.sql.call_count, 1)
                self.assertEqual(self.spark.stop.call_count, 2)
                self.assertTrue(all(not path.exists() for path in self.directories))

    def test_scratch_write_failure_prevents_merge(self):
        for dimension in DIMENSIONS:
            with self.subTest(dimension=dimension[0]):
                run = self.setup_dimension(*dimension)
                error = OSError("synthetic scratch failure")

                def fail_write(uri):
                    self.write_stage(uri)
                    raise error

                self.staged.write.parquet.side_effect = fail_write
                with self.assertRaises(OSError) as raised:
                    run()
                self.assertIs(raised.exception, error)
                self.spark.sql.assert_not_called()
                self.spark.stop.assert_called_once_with()
                self.assertFalse(self.directories[0].exists())
