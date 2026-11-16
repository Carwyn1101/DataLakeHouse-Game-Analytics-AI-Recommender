"""Offline checks of the two Gold fact refresh entrypoints and their MERGE boundary."""

from collections import Counter
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, call
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname

from test_characterization import load_functions


FACTS = (("review", "reviews", "ts_ingested"), ("popularity", "popularity", "ts"))


class GoldFactRefreshTests(unittest.TestCase):
    def setup_writer(self, entity, silver, stamp, *, exists=True):
        self.name = f"fact_{entity}_snapshot"
        self.target = f"hive.gold.{self.name}"
        self.columns = ["game_key", "date_key", f"{entity}_key", "snapshot_ts"]
        self.spark = Mock()
        self.fact = Mock(columns=self.columns)
        raw = Mock()
        active = raw.filter.return_value.select.return_value
        game, date, detail = Mock(), Mock(), Mock()
        self.spark.table.side_effect = {
            f"hive.silver.{silver}": raw, "hive.gold.dim_game": game, "hive.gold.dim_date": date,
            f"hive.gold.dim_{entity}_info": detail,
        }.__getitem__
        final_projection = (active.join.return_value.join.return_value.drop.return_value
                            .withColumn.return_value.join.return_value.drop.return_value.select)
        final_projection.return_value = self.fact
        self.raw, self.active, self.final_projection = raw, active, final_projection
        self.col = MagicMock()
        self.source_rows = Mock(return_value=4)
        self.spark.read.parquet.return_value.count = self.source_rows
        self.paths = []
        self.events = Mock()
        self.events.attach_mock(self.fact.write.parquet, "stage")
        self.events.attach_mock(self.spark.read.parquet, "read")
        self.events.attach_mock(self.source_rows, "count")
        self.events.attach_mock(self.spark.sql, "merge")
        self.events.attach_mock(self.spark.stop, "stop")
        self.fact.write.parquet.side_effect = self.write_stage
        self.spark.sql.side_effect = self.merge
        self.table_exists = Mock(return_value=exists)
        self.get_session = Mock(return_value=self.spark)
        fn = f"create_{self.name}"
        return load_functions("silver_to_gold_etl.py", {fn, "_merge_fact_refresh"}, {
            "get_spark_session": self.get_session, "logger": Mock(), "col": self.col,
            "date_format": Mock(), "table_exists": self.table_exists, "Config": Mock(GOLD_PATH="synthetic"),
            "Path": Path, "tempfile": tempfile,
        })[fn]

    def write_stage(self, uri):
        parts = urlsplit(uri)
        self.assertEqual(parts.scheme, "file")
        path = Path(url2pathname(unquote(parts.path)))
        self.assertTrue(path.parent.name.startswith("gold-" + self.name + "-"))
        self.paths.append(path.parent)
        path.mkdir()
        (path / "part-00000.parquet").touch()

    def merge(self, sql):
        self.assertTrue((self.paths[-1] / "rows" / "part-00000.parquet").is_file())
        uri = self.fact.write.parquet.call_args.args[0]
        self.assertEqual(self.spark.read.parquet.call_args, call(uri))
        columns = ", ".join(self.columns)
        values = ", ".join(f"s.{column}" for column in self.columns)
        quote = chr(96)
        self.assertEqual(" ".join(sql.split()), (
            f"MERGE INTO {self.target} AS t USING ( "
            "SELECT *, explode(IF(game_key IS NOT NULL AND __fact_rank = 1, array(false, true), array(false))) "
            "AS __fact_delete FROM ( SELECT *, row_number() OVER (PARTITION BY game_key ORDER BY game_key) "
            f"AS __fact_rank FROM parquet.{quote}{uri}{quote} ) AS ranked ) AS s "
            "ON s.__fact_delete AND t.game_key = s.game_key WHEN MATCHED THEN DELETE "
            "WHEN NOT MATCHED AND NOT s.__fact_delete THEN "
            f"INSERT ({columns}) VALUES ({values})"
        ))

    def test_incremental_reads_frozen_payload_and_mutates_target_once(self):
        for case in FACTS:
            with self.subTest(entity=case[0]):
                run = self.setup_writer(*case)
                run()
                self.table_exists.assert_called_once_with(self.spark, self.target)
                self.get_session.assert_called_once_with(self.name)
                self.raw.filter.return_value.select.assert_called_once_with("steam_appid", case[2])
                self.final_projection.assert_called_once_with(
                    *self.columns[:-1], self.col.return_value.alias.return_value
                )
                self.assertEqual([item[0] for item in self.events.mock_calls],
                                 ["stage", "read", "count", "merge", "stop"])
                self.fact.writeTo.assert_not_called()
                self.fact.count.assert_not_called()
                self.fact.select.assert_not_called()
                self.assertTrue(all(not path.exists() for path in self.paths))

    def test_initial_load_still_uses_create_or_replace(self):
        for case in FACTS:
            with self.subTest(entity=case[0]):
                run = self.setup_writer(*case, exists=False)
                self.fact.count.return_value = 4
                run()
                self.fact.writeTo.assert_called_once_with(self.target)
                writer = self.fact.writeTo.return_value.using.return_value.tableProperty
                writer.assert_called_once_with("location", "synthetic/" + self.name)
                final = writer.return_value.tableProperty.return_value.tableProperty.return_value
                final.createOrReplace.assert_called_once_with()
                self.spark.sql.assert_not_called()
                self.fact.write.parquet.assert_not_called()
                self.spark.stop.assert_called_once_with()

    def test_empty_source_skips_target_write(self):
        for case in FACTS:
            with self.subTest(entity=case[0]):
                run = self.setup_writer(*case)
                self.source_rows.return_value = 0
                run()
                self.spark.sql.assert_not_called()
                self.fact.writeTo.assert_not_called()
                self.spark.stop.assert_called_once_with()
                self.assertTrue(all(not path.exists() for path in self.paths))

    def test_stage_and_merge_errors_propagate_and_cleanup(self):
        for case in FACTS:
            for failure_point in ("stage", "count", "merge"):
                with self.subTest(entity=case[0], failure_point=failure_point):
                    run = self.setup_writer(*case)
                    error = OSError("synthetic failure")
                    if failure_point == "stage":
                        def fail_stage(uri):
                            self.write_stage(uri)
                            raise error
                        self.fact.write.parquet.side_effect = fail_stage
                    elif failure_point == "count":
                        self.source_rows.side_effect = error
                    else:
                        def fail_merge(sql):
                            self.merge(sql)
                            raise error
                        self.spark.sql.side_effect = fail_merge
                    with self.assertRaises(OSError) as failure:
                        run()
                    self.assertIs(failure.exception, error)
                    self.assertEqual(self.spark.sql.call_count, int(failure_point == "merge"))
                    self.spark.stop.assert_called_once_with()
                    self.assertTrue(all(not path.exists() for path in self.paths))


    def test_retry_after_precommit_error_and_lost_acknowledgement(self):
        for case in FACTS:
            with self.subTest(entity=case[0]):
                run = self.setup_writer(*case)
                original = ((1, 20200101, 8, "old"), (2, 20200101, 10, "target-only"))
                payload = ((1, 20200102, 8, "new"), (3, None, 11, "new"))
                committed = Counter(original)

                def simulated_commit(sql):
                    nonlocal committed
                    self.merge(sql)
                    committed = apply_actions(committed.elements(), payload)

                def before_commit(sql):
                    self.merge(sql)
                    raise OSError("synthetic precommit error")

                def lost_ack(sql):
                    simulated_commit(sql)
                    raise TimeoutError("synthetic lost acknowledgement")

                self.spark.sql.side_effect = before_commit
                with self.assertRaisesRegex(OSError, "precommit"):
                    run()
                self.assertEqual(committed, Counter(original))
                self.spark.sql.side_effect = lost_ack
                with self.assertRaisesRegex(TimeoutError, "acknowledgement"):
                    run()
                expected = Counter((*original[1:], *payload))
                self.assertEqual(committed, expected)
                self.spark.sql.side_effect = simulated_commit
                run()
                self.assertEqual(committed, expected)
                self.assertEqual(len(set(self.paths)), 3)
                self.assertTrue(all(not path.exists() for path in self.paths))
                self.assertEqual(self.spark.stop.call_count, 3)


def apply_actions(target, payload):
    """Represent the intended MERGE result as a bag, with ordinary SQL key matching."""
    delete_markers = {row[0] for row in payload if row[0] is not None}
    return Counter(row for row in target if row[0] not in delete_markers) + Counter(payload)


class FactMultisetOracleTests(unittest.TestCase):
    def test_preservation_and_frozen_replay(self):
        old = ((1, 20200101, 8, "old"), (1, 20200101, 9, "old"),
               (2, 20200101, 10, "untouched"), (None, None, None, "legacy"))
        payload = ((1, 20200102, 8, "new"), (1, 20200102, 8, "new"),
                   (1, None, 9, "new"), (3, 20200102, None, "new"))
        expected = Counter((*old[2:], *payload))
        self.assertEqual(apply_actions(old, ()), Counter(old))
        self.assertEqual(apply_actions(old, payload), expected)
        self.assertEqual(apply_actions(expected.elements(), payload), expected)

    def test_null_key_payload_accumulates_on_replay(self):
        legacy_null = (None, 20200101, 8, "legacy")
        incoming_null = (None, None, None, "new")
        initial = (legacy_null,)
        committed = apply_actions(initial, (incoming_null,))
        self.assertEqual(committed, Counter({legacy_null: 1, incoming_null: 1}))
        self.assertEqual(apply_actions(committed.elements(), (incoming_null,)),
                         Counter({legacy_null: 1, incoming_null: 2}))
