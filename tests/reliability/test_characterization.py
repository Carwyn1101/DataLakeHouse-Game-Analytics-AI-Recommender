"""Offline characterization of existing behavior, including known defects.

Run: python -B tests/reliability/test_characterization.py -v
No ETL module imports, services, credentials, fixture files, or production writes.
Selected function bodies are compiled from the actual source, not reimplemented.
Passing assertions about undesirable behavior are evidence, not acceptance tests.
"""

import ast
import io
import logging
import os
from pathlib import Path
import unittest
from unittest.mock import MagicMock, Mock, call, patch

try:
    import pandas as pd
except ImportError:
    pd = None


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "LakeHouse_Project" / "scripts"


def load_functions(filename, names, namespace):
    """Exclude module imports/configuration and execute only requested functions."""
    path = SCRIPTS / filename
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    selected = [node for node in tree.body
                if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in selected} != set(names):
        raise AssertionError("Requested production functions were not found")
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


class Response(io.BytesIO):
    def release_conn(self):
        pass


class MemoryMinio:
    def __init__(self, target=None):
        self.content = None if target is None else target.to_csv(index=False).encode()
        self.fail_read = False
        self.writes = 0

    def get_object(self, *_args):
        if self.fail_read:
            raise TimeoutError("synthetic read failure")
        if self.content is None:
            raise FileNotFoundError("synthetic absent object")
        return Response(self.content)

    def put_object(self, _bucket, _object_name, stream, **_kwargs):
        self.content = stream.read()
        self.writes += 1


@unittest.skipIf(pd is None, "pandas is required for Bronze characterization")
class BronzeCharacterization(unittest.TestCase):
    def rows(self, ids=(101, 202), ts="2026-01-01", name="original"):
        return pd.DataFrame({"steam_appid": list(ids), "name": name, "ts_ingested": ts})

    def run_upsert(self, source, client):
        namespace = load_functions("rawData_to_bronze.py", {
            "_detect_ts_column", "_expected_ts_column", "_normalize_ts",
            "_build_content_diff_mask", "upsert_to_minio",
        }, {"pd": pd, "io": io, "os": os, "logging": logging,
            "Minio": lambda *_args, **_kwargs: client})
        original_read_csv = pd.read_csv

        def read_csv(value, *args, **kwargs):
            if isinstance(value, str) and value == "synthetic.csv":
                return source.copy()
            return original_read_csv(value, *args, **kwargs)

        with patch.object(pd, "read_csv", side_effect=read_csv):
            namespace["upsert_to_minio"]("synthetic.csv", "synthetic", "bronze/games.csv")
        return original_read_csv(io.BytesIO(client.content))

    def test_initial_load_deduplicates_to_latest_valid_timestamp(self):
        source = pd.concat([self.rows((101,), "2026-01-02", "new"),
                            self.rows((101,), "2026-01-01", "old"), self.rows((202,))])
        result = self.run_upsert(source, MemoryMinio())
        self.assertEqual(len(result), 2)
        self.assertEqual(result.set_index("steam_appid").loc[101, "name"], "new")

    def test_incremental_insert_preserves_target_only_keys(self):
        result = self.run_upsert(self.rows((303,)), MemoryMinio(self.rows()))
        self.assertEqual(set(result.steam_appid), {101, 202, 303})

    def test_same_timestamp_content_change_is_accepted(self):
        result = self.run_upsert(self.rows((101,), name="changed"), MemoryMinio(self.rows()))
        self.assertEqual(result.set_index("steam_appid").loc[101, "name"], "changed")
        self.assertEqual(len(result), 2)

    def test_unchanged_rerun_has_stable_rows_but_still_writes(self):
        client = MemoryMinio()
        first = self.run_upsert(self.rows(), client)
        second = self.run_upsert(self.rows(), client)
        pd.testing.assert_frame_equal(first, second)
        self.assertEqual(client.writes, 2)

    def test_older_timestamp_can_replace_newer_content_known_defect(self):
        result = self.run_upsert(self.rows((101,), "2026-01-01", "old"),
                                 MemoryMinio(self.rows((101,), "2026-01-02", "new")))
        self.assertEqual(result.iloc[0]["name"], "old")
        self.assertTrue(result.iloc[0]["ts_ingested"].startswith("2026-01-01"))

    def test_read_error_can_drop_target_only_key_known_defect(self):
        client = MemoryMinio(self.rows())
        client.fail_read = True
        result = self.run_upsert(self.rows((101,)), client)
        self.assertEqual(set(result.steam_appid), {101})
        self.assertEqual(client.writes, 1)

    def test_invalid_timestamp_wins_over_valid_duplicate_known_defect(self):
        source = pd.concat([self.rows((101,), "2026-01-02", "valid"),
                            self.rows((101,), None, "invalid")])
        result = self.run_upsert(source, MemoryMinio())
        self.assertEqual(result.iloc[0]["name"], "invalid")

    def test_unchanged_legacy_target_duplicates_are_retained(self):
        target = pd.concat([self.rows((101,)), self.rows((101,))])
        result = self.run_upsert(self.rows((101,)), MemoryMinio(target))
        self.assertEqual(len(result), 2)


class SilverCatalogSafety(unittest.TestCase):
    tables = ("hive.silver.games", "hive.silver.popularity", "hive.silver.reviews")

    def setUp(self):
        self.spark = Mock()
        self.manager = self.spark._jsparkSession.sessionState().catalogManager()
        self.catalog = self.manager.catalog.return_value
        self.spark.sparkContext._gateway.new_array.side_effect = lambda _kind, size: [None] * size
        identifier = self.spark._jvm.org.apache.spark.sql.connector.catalog.Identifier
        identifier.of.side_effect = lambda namespace, name: (tuple(namespace), name)

        self.spark.read.option.return_value = self.spark.read
        raw = MagicMock()
        self.spark.read.csv.return_value.toPandas.return_value = raw
        self.spark.table.return_value.count.return_value = 1
        self.sdf = self.spark.read.parquet.return_value
        self.init = Mock()
        self.init.count.return_value = 1
        self.writer = self.init.writeTo.return_value
        self.writer.using.return_value = self.writer
        self.writer.tableProperty.return_value = self.writer
        self.change_cols = {"games": ["name_cleaned"], "popularity": ["owners_midpoint"],
                            "reviews": ["steam_review_total"]}
        self.upsert = Mock(return_value=0)
        self.add_scd2_columns = Mock(return_value=self.init)

        namespace = load_functions("bronze_to_silver_etl.py", {
            "_table_exists", "bronze_to_silver_etl",
        }, {
            "logging": Mock(),
            "get_spark_session": Mock(return_value=self.spark),
            "tempfile": Mock(mkdtemp=Mock(return_value="synthetic-temp")),
            "shutil": Mock(),
            "Config": Mock(BRONZE_PATH="synthetic-bronze", SILVER_PATH="synthetic-silver",
                           SCD2_CHANGE_COLS=self.change_cols),
            "np": Mock(),
            "preprocess_games": Mock(return_value=(raw, [])),
            "preprocess_reviews": Mock(return_value=raw),
            "preprocess_popularity": Mock(return_value=raw),
            "_add_scd2_columns": self.add_scd2_columns,
            "_upsert_scd2": self.upsert,
        })
        self.run_silver = namespace["bronze_to_silver_etl"]

    def test_existing_tables_take_the_scd2_upsert_path(self):
        self.catalog.tableExists.return_value = True
        self.run_silver()
        self.assertEqual(self.manager.catalog.call_args_list, [call("hive")] * 3)
        self.assertEqual(self.catalog.tableExists.call_args_list,
                         [call((("silver",), name.split(".")[-1])) for name in self.tables])
        self.assertEqual(self.upsert.call_args_list, [
            call(self.spark, self.sdf, table, "steam_appid", self.change_cols[table.split(".")[-1]])
            for table in self.tables
        ])
        self.add_scd2_columns.assert_not_called()
        self.init.writeTo.assert_not_called()

    def test_verified_absence_takes_the_create_only_path(self):
        self.catalog.tableExists.return_value = False
        self.run_silver()
        self.upsert.assert_not_called()
        self.assertEqual(self.add_scd2_columns.call_args_list, [call(self.sdf)] * 3)
        self.assertEqual(self.init.writeTo.call_args_list, [call(name) for name in self.tables])
        self.assertEqual(self.writer.using.call_args_list, [call("iceberg")] * 3)
        self.assertEqual(self.writer.tableProperty.call_args_list, [
            call("write.format.default", "parquet"), call("format-version", "2"),
        ] * 3)
        self.assertEqual(self.writer.create.call_count, 3)
        self.writer.createOrReplace.assert_not_called()

    def test_catalog_failures_propagate_without_reaching_table_writes(self):
        for error in (ConnectionError("synthetic outage"), PermissionError("synthetic denial"),
                      RuntimeError("unexpected catalog error")):
            with self.subTest(error=type(error).__name__):
                self.catalog.tableExists.side_effect = error
                with self.assertRaises(type(error)) as raised:
                    self.run_silver()
                self.assertIs(raised.exception, error)
                self.add_scd2_columns.assert_not_called()
                self.init.writeTo.assert_not_called()
                self.sdf.writeTo.assert_not_called()
                self.upsert.assert_not_called()

    def test_catalog_resolution_failure_propagates_without_a_write(self):
        error = RuntimeError("synthetic catalog resolution failure")
        self.manager.catalog.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            self.run_silver()
        self.assertIs(raised.exception, error)
        self.catalog.tableExists.assert_not_called()
        self.init.writeTo.assert_not_called()
        self.upsert.assert_not_called()

    def test_creation_collision_propagates_without_replacing_the_table(self):
        self.catalog.tableExists.return_value = False
        error = RuntimeError("synthetic table already exists")
        self.writer.create.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            self.run_silver()
        self.assertIs(raised.exception, error)
        self.init.writeTo.assert_called_once_with(self.tables[0])
        self.writer.create.assert_called_once_with()
        self.writer.createOrReplace.assert_not_called()
        self.writer.replace.assert_not_called()
        self.upsert.assert_not_called()


class ErrorHandlingCharacterization(unittest.TestCase):
    def test_gold_helpers_treat_service_failure_as_absence_and_zero(self):
        spark = Mock()
        spark.table.side_effect = ConnectionError("synthetic catalog outage")
        ns = load_functions("silver_to_gold_etl.py", {"table_exists", "get_max_key"}, {})
        self.assertFalse(ns["table_exists"](spark, "synthetic.table"))
        self.assertEqual(ns["get_max_key"](spark, "synthetic.table", "key"), 0)

    def test_gold_verifier_returns_normally_when_all_reads_fail(self):
        spark = Mock()
        spark.table.side_effect = ConnectionError("synthetic catalog outage")
        spark.sql.side_effect = ConnectionError("synthetic query failure")
        ns = load_functions("silver_to_gold_etl.py", {"verify_gold_layer"}, {
            "get_spark_session": lambda *_args: spark, "logger": Mock(),
        })
        results = ns["verify_gold_layer"]()
        self.assertEqual(len(results), 18)
        self.assertTrue(all(value is None for value in results.values()))
        spark.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
