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
from unittest.mock import Mock, patch

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


class ErrorHandlingCharacterization(unittest.TestCase):
    def test_silver_table_probe_treats_service_failure_as_absence(self):
        spark = Mock()
        spark.table.side_effect = ConnectionError("synthetic catalog outage")
        ns = load_functions("bronze_to_silver_etl.py", {"_table_exists"}, {})
        self.assertFalse(ns["_table_exists"](spark, "synthetic.table"))

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
