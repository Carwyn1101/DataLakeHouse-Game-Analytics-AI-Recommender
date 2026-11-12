"""Offline Bronze regression tests and Silver/Gold characterization.

Run: python -B tests/reliability/test_characterization.py -v
Bronze uses a stub MinIO SDK and in-memory CSV. Silver and Gold are AST-selected.
No services, persisted data, or real credentials are used.
"""

import ast
import csv
import io
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, call, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from collectors.contracts import (
    ENRICHED_GAME_COLUMNS, GAME_COLUMNS, POPULARITY_COLUMNS, REVIEW_COLUMNS, TAG_COLUMNS,
    encode_csv, enrich_games,
)

try:
    import pandas as pd
except ImportError:
    pd = None


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


class S3Error(Exception):
    """Stand-in for the MinIO SDK exception and its code attribute."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


class Response(io.BytesIO):
    def __init__(self, content, read_error=None, close_error=None):
        super().__init__(content)
        self.read_error = read_error
        self.close_error = close_error
        self.released = False

    def read(self, *args):
        if self.read_error:
            raise self.read_error
        return super().read(*args)

    def read1(self, *args):
        if self.read_error:
            raise self.read_error
        return super().read1(*args)

    def stream(self, amt):
        yield from iter(lambda: self.read(amt), b"")

    def close(self):
        super().close()
        if self.close_error:
            raise self.close_error

    def release_conn(self):
        self.released = True


class ResponseClosingAtEOF(Response):
    """Emulate an SDK response whose file closes when it is drained."""

    def stream(self, amt):
        yield from super().stream(amt)
        self.close()

    def read1(self, *args):
        data = super().read1(*args)
        if not data:
            self.close()
        return data


class MemoryMinio:
    response_type = Response

    def __init__(self, content=None):
        self.content = content
        self.get_error = None
        self.read_error = None
        self.close_error = None
        self.write_failure = None
        self.response = None
        self.attempts = 0
        self.writes = 0

    def get_object(self, *, bucket_name, object_name):
        if self.get_error:
            raise self.get_error
        if self.content is None:
            raise S3Error("NoSuchKey")
        self.response = self.response_type(self.content, self.read_error, self.close_error)
        return self.response

    def put_object(self, *, bucket_name, object_name, data, length, content_type):
        self.attempts += 1
        if self.write_failure == "before_commit":
            raise TimeoutError("synthetic write failure")
        payload = data.read()
        if len(payload) != length or content_type != "application/csv":
            raise AssertionError("Unexpected put_object payload contract")
        self.content = payload
        self.writes += 1
        if self.write_failure == "lost_acknowledgement":
            raise TimeoutError("synthetic lost acknowledgement")


@unittest.skipIf(pd is None, "pandas is required for Bronze regression tests")
class BronzeUpsertTests(unittest.TestCase):
    header = "steam_appid,name,ts_ingested"
    old = "101,original,2026-01-02"
    other = "202,other,2026-01-02"
    newer = "101,updated,2026-01-03"

    def csv(self, *rows):
        return "\n".join((self.header, *rows)) + "\n"

    def load_bronze(self, client):
        sdk = ModuleType("minio")
        sdk.Minio = Mock(return_value=client)
        errors = ModuleType("minio.error")
        errors.S3Error = S3Error
        spec = spec_from_file_location(
            "LakeHouse_Project.scripts.bronze_under_test", SCRIPTS / "rawData_to_bronze.py"
        )
        bronze = module_from_spec(spec)
        with patch.dict(sys.modules, {"minio": sdk, "minio.error": errors}):
            spec.loader.exec_module(bronze)
        return bronze

    def run_upsert(self, source, client, name="synthetic.csv"):
        bronze = self.load_bronze(client)
        with patch.object(bronze, "open", return_value=io.StringIO(source, newline=""), create=True):
            bronze.upsert_to_minio("synthetic.csv", "synthetic", f"bronze/{name}")

    def assert_rows(self, client, expected):
        csv_rows = pd.read_csv(io.BytesIO(client.content))
        self.assertEqual(dict(zip(csv_rows.steam_appid, csv_rows.name)), expected)
        self.assertEqual(len(csv_rows), len(expected))

    def assert_no_write(self, client, content):
        self.assertEqual(client.content, content)
        self.assertEqual(client.attempts, 0)

    def test_missing_object_initial_load_deduplicates_latest(self):
        client = MemoryMinio()
        self.run_upsert(self.csv(self.newer, self.old, self.newer, self.other), client)
        self.assert_rows(client, {101: "updated", 202: "other"})
        self.assertEqual(client.writes, 1)

    def test_successful_read_updates_inserts_and_preserves_target_only_keys(self):
        client = MemoryMinio(self.csv(self.old, self.other).encode())
        self.run_upsert(self.csv(self.newer, "303,new,2026-01-02"), client)
        self.assert_rows(client, {101: "updated", 202: "other", 303: "new"})
        self.assertTrue(client.response.closed)
        self.assertTrue(client.response.released)

    def test_response_that_closes_at_eof_can_be_updated(self):
        client = MemoryMinio(self.csv(self.old, self.other).encode())
        client.response_type = ResponseClosingAtEOF
        self.run_upsert(self.csv(self.newer), client)
        self.assert_rows(client, {101: "updated", 202: "other"})
        self.assertTrue(client.response.closed)
        self.assertTrue(client.response.released)

    def test_stream_error_after_parseable_prefix_does_not_write(self):
        class TruncatedResponse(Response):
            def stream(self, amt):
                yield from super().stream(amt)
                raise ConnectionError("synthetic incomplete body")

        content = self.csv(self.old, self.other).encode()
        client = MemoryMinio(content)
        client.response_type = TruncatedResponse
        with self.assertRaises(ConnectionError):
            self.run_upsert(self.csv(self.newer), client)
        self.assert_no_write(client, content)
        self.assertTrue(client.response.closed)
        self.assertTrue(client.response.released)

    def test_legacy_null_markers_do_not_change_review_numeric_types(self):
        common = {
            "steam_review_total_positive": 9, "steam_review_total_negative": 1,
            "steam_review_total": 10, "steam_review_score_desc": "Positive",
            "steam_review_positive_ratio": 0.9, "ts_ingested": "2026-01-02",
        }
        source = encode_csv(REVIEW_COLUMNS, [
            dict(common, steam_appid=101, steam_review_total="NULL", steam_review_positive_ratio="N/A"),
            dict(common, steam_appid=202),
        ]).decode("utf-8")
        client = MemoryMinio()
        self.run_upsert(source, client, "reviews.csv")
        rows = list(csv.DictReader(io.StringIO(client.content.decode("utf-8"), newline="")))
        self.assertEqual(rows[0]["steam_review_total"], "")
        self.assertEqual(rows[0]["steam_review_positive_ratio"], "")

        namespace = load_functions("bronze_to_silver_etl.py", {
            "preprocess_reviews", "remove_duplicates", "ensure_numeric",
        }, {"pd": pd})
        frame = pd.read_csv(io.BytesIO(client.content), keep_default_na=False, na_values=[""])
        result = namespace["preprocess_reviews"](frame)
        self.assertEqual(result.loc[0, "steam_review_total"], 0)
        self.assertEqual(result.loc[1, "steam_review_positive_ratio"], 0.9)

    def test_header_only_target_can_accept_inserts(self):
        client = MemoryMinio(self.csv().encode())
        self.run_upsert(self.csv(self.newer), client)
        self.assert_rows(client, {101: "updated"})

    def test_empty_source_does_not_write(self):
        for source in ("", self.csv()):
            with self.subTest(source=source):
                client = MemoryMinio()
                self.run_upsert(source, client)
                self.assert_no_write(client, None)

    def test_non_missing_get_errors_propagate(self):
        existing = self.csv(self.old, self.other).encode()
        for error in (TimeoutError("timeout"), FileNotFoundError("unverified"),
                      S3Error("AccessDenied"), S3Error("InvalidAccessKeyId"),
                      S3Error("SignatureDoesNotMatch"), S3Error("NoSuchBucket")):
            with self.subTest(error=error):
                client = MemoryMinio(existing)
                client.get_error = error
                with self.assertRaises(type(error)) as raised:
                    self.run_upsert(self.csv(self.newer), client)
                self.assertIs(raised.exception, error)
                self.assert_no_write(client, existing)

    def test_response_read_errors_release_connection_and_abort(self):
        existing = self.csv(self.old, self.other).encode()
        for error in (ConnectionError("stream failed"), S3Error("NoSuchKey")):
            with self.subTest(error=error):
                client = MemoryMinio(existing)
                client.read_error = error
                with self.assertRaises(type(error)):
                    self.run_upsert(self.csv(self.newer), client)
                self.assertTrue(client.response.closed)
                self.assertTrue(client.response.released)
                self.assert_no_write(client, existing)

    def test_close_failure_still_releases_connection(self):
        existing = self.csv(self.old).encode()
        client = MemoryMinio(existing)
        client.close_error = OSError("close failed")
        with self.assertRaises(OSError):
            self.run_upsert(self.csv(self.newer), client)
        self.assertTrue(client.response.released)
        self.assert_no_write(client, existing)

    def test_invalid_target_never_replaced(self):
        for content in (b"", b"\xff", self.csv('101,"unterminated').encode(),
                        self.csv("101,short").encode(), self.csv("101,long,2026-01-02,extra").encode(),
                        b"name,ts_ingested\nmissing_key,2026-01-02\n",
                        b"steam_appid,name,ts\n101,wrong_timestamp_column,2026-01-02\n",
                        b"steam_appid,name,name,ts_ingested\n101,a,b,2026-01-02\n",
                        self.csv("101,bad,not-a-date").encode(),
                        self.csv("0,bad,2026-01-02").encode(),
                        self.csv(self.old, self.old).encode(),
                        self.csv(self.old, "00101,older,2026-01-01").encode()):
            with self.subTest(content=content):
                client = MemoryMinio(content)
                with self.assertRaises(ValueError):
                    self.run_upsert(self.csv(self.newer), client)
                self.assertTrue(client.response.closed)
                self.assertTrue(client.response.released)
                self.assert_no_write(client, content)

    def test_invalid_source_never_writes_even_with_valid_rows(self):
        for source in ("name,ts_ingested\nmissing_key,2026-01-02\n",
                       "steam_appid,name,ts\n101,wrong_timestamp_column,2026-01-02\n",
                       self.csv(self.newer, "101,invalid,"), self.csv(self.newer, "101,bad,2026-02-30"),
                       self.csv("101,bad,2026-01-02T00:00:00+0199"),
                       self.csv("1.5,bad,2026-01-02"), self.csv(",bad,2026-01-02"),
                       self.csv("9223372036854775808,bad,2026-01-02")):
            with self.subTest(source=source):
                client = MemoryMinio()
                with self.assertRaises(ValueError):
                    self.run_upsert(source, client)
                self.assert_no_write(client, None)

    def test_source_latest_tie_conflict_rejected_in_both_orders(self):
        conflict = "00101,conflict,2026-01-02T07:00:00+07:00"
        for rows in ((self.old, conflict), (conflict, self.old)):
            with self.subTest(rows=rows):
                client = MemoryMinio()
                with self.assertRaisesRegex(ValueError, "conflicting rows"):
                    self.run_upsert(self.csv(*rows), client)
                self.assert_no_write(client, None)

    def test_exact_legacy_appid_notation_updates_the_same_key(self):
        existing = self.csv("101.0,old,2026-01-02", self.other).encode()
        client = MemoryMinio(existing)
        source = self.csv("00101,older,2026-01-01", "1.01e2,newer,2026-01-03")
        self.run_upsert(source, client)
        self.assert_rows(client, {101: "newer", 202: "other"})
        rows = list(csv.DictReader(io.StringIO(client.content.decode("utf-8"), newline="")))
        self.assertEqual(rows[0]["steam_appid"], "101")

    def test_exact_large_appid_does_not_round_to_a_neighbor(self):
        client = MemoryMinio(self.csv("9007199254740992,original,2026-01-02").encode())
        self.run_upsert(self.csv("9.007199254740993e15,distinct,2026-01-03"), client)
        self.assert_rows(client, {9007199254740992: "original", 9007199254740993: "distinct"})

    def test_intrinsically_invalid_appid_aborts_without_dropping_a_row(self):
        existing = self.csv(self.old).encode()
        for key in ("0", "-1", "101.000000000000000001", "9.223372036854775808e18", "NaN"):
            with self.subTest(key=key):
                client = MemoryMinio(existing)
                with self.assertRaisesRegex(ValueError, "invalid steam_appid"):
                    self.run_upsert(self.csv(self.newer, f"{key},invalid,2026-01-03"), client)
                self.assert_no_write(client, existing)

    def test_latest_valid_source_wins_over_conflicts_at_older_timestamps(self):
        conflict = "101,conflict,2026-01-02"
        for rows in ((self.old, conflict, self.newer), (self.newer, conflict, self.old)):
            with self.subTest(rows=rows):
                client = MemoryMinio()
                self.run_upsert(self.csv(*rows), client)
                self.assert_rows(client, {101: "updated"})

    def test_utc_conversion_preserves_subsecond_timestamp_order(self):
        client = MemoryMinio(self.csv("101,original,2026-01-02T00:00:00.000000001Z").encode())
        self.run_upsert(self.csv("101,new,2026-01-02T07:00:00.000000002+07:00"), client)
        self.assert_rows(client, {101: "new"})
        self.assertIn(b"2026-01-02 00:00:00.000000002", client.content)

    def test_older_source_cannot_replace_newer_target(self):
        existing = self.csv(self.newer, self.other).encode()
        client = MemoryMinio(existing)
        self.run_upsert(self.csv(self.old), client)
        self.assert_no_write(client, existing)

    def test_equal_time_conflict_keeps_target_while_new_keys_are_written(self):
        client = MemoryMinio(self.csv(self.old).encode())
        with self.assertLogs(level="WARNING") as logs:
            self.run_upsert(self.csv("101,conflict,2026-01-02T07:00:00+07:00", self.other), client)
        self.assert_rows(client, {101: "original", 202: "other"})
        self.assertIn("retained from target=1", logs.output[0])

    def test_unchanged_rerun_preserves_bytes_and_skips_put(self):
        client = MemoryMinio()
        source = self.csv(self.old, self.other)
        self.run_upsert(source, client)
        first = client.content
        self.run_upsert(source, client)
        self.assertEqual(client.content, first)
        self.assertEqual(client.attempts, 1)

    def test_mismatched_headers_fail_before_put(self):
        existing = self.csv(self.old).encode()
        client = MemoryMinio(existing)
        source = "steam_appid,other_column,ts_ingested\n101,updated,2026-01-03\n"
        with self.assertRaisesRegex(ValueError, "headers differ"):
            self.run_upsert(source, client)
        self.assert_no_write(client, existing)

    def test_reordered_headers_preserve_target_order_and_quoted_values(self):
        client = MemoryMinio(self.csv(self.old).encode())
        source = 'ts_ingested,name,steam_appid\n2026-01-03,"Synthetic, ""quote""\nsecond line",101\n'
        self.run_upsert(source, client)
        self.assertTrue(client.content.startswith(self.header.encode()))
        self.assert_rows(client, {101: 'Synthetic, "quote"\nsecond line'})

    def test_write_failure_retries_pending_delta(self):
        existing = self.csv(self.old).encode()
        client = MemoryMinio(existing)
        client.write_failure = "before_commit"
        with self.assertRaises(TimeoutError):
            self.run_upsert(self.csv(self.newer), client)
        self.assertEqual(client.content, existing)
        client.write_failure = None
        self.run_upsert(self.csv(self.newer), client)
        self.assert_rows(client, {101: "updated"})
        self.assertEqual((client.attempts, client.writes), (2, 1))

    def test_lost_acknowledgement_retry_reads_commit_and_skips_put(self):
        client = MemoryMinio(self.csv(self.old).encode())
        client.write_failure = "lost_acknowledgement"
        with self.assertRaises(TimeoutError):
            self.run_upsert(self.csv(self.newer), client)
        client.write_failure = None
        self.run_upsert(self.csv(self.newer), client)
        self.assert_rows(client, {101: "updated"})
        self.assertEqual((client.attempts, client.writes), (1, 1))

    def test_silver_preprocessors_consume_published_csv_contracts(self):
        datasets = (
            ("games", ENRICHED_GAME_COLUMNS,
             (101, "Synthetic Game", "2024-05-03", False, 80, 1999, 999, "USD",
              "Studio", "Publisher", "Action", "Co-op", 0, 5, "Action, Co-op"),
             "name_cleaned", "Synthetic Game"),
            ("reviews", REVIEW_COLUMNS, (101, 9, 1, 10, "Positive", 0.9),
             "steam_review_positive_ratio", 0.9),
            ("popularity", POPULARITY_COLUMNS, (101, 25, "0 .. 20000", 120, 120, 60),
             "playtime_avg_hours", 2),
        )
        transforms = load_functions("bronze_to_silver_etl.py", {
            "clean_text_column", "ensure_numeric", "remove_duplicates", "preprocess_games",
            "preprocess_reviews", "preprocess_popularity",
        }, {"pd": pd, "Config": SimpleNamespace(CURRENT_YEAR=2026, USD_TO_VND=24500)})
        for name, columns, values, result_field, expected in datasets:
            with self.subTest(name=name):
                ts_column = "ts" if name == "popularity" else "ts_ingested"
                fields = dict(zip((c for c in columns if c != ts_column), values))
                fields[ts_column] = "2026-01-02T07:00:00+07:00"
                source = pd.DataFrame([fields], columns=list(columns)).to_csv(index=False)
                client = MemoryMinio()
                self.run_upsert(source, client, name + ".csv")
                raw = pd.read_csv(io.BytesIO(client.content))
                self.assertEqual(tuple(raw.columns), columns)
                self.assertEqual(raw.loc[0, ts_column], "2026-01-02 00:00:00")
                result = transforms["preprocess_" + name](raw)
                frame = result[0] if name == "games" else result
                self.assertEqual(frame.loc[0, result_field], expected)

    def test_collector_games_and_tag_correction_limitations(self):
        """Characterize both blocked corrections and accepted blank tags."""
        game = {
            "steam_appid": 101, "name": 'Synthetic, "Game"', "release_date": "2024-05-03",
            "is_free": False, "steam_metacritic_score": 80, "steam_price_initial": 1999,
            "steam_price_final": 999, "steam_currency": "USD", "developers": "Studio",
            "publishers": "Publisher", "genres": "Action", "categories": "Co-op",
            "required_age": 0, "recommendations_total": 5, "ts_ingested": "2026-01-02T00:00:00Z",
        }
        games = encode_csv(GAME_COLUMNS, [game, dict(game, steam_appid=202, name="Other")])
        tags = encode_csv(TAG_COLUMNS, [
            {"steam_appid": 101, "tags": "Action, Co-op"},
            {"steam_appid": 202, "tags": "Puzzle"},
        ])
        enriched, _ = enrich_games(games, tags)
        client = MemoryMinio()
        source = encode_csv(ENRICHED_GAME_COLUMNS, enriched).decode("utf-8")
        self.run_upsert(source, client, "games.csv")
        original = client.content

        changed = dict(enriched[0], tags="Adventure")
        equal_time = encode_csv(ENRICHED_GAME_COLUMNS, [changed]).decode("utf-8")
        with self.assertLogs(level="WARNING"):
            self.run_upsert(equal_time, client, "games.csv")
        self.assertEqual(client.content, original)
        self.assertEqual(client.attempts, 1)

        blank = dict(enriched[0], tags="", developers="", publishers="   ", ts_ingested="2026-01-03T00:00:00Z")
        newer = encode_csv(ENRICHED_GAME_COLUMNS, [blank]).decode("utf-8")
        self.run_upsert(newer, client, "games.csv")
        self.assertEqual(client.attempts, 2)
        published = list(csv.DictReader(io.StringIO(client.content.decode("utf-8"), newline="")))
        self.assertEqual(published[0]["tags"], "")
        self.assertEqual(published[0]["developers"], "")
        self.assertEqual(published[0]["publishers"], "   ")
        self.assertEqual(published[0]["ts_ingested"], "2026-01-03 00:00:00")
        namespace = load_functions("bronze_to_silver_etl.py", {
            "preprocess_games", "clean_text_column", "ensure_numeric", "remove_duplicates",
        }, {"pd": pd, "Config": SimpleNamespace(CURRENT_YEAR=2026, USD_TO_VND=24500)})
        frame, removed = namespace["preprocess_games"](pd.read_csv(io.BytesIO(client.content)))
        self.assertEqual(removed, [101])
        self.assertEqual(frame["steam_appid"].tolist(), [202])

    def test_scan_publishes_only_supported_object_names(self):
        bronze = self.load_bronze(MemoryMinio())
        bronze.os = SimpleNamespace(
            path=bronze.os.path,
            listdir=Mock(return_value=[
                "game_tags.csv", "games.csv", "README.txt", "reviews.csv",
                "checkpoint", "popularity.csv", "Other.csv",
            ]),
        )
        with patch.object(bronze, "upsert_to_minio") as upsert:
            bronze.detect_and_upload()
        objects = [(Path(args[0]).name, args[1], args[2]) for args, _ in upsert.call_args_list]
        self.assertEqual(objects, [
            ("games.csv", "lakehouse", "bronze/games.csv"),
            ("reviews.csv", "lakehouse", "bronze/reviews.csv"),
            ("popularity.csv", "lakehouse", "bronze/popularity.csv"),
        ])

    def test_overlapping_writers_can_lose_an_insert(self):
        """Characterize the read/modify/write race with a deterministic barrier."""
        client = MemoryMinio(self.csv(self.old).encode())
        commit = client.put_object

        def write_after_second_writer(*args, **kwargs):
            with patch.object(client, "put_object", commit):
                self.run_upsert(self.csv("303,second,2026-01-02"), client)
            self.assert_rows(client, {101: "original", 303: "second"})
            commit(*args, **kwargs)

        with patch.object(client, "put_object", side_effect=write_after_second_writer):
            self.run_upsert(self.csv(self.other), client)
        self.assertEqual(client.writes, 2)
        self.assert_rows(client, {101: "original", 202: "other"})

    def test_games_without_tags_fails_before_initial_upload(self):
        client = MemoryMinio()
        source = self.csv(self.newer)
        with self.assertRaisesRegex(ValueError, "lacks required columns"):
            self.run_upsert(source, client, "games.csv")
        self.assert_no_write(client, None)



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


class GoldCatalogKeySafetyTests(unittest.TestCase):
    table_name = "hive.gold.dim_game"
    key_col = "game_key"

    def setUp(self):
        self.spark = Mock()
        self.manager = self.spark._jsparkSession.sessionState().catalogManager()
        self.catalog = self.manager.catalog.return_value
        self.spark.sparkContext._gateway.new_array.side_effect = lambda _kind, size: [None] * size
        self.identifier = self.spark._jvm.org.apache.spark.sql.connector.catalog.Identifier
        self.identifier.of.side_effect = lambda namespace, name: (tuple(namespace), name)
        self.functions = MagicMock()
        self.gold = Mock()
        self.silver = Mock()
        self.spark.table.side_effect = {
            self.table_name: self.gold, "hive.silver.games": self.silver,
        }.__getitem__
        self.max_query = self.gold.agg.return_value
        self.get_changed_keys = Mock()
        self.gold_functions = load_functions("silver_to_gold_etl.py", {
            "table_exists", "get_max_key", "get_changed_game_keys", "create_dim_date", "create_dim_game",
        }, {
            "F": self.functions,
            "col": self.functions.col,
            "Window": Mock(),
            "row_number": Mock(),
            "logger": Mock(),
            "get_spark_session": Mock(return_value=self.spark),
            "get_changed_keys": self.get_changed_keys,
        })

    def max_key(self):
        return self.gold_functions["get_max_key"](self.spark, self.table_name, self.key_col)

    def test_existing_table_is_checked_in_named_v2_catalog(self):
        self.catalog.tableExists.return_value = True
        self.assertIs(self.gold_functions["table_exists"](self.spark, self.table_name), True)
        self.manager.catalog.assert_called_once_with("hive")
        self.identifier.of.assert_called_once_with(["gold"], "dim_game")
        self.catalog.tableExists.assert_called_once_with((("gold",), "dim_game"))
        self.spark.table.assert_not_called()

    def test_confirmed_missing_table_returns_false(self):
        self.catalog.tableExists.return_value = False
        self.assertIs(self.gold_functions["table_exists"](self.spark, self.table_name), False)

    def test_connection_authorization_and_unexpected_catalog_errors_propagate(self):
        for error in (ConnectionError("synthetic outage"), PermissionError("synthetic denial"),
                      RuntimeError("synthetic unexpected catalog error")):
            with self.subTest(error=type(error).__name__):
                self.catalog.tableExists.side_effect = error
                with self.assertRaises(type(error)) as raised:
                    self.gold_functions["table_exists"](self.spark, self.table_name)
                self.assertIs(raised.exception, error)

    def test_empty_existing_table_returns_zero(self):
        self.max_query.collect.return_value = [(None, 0)]
        self.assertEqual(self.max_key(), 0)
        self.functions.max.assert_called_once_with(self.key_col)
        self.functions.count.assert_called_once_with("*")
        self.gold.agg.assert_called_once_with(self.functions.max.return_value, self.functions.count.return_value)

    def test_nonempty_table_returns_actual_maximum(self):
        self.max_query.collect.return_value = [(137, 5)]
        self.assertEqual(self.max_key(), 137)

    def test_nonempty_table_without_a_non_null_key_is_rejected(self):
        self.max_query.collect.return_value = [(None, 3)]
        with self.assertRaisesRegex(ValueError, "No non-null surrogate keys"):
            self.max_key()

    def test_max_key_lookup_aggregation_and_collect_failures_propagate(self):
        cases = (
            (self.spark.table, FileNotFoundError("synthetic table missing")),
            (self.spark.table, ConnectionError("synthetic lookup outage")),
            (self.gold.agg, ValueError("synthetic unknown column")),
            (self.max_query.collect, PermissionError("synthetic collect denial")),
        )
        for operation, error in cases:
            with self.subTest(operation=operation, error=type(error).__name__):
                previous = operation.side_effect
                operation.side_effect = error
                try:
                    with self.assertRaises(type(error)) as raised:
                        self.max_key()
                    self.assertIs(raised.exception, error)
                finally:
                    operation.side_effect = previous

    def test_dim_date_catalog_failure_prevents_initial_write(self):
        error = ConnectionError("synthetic outage before creation")
        self.catalog.tableExists.side_effect = error
        with self.assertRaises(ConnectionError) as raised:
            self.gold_functions["create_dim_date"]()
        self.assertIs(raised.exception, error)
        self.spark.range.assert_not_called()
        self.spark.sql.assert_not_called()
        self.spark.stop.assert_called_once_with()

    def test_dim_game_max_key_failure_prevents_delete_and_append(self):
        self.catalog.tableExists.return_value = True
        error = RuntimeError("synthetic max collect failure")
        self.max_query.collect.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            self.gold_functions["create_dim_game"]()
        self.assertIs(raised.exception, error)
        self.get_changed_keys.assert_not_called()
        self.spark.sql.assert_not_called()
        self.gold.writeTo.assert_not_called()
        latest = self.silver.filter.return_value.withColumn.return_value.filter.return_value.drop.return_value
        incoming = latest.select.return_value
        incoming.writeTo.assert_not_called()
        self.spark.stop.assert_called_once_with()

    def test_missing_fact_allows_all_games_but_failed_probe_does_not(self):
        self.catalog.tableExists.return_value = False
        dim_game = Mock()
        result = self.gold_functions["get_changed_game_keys"](self.spark, dim_game)
        self.assertIs(result, dim_game.join.return_value.select.return_value)
        self.spark.table.assert_called_once_with("hive.silver.games")
        self.spark.table.reset_mock()
        dim_game.reset_mock()
        self.catalog.tableExists.side_effect = ConnectionError("synthetic fact lookup outage")
        with self.assertRaises(ConnectionError):
            self.gold_functions["get_changed_game_keys"](self.spark, dim_game)
        self.spark.table.assert_not_called()
        dim_game.join.assert_not_called()


class GoldVerificationTests(unittest.TestCase):
    tables = (
        "hive.gold.dim_date", "hive.gold.dim_game", "hive.gold.dim_developer",
        "hive.gold.dim_genre", "hive.gold.dim_publisher", "hive.gold.dim_category",
        "hive.gold.dim_tag", "hive.gold.dim_detail_info", "hive.gold.dim_review_info",
        "hive.gold.dim_popularity_info", "hive.gold.bridge_game_developer",
        "hive.gold.bridge_game_genre", "hive.gold.bridge_game_publisher",
        "hive.gold.bridge_game_category", "hive.gold.bridge_game_tag",
        "hive.gold.fact_game_snapshot", "hive.gold.fact_review_snapshot",
        "hive.gold.fact_popularity_snapshot",
    )
    success_log = "Gold verification passed for %d required tables."

    def setUp(self):
        self.spark = Mock()
        self.logger = Mock()
        self.counts = {table: i for i, table in enumerate(self.tables, start=1)}
        self.frames = {table: Mock(count=Mock(return_value=count)) for table, count in self.counts.items()}
        self.spark.table.side_effect = self.frames.__getitem__
        self.spark.sql.return_value.count.return_value = 3
        ns = load_functions("silver_to_gold_etl.py", {"verify_gold_layer"}, {
            "get_spark_session": Mock(return_value=self.spark), "logger": self.logger,
        })
        self.verify = ns["verify_gold_layer"]

    def assert_required_failure(self, table, error_type):
        with self.assertRaisesRegex(RuntimeError, rf"{table}.*{error_type}") as raised:
            self.verify()
        self.assertTrue(raised.exception.__suppress_context__)
        self.assertNotIn("synthetic-private-detail", str(raised.exception))
        self.assertNotIn("synthetic-private-detail", repr(self.logger.mock_calls))
        self.assertNotIn(call(self.success_log, len(self.tables)), self.logger.info.call_args_list)
        self.spark.sql.assert_not_called()
        self.spark.stop.assert_called_once_with()

    def test_all_required_tables_return_existing_count_mapping(self):
        self.assertEqual(self.verify(), self.counts)
        self.assertEqual(self.spark.table.call_args_list, [call(table) for table in self.tables])
        for frame in self.frames.values():
            frame.count.assert_called_once_with()
        self.spark.sql.return_value.count.assert_called_once_with()
        self.spark.sql.return_value.show.assert_not_called()
        self.logger.info.assert_any_call(self.success_log, len(self.tables))
        self.spark.stop.assert_called_once_with()

    def test_first_table_lookup_failure_raises_and_stops_spark(self):
        self.spark.table.side_effect = ConnectionError("synthetic-private-detail")
        self.assert_required_failure(self.tables[0], "ConnectionError")
        self.spark.table.assert_called_once_with(self.tables[0])

    def test_intermediate_count_failure_raises_without_success(self):
        index = self.tables.index("hive.gold.dim_review_info")
        table = self.tables[index]
        self.frames[table].count.side_effect = PermissionError("synthetic-private-detail")
        self.assert_required_failure(table, "PermissionError")
        self.assertEqual(self.spark.table.call_args_list, [call(name) for name in self.tables[:index + 1]])

    def test_missing_required_table_raises_without_success(self):
        table = "hive.gold.fact_popularity_snapshot"

        def lookup(name):
            if name == table:
                raise FileNotFoundError("synthetic-private-detail")
            return self.frames[name]

        self.spark.table.side_effect = lookup
        self.assert_required_failure(table, "FileNotFoundError")

    def test_empty_input_dependent_tables_keep_zero_counts(self):
        for table in self.tables[1:]:
            self.frames[table].count.return_value = self.counts[table] = 0
        self.assertEqual(self.verify(), self.counts)
        self.spark.stop.assert_called_once_with()

    def test_empty_required_calendar_raises_and_stops_spark(self):
        self.frames[self.tables[0]].count.return_value = 0
        with self.assertRaisesRegex(RuntimeError, r"hive\.gold\.dim_date is empty"):
            self.verify()
        self.spark.table.assert_called_once_with(self.tables[0])
        self.spark.sql.assert_not_called()
        self.spark.stop.assert_called_once_with()

    def test_sample_join_failure_is_diagnostic_only_and_sanitized(self):
        self.spark.sql.return_value.count.side_effect = RuntimeError("synthetic-private-detail")
        self.assertEqual(self.verify(), self.counts)
        self.logger.warning.assert_called_once_with("Gold sample JOIN diagnostic failed (%s).", "RuntimeError")
        self.assertNotIn("synthetic-private-detail", repr(self.logger.mock_calls))
        self.spark.sql.return_value.show.assert_not_called()
        self.spark.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
