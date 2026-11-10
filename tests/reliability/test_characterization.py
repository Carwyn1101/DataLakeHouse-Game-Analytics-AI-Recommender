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
from unittest.mock import Mock, patch

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
        spec = spec_from_file_location("bronze_under_test", SCRIPTS / "rawData_to_bronze.py")
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

        blank = dict(enriched[0], tags="", ts_ingested="2026-01-03T00:00:00Z")
        newer = encode_csv(ENRICHED_GAME_COLUMNS, [blank]).decode("utf-8")
        self.run_upsert(newer, client, "games.csv")
        self.assertEqual(client.attempts, 2)
        namespace = load_functions("bronze_to_silver_etl.py", {
            "preprocess_games", "clean_text_column", "ensure_numeric", "remove_duplicates",
        }, {"pd": pd, "Config": SimpleNamespace(CURRENT_YEAR=2026, USD_TO_VND=24500)})
        frame, removed = namespace["preprocess_games"](pd.read_csv(io.BytesIO(client.content)))
        self.assertEqual(removed, [101])
        self.assertEqual(frame["steam_appid"].tolist(), [202])

    def test_scan_sidecar_can_interrupt_after_a_valid_object_is_written(self):
        """Capture the scan limitation without changing ingestion policy."""
        review = dict.fromkeys(REVIEW_COLUMNS, "")
        review.update(steam_appid=101, ts_ingested="2026-01-02", steam_review_total=10)
        fixtures = {
            "reviews.csv": encode_csv(REVIEW_COLUMNS, [review]).decode("utf-8"),
            "game_tags.csv": "steam_appid,tags\n101,Action\n",
        }
        for entries, writes in ((list(fixtures), 1), (list(reversed(fixtures)), 0)):
            with self.subTest(writes=writes):
                client = MemoryMinio()
                bronze = self.load_bronze(client)
                bronze.os = SimpleNamespace(path=bronze.os.path, listdir=Mock(return_value=entries))

                def source_file(path, **kwargs):
                    return io.StringIO(fixtures[Path(path).name], newline="")

                with patch.object(bronze, "open", side_effect=source_file, create=True):
                    with self.assertRaisesRegex(ValueError, "lacks required columns"):
                        bronze.detect_and_upload()
                self.assertEqual(client.writes, writes)

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
