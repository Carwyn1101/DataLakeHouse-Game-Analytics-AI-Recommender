"""Synthetic checks for the local Bronze preflight CLI."""

import ast
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from collectors.contracts import encode_csv
from tools.bronze_preflight import NULL_MARKERS, REQUIRED_COLUMNS, main, preflight


class PreflightTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="bronze-preflight-fixture-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source"
        self.target = self.root / "target"
        self.source.mkdir()
        self.target.mkdir()

    def write_csv(self, directory, name, *, columns=None, rows=None):
        columns = sorted(REQUIRED_COLUMNS[name]) if columns is None else columns
        (directory / name).write_bytes(encode_csv(columns, rows or []))

    def write_valid(self, directory, reverse=False):
        for name, required in REQUIRED_COLUMNS.items():
            columns = sorted(required, reverse=reverse)
            row = dict.fromkeys(columns, "")
            row.update(steam_appid="0007")
            row["ts" if name == "popularity.csv" else "ts_ingested"] = "2026-01-02T07:00:00.123456789+07:00"
            if name == "games.csv":
                row["tags"] = "Action, Co-op"
                row["name"] = 'Synthetic, "quoted"\nmultiline'
            self.write_csv(directory, name, columns=columns, rows=[row])

    def test_conforming_source_only_report_labels_target_unchecked(self):
        self.write_valid(self.source)
        report = preflight(self.source)
        self.assertEqual(report["status"], "PASS")
        self.assertFalse(report["target_checked"])
        self.assertNotIn("schema_comparison", report)
        self.assertTrue(all(item["rows"] == 1 for item in report["source"]["datasets"].values()))

    def test_reordered_headers_and_matching_extras_pass_without_writes(self):
        self.write_valid(self.source)
        self.write_valid(self.target, reverse=True)
        for directory in (self.source, self.target):
            columns = sorted(REQUIRED_COLUMNS["reviews.csv"]) + ["synthetic_extra"]
            self.write_csv(directory, "reviews.csv", columns=columns)
        before = {path: path.read_bytes() for directory in (self.source, self.target)
                  for path in directory.iterdir()}
        report = preflight(self.source, self.target)
        self.assertEqual(report["status"], "PASS")
        self.assertTrue(report["target_checked"])
        self.assertTrue(all(item["compared"] for item in report["schema_comparison"].values()))
        self.assertEqual({path: path.read_bytes() for path in before}, before)
        self.assertEqual(set(self.source.iterdir()) | set(self.target.iterdir()), set(before))

    def test_schema_mismatch_reports_counts_only(self):
        self.write_valid(self.source)
        self.write_valid(self.target)
        columns = sorted(REQUIRED_COLUMNS["reviews.csv"]) + ["fixture-private-column"]
        self.write_csv(self.target, "reviews.csv", columns=columns)
        report = preflight(self.source, self.target)
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["schema_comparison"]["reviews.csv"], {
            "status": "FAIL", "compared": True, "source_only_columns": 0, "target_only_columns": 1,
        })
        self.assertNotIn("fixture-private-column", json.dumps(report))

    def test_auxiliary_entries_are_counted_without_opening_them(self):
        self.write_valid(self.source)
        for name in ("game_tags.csv", "private-extra.csv", "private-notes.txt"):
            (self.source / name).write_bytes(b"ignored")
        (self.source / "private-directory").mkdir()
        real_open = Path.open

        def read_expected(path, *args, **kwargs):
            self.assertIn(path.name, REQUIRED_COLUMNS)
            self.assertEqual(args[0], "r")
            return real_open(path, *args, **kwargs)

        with patch.object(Path, "open", read_expected):
            report = preflight(self.source)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["source"]["inventory"], {
            "directory_errors": 0, "tags_sidecars": 1, "other_csv": 1,
            "other_files": 1, "non_regular_entries": 1,
        })
        self.assertNotIn("private-", json.dumps(report))

    def test_missing_duplicate_and_incomplete_headers_fail(self):
        (self.source / "games.csv").write_bytes(b"")
        (self.source / "reviews.csv").write_bytes(b"steam_appid,steam_appid,ts_ingested\n")
        (self.target / "games.csv").write_bytes(b"steam_appid,ts_ingested\n")
        report = preflight(self.source, self.target)
        source = report["source"]["datasets"]
        self.assertEqual(source["games.csv"]["missing_header"], 1)
        self.assertEqual(source["reviews.csv"]["duplicate_headers"], 1)
        self.assertEqual(source["popularity.csv"]["missing_file"], 1)
        self.assertGreater(report["target"]["datasets"]["games.csv"]["missing_required_columns"], 0)
        self.assertEqual(report["schema_comparison"]["games.csv"], {"status": "FAIL", "compared": False})

    def test_invalid_ids_timestamps_tags_and_duplicate_counts(self):
        self.write_valid(self.source)
        self.write_valid(self.target)
        rows = [
            ("007", "", "2026-01-02"),
            ("7", "NULL", "01/02/2026"),
            ("07", " , ", "2026-02-30"),
            ("", "Good", ""),
            ("1.5", "Good", "2026-01-02T03:04:05+0199"),
            ("9223372036854775808", "Good", "9999-01-02"),
            ("11", "Good", "2262-04-11T23:47:16.854775807Z"),
            ("12", "Good", "2262-04-11T23:47:16.854775808Z"),
        ]
        self.write_csv(self.target, "games.csv", rows=[
            dict(steam_appid=key, tags=tags, ts_ingested=ts) for key, tags, ts in rows
        ])
        scan = preflight(self.source, self.target)["target"]["datasets"]["games.csv"]
        self.assertEqual(scan["status"], "FAIL")
        self.assertEqual(scan["rows"], 8)
        self.assertEqual(scan["empty_tags"], 3)
        self.assertEqual(scan["missing_appids"], 1)
        self.assertEqual(scan["invalid_appids"], 2)
        self.assertEqual(scan["duplicate_appids"], 1)
        self.assertEqual(scan["duplicate_appid_rows"], 2)
        self.assertEqual(scan["missing_timestamps"], 1)
        self.assertEqual(scan["non_iso_timestamps"], 2)
        self.assertEqual(scan["invalid_timestamps"], 3)

    def test_source_duplicate_observations_and_tags_do_not_fail_bronze_screen(self):
        self.write_valid(self.source)
        self.write_valid(self.target)
        rows = [
            dict(steam_appid="7.0", ts_ingested="2026-01-02", tags="Action"),
            dict(steam_appid="7e0", ts_ingested="2026-01-03", tags=""),
        ]
        self.write_csv(self.source, "games.csv", rows=rows)
        source = preflight(self.source, self.target)
        games = source["source"]["datasets"]["games.csv"]
        self.assertEqual(source["status"], "PASS")
        self.assertEqual(games["duplicate_appids"], 1)
        self.assertEqual(games["invalid_appids"], 0)
        self.assertEqual(games["tags_quality_status"], "FAIL")
        self.write_csv(self.target, "games.csv", rows=rows)
        target = preflight(self.source, self.target)
        self.assertEqual(target["target"]["datasets"]["games.csv"]["status"], "FAIL")

    def test_width_decode_and_quoting_errors_fail_without_values(self):
        self.write_valid(self.source)
        games = self.source / "games.csv"
        games.write_bytes(games.read_bytes() + b"broken,row\n")
        (self.source / "reviews.csv").write_bytes(b"\xff")
        pop = self.source / "popularity.csv"
        pop.write_bytes(pop.read_bytes() + b'"synthetic-unterminated')
        report = preflight(self.source)
        datasets = report["source"]["datasets"]
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(datasets["games.csv"]["bad_width_rows"], 1)
        self.assertEqual(datasets["reviews.csv"]["parse_errors"], 1)
        self.assertEqual(datasets["popularity.csv"]["parse_errors"], 1)
        self.assertNotIn("synthetic-unterminated", json.dumps(report))

    def test_cli_errors_and_unreadable_inputs_do_not_emit_paths(self):
        for args, key in ((["--unknown-private-value"], "argument_errors"),
                          (["--source-dir", str(self.root / "private-path")], "source")):
            with self.subTest(key=key):
                out, err = io.StringIO(), io.StringIO()
                with redirect_stdout(out), redirect_stderr(err):
                    self.assertEqual(main(args), 1)
                self.assertEqual(err.getvalue(), "")
                report = json.loads(out.getvalue())
                self.assertEqual(report["status"], "FAIL")
                self.assertIn(key, report)
                self.assertNotIn("private", out.getvalue())

    def test_contract_columns_and_null_markers_match_bronze_source(self):
        root = Path(__file__).resolve().parents[2]
        script = root / "LakeHouse_Project" / "scripts" / "rawData_to_bronze.py"
        tree = ast.parse(script.read_text(encoding="utf-8"))
        constants = {node.targets[0].id: ast.literal_eval(node.value)
                     for node in tree.body if isinstance(node, ast.Assign)
                     and isinstance(node.targets[0], ast.Name)
                     and node.targets[0].id in {"_SILVER_COLUMNS", "_LEGACY_NULL_MARKERS"}}
        for name, columns in constants["_SILVER_COLUMNS"].items():
            timestamp = "ts" if name == "popularity.csv" else "ts_ingested"
            self.assertEqual(REQUIRED_COLUMNS[name], columns | {"steam_appid", timestamp})
        self.assertEqual(NULL_MARKERS, constants["_LEGACY_NULL_MARKERS"] | {""})


if __name__ == "__main__":
    unittest.main()
