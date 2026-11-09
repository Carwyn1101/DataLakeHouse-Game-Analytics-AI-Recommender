"""Staging and downstream contract checks with no network or pipeline services."""

import ast
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
import warnings

from collectors.contracts import (
    ContractError, ENRICHED_GAME_COLUMNS, GAME_COLUMNS, RAW_COLUMNS, TAG_COLUMNS, encode_csv, enrich_games, read_csv,
)
from collectors.staging import collect_batch, enrich_batch
from test_collectors import GAME, STAMP, TAG_TEXT


LATER = "2026-01-03T00:00:00+00:00"


def observation(key, stamp=STAMP):
    rows = {name: dict.fromkeys(columns) for name, columns in RAW_COLUMNS.items()}
    for row in rows.values():
        row["steam_appid"] = key
    rows["games.csv"] = dict(GAME, steam_appid=key, ts_ingested=stamp)
    rows["reviews.csv"]["ts_ingested"] = stamp
    rows["popularity.csv"]["ts"] = stamp
    rows["game_tags.csv"]["tags"] = TAG_TEXT
    return rows


class StageTests(unittest.TestCase):
    def setUp(self):
        parent = Path(tempfile.gettempdir()).resolve()
        temp = tempfile.TemporaryDirectory(prefix="collector-fixture-", dir=parent)
        self.root = Path(temp.name).resolve()
        self.assertEqual(self.root.parent, parent)
        self.addCleanup(temp.cleanup)
        self.repository = self.root / "repo"
        compose = self.repository / "LakeHouse_Project" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        self.watched = self.root / "thesis" / "data"
        self.watched.mkdir(parents=True)
        self.sentinel = self.watched / "games.csv"
        self.sentinel.write_bytes(b"synthetic sentinel")
        compose.write_text(f"volumes:\n  - {self.watched.as_posix()}:/opt/airflow/source_data\n", encoding="utf-8")
        self.stage = self.root / "isolated"
        self.collector = Mock()
        self.collector.capture.side_effect = lambda key, language: observation(key)

    def tearDown(self):
        self.assertEqual(self.sentinel.read_bytes(), b"synthetic sentinel")
        self.assertEqual(list(self.watched.iterdir()), [self.sentinel])

    def collect(self, ids=(1,), batch="demo"):
        return collect_batch(self.stage, batch, ids, self.collector, repository=self.repository)

    def test_collect_deduplicates_ids_and_replay_is_immutable(self):
        output = self.collect((2, "001", 1, "2"))
        self.assertEqual([c.args[0] for c in self.collector.capture.call_args_list], [1, 2])
        original = {f.name: f.read_bytes() for f in output.iterdir()}
        self.assertEqual(self.collect((1, 2)), output)
        self.assertEqual(self.collector.capture.call_count, 2)
        self.assertEqual({f.name: f.read_bytes() for f in output.iterdir()}, original)
        manifest = json.loads(original["manifest.json"])
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["validation"], {"observations": 2})
        self.assertEqual(set(manifest["files"]), set(RAW_COLUMNS))
        with self.assertRaisesRegex(ContractError, "requested inputs"):
            self.collect((1, 3))

    def test_incremental_batch_preserves_previous_completed_batch(self):
        first = self.collect((1,))
        original = (first / "games.csv").read_bytes()
        self.collector.capture.side_effect = lambda key, language: observation(key, LATER)
        second = self.collect((1, 2), batch="new-observation")
        self.assertEqual((first / "games.csv").read_bytes(), original)
        current = read_csv((second / "games.csv").read_bytes(), GAME_COLUMNS)
        self.assertEqual([(r["steam_appid"], r["ts_ingested"]) for r in current], [("1", LATER), ("2", LATER)])

    def test_resume_after_partial_capture_preserves_checkpoint_timestamp(self):
        self.collector.capture.side_effect = [observation(1), ContractError("Synthetic interruption")]
        with self.assertRaisesRegex(ContractError, "interruption"):
            self.collect((1, 2))
        self.assertFalse((self.stage / "raw" / "demo").exists())
        self.collector.reset_mock()
        self.collector.capture.side_effect = lambda key, language: observation(key, LATER)
        output = self.collect((2, 1, 2))
        self.collector.capture.assert_called_once_with(2, "all")
        games = read_csv((output / "games.csv").read_bytes(), GAME_COLUMNS)
        self.assertEqual([row["ts_ingested"] for row in games], [STAMP, LATER])

    def test_interrupted_directory_commit_retries_without_recapture(self):
        with patch("collectors.staging.os.rename", side_effect=OSError("Synthetic rename failure")):
            with self.assertRaises(OSError):
                self.collect()
        self.assertFalse((self.stage / "raw" / "demo").exists())
        self.assertTrue((self.collect() / "manifest.json").exists())
        self.collector.capture.assert_called_once_with(1, "all")

    def test_invalid_or_unreadable_output_is_not_treated_as_missing(self):
        output = self.collect()
        games = output / "games.csv"
        previous = games.read_bytes()
        real_read = Path.read_bytes

        def unreadable(path):
            if path == games:
                raise PermissionError("Synthetic read error")
            return real_read(path)

        with patch.object(Path, "read_bytes", unreadable), self.assertRaises(PermissionError):
            self.collect()
        self.assertEqual(games.read_bytes(), previous)
        games.write_bytes(b"corrupt synthetic output")
        with self.assertRaisesRegex(ContractError, "integrity verification"):
            self.collect()
        self.assertEqual(games.read_bytes(), b"corrupt synthetic output")
        self.collector.capture.assert_called_once()

    def test_corrupt_checkpoint_is_not_recaptured(self):
        with patch("collectors.staging.os.rename", side_effect=OSError("Synthetic interruption")):
            with self.assertRaises(OSError):
                self.collect()
        checkpoint = self.stage / ".work" / "raw" / "demo" / "1.json"
        checkpoint.write_bytes(b"corrupt synthetic checkpoint")
        with self.assertRaisesRegex(ContractError, "Unreadable staging metadata"):
            self.collect()
        self.collector.capture.assert_called_once()

    def test_airflow_and_repository_paths_rejected_before_capture(self):
        for path in (self.watched, self.watched.parent / "collector", self.repository / "staging", self.root):
            with self.subTest(path=path.name), self.assertRaisesRegex(ContractError, "outside"):
                collect_batch(path, "demo", [1], self.collector, repository=self.repository)
        self.collector.capture.assert_not_called()
        self.assertFalse((self.watched.parent / "collector").exists())
        self.assertFalse((self.repository / "staging").exists())

    def test_enrichment_preserves_inputs_and_completed_output(self):
        raw = self.collect()
        games, tags = raw / "games.csv", raw / "game_tags.csv"
        originals = games.read_bytes(), tags.read_bytes()
        output = enrich_batch(self.stage, "joined", games, tags, repository=self.repository)
        joined = output / "games.csv"
        complete = joined.read_bytes()
        rows = read_csv(complete, ENRICHED_GAME_COLUMNS)
        self.assertEqual(rows[0]["tags"], TAG_TEXT)
        self.assertEqual(rows[0]["ts_ingested"], STAMP)
        self.assertEqual((games.read_bytes(), tags.read_bytes()), originals)
        self.assertEqual(enrich_batch(self.stage, "joined", games, tags, repository=self.repository), output)
        other = self.stage / "other-tags.csv"
        other.write_bytes(encode_csv(TAG_COLUMNS, [dict(steam_appid=1, tags="Different")]))
        with self.assertRaisesRegex(ContractError, "requested inputs"):
            enrich_batch(self.stage, "joined", games, other, repository=self.repository)
        self.assertEqual(joined.read_bytes(), complete)

    def test_interrupted_enrichment_checks_input_identity_before_retry(self):
        raw = self.collect()
        games, tags = raw / "games.csv", raw / "game_tags.csv"
        with patch("collectors.staging.os.rename", side_effect=OSError("Synthetic interruption")):
            with self.assertRaises(OSError):
                enrich_batch(self.stage, "joined", games, tags, repository=self.repository)
        self.assertFalse((self.stage / "enriched" / "joined").exists())
        other = self.stage / "alternative-tags.csv"
        other.write_bytes(encode_csv(TAG_COLUMNS, [dict(steam_appid=1, tags="Different")]))
        with self.assertRaisesRegex(ContractError, "saved batch plan"):
            enrich_batch(self.stage, "joined", games, other, repository=self.repository)
        output = enrich_batch(self.stage, "joined", games, tags, repository=self.repository)
        self.assertEqual(read_csv((output / "games.csv").read_bytes(), ENRICHED_GAME_COLUMNS)[0]["tags"], TAG_TEXT)

    def test_abandoned_temporary_plan_does_not_block_first_observation(self):
        work = self.stage / ".work" / "raw" / "demo"
        work.mkdir(parents=True)
        (work / ".pending-synthetic").write_bytes(b"unfinished plan")
        self.assertTrue((self.collect() / "manifest.json").exists())
        self.collector.capture.assert_called_once_with(1, "all")

    def test_unresolved_enrichment_and_nonstaging_input_do_not_complete(self):
        raw = self.collect()
        games = raw / "games.csv"
        tags = self.stage / "synthetic-empty-tags.csv"
        tags.write_bytes(encode_csv(TAG_COLUMNS, [dict(steam_appid=1, tags="")]))
        before = games.read_bytes(), tags.read_bytes()
        with self.assertRaisesRegex(ContractError, "missing tags"):
            enrich_batch(self.stage, "missing", games, tags, repository=self.repository)
        self.assertEqual((games.read_bytes(), tags.read_bytes()), before)
        self.assertFalse((self.stage / "enriched" / "missing").exists())
        report = json.loads((self.stage / ".work" / "enriched" / "missing" / "validation.json").read_bytes())
        self.assertEqual(report["status"], "rejected")
        self.assertIn("1 observations missing tags", report["reason"])
        with self.assertRaisesRegex(ContractError, "inputs must be inside"):
            enrich_batch(self.stage, "outside", self.sentinel, tags, repository=self.repository)
        self.assertFalse((self.stage / "enriched" / "outside").exists())


class ConsumerContractTests(unittest.TestCase):
    def test_actual_silver_transform_consumes_enriched_contract(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas required for AST-selected Silver contract check")
        project = Path(__file__).resolve().parents[2] / "LakeHouse_Project"
        source = (project / "scripts/bronze_to_silver_etl.py").read_text(encoding="utf-8-sig")
        names = {"preprocess_games", "ensure_numeric", "remove_duplicates", "clean_text_column"}
        selected = [n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name in names]
        env = {"pd": pd, "Config": types.SimpleNamespace(CURRENT_YEAR=2026, USD_TO_VND=24500)}
        exec(compile(ast.Module(body=selected, type_ignores=[]), "synthetic_silver_contract", "exec"), env)
        raw = encode_csv(GAME_COLUMNS, [GAME])
        tags = encode_csv(TAG_COLUMNS, [dict(steam_appid=1, tags=TAG_TEXT)])
        rows, _ = enrich_games(raw, tags)
        frame = pd.read_csv(io.BytesIO(encode_csv(ENRICHED_GAME_COLUMNS, rows)))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            if hasattr(pd.errors, "ChainedAssignmentError"):
                warnings.simplefilter("ignore", pd.errors.ChainedAssignmentError)
            out, removed = env["preprocess_games"](frame)
        self.assertEqual(removed, [])
        self.assertEqual(out.iloc[0]["tags_cleaned"], TAG_TEXT)
        self.assertAlmostEqual(out.iloc[0]["steam_price_final"], 9.99)
        self.assertEqual(out.iloc[0]["steam_appid"], 1)
