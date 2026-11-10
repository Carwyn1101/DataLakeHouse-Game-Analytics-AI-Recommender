"""Offline validation for staging, isolation rules and synthetic fixtures."""

import ast
import csv
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
from xml.etree import ElementTree

import fixtures
import prepare
import runtime
import scenarios


class StaticHarnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # This tests the committed review candidate. Run approval comes from the reviewer.
        cls.harness = prepare.read_git("rev-parse", "HEAD").decode().strip()

    def test_exact_candidate_staging_and_verification(self):
        with tempfile.TemporaryDirectory(prefix="harness-static-") as directory:
            with patch.object(prepare, "temp_root", return_value=Path(directory).resolve()):
                for candidate in sorted(prepare.CANDIDATES):
                    with self.subTest(candidate=candidate):
                        run = prepare.prepare(candidate, "silver", self.harness)
                        prepare.verify(run, self.harness, candidate=candidate, scenario="silver")
                        manifest = json.loads((run / "runner/run.json").read_bytes())
                        self.assertEqual(manifest["candidate"], candidate)
                        self.assertEqual(manifest["harness_revision"], self.harness)
                        self.assertEqual("bronze_appid.py" in manifest["scripts"], candidate == prepare.BRONZE)
                        for name in manifest["scripts"]:
                            source = (run / "runner/scripts" / name).read_text(encoding="utf-8")
                            self.assertNotIn("detect_and_upload", source)
                            self.assertNotIn("/opt/airflow/", source)
                            ast.parse(source)
                        self.assertEqual((run / "runner/runtime.py").read_bytes(),
                                         prepare.read_git("show", f"{self.harness}:tests/native/runtime.py"))

    def test_verify_rejects_unexpected_entries_and_changed_bundle_bytes(self):
        with tempfile.TemporaryDirectory(prefix="harness-tamper-") as directory:
            with patch.object(prepare, "temp_root", return_value=Path(directory).resolve()):
                run = prepare.prepare(prepare.BASE, "silver", self.harness)

                def verify():
                    prepare.verify(run, self.harness, candidate=prepare.BASE, scenario="silver")

                extra = run / "runner/.env"
                extra.write_bytes(b"unexpected")
                with self.assertRaisesRegex(ValueError, "file set"):
                    verify()
                extra.unlink()
                extra.mkdir()
                with self.assertRaisesRegex(ValueError, "file set"):
                    verify()
                extra.rmdir()
                for name in ("runner/runtime.py", "runner/scripts/bronze_to_silver_etl.py",
                             "metastore/core-site.xml", "compose.json"):
                    with self.subTest(name=name):
                        file = run / name
                        original = file.read_bytes()
                        file.write_bytes(original + b"\n")
                        with self.assertRaisesRegex(ValueError, "differs from the reviewed context"):
                            verify()
                        file.write_bytes(original)
                file = run / "runner/run.json"
                original = file.read_bytes()
                for key, altered in (("candidate", prepare.BRONZE), ("scenario", "gold"),
                                     ("harness_revision", "0" * 40)):
                    with self.subTest(key=key):
                        manifest = json.loads(original)
                        manifest[key] = altered
                        file.write_bytes(prepare.json_bytes(manifest))
                        with self.assertRaisesRegex(ValueError, "approved"):
                            verify()
                file.write_bytes(original)
                verify()

    def test_rejects_unreviewed_harness_files_before_packaging_or_verifying(self):
        with tempfile.TemporaryDirectory(prefix="harness-provenance-") as directory:
            root = Path(directory).resolve()
            working = root / "working-harness"
            shutil.copytree(prepare.HERE, working)
            with patch.object(prepare, "HERE", working), patch.object(prepare, "temp_root", return_value=root):
                run = prepare.prepare(prepare.BASE, "silver", self.harness)
                for name in sorted(prepare.HARNESS_FILES):
                    with self.subTest(name=name):
                        file = working / name
                        original = file.read_bytes()
                        file.write_bytes(original + b"\n")
                        with self.assertRaisesRegex(ValueError, "Working harness file differs"):
                            prepare.prepare(prepare.BASE, "silver", self.harness)
                        with self.assertRaisesRegex(ValueError, "Working harness file differs"):
                            prepare.verify(run, self.harness, candidate=prepare.BASE, scenario="silver")
                        file.write_bytes(original)
                (working / "unexpected.py").touch()
                with self.assertRaisesRegex(ValueError, "Working harness file set"):
                    prepare.prepare(prepare.BASE, "silver", self.harness)

    def test_reviewed_storage_paths_resolve_only_to_the_sanitized_bucket(self):
        for candidate in sorted(prepare.CANDIDATES):
            for name, paths in prepare.STORAGE_PATHS.items():
                with self.subTest(candidate=candidate, name=name):
                    tree = ast.parse(prepare.sanitized(name, prepare.git_blob(candidate, name)))
                    config = next(node for node in tree.body if isinstance(node, ast.ClassDef)
                                  and node.name == "Config")
                    fields = {item.targets[0].id: item.value for item in config.body if isinstance(item, ast.Assign)}
                    for key, expected in prepare.CONFIG.items():
                        self.assertEqual(ast.literal_eval(fields[key]), expected)
                    for key, suffix in paths.items():
                        expression = fields[key]
                        self.assertIsInstance(expression, ast.JoinedStr)
                        prefix, bucket, path = expression.values
                        self.assertIsNone(bucket.format_spec)
                        self.assertEqual(bucket.conversion, -1)
                        self.assertEqual(bucket.value.id, "BUCKET")
                        self.assertEqual(prefix.value + fields[bucket.value.id].value + path.value,
                                         f"s3a://unconfigured/{suffix}")

    def test_rejects_unreviewed_storage_and_connection_expressions(self):
        for name in prepare.STORAGE_PATHS:
            tree = ast.parse(prepare.git_blob(prepare.BASE, name))
            config = next(node for node in tree.body if isinstance(node, ast.ClassDef)
                          and node.name == "Config")
            fields = {item.targets[0].id: item for item in config.body if isinstance(item, ast.Assign)}
            for key, expression in (
                ("WAREHOUSE_PATH", 'f"s3a://{BUCKET}/outside"'),
                ("WAREHOUSE_PATH", 'f"s3a://{BUCKET!s}/warehouse"'),
                ("WAREHOUSE_PATH", 'f"s3a://{lookup()}/warehouse"'),
                ("WAREHOUSE_PATH", '"s3a://synthetic/warehouse"'),
                ("MINIO_ENDPOINT", 'f"{host}:9000"'),
                ("HIVE_METASTORE_URI", 'lookup()'),
            ):
                with self.subTest(name=name, key=key, expression=expression):
                    item = fields[key]
                    original = item.value
                    item.value = ast.parse(expression, mode="eval").body
                    with self.assertRaisesRegex(ValueError, "Unexpected (storage path|connection) binding"):
                        prepare.sanitized(name, ast.unparse(tree))
                    item.value = original
            config.body.append(ast.parse("BUCKET = 'unexpected'").body[0])
            with self.assertRaisesRegex(ValueError, "Unreviewed Config binding"):
                prepare.sanitized(name, ast.unparse(tree))

    def test_compose_has_only_private_resources(self):
        model = prepare.compose("native-0123456789abcdef")
        self.assertEqual(set(model["services"]), {"fixture-minio", "fixture-hms", "runner"})
        self.assertEqual(model["volumes"], {"objects": {}, "metastore": {}})
        self.assertEqual(set(model["networks"]), {"isolated"})
        self.assertTrue(model["networks"]["isolated"]["internal"])
        self.assertFalse(model["networks"]["isolated"]["enable_ipv6"])
        for name, service in model["services"].items():
            self.assertEqual(set(service["networks"]), {"isolated"})
            self.assertFalse(set(service) & {
                "ports", "container_name", "privileged", "network_mode", "pid", "devices",
                "extra_hosts", "extends", "volumes_from",
            }, name)
        self.assertEqual(model["services"]["fixture-minio"]["volumes"], ["objects:/data"])
        self.assertEqual(model["services"]["fixture-hms"]["volumes"], ["metastore:/native-meta"])
        self.assertNotIn("volumes", model["services"]["runner"])
        self.assertEqual(model["services"]["runner"]["environment"]["NATIVE_ALLOW_WRITES"], "0")
        self.assertEqual(model["services"]["runner"]["profiles"], ["native"])
        self.assertEqual(model["services"]["runner"]["networks"]["isolated"]["ipv4_address"],
                         runtime.RUNNER_IP)

    def test_metastore_configuration_uses_only_disposable_storage(self):
        def properties(name):
            root = ElementTree.parse(prepare.HERE / name)
            return {row.findtext("name"): row.findtext("value") for row in root.findall("property")}

        hive = properties("hive-site.xml")
        self.assertEqual(hive["javax.jdo.option.ConnectionURL"],
                         "jdbc:derby:;databaseName=/native-meta/metastore_db;create=true")
        self.assertEqual(hive["hive.metastore.warehouse.dir"], "file:///native-meta/warehouse")
        core = properties("core-site.xml")
        self.assertEqual(core["fs.s3a.endpoint"], "http://" + runtime.MINIO_ENDPOINT)
        self.assertEqual(core["fs.s3a.access.key"], "${env.MINIO_ROOT_USER}")
        self.assertEqual(core["fs.s3a.secret.key"], "${env.MINIO_ROOT_PASSWORD}")

    def test_bronze_sanitizer_preserves_logic_and_rejects_drift(self):
        source = """
def upsert():
    client = Minio('example.invalid', access_key='test-access', secret_key='test-secret', secure=False)
    return client
def detect_and_upload():
    pass
"""
        with self.assertRaises(ValueError):
            prepare.sanitized(prepare.MODULES[0], source)
        source += (
            source.split("def detect")[0].replace("upsert", "legacy_a") +
            source.split("def detect")[0].replace("upsert", "legacy_b")
        )
        sanitized = prepare.sanitized(prepare.MODULES[0], source).decode()
        for literal in ("example.invalid", "test-access", "test-secret", "detect_and_upload"):
            self.assertNotIn(literal, sanitized)
        self.assertEqual(sanitized.count("return client"), 3)
        with self.assertRaises(ValueError):
            prepare.sanitized(prepare.MODULES[0], source + "\nleak = 'test-secret'\n")

    def test_verified_candidate_and_scenarios_are_explicit(self):
        self.assertEqual(prepare.SCENARIOS, (
            "bronze", "silver", "silver-catalog", "silver-create-collision",
            "silver-before-update", "silver-append-failure", "silver-lost-ack",
            "gold", "gold-helpers", "gold-partial", "gold-create-collision",
        ))
        self.assertEqual(set(prepare.SCENARIOS), set(scenarios.SCENARIOS))
        self.assertEqual(prepare.CANDIDATES, {
            "5fd358ab12a637b06b175d4da77d339026eac3bf",
            "bcea0e946bb7b199e797943a82cfe82b65a89883",
            "b0f733580c75b561893580a1dd15b7f344602d36",
        })
        with self.assertRaisesRegex(ValueError, "Unreviewed candidate"):
            prepare.expected_files("main", "bronze", "native-0123456789abcdef", "a" * 48, self.harness)
        with self.assertRaisesRegex(ValueError, "full approved harness commit SHA"):
            prepare.reviewed_harness("HEAD")

    def test_fixtures_round_trip_and_match_input_contract(self):
        sizes = {"games": 16, "reviews": 7, "popularity": 7}
        for name, size in sizes.items():
            rows = list(csv.DictReader(io.StringIO(fixtures.csv_bytes(name, {}, {"steam_appid": 202}).decode())))
            self.assertEqual([row["steam_appid"] for row in rows], ["101", "202"])
            self.assertEqual(len(rows[0]), size)
            self.assertIn(fixtures.T0, rows[0].values())
        self.assertEqual(fixtures.F0["games"]["genres"], "Action, RPG")

    def test_missing_write_opt_in_stops_before_loading_manifest(self):
        with patch.dict(runtime.os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "opt-in"):
                runtime.preflight(Path("missing-run"))

    def test_dns_mismatch_stops_before_connecting(self):
        forged = [(None, None, None, None, ("192.0.2.1", 9000))]
        with patch.object(runtime.socket, "getaddrinfo", return_value=forged), \
                patch.object(runtime.socket, "create_connection") as connect:
            with self.assertRaisesRegex(RuntimeError, "DNS"):
                runtime.check_endpoint(runtime.MINIO_HOST, 9000, "172.30.233.2")
            connect.assert_not_called()

    def test_object_guard_blocks_unscoped_names(self):
        objects = runtime.ScopedObjects(None, "native-test")
        objects.check("native-test", "bronze/games.csv")
        with self.assertRaisesRegex(RuntimeError, "Unexpected Bronze object"):
            objects.check("another-bucket", "bronze/games.csv")
        with self.assertRaisesRegex(RuntimeError, "Unexpected Bronze object"):
            objects.check("native-test", "warehouse/anything")


if __name__ == "__main__":
    unittest.main()
