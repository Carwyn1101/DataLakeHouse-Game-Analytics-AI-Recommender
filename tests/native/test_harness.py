"""Offline validation for staging, isolation rules and synthetic fixtures."""

import ast
import csv
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from xml.etree import ElementTree

import fixtures
import prepare
import runtime
import scenarios


class StaticHarnessTests(unittest.TestCase):
    def test_exact_candidate_staging_and_tamper_detection(self):
        with tempfile.TemporaryDirectory(prefix="harness-static-") as directory:
            with patch.object(prepare, "temp_root", return_value=Path(directory).resolve()):
                for candidate in sorted(prepare.CANDIDATES):
                    with self.subTest(candidate=candidate):
                        run = prepare.prepare(candidate, "silver")
                        prepare.verify(run)
                        manifest = json.loads((run / "runner/run.json").read_bytes())
                        self.assertEqual(manifest["candidate"], candidate)
                        self.assertEqual("bronze_appid.py" in manifest["scripts"], candidate == prepare.BRONZE)
                        for name in manifest["scripts"]:
                            source = (run / "runner/scripts" / name).read_text(encoding="utf-8")
                            self.assertNotIn("detect_and_upload", source)
                            self.assertNotIn("/opt/airflow/", source)
                            ast.parse(source)
                        injected = run / "runner/.env"
                        injected.write_bytes(b"unexpected")
                        with self.assertRaises(ValueError):
                            prepare.verify(run)

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
        self.assertEqual(set(prepare.SCENARIOS), set(scenarios.SCENARIOS))
        with self.assertRaises(ValueError):
            prepare.expected_files("main", "bronze", "native-0123456789abcdef", "a" * 48)

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
