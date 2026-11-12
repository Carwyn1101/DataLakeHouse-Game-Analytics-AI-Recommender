"""Offline checks of the Bronze authorization and fail-closed execution order."""

from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

import run_native_bronze as bronze

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "native-0123456789abcdef"
MINIO_CONTEXT = Path("/tmp/bronze-test/minio-fixture")


def models(minio_context=MINIO_CONTEXT):
    network = f"{PROJECT}_isolated"
    services = {
        name: {
            "networks": {"isolated": {"ipv4_address": address}},
            "volumes": [{"type": "volume", "source": volume, "target": target}],
        }
        for name, (volume, target, address) in bronze.FIXTURES.items()
    }
    services["fixture-minio"]["image"] = bronze.REVIEWED_MINIO_IMAGE
    services["runner"] = {
        "networks": {"isolated": {"ipv4_address": "172.30.233.4"}},
        "environment": {"NATIVE_ALLOW_WRITES": "0"},
        "profiles": ["native"],
    }
    base = {
        "name": PROJECT,
        "services": services,
        "networks": {"isolated": {"name": network, "internal": True, "ipam": {
            "config": [{"subnet": "172.30.233.0/28"}],
        }}},
        "volumes": {name: {"name": f"{PROJECT}_{name}"} for name in ("objects", "metastore")},
    }
    effective = deepcopy(base)
    for name, (cpu, memory) in bronze.LIMITS.items():
        effective["services"][name].update(cpus=str(cpu), mem_limit=str(memory), logging=bronze.LOGGING)
    effective["services"]["fixture-minio"].update(bronze.minio_build(PROJECT, minio_context))
    return base, effective


class BronzePilotTests(unittest.TestCase):
    def test_gate_requires_an_owner_label_event_on_pr15(self):
        path = ROOT / ".github/workflows/native-bronze.yml"
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        self.assertEqual(workflow["on"], {"pull_request": {"types": ["labeled"]}})
        self.assertEqual(workflow["permissions"], {"contents": "read"})
        job = workflow["jobs"]["bronze"]
        self.assertEqual(job["runs-on"], "ubuntu-24.04")
        self.assertEqual(job["env"]["NATIVE_ALLOW_WRITES"], "0")
        conditions = set(part.strip() for part in job["if"].split("&&"))
        self.assertTrue({
            "github.event.label.name == 'native-bronze-approved'",
            "github.actor == github.repository_owner",
            "github.triggering_actor == github.repository_owner",
            "github.event.pull_request.head.repo.full_name == github.repository",
            "github.event.pull_request.number == 15",
            "github.run_attempt == '1'",
        } <= conditions)
        self.assertNotIn("native-pilot-smoke-approved", job["if"])
        self.assertEqual(job["steps"][1]["with"]["ref"], bronze.HARNESS)
        for step in job["steps"][:2]:
            self.assertFalse(step["with"]["persist-credentials"])

    def test_compose_overlay_preserves_reviewed_topology(self):
        base, effective = models()
        bronze.verify_models(base, effective, PROJECT, MINIO_CONTEXT)

    def test_compose_rejects_host_mount_or_changed_resource_overlay(self):
        base, effective = models()
        base["services"]["runner"]["volumes"] = [{"type": "bind", "source": "/host", "target": "/mnt"}]
        with self.assertRaisesRegex(bronze.PilotFailure, "mount"):
            bronze.verify_models(base, effective, PROJECT, MINIO_CONTEXT)
        base, effective = models()
        effective["services"]["runner"]["environment"]["NATIVE_ALLOW_WRITES"] = "1"
        with self.assertRaisesRegex(bronze.PilotFailure, "Overlay changed"):
            bronze.verify_models(base, effective, PROJECT, MINIO_CONTEXT)

    def test_compose_rejects_an_unapproved_minio_image_or_build_context(self):
        for field, value in (
            ("image", "unknown.example/minio:latest"),
            ("build", {"context": "/unverified", "dockerfile": bronze.MINIO_DOCKERFILE}),
        ):
            with self.subTest(field=field):
                base, effective = models()
                effective["services"]["fixture-minio"][field] = value
                with self.assertRaisesRegex(bronze.PilotFailure, "MinIO fixture build or image"):
                    bronze.verify_models(base, effective, PROJECT, MINIO_CONTEXT)

    def test_minio_fixture_pins_verified_inputs_and_entrypoint(self):
        dockerfile = (ROOT / "tests/ci" / bronze.MINIO_DOCKERFILE).read_text(encoding="utf-8")
        self.assertIn("FROM registry.access.redhat.com/ubi9/ubi-micro@sha256:"
                      "c55b3d2c48728e4a4ebc3f63a716e5ba135a5f44097667eaea8ccd3f8404fa66", dockerfile)
        self.assertIn("ADD --checksum=sha256:"
                      "eef6581f6509f43ece007a6f2eb4c5e3ce41498c8956e919a7ac7b4b170fa431 --chmod=0755 "
                      "https://github.com/minio/minio/releases/download/RELEASE.2025-07-23T15-54-02Z/"
                      "minio.linux-amd64.RELEASE.2025-07-23T15-54-02Z /usr/bin/minio", dockerfile)
        self.assertIn('VOLUME ["/data"]', dockerfile)
        self.assertIn('ENTRYPOINT ["/usr/bin/minio"]', dockerfile)
        self.assertIn('CMD ["server", "/data", "--address", ":9000"]', dockerfile)
        self.assertNotRegex(dockerfile, r"(?m)^(RUN|COPY)\s")

    def test_freshness_rejects_existing_subnet_overlap(self):
        pilot = bronze.BronzePilot()
        with (patch.object(pilot, "resources", return_value=([], [], [])),
              patch.object(pilot, "docker", side_effect=[
                  b"id", b'[{"IPAM":{"Config":[{"Subnet":"172.30.0.0/16"}]}}]',
              ]), patch.object(pilot, "command", return_value=b"[]")):
            with self.assertRaisesRegex(bronze.PilotFailure, "overlaps"):
                pilot.verify_fresh()

    def test_isolation_failure_stops_before_write_opt_in(self):
        pilot = bronze.BronzePilot()
        pilot.project = PROJECT
        with tempfile.TemporaryDirectory() as folder:
            base, effective = models(Path(folder) / "minio-fixture")
            with (patch.object(pilot, "check_host"), patch.object(pilot, "prepare"),
                  patch.object(pilot, "verify_fresh"),
                  patch.object(pilot, "docker", return_value=b"sha256:" + b"a" * 64),
                  patch.object(pilot, "compose", side_effect=[
                      json.dumps(base).encode(), json.dumps(effective).encode(), b"", b"", b"",
                  ]) as compose,
                  patch.object(pilot, "verify_running", side_effect=bronze.PilotFailure("unexpected fixture mount"))):
                with self.assertRaisesRegex(bronze.PilotFailure, "unexpected fixture mount"):
                    pilot.run(Path(folder))
        self.assertTrue(pilot.started)
        self.assertEqual(pilot.invocation, "NOT EXECUTED")
        self.assertFalse(any("NATIVE_ALLOW_WRITES=1" in call.args for call in compose.call_args_list))

    def test_success_invokes_only_the_single_bronze_runner_after_isolation(self):
        pilot = bronze.BronzePilot()
        pilot.project = PROJECT
        with tempfile.TemporaryDirectory() as folder:
            base, effective = models(Path(folder) / "minio-fixture")
            responses = [json.dumps(base).encode(), json.dumps(effective).encode(), b"", b"", b"",
                         b"Bronze final object SHA-256: " + b"a" * 64 + b"\nScenario completed: bronze\n"]
            with (patch.object(pilot, "check_host"), patch.object(pilot, "prepare"),
                  patch.object(pilot, "verify_fresh"),
                  patch.object(pilot, "docker", return_value=b"sha256:" + b"a" * 64),
                  patch.object(pilot, "verify_running") as isolation,
                  patch.object(pilot, "compose", side_effect=responses) as compose,
                  redirect_stdout(io.StringIO())):
                pilot.run(Path(folder))
        isolation.assert_called_once()
        self.assertEqual(sum("build" in call.args and call.args[-1] == "fixture-minio"
                             for call in compose.call_args_list), 1)
        self.assertFalse(any("pull" in call.args for call in compose.call_args_list))
        writes = [call for call in compose.call_args_list if "NATIVE_ALLOW_WRITES=1" in call.args]
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0].args[-1], "runner")
        self.assertIn("--no-deps", writes[0].args)
        self.assertEqual(writes[0].kwargs["timeout"], 300)
        self.assertEqual(pilot.invocation, "PASS")

    def test_runtime_rejects_a_minio_container_from_another_image(self):
        pilot = bronze.BronzePilot()
        pilot.project = PROJECT
        pilot.minio_image_id = "sha256:" + "a" * 64
        network_name = f"{PROJECT}_isolated"
        containers = [
            {
                "Id": "minio",
                "Config": {"Labels": {bronze.LABEL + "service": "fixture-minio"}},
                "State": {"Running": True},
                "HostConfig": {"Privileged": False, "Memory": bronze.GIB, "NanoCpus": 500_000_000},
                "NetworkSettings": {"Networks": {network_name: {"IPAddress": "172.30.233.2"}}},
                "Mounts": [{"Type": "volume", "Name": f"{PROJECT}_objects", "Destination": "/data"}],
                "Image": "sha256:" + "b" * 64,
            },
            {"Id": "hms", "Config": {"Labels": {bronze.LABEL + "service": "fixture-hms"}}},
        ]
        network = {
            "Internal": True, "Driver": "bridge", "EnableIPv6": False,
            "IPAM": {"Config": [{"Subnet": str(bronze.SUBNET)}]},
            "Containers": {"minio": {}, "hms": {}},
        }
        with (patch.object(pilot, "resources", return_value=(containers, [{}, {}], [network])),
              patch.object(pilot, "verify_ownership")):
            with self.assertRaisesRegex(bronze.PilotFailure, "differs from the verified build"):
                pilot.verify_running()

    def test_cleanup_refuses_unowned_resource(self):
        pilot = bronze.BronzePilot()
        pilot.project = PROJECT
        pilot.started = True
        foreign = {"Name": f"{PROJECT}_objects", "Driver": "local", "Labels": None}
        with patch.object(pilot, "resources", return_value=([], [foreign], [])), patch.object(pilot, "docker") as docker:
            with self.assertRaisesRegex(bronze.PilotFailure, "ownership"):
                pilot.cleanup()
            docker.assert_not_called()

    def test_cleanup_refuses_an_extra_project_labeled_network(self):
        pilot = bronze.BronzePilot()
        pilot.project = PROJECT
        pilot.started = True
        extra = {
            "Name": f"{PROJECT}_extra",
            "Labels": {bronze.LABEL + "project": PROJECT, bronze.LABEL + "network": "isolated"},
            "Containers": {},
        }
        with patch.object(pilot, "resources", return_value=([], [], [extra])), patch.object(pilot, "docker") as docker:
            with self.assertRaisesRegex(bronze.PilotFailure, "network ownership"):
                pilot.cleanup()
            docker.assert_not_called()

    def test_diagnostics_redact_fixture_values_and_workflow_commands(self):
        pilot = bronze.BronzePilot()
        pilot.redactions = ["sample-test-user", "sample-generated-password"]
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            pilot.diagnostic("::error:: sample-test-user sample-generated-password ##[ \x1b")
        output = buffer.getvalue()
        self.assertIn("[redacted]", output)
        for text in (*pilot.redactions, "::", "##[", "\x1b"):
            self.assertNotIn(text, output)


if __name__ == "__main__":
    unittest.main()
