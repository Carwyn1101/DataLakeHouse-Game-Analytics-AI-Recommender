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


def models():
    network = f"{PROJECT}_isolated"
    services = {
        name: {
            "networks": {"isolated": {"ipv4_address": address}},
            "volumes": [{"type": "volume", "source": volume, "target": target}],
        }
        for name, (volume, target, address) in bronze.FIXTURES.items()
    }
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
        bronze.verify_models(base, effective, PROJECT)

    def test_compose_rejects_host_mount_or_changed_resource_overlay(self):
        base, effective = models()
        base["services"]["runner"]["volumes"] = [{"type": "bind", "source": "/host", "target": "/mnt"}]
        with self.assertRaisesRegex(bronze.PilotFailure, "mount"):
            bronze.verify_models(base, effective, PROJECT)
        base, effective = models()
        effective["services"]["runner"]["environment"]["NATIVE_ALLOW_WRITES"] = "1"
        with self.assertRaisesRegex(bronze.PilotFailure, "overlay"):
            bronze.verify_models(base, effective, PROJECT)

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
        base, effective = models()
        with (tempfile.TemporaryDirectory() as folder, patch.object(pilot, "check_host"),
              patch.object(pilot, "prepare"), patch.object(pilot, "verify_fresh"),
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
        base, effective = models()
        responses = [json.dumps(base).encode(), json.dumps(effective).encode(), b"", b"", b"",
                     b"Bronze final object SHA-256: " + b"a" * 64 + b"\nScenario completed: bronze\n"]
        with (tempfile.TemporaryDirectory() as folder, patch.object(pilot, "check_host"),
              patch.object(pilot, "prepare"), patch.object(pilot, "verify_fresh"),
              patch.object(pilot, "docker", return_value=b"sha256:" + b"a" * 64),
              patch.object(pilot, "verify_running") as isolation,
              patch.object(pilot, "compose", side_effect=responses) as compose):
            with redirect_stdout(io.StringIO()):
                pilot.run(Path(folder))
        isolation.assert_called_once()
        writes = [call for call in compose.call_args_list if "NATIVE_ALLOW_WRITES=1" in call.args]
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0].args[-1], "runner")
        self.assertIn("--no-deps", writes[0].args)
        self.assertEqual(writes[0].kwargs["timeout"], 300)
        self.assertEqual(pilot.invocation, "PASS")
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
