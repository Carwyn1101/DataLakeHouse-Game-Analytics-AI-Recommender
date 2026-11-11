"""Offline workflow safety and syntax checks. Docker is never invoked."""

import ast
import os
from pathlib import Path
import re
import shutil
import subprocess
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/native-pilot-smoke.yml"


class NativePilotWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = WORKFLOW.read_text(encoding="utf-8")
        cls.workflow = yaml.safe_load(cls.source)
        cls.job = cls.workflow["jobs"]["smoke"]

    def test_only_manual_label_gate_and_hosted_runner(self):
        self.assertEqual(self.workflow["on"], {"pull_request": {"types": ["labeled"]}})
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        self.assertEqual(self.job["runs-on"], "ubuntu-24.04")
        self.assertEqual(self.job["timeout-minutes"], 15)
        expected_checks = {
            "github.repository == 'Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender'",
            "github.event.label.name == 'native-pilot-smoke-approved'",
            "github.actor == github.repository_owner",
            "github.triggering_actor == github.repository_owner",
            "github.event.pull_request.head.repo.full_name == github.repository",
            "github.event.pull_request.head.ref == 'codex/github-actions-native-pilot'",
            "github.event.pull_request.base.ref == 'main'",
            "github.event.pull_request.state == 'open'",
            "github.event.pull_request.draft == false",
        }
        self.assertEqual({item.strip() for item in self.job["if"].split("&&")}, expected_checks)
        self.assertFalse({"environment", "container", "services"} & self.job.keys())

    def test_reviewed_harness_and_checkout_are_pinned(self):
        actions = [step for step in self.job["steps"] if "uses" in step]
        self.assertEqual(len(actions), 1)
        checkout = actions[0]
        self.assertEqual(checkout["uses"], "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd")
        self.assertEqual(checkout["with"]["ref"], "adc2d56803b9493c2e366f9a31cc3b85ad4e9816")
        self.assertEqual(self.job["env"]["HARNESS_SHA"], checkout["with"]["ref"])
        self.assertFalse(checkout["with"]["persist-credentials"])
        self.assertEqual(checkout["with"]["fetch-depth"], 1)
        self.assertEqual(checkout["with"]["sparse-checkout"], "tests/native")

    def test_no_secrets_or_native_execution_paths(self):
        self.assertEqual(self.job["env"]["NATIVE_ALLOW_WRITES"], "0")
        self.assertNotIn("secrets.", self.source)
        self.assertNotIn("id-token", self.source)
        self.assertNotIn("self-hosted", self.source)
        self.assertNotIn("pull_request_target", self.source)
        for forbidden in (
            r"docker\s+(?:run|build|pull|start|exec|volume|network)\b",
            r"docker\s+compose[^\n]*\b(?:up|run|build|pull|start)\b",
            r"NATIVE_ALLOW_WRITES[=: ]+[\"']?1",
            r"D:[/\\]TLCN_KLTN",
            r"https?://[^/\s]*(?:minio|metastore|thesis)",
        ):
            self.assertNotRegex(self.source, forbidden)
        self.assertIn('test "$endpoint" = "unix:///var/run/docker.sock"', self.source)
        self.assertIn("credential.helper= fetch", self.source)

    def test_bash_blocks_parse_without_execution(self):
        if os.name == "nt":
            # Windows' system32 bash.exe delegates to WSL. Use Git's parser explicitly.
            git = shutil.which("git")
            candidate = Path(git).parent.parent / "bin/bash.exe" if git else None
            bash = str(candidate) if candidate is not None and candidate.is_file() else None
        else:
            bash = shutil.which("bash")
        if bash is None:
            self.skipTest("Bash is unavailable for syntax-only validation")
        for step in self.job["steps"]:
            if "run" in step:
                with self.subTest(step=step["name"]):
                    result = subprocess.run([bash, "-n"], input=step["run"],
                                            text=True, capture_output=True)
                    self.assertEqual(result.returncode, 0, result.stderr)

    def test_embedded_python_and_candidate_plan(self):
        blocks = []
        for step in self.job["steps"]:
            if "run" in step:
                blocks.extend(re.findall(r"python3 -I -B - <<'PY'\n(.*?)\nPY", step["run"], re.S))
        self.assertEqual(len(blocks), 2)
        for source in blocks:
            compile(source, "<workflow-python>", "exec")
        plan = ast.parse(blocks[1])
        candidates = next(node.value for node in plan.body if isinstance(node, ast.Assign)
                          and isinstance(node.targets[0], ast.Name) and node.targets[0].id == "candidates")
        self.assertEqual(ast.literal_eval(candidates), (
            ("bcea0e946bb7b199e797943a82cfe82b65a89883", "bronze"),
            ("b0f733580c75b561893580a1dd15b7f344602d36", "gold-helpers"),
            ("5fd358ab12a637b06b175d4da77d339026eac3bf", "silver"),
        ))
        self.assertIn('"config", "--quiet"', blocks[1])
        self.assertIn('folder.parent.resolve() != Path("/tmp")', blocks[1])
        self.assertIn("shutil.rmtree(folder)", blocks[1])


if __name__ == "__main__":
    unittest.main()
