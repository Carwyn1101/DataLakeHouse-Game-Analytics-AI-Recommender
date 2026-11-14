"""Offline checks for the Phase 1B GitHub Actions trigger and exact checkouts."""

from pathlib import Path
import shutil
import subprocess
import unittest

import yaml


class Phase1BWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[2]
        path = root / ".github/workflows/native-gold-c03-phase-1b.yml"
        cls.workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        cls.job = cls.workflow["jobs"]["gold-c03-phase-1b"]

    def test_owner_label_is_the_only_write_gate(self):
        self.assertEqual(self.workflow["on"], {"pull_request": {"types": ["labeled"]}})
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        self.assertEqual(set(self.workflow["jobs"]), {"gold-c03-phase-1b"})
        self.assertEqual(self.job["runs-on"], "ubuntu-24.04")
        self.assertEqual(self.job["timeout-minutes"], 35)
        self.assertEqual(self.job["env"]["NATIVE_ALLOW_WRITES"], "0")
        gate = set(self.job["if"].split(" && "))
        expected = {
            "github.repository == 'Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender'",
            "github.event.label.name == 'native-gold-c03-phase-1b-approved'",
            "github.actor == github.repository_owner",
            "github.triggering_actor == github.repository_owner",
            "github.event.sender.login == github.repository_owner",
            "github.run_attempt == '1'",
            "github.event.pull_request.head.repo.full_name == github.repository",
            "github.event.pull_request.head.ref == 'codex/gold-c03-phase-1b-native-validation'",
            "github.event.pull_request.base.repo.full_name == github.repository",
            "github.event.pull_request.base.ref == 'main'",
            "github.event.pull_request.base.sha == 'a815d6bd9aaa09faf3b8e65ad4ec643abfed062d'",
            "github.event.pull_request.state == 'open'",
            "github.event.pull_request.draft == false",
        }
        self.assertEqual({item.strip() for item in gate}, expected)

    def test_exact_checkout_pins_and_shell_source(self):
        steps = self.job["steps"]
        self.assertEqual(len(steps), 5)
        self.assertEqual([step["with"]["ref"] for step in steps[:3]], [
            "${{ github.event.pull_request.head.sha }}",
            "adc2d56803b9493c2e366f9a31cc3b85ad4e9816",
            "ed0937882e505fac6bb658d00cfc858d73fa6332",
        ])
        self.assertEqual([step["with"]["path"] for step in steps[:3]], ["pilot", "reviewed-harness", "reviewed-pilot"])
        for step in steps[:3]:
            self.assertRegex(step["uses"], r"^actions/checkout@[0-9a-f]{40}$")
            self.assertIs(step["with"]["persist-credentials"], False)
        self.assertRegex(steps[3]["uses"], r"^actions/setup-python@[0-9a-f]{40}$")
        self.assertEqual(steps[3]["with"]["python-version"], "3.10.22")
        execution = steps[-1]
        self.assertEqual(execution["env"]["PILOT_HEAD"], "${{ github.event.pull_request.head.sha }}")
        shell = execution["run"]
        self.assertTrue(shell.startswith("set -euo pipefail"))
        match_head = 'test "$(git --no-replace-objects -C "$pilot" rev-parse HEAD)" = "$PILOT_HEAD"'
        driver = 'git --no-replace-objects -C "$pilot" show "$PILOT_HEAD:tests/ci/run_native_gold_c03_phase1b.py" |'
        self.assertLess(shell.index(match_head), shell.index(driver))
        self.assertTrue(shell.rstrip().endswith("python -I -B -"))

    def test_runner_shell_syntax(self):
        git = Path(shutil.which("git"))
        git_bash = git.parents[1] / "bin/bash.exe"
        runner = str(git_bash) if git_bash.is_file() else shutil.which("bash")
        if not runner:
            self.skipTest("Bash unavailable")
        shell = self.job["steps"][-1]["run"]
        subprocess.run([runner, "-n"], input=shell.encode(), check=True)
