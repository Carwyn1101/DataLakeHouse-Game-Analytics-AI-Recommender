"""Static checks for the only trigger and exact inputs of the Gold C03 workflow."""

from pathlib import Path
import unittest

import yaml

import run_native_gold_c03 as gold


class GoldWorkflowTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[2]
        workflow = root / ".github/workflows/native-gold-c03.yml"
        self.raw = workflow.read_text(encoding="utf-8")
        self.model = yaml.safe_load(self.raw)
        self.job = self.model["jobs"]["gold-c03"]

    def test_only_owner_label_event_can_run_the_single_hosted_job(self):
        self.assertEqual(self.model["on"], {"pull_request": {"types": ["labeled"]}})
        self.assertEqual(self.model["permissions"], {"contents": "read"})
        self.assertEqual(set(self.model["jobs"]), {"gold-c03"})
        self.assertEqual(self.job["runs-on"], "ubuntu-24.04")
        self.assertEqual(self.job["timeout-minutes"], 35)
        self.assertEqual(self.job["env"]["NATIVE_ALLOW_WRITES"], "0")
        gate = self.job["if"]
        for clause in (
            f"github.repository == '{gold.REPOSITORY}'",
            f"github.event.label.name == '{gold.APPROVAL}'",
            "github.actor == github.repository_owner",
            "github.triggering_actor == github.repository_owner",
            "github.event.sender.login == github.repository_owner",
            "github.run_attempt == '1'",
            "github.event.pull_request.head.repo.full_name == github.repository",
            f"github.event.pull_request.head.ref == '{gold.BRANCH}'",
            "github.event.pull_request.base.repo.full_name == github.repository",
            "github.event.pull_request.base.ref == 'main'",
            "github.event.pull_request.state == 'open'",
            "github.event.pull_request.draft == false",
        ):
            self.assertIn(clause, gate)
        self.assertNotIn("||", gate)

    def test_checkout_pins_and_streams_immutable_driver_bytes(self):
        pilot, harness, reviewed, python, run = self.job["steps"]
        self.assertEqual(
            [step["with"]["ref"] for step in (pilot, harness, reviewed)],
            ["${{ github.event.pull_request.head.sha }}", gold.HARNESS, gold.REVIEWED_PILOT],
        )
        for checkout in (pilot, harness, reviewed):
            self.assertEqual(checkout["uses"],
                             "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd")
            self.assertIs(checkout["with"]["persist-credentials"], False)
        self.assertEqual(python["uses"],
                         "actions/setup-python@a309ff8b426b58ec0e2a45f0f869d46889d02405")
        self.assertEqual(python["with"]["python-version"], "3.10.22")
        self.assertEqual(run["working-directory"], "reviewed-harness")
        self.assertEqual(run["shell"], "bash")
        self.assertEqual(run["env"]["PILOT_HEAD"], "${{ github.event.pull_request.head.sha }}")
        self.assertIn("set -o pipefail", run["run"])
        self.assertIn(
            'git --no-replace-objects -C "$GITHUB_WORKSPACE/pilot" '
            'show "$PILOT_HEAD:tests/ci/run_native_gold_c03.py" |',
            run["run"],
        )
        self.assertIn("python -I -B -", run["run"])
        self.assertNotIn("secrets.", self.raw)
        self.assertNotIn("NATIVE_ALLOW_WRITES=1", self.raw)


if __name__ == "__main__":
    unittest.main()
