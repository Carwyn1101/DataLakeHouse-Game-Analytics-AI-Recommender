"""Offline checks of the Silver C02 gate, bundle adaptation and write boundary."""

from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

import run_native_silver_c02 as silver


def approved():
    head = "a" * 40
    environment = {
        "PILOT_HEAD": head,
        "GITHUB_ACTIONS": "true",
        "RUNNER_ENVIRONMENT": "github-hosted",
        "RUNNER_OS": "Linux",
        "GITHUB_EVENT_NAME": "pull_request",
        "GITHUB_REPOSITORY": silver.REPOSITORY,
        "GITHUB_ACTOR": silver.OWNER,
        "GITHUB_TRIGGERING_ACTOR": silver.OWNER,
        "GITHUB_RUN_ATTEMPT": "1",
        "NATIVE_ALLOW_WRITES": "0",
    }
    repository = {"full_name": silver.REPOSITORY}
    event = {
        "action": "labeled",
        "label": {"name": silver.APPROVAL},
        "repository": repository,
        "sender": {"login": silver.OWNER},
        "pull_request": {
            "state": "open",
            "draft": False,
            "base": {"ref": "main", "repo": repository},
            "head": {"ref": silver.BRANCH, "sha": head, "repo": repository},
        },
    }
    return environment, event


def completed():
    return b"\n".join([f'{case} PASS {{"snapshot":1}}'.encode() for case in silver.CASE_IDS]
                       + [b"Scenario completed: silver-c02"])


class SilverPilotTests(unittest.TestCase):
    def test_only_owner_first_label_event_with_exact_sha_is_accepted(self):
        environment, event = approved()
        self.assertEqual(silver.approved_event(environment, event), "a" * 40)
        for field, value in {
            "GITHUB_ACTIONS": "false",
            "RUNNER_ENVIRONMENT": "self-hosted",
            "RUNNER_OS": "Windows",
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_REPOSITORY": "someone/else",
            "GITHUB_ACTOR": "someone",
            "GITHUB_TRIGGERING_ACTOR": "someone",
            "GITHUB_RUN_ATTEMPT": "2",
            "NATIVE_ALLOW_WRITES": "1",
            "PILOT_HEAD": "b" * 40,
        }.items():
            with self.subTest(field=field), self.assertRaises(silver.C02Failure):
                silver.approved_event(dict(environment, **{field: value}), event)

        for path, value in (
            (("action",), "synchronize"),
            (("label", "name"), "native-bronze-approved"),
            (("sender", "login"), "someone"),
            (("pull_request", "draft"), True),
            (("pull_request", "state"), "closed"),
            (("pull_request", "base", "ref"), "development"),
            (("pull_request", "head", "ref"), "another-branch"),
            (("pull_request", "head", "repo"), {"full_name": "someone/fork"}),
        ):
            altered = deepcopy(event)
            parent = altered
            for key in path[:-1]:
                parent = parent[key]
            parent[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(silver.C02Failure):
                silver.approved_event(environment, altered)

    def test_pinned_blob_read_rejects_wrong_checkout_head(self):
        repository = Path("reviewed")
        with patch.object(silver, "git", return_value=b"wrong\n") as git:
            with self.assertRaisesRegex(silver.C02Failure, "checkout revision"):
                silver.checked_blob(repository, silver.HARNESS, "tests/native/prepare.py")
            git.assert_called_once_with(repository, "rev-parse", "HEAD")

    def test_preparation_accepts_one_pair_and_adds_exact_helper_and_scenario(self):
        source = b"""
import json
HARNESS_DIR = "tests/native"
def json_bytes(data):
    return json.dumps(data).encode()
def expected_files(candidate, scenario, project, password, harness):
    assert candidate in CANDIDATES and scenario in SCENARIOS
    return {"runner/run.json": b'{"scripts":{"rawData_to_bronze.py":"unchanged"}}',
            "runner/scenarios.py": b"old scenario", "compose.json": b"unchanged"}
def git_blob(revision, name):
    assert revision in CANDIDATES and name == "bronze_appid.py"
    return b"approved helper"
def sanitized(name, source):
    return source
"""
        scope = silver.bind_preparation(source, b"reviewed C02 scenario", Path("approved"))
        bundle = scope["expected_files"]
        args = (silver.CANDIDATE, silver.SCENARIO, "native-fixture", "synthetic", silver.HARNESS)
        files = bundle(*args)
        self.assertEqual(files["compose.json"], b"unchanged")
        self.assertEqual(files["runner/scenarios.py"], b"reviewed C02 scenario")
        self.assertEqual(files["runner/scripts/bronze_appid.py"], b"approved helper")
        self.assertEqual(json.loads(files["runner/run.json"])["scripts"], {
            "rawData_to_bronze.py": "unchanged",
            "bronze_appid.py": hashlib.sha256(b"approved helper").hexdigest(),
        })
        for position, invalid in ((0, "f" * 40), (1, "silver-append-failure"), (4, "e" * 40)):
            changed = list(args)
            changed[position] = invalid
            with self.subTest(position=position), self.assertRaises(silver.C02Failure):
                bundle(*changed)

    def test_evidence_requires_all_cases_in_order_and_keeps_credentials_private(self):
        self.assertEqual(len(silver.evidence_lines(completed(), ["sensitive"])), 4)
        cases = completed().splitlines()
        invalid = [
            b"\n".join(cases[1:]),
            b"\n".join([cases[1], cases[0], *cases[2:]]),
            completed() + b'\nC02-01 PASS {"snapshot":2}',
            b"\n".join(cases[:-1]),
            completed().replace(b'"snapshot":1', b'"secret":"sensitive"'),
            completed().replace(b'{"snapshot":1}', b'{"long":"' + b"x" * 6000 + b'"}'),
        ]
        for index, payload in enumerate(invalid):
            with self.subTest(index=index), self.assertRaises(silver.C02Failure):
                silver.evidence_lines(payload, ["sensitive"])

    def test_stage_boundary_disk_budget_rejects_low_free_space_or_excess_growth(self):
        floor = 4 * silver.GIB
        within = SimpleNamespace(used=3 * silver.GIB, free=8 * silver.GIB)
        with patch.object(silver.shutil, "disk_usage", return_value=within) as usage:
            budget = silver.DiskBudget(Path("/var/lib/docker").resolve())
            budget.check()
            for changed in (SimpleNamespace(used=within.used, free=floor - 1),
                            SimpleNamespace(used=16 * silver.GIB, free=floor)):
                usage.return_value = changed
                with self.assertRaisesRegex(silver.C02Failure, "storage exceeded"):
                    budget.check()

    def setup_pilot(self):
        pilot = Mock(project="native-0123456789abcdef", redactions=[])
        baseline = Mock(
            LIMITS={"fixture-minio": (0.5, silver.GIB), "fixture-hms": (1, 2 * silver.GIB),
                    "runner": (2, 4 * silver.GIB)},
            LOGGING={"driver": "json-file"},
            FIXTURES={"fixture-minio": (), "fixture-hms": ()},
        )
        baseline.minio_build.return_value = {"image": "synthetic"}
        pilot.docker.side_effect = [b"/var/lib/docker", b"sha256:" + b"1" * 64,
                                    b"sha256:" + b"2" * 64, b"sha256:" + b"1" * 64]
        pilot.compose.side_effect = [b"{}", b"{}", b"", b"", b"", completed()]
        steps = Mock()
        steps.attach_mock(pilot.verify_fresh, "fresh")
        steps.attach_mock(pilot.verify_running, "isolated")
        steps.attach_mock(baseline.verify_models, "topology")
        steps.attach_mock(pilot.compose, "compose")
        return pilot, baseline, steps

    def test_only_one_runner_command_enables_writes_after_isolation_checks(self):
        pilot, baseline, steps = self.setup_pilot()
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(silver, "DiskBudget"), patch.object(silver, "stage_bundle"), \
                patch("sys.stdout", new_callable=io.StringIO):
            silver.run(pilot, baseline, {}, b"", Path(directory))
        self.assertEqual(pilot.invocation, "PASS")
        self.assertEqual([step[0] for step in steps.mock_calls],
                         ["compose", "compose", "topology", "fresh",
                          "compose", "compose", "compose", "isolated", "compose"])
        writes = [entry for entry in pilot.compose.call_args_list if "NATIVE_ALLOW_WRITES=1" in entry.args]
        self.assertEqual(writes, [call(
            "run bounded native Silver C02", "run", "--pull", "never", "--rm", "--no-deps",
            "-T", "--interactive=false", "-e", "NATIVE_ALLOW_WRITES=1", "runner",
            timeout=420, diagnostics=True,
        )])

    def test_failed_isolation_never_invokes_write_enabled_runner(self):
        pilot, baseline, _ = self.setup_pilot()
        pilot.verify_running.side_effect = silver.C02Failure("synthetic isolation failure")
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(silver, "DiskBudget"), patch.object(silver, "stage_bundle"), \
                patch("sys.stdout", new_callable=io.StringIO):
            with self.assertRaisesRegex(silver.C02Failure, "isolation failure"):
                silver.run(pilot, baseline, {}, b"", Path(directory))
        self.assertFalse(any("NATIVE_ALLOW_WRITES=1" in entry.args for entry in pilot.compose.call_args_list))


if __name__ == "__main__":
    unittest.main()
