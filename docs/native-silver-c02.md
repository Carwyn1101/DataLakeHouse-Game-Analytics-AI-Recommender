# Silver SCD2 C02 native validation

This workflow prepares a synthetic validation of [PR #16](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/pull/16). It pins candidate `724cf1554111951065120ea64a9ee5f7abe6f038`, the [PR #14 harness](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/commit/adc2d56803b9493c2e366f9a31cc3b85ad4e9816) and the [PR #15 pilot](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/commit/ed0937882e505fac6bb658d00cfc858d73fa6332). The [prior Bronze run](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/43142336125) verified the fixture infrastructure for Bronze. Native Silver validation for this candidate remains NOT EXECUTED.

## Authorization and later execution

1. The Tech Lead reviews the local diff. Publishing the dedicated `codex/silver-c02-native-validation` branch and opening its review PR require separate Owner authorization. PR #16 remains the production candidate.
2. For an authorized run, the Owner checks that the dedicated review PR is open, non-draft, same-repository, targets `main` and has exactly the reviewed head SHA. The Owner applies `native-silver-c02-approved` to that review PR. The following example requires GitHub CLI in the Owner's authenticated environment and is for the authorized run only.

```bash
repo=Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender
review_pr=YOUR_DEDICATED_REVIEW_PR_NUMBER
review_head=YOUR_FULL_TECH_LEAD_REVIEWED_HEAD_SHA
test "$(gh pr view "$review_pr" --repo "$repo" --json headRefOid --jq .headRefOid)" = "$review_head" &&
  gh pr edit "$review_pr" --repo "$repo" --add-label native-silver-c02-approved
```

The workflow accepts only the first `pull_request:labeled` attempt with both actor and triggering actor equal to the repository Owner. The driver rechecks the payload, actor, branch and SHA before invoking Docker. The job checks out the label-event SHA, streams its driver Git blob into `python -I -B -` and reads the C02 scenario from the same commit. Changes made after the label event are outside that run. For another run, the Owner must review the exact head again and issue a fresh label event. A job rerun is rejected.

## Boundaries and cleanup

- Ubuntu 24.04 GitHub-hosted VM, read-only repository token, no persisted checkout credentials. Host staging uses CPython 3.10.22. The pinned runner uses Spark 3.3.2 and Iceberg 1.3.1.
- The driver verifies the staged file set, hashes and Python syntax before building. It reuses PR #15's Compose checks and checksum-pinned MinIO binary recipe. Reviewed public dependencies are fetched during checkout or image build. Native services communicate only on the internal fixture network, without host mounts, exposed ports or production credentials.
- A fresh random project owns two volumes for MinIO and Hive and an internal network. The runtime checks DNS, route, endpoint and warehouse scope before creating the fixture bucket. `NATIVE_ALLOW_WRITES` is `0` at job and Compose scope. The sole C02 runner invocation gets `1` after provenance and isolation pass.
- Runner: 2 CPU / 4 GiB. Hive: 1 CPU / 2 GiB. MinIO: 0.5 CPU / 1 GiB. Docker JSON logs rotate at two 10 MB files per container. The job is capped at 35 minutes and the runner command at 420 seconds. Initial disk headroom is 8 GiB. Stage checks require at least 4 GiB free and at most 12 GiB growth from the starting budget. These checks sample disk use at boundaries. Docker volumes have no hard quota.
- Cleanup verifies labels, resource names and network endpoints before removing project containers, volumes and network. It removes only its verified temporary bundle. Image/build cache and any residue after VM termination are confined to the disposable hosted runner. No global prune or artifact upload runs.

## Expected evidence

| Case | Expected result |
| --- | --- |
| C02-01 | The real `_upsert_scd2()` expires version 1 and inserts version 2 in one new snapshot. Historical business fields are preserved, expiry equals successor effective time and the key has one current row. |
| C02-02 | Identical replay returns zero and leaves rows, versions and snapshots identical. |
| C02-03 | A Python fault before calling MERGE leaves the previous snapshot and current row visible to a fresh reader. Frozen retry commits one successor. The lost-ack simulation executes MERGE, confirms the commit and raises at the Python boundary. A fresh reader sees the successor and replay creates no new snapshot. |
| C02-04 | An independent Spark session verifies current-row uniqueness, history and the Gold input projection. Gold ETL and Gold writes are outside this scenario. |

Use the four bounded `C02-0N PASS` JSON records and the job summary to review synthetic rows, version/validity timestamps and snapshot lineage. An assertion or scope check failure fails the job. The injected faults exercise Python/application boundaries. Actual JVM or storage-level interruption, concurrent writers, production data and a complete Silver-to-Gold run remain outside this pilot.

## C02-01 failure diagnostics

[Run 43366693077](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/43366693077) passed provenance, isolation and fixture startup, then reported `IllegalArgumentException` during C02-01. The log did not identify the failing operation or whether MERGE ran.

The runner now reports one allowlisted C02-01 stage and exception type on failure. The stage distinguishes fixture creation, independent reads, pre-MERGE planning, the SQL call, cleanup and assertions. Native failure output omits exception messages, SQL, endpoints and tracebacks. A process exit without a valid marker yields a fixed fallback and still fails the job.

[Run 43370420506](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/43370420506/job/131704616866) localized the failure to the native MERGE SQL call. It did not establish the JVM cause or commit outcome. [Iceberg issue #7766](https://github.com/apache/iceberg/issues/7766) describes a Spark 3.3.2 failure involving metadata-column injection into a temporary view. The earlier candidate `9a7e7a3f492a03dec514d6f9e0a147fd60bec13d` used a DataFrame temp view derived in part from the target. [Run 43423969080](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/43423969080/job/131880610812) confirmed the view/analyzer signature on that candidate. The current pin uses a direct temporary Parquet source.

A MERGE failure can now include a fixed category derived from Spark's already-captured JVM frames. `spark-view-metadata-columns` identifies the upstream view/analyzer signature, `iceberg-write-schema` identifies Iceberg write-schema validation, and `iceberg-commit` identifies a commit-path frame. A commit-path match does not establish whether the commit became visible. `unclassified` means no reviewed signature was available. The runner emits only the category. It never emits the frame, exception text, SQL, trace or runtime configuration.

## Offline review

Run `python -I -B -m unittest discover -s tests/ci -p 'test_*c02*.py' -v` with PyYAML available for the workflow checks. These tests cover gate, plan, evidence and assertion logic without Docker. Run `python -B -m unittest discover -s tests/reliability -v` for the baseline reliability suite.
