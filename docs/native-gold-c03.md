# Gold C03 native validation

This workflow validates `create_dim_game()` for [PR #18](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/pull/18) at `d92bd5e683c0125e83e2444b9d125dcaca849007`. It reuses [PR #14](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/commit/adc2d56803b9493c2e366f9a31cc3b85ad4e9816) for the verified fixture bundle and [PR #15](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/commit/ed0937882e505fac6bb658d00cfc858d73fa6332) for isolation and the checksum-pinned MinIO recipe. The driver and scenario come from the exact approved validation PR event SHA. **NATIVE NOT EXECUTED. PR #18 merge remains HOLD.**

## Authorization

On the dedicated validation PR, the Owner reviews the exact HEAD with the Tech Lead before moving it out of draft. After separate authorization to run, the Owner applies the one dedicated label. Opening or updating the PR does not trigger this job. The first labeled event must come from the Owner, and the driver repeats the same-repository, branch, non-draft, event and SHA checks before touching Docker.

```bash
repo=Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender
review_pr=YOUR_GOLD_VALIDATION_PR_NUMBER
review_head=YOUR_FULL_REVIEWED_HEAD_SHA
test "$(gh pr view "$review_pr" --repo "$repo" --json headRefOid --jq .headRefOid)" = "$review_head" &&
  gh pr edit "$review_pr" --repo "$repo" --add-label native-gold-c03-approved
```

Apply the label only to the validation PR. A rerun requires a fresh Owner label event on the reviewed SHA. Re-running the same GitHub Actions attempt is rejected.

## Acceptance evidence

| Case | Expected result |
| --- | --- |
| C03-01 | Changed AppIDs keep their keys, changed duplicates keep the highest key, new AppIDs get the next keys, and unchanged duplicates and target-only rows remain. One write adds one Iceberg snapshot. |
| C03-02 | Frozen unchanged replay preserves the same rows and snapshot lineage. |
| C03-03 | A fault before calling MERGE preserves the old state and adds no snapshot. Frozen retry publishes one transition. After a confirmed MERGE and simulated lost acknowledgement, a fresh reader sees the committed result and frozen replay adds no snapshot or remaps keys. |
| C03-04 | A separate Spark SQL session in the fixture verifies synthetic bridge/fact references to retained keys. Spark reads scratch Parquet in local executor tasks before MERGE, and scoped scratch directories are removed after each attempt. |

The before-MERGE and lost-acknowledgement faults are Python application-boundary simulations. They provide no evidence of a real JVM or storage interruption. The suite covers one dimension, synthetic consumer projections and the local Spark topology. Concurrent writers, multi-node filesystem visibility, persisted thesis schemas and a full Gold ETL run are outside this pilot.

## Isolation and limits

Spark 3.3.2 and Iceberg 1.3.1 use only fixture Hive and MinIO, with synthetic credentials, an internal network, two project-scoped volumes and no host mounts or exposed ports. The GitHub-hosted Ubuntu 24.04 job uses a read-only token and CPython 3.10.22. Writes are disabled by default and are enabled only for the single Gold invocation after verification. Public dependency downloads occur during checkout or image build.

Runner, Hive and MinIO are capped at 2/1/0.5 CPU and 4/2/1 GiB RAM. Docker JSON logs rotate at two 10 MB files per container. The job times out at 35 minutes and the scenario at 420 seconds. Disk starts with at least 8 GiB free and is checked at stage boundaries for a 4 GiB free floor and at most 12 GiB growth. The volumes have no hard disk quota. Each of four evidence records is capped at 6000 bytes, and failures forward only an allowlisted case and exception type. Cleanup verifies resource names, labels and endpoints before removal.

## Offline checks

Run `python -I -B -m unittest discover -s tests/ci -p 'test_*c03*.py' -v` with PyYAML available. Run `python -B -m unittest discover -s tests/reliability -v` for repository baseline regression. These tests exercise control flow, boundary checks and fixture assertions. Spark execution and Iceberg commits require the separately approved native run.
