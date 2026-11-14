# Gold C03 Phase 1B native validation

**NATIVE NOT EXECUTED. Production PR #20 merge remains HOLD.** This pilot exercises only the incremental recovery of `dim_detail_info`, `dim_review_info`, and `dim_popularity_info` for TP-19 and TP-25. It uses the candidate's production functions, with synthetic Silver inputs and synthetic fact-key probes.

## Revisions and execution gate

| Input | Exact revision |
| --- | --- |
| Canonical base | `a815d6bd9aaa09faf3b8e65ad4ec643abfed062d` |
| [Production PR #20](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/pull/20) | `90fcdce6a78caa7b0c74b7d9708eb92ffce80275` |
| PR #14 bundle and runtime | `adc2d56803b9493c2e366f9a31cc3b85ad4e9816` |
| PR #15 isolation driver and MinIO recipe | `ed0937882e505fac6bb658d00cfc858d73fa6332` |
| PR #19 Gold pilot driver | `42e45f441c97522bdd6afbbc55cd2a9f5dc27bad` |
| Validation driver and scenario | Exact validation PR HEAD from the Owner's label event. The runner verifies the checkout SHA and reads both files from the corresponding Git object. |

The Independent Tech Lead reviews the exact validation SHA before the PR leaves draft. The Project Owner authorizes execution separately by applying `native-gold-c03-phase-1b-approved` only to the reviewed validation PR. Opening, updating or marking the PR ready does not execute the job. The job and driver require the Owner's first attempt on an open, non-draft, same-repository PR against the pinned base. A changed base or validation HEAD requires reviewing the new revision and a fresh Owner event.

## Gate A: atomicity

For `C03B-DETAIL`, `C03B-REVIEW` and `C03B-POPULARITY`, assert the full row multiset and the retained/new keys. Check changed-target duplicate cleanup, target-only preservation and unchanged rows. Require one child snapshot for each successful change and no new snapshot or remapped key on unchanged replay, pre-MERGE failure or frozen retry after lost acknowledgement.

Gate A must PASS for all three cases. The driver requires positive row and retained-fact counts, three successful snapshots across the transitions, verified second-session readback and scratch cleanup. Each attempt checks staged Parquet schema, values and reader visibility. Any failed assertion or missing evidence fails the workflow. The independent SQL session shares the fixture SparkContext. Faults are injected before calling MERGE and after observing a commit. Real storage/JVM interruption, cross-process readers, multi-node executor storage, concurrent writers and real schemas remain outside the pilot.

## Gate B: fail-closed duplicate input

The Project Owner approved rejection of any repeated current AppID for incremental review and popularity. For each dimension, run exact and conflicting payloads against existing and new AppIDs. The candidate must raise `DuplicateCurrentInputError` on both attempts against the same Silver batch. The fixture intercepts SQL and WriterV2 calls, asserts zero target write attempts, compares complete rows and Iceberg snapshot history from another SQL session, checks pre-existing fact-key references, and verifies that both attempts clean their source scratch.

Each fixture emits `candidate.outcome = REJECTED_DUPLICATE_INPUT` only after all checks succeed, with attempt count, target-write count and row/snapshot/reference/cleanup flags. Failed candidate fixtures emit a bounded exception class and fail Gate B. Neither rejection nor Gate B PASS constitutes successful ingestion. All three Gate A cases and both Gate B cases must pass.

The historical baseline still runs the exact sanitized source from the pinned main SHA on the same frozen Silver batch and a target re-seeded to the same row multiset. Its DELETE and lazy APPEND are exercised as written. `baseline` preserves measured collisions, fan-out, repeat-commit occurrence, key drift, and per-attempt rows and snapshot counts. The historical outcome is characterized separately and may differ from the candidate. Baseline errors or inconclusive measurements remain `UNVERIFIED` in the run summary and keep the overall result on HOLD.

## Bounded failure evidence

The previous [native run](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/43581189093/job/132406058746) preserved only a trailing slice of output. It measured the popularity conflicting-new replay collision but omitted early Gate A records. The command adapter now scans the whole native stdout for allowlisted records, with a 6,100-byte line cap and a 32 KiB total evidence budget. Native stderr is discarded. Raw output is never published.

An observed failure retains the completed case records, separately identifies unfinished cases as `NOT OBSERVED`, and reports the first allowlisted diagnostic and the scoped cleanup outcome. Runner resource limits and the Owner label gate are unchanged.

## Isolation and limits

Reuse the reviewed PR #14 Spark 3.3.2/Iceberg 1.3.1 image recipes, PR #15 host and Compose isolation checks and checksum-pinned MinIO build, and PR #19's scoped write boundary. The GitHub-hosted Ubuntu 24.04 job uses a read-only token, no saved checkout credentials, an internal fixture network and two disposable volumes. Default writes are off. Only the single scoped scenario enables writes after checking the running fixtures.

The job cap is 35 minutes and the native invocation cap is 420 seconds. Runner/Hive/MinIO are limited to 2/1/0.5 CPU and 4/2/1 GiB RAM, with two 10 MB log files each. Disk budget is checked at stage boundaries. The volumes have no hard disk quota. The paired baseline/candidate workload still needs runtime timing evidence within this unchanged cap. Unexpected diagnostics fail closed, and fixture ownership checks precede teardown.

## Offline checks

Use `python -I -B -m unittest discover -s tests/ci -p 'test_*c03_phase1b*.py' -v` with PyYAML installed and `python -B -m unittest discover -s tests/reliability`. The offline bundle check reconstructs the pinned candidate and scenario, verifies digests and isolation configuration, parses the staged sources for Python 3.10 grammar, and verifies tamper rejection. Offline checks cannot establish native acceptance.
