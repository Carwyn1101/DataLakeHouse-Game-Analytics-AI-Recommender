# Gold C03 Phase 1B native validation

**NATIVE NOT EXECUTED. Production PR #20 merge remains HOLD.** This pilot exercises only the incremental recovery of `dim_detail_info`, `dim_review_info`, and `dim_popularity_info` for TP-19 and TP-25. It uses the candidate's production functions, with synthetic Silver inputs and synthetic fact-key probes.

## Revisions and execution gate

| Input | Exact revision |
| --- | --- |
| Canonical base | `a815d6bd9aaa09faf3b8e65ad4ec643abfed062d` |
| [Production PR #20](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/pull/20) | `d06e1d69f82465bba9a8a72b7edda62bb2e23adf` |
| PR #14 bundle and runtime | `adc2d56803b9493c2e366f9a31cc3b85ad4e9816` |
| PR #15 isolation driver and MinIO recipe | `ed0937882e505fac6bb658d00cfc858d73fa6332` |
| PR #19 Gold pilot driver | `42e45f441c97522bdd6afbbc55cd2a9f5dc27bad` |
| Validation driver and scenario | Exact validation PR HEAD from the Owner's label event. The runner verifies the checkout SHA and reads both files from the corresponding Git object. |

The Independent Tech Lead reviews the exact validation SHA before the PR leaves draft. The Project Owner authorizes execution separately by applying `native-gold-c03-phase-1b-approved` only to the reviewed validation PR. Opening, updating or marking the PR ready does not execute the job. The job and driver require the Owner's first attempt on an open, non-draft, same-repository PR against the pinned base. A changed base or validation HEAD requires reviewing the new revision and a fresh Owner event.

## Gate A: atomicity

For `C03B-DETAIL`, `C03B-REVIEW` and `C03B-POPULARITY`, assert the full row multiset and the retained/new keys. Check changed-target duplicate cleanup, target-only preservation and unchanged rows. Require one child snapshot for each successful change and no new snapshot or remapped key on unchanged replay, pre-MERGE failure or frozen retry after lost acknowledgement.

Gate A must PASS for all three cases. The driver requires positive row and retained-fact counts, three successful snapshots across the transitions, verified second-session readback and scratch cleanup. Each attempt checks staged Parquet schema, values and reader visibility. Any failed assertion or missing evidence fails the workflow. The independent SQL session shares the fixture SparkContext. Faults are injected before calling MERGE and after observing a commit. Real storage/JVM interruption, cross-process readers, multi-node executor storage, concurrent writers and real schemas remain outside the pilot.

## Gate B: duplicate integrity

`C03B-REVIEW-DUPS` and `C03B-POPULARITY-DUPS` run the same four fixtures against the candidate and historical `main`: exact and conflicting current duplicates, both for existing and new AppIDs. Each pair starts with verified identical target rows and the same materialized Silver batch. The baseline uses its exact pinned `silver_to_gold_etl.py`, sanitized by PR #14 and embedded by the bundle builder. The manifest carries the baseline revision and SHA-256, and the existing verifier reconstructs and compares both files. The legacy functions run against the isolated catalog with the Spark session binding replaced. The old DELETE, lazy APPEND and replay behavior are exercised as written.

Each fixture emits `candidate` and `baseline` measurements for `collision`, `repeated_commit`, `fanout`, `key_drift`, `first_snapshots`, `replay_snapshots`, `first_rows` and `replay_rows`. Replay commits are compared by occurrence because the historical DELETE/APPEND can create two snapshots for one replay. Raw snapshot deltas are preserved. The fixture measures fact-shaped joins and key multisets after both attempts. It preserves first-write and replay collision and fan-out counts individually, so a regression at one phase cannot hide behind an unchanged maximum. It also reports `baseline_status` (`MEASURED`, `EXECUTION_ERROR` or `INCONCLUSIVE`) and an `observations` map for the four integrity metrics. An exception can leave the paired baseline measurement unavailable. If only replay fails, first-write counters remain visible. A completed baseline that loses expected payloads or violates fixture controls retains its counters but is inconclusive.

| Observation | Meaning and gate outcome |
| --- | --- |
| `REGRESSION` | A measured candidate count exceeds the measured baseline count, including a first-write or replay collision/fan-out count. The workflow fails. |
| `BASELINE-KNOWN` | A positive candidate count is no greater than a valid baseline measurement of the same observation. Gate B is `CHARACTERIZED` when the other observations are known and none regress. |
| `UNVERIFIED` | Baseline equivalence could not be demonstrated. The workflow surfaces `HOLD` and exits nonzero, even if Gate A passed. |
| `NO ISSUE` | The candidate count is zero and valid baseline evidence is available. A fixture has no observed violation only when all four metrics qualify. |

The driver verifies measured labels before accepting evidence. Gate A and Gate B appear separately in the workflow summary. A characterized run can finish successfully for diagnostic purposes. Merge readiness remains HOLD pending Independent Tech Lead review and explicit Project Owner disposition of any material baseline-known risk. This fixed workload does not establish duplicate integrity for other inputs.

## Isolation and limits

Reuse the reviewed PR #14 Spark 3.3.2/Iceberg 1.3.1 image recipes, PR #15 host and Compose isolation checks and checksum-pinned MinIO build, and PR #19's scoped write boundary. The GitHub-hosted Ubuntu 24.04 job uses a read-only token, no saved checkout credentials, an internal fixture network and two disposable volumes. Default writes are off. Only the single scoped scenario enables writes after checking the running fixtures.

The job cap is 35 minutes and the native invocation cap is 420 seconds. Runner/Hive/MinIO are limited to 2/1/0.5 CPU and 4/2/1 GiB RAM, with two 10 MB log files each. Disk budget is checked at stage boundaries. The volumes have no hard disk quota. The paired baseline/candidate workload still needs runtime timing evidence within this unchanged cap. Unexpected diagnostics fail closed, and fixture ownership checks precede teardown.

## Offline checks

Use `python -I -B -m unittest discover -s tests/ci -p 'test_*c03_phase1b*.py' -v` with PyYAML installed and `python -B -m unittest discover -s tests/reliability`. The offline bundle check reconstructs the pinned candidate and scenario, verifies digests and isolation configuration, parses the staged sources for Python 3.10 grammar, and verifies tamper rejection. Offline checks cannot establish native acceptance.
