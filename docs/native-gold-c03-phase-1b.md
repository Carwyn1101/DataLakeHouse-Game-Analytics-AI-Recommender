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

## Acceptance evidence

| Cases | Required evidence |
| --- | --- |
| `C03B-DETAIL`, `C03B-REVIEW`, `C03B-POPULARITY` | Assert the full row multiset and the retained/new keys. Check both changed target duplicate cleanup and untouched target-only and unchanged rows. Confirm one child snapshot for each successful change, no snapshot for unchanged replay or a pre-commit failure, and no new snapshot or key change for frozen-input retries after a verified commit followed by lost acknowledgement. |
| `C03B-REVIEW-DUPS`, `C03B-POPULARITY-DUPS` | Track exact and conflicting current duplicates, each for existing and new AppIDs. Record collisions, repeat commits, key drift and join fan-out. Any such finding is a FAIL, including when production inherited the behavior from baseline. The driver forwards only fixed case IDs, typed allowlisted counters and a bounded exception class when available. |

A second Spark SQL session observes committed Iceberg state. Each staged Parquet read checks the real schema and values before the MERGE. Scratch is checked for cleanup after normal and faulted attempts. Fact probes use synthetic AppID lookups as well as references to retained surrogate keys. They exercise the dimension mapping without running other Gold table builders. Injected faults occur at the Python application boundary immediately before MERGE or after an observed commit. The SQL reader shares the fixture SparkContext. Cross-process reader isolation, multi-node executor storage, concurrent writers and real production schemas remain outside the pilot.

## Isolation and limits

Reuse the reviewed PR #14 Spark 3.3.2/Iceberg 1.3.1 image recipes, PR #15 host and Compose isolation checks and checksum-pinned MinIO build, and PR #19's scoped write boundary. The GitHub-hosted Ubuntu 24.04 job uses a read-only token, no saved checkout credentials, an internal fixture network and two disposable volumes. Default writes are off. Only the single scoped scenario enables writes after checking the running fixtures.

The job cap is 35 minutes and the native invocation cap is 420 seconds. Runner/Hive/MinIO are limited to 2/1/0.5 CPU and 4/2/1 GiB RAM, with two 10 MB log files each. Disk budget is checked at stage boundaries. The volumes have no hard disk quota. The larger Phase 1B workload still needs runtime timing evidence. Unexpected diagnostics fail closed, and fixture ownership checks precede teardown.

## Offline checks

Use `python -I -B -m unittest discover -s tests/ci -p 'test_*c03_phase1b*.py' -v` with PyYAML installed and `python -B -m unittest discover -s tests/reliability`. The offline bundle check reconstructs the pinned candidate and scenario, verifies digests and isolation configuration, parses the staged sources for Python 3.10 grammar, and verifies tamper rejection. Offline checks cannot establish native acceptance.
