# Gold C03 Phase 1C-A native fact validation

**NATIVE NOT EXECUTED.** This pilot exercises only the review and popularity fact writers in [production PR #22](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/pull/22) at `f96212899e9f188524bcea6d2cd1a019f5a1aca6`, on base `d9fb3fd06a1e04ab28f7f70bcb288d7eaba1d533`.

| Dependency | Reviewed Git revision | Reuse |
| --- | --- | --- |
| PR #14 | `adc2d56803b9493c2e366f9a31cc3b85ad4e9816` | Deterministic bundle, sanitizer, runtime, fixture topology and file/digest verification |
| PR #15 | `ed0937882e505fac6bb658d00cfc858d73fa6332` | Docker host and Compose validation, MinIO recipe, limits and ownership-checked cleanup |
| PR #19 | `42e45f441c97522bdd6afbbc55cd2a9f5dc27bad` | Candidate bundle adapter, build/startup sequence and isolated write boundary |
| PR #21 | `213a537e60d30a9ab8d2e3b7157d2cb294f9f9de` | Native command capture across stdout, with a fact-specific marker parser |

## Approval and isolation

The Draft PR is inert. After Independent Tech Lead review of both full SHAs, the Project Owner separately authorizes a native run, marks the reviewed validation PR ready and applies `native-gold-c03-phase-1c-a-approved`. The first `pull_request:labeled` event must come from the same repository and Owner at the pinned base. The runner verifies the event HEAD and reads the driver and scenario from that Git object. A rerun or another label is insufficient.

The GitHub-hosted job uses `contents: read` and does not persist checkout credentials. The reviewed bundle contains only synthetic data and disposable fixture resources on an internal network. Topology, resource ownership and runtime endpoints are checked before the single write-enabled runner invocation. The workflow runs no checkout or mount from the thesis environment. Existing limits are 35 minutes per job, 420 seconds per native invocation, per-container CPU and memory limits, rotating Docker logs and stage-boundary disk checks. Cleanup verifies project ownership and removal.

## Acceptance

| Gate | Required evidence for each fact |
| --- | --- |
| A — Normal replacement | Actual initial load and incremental refresh preserve schema, exact source timestamps and row multiset. Changed/new/unchanged and target-only rows match the oracle. Exact duplicates, fan-out and nullable FKs survive. Nonempty refresh adds one descendant snapshot and empty input adds none. |
| B — Fault and replay | A staging fault and pre-MERGE injection leave the target unchanged. Frozen retry equals an uninterrupted reference. Lost acknowledgement is injected only after a confirmed native commit and readback from a second SQL session. Non-null replay preserves the reference multiset and FKs. |
| C — Legacy NULL-key behavior | Existing null-key rows survive a normal replacement. Incoming null-key rows append again on frozen replay. `LEGACY_BEHAVIOR_PRESERVED` covers this expected accumulation. |

TP-25 checks actual staged Parquet names/types/nullability, executor readback, direct file MERGE, parent/child snapshot lineage, scoped scratch lifetime and cleanup. Both fact functions run unmodified against disposable Iceberg tables. Application-boundary fault injection does not simulate JVM or storage interruptions. The second SQL session shares a SparkContext. Runner performance, native SQL planning, cross-process readers and production data remain outside the offline evidence.

A run requires six ordered, typed case records and successful native exit, with separate gate, compatibility, isolation and cleanup statuses. A late failure retains completed case records. Unexpected evidence fails validation. Only sanitized exception classes and validated synthetic measurements are published. Limits are 6,100 bytes per evidence line and 32 KiB of retained evidence. Native stderr is discarded.

## Offline checks

Run `python -I -B -m unittest discover -s tests/ci -p 'test_*c03_fact*.py' -v` with PyYAML available, and `python -B -m unittest discover -s tests/reliability`. The offline bundle probe verifies the exact Git inputs, Python 3.10 grammar, unchanged production function AST, default writes-off topology, and tamper rejection. Native execution, merge and rollout remain **HOLD**.
