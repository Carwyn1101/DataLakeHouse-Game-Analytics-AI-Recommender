# Silver SCD2 failure and retry characterization

Baseline: canonical main at `bdbf14f`, after PR #10. This phase changes only tests and documentation. The [Silver job](../LakeHouse_Project/scripts/bronze_to_silver_etl.py) still processes games, popularity, then reviews in one task. It creates a missing table with version 1 and uses `_upsert_scd2()` for an existing table. The [DAG](../LakeHouse_Project/airflow/dags/bronze_to_silver_etl.py) has one task-level retry, which reruns extraction and all three tables.

## Evidence

The [offline tests](../tests/reliability/test_silver_scd2_retry.py) execute the actual functions extracted with AST. Spark plans, counts and write outcomes are scripted. The tests verify call order and arguments. They do not execute joins, evaluate Spark expressions, or inspect Iceberg rows.

| Scenario | Verified from source | Verified by offline test | Not verified with native Iceberg |
| --- | --- | --- | --- |
| Initial load | The absent-table branch adds current metadata and starts at version 1. | Default version, metadata columns and type casts. Existing catalog tests cover create-only. | Actual row values and table snapshot. |
| New key | Left anti join uses the current set. A key with no history gets version 1. | Left anti join, union of keys, historical max and `coalesce(max_ver, 0) + 1` expression. | Evaluated join results and version sequence. |
| Changed tracked fields | Current-key matches are compared with null-safe equality. Only matched changed current rows are expired. Successors append after UPDATE. | Null-safe predicate construction, version expression, exact UPDATE scope, temp view and APPEND ordering. | Historical payload preservation and current-row cardinality. |
| Repeated unchanged input | A zero selected-key count returns before table writes. | Two runs with zero selected keys skip UPDATE and APPEND. | Whether real repeated inputs produce zero keys and leave the snapshot unchanged. |
| Failure before or during UPDATE | Failures escape the helper. The caller reaches its cleanup block. | Materialization failure never calls UPDATE. A raised UPDATE error prevents APPEND. | UPDATE commit state if its acknowledgement is lost. |
| Failure after UPDATE before APPEND | No rollback spans the two commands. | Writer setup raises after the mocked UPDATE returns. No APPEND is called. | Visible snapshot and current-row state. |
| APPEND failure and retry | The helper propagates the error. A key with history and no current row is eligible through the anti join and uses the historical max. | APPEND failure propagates. A scripted retry plans against a separate current-set and historical aggregate, then calls APPEND. | Whether retry creates exactly one successor and preserves the expected validity interval. |
| APPEND committed, acknowledgement lost | A retry that sees the committed successor and identical tracked values should select no key. | A mocked timeout propagates. A retry given zero keys skips both writes. | Commit visibility, idempotency and actual duplicate-version risk. |
| Cached keys and lazy history | The three key plans are cached and counted. The successor plan still includes the historical max query, with no explicit materialization of complete successor rows before UPDATE. | The changed-key action and temp view registration precede writes. | Snapshot binding, invalidation, recomputation and version aggregate observed at APPEND. |

## Confirmed defect and potential risks

**Confirmed defect.** UPDATE and APPEND are separate operations. A successful expiration has no compensating rollback if successor creation fails. The source therefore admits a gap where a key has no current row. The two commands also evaluate validity timestamps separately. No persisted incident or actual gap duration was observed.

**Potential risks.** Key deduplication in the selected set does not deduplicate the full incoming rows that join back to that set. Normal execution assumes a unique incoming key and at most one current row per key. A pre-existing duplicate, colliding retry or overlapping writer can invalidate that assumption. Mock counts do not prove at most one current row, unique `(steam_appid, scd_version)`, unchanged historical payloads, or interval continuity. A failure between datasets can expose different publication points, and a retry re-reads Bronze inputs.

**Silver and Gold compatibility.** No table identifier, schema, transformation or tracked field changes. [Gold ETL](../LakeHouse_Project/scripts/silver_to_gold_etl.py) reads current Silver rows. A zero-current-key gap can omit that key from a Gold run or leave the previous Gold value visible. An actual downstream outcome depends on which task and snapshot were read. Existing Gold behavior was reviewed from source only.

## Native follow-up

Use an isolated synthetic warehouse after separate approval. Seed v1 rows, an expired-only key, and an unchanged key. Run initial load, new key, tracked change and exact replay. Record table snapshots, `(steam_appid, scd_version)` uniqueness, current counts, validity timestamps and hashes of historical business fields.

Inject failures before UPDATE, after its commit and before APPEND, during APPEND, and after a committed APPEND before acknowledgement. Read from an independent session at each boundary. Retry frozen input in a fresh session and compare final rows and snapshots against the uninterrupted reference. Capture physical plans and which snapshots feed the key plans and historical max. Repeat with controlled cache eviction and overlapping writers as separate cases. Check Gold's current-key input at the intermediate and recovered snapshots. [Baseline reliability test plan](reliability-test-plan.md)

Review the evidence and define the required post-failure invariant before selecting a recovery change. Keep SCD2 schemas and tracked fields stable, and verify a frozen replay before enabling any repair or new write strategy.
