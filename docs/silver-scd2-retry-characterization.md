# Silver SCD2 failure and retry characterization

## Original two-write implementation

Baseline: canonical main at `bdbf14f`, after PR #10. This phase changes only tests and documentation. The [Silver job at the original implementation](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/blob/06f7b8045c30a8504279e85b3b83484c622b1137/LakeHouse_Project/scripts/bronze_to_silver_etl.py) still processes games, popularity, then reviews in one task. It creates a missing table with version 1 and uses `_upsert_scd2()` for an existing table. The [DAG](../LakeHouse_Project/airflow/dags/bronze_to_silver_etl.py) has one task-level retry, which reruns extraction and all three tables.

## Evidence

The [offline tests at the original implementation](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/blob/06f7b8045c30a8504279e85b3b83484c622b1137/tests/reliability/test_silver_scd2_retry.py) execute the actual functions extracted with AST. Spark plans, counts and write outcomes are scripted. The tests verify call order and arguments. They do not execute joins, evaluate Spark expressions, or inspect Iceberg rows.

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

## Original native follow-up plan

Use an isolated synthetic warehouse after separate approval. Seed v1 rows, an expired-only key, and an unchanged key. Run initial load, new key, tracked change and exact replay. Record table snapshots, `(steam_appid, scd_version)` uniqueness, current counts, validity timestamps and hashes of historical business fields.

Inject failures before UPDATE, after its commit and before APPEND, during APPEND, and after a committed APPEND before acknowledgement. Read from an independent session at each boundary. Retry frozen input in a fresh session and compare final rows and snapshots against the uninterrupted reference. Capture physical plans and which snapshots feed the key plans and historical max. Repeat with controlled cache eviction and overlapping writers as separate cases. Check Gold's current-key input at the intermediate and recovered snapshots. [Baseline reliability test plan](reliability-test-plan.md)

Review the evidence and define the required post-failure invariant before selecting a recovery change. Keep SCD2 schemas and tracked fields stable, and verify a frozen replay before enabling any repair or new write strategy.

## P1 — C02 recovery

Silver now submits one Iceberg `MERGE INTO` per existing table with selected keys. A changed key stages one expiration action and one successor. A new or expired-only key stages only a successor. Both sides of a normal validity boundary use `current_timestamp()` in the same statement. The predecessor's business values and version stay intact. The successor uses `max(scd_version) + 1` across the key's full history, or 1 for a key without history. An unchanged replay sends no write. The initial create-only path, table schemas, tracked fields and source timestamps stay the same.

The upsert rejects missing or unexpected business columns, null or duplicate incoming keys, and multiple current rows for a key in the incoming batch before issuing a write. An expired-only key created by the older split path can resume when it appears in the input. Its previous expiry remains intact, so an existing historical gap can remain.

The project uses Spark 3.3.2 with the Iceberg 1.3.1 Spark 3.3 extension. The tagged Iceberg tests exercise [matched UPDATE with conditional INSERT](https://github.com/apache/iceberg/blob/apache-iceberg-1.3.1/spark/v3.3/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestMerge.java#L391) and [reading the target from the source of a MERGE](https://github.com/apache/iceberg/blob/apache-iceberg-1.3.1/spark/v3.3/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestMerge.java#L1079). The [copy-on-write implementation](https://github.com/apache/iceberg/blob/apache-iceberg-1.3.1/spark/v3.3/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java#L365) commits removals and additions in one overwrite for a table. Gold still reads `is_current = true` and uses the existing source timestamps.

[Offline regression tests](../tests/reliability/test_silver_scd2_retry.py) check the staged action plan, write scope, cleanup and retries against scripted old or committed states. They do not evaluate Spark expressions or observe commits. Before rollout, use a disposable warehouse with the deployed stack to verify one snapshot for a changed batch, zero for an unchanged replay, shared validity timestamps and history preservation. Use an independent reader to check current rows before commit, after a pre-commit failure, and after a successful commit with a lost acknowledgement. Replay frozen input and include an expired-only key and a Gold reader.

Use a single writer for each Silver table during rollout. A competing writer can change the target after staging has classified keys. Snapshot atomicity applies per table. The Silver job still publishes games, popularity and reviews separately, and an Airflow retry can read a newer Bronze input.
