# M1.1 Reliability and recovery audit — Phase 1

Audit date: 2026-11-05 (Asia/Bangkok). Scope: inspection, characterization, and test design only. No production fixes or persisted-data operations were performed.

## 1. Provenance and scope

Requested repository: `Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender`; PR base inspected: `main` at `0962090a97371a7d9ba61367a46e9080cd347b22`. Requested audit branch: `feature/m1-reliability-audit`.

The local checkout initially pointed to `Carwyn1101/KLTN`, on `main` at `62d6605543e590663d239ae175d0956cd180f806`. It contained six staged paths (including Compose and AI evaluation work) and five untracked user files. They must remain outside this audit's commit. The local feature branch was created without resetting, stashing, staging, or committing that work. The review branch in the requested repository is based on its own `main`; the local checkout and remote review branch have different base histories. The existing origin is not repointed.

All seven scoped files were read locally and compared with the requested repository. Gold and the three DAG definitions match after newline normalization. Bronze and Silver differ only by inline comments; local Compose additionally contains a pre-existing Streamlit evaluation environment flag. These differences do not change the audited pipeline behavior. Line references below apply to both versions.

| Reference | Implementation inspected |
|---|---|
| B | [rawData_to_bronze.py](../LakeHouse_Project/scripts/rawData_to_bronze.py) |
| S | [bronze_to_silver_etl.py](../LakeHouse_Project/scripts/bronze_to_silver_etl.py) |
| G | [silver_to_gold_etl.py](../LakeHouse_Project/scripts/silver_to_gold_etl.py) |
| DB | [auto_upload_data.py](../LakeHouse_Project/airflow/dags/auto_upload_data.py) |
| DS | [Silver DAG](../LakeHouse_Project/airflow/dags/bronze_to_silver_etl.py) |
| DG | [Gold DAG](../LakeHouse_Project/airflow/dags/silver_to_gold_etl.py) |
| C | [docker-compose.yml](../LakeHouse_Project/docker-compose.yml) |

Evidence labels:

- **Executed characterization**: actual selected function bodies ran against synthetic in-memory inputs or service mocks. This does not validate real storage behavior.
- **Confirmed by code**: the implementation explicitly has the stated behavior or missing safeguard. Conditional failure consequences are explained; no real incident is claimed.
- **Potential runtime risk**: depends on Spark behavior, concurrency, deployment settings, or input contracts and needs integration testing.

Severity: P1 = loss, missing/stale data, broken recovery, or false successful publication; P2 = duplicate handling, determinism, observability, or efficiency. This is a reliability audit, not a credential/security audit; no credential values are reproduced.

## 2. Existing behavior and safeguards

### Bronze ingestion

`detect_and_upload` lists every source-folder entry on every invocation (B:219–231). `uploaded_files` is an invocation-local set, not a durable checkpoint. No filename whitelist, extension check, or batch manifest is present. The active function is `upsert_to_minio`; the older append helper is not called by this DAG.

For each object, the code reads the entire existing CSV and local CSV. It deduplicates local rows on `steam_appid`, sorting by normalized UTC timestamp when available (`ts` for popularity, `ts_ingested` otherwise), or keeping the last input row without a timestamp (B:112–120). It compares shared columns and timestamps against the last target row per key. New and changed source keys replace their previous target rows; target-only keys survive ordinary successful reads (B:137–204). This is current-state/SCD1 ingestion, not Bronze event-history retention. A successful write replaces the whole CSV object, even with zero logical delta (B:206–213).

Useful existing safeguards: empty input skips writes, missing business-key columns are logged and skipped, source duplicates are reduced, timestamp parsing is normalized, same-timestamp content changes are supported, target-only records are normally retained, and counts are logged. An unchanged clean dataset remains logically stable in the executed fixture, although it still incurs an object write.

### Silver SCD Type 2

The job reads all three Bronze CSVs, converts them to pandas, cleans them, writes temporary Parquet, then processes Silver tables sequentially in the order games, popularity, reviews (S:419–495). Each preprocessor deduplicates by business key using the first row before numeric coercion (S:88–94, 204, 300, 359). This differs from Bronze's latest-timestamp policy.

Initial tables get `effective_date`, null `expiry_date`, `is_current=true`, and `scd_version=1` (S:107–113, 487–494). Existing-table processing compares only the configured tracked columns, using null-safe comparisons against current rows. It caches changed/new key sets, computes version `max(history)+1`, expires changed current rows with UPDATE, then appends new versions (S:129–192). New keys are detected against **current rows**, despite the comment saying no existing entry at all. This allows retry to reinsert a key whose previous current row was expired before a crash.

Under unique, clean input, a fixed transformation date, no concurrency, and successful writes, a rerun with unchanged tracked values inserts no versions. Timestamp-only changes do not create a version or refresh the stored timestamp. There is no source-event ledger or deletion/tombstone processing. Failures normally propagate; the `finally` block removes only the job's temporary directory and stops Spark (S:498–501).

### Gold incremental dimensions, bridges, and facts

- `dim_date` is created once for 2000–2030 and skipped when present (G:135–189).
- `dim_game`, `dim_detail_info`, `dim_review_info`, and `dim_popularity_info` compare projected business fields, retain surrogate keys for changed rows during an uninterrupted update, and allocate new keys as target maximum plus `row_number`. Changed rows use DELETE followed by APPEND (G:233–279, 579–619, 666–706, 755–795).
- Developer, genre, publisher, category, and tag lookup dimensions append distinct previously unseen names via an anti-join (G:360–517). Existing names and keys are retained during serial successful runs.
- Bridges are created from all current games initially. Later, `get_changed_game_keys` chooses games using current Silver `ts_ingested > fact_game_snapshot.snapshot_ts`, or missing fact rows. Selected games' bridge rows are deleted and rebuilt; bridge surrogate keys are reassigned from the remaining maximum (G:76–100, 303–352).
- The three fact functions rebuild rows from **all current corresponding Silver rows**, deleting target rows for the resulting game keys and appending the result (G:835–873, 887–915, 929–957). They do not append a historical snapshot per run or limit work to changed keys. `snapshot_ts` is the source timestamp, not processing time. Successful serial reruns with unique joins and non-null keys can preserve logical rows, while still generating writes and temporary deletion windows.
- Facts retain references to one selected bridge row per relationship type. Missing bridge references become `-1`, with no corresponding unknown-member creation in the scoped code. Missing game/detail/date/review/popularity matches can remain null. No uniqueness or referential-integrity assertions run.

The word “snapshot” therefore describes latest/current fact rows per game in this implementation. Historical fact retention is an unresolved business requirement, not automatically a defect. Updating current-state dimensions also changes the interpretation of any externally retained old fact references.

### Airflow and deployment recovery already present

All DAGs configure one retry after five minutes and `catchup=False` (DB:6–17; DS:7–19; DG:18–32). Bronze is scheduled every two minutes. Silver and Gold have no schedule and run when manually or externally triggered. The scoped DAGs contain no cross-DAG dependency, shared batch ID, snapshot pinning, explicit pool, `max_active_runs`, or execution timeout; effective global limits may exist outside this scope.

Gold orders database → date → game → bridges → detail → review → popularity dimensions. Fact-game can begin after detail and bridges, fact-review after review, and fact-popularity after popularity; these branches can overlap. Verification waits for all three fact tasks (DG:48–60). Default task-success dependencies prevent downstream execution after an unhandled upstream failure, but do not roll back previously committed tables.

Compose provides persistent PostgreSQL volumes, MinIO/Hive/other bind mounts, PostgreSQL/Redis and Airflow health checks, database migration initialization, and restart policies for several services. These support process restart and metadata persistence, not business-level recovery. Airflow's shared dependencies gate on PostgreSQL and Redis, not MinIO/Hive readiness (C:32–37). Hive gates on its database and MinIO startup, then probes port 9083 (C:300–324). The schema initialization command treats any schema-tool failure as “already exists”; a transient initialization failure may be obscured. MinIO lacks an explicit health check/restart policy in this file. No coordinated object/catalog backup or restore procedure is specified here; none was tested.

## 3. Confirmed issues

| ID / severity | Evidence and trigger | Impact and recovery implication | Validation |
|---|---|---|---|
| C01 / P1 | B:89–95 catches every target-read/CSV error and substitutes an empty target; B:122–135 then accepts source-only replacement. | A transient read failure followed by a successful write can discard target-only keys. A retry cannot restore keys absent from the source and new target. Missing objects must be distinguished from failed reads. | Executed with target `{101,202}`, source `{101}`, mocked timeout: result `{101}`. TP-12. |
| C02 / P1 | S:179–192 commits expiration and insertion separately; two independent timestamps are used. | A failure after UPDATE leaves a business key with history but no current row. Frozen-input retry can discover it as new and append `max+1`, but does not restore the original validity boundary. Readers see incomplete state between commits; contiguous SCD intervals are not guaranteed. This is a confirmed transaction gap, not an observed storage incident. | Code trace; real fault injection not run. TP-16/17/18. |
| C03 / P1 | G:272–279, 614–619, 701–706, 790–795 and bridge/fact DELETE/APPEND sequences. | A failure after DELETE leaves missing rows. Dimension retry derives keys from the now-incomplete target and can assign a different surrogate key, leaving old facts/bridges orphaned. Facts can restore latest rows on frozen-input retry if dimensions remain stable, but cannot ensure all tables represent the same run. | Code trace; real commit and key behavior not run. TP-19/20/21. |
| C04 / P1 | S:35–50 omits fields consumed downstream. | A games-only change to developer/genre/publisher/category/tag or release date is ignored if no tracked field changes. Popularity's `steamspy_players_2weeks` and median playtime can change without a Silver version. Review positive/negative totals can change while tracked ratio/total remain fixed. Gold never receives these updates. | Confirmed comparison coverage; synthetic end-to-end tests proposed. TP-07/08. |
| C05 / P1 | G:82–94 checks strict source timestamp ordering, not Silver version or bridge contents. B:166–168 explicitly permits equal-timestamp changes. | With an existing fact at T, a Silver version whose timestamp is T (or older) is not selected for bridge refresh. Example: change developer plus a tracked price at T: Silver accepts a version, but bridges stay stale. If the fact task succeeds while a bridge is externally removed later, a rerun at T also cannot detect the missing bridge. Bridge-only changes additionally hit C04. | Confirmed predicate mismatch; runtime examples proposed. TP-09/22. |
| C06 / P1 | G:993–1023 catches table/query failures, logs warnings, returns counts, and announces success; zero counts are logged as ERROR without failing. | Airflow can report verification success with missing/unreadable tables. Counts/sample joins do not enforce SCD, uniqueness, null-key, or FK contracts. | Executed: all 18 table reads and the sample query failed; function returned normally with 18 null counts. TP-24. |
| C07 / P2 | B:158–168 compares timestamps as content and permits any content difference when newer-timestamp comparison is false; B:116–117 sorts invalid timestamps last. | Older input can overwrite newer content, even when only its timestamp differs. An invalid timestamp can win over a valid duplicate. Equal-timestamp conflicting rows lack an explicit tie-break contract. Unchanged legacy target duplicates are not repaired because final assembly uses the original target (B:203). | Older overwrite, invalid winner, and retained duplicate cases executed. TP-05/10/11. |
| C08 / P1 | S:98–104; G:58–73 convert any table lookup failure to “absent” and any key lookup failure to zero. Initial paths use `createOrReplace`. | A transient catalog/authorization/schema error can select replacement or incorrect allocation paths if a subsequent operation succeeds. Successful replacement, key collisions, and metadata loss require integration reproduction; the error misclassification itself is confirmed. | Service-error-to-false/zero executed. TP-23. |

## 4. Potential risks and input-contract questions

| ID | Risk, evidence, and boundary of certainty | Next test / decision |
|---|---|---|
| R01 / P1 | Gold replacement DataFrames depend on target tables later deleted. Calling `count()` does not make the later write use a persisted result. Spark may retain a pre-delete snapshot or recompute against the mutated target. Silver's cached keys and lazy history aggregation also need examination across UPDATE. Do not assert that every append loses data or doubles versions without observing the deployed Spark/Iceberg behavior. | TP-25: capture physical plans and snapshot IDs before/after mutation; validate materialization and cached-key behavior. |
| R02 / P1 | Concurrent runs may read the same Bronze object and lose each other's changes; `max+row_number` may allocate duplicate keys; two SCD runs can both plan successors. No explicit writer serialization is in scoped code. Iceberg conflicts may reject some writes, but are not proof of application-level uniqueness. | TP-26; inspect actual Airflow global concurrency settings before assigning incident likelihood. |
| R03 / P1 | Bronze objects, Silver tables, and Gold tasks are read/written independently. A partial batch, an upload during extraction, or a Silver change during Gold can mix source epochs. No common input manifest or pinned snapshot set is passed. | TP-13/18/22; decide the minimum publication/batch-completeness contract. |
| R04 / P2 | Invalid keys are coerced to zero after preprocessor deduplication (S:84–94, 278–290, 337–350, 389–399). Distinct malformed identifiers may collapse to the same business key. Empty/all-invalid datasets, unexpected source-folder entries, schema drift, and out-of-calendar dates lack strong validation. | TP-14/15/27; define reject/quarantine policy and date coverage. |
| R05 / P2 | Derived `game_age_years`/tracked `age_category` use wall-clock year (S:33, 240–248), and missing timestamps use wall-clock now (S:272–276, 383–387). Identical raw bytes across dates or first-load retries may yield different outputs. SCD omits timestamp-only refreshes. | TP-28; distinguish source-change versions from policy-driven aging and specify replay date. |
| R06 / P2 | Retained Silver current rows are not expired when source rows disappear or are rejected. Fact deletion uses only incoming game keys; null keys do not match SQL IN predicates, and left joins can fan out when dimensions contain duplicates. | TP-15/27; decide deletion semantics, unknown-member handling, and fact grain. |
| R07 / P2 | Container health/restart checks and persistent volumes do not prove catalog/object consistency after host failure. Mutable image tags, readiness gaps, ignored schema-init failures, and the host-specific data mount may affect repeatability. | TP-29; isolated deployment recovery drill only, with no production mounts. |

## 5. Failure and recovery walkthrough

| Failure point | Visible state | Existing retry behavior and limitation |
|---|---|---|
| Before the first write | Previously committed data stays unchanged; temporary work may exist. | One Airflow retry can recover if input and dependencies remain available. A Bronze read exception is an exception to this safety expectation because C01 suppresses it. |
| Silver after expiration, before append | Old row remains as expired history; no current row for affected keys. | Frozen-source replay can append the next version using history maximum. It does not make the original two-step operation atomic or recover the original transition time. Check every table, not only the failed key. |
| Silver after one table completes | Games may have new history while popularity/reviews remain old. | The entire Python task retries; already-complete tables should skip unchanged tracked input, then remaining tables can converge. Input changes between attempts invalidate that expectation. |
| Gold dimension after DELETE | Missing business-key row and lost in-table surrogate mapping. | Automatic task retry can allocate a new key. Restoring logical attributes alone is insufficient; compare old/new mappings and downstream FKs before publication. |
| Bridge task midway through five relationships | Some lookup dimensions/bridges updated, others old or missing. | Whole bridge task retries. A still-pending fact watermark may keep keys eligible for newer-timestamp input; equal/older timestamps and already-advanced watermarks can skip repair. |
| Fact after DELETE, before APPEND | Missing fact rows for processed keys. | Retry can rebuild from current dimensions/Silver, subject to unique joins and a stable source snapshot. The three facts are not committed as a unit. |
| Append commits, worker loses success acknowledgement | Data may be fully committed while Airflow marks an attempt failed. | Retry must reconcile business state, not assume the append did not happen. Serial clean-state paths often converge; duplicate input, null joins, concurrency, or lazy recomputation must be tested. |
| A Gold dependency fails after fact-game succeeds | Gold can be partly refreshed; verification is blocked by default task-success dependencies. | Retrying the failed task resumes that branch; restarting an entire DAG rereads current data. Neither procedure establishes an all-table transaction. |

No automatic rollback, compensating operation, durable recovery journal, or multi-table transaction was found in the scoped files. Iceberg table/snapshot mechanisms are relevant to a future recovery procedure, but retention, snapshot availability, rollback permissions, and a coordinated restore were not inspected or exercised. Do not roll back or delete any existing persisted data as part of this phase.

## 6. Recommended fixes for a later phase, ordered by severity

1. **P1 — Fail closed on unreadable Bronze/catalog state (C01/C08).** Treat only a verified missing object/table as initial load; propagate read, parse, permission, and connectivity failures. Never reset allocation to zero because a lookup failed.
2. **P1 — Make Silver transitions recoverable (C02).** Evaluate a single atomic table operation for expiration plus insertion, or a durable staged transition with deterministic replay and a shared transition timestamp. Preserve the existing SCD model. Prove TP-16/17 before rollout.
3. **P1 — Remove Gold deletion gaps while preserving mappings (C03/R01).** Evaluate targeted atomic upserts/replacements supported by the deployed Iceberg version; persist key mappings and inputs before mutation. Cover both crash-before-append and lost-acknowledgement retries. Do not rebuild the warehouse.
4. **P1 — Propagate all intended changes and repair bridges independently (C04/C05).** Agree tracked business fields, use source version/content state rather than only strict timestamps, and detect missing relationships without relying solely on fact-game state.
5. **P1 — Enforce publication checks (C06/R03).** Fail verification on required missing tables, invalid keys, duplicate grain, SCD violations, or orphan FKs. Define a completed-batch boundary while retaining Airflow/Spark/Iceberg/MinIO.
6. **P1 — Bound overlapping writers (R02).** Verify deployment limits, then add narrowly scoped orchestration serialization and conflict/retry tests where needed.
7. **P2 — Specify deterministic input/replay contracts (C07/R04–R06).** Define late/equal/invalid timestamps, duplicate winner, null/invalid IDs, deletions, fact retention, and processing-date behavior. Add metrics for rejected rows and actual logical deltas.
8. **P2 — Exercise operational recovery (R07).** Document compatible pinned runtime versions, service readiness, timeout handling, and coordinated catalog/object backup/restore in an isolated environment.

These are proposals, not implemented changes. Review the test plan and priorities before Phase 2.

## 7. Execution status and limitations

The [offline characterization scaffold](../tests/reliability/test_characterization.py) executed **11 tests, all passed**, with Python 3.12.14, pandas 3.0.1, and NumPy 2.3.5. Assertions intentionally confirm both existing safeguards and undesirable current behavior; passing does **not** mean the pipeline meets the acceptance criteria. Actual production function bodies are AST-selected to avoid importing/initializing Airflow, PySpark, MinIO, or credential-bearing configuration. CSV fixtures and object storage are entirely in memory; no real endpoint is contacted.

PySpark, Airflow, and the MinIO Python package are absent from the available Python runtime. Docker and GitHub CLI were not found on PATH. No packages were installed, Docker services started, Airflow DAGs triggered, Spark jobs run, or persisted datasets queried/changed. No distributed integration, SCD write, Iceberg commit-failure, or deployment recovery test was executed. An initial long shell comparison hit a Windows packaged-shell sandbox launch error; a shorter approved Git blob comparison confirmed Gold's content match. GitHub operations use the connected GitHub tool instead of the unavailable CLI.

Read-only validation also parsed all six scoped Python files successfully, checked the three new files for trailing whitespace and local Markdown link targets, and verified the calendar fixture count. Before/after SHA-256 checks matched for all seven scoped production files. The staged-diff Git object hash stayed unchanged, the unstaged tracked diff stayed empty, and all pre-existing untracked user paths remained present. These checks validate scope preservation and document consistency, not ETL runtime correctness.

See [reliability-test-plan.md](reliability-test-plan.md) for fixtures, acceptance assertions, fault injection, test status, and release gates. Phase 1 stops here pending review.
