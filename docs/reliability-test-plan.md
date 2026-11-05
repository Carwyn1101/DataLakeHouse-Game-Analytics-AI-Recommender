# M1.1 Reliability test plan — Phase 1

Status: proposed integration suite plus an executed offline characterization scaffold. Date: 2026-11-05 (Asia/Bangkok). Finding IDs refer to [reliability-audit.md](reliability-audit.md).

## 1. Test boundaries and isolation

The scope is the existing Bronze → Silver SCD2 → Gold pipeline and Airflow recovery behavior. No pipeline fixes are included. The matrix below describes **desired acceptance behavior** separately from the predicted/current implementation; unexecuted rows are not claimed as passing.

Integration tests must use a new disposable MinIO service, separate Hive Metastore and metadata database, a separate Iceberg warehouse, and a separate Airflow metadata database/network. Use synthetic data only. Do not reuse the existing Compose project, its fixed container/network names, root source-data mount, `lakehouse` bucket, production catalog, or bind-mounted persisted directories. A test-only Compose definition or equivalent isolated harness must be reviewed before execution; it is not implemented in this phase.

The ETL hardcodes the `hive` catalog, Silver/Gold namespaces, source folder, service names, and storage paths. Merely creating a test bucket or namespace is insufficient. A future harness should inject a test Spark session and test configuration at the function boundary (including complete derived path overrides), map the source folder only inside disposable containers, and assert that every table location and service endpoint belongs to the disposable environment before any write. Run the three DAG definitions only in that isolated Airflow installation. Never call these functions with an unmodified production session to “try a fixture.”

For each case, record input content hashes, batch/attempt ID, fixed replay clock, source timestamps, table snapshot IDs, row counts, canonical business-row hashes, business-key → surrogate-key mappings, and FK anti-join counts. Keep runtime evidence outside Git. The only checked-in scaffold uses in-memory data and mocks, with no service or fixture-file writes.

## 2. Required synthetic fixtures

Use timezone-aware UTC source timestamps: T0 = `2026-01-01T10:00:00Z`, T1 = `2026-01-02T10:00:00Z`, T2 = `2026-01-03T10:00:00Z`. Set a fixed transformation/replay year of 2026 in the future harness. Use a source timestamp after the baseline fact timestamp where a case is explicitly testing newer-input behavior.

### F0 — two-game baseline

Games 101 (“Orbit Quest”) and 202 (“Garden Race”), one review and popularity row per game. All required fields are non-null, release dates are within 2000–2030, and timestamps are T0. Provide these input columns (additional fixture fields can be added only when required by the actual schema):

| CSV | Required fixture columns and representative values for 101 |
|---|---|
| games | `steam_appid=101`, `name=Orbit Quest`, `release_date=2020-06-01`, `developers=Studio A`, `publishers=Publisher A`, `genres=Action, RPG`, `categories=Single-player`, `tags=Space, Exploration`, `steam_currency=USD`, `steam_price_initial=2000`, `steam_price_final=1000`, `required_age=0`, `is_free=FALSE`, `steam_metacritic_score=80`, `recommendations_total=100`, `ts_ingested=T0` |
| reviews | `steam_appid=101`, `steam_review_total_positive=80`, `steam_review_total_negative=20`, `steam_review_total=100`, `steam_review_positive_ratio=0.8`, `steam_review_score_desc=Very Positive`, `ts_ingested=T0` |
| popularity | `steam_appid=101`, `steam_concurrent_players=500`, `steamspy_owners_range=1000 .. 2000`, `steamspy_players_2weeks=100`, `steamspy_average_forever_min=120`, `steamspy_median_forever_min=60`, `ts=T0` |

For 202, use a different name, developer/publisher, genre `Racing`, tag `Casual`, and valid distinct metrics; keep the required column set. This makes lookup/bridge membership and unaffected-key assertions meaningful. The initial Gold model has 18 tables: 10 dimensions, five bridges, and three facts. `dim_date` contains 11,323 rows. Each fact has two rows in this fixture; bridge counts derive from the comma-separated distinct memberships rather than a fixed row count per game.

### Delta and adversarial variants

| Fixture | Construction / purpose |
|---|---|
| F1 insert | Add 303 (“Sky Builder”), a valid row in all three CSVs at T1, with a new developer/tag. Also supply a source-only delta variant containing only 303 to test preservation of target-only keys. |
| F2 tracked update | For 101, change final price from 1000 to 800 and recommendations from 100 to 120 at T1; change review total/ratio and concurrent players at T1. Keep 202 byte-identical. Repeat another tracked change at T2. |
| F3 omitted-field update | Independently change only developer, genre, publisher, category, tag, or release date; only `steamspy_players_2weeks`; only median playtime; only review positive/negative counts while retaining the recorded total/ratio/descriptor. The last variant deliberately tests whether inconsistent component counters are detected or propagated rather than silently ignored. |
| F4 duplicate | Exact duplicate 101; conflicting 101 rows at T0 and T1; reversed input order; conflicting rows sharing T1; valid-T1 and invalid/null timestamp rows sharing 101. Seed a separate legacy Bronze target with duplicate unchanged keys. |
| F5 late/equal | Existing target at T1; source 101 at T0 with different content, then at T1 with different content. For the bridge case change developer plus tracked price so Silver definitely versions despite C04. |
| F6 invalid | Missing/null/invalid key, two different nonnumeric identifiers, missing required column, empty file, header-only file, all-invalid game rows, missing relationship, unrelated source-folder entry, unexpected extra column, and out-of-calendar date. Each is a separate case, not one mixed input. |
| F7 faulted baseline | F0 with captured table snapshot IDs and surrogate maps; inject failures at the exact boundaries in section 5 while applying F1/F2. Include key 101 with a low surrogate key and a retained higher key for 202, so a delete/reallocate retry exposes mapping drift. |
| F8 skew/concurrency | Two disjoint deltas and a same-key conflicting delta, plus controllable barriers between reads and writes. Permit an upstream source/table update only when the scenario explicitly requests it. |

Silver-level unit fixtures may be directly constructed typed DataFrames containing the columns required by `_upsert_scd2`, including nullable tracked values and pre-seeded historical versions. These isolate SCD mechanics from pandas preprocessing; full-pipeline cases still use the raw F0 column contract.

## 3. Invariants and comparison oracles

1. **Bronze current-state grain:** exactly one non-null `steam_appid` per logical game object; updates preserve target-only keys. Read failure must leave the previous object unchanged and fail the task. Late/equal/invalid timestamp conflict handling must follow an approved deterministic policy. Recommended acceptance policy: older records cannot regress newer state; equal-timestamp conflicts are rejected or resolved by a documented stable tie-break; invalid timestamps cannot supersede valid timestamps.
2. **Silver SCD2 grain:** `(steam_appid, scd_version)` is unique; exactly one current version exists for every accepted active key; current expiry is null; expired rows have non-null expiry; version increments once for each accepted business change; historical business values remain unchanged. No new version on unchanged input. For ordered half-open validity intervals, old expiry equals next effective time and the final current interval is open. This continuity is a proposed reliability contract and is not currently guaranteed by the separate UPDATE/APPEND timestamps.
3. **Gold dimension/bridge grain:** unique business key and unique surrogate key in each applicable dimension; existing business-key → surrogate-key mappings stay stable through updates and recovery. Bridges have unique `(game_key, relationship_sk)` memberships matching the current accepted Silver state. Unrelated games and lookup mappings stay unchanged. Whether bridge row surrogate IDs must themselves stay stable across relationship refreshes needs review because current facts reference them.
4. **Gold fact grain:** use the implementation's current-state contract: one row per game per fact table when that source entity exists, not one per run or source event. All game/detail/review/popularity/date references resolve; bridge references resolve or use an explicitly approved unknown-member contract. `-1` must not be silently counted as a valid FK without a corresponding policy. The release date drives fact-game's date; source review/popularity dates drive the other facts.
5. **Idempotency:** compare sorted full business rows, SCD history, and stable dimension mappings after success, retry, and complete DAG rerun. Exclude infrastructure snapshot/commit IDs and log timestamps; record them separately. Include source timestamps in the logical comparison. Bronze and facts currently rewrite on unchanged input; physical commit count is a separate efficiency metric, not a logical idempotency failure.
6. **Failure visibility/publication:** a required read, write, or validation failure must not be reported as successful. Failed/incomplete batches must not be treated as complete by downstream consumers. Validate all affected tables against the same accepted input batch, not merely nonzero counts.

Representative read-only SQL assertions, executed only against the disposable catalog:

```sql
-- Expect no rows for each Silver entity.
SELECT steam_appid
FROM hive.silver.games
GROUP BY steam_appid
HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1;

SELECT steam_appid, scd_version
FROM hive.silver.games
GROUP BY steam_appid, scd_version HAVING COUNT(*) <> 1;

WITH history AS (
  SELECT *, LEAD(effective_date) OVER (
    PARTITION BY steam_appid ORDER BY scd_version
  ) AS next_effective
  FROM hive.silver.games
)
SELECT steam_appid, scd_version FROM history
WHERE (is_current AND expiry_date IS NOT NULL)
   OR (NOT is_current AND expiry_date IS NULL)
   OR expiry_date < effective_date
   OR (next_effective IS NOT NULL AND NOT (expiry_date <=> next_effective));

SELECT steam_appid FROM hive.gold.dim_game
GROUP BY steam_appid HAVING COUNT(*) <> 1;

SELECT game_key FROM hive.gold.fact_game_snapshot
GROUP BY game_key HAVING COUNT(*) <> 1;

SELECT f.* FROM hive.gold.fact_game_snapshot f
LEFT ANTI JOIN hive.gold.dim_game d ON f.game_key = d.game_key;
```

Also assert non-null keys directly, surrogate-key uniqueness, and other FK anti-joins for all dimensions/bridges/facts. Check source expected-key coverage: an entirely missing key is invisible to a GROUP BY over existing rows. For 101 after F0→F2(T1)→F2(T2), expect versions `[1,2,3]`, only 3 current; 202 remains version 1. Compare canonical rows and mappings to a serial uninterrupted reference run from the same fixture.

## 4. Test matrix

Every integration case below is **proposed / not executed**. “Offline” identifies the narrower existing characterization that was executed; it does not change integration status. Current-behavior failures should become explicit expected failures only when a future acceptance suite has actually reproduced them; do not silently relax the oracle to match a defect.

| ID | Scenario / fixture / execution | Desired result | Current behavior or risk / finding / offline evidence |
|---|---|---|---|
| TP-01 | Initial load F0 into empty disposable services; run Bronze, Silver, Gold. | Bronze 2 keys/object; Silver 2 current v1/entity; Gold 18 tables, expected membership, 2 facts/entity, no invalid FKs. | Full path untested. Offline Bronze latest-valid dedup test passes. |
| TP-02 | F0 then F1; also source-only 303 delta. | 303 inserted once; old keys/mappings/history retained; one new current Silver version for 303. | Offline Bronze target-preserving insert passes. Gold key allocation needs integration. |
| TP-03 | F0 then F2 at T1, then T2. | Exactly one successor per changed key per change; 202 unchanged; Gold values/FKs current, mappings stable. | SCD and Gold transaction mechanics untested; C02/C03. |
| TP-04 | Reprocess byte-identical F0 after success three times. | Same logical rows, SCD counts, mappings, and memberships; no added fact rows. | Offline Bronze logical rerun passes but writes every time. Silver skips tracked no-op; Gold facts rewrite by code. |
| TP-05 | F4 exact, timestamp-conflicting, tied, invalid, and reversed duplicates. | One deterministic winner per key or explicit rejection; no duplicate current/history/grain. | Bronze valid latest winner passes offline; invalid winner defect reproduced. Silver first-row policy differs; C07. |
| TP-06 | Repeat successful F2 as a task retry and as whole DAG rerun. | Identical final state to one successful F2 run, including version count and key maps. | Runtime idempotency untested; joins/materialization/concurrency matter. |
| TP-07 | F3 relationship/release-only changes, each separately, at T1. | Approved field changes reach Silver and dependent Gold tables. | Silver tracked fields omit these: expected acceptance failure, C04. |
| TP-08 | F3 popularity/review omitted-field changes. | Changed accepted counters preserved or inconsistent counters explicitly rejected. | May remain stale because change comparison excludes them, C04. |
| TP-09 | F5 equal timestamp plus tracked price and developer change. | Silver versions once and bridges refresh despite unchanged timestamp. | Bronze equal-time update reproduced offline; strict bridge predicate skips it by code, C05. |
| TP-10 | F5 older timestamp, including timestamp-only difference. | Reject/skip or quarantine stale input; no newer-state regression. | Older-content regression reproduced offline; C07. |
| TP-11 | Seed duplicate unchanged Bronze target from F4, then replay matching source. | Detect/reject or repair duplicate target per approved policy; never silently certify clean grain. | Offline target duplicates persist; C07. |
| TP-12 | F7 target read timeout/parse/permission failure; source contains only 101; permit subsequent write. | Task fails, no write, 202 preserved; only actual object absence allows initial creation. | Offline timeout loses 202 and returns success; C01. Test other failures independently. |
| TP-13 | Fail before first write; then fail upload between CSV objects with F1/F2. | Before-write failure preserves old state; incomplete object batch is visible as incomplete; retry converges on a frozen source. | No cross-object manifest; mixed Bronze epochs possible, R03. |
| TP-14 | Each F6 malformed/empty/schema case. | Approved no-op or explicit reject; no accidental replacement; report rejected-row/key counts. | Empty input currently skips; missing key logs/skips; directory/non-CSV may fail arbitrarily. No schema/grain validation. |
| TP-15 | F6 nonnumeric IDs, orphan reviews, rejected/missing games, date outside 2000–2030. | Reject/quarantine bad keys; no null/unresolved FKs; documented deletion/unknown/date policy. | Coercion to zero, no tombstones, left joins and calendar gaps need tests; R04/R06. |
| TP-16 | F7 fail Silver immediately after expiration commits, before append. Inspect state before retry. | No published zero-current-key state; after recovery exactly one v2 and unchanged v1 business data, with valid interval boundary. | Confirmed two-statement gap; retry may restore current state with a temporal gap, C02. |
| TP-17 | F7 let Silver append commit, then simulate acknowledgement loss before task success; retry. | No duplicate version or extra successor; exactly one current row; history/mapping equal reference run. | Version allocation, cached keys, and replay need real engine evidence; C02/R01. |
| TP-18 | Fail Silver after games commits and before popularity/reviews; retry frozen F2, then repeat with deliberate input mutation. | Frozen replay converges; mutated replay is rejected or identified as a new batch; no false batch success. | One Python task retries all tables; no shared input snapshot, R03. |
| TP-19 | For each mutable Gold dimension, fail after DELETE with F7; retry only that task. | Recover attributes and original surrogate mapping; no orphan downstream references. | Missing mapping may be allocated a new key; C03. Use 101 low key, retained 202 high key. |
| TP-20 | Fail after each bridge DELETE/APPEND, and between developer/genre/publisher/category/tag updates. | All memberships recover once, fact bridge references resolve; unaffected games preserved. | Partial five-table state and changing bridge IDs; C03/R01. |
| TP-21 | For each fact, fail after DELETE and after committed APPEND/lost acknowledgement; retry. | Latest facts restored exactly once with valid keys, timestamps and one-row grain. | Rebuild may converge with stable unique inputs; deletion gap confirmed, C03. |
| TP-22 | Fail a Gold dimension dependency after fact-game can succeed; separately remove a bridge only in disposable storage after completed fact at T; replay. | Failed dependencies block verification; rerun repairs missing relationship and does not certify mixed epochs. | Task graph blocks failures but cannot roll back earlier facts; watermark does not check missing bridge, C05/R03. |
| TP-23 | Catalog read failure on existence/max-key probes, then recovery before next write. | Fail without replacement, reset allocation, or collision. | Offline helpers return false/zero. Actual overwrite/collision not proven; C08. |
| TP-24 | Missing/unreadable/empty required Gold table; null keys, duplicate facts, orphan FK. | Verification task fails with actionable invariant/table; downstream publication blocked. | Offline all-reads-fail returns normally; integrity not checked by code, C06. |
| TP-25 | Observe lazy replacement plans across target mutations with F2 in Silver/Gold. | Candidate rows, keys and versions reflect one immutable input state; post-write state equals reference. | Runtime-dependent self-read/cache invalidation risk, R01. Capture plans and actual table snapshots. |
| TP-26 | F8 two overlapping Bronze/Silver/Gold runs at read barriers; test disjoint and same keys. | Serial-equivalent results or explicit conflict/failure with safe retry; unique keys/versions. | No scoped explicit serialization; exact Iceberg conflict handling untested, R02. |
| TP-27 | Pre-seed duplicate current Silver/dimension keys and null fact keys, then run affected tasks. | Reject or repair before publication, no join fan-out or retained null-key duplicates. | No enforced uniqueness; SQL IN does not remove null matches; R06. |
| TP-28 | Replay unchanged raw data with controlled clock/year boundary and missing source timestamps. | Behavior follows approved replay-time contract; no accidental unexplained SCD churn. | Year/now-derived fields can change; R05. |
| TP-29 | Stop/restart disposable MinIO/Hive/worker; delayed startup and failed schema initialization; restore disposable catalog/object backup. | Clear failed task state, consistent tables after reviewed recovery, no false initialization success. | Restart/health checks present but no recovery drill executed; R07. Requires approved environment setup. |

## 5. Fault injection and recovery protocol

The future harness must call actual functions with test sessions, rather than reproducing ETL behavior in a model. Wrap only the test Spark session's SQL/write boundaries or disposable service client. Do not edit production source to add a crash.

1. Start from a fresh isolated F0 state, capture all snapshot IDs and surrogate maps, and freeze the F1/F2 input. Save an uninterrupted reference result in a separate disposable namespace/environment.
2. Inject a single exception **before** the first mutation. Confirm no committed state changes and that the exception is visible to Airflow.
3. Inject after `spark.sql(UPDATE ...)` returns for Silver or `spark.sql(DELETE ...)` returns for Gold, immediately before the corresponding append starts. The first statement must genuinely commit; failing the entire transaction before commit would not test the observed gap.
4. Independently inject after append commit but before the operator reports success. Verify with a separate session that the commit exists before retrying. This tests ambiguous success, distinct from append rejection before commit.
5. Inspect current rows, history, keys, FKs, and table snapshot changes while the job is failed. Do not hide an inconsistent intermediate state by checking only after retry.
6. Remove the one-shot injection and retry the same callable, then use isolated Airflow to retry the failed task and clear/rerun the whole DAG in separate cases. The DAGs allow one automatic retry after five minutes; test both recovery on that retry and exhausted retries remaining visibly failed. Keep the production delay unchanged; a test-only clock or scheduler harness may accelerate observation.
7. Compare with the uninterrupted oracle using the invariants in section 3. Record current-state recovery separately from historical interval continuity and surrogate stability. A successful task status or a matching total count alone is insufficient.
8. Repeat with upstream mutation/concurrency only for TP-18/25/26. Any changed input must have a separate captured identity; do not attribute a new business version to retry of the old batch.

If the existing implementation cannot recover safely, stop the test and preserve isolated evidence. Design a later repair around captured mappings/input/snapshots; do not “fix” it by truncating tables or deleting production data. Any cleanup may remove only explicitly identified disposable resources after verifying resolved paths and volume names.

## 6. Implemented scaffold and actual execution

File: [tests/reliability/test_characterization.py](../tests/reliability/test_characterization.py).

The scaffold compiles selected function definitions from the actual ETL files with Python AST. It bypasses module imports/configuration, supplies in-memory CSVs and a fake MinIO client, and mocks Spark failures. It does not emulate SCD/Gold writes or assert distributed transaction correctness. Production function names must exist or loading fails. Bronze cases require pandas and are skipped with an explicit reason if it is unavailable; skipped tests are not passes.

Portable invocation from the repository root:

```powershell
python -B tests/reliability/test_characterization.py -v
```

Actual invocation used on this workstation:

```powershell
& 'C:/Users/PC/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe' -B tests/reliability/test_characterization.py -v
```

Executed result on 2026-11-05: **11 tests passed, 0 skipped, 0 failures**. Python 3.12.14; pandas 3.0.1; NumPy 2.3.5. Eight Bronze cases cover initial deduplication, incremental insertion, equal-timestamp update, unchanged rerun, older-input regression, read-error data loss, invalid-timestamp selection, and retained legacy duplicates. Three service-mock cases cover Silver existence probing, Gold existence/key probing, and verification returning despite all failed reads.

These are characterization assertions: several tests pass precisely because a known defect is reproduced. They are not a release gate. No SCD2 Spark operation, Iceberg write, actual object write, Docker startup, DAG retry, or integration-matrix case was executed. PySpark, Airflow, and MinIO packages are absent; Docker and `gh` are unavailable on PATH. No installation or runtime setup was attempted.

## 7. Review and future release gates

- Review and approve the confirmed-defect priorities, late/equal timestamp policy, complete tracked-field list, half-open SCD interval contract, current-state fact grain, unknown-member/deletion behavior, and scope of stable surrogate keys.
- Approve and prepare the isolated integration harness. Verify complete endpoint/path isolation before enabling fault injection.
- Reproduce P1 acceptance failures, then implement only reviewed targeted fixes in a later phase. Preserve these characterization results as baseline evidence; update defect-expecting tests deliberately when behavior changes and add positive acceptance assertions.
- Before release, pass initial/incremental/unchanged/duplicate/rerun/history cases and all P1 failure/recovery cases with snapshot/key evidence. Exercise Airflow task-level and whole-DAG retries. Record untested deployment scenarios as remaining limitations rather than inferred passes.

Phase 1 is complete at documentation and characterization. Implementation remains pending review.
