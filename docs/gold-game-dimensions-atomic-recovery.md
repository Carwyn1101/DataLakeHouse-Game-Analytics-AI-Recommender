# Gold C03 Phase 1B: atomic refresh of detail, review, and popularity dimensions

The baseline commits a DELETE by AppID before appending a lazy DataFrame that reads the target. If execution stops between writes, a fact can lose its dimension join, and the next task attempt can classify a deleted AppID as new and assign a different key. Counting planned rows before the DELETE does not materialize the DataFrame used for the APPEND. The [Gold DAG](../LakeHouse_Project/airflow/dags/silver_to_gold_etl.py) retries each task once after five minutes.

## Contracts and decision

| Dimension | Current Silver source and tracked fields | Existing surrogate key and consumer |
| --- | --- | --- |
| `dim_detail_info` | `games`, latest current row per AppID by descending `ts_ingested`. Tracks Metacritic score, initial/final price, USD currency, derived discount, recommendations, age/price categories and VND price/category. | `game_detail_key` in `fact_game_snapshot` |
| `dim_review_info` | `reviews`, all current rows. Tracks positive/negative counts, total, positive ratio, score description, volume category and quality score. | `review_key` in `fact_review_snapshot` |
| `dim_popularity_info` | `popularity`, all current rows. Tracks concurrent players, owner range and midpoint, two-week players, average/median playtime, popularity and engagement categories. | `popularity_key` in `fact_popularity_snapshot` |

Each fact joins its dimension by `steam_appid` and stores the corresponding surrogate key. The refresh keeps the highest existing key for a changed AppID, allocates new keys as `int(max_key + row_number())` ordered by AppID, and keeps the null-safe tracked-column comparison. A changed AppID replaces all its old rows with all selected payload rows. Unchanged duplicates and target-only rows stay in place. Initial load, schema and fact joins retain their existing implementations.

A frozen source with separate DELETE and APPEND would still leave two commits. Updating in place with an AppID match or a highest-key match would retain old target duplicates that the previous refresh removed. Full overwrite would rewrite unrelated rows in these unpartitioned tables. A staged MERGE can replace only affected AppIDs in a single target write.

The three incremental paths now stage payloads and assigned keys to private temporary Parquet. The count and MERGE read the same files. Only a nonempty stage issues a MERGE. Its source is a direct Parquet relation to avoid the Spark 3.3.2 temp-view analyzer path in [Iceberg #7766](https://github.com/apache/iceberg/issues/7766). The SQL follows the [Phase 1A pattern](gold-dim-game-atomic-recovery.md), which passed [native validation for dim_game](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/43557820489/job/132334558873).

The incremental review and popularity paths reject more than one current Silver row per AppID, including identical payloads. They write the selected source to attempt-local Parquet before checking its AppID counts. Key allocation, change detection and MERGE staging read those same files. Rejections raise `DuplicateCurrentInputError` before reading or mutating the Gold target beyond its existence check. A singleton null AppID retains the previous behavior. More than one null AppID forms a duplicate group. The initial-load paths retain their existing input policy.

Native run [43581189093](https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender/actions/runs/43581189093/job/132406058746) for the previous candidate measured a replay collision in the conflicting-new popularity fixture. The historical run preserves the comparison evidence. Duplicate input now requires an upstream correction before retry. This guard does not repair existing Gold keys or dimension rows.

## Native acceptance gates (TP-19 / TP-25)

Use the [reliability test plan](reliability-test-plan.md) on Spark 3.3.2 / Iceberg 1.3.1 in an approved disposable catalog. The offline tests check plan construction, exact MERGE SQL, staging lifetime and scripted retry control flow.

- **TP-19, all three:** Seed each schema through its ordinary initial path. Include a changed AppID with the retained lower key, a different AppID with a higher key, a new AppID, a changed duplicate target, unchanged duplicates, and a target-only AppID. Assert the full row multiset and the retained key of each changed AppID. Seed references to surviving keys in `fact_game_snapshot`, `fact_review_snapshot` and `fact_popularity_snapshot` respectively and verify that each still resolves.
- **TP-19, detail:** Include multiple current rows with distinct ingestion times and a historical row. Assert the chosen current payload including the derived discount and currency.
- **TP-19, review and popularity:** Reject exact and conflicting duplicates for existing and new AppIDs, including on frozen-input retry. Assert zero target writes, unchanged rows and snapshots, preserved fact-key references, and input scratch cleanup. Characterize the pinned historical implementation separately.
- **TP-25, all three:** Capture schema, staged values and snapshot IDs. Compare normal completion with an injected pre-commit failure followed by frozen-input retry, and with a commit followed by a lost acknowledgement and frozen-input retry. For unambiguous inputs, expect one new snapshot on success, none for pre-commit failure and none for unchanged or post-commit replay. An independent reader must see either the old row multiset or the whole committed result.
- **TP-25, per schema:** Check Parquet types, nullability, Spark executor file visibility and scratch cleanup on success/failure. Cover detail price/discount and VND values, review counts/ratio, and popularity owner counts and fractional playtimes. Validate the topology and any custom required fields before rollout.

Keep merge on **HOLD** pending native evidence and Independent Tech Lead review. The design assumes one writer per target and executor access to the scratch files. It does not repair already lost keys or coordinate concurrent max-key allocation.
