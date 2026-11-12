# Gold catalog and surrogate key safety

## Behavior

The [Gold helpers](../LakeHouse_Project/scripts/silver_to_gold_etl.py) previously converted every existence error to `False` and every max-key error to `0`. Those defaults could reach replacement or allocation code after an unreadable target.

- `table_exists()` resolves the named `hive` catalog and supplies the `gold` namespace and table name as a V2 identifier through the Spark JVM bridge. Spark 3.3.2's Python catalog wrapper checks the V1 session catalog. The V2 default uses `loadTable()` and catches only Spark's typed `NoSuchTableException`. Iceberg 1.3.1 translates its typed absence to that exception. Connection, authorization and unexpected errors propagate. An existing empty table is still present. [Spark V1 lookup](https://github.com/apache/spark/blob/v3.3.2/sql/core/src/main/scala/org/apache/spark/sql/internal/CatalogImpl.scala#L224) · [Spark V2 lookup](https://github.com/apache/spark/blob/v3.3.2/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableCatalog.java#L133) · [Iceberg lookup](https://github.com/apache/iceberg/blob/apache-iceberg-1.3.1/spark/v3.3/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java#L149)
- `get_max_key()` uses one aggregate query for `max(key)` and row count. Only a successful zero-row result returns the empty-table default. A nonempty table with no non-null maximum raises. Lookup, Spark schema analysis, aggregation, collection and conversion errors escape. Valid maxima and `max + row_number()` are unchanged.

## Complete production caller inventory

Every target below has the `hive.gold.` prefix. There are 15 existence-check sites and 10 max-key sites, including one nested bridge helper called for five tables. None of the max-key callers intentionally uses a missing-table default.

| Caller | Existence target and false branch | Max-key target and timing |
| --- | --- | --- |
| `get_changed_game_keys()` in `create_dims_and_bridges()` | `fact_game_snapshot`. Select all current Silver games only on absence. | None. A failed fact lookup now aborts instead of forcing the all-game path. |
| `create_dim_date()` | `dim_date`. Initial calendar creation. | None. |
| `create_dim_game()` | `dim_game`. Initial game dimension creation. | `game_key` before refresh DELETE/APPEND. |
| `create_dims_and_bridges()` lookup paths | `dim_developer`, `dim_genre`, `dim_publisher`, `dim_category`, `dim_tag`. Initial lookup creation. | `developer_sk`, `genre_sk`, `publisher_sk`, `category_sk`, `tag_sk`, respectively, before append when new values exist. |
| Nested `_upsert_bridge()` in `create_dims_and_bridges()` | `bridge_game_developer`, `bridge_game_genre`, `bridge_game_publisher`, `bridge_game_category`, `bridge_game_tag`. Initial bridge creation. | `game_dev_key`, `game_gen_key`, `game_pub_key`, `game_cate_key`, `game_tag_key`, respectively, **after** the refresh DELETE and before APPEND. |
| `create_dim_detail_info()` | `dim_detail_info`. Initial detail creation. | `game_detail_key` before refresh DELETE/APPEND. |
| `create_dim_review_info()` | `dim_review_info`. Initial review dimension creation. | `review_key` before refresh DELETE/APPEND. |
| `create_dim_popularity_info()` | `dim_popularity_info`. Initial popularity dimension creation. | `popularity_key` before refresh DELETE/APPEND. |
| `create_fact_game_snapshot()` | `fact_game_snapshot`. Initial fact creation. | None. |
| `create_fact_review_snapshot()` | `fact_review_snapshot`. Initial fact creation. | None. |
| `create_fact_popularity_snapshot()` | `fact_popularity_snapshot`. Initial fact creation. | None. |

The false branches that create tables use 14 `createOrReplace()` sites covering 18 tables. Each site could replace a table if the probe falsely reported absence or if another run created the table after the probe. Fail-closed lookup prevents the old catch-all from selecting that branch. The check and create still run separately. Keep the create-only change for separate review.

## Deferred risks and native checks

- Two writers can still read the same maximum and allocate overlapping ranges. The helper does not serialize allocation with the write.
- Bridge refresh deletes rows before looking up the maximum. A failed lookup stops the append but can leave the preceding delete visible. Lookup/dimension/fact operations also publish separately, and earlier writes in a multi-table task are not rolled back.
- Validate the pinned stack with a disposable catalog and synthetic Gold tables. Check absent, existing and empty tables, denied/outage cases, and aggregation schema or read failures. Then separately reproduce concurrent creation, overlapping allocation, and failure after bridge deletion. Compare snapshots, key uniqueness, stable mappings and foreign-key references.

The patch preserves Gold schemas, table identifiers, allocation formulas, writers, DAG dependencies and downstream contracts. [Offline tests](../tests/reliability/test_characterization.py) exercise helper and representative caller control flow. Native Spark/Iceberg and persisted data were not accessed.
