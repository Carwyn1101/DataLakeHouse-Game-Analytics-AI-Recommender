# Gold verification fail-closed behavior

The final [Gold DAG task](../LakeHouse_Project/airflow/dags/silver_to_gold_etl.py) calls [`verify_gold_layer()`](../LakeHouse_Project/scripts/silver_to_gold_etl.py) after the three fact tasks. It checks the same 18 Gold table identifiers. The [baseline audit](reliability-audit.md) records the previous failure in C06.

| Condition | Before | After |
| --- | --- | --- |
| Required table lookup or count fails | Logs the exception, records `None`, and returns normally. | Raises a verification error with the table identifier and exception type. The task can fail or retry. No success result is returned. |
| Input-dependent table has zero rows | Logs `ERROR` and returns `0`. | Returns `0` as a valid count. A lookup, bridge, dimension or fact can be empty when no input rows qualify. |
| `hive.gold.dim_date` has zero rows | Logs `ERROR` and returns `0`. | Raises. The builder generates a fixed 2000–2030 calendar independent of source rows. |
| Bounded sample JOIN fails | Logs a warning and continues. | Remains diagnostic only. Warns with the exception type and retains successful required counts. The diagnostic logs a row count instead of displaying rows. |

Success returns the existing table-to-count dictionary. Spark shutdown is attempted in `finally` on every path after session creation. The success log and return follow cleanup. Required count errors suppress the original exception text in the task traceback. The JOIN remains a diagnostic and is not a release-blocking join or schema invariant.

The [offline tests](../tests/reliability/test_characterization.py) exercise lookup and count failures, missing tables, zero-row policy, diagnostic failure, result shape and cleanup with mocks. This change adds no foreign-key, grain, completeness or cross-table snapshot validation. Native Spark/Iceberg and Airflow behavior remains unverified. Before rollout, exercise a missing or unreadable table and valid empty data-dependent tables in an isolated warehouse, then confirm task failure/retry state and cleanup in Airflow.
