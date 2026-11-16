# Gold C03 Phase 1C-A: atomic review and popularity fact refresh

The incremental paths used separate DELETE and APPEND commits. An interruption could remove the old rows without inserting the replacements. The two actions also evaluated the fact query independently. The reviewed change writes the complete fact payload to attempt-local Parquet before mutation and uses a direct file relation in one MERGE. This avoids the temporary-view analyzer path seen on Spark 3.3.2. See [Phase 1A](gold-dim-game-atomic-recovery.md) and [Iceberg #7766](https://github.com/apache/iceberg/issues/7766).

For each non-null `game_key` present in the stage, one staged row emits a delete marker that matches all old target rows for that key. Every payload row emits an insert action, including repeated rows from joins. The window rank selects only which row emits the marker. Existing null-key and target-only rows remain. Incoming null-key rows still append, including after replay. An empty stage issues no MERGE. Unchanged nonempty input may still commit a physical refresh. Initial load, schemas, joins, timestamps, and downstream contracts remain unchanged.

Freezing rows while retaining separate DELETE and APPEND would leave the failure gap. An in-place UPDATE requires a row identity that the current multiset does not guarantee. Full overwrite would endanger unrelated rows and expand the write scope. The staged MERGE is per table and does not coordinate concurrent writers or publish both facts together.

## Native acceptance (design only)

Use the pinned Spark 3.3.2 / Iceberg 1.3.1 stack, synthetic fixtures and a disposable catalog after separate authorization. Run the same matrix for both facts.

| Case | Required evidence |
| --- | --- |
| TP-21 initial and normal refresh | Exercise initial and existing tables. Compare exact schemas, source timestamps and full row multisets for changed, new, unchanged, and target-only keys. Verify FK values and one row per game when source and joins are unique. Record one child snapshot for each committed nonempty atomic refresh. |
| TP-21 duplicate, null and empty inputs | Preserve exact duplicate rows and distinguishable join fan-out rows. Verify nullable date and dimension FKs. Existing null-key rows remain and replay can append more null-key rows. An empty input preserves rows and adds no target snapshot. |
| TP-21 fault and replay | Inject a failure before MERGE commit. Expect unchanged rows and snapshot history. Retry the frozen input, then simulate lost acknowledgement after a verified commit. An independent reader sees a complete committed snapshot. Replaying valid non-null-key input preserves the logical multiset and FK mappings, with one child snapshot if the replay commits. |
| TP-25 staged execution | Verify actual Parquet schema and nullability, executor visibility, planner use of the staged source, snapshot lineage, cleanup on success/failure, and runner isolation and budgets. |

Offline mocks and a multiset oracle exercise the write boundary and intended row behavior. Native acceptance and rollout remain on **HOLD** pending Independent Tech Lead review and a new Project Owner authorization.
