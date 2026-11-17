# Gold bridge refresh synthetic prototype

This prototype uses synthetic rows and an in-memory test double for the developer,
genre, publisher, category, and tag bridges. Run
`python -B -m unittest discover -s tests/reliability -q` from the repository root.

## Membership and allocation

- Freeze eligible game keys, desired membership pairs, and the target mapping before
  planning. Keep the bridge surrogate key (SK) of each surviving
  `(game_key, dimension_sk)` pair. Preserve unrelated games. Derive removals from
  eligible keys even when no replacements exist.
- Reject NULL or invalid IDs, duplicate desired or target pairs, reused target SKs,
  and 32-bit overflow. This model starts with resolved game and dimension keys.
- An explicitly fresh synthetic identity starts at zero. A separate witness in the
  double detects missing, corrupt, untrusted, or regressed high-water values.
  Existing tables need a trustworthy historical bound. Live `MAX(bridge_sk)` only
  describes rows still present. The baseline Gold writer calculates that maximum
  after deleting rows, so it can lose evidence of retired keys.
- Reserve the next range before the logical data commit. A failed write can leave
  reserved keys unused. A stale reservation or data plan fails. Replaying frozen
  input after a committed write with lost acknowledgement keeps the keys already
  assigned. Conflicting writers are modeled as ordered interleavings. The baseline provides no verified
  writer exclusion for reservation and bridge commit.

## Fact verification and publication

R2 picks the lowest valid dimension SK, with bridge SK as a deterministic
tie-breaker. A fact uses `-1` only when its game has no valid membership in that
bridge. Verification checks complete synthetic fact coverage for the expected
games and all five FK columns. Keep publication on HOLD until all five bridges
and the fact refresh complete for the same batch and FK verification passes.
An eligible batch still requires a separate publication decision.

The model cannot shield live Dremio or Power BI readers from intermediate commits.
Before production rollout, verify a reader barrier and recover in this order
`bridges → fact_game_snapshot → FK verification → publication decision`.
A failed or incomplete recovery remains on HOLD.

## Future native acceptance

**NOT NATIVE VERIFIED.** The logical commit and high-water witness exist only in
memory. Production readiness requires the following evidence on synthetic data.

- Spark 3.3.2 and Iceberg 1.3.1 execute the scoped MERGE with the exact staged
  Parquet schema, executor file visibility, and scoped scratch cleanup on both
  success and failure.
- Each changed bridge has one child data snapshot. No-op and pre-commit failures
  create no data snapshot. Inspect allocator metadata commits separately and
  verify durable reservations, failure gaps, and actual exclusive-writer
  enforcement.
- An independent reader confirms complete mappings for all five bridges and
  fact FKs, including final-member removal, unrelated rows, R2, and the sentinel.
  Check highest-key deletion followed by allocation, frozen retries after an
  acknowledged or lost commit, and rejection of ambiguous inputs.
- Failures between bridge commits, before or during fact refresh, or during FK
  validation keep publication on HOLD. Establish a trustworthy per-table bootstrap
  and a restore procedure that preserves or reconciles the historical high-water.

Production ETL, table properties, DAGs, and reader controls are outside this
prototype's execution path.
