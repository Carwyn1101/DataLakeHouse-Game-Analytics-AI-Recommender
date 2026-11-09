# Gold SQL analytics

> **Power BI compatibility hold.** The existing dashboard uses the original analytics views. Keep its deployed `analytics_insights` views and model in place. V2 validation belongs in a separate `analytics_insights_v2` space. Actual PBIX bindings remain unverified until the Owner separately approves model inspection.

| Contract | Artifact | Purpose |
| --- | --- | --- |
| Legacy `analytics_insights` | [Original SQL reference](reference/Insights.legacy.sql.txt) | Exact SQL from commit `7067e7c`, including all 16 original definitions, names and output expressions. Retained for contract comparison and migration planning. |
| V2 `analytics_insights_v2` | [Insights.v2.sql](../LakeHouse_Project/Insights.v2.sql) | 14 corrected current-state views for isolated validation. Column and grain changes require BI migration review before adoption. |

**Legacy is reference only.** It retains the original draft headings and known defects, including cross-domain surrogate-key JOINs, SteamSpy playtime mislabeled as player counts, and trend queries over current-state facts. Do not execute the reference or treat it as validated analytics.

## V2 Dremio validation setup

1. Use an isolated Dremio environment with synthetic Gold tables. The script reads `"lakehouse".gold.<table>`. Register a synthetic source at that path or change only the source qualifier in a validation copy.
2. Create a dedicated space named `analytics_insights_v2`. Use a role limited to synthetic inputs and the V2 space. Keep all V2 targets and inter-view references in that space.
3. During approved validation, run the semicolon-delimited V2 statements in order. `CREATE OR REPLACE VIEW` can replace same-named views inside the V2 space. Query each created view as well as creating it.
4. Order results explicitly in consuming queries, for example by `rank_position` for the Pareto view. A view does not guarantee display order.

The script uses CTEs, `ROW_NUMBER`, window aggregates, `NULLS LAST`, and `NULLIF`. It avoids ordered-set aggregates such as `MEDIAN` and `PERCENTILE_CONT` and avoids grouping by SELECT aliases. Native Dremio execution has yet to be verified. See [Dremio CREATE VIEW](https://docs.dremio.com/current/reference/sql/commands/create-view/).

## Inputs and analytical grain

| Detail view | Driving fact | Lookups |
| --- | --- | --- |
| `vw_current_game_performance` | `fact_game_snapshot` | `dim_game`, `dim_detail_info`, `dim_date`, genre/publisher bridges and dimensions |
| `vw_review_sentiment_analysis` | `fact_review_snapshot` | `dim_review_info` by review key and the game view by game key |
| `vw_popularity_engagement_metrics` | `fact_popularity_snapshot` | `dim_popularity_info` by popularity key and the game view by game key |

- Genre and publisher come from the representative bridge foreign keys already stored in the game fact. Both the bridge row key and its owning game key must match. Other relationships in the bridges are outside this attribution.
- Exact duplicate lookup projections collapse. Conflicts under a lookup key yield NULL via the left join. Missing relationships also yield NULL, and grouped views retain a NULL bucket. A missing game fact leaves review/popularity measures available with unknown game metadata.
- When a fact has several rows for the same game key, the view takes the greatest observed `snapshot_ts` and breaks timestamp ties by ascending referenced foreign keys. This keeps aggregates at one row per game. Null game keys are excluded because their game identity cannot be established. These policies do not repair the source tables.
- The release cohort uses the game fact's release-date key. Dates absent from the Gold calendar remain in the unknown cohort. Review/popularity timestamps describe the observed current rows and may differ across games and facts.

## Metric definitions

| Metric | Meaning |
| --- | --- |
| `price_usd`, `discount_pct` | Final price in USD and discount as a percentage (25 means 25%), as prepared by Gold. Price bands follow the upstream price categories |
| `recommendations_total` | Steam Store recommendations count from Gold |
| `pct_free_among_known` | Percentage of games whose monetization is known that are free. Undefined when the denominator is zero |
| `positive_review_ratio` | Per-game Gold ratio on a 0–1 scale for games with positive review volume. The pooled ratio divides valid positive counts by their corresponding total review counts |
| `steam_concurrent_players` | Observed Steam concurrent count per game at its snapshot time |
| `avg_playtime_2weeks_minutes` | Legacy Gold `steamspy_players_2weeks` carries SteamSpy `average_2weeks`, the average playtime during the prior two weeks in **minutes** |
| `mean_game_avg_playtime_2weeks_minutes` | Unweighted mean across games of their reported two-week average playtime |
| `estimated_owners_midpoint` | Midpoint of SteamSpy's estimated owners range |
| `playtime_avg_hours`, `playtime_median_hours` | SteamSpy lifetime mean and median playtime converted to hours upstream |
| `mean_to_median_playtime_ratio` | Unitless ratio. Returns NULL when median playtime is zero or missing |
| Pareto percentages | Shares of included games and the sum of their observed concurrent counts. A zero count sum yields NULL for the latter |

Gold can already normalize missing numeric inputs to zero. These views cannot recover missingness lost upstream. Adoption and concurrency-rate formulas based on `average_2weeks` were removed because they require genuine player counts. See [SteamSpy API field definitions](https://steamspy.com/api.php).

## Original-to-recovered mapping

All entries use the `vw_` prefix. The legacy reference retains every original view. Exclusions and changed contracts apply to V2 only.

| Original suffix | Recovered suffix | Status and purpose |
| --- | --- | --- |
| `top_games_by_recommendations` | `current_game_performance` | Revised. Per-game recommendations, price and representative genre/publisher |
| `genre_performance_summary` | `representative_genre_performance` | Revised. Game and performance aggregates by representative genre |
| `publisher_performance_summary` | `representative_publisher_performance` | Revised. Game and performance aggregates by representative publisher |
| `price_recommendations_correlation` | `price_band_recommendations` | Revised. Descriptive means by price band and genre. No correlation coefficient |
| `games_release_trends` | `current_catalog_by_release_month` | Revised. Current catalog grouped by release month |
| `review_sentiment_analysis` | Same | Revised. Review counts and positive ratio per game |
| `genre_sentiment_summary` | `representative_genre_sentiment` | Revised. Review aggregates for games with at least 50 reviews |
| `polarized_games` | `high_volume_mixed_reviews` | Revised. At least 1,000 reviews and 40–60% positive. The split does not establish polarization |
| `review_trends_over_time` | — | Excluded. Requires historical review observations |
| `free_vs_paid_sentiment` | Same | Revised. Review metrics by monetization and representative genre, with an Unknown group |
| `popularity_engagement_metrics` | Same | Revised. Per-game activity, owners and correctly labeled playtime |
| `top_games_by_activity` | `current_game_activity_ranks` | Revised. Separate ranks by metric, tied scores ordered by game key |
| `genre_popularity_summary` | `representative_genre_popularity` | Revised. Per-game means and category counts, including Dedicated |
| `game_segmentation_matrix` | Same | Revised. Grouped by the Gold popularity and engagement categories |
| `pareto_analysis_concurrent_players` | Same | Revised. Cumulative shares with consistent ordering and guarded denominators |
| `popularity_trends_over_time` | — | Excluded. Requires historical popularity observations |

## Limits and verification

Gold refreshes current rows. Release cohorts describe the games still in the current fact, and the Pareto curve can combine observations taken at different times. Historical review or popularity trends would require a separate, time-aligned observation model. Developer/category detail, review volume labels, percentile summaries and heuristic segmentation labels are omitted from this recovery. BI consumers of the original draft need to review renamed views and columns.

Run `python -B -m unittest discover -s tests/sql -v` from the repository root. Tests verify the legacy reference fingerprint and V2 namespace. They execute only the V2 view bodies against in-memory SQLite fixtures with namespace and CREATE VIEW wrapper adaptation. Native Dremio planning, execution and performance need a separate synthetic smoke test.
