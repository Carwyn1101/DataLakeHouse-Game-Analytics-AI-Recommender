-- See docs/sql-analytics.md for setup, units and the original view mapping.
-- Source: Dremio Hive source "lakehouse", schema gold. Target: analytics_insights space.
-- Run in order. Replace those paths with isolated equivalents for smoke tests.

-- Each fact view selects one row per non-null game_key. For duplicate facts,
-- prefer the latest observed timestamp, then the lowest projected foreign keys.
-- Lookups collapse identical projections and suppress conflicting rows per key.
-- A missing/ambiguous lookup leaves NULL metadata rather than dropping or expanding facts.
CREATE OR REPLACE VIEW analytics_insights.vw_current_game_performance AS
WITH current_fact AS (
    SELECT f.*,
           ROW_NUMBER() OVER (
               PARTITION BY game_key
               ORDER BY snapshot_ts DESC NULLS LAST, game_detail_key ASC NULLS LAST, date_key ASC NULLS LAST, game_gen_key ASC NULLS LAST, game_pub_key ASC NULLS LAST
           ) AS row_num
    FROM "lakehouse".gold.fact_game_snapshot f
    WHERE game_key IS NOT NULL
),
dim_game_lookup AS (
    SELECT game_key, steam_appid, name, is_free
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY game_key) AS lookup_count
        FROM (
            SELECT DISTINCT game_key, steam_appid, name, is_free
            FROM "lakehouse".gold.dim_game
        ) d
    ) candidates
    WHERE lookup_count = 1
),
dim_detail_info_lookup AS (
    SELECT game_detail_key, steam_metacritic_score, steam_price_final, discount_pct, recommendations_total, price_category
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY game_detail_key) AS lookup_count
        FROM (
            SELECT DISTINCT game_detail_key, steam_metacritic_score, steam_price_final, discount_pct, recommendations_total, price_category
            FROM "lakehouse".gold.dim_detail_info
        ) d
    ) candidates
    WHERE lookup_count = 1
),
dim_date_lookup AS (
    SELECT date_key, "year", quarter, "month"
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY date_key) AS lookup_count
        FROM (
            SELECT DISTINCT date_key, "year", quarter, "month"
            FROM "lakehouse".gold.dim_date
        ) d
    ) candidates
    WHERE lookup_count = 1
),
bridge_game_genre_lookup AS (
    SELECT game_key, game_gen_key, genre_sk
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY game_key, game_gen_key) AS lookup_count
        FROM (
            SELECT DISTINCT game_key, game_gen_key, genre_sk
            FROM "lakehouse".gold.bridge_game_genre
        ) d
    ) candidates
    WHERE lookup_count = 1
),
bridge_game_publisher_lookup AS (
    SELECT game_key, game_pub_key, publisher_sk
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY game_key, game_pub_key) AS lookup_count
        FROM (
            SELECT DISTINCT game_key, game_pub_key, publisher_sk
            FROM "lakehouse".gold.bridge_game_publisher
        ) d
    ) candidates
    WHERE lookup_count = 1
),
dim_genre_lookup AS (
    SELECT genre_sk, genre_name
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY genre_sk) AS lookup_count
        FROM (
            SELECT DISTINCT genre_sk, genre_name
            FROM "lakehouse".gold.dim_genre
        ) d
    ) candidates
    WHERE lookup_count = 1
),
dim_publisher_lookup AS (
    SELECT publisher_sk, publisher_name
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY publisher_sk) AS lookup_count
        FROM (
            SELECT DISTINCT publisher_sk, publisher_name
            FROM "lakehouse".gold.dim_publisher
        ) d
    ) candidates
    WHERE lookup_count = 1
)
SELECT
    f.game_key,
    g.steam_appid,
    g.name AS game_name,
    CASE WHEN g.is_free = TRUE THEN 'Free'
         WHEN g.is_free = FALSE THEN 'Paid'
         ELSE 'Unknown'
    END AS monetization_type,
    dd.steam_metacritic_score,
    dd.steam_price_final AS price_usd,
    dd.discount_pct,
    dd.recommendations_total,
    dd.price_category,
    gen.genre_name AS representative_genre,
    pub.publisher_name AS representative_publisher,
    d."year" AS release_year,
    d.quarter AS release_quarter,
    d."month" AS release_month,
    f.snapshot_ts
FROM current_fact f
LEFT JOIN dim_game_lookup g ON f.game_key = g.game_key
LEFT JOIN dim_detail_info_lookup dd ON f.game_detail_key = dd.game_detail_key
LEFT JOIN dim_date_lookup d ON f.date_key = d.date_key
LEFT JOIN bridge_game_genre_lookup bgg
    ON f.game_gen_key = bgg.game_gen_key AND f.game_key = bgg.game_key
LEFT JOIN dim_genre_lookup gen ON bgg.genre_sk = gen.genre_sk
LEFT JOIN bridge_game_publisher_lookup bgp
    ON f.game_pub_key = bgp.game_pub_key AND f.game_key = bgp.game_key
LEFT JOIN dim_publisher_lookup pub ON bgp.publisher_sk = pub.publisher_sk
WHERE f.row_num = 1;

CREATE OR REPLACE VIEW analytics_insights.vw_representative_genre_performance AS
SELECT
    representative_genre,
    COUNT(*) AS games_count,
    AVG(steam_metacritic_score) AS avg_metacritic_score,
    SUM(recommendations_total) AS total_recommendations,
    AVG(recommendations_total) AS avg_recommendations,
    AVG(discount_pct) AS avg_discount_pct,
    AVG(price_usd) AS avg_price_usd,
    SUM(CASE WHEN monetization_type = 'Free' THEN 1 ELSE 0 END) AS free_games_count,
    SUM(CASE WHEN monetization_type = 'Paid' THEN 1 ELSE 0 END) AS paid_games_count,
    100.0 * SUM(CASE WHEN monetization_type = 'Free' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN monetization_type IN ('Free', 'Paid') THEN 1 ELSE 0 END), 0)
        AS pct_free_among_known
FROM analytics_insights.vw_current_game_performance
GROUP BY representative_genre;

CREATE OR REPLACE VIEW analytics_insights.vw_representative_publisher_performance AS
SELECT
    representative_publisher,
    COUNT(*) AS games_count,
    AVG(steam_metacritic_score) AS avg_metacritic_score,
    SUM(recommendations_total) AS total_recommendations,
    AVG(discount_pct) AS avg_discount_pct,
    SUM(CASE WHEN monetization_type = 'Free' THEN 1 ELSE 0 END) AS free_games_count,
    100.0 * SUM(CASE WHEN monetization_type = 'Free' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN monetization_type IN ('Free', 'Paid') THEN 1 ELSE 0 END), 0)
        AS pct_free_among_known,
    MAX(snapshot_ts) AS latest_snapshot_ts
FROM analytics_insights.vw_current_game_performance
GROUP BY representative_publisher;

CREATE OR REPLACE VIEW analytics_insights.vw_price_band_recommendations AS
SELECT
    price_category,
    representative_genre,
    COUNT(*) AS games_count,
    AVG(price_usd) AS avg_price_usd,
    AVG(recommendations_total) AS avg_recommendations,
    AVG(steam_metacritic_score) AS avg_metacritic_score
FROM analytics_insights.vw_current_game_performance
GROUP BY price_category, representative_genre;

CREATE OR REPLACE VIEW analytics_insights.vw_current_catalog_by_release_month AS
SELECT
    release_year,
    release_quarter,
    release_month,
    representative_genre,
    representative_publisher,
    COUNT(*) AS games_count,
    AVG(steam_metacritic_score) AS avg_metacritic_score,
    AVG(recommendations_total) AS avg_recommendations
FROM analytics_insights.vw_current_game_performance
GROUP BY release_year, release_quarter, release_month, representative_genre, representative_publisher;

CREATE OR REPLACE VIEW analytics_insights.vw_review_sentiment_analysis AS
WITH current_fact AS (
    SELECT f.*,
           ROW_NUMBER() OVER (
               PARTITION BY game_key
               ORDER BY snapshot_ts DESC NULLS LAST, review_key ASC NULLS LAST
           ) AS row_num
    FROM "lakehouse".gold.fact_review_snapshot f
    WHERE game_key IS NOT NULL
),
dim_review_info_lookup AS (
    SELECT review_key, steam_review_total, steam_review_total_positive, steam_review_total_negative, steam_review_positive_ratio, steam_review_score_desc
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY review_key) AS lookup_count
        FROM (
            SELECT DISTINCT review_key, steam_review_total, steam_review_total_positive, steam_review_total_negative, steam_review_positive_ratio, steam_review_score_desc
            FROM "lakehouse".gold.dim_review_info
        ) d
    ) candidates
    WHERE lookup_count = 1
)
SELECT
    f.game_key,
    g.steam_appid,
    g.game_name,
    COALESCE(g.monetization_type, 'Unknown') AS monetization_type,
    g.representative_genre,
    g.representative_publisher,
    r.steam_review_total,
    r.steam_review_total_positive,
    r.steam_review_total_negative,
    CASE WHEN r.steam_review_total > 0 THEN r.steam_review_positive_ratio END AS positive_review_ratio,
    r.steam_review_score_desc,
    f.snapshot_ts
FROM current_fact f
LEFT JOIN analytics_insights.vw_current_game_performance g ON f.game_key = g.game_key
LEFT JOIN dim_review_info_lookup r ON f.review_key = r.review_key
WHERE f.row_num = 1;

CREATE OR REPLACE VIEW analytics_insights.vw_representative_genre_sentiment AS
SELECT
    representative_genre,
    COUNT(*) AS games_with_review_metrics,
    SUM(steam_review_total) AS total_reviews,
    AVG(positive_review_ratio) AS avg_game_positive_review_ratio,
    SUM(CASE WHEN steam_review_total_positive BETWEEN 0 AND steam_review_total
             THEN CAST(steam_review_total_positive AS DOUBLE) END)
        / NULLIF(SUM(CASE WHEN steam_review_total_positive BETWEEN 0 AND steam_review_total
                          THEN steam_review_total END), 0) AS pooled_positive_review_ratio
FROM analytics_insights.vw_review_sentiment_analysis
WHERE steam_review_total >= 50
GROUP BY representative_genre;

CREATE OR REPLACE VIEW analytics_insights.vw_high_volume_mixed_reviews AS
SELECT
    game_key, steam_appid, game_name, representative_genre, representative_publisher,
    steam_review_total, positive_review_ratio,
    ABS(positive_review_ratio - 0.5) AS distance_from_neutral,
    snapshot_ts
FROM analytics_insights.vw_review_sentiment_analysis
WHERE steam_review_total >= 1000 AND positive_review_ratio BETWEEN 0.4 AND 0.6;

CREATE OR REPLACE VIEW analytics_insights.vw_free_vs_paid_sentiment AS
SELECT
    monetization_type,
    representative_genre,
    COUNT(*) AS games_with_review_metrics,
    AVG(positive_review_ratio) AS avg_game_positive_review_ratio,
    SUM(steam_review_total) AS total_reviews,
    AVG(steam_review_total) AS avg_reviews_per_game
FROM analytics_insights.vw_review_sentiment_analysis
WHERE steam_review_total >= 50
GROUP BY monetization_type, representative_genre;

CREATE OR REPLACE VIEW analytics_insights.vw_popularity_engagement_metrics AS
WITH current_fact AS (
    SELECT f.*,
           ROW_NUMBER() OVER (
               PARTITION BY game_key
               ORDER BY snapshot_ts DESC NULLS LAST, popularity_key ASC NULLS LAST
           ) AS row_num
    FROM "lakehouse".gold.fact_popularity_snapshot f
    WHERE game_key IS NOT NULL
),
dim_popularity_info_lookup AS (
    SELECT popularity_key, steam_concurrent_players, steamspy_players_2weeks, owners_midpoint, playtime_avg_hours, playtime_median_hours, popularity_category, engagement_category
    FROM (
        SELECT d.*, COUNT(*) OVER (PARTITION BY popularity_key) AS lookup_count
        FROM (
            SELECT DISTINCT popularity_key, steam_concurrent_players, steamspy_players_2weeks, owners_midpoint, playtime_avg_hours, playtime_median_hours, popularity_category, engagement_category
            FROM "lakehouse".gold.dim_popularity_info
        ) d
    ) candidates
    WHERE lookup_count = 1
)
SELECT
    f.game_key,
    g.steam_appid,
    g.game_name,
    g.representative_genre,
    g.representative_publisher,
    p.steam_concurrent_players,
    p.steamspy_players_2weeks AS avg_playtime_2weeks_minutes,
    p.owners_midpoint AS estimated_owners_midpoint,
    p.playtime_avg_hours,
    p.playtime_median_hours,
    p.popularity_category,
    p.engagement_category,
    CAST(p.playtime_avg_hours AS DOUBLE) / NULLIF(p.playtime_median_hours, 0)
        AS mean_to_median_playtime_ratio,
    f.snapshot_ts
FROM current_fact f
LEFT JOIN analytics_insights.vw_current_game_performance g ON f.game_key = g.game_key
LEFT JOIN dim_popularity_info_lookup p ON f.popularity_key = p.popularity_key
WHERE f.row_num = 1;

CREATE OR REPLACE VIEW analytics_insights.vw_current_game_activity_ranks AS
SELECT
    game_key, steam_appid, game_name, representative_genre, representative_publisher,
    steam_concurrent_players, avg_playtime_2weeks_minutes, estimated_owners_midpoint, playtime_avg_hours,
    CASE WHEN steam_concurrent_players IS NOT NULL THEN
        ROW_NUMBER() OVER (ORDER BY steam_concurrent_players DESC NULLS LAST, game_key)
    END AS rank_by_concurrent_players,
    CASE WHEN avg_playtime_2weeks_minutes IS NOT NULL THEN
        ROW_NUMBER() OVER (ORDER BY avg_playtime_2weeks_minutes DESC NULLS LAST, game_key)
    END AS rank_by_avg_playtime_2weeks,
    CASE WHEN estimated_owners_midpoint IS NOT NULL THEN
        ROW_NUMBER() OVER (ORDER BY estimated_owners_midpoint DESC NULLS LAST, game_key)
    END AS rank_by_estimated_owners,
    CASE WHEN playtime_avg_hours IS NOT NULL THEN
        ROW_NUMBER() OVER (ORDER BY playtime_avg_hours DESC NULLS LAST, game_key)
    END AS rank_by_avg_playtime,
    snapshot_ts
FROM analytics_insights.vw_popularity_engagement_metrics;

CREATE OR REPLACE VIEW analytics_insights.vw_representative_genre_popularity AS
SELECT
    representative_genre,
    COUNT(*) AS games_count,
    AVG(steam_concurrent_players) AS avg_concurrent_players,
    AVG(avg_playtime_2weeks_minutes) AS mean_game_avg_playtime_2weeks_minutes,
    AVG(estimated_owners_midpoint) AS avg_estimated_owners_midpoint,
    AVG(playtime_avg_hours) AS mean_game_avg_playtime_hours,
    SUM(CASE WHEN popularity_category IN ('Popular', 'Very Popular', 'Extremely Popular')
             THEN 1 ELSE 0 END) AS high_popularity_count,
    SUM(CASE WHEN engagement_category IN ('Engaged', 'Hardcore', 'Dedicated')
             THEN 1 ELSE 0 END) AS high_engagement_count
FROM analytics_insights.vw_popularity_engagement_metrics
GROUP BY representative_genre;

CREATE OR REPLACE VIEW analytics_insights.vw_game_segmentation_matrix AS
SELECT
    popularity_category,
    engagement_category,
    COUNT(*) AS games_count,
    AVG(steam_concurrent_players) AS avg_concurrent_players,
    AVG(avg_playtime_2weeks_minutes) AS mean_game_avg_playtime_2weeks_minutes,
    AVG(playtime_avg_hours) AS mean_game_avg_playtime_hours,
    AVG(estimated_owners_midpoint) AS avg_estimated_owners_midpoint
FROM analytics_insights.vw_popularity_engagement_metrics
GROUP BY popularity_category, engagement_category;

CREATE OR REPLACE VIEW analytics_insights.vw_pareto_analysis_concurrent_players AS
WITH ranked AS (
    SELECT
        game_key, steam_appid, game_name, representative_genre, steam_concurrent_players, snapshot_ts,
        ROW_NUMBER() OVER (ORDER BY steam_concurrent_players DESC, game_key) AS rank_position,
        COUNT(*) OVER () AS observed_game_count,
        SUM(steam_concurrent_players) OVER () AS sum_observed_concurrent_counts,
        SUM(steam_concurrent_players) OVER (
            ORDER BY steam_concurrent_players DESC, game_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_concurrent_count
    FROM analytics_insights.vw_popularity_engagement_metrics
    WHERE steam_concurrent_players >= 0
)
SELECT
    game_key, steam_appid, game_name, representative_genre, steam_concurrent_players, snapshot_ts,
    rank_position, observed_game_count, sum_observed_concurrent_counts,
    100.0 * rank_position / NULLIF(observed_game_count, 0) AS cumulative_games_pct,
    100.0 * cumulative_concurrent_count / NULLIF(sum_observed_concurrent_counts, 0)
        AS cumulative_observed_concurrent_pct
FROM ranked;
