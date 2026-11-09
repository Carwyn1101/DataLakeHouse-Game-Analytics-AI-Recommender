"""Offline regression tests for the actual SQL view bodies.

SQLite provides synthetic relational checks. Only namespaces and Dremio's
CREATE OR REPLACE wrapper are adapted. These tests do not validate Dremio.
"""
import hashlib
from pathlib import Path
import re
import sqlite3
import unittest


ROOT = Path(__file__).resolve().parents[2]
SQL = (ROOT / "LakeHouse_Project" / "Insights.v2.sql").read_text(encoding="utf-8")
V2_NAMESPACE = "analytics_insights_v2"
VIEW_PREFIX = f"CREATE OR REPLACE VIEW {V2_NAMESPACE}."
# Used columns transcribed from the Gold builders. SQLite tables use explicit
# names so that missing tables, bad aliases and unknown columns fail execution.
GOLD_SCHEMA = {
    "dim_game": "game_key, steam_appid, name, is_free",
    "dim_detail_info": (
        "game_detail_key, steam_metacritic_score, steam_price_final, "
        "discount_pct, recommendations_total, price_category"
    ),
    "dim_date": 'date_key, "year", quarter, "month"',
    "dim_genre": "genre_sk, genre_name",
    "dim_publisher": "publisher_sk, publisher_name",
    "bridge_game_genre": "game_key, game_gen_key, genre_sk",
    "bridge_game_publisher": "game_key, game_pub_key, publisher_sk",
    "fact_game_snapshot": "game_key, game_detail_key, date_key, game_gen_key, game_pub_key, snapshot_ts",
    "fact_review_snapshot": "game_key, review_key, date_key, snapshot_ts",
    "fact_popularity_snapshot": "game_key, popularity_key, date_key, snapshot_ts",
    "dim_review_info": (
        "review_key, steam_review_total, steam_review_total_positive, steam_review_total_negative, "
        "steam_review_positive_ratio, steam_review_score_desc"
    ),
    "dim_popularity_info": (
        "popularity_key, steam_concurrent_players, steamspy_players_2weeks, owners_midpoint, "
        "playtime_avg_hours, playtime_median_hours, popularity_category, engagement_category"
    ),
}
BASE_VIEWS = (
    "vw_current_game_performance",
    "vw_review_sentiment_analysis",
    "vw_popularity_engagement_metrics",
)


class SqlContractTests(unittest.TestCase):
    def test_legacy_reference_matches_original(self):
        reference = ROOT / "docs" / "reference" / "Insights.legacy.sql.txt"
        # Git stores LF in the original blob and may check out CRLF.
        content = reference.read_bytes().replace(b"\r\n", b"\n")
        self.assertEqual(
            hashlib.sha256(content).hexdigest(),
            "7829b0b10a5ae8816202d3a91cbb92d0655e3071bbd84978cb2b450796c6c8b1",
        )

    def test_v2_does_not_reference_legacy_namespace(self):
        self.assertNotRegex(SQL.casefold(), r"\banalytics_insights\b")


class InsightsTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        for table, columns in GOLD_SCHEMA.items():
            self.db.execute(f"CREATE TABLE {table} ({columns})")

        self.insert("dim_game", (10, 1010, "First", 1), (20, 1020, "Second", 0), (30, 1030, "Third", None))
        self.insert("dim_detail_info", (710, 85, 0, 0, 300, "Free"), (720, 75, 10, 25, 100, "Budget"))
        self.insert("dim_date", (701, 2020, 1, 2))
        self.insert("dim_genre", (310, "Action"), (320, "Puzzle"), (330, "Unselected"))
        self.insert("dim_publisher", (410, "Alpha"), (420, "Beta"), (430, "Unselected"))
        self.insert("bridge_game_genre", (10, 111, 310), (10, 112, 330), (20, 121, 310), (20, 10, 320))
        self.insert("bridge_game_publisher", (10, 211, 410), (10, 212, 430), (20, 221, 420), (20, 10, 430))
        self.insert(
            "fact_game_snapshot",
            (10, 710, 701, 111, 211, "2026-01-02"),
            (20, 720, 701, 121, 221, "2026-01-01"),
            (30, 730, 799, -1, -1, "2026-01-01"),
        )
        self.insert(
            "dim_review_info",
            (510, 1000, 550, 450, 0.55, "Mixed"),
            (520, 100, 90, 10, 0.9, "Positive"),
            (530, 0, 0, 0, 0, "No Reviews"),
        )
        self.insert(
            "fact_review_snapshot", (10, 510, 800, "2026-01-03"),
            (20, 520, 800, "2026-01-02"), (30, 530, None, "2026-01-02"),
        )
        self.insert(
            "dim_popularity_info",
            (610, 50, 120, 500, 12, 4, "Low", "Dedicated"),
            (620, 50, 60, 250, 10, 0, "Low", "Casual"),
            (630, 0, None, None, None, None, "Inactive", "No Data"),
        )
        self.insert(
            "fact_popularity_snapshot", (10, 610, 800, "2026-01-04"),
            (20, 620, 800, "2026-01-01"), (30, 630, None, "2026-01-01"),
        )

        without_comments = re.sub(r"(?m)^--[^\n]*", "", SQL)
        self.views = []
        for statement in without_comments.split(";"):
            statement = statement.strip()
            if not statement:
                continue
            self.assertTrue(statement.startswith(VIEW_PREFIX), statement)
            name = statement[len(VIEW_PREFIX):].split()[0]
            self.views.append(name)
            portable = statement.replace(VIEW_PREFIX, "CREATE VIEW ", 1)
            portable = portable.replace('"lakehouse".gold.', "").replace(f"{V2_NAMESPACE}.", "")
            self.db.execute(portable)

    def insert(self, table, *rows):
        placeholders = ", ".join("?" for _ in rows[0])
        self.db.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    def rows(self, sql):
        return [tuple(row) for row in self.db.execute(sql)]

    def test_all_views_resolve_against_gold_contract(self):
        self.assertEqual(len(self.views), 14)
        for view in self.views:
            with self.subTest(view=view):
                self.assertGreater(len(self.rows(f"SELECT * FROM {view}")), 0)

    def test_representative_relationships_use_fact_foreign_keys(self):
        expected = [(10, "Action", "Alpha"), (20, "Action", "Beta"), (30, None, None)]
        for view in BASE_VIEWS:
            with self.subTest(view=view):
                self.assertEqual(
                    self.rows(f"SELECT game_key, representative_genre, representative_publisher FROM {view} ORDER BY game_key"),
                    expected,
                )

    def test_missing_relationships_and_cross_game_bridge_keys_keep_facts(self):
        self.db.execute("UPDATE fact_game_snapshot SET game_gen_key = 121 WHERE game_key = 10")
        self.db.execute("DELETE FROM dim_publisher WHERE publisher_sk = 410")
        self.db.execute("DELETE FROM fact_game_snapshot WHERE game_key = 20")
        self.assertEqual(
            self.rows("SELECT game_key, representative_genre, representative_publisher, steam_review_total "
                      "FROM vw_review_sentiment_analysis ORDER BY game_key"),
            [(10, None, None, 1000), (20, None, None, 100), (30, None, None, 0)],
        )

    def test_duplicate_lookups_and_facts_do_not_multiply_aggregates(self):
        self.insert("dim_genre", (310, "Action"))
        self.insert("bridge_game_genre", (10, 111, 310))
        self.insert("dim_detail_info", (710, 85, 0, 0, 300, "Free"))
        self.insert("fact_game_snapshot", (10, 710, 701, 111, 211, "2026-01-02"))
        self.insert("fact_review_snapshot", (10, 520, 800, "2025-01-01"))
        self.assertEqual(
            self.rows("SELECT games_count, total_recommendations, pct_free_among_known "
                      "FROM vw_representative_genre_performance WHERE representative_genre = 'Action'"),
            [(2, 400, 50)],
        )
        self.assertEqual(
            self.rows("SELECT games_with_review_metrics, total_reviews "
                      "FROM vw_representative_genre_sentiment WHERE representative_genre = 'Action'"),
            [(2, 1100)],
        )
        ratio = self.rows("SELECT pooled_positive_review_ratio FROM vw_representative_genre_sentiment")[0][0]
        self.assertAlmostEqual(ratio, 640 / 1100)

    def test_conflicting_lookups_leave_null_instead_of_guessing(self):
        self.insert("bridge_game_genre", (10, 111, 320))
        self.insert("dim_publisher", (410, "Conflicting name"))
        self.insert("dim_review_info", (510, 2000, 1000, 1000, 0.5, "Mixed"))
        self.assertEqual(
            self.rows("SELECT representative_genre, representative_publisher, steam_review_total "
                      "FROM vw_review_sentiment_analysis WHERE game_key = 10"),
            [(None, None, None)],
        )

    def test_steamspy_minutes_and_dedicated_category(self):
        self.assertEqual(
            self.rows("SELECT steam_concurrent_players, avg_playtime_2weeks_minutes "
                      "FROM vw_popularity_engagement_metrics WHERE game_key = 10"),
            [(50, 120)],
        )
        self.assertEqual(
            self.rows("SELECT avg_concurrent_players, mean_game_avg_playtime_2weeks_minutes, high_engagement_count "
                      "FROM vw_representative_genre_popularity WHERE representative_genre = 'Action'"),
            [(50, 90, 1)],
        )
        self.assertEqual(
            self.rows("SELECT mean_game_avg_playtime_2weeks_minutes "
                      "FROM vw_game_segmentation_matrix WHERE engagement_category = 'Dedicated'"),
            [(120,)],
        )
        for view in self.views:
            columns = [column[0] for column in self.db.execute(f"SELECT * FROM {view}").description]
            for column in columns:
                self.assertNotRegex(column, r"players_2weeks|adoption_rate|concurrency_rate")

    def test_current_catalog_cohorts_and_excluded_history(self):
        self.assertNotIn("vw_review_trends_over_time", self.views)
        self.assertNotIn("vw_popularity_trends_over_time", self.views)
        self.assertEqual(
            self.rows("SELECT release_year, release_month, SUM(games_count) "
                      "FROM vw_current_catalog_by_release_month GROUP BY release_year, release_month"),
            [(None, None, 1), (2020, 2, 2)],
        )

    def test_zero_denominators_and_unknown_values(self):
        self.assertEqual(
            self.rows("SELECT pct_free_among_known FROM vw_representative_genre_performance "
                      "WHERE representative_genre IS NULL"), [(None,)]
        )
        self.assertEqual(
            self.rows("SELECT mean_to_median_playtime_ratio FROM vw_popularity_engagement_metrics WHERE game_key = 20"),
            [(None,)],
        )
        self.assertEqual(
            self.rows("SELECT monetization_type, positive_review_ratio FROM vw_review_sentiment_analysis WHERE game_key = 30"),
            [("Unknown", None)],
        )
        self.db.execute("UPDATE dim_popularity_info SET steam_concurrent_players = 0")
        self.assertEqual(
            self.rows("SELECT cumulative_observed_concurrent_pct FROM vw_pareto_analysis_concurrent_players"),
            [(None,), (None,), (None,)],
        )

    def test_ranking_ties_and_pareto_order_use_same_game_key(self):
        self.assertEqual(
            self.rows("SELECT game_key, rank_by_concurrent_players, rank_by_avg_playtime_2weeks "
                      "FROM vw_current_game_activity_ranks ORDER BY game_key"),
            [(10, 1, 1), (20, 2, 2), (30, 3, None)],
        )
        self.assertEqual(
            self.rows("SELECT game_key, rank_position, cumulative_observed_concurrent_pct "
                      "FROM vw_pareto_analysis_concurrent_players ORDER BY rank_position"),
            [(10, 1, 50), (20, 2, 100), (30, 3, 100)],
        )


if __name__ == "__main__":
    unittest.main()
