"""Generate only the synthetic F0, F1 and F2 records from the reliability plan."""

import csv
import io

T0 = "2026-01-01T10:00:00Z"
T1 = "2026-01-02T10:00:00Z"
T2 = "2026-01-03T10:00:00Z"
F0 = {
    "games": {
        "steam_appid": 101, "name": "Orbit Quest", "release_date": "2020-06-01",
        "developers": "Studio A", "publishers": "Publisher A", "genres": "Action, RPG",
        "categories": "Single-player", "tags": "Space, Exploration", "steam_currency": "USD",
        "steam_price_initial": 2000, "steam_price_final": 1000, "required_age": 0,
        "is_free": "FALSE", "steam_metacritic_score": 80, "recommendations_total": 100,
        "ts_ingested": T0,
    },
    "reviews": {
        "steam_appid": 101, "steam_review_total_positive": 80, "steam_review_total_negative": 20,
        "steam_review_total": 100, "steam_review_positive_ratio": 0.8,
        "steam_review_score_desc": "Very Positive", "ts_ingested": T0,
    },
    "popularity": {
        "steam_appid": 101, "steam_concurrent_players": 500, "steamspy_owners_range": "1000 .. 2000",
        "steamspy_players_2weeks": 100, "steamspy_average_forever_min": 120,
        "steamspy_median_forever_min": 60, "ts": T0,
    },
}


def csv_bytes(name, *changes):
    stream = io.StringIO(newline="")
    fields = F0[name]
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(fields | row for row in changes)
    return stream.getvalue().encode()


def initial(run):
    for name in F0:
        second = {"steam_appid": 202}
        if name == "games":
            second.update(name="Garden Race", genres="Racing", tags="Driving")
        run.ingest(name, csv_bytes(name, {}, second))


def update(run):
    run.ingest("games", csv_bytes("games", {
        "name": "Orbit Quest Updated", "steam_price_final": 800,
        "recommendations_total": 120, "ts_ingested": T1,
    }, {"steam_appid": 303, "name": "Puzzle Harbor", "genres": "Puzzle", "ts_ingested": T1}))
    run.ingest("reviews", csv_bytes("reviews", {
        "steam_review_total_positive": 96, "steam_review_total_negative": 24,
        "steam_review_total": 120, "ts_ingested": T1,
    }))
    run.ingest("popularity", csv_bytes("popularity", {
        "steam_concurrent_players": 650, "steamspy_owners_range": "2000 .. 4000", "ts": T2,
    }))
