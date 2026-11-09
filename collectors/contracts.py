"""Historical CSV contracts and pure staging transformations."""

import csv
import io
from datetime import datetime, timedelta


GAME_COLUMNS = (
    "steam_appid", "name", "release_date", "is_free", "steam_metacritic_score",
    "steam_price_initial", "steam_price_final", "steam_currency", "developers",
    "publishers", "genres", "categories", "required_age", "recommendations_total",
    "ts_ingested",
)
TAG_COLUMNS = ("steam_appid", "tags")
ENRICHED_GAME_COLUMNS = (*GAME_COLUMNS, "tags")
REVIEW_COLUMNS = (
    "steam_appid", "steam_review_total_positive", "steam_review_total_negative",
    "steam_review_total", "steam_review_score_desc", "steam_review_positive_ratio", "ts_ingested",
)
POPULARITY_COLUMNS = (
    "steam_appid", "ts", "steam_concurrent_players", "steamspy_owners_range",
    "steamspy_players_2weeks", "steamspy_average_forever_min", "steamspy_median_forever_min",
)
RAW_COLUMNS = {
    "games.csv": GAME_COLUMNS,
    "reviews.csv": REVIEW_COLUMNS,
    "popularity.csv": POPULARITY_COLUMNS,
    "game_tags.csv": TAG_COLUMNS,
}
CRITICAL_GAME_TEXT = ("name", "release_date", "developers", "publishers", "genres", "categories")


class ContractError(ValueError):
    """Safe error message containing no input data or local paths."""


def appid(value):
    """Parse a positive signed-64-bit Steam App ID without float coercion."""
    if isinstance(value, str):
        value = value.strip().lstrip("0")
        if not value.isascii() or not value.isdecimal() or len(value) > 19:
            raise ContractError("Invalid Steam App ID")
        value = int(value)
    if type(value) is not int or not 0 < value < 2**63:
        raise ContractError("Invalid Steam App ID")
    return value


def utc_observation(value):
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00" if value.endswith("Z") else value)
    except (AttributeError, ValueError, TypeError):
        raise ContractError("Invalid UTC observation timestamp") from None
    if parsed.utcoffset() != timedelta(0):
        raise ContractError("Observation timestamp must be UTC")
    return parsed


def read_csv(payload, columns):
    try:
        stream = io.StringIO(payload.decode("utf-8-sig"), newline="")
        reader = csv.reader(stream, strict=True)
        if tuple(next(reader, ())) != columns:
            raise ContractError("Unexpected CSV header or column order")
        rows = []
        for row in reader:
            if len(row) != len(columns):
                raise ContractError("CSV row width does not match its header")
            rows.append(dict(zip(columns, row)))
        return rows
    except (UnicodeError, csv.Error):
        raise ContractError("CSV must be well-formed UTF-8") from None


def encode_csv(columns, rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def enrich_games(games_csv, tags_csv):
    """Return enriched rows and counts. Fail the batch if any row is unresolved."""
    games = read_csv(games_csv, GAME_COLUMNS)
    tags = read_csv(tags_csv, TAG_COLUMNS)
    if not games:
        raise ContractError("Games input has no observations")

    tags_by_id = {}
    blank_tags = duplicate_tags = 0
    for row in tags:
        key = appid(row["steam_appid"])
        value = row["tags"]
        if not any(tag.strip() for tag in value.split(",")):
            blank_tags += 1
            continue
        choices = tags_by_id.setdefault(key, set())
        duplicate_tags += value in choices
        choices.add(value)
    conflicting_tags = sum(len(choices) > 1 for choices in tags_by_id.values())

    observations = {}
    duplicate_games = conflicting_games = incomplete_games = missing_tags = 0
    for row in games:
        key = appid(row["steam_appid"])
        observation = key, utc_observation(row["ts_ingested"])
        row["steam_appid"] = str(key)
        if observation in observations:
            if observations[observation] == row:
                duplicate_games += 1
            else:
                conflicting_games += 1
            continue
        observations[observation] = row
        incomplete_games += any(not row[column].strip() for column in CRITICAL_GAME_TEXT)
        missing_tags += key not in tags_by_id
        currency = row["steam_currency"].strip()
        if currency and currency != "USD":
            raise ContractError("Game prices must use USD minor units")
        if not currency and any(row[column].strip() not in {"", "0"}
                                for column in ("steam_price_initial", "steam_price_final")):
            raise ContractError("Nonzero game prices require USD currency")

    if conflicting_tags or conflicting_games or incomplete_games or missing_tags:
        raise ContractError(
            f"Unresolved enrichment: {missing_tags} observations missing tags, "
            f"{conflicting_tags} conflicting tag IDs, {conflicting_games} conflicting game rows, "
            f"{incomplete_games} incomplete game rows"
        )
    output = [
        dict(row, tags=next(iter(tags_by_id[key])))
        for (key, _), row in observations.items()
    ]
    return output, {
        "game_input_rows": len(games), "tag_input_rows": len(tags),
        "duplicate_games": duplicate_games, "duplicate_tags": duplicate_tags, "blank_tags": blank_tags,
    }
