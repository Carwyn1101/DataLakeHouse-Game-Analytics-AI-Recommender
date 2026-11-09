"""Validated Steam and SteamSpy collection with no staging side effects."""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.client import IncompleteRead
import json
import math
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .contracts import ContractError, appid, utc_observation


class HttpClient:
    """Bounded GET retries. Pace every SteamSpy attempt within this client."""

    def __init__(self, *, timeout=15.0, attempts=3, max_delay=60.0, steamspy_interval=1.1,
                 opener=urlopen, sleep=time.sleep, monotonic=time.monotonic, epoch=time.time):
        if (type(attempts) is not int or not 1 <= attempts <= 10
                or any(not math.isfinite(x) or x <= 0 for x in (timeout, max_delay, steamspy_interval))
                or steamspy_interval < 1):
            raise ContractError("Invalid HTTP timeout, retry or pacing configuration")
        self.timeout = timeout
        self.attempts = attempts
        self.max_delay = max_delay
        self.interval = steamspy_interval
        self.opener = opener
        self.sleep = sleep
        self.monotonic = monotonic
        self.epoch = epoch
        self.last_steamspy = None

    def _retry_after(self, header):
        if not header:
            return 0.0
        value = header.strip()
        if value.isascii() and value.isdecimal():
            return float(value)
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0.0, date.timestamp() - self.epoch())
        except (ValueError, TypeError, OverflowError, OSError):
            return 0.0

    def get(self, endpoint, params, *, steamspy=False):
        request = Request(f"{endpoint}?{urlencode(params)}", headers={
            "Accept": "application/json", "User-Agent": "game-lakehouse-staging/1.0",
        })
        for attempt in range(self.attempts):
            if steamspy:
                if self.last_steamspy is not None:
                    delay = self.last_steamspy + self.interval - self.monotonic()
                    if delay > 0:
                        self.sleep(delay)
                self.last_steamspy = self.monotonic()
            retry_after = 0.0
            try:
                with self.opener(request, timeout=self.timeout) as response:
                    if not 200 <= response.status < 300:
                        raise HTTPError(request.full_url, response.status, "", response.headers, None)
                    payload = response.read(2_000_001)
                if len(payload) > 2_000_000:
                    raise ContractError("API response exceeds size limit")
                return object_value(json.loads(payload, object_pairs_hook=unique_json_object), "API response")
            except HTTPError as error:
                if error.code != 429 and not 500 <= error.code < 600:
                    error.close()
                    raise ContractError(f"Non-retryable API HTTP status {error.code}") from None
                retry_after = self._retry_after(error.headers.get("Retry-After") if error.headers else None)
                error.close()
            except (URLError, TimeoutError, ConnectionError, IncompleteRead):
                pass
            except (UnicodeError, json.JSONDecodeError):
                raise ContractError("Malformed API JSON") from None

            if attempt + 1 == self.attempts:
                raise ContractError("API retry attempts exhausted") from None
            if retry_after > self.max_delay:
                raise ContractError("Retry-After exceeds the configured retry delay limit")
            self.sleep(max(retry_after, min(self.max_delay, 0.5 * 2**attempt)))


def unique_json_object(pairs):
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise ContractError("Duplicate keys in API JSON")
    return obj


def object_value(value, label, *, optional=False):
    if value is None and optional:
        return {}
    if not isinstance(value, dict):
        raise ContractError(f"{label} must be an object")
    return value


def text_value(value, label, *, required=False):
    if value is None and not required:
        return ""
    if not isinstance(value, str) or (required and not value.strip()):
        raise ContractError(f"Invalid {label} text")
    return value


def integer_value(value, label, *, required=False):
    if value is None and not required:
        return None
    if type(value) is not int or not 0 <= value < 2**63:
        raise ContractError(f"Invalid {label} count or integer value")
    return value


def labels(value, field, *, objects=False):
    if value is None:
        return ""
    if not isinstance(value, list):
        raise ContractError(f"{field} must be a list")
    if objects:
        value = [object_value(item, field).get("description") for item in value]
    return ", ".join(text_value(item, field, required=True) for item in value)


def game_fields(payload, key):
    envelope = object_value(payload.get(str(key)), "Steam game")
    if envelope.get("success") is not True:
        raise ContractError("Steam game details unavailable")
    game = object_value(envelope.get("data"), "Steam details")
    if game.get("type") != "game" or ("steam_appid" in game and appid(game["steam_appid"]) != key):
        raise ContractError("Steam details do not match the requested game")
    if type(game.get("is_free")) is not bool:
        raise ContractError("Invalid Steam is_free value")
    price = object_value(game.get("price_overview"), "Steam price", optional=True)
    currency = text_value(price.get("currency"), "Steam currency", required=bool(price))
    if price and currency != "USD":
        raise ContractError("Steam price must use USD minor units")
    age = game.get("required_age")
    if isinstance(age, str):
        digits = age.strip().removesuffix("+")
        if not digits.isascii() or not digits.isdecimal() or len(digits) > 3:
            raise ContractError("Invalid required_age value")
        age = int(digits)
    return {
        "steam_appid": key,
        "name": text_value(game.get("name"), "name", required=True),
        "release_date": text_value(object_value(game.get("release_date"), "release date", optional=True).get("date"),
                                   "release date"),
        "is_free": game["is_free"],
        "steam_metacritic_score": integer_value(
            object_value(game.get("metacritic"), "metacritic", optional=True).get("score"), "metacritic"),
        "steam_price_initial": integer_value(price.get("initial"), "initial price", required=bool(price)),
        "steam_price_final": integer_value(price.get("final"), "final price", required=bool(price)),
        "steam_currency": currency,
        "developers": labels(game.get("developers"), "developers"),
        "publishers": labels(game.get("publishers"), "publishers"),
        "genres": labels(game.get("genres"), "genres", objects=True),
        "categories": labels(game.get("categories"), "categories", objects=True),
        "required_age": integer_value(age, "required_age"),
        "recommendations_total": integer_value(
            object_value(game.get("recommendations"), "recommendations", optional=True).get("total"), "recommendations"),
    }


def review_fields(payload, key):
    if payload.get("success") != 1:
        raise ContractError("Steam review summary unavailable")
    summary = object_value(payload.get("query_summary"), "Steam review summary")
    positive = integer_value(summary.get("total_positive"), "positive reviews", required=True)
    negative = integer_value(summary.get("total_negative"), "negative reviews", required=True)
    total = integer_value(summary.get("total_reviews"), "total reviews", required=True)
    if positive + negative != total:
        raise ContractError("Inconsistent Steam review counts")
    return {
        "steam_appid": key, "steam_review_total_positive": positive,
        "steam_review_total_negative": negative, "steam_review_total": total,
        "steam_review_score_desc": text_value(summary.get("review_score_desc"), "review score"),
        "steam_review_positive_ratio": positive / total if total else "",
    }


class SteamCollector:
    """Collect one observation and its tags using a single SteamSpy response."""

    def __init__(self, http, *, now=lambda: datetime.now(timezone.utc)):
        self.http = http
        self.now = now

    def capture(self, key, language):
        key = appid(key)
        game = game_fields(self.http.get("https://store.steampowered.com/api/appdetails",
                                        {"appids": key, "cc": "us", "l": "en"}), key)
        review = review_fields(self.http.get(f"https://store.steampowered.com/appreviews/{key}", {
            "json": 1, "language": language, "filter": "all", "review_type": "all",
            "purchase_type": "all", "num_per_page": 1, "cursor": "*",
        }), key)
        current = object_value(self.http.get(
            "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/",
            {"appid": key}).get("response"), "Steam concurrent players")
        spy = self.http.get("https://steamspy.com/api.php", {"request": "appdetails", "appid": key}, steamspy=True)
        if appid(spy.get("appid")) != key:
            raise ContractError("SteamSpy response does not match the requested game")
        raw_tags = spy.get("tags")
        tags = {} if raw_tags == [] else object_value(raw_tags, "SteamSpy tags", optional=True)
        tag_text = ", ".join(text_value(tag, "tag", required=True) for tag in tags)
        timestamp = self.now().isoformat()
        utc_observation(timestamp)
        return {
            "games.csv": dict(game, ts_ingested=timestamp),
            "reviews.csv": dict(review, ts_ingested=timestamp),
            "popularity.csv": {
                "steam_appid": key, "ts": timestamp,
                "steam_concurrent_players": integer_value(current.get("player_count"), "concurrent players"),
                "steamspy_owners_range": text_value(spy.get("owners"), "SteamSpy owners range"),
                "steamspy_players_2weeks": integer_value(spy.get("average_2weeks"), "two-week playtime"),
                "steamspy_average_forever_min": integer_value(spy.get("average_forever"), "mean playtime"),
                "steamspy_median_forever_min": integer_value(spy.get("median_forever"), "median playtime"),
            },
            "game_tags.csv": {"steam_appid": key, "tags": tag_text},
        }
