"""Offline HTTP, Steam record and enrichment tests with synthetic fixtures."""

from datetime import datetime, timezone
from email.utils import formatdate
import io
import json
import unittest
from unittest.mock import Mock
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from collectors.contracts import (
    ContractError, ENRICHED_GAME_COLUMNS, GAME_COLUMNS, RAW_COLUMNS, TAG_COLUMNS,
    appid, encode_csv, enrich_games, read_csv,
)
from collectors.steam import HttpClient, SteamCollector


STAMP = "2026-01-02T00:00:00+00:00"
GAME = dict(steam_appid=1, name='Synthetic, "Game"', release_date="2020-01-01", is_free=False,
            steam_metacritic_score=75, steam_price_initial=1299, steam_price_final=999,
            steam_currency="USD", developers="Studio", publishers="Publisher", genres="Action",
            categories="Multi-player", required_age=0, recommendations_total=4, ts_ingested=STAMP)
TAG_TEXT = "RPG, H\u00e0nh \u0111\u1ed9ng, Co-op"


class Response(io.BytesIO):
    status = 200
    headers = {}


class Clock:
    value = 0.0

    def now(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


def status_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return HTTPError("https://fixture.invalid", code, "private-response", headers, None)


class HttpTests(unittest.TestCase):
    def client(self, *responses, **kwargs):
        clock = Clock()
        start_times = []

        def respond(*args, **options):
            start_times.append(clock.now())
            response = next(responses_iter)
            if isinstance(response, Exception):
                raise response
            return Response(response)

        responses_iter = iter(responses)
        opener = Mock(side_effect=respond)
        client = HttpClient(opener=opener, sleep=clock.sleep, monotonic=clock.now, epoch=clock.now, **kwargs)
        return client, opener, start_times

    def test_timeout_then_503_then_success(self):
        client, opener, starts = self.client(TimeoutError(), status_error(503), b'{"ok":true}', timeout=7)
        self.assertEqual(client.get("https://fixture.invalid", {"x": 1}), {"ok": True})
        self.assertEqual(starts, [0, .5, 1.5])
        self.assertTrue(all(call.kwargs["timeout"] == 7 for call in opener.call_args_list))

    def test_429_honors_seconds_and_date(self):
        for value in ("3", formatdate(3, usegmt=True)):
            with self.subTest(value=value):
                client, _, starts = self.client(status_error(429, value), b"{}")
                client.get("https://fixture.invalid", {}, steamspy=True)
                self.assertEqual(starts, [0, 3])

    def test_steamspy_paces_retries_and_successive_requests(self):
        client, _, starts = self.client(status_error(503), b"{}", b"{}")
        client.get("https://fixture.invalid", {}, steamspy=True)
        client.get("https://fixture.invalid", {}, steamspy=True)
        self.assertEqual(starts, [0, 1.1, 2.2])

    def test_bounded_exhaustion_and_no_retry_for_permanent_errors(self):
        client, opener, _ = self.client(*[TimeoutError()] * 3)
        with self.assertRaisesRegex(ContractError, "exhausted"):
            client.get("https://fixture.invalid", {})
        self.assertEqual(opener.call_count, 3)
        for response, message in [(status_error(404), "Non-retryable"),
                                  (status_error(429, "120"), "delay limit"),
                                  (b"[1]", "must be an object"), (b"not-json", "Malformed"),
                                  (b'{"tags":{"RPG":1,"RPG":2}}', "Duplicate keys")]:
            with self.subTest(message=message):
                client, opener, _ = self.client(response)
                with self.assertRaisesRegex(ContractError, message):
                    client.get("https://fixture.invalid", {})
                opener.assert_called_once()


def api_payloads():
    return [
        {"1": {"success": True, "data": {
            "type": "game", "steam_appid": 1, "name": GAME["name"], "is_free": False,
            "release_date": {"date": "2020-01-01"},
            "price_overview": {"initial": 1299, "final": 999, "currency": "USD"},
            "developers": ["Studio"], "publishers": ["Publisher"], "genres": [{"description": "Action"}],
            "categories": [{"description": "Multi-player"}], "required_age": "0",
        }}},
        {"success": 1, "query_summary": {"total_positive": 8, "total_negative": 2, "total_reviews": 10}},
        {"response": {"player_count": 13}},
        {"appid": 1, "owners": "1,000 .. 2,000", "average_2weeks": 120, "average_forever": 240,
         "median_forever": 60, "tags": dict.fromkeys(TAG_TEXT.split(", "), 1)},
    ]


class CollectorTests(unittest.TestCase):
    def capture(self, payloads):
        opener = Mock(side_effect=[Response(json.dumps(payload).encode()) for payload in payloads])
        collector = SteamCollector(HttpClient(opener=opener), now=lambda: datetime(2026, 1, 2, tzinfo=timezone.utc))
        return collector.capture("001", "all"), opener

    def test_valid_response_fields_units_and_timestamp(self):
        rows, opener = self.capture(api_payloads())
        for name, fields in RAW_COLUMNS.items():
            self.assertEqual(set(rows[name]), set(fields))
        self.assertEqual(rows["games.csv"]["steam_price_final"], 999)
        self.assertIsNone(rows["games.csv"]["steam_metacritic_score"])
        self.assertEqual(rows["reviews.csv"]["steam_review_positive_ratio"], .8)
        self.assertEqual(rows["popularity.csv"]["steam_concurrent_players"], 13)
        self.assertEqual(rows["popularity.csv"]["steamspy_players_2weeks"], 120)
        self.assertEqual(rows["popularity.csv"]["steamspy_average_forever_min"], 240)
        self.assertEqual(rows["game_tags.csv"], {"steam_appid": 1, "tags": TAG_TEXT})
        self.assertEqual(rows["games.csv"]["ts_ingested"], STAMP)
        self.assertEqual(rows["reviews.csv"]["ts_ingested"], STAMP)
        self.assertEqual(rows["popularity.csv"]["ts"], STAMP)
        urls = [urlsplit(c.args[0].full_url) for c in opener.call_args_list]
        self.assertEqual([u.scheme for u in urls], ["https"] * 4)
        self.assertEqual(parse_qs(urls[0].query), {"appids": ["1"], "cc": ["us"], "l": ["en"]})
        self.assertEqual(parse_qs(urls[3].query), {"request": ["appdetails"], "appid": ["1"]})

    def test_missing_optional_values_and_zero_reviews(self):
        payload = api_payloads()
        payload[1]["query_summary"] = dict(total_positive=0, total_negative=0, total_reviews=0)
        payload[2]["response"] = {}
        payload[3] = {"appid": 1, "tags": []}
        rows, _ = self.capture(payload)
        self.assertEqual(rows["reviews.csv"]["steam_review_positive_ratio"], "")
        self.assertIsNone(rows["popularity.csv"]["steam_concurrent_players"])
        self.assertIsNone(rows["popularity.csv"]["steamspy_players_2weeks"])
        self.assertEqual(rows["game_tags.csv"]["tags"], "")

    def test_malformed_api_fields_fail_before_persistence(self):
        for idx, value, expected in [
            (0, {"1": {"success": True, "data": {"type": "game"}}}, "is_free"),
            (1, {"success": 1, "query_summary": {"total_reviews": 8}}, "positive reviews"),
            (2, {"response": {"player_count": True}}, "concurrent players"),
            (3, {"appid": 2}, "requested game"),
            (3, {"appid": 1, "tags": ["Action"]}, "tags must be an object"),
        ]:
            with self.subTest(expected=expected):
                payload = api_payloads()
                payload[idx] = value
                with self.assertRaisesRegex(ContractError, expected):
                    self.capture(payload)


class EnrichmentTests(unittest.TestCase):
    def test_exact_contract_utf8_quoting_and_duplicate_policy(self):
        raw = encode_csv(GAME_COLUMNS, [GAME, dict(GAME, steam_appid="0001"),
                                       dict(GAME, ts_ingested="2026-01-03T00:00:00Z")])
        sidecar = encode_csv(TAG_COLUMNS, [{"steam_appid": key, "tags": value}
                                         for key, value in [(1, ""), (1, TAG_TEXT), ("001", TAG_TEXT)]])
        rows, counts = enrich_games(raw, b"\xef\xbb\xbf" + sidecar)
        self.assertEqual(len(rows), 2)
        self.assertEqual(counts["duplicate_games"], 1)
        self.assertEqual(counts["duplicate_tags"], 1)
        self.assertEqual(counts["blank_tags"], 1)
        self.assertEqual([row["ts_ingested"] for row in rows], [STAMP, "2026-01-03T00:00:00Z"])
        payload = encode_csv(ENRICHED_GAME_COLUMNS, rows)
        self.assertEqual(payload.splitlines()[0].decode(),
            "steam_appid,name,release_date,is_free,steam_metacritic_score,steam_price_initial,"
            "steam_price_final,steam_currency,developers,publishers,genres,categories,"
            "required_age,recommendations_total,ts_ingested,tags")
        self.assertIn(b'"Synthetic, ""Game"""', payload)
        self.assertEqual(read_csv(payload, ENRICHED_GAME_COLUMNS)[0]["tags"], TAG_TEXT)

    def test_unresolved_tags_and_conflicting_games_rejected(self):
        for tags, message in [([], "missing tags"), (["", " , "], "missing tags"),
                              (["RPG, Co-op", "Co-op, RPG"], "1 conflicting tag IDs")]:
            with self.subTest(tags=tags):
                sidecar = encode_csv(TAG_COLUMNS, [dict(steam_appid=1, tags=tag) for tag in tags])
                with self.assertRaisesRegex(ContractError, message):
                    enrich_games(encode_csv(GAME_COLUMNS, [GAME]), sidecar)
        with self.assertRaisesRegex(ContractError, "1 conflicting game rows"):
            enrich_games(encode_csv(GAME_COLUMNS, [GAME, dict(GAME, name="Conflicting")]),
                         encode_csv(TAG_COLUMNS, [dict(steam_appid=1, tags=TAG_TEXT)]))

    def test_invalid_ids_and_csv_contract(self):
        for invalid in (False, 1.0, "1.0", "-1", "0", 2**63, "\u0661"):
            with self.subTest(invalid=invalid), self.assertRaises(ContractError):
                appid(invalid)
        sidecar = encode_csv(TAG_COLUMNS, [dict(steam_appid=1, tags=TAG_TEXT)])
        with self.assertRaisesRegex(ContractError, "header"):
            enrich_games(encode_csv(ENRICHED_GAME_COLUMNS, [dict(GAME, tags=TAG_TEXT)]), sidecar)
        with self.assertRaisesRegex(ContractError, "timestamp"):
            enrich_games(encode_csv(GAME_COLUMNS, [dict(GAME, ts_ingested="2026-01-02")]), sidecar)
        with self.assertRaisesRegex(ContractError, "incomplete game"):
            enrich_games(encode_csv(GAME_COLUMNS, [dict(GAME, publishers="")]), sidecar)
