# Collector staging contract

This workflow prepares files for review in an isolated staging directory. Publishing to the Airflow mount or Bronze requires separate approval.

## Verified input and output schemas

The ordered historical fields below come from `7067e7c`. The final `tags` position was verified from the header only of the existing enriched `games.csv`. The header establishes column names and order. It does not establish row contents or how the existing CSV was enriched.

| File | Ordered fields |
| --- | --- |
| Raw games.csv | steam_appid, name, release_date, is_free, steam_metacritic_score, steam_price_initial, steam_price_final, steam_currency, developers, publishers, genres, categories, required_age, recommendations_total, ts_ingested |
| Tags sidecar game_tags.csv | steam_appid, tags |
| Enriched games.csv | The 15 raw games fields in the same order, followed by tags |
| reviews.csv | steam_appid, steam_review_total_positive, steam_review_total_negative, steam_review_total, steam_review_score_desc, steam_review_positive_ratio, ts_ingested |
| popularity.csv | steam_appid, ts, steam_concurrent_players, steamspy_owners_range, steamspy_players_2weeks, steamspy_average_forever_min, steamspy_median_forever_min |

- CSV input is UTF-8 with optional BOM. Output is UTF-8, comma-delimited, with standard double-quote escaping. A tag list is one CSV field containing comma-separated text. Sidecar join preserves the text and its order. Gold splits on commas and AI uses the original order for top tags.
- App IDs are positive decimal integers within signed 64-bit range. Leading zeros are canonicalized. Collection has one observation per App ID per batch. Enrichment preserves distinct observations of the same ID and drops identical replays of the same ID and UTC timestamp. Conflicting games at the same key block completion.
- `ts_ingested` and `ts` are ISO timestamps in UTC. The collector assigns one timestamp when an App ID observation is collected and uses it across the three files. Enrichment preserves its original text. Resume reuses durable observations.
- Game price fields are integer USD minor units for the US storefront. Silver divides by 100 and derives VND. Optional unavailable numeric fields remain blank in raw staging. The review ratio is positive count divided by total, on a 0–1 scale, and remains blank when the total is zero.
- `steam_concurrent_players` is a count. The SteamSpy owners range is text. Legacy `steamspy_players_2weeks` carries `average_2weeks` in **minutes of average playtime**. Lifetime mean and median are also collected in minutes and converted to hours by Silver.
- Enrichment requires nonblank name, release date, developers, publishers, genres, categories and tags. Identical nonblank tags for an ID collapse. Blank sidecar entries provide no value. Multiple different nonblank values are conflicts. Missing or conflicting tags block the batch. No timestamp-based selection is possible from the sidecar contract.

## Dependency boundary

Historical flow: Steam data → 15-column games CSV, SteamSpy tags → sidecar, then an unverified enrichment step → the existing 16-column CSV.

Staging flow: explicit App IDs → collector batch → raw games and tags sidecar → standalone enrichment → a new 16-column games CSV in staging. Bronze's mount and the existing enriched CSV remain separate. Silver reads the three established filenames and expects tags embedded in games. Tags-only and two-week-playtime-only SCD2 detection remain separate reliability issues.

## Run and review

Run from the repository root. Choose a dedicated, private staging root outside the repository and the configured Airflow `source_data` bind mount. The guard reads that mount from the repository Compose file and rejects unresolved paths and overlaps, including the thesis workspace that contains the configured data directory. Enrichment inputs and optional ID files must resolve inside staging. Verify separately that no external Compose override watches the chosen root.

Offline checks:

- `python -B -m collectors --help`
- `python -B -m unittest discover -s tests/collectors -v`

Only after separate approval for live API calls, use `collect` with approved IDs. Supply `--app-id` one or more times, or a staged UTF-8 ID file, one ID per line. Automatic discovery is omitted. For example, in PowerShell set `$stage` to the isolated directory, put approved IDs in `$stage/ids.txt`, and run:

- `python -B -m collectors --stage-dir $stage collect --batch-id capture-001 --ids-file "$stage/ids.txt"`
- `python -B -m collectors --stage-dir $stage enrich --batch-id joined-001 --games "$stage/raw/capture-001/games.csv" --tags "$stage/raw/capture-001/game_tags.csv"`

The second command can also use historical-format inputs already present in the isolated directory. The commands produce immutable completed directories under `raw/<batch-id>` and `enriched/<batch-id>`. The raw batch includes games, reviews, popularity and the tags sidecar. The enriched batch includes the 16-column games file. Keep raw and enriched outputs in staging for review.

## Retry, validation and resume guarantees

- Requests use HTTPS, a configurable socket timeout, and bounded exponential backoff on timeouts, connection/read failures, 429 and 5xx. The default is three attempts, 15 seconds of socket timeout and a 60-second retry delay cap. Retry-After can be seconds or an HTTP date. A requested wait above the cap stops the attempt instead of retrying early. SteamSpy attempts are at least 1.1 seconds apart by default within one client, including retries. Avoid parallel collectors sharing the same API rate budget.
- Collection sorts and deduplicates App IDs before calls. The plan binds batch ID to the normalized App IDs and review language. Each committed per-game checkpoint holds normalized rows and a digest. Resume skips verified checkpoints and keeps their UTC timestamps. An observation that failed before a durable checkpoint may be collected again. To collect a genuinely new observation, use a new batch ID.
- Enrichment binds its batch ID to the two input hashes. It reports counts for unresolved input and leaves the batch incomplete. If the inputs are corrected, use a new batch ID. A rejected report contains only validation status and safe reason text. Successful manifests record identity, row counts and hashes. Raw records in staging and checkpoints should remain private.
- Checkpoints use same-directory temporary files and atomic replacement where supported. A completed staging directory is exposed only after all of its files and manifest are written, via a same-parent rename. Ignore `.pending-*` directories left by an interruption. This is a local staging boundary. It does not coordinate three-file publication to Airflow or provide cross-volume or power-loss guarantees.
- Replay of a completed batch verifies matching identity and file hashes before returning it. Corrupt, unreadable or incomplete existing state raises an error. Concurrent writers for the same batch are rejected. After an abrupt process exit, review a stale lock and confirm no writer is running before removing only that batch's lock.

Offline tests exercise mocked transport, CSV contracts, an AST-selected Silver preprocessing function and synthetic interruption/resume. Live APIs, native Spark CSV ingestion and production publication require separate approval and verification.
