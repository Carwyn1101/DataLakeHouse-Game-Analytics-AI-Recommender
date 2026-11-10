"""Native scenarios. Faults are injected at documented call boundaries only."""

import csv
from datetime import datetime
import hashlib
import io
import re
from unittest.mock import patch

import fixtures
from runtime import require

GAMES = "hive.silver.games"
DIM_GAME = "hive.gold.dim_game"
RAW_GAMES = "bronze/games.csv"
GOLD_STEPS = (
    "create_gold_database", "create_dim_date", "create_dim_game",
    "create_dims_and_bridges", "create_dim_detail_info", "create_dim_review_info",
    "create_dim_popularity_info", "create_fact_game_snapshot",
    "create_fact_review_snapshot", "create_fact_popularity_snapshot",
)


class InjectedFault(Exception):
    """A fault requested by a scenario, distinct from an unexpected native failure."""


def fails(action, expected):
    try:
        action()
    except expected:
        return
    raise RuntimeError("Operation unexpectedly succeeded")


def bronze(run):
    from minio.error import S3Error

    fixtures.initial(run)
    initial = list(csv.DictReader(io.StringIO(run.objects.read(RAW_GAMES).decode())))
    require([int(row["steam_appid"]) for row in initial] == [101, 202], "Initial Bronze keys")
    fixtures.update(run)
    updated = run.objects.read(RAW_GAMES)
    rows = list(csv.DictReader(io.StringIO(updated.decode())))
    require(len(rows) == 3, "Expected exactly three Bronze rows")
    by_id = {int(row["steam_appid"]): row for row in rows}
    require(set(by_id) == {101, 202, 303} and by_id[101]["steam_price_final"] == "800",
            "Incremental update or target-only key lost")
    puts = run.objects.puts
    fixtures.update(run)
    for change in (
        {"steam_appid": "1.01e2", "steam_price_final": 800, "recommendations_total": 120,
         "name": "Orbit Quest Updated", "ts_ingested": fixtures.T1},
        {"steam_price_final": 10, "ts_ingested": fixtures.T0},
        {"steam_price_final": 10, "ts_ingested": fixtures.T1},
    ):
        run.ingest("games", fixtures.csv_bytes("games", change))
    require(run.objects.puts == puts and run.objects.read(RAW_GAMES) == updated,
            "Unchanged, legacy, stale or equal-timestamp replay wrote data")
    legacy_target, aliases = re.subn(rb"(?m)^101,", b"101.0,", updated)
    require(aliases == 1, "Legacy target fixture lacks the expected key")
    run.objects.seed(RAW_GAMES, legacy_target)
    puts = run.objects.puts
    fixtures.update(run)
    require(run.objects.puts == puts and run.objects.read(RAW_GAMES) == legacy_target,
            "Unchanged legacy target key was duplicated or overwritten")
    incoming = fixtures.csv_bytes("games", {"ts_ingested": fixtures.T2})
    # Both a malformed target and duplicate logical target keys must fail without an overwrite.
    for bad_target in (b"not_a_games_header\ninvalid\n",
                       fixtures.csv_bytes("games", {}, {"steam_appid": "101.0"}),
                       fixtures.csv_bytes("games", {"steam_appid": "invalid"}),
                       fixtures.csv_bytes("games", {"ts_ingested": "invalid"})):
        run.objects.seed(RAW_GAMES, bad_target)
        puts = run.objects.puts
        fails(lambda: run.ingest("games", incoming), ValueError)
        require(run.objects.puts == puts and run.objects.read(RAW_GAMES) == bad_target,
                "Invalid Bronze target was overwritten")
    run.objects.seed(RAW_GAMES, updated)
    denied = S3Error(response=None, code="AccessDenied", message="Synthetic denial",
                     resource=RAW_GAMES, request_id="synthetic", host_id="synthetic")
    puts = run.objects.puts
    with patch.object(run.objects, "get_object", side_effect=denied):
        fails(lambda: run.ingest("games", incoming), S3Error)
    require(run.objects.puts == puts and run.objects.read(RAW_GAMES) == updated,
            "Denied Bronze read changed the target")
    print("Bronze final object SHA-256:", hashlib.sha256(updated).hexdigest())


def snapshot_ids(spark, table):
    return sorted(row.snapshot_id for row in spark.table(f"{table}.snapshots").select("snapshot_id").collect())


def silver_state(run):
    with run.session() as spark:
        return {
            name: (
                sorted((row.steam_appid, row.scd_version, row.is_current,
                        row.expiry_date is not None)
                       for row in spark.table(f"hive.silver.{name}").collect()),
                snapshot_ids(spark, f"hive.silver.{name}"),
            )
            for name in fixtures.F0
        }


def silver_initial(run):
    fixtures.initial(run)
    run.silver.bronze_to_silver_etl()
    expected = [(101, 1, True, False), (202, 1, True, False)]
    require(all(rows == expected for rows, _ in silver_state(run).values()),
            "Initial Silver version or current flag")


def silver(run):
    silver_initial(run)
    with run.session() as spark:
        require(run.silver._table_exists(spark, GAMES), "Existing Silver table was not found")
        require(not run.silver._table_exists(spark, "hive.silver.absent"), "Missing Silver table was found")
        old_price = spark.table(GAMES).where("steam_appid = 101").first().steam_price_final
    fixtures.update(run)
    run.silver.bronze_to_silver_etl()
    state = silver_state(run)
    base = [(101, 1, False, True), (101, 2, True, False), (202, 1, True, False)]
    require(all(rows == base + ([(303, 1, True, False)] if name == "games" else [])
                for name, (rows, _) in state.items()), "Silver history or new key")
    with run.session() as spark:
        price = spark.table(GAMES).where("steam_appid = 101 AND scd_version = 1").first().steam_price_final
        require(old_price == price, "Historical Silver business values changed")
    run.silver.bronze_to_silver_etl()
    require(silver_state(run) == state, "Unchanged rerun changed Silver rows or snapshots")


def catalog_failure(run, helper, table):
    from py4j.protocol import Py4JJavaError
    from pyspark.sql.utils import AnalysisException

    with run.session(unavailable_catalog=True) as spark:
        fails(lambda: helper(spark, table), (Py4JJavaError, AnalysisException))


def silver_catalog(run):
    catalog_failure(run, run.silver._table_exists, GAMES)


def silver_collision(run):
    from py4j.protocol import Py4JJavaError
    from pyspark.sql.utils import AnalysisException

    fixtures.initial(run)
    attach = run.silver._add_scd2_columns
    injected = []

    def create_first(frame, version=1):
        result = attach(frame, version)
        if not injected:
            result.limit(1).writeTo(GAMES).using("iceberg").tableProperty("format-version", "2").create()
            injected.append(snapshot_ids(frame.sparkSession, GAMES))
        return result

    with patch.object(run.silver, "_add_scd2_columns", create_first):
        fails(run.silver.bronze_to_silver_etl, (Py4JJavaError, AnalysisException))
    require(len(injected) == 1, "Create collision was not reached")
    with run.session() as spark:
        require(spark.table(GAMES).count() == 1 and snapshot_ids(spark, GAMES) == injected[0],
                "Silver create-only replaced the racing table")


def silver_fault(run, boundary):
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.readwriter import DataFrameWriterV2

    silver_initial(run)
    before = silver_state(run)["games"]
    with run.session() as spark:
        incoming = (spark.table(GAMES).where("steam_appid = 101")
                    .drop("effective_date", "expiry_date", "is_current", "scd_version")
                    .withColumn("name_cleaned", F.lit("Orbit Quest Updated")))
        # Detach the synthetic input from the target scan before fault injection.
        incoming = spark.createDataFrame(incoming.collect(), incoming.schema)
        append = DataFrameWriterV2.append
        sql = SparkSession.sql

        def append_fault(writer):
            if boundary == "lost-ack":
                append(writer)
            raise InjectedFault

        def sql_fault(session, statement, *args, **kwargs):
            if statement.strip().upper().startswith("UPDATE "):
                raise InjectedFault
            return sql(session, statement, *args, **kwargs)

        def upsert(session):
            return run.silver._upsert_scd2(session, incoming, GAMES, "steam_appid", ["name_cleaned"])

        rows, schema = incoming.collect(), incoming.schema
        fault = (patch.object(SparkSession, "sql", sql_fault) if boundary == "before-update"
                 else patch.object(DataFrameWriterV2, "append", append_fault))
        with fault:
            fails(lambda: upsert(spark), InjectedFault)
    interrupted = silver_state(run)["games"]
    print("Silver snapshots before/interrupted:", before[1], interrupted[1])
    if boundary == "before-update":
        require(interrupted == before, "Before-UPDATE failure changed Silver state")
    elif boundary == "append-failure":
        require(interrupted[0] == [(101, 1, False, True), (202, 1, True, False)],
                "Unexpected state between UPDATE and APPEND")
    else:
        require(interrupted[0] == [(101, 1, False, True), (101, 2, True, False), (202, 1, True, False)],
                "APPEND did not commit before lost acknowledgement")
    with run.session() as spark:
        incoming = spark.createDataFrame(rows, schema)
        require(upsert(spark) == (0 if boundary == "lost-ack" else 1), "Unexpected Silver retry count")
    recovered = silver_state(run)["games"]
    require(recovered[0] == [(101, 1, False, True), (101, 2, True, False), (202, 1, True, False)],
            "Silver retry did not restore exactly one current row at version two")
    if boundary == "lost-ack":
        require(recovered == interrupted, "Lost acknowledgement retry wrote another snapshot")
    print("Silver snapshots after retry:", recovered[1])


def gold_state(run):
    with run.session() as spark:
        pairs = sorted((row.steam_appid, row.game_key) for row in spark.table(DIM_GAME).collect())
        return pairs, snapshot_ids(spark, DIM_GAME)


def gold(run):
    def verify_missing(name):
        try:
            run.gold.verify_gold_layer()
        except RuntimeError as error:
            require(f"cannot count required table hive.gold.{name}" in str(error),
                    "Unexpected Gold verification failure")
        else:
            raise RuntimeError("Missing Gold table did not fail verification")

    silver_initial(run)
    run.gold.create_gold_database()
    verify_missing("dim_date")
    run.gold.create_dim_date()
    verify_missing("dim_game")
    for step in GOLD_STEPS[2:]:
        getattr(run.gold, step)()
    counts = run.gold.verify_gold_layer()
    require(len(counts) == 18 and counts["hive.gold.dim_date"] == 11323, "Gold verification counts")
    require(all(counts[f"hive.gold.fact_{fact}_snapshot"] == 2
                for fact in ("game", "review", "popularity")), "Initial Gold fact counts")
    pairs, _ = gold_state(run)
    require(len(pairs) == 2 and len({key for _, key in pairs}) == 2, "Gold key uniqueness")
    with run.session() as spark:
        for fact in ("game", "review", "popularity"):
            keys = {row.game_key for row in spark.table(f"hive.gold.fact_{fact}_snapshot").collect()}
            require(keys == {key for _, key in pairs}, "Gold fact references wrong game keys")


def gold_helpers(run):
    from py4j.protocol import Py4JJavaError
    from pyspark.sql.utils import AnalysisException

    silver_initial(run)
    run.gold.create_gold_database()
    run.gold.create_dim_game()
    fixture = "hive.gold.fixture_keys"
    with run.session() as spark:
        require(run.gold.table_exists(spark, DIM_GAME), "Existing Gold table was not found")
        require(not run.gold.table_exists(spark, fixture), "Missing Gold table was found")
        require(run.gold.get_max_key(spark, DIM_GAME, "game_key") == 2, "Wrong Gold key maximum")
        fails(lambda: run.gold.get_max_key(spark, DIM_GAME, "absent"), (Py4JJavaError, AnalysisException))
        fails(lambda: run.gold.get_max_key(spark, fixture, "game_key"), (Py4JJavaError, AnalysisException))
        empty = spark.range(0).selectExpr("CAST(id AS INT) AS game_key")
        empty.writeTo(fixture).using("iceberg").create()
        require(run.gold.get_max_key(spark, fixture, "game_key") == 0, "Wrong empty Gold maximum")
        spark.range(1).selectExpr("CAST(NULL AS INT) AS game_key").writeTo(fixture).append()
        fails(lambda: run.gold.get_max_key(spark, fixture, "game_key"), ValueError)
    before = gold_state(run)
    catalog_failure(run, run.gold.table_exists, DIM_GAME)
    with patch.object(run.gold, "get_max_key", side_effect=InjectedFault):
        fails(run.gold.create_dim_game, InjectedFault)
    require(gold_state(run) == before, "Gold key read failure changed the target")


def gold_partial(run):
    from pyspark.sql.readwriter import DataFrameWriterV2

    gold(run)
    before = gold_state(run)
    fixtures.update(run)
    run.silver.bronze_to_silver_etl()
    with patch.object(DataFrameWriterV2, "append", side_effect=InjectedFault):
        fails(run.gold.create_dim_game, InjectedFault)
    interrupted = gold_state(run)
    run.gold.create_dim_game()
    recovered = gold_state(run)
    print("Gold game key mapping and snapshots before/interrupted/retried:", before, interrupted, recovered)
    before_keys = dict(before[0])
    recovered_keys = dict(recovered[0])
    require(len(recovered[0]) == len(recovered_keys) == 3, "Missing or duplicate Gold business keys")
    require(len(set(recovered_keys.values())) == 3, "Duplicate Gold surrogate keys")
    require(all(recovered_keys.get(key) == value for key, value in before_keys.items()),
            "Gold retry remapped an existing game key")


def gold_collision(run):
    silver_initial(run)
    run.gold.create_gold_database()
    original = run.gold.table_exists
    injected = []

    def create_first(spark, table):
        exists = original(spark, table)
        if table == DIM_GAME and not exists:
            spark.range(1).selectExpr("CAST(id AS INT) AS marker").writeTo(DIM_GAME).using("iceberg").create()
            injected.extend(snapshot_ids(spark, DIM_GAME))
            print("Racing Gold table created before production initial write")
        return exists

    from py4j.protocol import Py4JJavaError
    from pyspark.sql.utils import AnalysisException

    with patch.object(run.gold, "table_exists", create_first):
        try:
            run.gold.create_dim_game()
        except (Py4JJavaError, AnalysisException) as error:
            print("Gold initial write rejected:", type(error).__name__)
    with run.session() as spark:
        require(injected and spark.table(DIM_GAME).columns == ["marker"] and
                spark.table(DIM_GAME).count() == 1 and snapshot_ids(spark, DIM_GAME) == injected,
                "Gold initial write replaced the racing table")


SCENARIOS = {
    "bronze": bronze,
    "silver": silver,
    "silver-catalog": silver_catalog,
    "silver-create-collision": silver_collision,
    "silver-before-update": lambda run: silver_fault(run, "before-update"),
    "silver-append-failure": lambda run: silver_fault(run, "append-failure"),
    "silver-lost-ack": lambda run: silver_fault(run, "lost-ack"),
    "gold": gold,
    "gold-helpers": gold_helpers,
    "gold-partial": gold_partial,
    "gold-create-collision": gold_collision,
}
