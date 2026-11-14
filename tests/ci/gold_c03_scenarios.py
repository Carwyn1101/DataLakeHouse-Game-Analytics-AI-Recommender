"""Run TP-19 and TP-25 against the pinned Gold dim_game function and synthetic data."""

from collections import Counter
from datetime import date
import json
from pathlib import Path
import re
import sys
import tempfile
from types import SimpleNamespace
from typing import NamedTuple
from unittest.mock import patch

DIM = "hive.gold.dim_game"
SILVER = "hive.silver.games"
COLUMNS = ("game_key", "steam_appid", "name", "release_date", "release_year",
           "game_age_year", "required_age", "is_free")
MERGE_PREFIX = "MERGE INTO hive.gold.dim_game AS t "
STAGE_NAME = "gold-dim-game-"


class ApplicationFault(RuntimeError):
    """Injected at the Python call boundary, outside the native commit."""


class State(NamedTuple):
    rows: tuple
    snapshots: tuple


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def state(spark):
    rows = tuple(tuple(row) for row in spark.table(DIM).select(*COLUMNS)
                 .orderBy(*COLUMNS).collect())
    snapshots = tuple(tuple(row) for row in spark.sql(
        f"SELECT snapshot_id, parent_id, operation FROM {DIM}.snapshots ORDER BY snapshot_id"
    ).collect())
    return State(rows, snapshots)


def one_snapshot(before, after):
    previous = {row[0]: row for row in before.snapshots}
    current = {row[0]: row for row in after.snapshots}
    tips = previous.keys() - {row[1] for row in before.snapshots}
    added = current.keys() - previous.keys()
    require(len(tips) == len(added) == 1 and len(after.snapshots) == len(before.snapshots) + 1,
            "Expected a single descendant snapshot")
    require(all(current.get(key) == value for key, value in previous.items()), "Snapshot history changed")
    snapshot = current[next(iter(added))]
    require(snapshot[1] in tips, "New snapshot did not descend from the previous tip")
    return snapshot


def gold_row(key, appid, name):
    return (key, appid, name, date(2020, 6, 1), 2020, 6, 0, False)


def assert_replacement(before, after, changed, inserted):
    # Count rows rather than using a set. Identical historical duplicates must be removed too.
    expected = Counter(row for row in before.rows if row[1] not in changed)
    expected.update(gold_row(key, appid, name) for appid, (key, name) in (changed | inserted).items())
    require(Counter(after.rows) == expected, "Expected keys, payloads or retained rows differ")
    return one_snapshot(before, after)


def seed(spark, storage):
    require(re.fullmatch(r"s3a://native-[0-9a-f]{16}", storage), "Expected a disposable warehouse")
    for namespace in ("silver", "gold"):
        spark.sql(f"CREATE NAMESPACE hive.{namespace} LOCATION '{storage}/{namespace}'").collect()

    spark.sql(f"""
        CREATE TABLE {SILVER} (
            steam_appid BIGINT, name_cleaned STRING, release_date DATE, release_year INT,
            game_age_years INT, required_age INT, is_free BOOLEAN, ts_ingested TIMESTAMP, is_current BOOLEAN
        ) USING iceberg TBLPROPERTIES ('format-version' = '2')
    """).collect()
    # The production initial path uses createOrReplace, which provides a nullable target schema.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DIM} USING iceberg TBLPROPERTIES ('format-version' = '2')
        AS SELECT game_key, CAST(appid AS BIGINT) AS steam_appid, name,
            DATE '2020-06-01' AS release_date, 2020 AS release_year, 6 AS game_age_year,
            0 AS required_age, false AS is_free
        FROM VALUES (2, 101, 'Original'), (4, 202, 'Lower'), (7, 202, 'Winner'),
            (7, 202, 'Winner'), (8, 404, 'Old duplicate'), (9, 404, 'Unchanged'),
            (20, 909, 'Target only') AS seed(game_key, appid, name)
    """).collect()
    for table in ("c03_bridge_refs", "c03_fact_refs"):
        spark.sql(f"""
            CREATE TABLE hive.gold.{table} USING iceberg AS
            SELECT game_key, CAST(appid AS BIGINT) AS appid
            FROM VALUES (2, 101), (7, 202), (9, 404), (20, 909) AS refs(game_key, appid)
        """).collect()


def freeze_input(spark, batch):
    rows = [
        (101, f"Changed {batch}", 2, True),
        (101, "Old current input", 1, True),
        (101, "Historical input", 3, False),
        (202, "Collapsed", 2, True),
        (303, "New game", 2, True),
        (404, "Unchanged", 2, True),
    ]
    if batch >= 2:
        rows.append((505, "New on retry", 2, True))
    if batch >= 3:
        rows.append((606, "New before acknowledgement", 2, True))
    # All names and timestamps above are controlled fixture literals.
    values = ", ".join(f"({appid}, '{name}', TIMESTAMP '2026-01-0{day} 00:00:00', {str(current).lower()})"
                       for appid, name, day, current in rows)
    spark.sql(f"""
        INSERT OVERWRITE TABLE {SILVER}
        SELECT CAST(appid AS BIGINT), name, DATE '2020-06-01', 2020, 6, 0, false, ingest, active
        FROM VALUES {values} AS fixture(appid, name, ingest, active)
    """).collect()


def refresh(run, spark, expected_stage, *, fault=None, before=None):
    writer = spark.newSession()
    original_sql = writer.sql
    scratch = []
    commits = []
    merge_attempts = []

    def temporary_directory(*, prefix):
        require(prefix == STAGE_NAME, "Unexpected scratch directory prefix")
        directory = tempfile.TemporaryDirectory(prefix=prefix)
        scratch.append(Path(directory.name).resolve())
        return directory

    def sql(statement, *args, **kwargs):
        if not " ".join(statement.split()).startswith(MERGE_PREFIX):
            return original_sql(statement, *args, **kwargs)
        require(len(scratch) == 1 and not merge_attempts, "Expected one staged MERGE")
        merge_attempts.append(True)
        match = re.search(r"\bFROM parquet\.\x60(file:///[^ \x60]+)\x60", statement)
        require(match is not None, "Expected a direct Parquet source")
        path = scratch[0] / "rows"
        require(match[1] == path.as_uri() and any(path.glob("*.parquet")), "Unexpected scratch source")
        # A Spark action reads the real staged rows from executor tasks.
        staged = writer.read.parquet(match[1]).select(
            "steam_appid", "game_key", "__dim_game_changed"
        ).collect()
        require(sorted(tuple(row) for row in staged) == sorted(expected_stage), "Staged actions differ")
        if fault == "before":
            raise ApplicationFault("before native MERGE")
        result = original_sql(statement, *args, **kwargs)
        result.collect()
        commits.append(True)
        if fault == "lost-ack":
            one_snapshot(before, state(spark.newSession()))
            raise ApplicationFault("after confirmed native MERGE")
        return result

    failure = None
    try:
        # Keep the parent SparkContext alive for independent readers. Production still calls stop().
        with patch.object(run.gold, "get_spark_session", return_value=writer), \
                patch.object(run.gold, "tempfile", SimpleNamespace(TemporaryDirectory=temporary_directory)), \
                patch.object(writer, "stop") as stop, patch.object(writer, "sql", side_effect=sql):
            try:
                run.gold.create_dim_game()
            except ApplicationFault as error:
                failure = error
            stop.assert_called_once_with()
    finally:
        require(all(not path.exists() for path in scratch), "Scratch directory was not cleaned")

    require(len(scratch) == 1, "Expected one scratch directory")
    require(len(merge_attempts) == int(bool(expected_stage)), "Unexpected MERGE attempt count")
    require((failure is not None) == (fault is not None), "Expected only the selected application fault")
    require(len(commits) == int(bool(expected_stage) and fault != "before"), "Unexpected commit call count")


def consumer_counts(spark):
    counts = {}
    for table in ("c03_bridge_refs", "c03_fact_refs"):
        refs = spark.table(f"hive.gold.{table}")
        dimension = spark.table(DIM).selectExpr("game_key", "steam_appid AS appid")
        join = refs.join(dimension, ["game_key", "appid"], "inner")
        counts[table] = join.count()
        require(counts[table] == refs.count() == 4, "A retained key lost or multiplied a reference")
        require(refs.join(dimension, ["game_key", "appid"], "left_anti").count() == 0,
                "Found an orphaned reference to a retained key")
    return counts


def evidence(case, after, **checks):
    payload = json.dumps({"snapshots": after.snapshots,
                          "mapping": [(row[1], row[0]) for row in after.rows], **checks},
                         separators=(",", ":"))
    require(len(payload.encode()) <= 6000, "Fixture evidence exceeded its budget")
    print(f"{case} PASS {payload}", flush=True)


def gold_c03(run):
    case = "C03-01"
    try:
        with run.session() as spark:
            spark.sparkContext.setLogLevel("ERROR")
            require(spark.version == "3.3.2" and spark.sparkContext.master == "local[2]",
                    "Unexpected Spark fixture topology")
            print(f"{case} START", flush=True)
            seed(spark, run.storage)
            before = state(spark.newSession())
            require(len(before.rows) == 7 and len(before.snapshots) == 1, "Unexpected seeded dimension")
            freeze_input(spark, 1)
            refresh(run, spark, [(101, 2, True), (202, 7, True), (303, 21, False)])
            first = state(spark.newSession())
            assert_replacement(before, first, {101: (2, "Changed 1"), 202: (7, "Collapsed")},
                               {303: (21, "New game")})
            evidence(case, first, changed=2, inserted=1)

            case = "C03-02"
            print(f"{case} START", flush=True)
            refresh(run, spark, [])
            require(state(spark.newSession()) == first, "No-op changed the target or snapshots")
            evidence(case, first, replay_unchanged=True)

            case = "C03-03"
            print(f"{case} START", flush=True)
            freeze_input(spark, 2)
            second_stage = [(101, 2, True), (505, 22, False)]
            refresh(run, spark, second_stage, fault="before")
            require(state(spark.newSession()) == first, "Before-MERGE fault changed the target")
            refresh(run, spark, second_stage)
            second = state(spark.newSession())
            assert_replacement(first, second, {101: (2, "Changed 2")}, {505: (22, "New on retry")})

            freeze_input(spark, 3)
            refresh(run, spark, [(101, 2, True), (606, 23, False)], fault="lost-ack", before=second)
            committed = state(spark.newSession())
            assert_replacement(second, committed, {101: (2, "Changed 3")},
                               {606: (23, "New before acknowledgement")})
            refresh(run, spark, [])
            require(state(spark.newSession()) == committed, "Retry after lost acknowledgement wrote again")
            evidence(case, committed, before_merge_unchanged=True, lost_ack_replay_unchanged=True)

            case = "C03-04"
            print(f"{case} START", flush=True)
            reader = spark.newSession()
            require(state(reader) == committed, "Independent reader saw different mappings")
            evidence(case, committed, references=consumer_counts(reader), executor_parquet_read=True,
                     scratch_cleaned=True)
    except Exception as error:
        print(f"{case} DIAGNOSTIC {type(error).__name__}", file=sys.stderr, flush=True)
        raise


SCENARIOS = {"gold-c03": gold_c03}
