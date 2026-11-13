"""Validate the exact Silver C02 MERGE with bounded, disposable game fixtures."""

import io
import json
from pathlib import Path
import tempfile
import sys
from typing import NamedTuple
from unittest.mock import patch

TABLE = "hive.silver.games"
KEY = "steam_appid"
TARGET_KEY = 101
MERGE_PREFIX = "MERGE INTO `HIVE`.`SILVER`.`GAMES` AS T "
METADATA = {"effective_date", "expiry_date", "is_current", "scd_version"}


class InjectedFault(RuntimeError):
    """A deliberately injected Python application fault."""


class State(NamedTuple):
    rows: tuple
    snapshots: tuple


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def snapshot_rows(spark):
    return tuple(
        row.asDict() for row in spark.sql(
            f"SELECT snapshot_id, parent_id, operation FROM {TABLE}.snapshots ORDER BY snapshot_id"
        ).collect()
    )


def ignore_stage(_stage):
    """Keep diagnostics scoped to the first transition."""


def read_state(spark, mark=ignore_stage, prefix="reader"):
    mark(f"{prefix}-rows")
    rows = tuple(row.asDict(recursive=True)
                 for row in spark.table(TABLE).orderBy(KEY, "scd_version").collect())
    mark(f"{prefix}-snapshots")
    return State(rows, snapshot_rows(spark))


def independent_state(run, mark=ignore_stage, prefix="reader"):
    mark(f"{prefix}-open")
    with run.session() as reader:
        reader.sparkContext.setLogLevel("ERROR")
        state = read_state(reader, mark, prefix)
        mark(f"{prefix}-close")
    return state


def index_rows(rows):
    indexed = {(row[KEY], row["scd_version"]): row for row in rows}
    require(len(indexed) == len(rows), "Duplicate key/version")
    return indexed


def current_is_unique(rows):
    keys = {row[KEY] for row in rows}
    require(all(sum(row[KEY] == key and row["is_current"] is True for row in rows) == 1
                for key in keys), "Expected one current row per fixture key")


def one_snapshot(before, after):
    previous = {row["snapshot_id"]: row for row in before}
    following = {row["snapshot_id"]: row for row in after}
    parents = {row["parent_id"] for row in before}
    tip = set(previous) - parents
    added = following.keys() - previous.keys()
    require(len(tip) == 1 and len(added) == 1 and len(after) == len(before) + 1,
            "Expected one added snapshot and one previous tip")
    require(all(following.get(key) == row for key, row in previous.items()),
            "Previous snapshot metadata changed")
    snapshot = following[next(iter(added))]
    require(snapshot["parent_id"] in tip, "New snapshot is not a child of the previous tip")
    return snapshot


def assert_transition(before, after, incoming, version):
    old_key = (TARGET_KEY, version - 1)
    new_key = (TARGET_KEY, version)
    old_rows = index_rows(before.rows)
    new_rows = index_rows(after.rows)
    require(new_key not in old_rows and new_rows.keys() == old_rows.keys() | {new_key},
            "Unexpected key/version set")
    predecessor = old_rows[old_key]
    successor = new_rows[new_key]
    boundary = successor["effective_date"]
    require(predecessor["is_current"] is True and predecessor["expiry_date"] is None,
            "Expected a valid predecessor")
    require(boundary is not None and boundary >= predecessor["effective_date"]
            and successor["is_current"] is True and successor["expiry_date"] is None,
            "Unexpected successor validity")
    require(new_rows[old_key] == dict(predecessor, is_current=False, expiry_date=boundary),
            "Predecessor business fields or validity boundary changed unexpectedly")
    require({key: value for key, value in successor.items() if key not in METADATA} == incoming,
            "Successor differs from frozen input")
    require(all(new_rows[key] == row for key, row in old_rows.items() if key != old_key),
            "A different key or historical version changed")
    current_is_unique(after.rows)
    one_snapshot(before.snapshots, after.snapshots)


def fixture(run, mark):
    mark("fixture-transform")
    import pandas as pd
    from fixtures import csv_bytes

    source = csv_bytes("games", {}, {
        "steam_appid": 202, "name": "Garden Race", "genres": "Racing", "tags": "Driving",
    })
    processed, removed = run.silver.preprocess_games(pd.read_csv(io.BytesIO(source)))
    require(not removed and len(processed) == 2, "Unexpected synthetic fixture transform")
    mark("fixture-parquet-write")
    with tempfile.TemporaryDirectory(prefix="silver-c02-fixture-") as directory:
        path = Path(directory) / "games.parquet"
        processed.to_parquet(path, index=False)
        mark("fixture-spark-open")
        with run.session() as spark:
            spark.sparkContext.setLogLevel("ERROR")
            mark("fixture-namespace-create")
            spark.sql(f"CREATE NAMESPACE hive.silver LOCATION '{run.storage}/silver'").collect()
            mark("fixture-parquet-read")
            games = spark.read.parquet(path.as_uri())
            mark("fixture-iceberg-seed")
            run.silver._add_scd2_columns(games).writeTo(TABLE).using("iceberg") \
                .tableProperty("format-version", "2").create()
            mark("fixture-source-collect")
            original = games.where(f"{KEY} = {TARGET_KEY}").collect()
            require(len(original) == 1, "Expected exactly one source fixture key")
            seed = games.schema, original[0].asDict(recursive=True)
            mark("fixture-spark-close")
        mark("fixture-temp-cleanup")
    return seed


def is_fixture_merge(statement):
    return " ".join(statement.upper().split()).startswith(MERGE_PREFIX)


def intercept_merge(sql, confirmed=None):
    def call(statement, *args, **kwargs):
        if not is_fixture_merge(statement):
            return sql(statement, *args, **kwargs)
        if confirmed is not None:
            sql(statement, *args, **kwargs).collect()
            confirmed()
        raise InjectedFault("after native commit" if confirmed else "before native SQL")
    return call


def upsert(run, schema, payload, *, fault=None, before=None, mark=ignore_stage):
    mark("writer-open")
    with run.session() as writer:
        writer.sparkContext.setLogLevel("ERROR")
        mark("writer-create-input")
        incoming = writer.createDataFrame([tuple(payload[name] for name in schema.fieldNames())], schema)

        tracked = run.silver.Config.SCD2_CHANGE_COLS["games"]
        require("name_cleaned" in tracked, "Expected the changed field in the reviewed tracked set")

        def apply():
            return run.silver._upsert_scd2(writer, incoming, TABLE, KEY, tracked)

        if fault is None:
            if mark is ignore_stage:
                return apply()
            sql = writer.sql

            def traced_sql(statement, *args, **kwargs):
                if not is_fixture_merge(statement):
                    return sql(statement, *args, **kwargs)
                mark("merge-sql")
                result = sql(statement, *args, **kwargs)
                mark("upsert-cleanup")
                return result

            mark("upsert-plan")
            with patch.object(writer, "sql", side_effect=traced_sql):
                count = apply()
            mark("writer-close")
            return count
        require(fault in {"before", "lost-ack"} and before is not None, "Unexpected fault configuration")

        def confirmed_commit():
            # A new catalog session confirms the snapshot before Python raises the lost acknowledgement.
            one_snapshot(before.snapshots, snapshot_rows(writer.newSession()))

        intercept = intercept_merge(writer.sql, confirmed_commit if fault == "lost-ack" else None)
        with patch.object(writer, "sql", side_effect=intercept):
            try:
                apply()
            except InjectedFault:
                return
        raise AssertionError("MERGE fault was not reached")


def evidence(case, state, **details):
    rows = [{key: row[key] for key in (KEY, "name_cleaned", "scd_version", "is_current",
                                     "effective_date", "expiry_date")} for row in state.rows]
    payload = json.dumps({"rows": rows, "snapshots": state.snapshots, **details},
                         default=str, separators=(",", ":"))
    require(len(payload.encode()) <= 6000, "Fixture evidence exceeded its budget")
    print(f"{case} PASS {payload}", flush=True)


def atomic_transition(run):
    stage = "fixture-transform"

    def mark(current):
        nonlocal stage
        stage = current

    try:
        schema, base = fixture(run, mark)
        seed = independent_state(run, mark, "seed-reader")
        mark("seed-assertions")
        current_is_unique(seed.rows)
        require(len(seed.rows) == 2 and len(seed.snapshots) == 1
                and all(row["scd_version"] == 1 for row in seed.rows),
                "Expected only initial version 1 fixtures")
        second = dict(base, name_cleaned="Orbit Quest C02 version 2")
        count = upsert(run, schema, second, mark=mark)
        mark("transition-count")
        require(count == 1, "Expected one successor from the native MERGE")
        atomic = independent_state(run, mark, "committed-reader")
        mark("transition-assertions")
        assert_transition(seed, atomic, second, 2)
        mark("transition-evidence")
        evidence("C02-01", atomic, before_snapshot=seed.snapshots[0]["snapshot_id"])
        return schema, base, seed, second, atomic
    except Exception as error:
        # The host forwards only its allowlisted stage and type, never the exception message.
        print(f"C02-01 DIAGNOSTIC {stage} {type(error).__name__}", file=sys.stderr, flush=True)
        raise


def silver_c02(run):
    print("Starting C02-01 atomic transition", flush=True)
    schema, base, seed, second, atomic = atomic_transition(run)

    print("Starting C02-02 unchanged replay", flush=True)
    require(upsert(run, schema, second) == 0, "Unchanged input planned a successor")
    replay = independent_state(run)
    require(replay == atomic, "Unchanged input changed rows or snapshots")
    evidence("C02-02", replay, inserted=0)

    print("Starting C02-03 before-MERGE fault and frozen retry", flush=True)
    third = dict(base, name_cleaned="Orbit Quest C02 version 3")
    upsert(run, schema, third, fault="before", before=atomic)
    interrupted = independent_state(run)
    require(interrupted == atomic, "Before-MERGE fault changed rows or snapshots")
    require(upsert(run, schema, third) == 1, "Frozen retry failed to plan one successor")
    recovered = independent_state(run)
    assert_transition(interrupted, recovered, third, 3)

    print("Starting C02-03 lost acknowledgement and frozen retry", flush=True)
    fourth = dict(base, name_cleaned="Orbit Quest C02 version 4")
    upsert(run, schema, fourth, fault="lost-ack", before=recovered)
    committed = independent_state(run)
    assert_transition(recovered, committed, fourth, 4)
    require(upsert(run, schema, fourth) == 0, "Retry after visible commit planned a duplicate")
    require(independent_state(run) == committed, "Retry after visible commit changed rows or snapshots")
    evidence("C02-03", committed, faults="Python application boundary",
             before_merge_tip=one_snapshot(seed.snapshots, interrupted.snapshots)["snapshot_id"],
             retry_tip=one_snapshot(interrupted.snapshots, recovered.snapshots)["snapshot_id"],
             lost_ack_tip=one_snapshot(recovered.snapshots, committed.snapshots)["snapshot_id"])

    print("Starting C02-04 independent Silver reader and Gold input contract", flush=True)
    with run.session() as reader:
        reader.sparkContext.setLogLevel("ERROR")
        final = read_state(reader)
        require(final == committed, "Independent reader observed different committed history")
        current_is_unique(final.rows)
        # Exercise the current filter and the columns read by Gold, without running Gold writers.
        gold = reader.table(TABLE).where("is_current = true").selectExpr(
            KEY, "name_cleaned AS name", "release_date", "release_year",
            "game_age_years AS game_age_year", "required_age", "is_free", "ts_ingested",
        ).orderBy(KEY).collect()
        expected = [row for row in final.rows if row["is_current"]]
        require(len(gold) == len(expected) and all(
            tuple(row) == (source[KEY], source["name_cleaned"], source["release_date"],
                           source["release_year"], source["game_age_years"], source["required_age"],
                           source["is_free"], source["ts_ingested"])
            for row, source in zip(gold, expected)
        ), "Gold projection differs from the committed current rows")
    evidence("C02-04", final, gold_rows=len(gold))


SCENARIOS = {"silver-c02": silver_c02}
