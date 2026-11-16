"""Exercise the actual review and popularity fact writers in disposable Iceberg."""

from collections import Counter
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
from typing import NamedTuple
from unittest.mock import patch


SCENARIO = "gold-c03-phase-1c-a"
STAMP1 = "2026-01-01 10:00:00"
STAMP2 = "2026-01-02 11:00:00"
OUTSIDE = "2032-01-01 12:00:00"
LEGACY_NULL = "LEGACY_BEHAVIOR_PRESERVED"


class Fact(NamedTuple):
    entity: str
    silver: str
    stamp: str

    @property
    def name(self):
        return f"fact_{self.entity}_snapshot"

    @property
    def table(self):
        return f"hive.gold.{self.name}"

    @property
    def dimension(self):
        return f"hive.gold.dim_{self.entity}_info"

    @property
    def key(self):
        return f"{self.entity}_key"

    @property
    def source(self):
        return f"hive.silver.{self.silver}"

    @property
    def schema(self):
        return (("game_key", "int"), ("date_key", "int"), (self.key, "int"), ("snapshot_ts", "timestamp"))


FACTS = (Fact("review", "reviews", "ts_ingested"), Fact("popularity", "popularity", "ts"))


class State(NamedTuple):
    rows: Counter
    snapshots: dict


class InjectedFault(RuntimeError):
    """An application-boundary staging, pre-commit or lost-acknowledgement fault."""


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def snapshot_child(before, after):
    prior, current = before.snapshots, after.snapshots
    tips = prior.keys() - {entry[0] for entry in prior.values()}
    added = current.keys() - prior.keys()
    require(len(tips) == len(added) == 1 and len(current) == len(prior) + 1, "one child snapshot")
    require(all(current.get(key) == entry for key, entry in prior.items()), "prior snapshot lineage unchanged")
    require(current[next(iter(added))][0] in tips, "new snapshot points to the prior tip")
    return len(added)


def replacement(before, payload):
    """Expected current-state row bag, with ordinary SQL NULL equality."""
    keys = {row[0] for row in payload if row[0] is not None}
    return Counter({row: count for row, count in before.items() if row[0] not in keys}) + Counter(payload)


def schema_text(fields):
    return ", ".join(f"{name} {kind}" for name, kind in fields)


def utc(stamp):
    return datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc)


def collect_rows(frame, fact):
    require(tuple(frame.dtypes) == fact.schema and all(field.nullable for field in frame.schema.fields),
            "exact nullable fact schema")
    # The reviewed runtime uses UTC. String comparison avoids Python epoch conversion.
    projected = frame.selectExpr("game_key", "date_key", fact.key,
                                 "date_format(snapshot_ts, 'yyyy-MM-dd HH:mm:ss') AS snapshot_ts")
    return Counter(tuple(row) for row in projected.collect())


def observe(spark, fact):
    reader = spark.newSession()
    rows = collect_rows(reader.table(fact.table), fact)
    history = reader.sql(f"SELECT snapshot_id, parent_id, operation FROM {fact.table}.snapshots")
    return State(rows, {row[0]: tuple(row[1:]) for row in history.collect()})


def initial_table(spark, table, schema, rows):
    spark.createDataFrame(rows, schema).writeTo(table).using("iceberg") \
        .tableProperty("format-version", "2").create()


def seed(run, spark):
    require(re.fullmatch(r"s3a://native-[0-9a-f]{16}", run.storage), "scoped warehouse")
    for space in ("silver", "gold"):
        spark.sql(f"CREATE NAMESPACE hive.{space} LOCATION '{run.storage}/{space}'").collect()
    initial_table(spark, "hive.gold.dim_game", "steam_appid bigint, game_key int",
                  [(101, 1), (202, 2), (303, 3), (404, 4), (505, 5), (909, 9)])
    initial_table(spark, "hive.gold.dim_date", "date_id int, date_key int", [(20260101, 11), (20260102, 12)])
    for fact in FACTS:
        initial_table(spark, fact.dimension, f"steam_appid bigint, {fact.key} int",
                      [(101, 21), (202, 22), (202, 23), (303, 24), (404, 25), (909, 29)])
        initial_table(spark, fact.source, f"steam_appid bigint, {fact.stamp} timestamp, is_current boolean", [])


def freeze(spark, fact, rows):
    from pyspark.sql.functions import lit

    data = [(appid, utc(stamp), current) for appid, stamp, current in rows]
    spark.createDataFrame(data, f"steam_appid bigint, {fact.stamp} timestamp, is_current boolean") \
        .writeTo(fact.source).overwrite(lit(True))


def restore(spark, fact, rows):
    from pyspark.sql.functions import lit

    data = [(*row[:3], utc(row[3])) for row in rows.elements()]
    spark.createDataFrame(data, schema_text(fact.schema)).writeTo(fact.table).overwrite(lit(True))
    restored = observe(spark, fact)
    require(restored.rows == rows, "identical fixture target seed")
    return restored


def check_stage(writer, fact, stage_path, payload):
    require(any(stage_path.glob("*.parquet")), "fixture stage has physical Parquet")
    require(collect_rows(writer.read.parquet(stage_path.as_uri()), fact) == Counter(payload),
            "staged executor payload differs from expected multiset")


def run_attempt(run, spark, fact, payload, *, before=None, fault=None):
    from pyspark.sql.readwriter import DataFrameWriter

    writer = spark.newSession()
    original_sql = writer.sql
    paths = []
    commands = []
    injected = InjectedFault(fault) if fault else None
    observed_commit = None

    @contextmanager
    def scoped_directory(*, prefix):
        require(prefix == f"gold-{fact.name}-" and not paths, "unexpected scratch identity")
        with tempfile.TemporaryDirectory(prefix=prefix) as directory:
            path = Path(directory).resolve()
            paths.append(path)
            yield directory
            if not commands:
                require(not payload and fault is None, "nonempty stage skipped MERGE")
                check_stage(writer, fact, path / "rows", payload)

    def stage_fault(_writer, uri, *args, **kwargs):
        require(len(paths) == 1 and uri == (paths[0] / "rows").as_uri(), "staging fault reached wrong scratch")
        (paths[0] / "rows").mkdir()
        (paths[0] / "rows" / "synthetic-partial").touch()
        raise injected

    def sql(statement, *args, **kwargs):
        nonlocal observed_commit
        flattened = " ".join(statement.split())
        require(flattened.startswith(f"MERGE INTO {fact.table} AS t ") and not commands, "expected one target MERGE")
        commands.append(True)
        match = re.search(r"\bFROM parquet\.\x60(file:///[^ \x60]+)\x60", flattened)
        require(len(paths) == 1 and match and match[1] == (paths[0] / "rows").as_uri(), "direct scoped Parquet source")
        check_stage(writer, fact, paths[0] / "rows", payload)
        if fault == "before":
            require(observe(spark, fact) == before, "reader saw changes before MERGE")
            raise injected
        result = original_sql(statement, *args, **kwargs)
        result.collect()
        require((paths[0] / "rows").is_dir(), "scratch kept through MERGE")
        if fault == "lost-ack":
            observed_commit = observe(spark, fact)
            snapshot_child(before, observed_commit)
            require(observed_commit.rows == replacement(before.rows, payload), "lost-ack readback differs")
            raise injected
        return result

    staging = patch.object(DataFrameWriter, "parquet", stage_fault) if fault == "staging" else nullcontext()
    try:
        with patch.object(run.gold, "get_spark_session", return_value=writer), \
                patch.object(run.gold, "tempfile", SimpleNamespace(TemporaryDirectory=scoped_directory)), \
                patch.object(writer, "stop") as stop, patch.object(writer, "sql", side_effect=sql), staging:
            try:
                getattr(run.gold, f"create_{fact.name}")()
            except InjectedFault as error:
                require(error is injected, "unexpected injected exception")
            else:
                require(not injected, "writer swallowed injected fault")
            stop.assert_called_once_with()
    finally:
        require(all(not path.exists() for path in paths), "scratch cleanup")
    require(len(paths) == 1 and len(commands) == int(bool(payload) and fault != "staging"),
            "unexpected staging or MERGE call count")
    return observed_commit


def initial_load(run, spark, fact):
    writer = spark.newSession()
    require(not run.gold.table_exists(writer, fact.table), "expected absent fact target")
    with patch.object(run.gold, "get_spark_session", return_value=writer), patch.object(writer, "stop") as stop:
        getattr(run.gold, f"create_{fact.name}")()
        stop.assert_called_once_with()
    state = observe(spark, fact)
    require(len(state.snapshots) == 1 and all(parent is None for parent, _ in state.snapshots.values()),
            "initial snapshot")
    return state


def check_commit(run, spark, fact, before, payload):
    run_attempt(run, spark, fact, payload)
    after = observe(spark, fact)
    require(after.rows == replacement(before.rows, payload), "committed row multiset")
    return after, snapshot_child(before, after)


def gate_a(run, spark, fact):
    freeze(spark, fact, [(101, STAMP1, True), (505, STAMP1, True), (909, STAMP1, True), (101, STAMP2, False)])
    initial = initial_load(run, spark, fact)
    require(initial.rows == Counter([(1, 11, 21, STAMP1), (5, 11, None, STAMP1), (9, 11, 29, STAMP1)]),
            "initial grain, timestamps and FKs")
    freeze(spark, fact, [(101, STAMP2, True), (101, STAMP2, True), (202, STAMP2, True),
                        (303, OUTSIDE, True), (505, STAMP1, True), (101, STAMP1, False)])
    payload = [(1, 12, 21, STAMP2)] * 2 + [
        (2, 12, 22, STAMP2), (2, 12, 23, STAMP2), (3, None, 24, OUTSIDE), (5, 11, None, STAMP1)]
    changed, refresh_snapshots = check_commit(run, spark, fact, initial, payload)
    unchanged, unchanged_snapshots = check_commit(run, spark, fact, changed, payload)
    require(changed.rows == unchanged.rows, "unchanged nonempty replay")
    freeze(spark, fact, [])
    run_attempt(run, spark, fact, [])
    empty = observe(spark, fact)
    require(empty == unchanged, "empty refresh changed target")
    return dict(initial_rows=sum(initial.rows.values()), refreshed_rows=sum(changed.rows.values()),
                duplicate_rows=changed.rows[(1, 12, 21, STAMP2)] - 1,
                fanout_rows=sum(count for row, count in changed.rows.items() if row[0] == 2),
                nullable_fk_rows=sum(count for row, count in changed.rows.items() if None in row[1:3]),
                refresh_snapshots=refresh_snapshots, unchanged_snapshots=unchanged_snapshots,
                empty_snapshots=len(empty.snapshots) - len(unchanged.snapshots))


def gate_b(run, spark, fact):
    before = observe(spark, fact)
    freeze(spark, fact, [(101, STAMP1, True), (404, STAMP2, True)])
    payload = [(1, 11, 21, STAMP1), (4, 12, 25, STAMP2)]
    reference, _ = check_commit(run, spark, fact, before, payload)
    frozen_seed = restore(spark, fact, before.rows)
    failure_counts = {}
    for fault in ("staging", "before"):
        run_attempt(run, spark, fact, payload, before=frozen_seed, fault=fault)
        state = observe(spark, fact)
        require(state == frozen_seed, "fault altered rows or snapshots before commit")
        failure_counts[fault] = len(state.snapshots) - len(frozen_seed.snapshots)
    retried, retry_snapshots = check_commit(run, spark, fact, frozen_seed, payload)
    require(retried.rows == reference.rows, "frozen retry differs from uninterrupted reference")
    # Restore the same target seed so the committed lost-ack attempt also changes the fact.
    lost_seed = restore(spark, fact, before.rows)
    seen = run_attempt(run, spark, fact, payload, before=lost_seed, fault="lost-ack")
    committed = observe(spark, fact)
    require(committed == seen and committed.rows == reference.rows, "lost acknowledgement was not committed")
    lost_ack_snapshots = snapshot_child(lost_seed, committed)
    replay, replay_snapshots = check_commit(run, spark, fact, committed, payload)
    require(replay.rows == reference.rows, "lost-ack retry differs from reference")
    return dict(reference_rows=sum(reference.rows.values()), retry_rows=sum(retried.rows.values()),
                lost_ack_rows=sum(committed.rows.values()), staging_failure_snapshots=failure_counts["staging"],
                precommit_snapshots=failure_counts["before"], retry_snapshots=retry_snapshots,
                lost_ack_snapshots=lost_ack_snapshots, replay_snapshots=replay_snapshots)


def gate_c(run, spark, fact):
    before = observe(spark, fact)
    freeze(spark, fact, [(777, STAMP1, True)])
    initial, _ = check_commit(run, spark, fact, before, [(None, 11, None, STAMP1)])
    freeze(spark, fact, [(777, STAMP2, True), (101, STAMP2, True)])
    payload = [(None, 12, None, STAMP2), (1, 12, 21, STAMP2)]
    first, refresh_snapshots = check_commit(run, spark, fact, initial, payload)
    replay, replay_snapshots = check_commit(run, spark, fact, first, payload)
    require(replay.rows[(None, 11, None, STAMP1)] == 1 and replay.rows[(None, 12, None, STAMP2)] == 2,
            "expected legacy null-key accumulation")
    null_rows = [sum(count for row, count in state.rows.items() if row[0] is None)
                 for state in (initial, first, replay)]
    return dict(initial_null_rows=null_rows[0], first_null_rows=null_rows[1], replay_null_rows=null_rows[2],
                null_payload_rows=sum(row[0] is None for row in payload), refresh_snapshots=refresh_snapshots,
                replay_snapshots=replay_snapshots, legacy_behavior=LEGACY_NULL)


def validation(run):
    case = "C03F-SETUP"
    try:
        with run.session() as spark:
            spark.sparkContext.setLogLevel("WARN")
            seed(run, spark)
            for gate, check in (("A", gate_a), ("B", gate_b), ("C", gate_c)):
                for fact in FACTS:
                    case = f"C03F-{gate}-{fact.entity.upper()}"
                    print(f"{case} START", flush=True)
                    evidence = check(run, spark, fact)
                    evidence.update(schema_verified=True, scratch_cleaned=True,
                                    independent_readback=True, parquet_verified=True)
                    print(f"{case} PASS " + json.dumps(evidence, separators=(",", ":")), flush=True)
            case = "C03F-TEARDOWN"
    except Exception as error:
        print(f"{case} DIAGNOSTIC {type(error).__name__}", flush=True)
        raise


SCENARIOS = {SCENARIO: validation}
