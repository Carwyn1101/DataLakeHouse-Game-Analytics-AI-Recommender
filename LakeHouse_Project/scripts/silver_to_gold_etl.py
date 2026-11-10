from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, explode, split, row_number, current_timestamp,
    date_format, to_timestamp, when, lit, concat, lpad, regexp_replace,
    weekofyear, quarter, dayofweek, year, month, dayofmonth, coalesce
)
from pyspark.sql.window import Window

# ================= CONFIG =================
class Config:
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "hongphuc"
    MINIO_SECRET_KEY = "hongphuc"
    BUCKET = "lakehouse"

    SILVER_PATH = f"s3a://{BUCKET}/silver"
    GOLD_PATH = f"s3a://{BUCKET}/gold"
    WAREHOUSE_PATH = f"s3a://{BUCKET}/warehouse"

    HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
    JARS = "/opt/airflow/jars/hadoop-aws-3.3.2.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/jars/iceberg-spark-runtime-3.3_2.12-1.3.1.jar"

# ================= SPARK SESSION =================
def get_spark_session(app_name="gold-etl"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", Config.JARS) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hive.type", "hive") \
        .config("spark.sql.catalog.hive.uri", Config.HIVE_METASTORE_URI) \
        .config("spark.sql.warehouse.dir", Config.WAREHOUSE_PATH) \
        .config("spark.hadoop.hive.metastore.warehouse.dir", Config.WAREHOUSE_PATH) \
        .config("spark.hadoop.fs.defaultFS", f"s3a://{Config.BUCKET}") \
        .config("spark.hadoop.fs.s3a.endpoint", Config.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", Config.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", Config.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

# ================= LOGGER =================
logger = logging.getLogger("gold_etl")
logger.setLevel(logging.INFO)

# ================= HELPER FUNCTION =================

# ================= INCREMENTAL HELPERS =================
def table_exists(spark, table_name):
    """Check the fully qualified table in the configured Iceberg V2 catalog."""
    catalog_name, namespace_name, name = table_name.split(".")
    catalog = spark._jsparkSession.sessionState().catalogManager().catalog(catalog_name)
    namespace = spark.sparkContext._gateway.new_array(spark._jvm.java.lang.String, 1)
    namespace[0] = namespace_name
    identifier = spark._jvm.org.apache.spark.sql.connector.catalog.Identifier.of(namespace, name)
    return catalog.tableExists(identifier)


def get_max_key(spark, table_name, key_col):
    """Return the maximum key, defaulting to zero only for an empty table."""
    maximum, rows = spark.table(table_name).agg(F.max(key_col), F.count("*")).collect()[0]
    if rows == 0:
        return 0
    if maximum is None:
        raise ValueError(f"No non-null surrogate keys in {table_name}.{key_col}.")
    return int(maximum)


def get_changed_game_keys(spark, dim_game):
    """
    Return a DataFrame(game_key, steam_appid) for games that are new OR
    were updated in silver since the last gold fact-snapshot run.
    Falls back to ALL games when fact_game_snapshot does not exist yet.
    """
    if table_exists(spark, "hive.gold.fact_game_snapshot"):
        last_snaps = spark.table("hive.gold.fact_game_snapshot") \
            .select("game_key", "snapshot_ts")
        # SCD2: only read the active (current) version of each game
        silver_ts = spark.table("hive.silver.games") \
            .filter(col("is_current") == True) \
            .select("steam_appid", "ts_ingested")
        return (
            dim_game
            .join(silver_ts, "steam_appid")
            .join(last_snaps, "game_key", "left")
            .filter(col("snapshot_ts").isNull() | (col("ts_ingested") > col("snapshot_ts")))
            .select("game_key", "steam_appid")
        )
    # No fact table yet → all games need processing
    return dim_game.join(
        spark.table("hive.silver.games").filter(col("is_current") == True).select("steam_appid"),
        "steam_appid"
    ).select("game_key", "steam_appid")


def explode_and_clean(df, column, out_col):
    """Split column by comma, trim, remove empty"""
    return df.withColumn(out_col, explode(split(col(column), ","))) \
             .withColumn(out_col, trim(col(out_col))) \
             .filter(col(out_col) != "")


def get_changed_keys(new_df, existing_df, key_col, tracked_cols):
    """Return distinct business keys whose tracked columns changed (null-safe compare)."""
    compare_cols = [c for c in tracked_cols if c in new_df.columns and c in existing_df.columns and c != key_col]
    if not compare_cols:
        return new_df.limit(0).select(key_col)

    joined = new_df.alias("src").join(existing_df.alias("tgt"), key_col, "inner")
    change_condition = None
    for c in compare_cols:
        col_changed = ~col(f"src.{c}").eqNullSafe(col(f"tgt.{c}"))
        change_condition = col_changed if change_condition is None else (change_condition | col_changed)

    return joined.filter(change_condition).select(col(f"src.{key_col}").alias(key_col)).distinct()

# ================= 1. CREATE GOLD DATABASE =================
def create_gold_database(**context):
    logger.info("BƯỚC 1: TẠO GOLD DATABASE")
    spark = get_spark_session("create_gold_db")
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS hive.gold LOCATION '{Config.GOLD_PATH}'")
        logger.info("Gold database tạo xong")
    finally:
        spark.stop()

# ================= 2. DIM_DATE (với date_key) =================
def create_dim_date(**context):
    logger.info("BƯỚC 2: TẠO DIM_DATE (với date_key)")
    spark = get_spark_session("dim_date")
    try:
        # dim_date is a static calendar (2000–2030) — skip rebuild if already exists
        if table_exists(spark, "hive.gold.dim_date"):
            logger.info("dim_date đã tồn tại, bỏ qua (static calendar không thay đổi).")
            return

        start = "2000-01-01"
        end = "2030-12-31"
        num_days = (datetime(2030, 12, 31) - datetime(2000, 1, 1)).days + 1

        df_days = (
            spark.range(0, num_days)
            .withColumn("date", F.expr(f"date_add(to_date('{start}'), CAST(id AS int))"))
        )

        dim_date = (
            df_days
            .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))
            .withColumn("date", col("date").cast("date"))
            .withColumn("day", date_format(col("date"), "dd"))
            .withColumn("week", weekofyear(col("date")))
            .withColumn("month", date_format(col("date"), "MM"))
            .withColumn("quarter", quarter(col("date")))
            .withColumn("year", date_format(col("date"), "yyyy").cast("int"))
            .withColumn("is_weekend", dayofweek(col("date")).isin(1, 7))
            .withColumn("is_holiday", lit(False))
            .withColumn("created_at", current_timestamp())
        )
        
        # Thêm date_key (surrogate PK)
        window_order = Window.orderBy("date_id")
        dim_date = dim_date.withColumn("date_key", row_number().over(window_order)) \
            .select(
                "date_key",
                "date_id",
                "date",
                "day",
                "week",
                "month",
                "quarter",
                "year",
                "is_weekend",
                "is_holiday",
                "created_at"
            )

        dim_date.writeTo("hive.gold.dim_date") \
            .using("iceberg") \
            .tableProperty("location", f"{Config.GOLD_PATH}/dim_date") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("format-version", "2") \
            .createOrReplace()

        logger.info(f"dim_date: {dim_date.count():,} rows (với date_key)")
    finally:
        spark.stop()

# ================= 3. DIM_GAME (với game_key) =================
def create_dim_game(**context):
    logger.info("BƯỚC 3: TẠO DIM_GAME (với game_key)")
    spark = get_spark_session("dim_game")
    try:
        # SCD2: read only the current (active) version of each game
        df = spark.table("hive.silver.games").filter(col("is_current") == True)

        # Lấy record mới nhất per steam_appid
        window_latest = Window.partitionBy("steam_appid").orderBy(col("ts_ingested").desc_nulls_last())
        df_latest = df.withColumn("rn", row_number().over(window_latest)).filter(col("rn") == 1).drop("rn")

        # Columns from silver (no surrogate key yet)
        _GAME_COLS = ["steam_appid", "name", "release_date", "release_year",
                      "game_age_year", "required_age", "is_free"]

        new_data = df_latest.select(
            "steam_appid",
            F.col("name_cleaned").alias("name"),
            "release_date",
            "release_year",
            col("game_age_years").alias("game_age_year"),
            "required_age",
            "is_free",
        )

        if not table_exists(spark, "hive.gold.dim_game"):
            # ── Initial load ──────────────────────────────────────────────────
            window_order = Window.orderBy("steam_appid")
            dim_game = new_data.withColumn("game_key", row_number().over(window_order)) \
                .select("game_key", *_GAME_COLS)
            dim_game.writeTo("hive.gold.dim_game") \
                .using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_game") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2") \
                .createOrReplace()
            logger.info(f"dim_game: {dim_game.count():,} rows (initial load)")
        else:
            # ── Incremental: DELETE + APPEND (stable with Iceberg) ───────────
            existing = spark.table("hive.gold.dim_game")
            max_key  = get_max_key(spark, "hive.gold.dim_game", "game_key")

            # Defensive dedup mapping in case target has duplicate steam_appid from older runs.
            existing_latest = (
                existing
                .withColumn("rn", row_number().over(Window.partitionBy("steam_appid").orderBy(col("game_key").desc())))
                .filter(col("rn") == 1)
                .drop("rn")
            )
            existing_key_map = existing_latest.select("game_key", "steam_appid")
            tracked_cols = [c for c in _GAME_COLS if c != "steam_appid"]
            changed_keys = get_changed_keys(
                new_data,
                existing_latest.select("steam_appid", *tracked_cols),
                "steam_appid",
                tracked_cols,
            )

            # New games not yet in dim_game → assign new surrogate keys
            new_games = new_data \
                .join(existing_key_map.select("steam_appid"), "steam_appid", "left_anti") \
                .withColumn(
                    "game_key",
                    (lit(max_key) + row_number().over(Window.orderBy("steam_appid"))).cast("int")
                ).select("game_key", *_GAME_COLS)

            # Existing games → keep current game_key, update other fields
            changed_games = new_data \
                .join(changed_keys, "steam_appid", "inner") \
                .join(existing_key_map, "steam_appid") \
                .select("game_key", *_GAME_COLS)

            all_updates = changed_games.unionByName(new_games)
            new_games_count = new_games.count()
            changed_games_count = changed_games.count()

            # Delete old rows for impacted steam_appid, then append refreshed rows.
            if (new_games_count + changed_games_count) > 0:
                all_updates.select("steam_appid").distinct().createOrReplaceTempView("__dim_game_refresh_keys__")
                spark.sql("""
                    DELETE FROM hive.gold.dim_game
                    WHERE steam_appid IN (SELECT steam_appid FROM __dim_game_refresh_keys__)
                """)
                all_updates.writeTo("hive.gold.dim_game").append()

                logger.info(
                    f"dim_game: UPSERTED — {new_games_count:,} new, {changed_games_count:,} updated"
                )
            else:
                logger.info("dim_game: no changes detected, skipped")
    finally:
        spark.stop()

# ================= 4. DIMS & BRIDGES (với surrogate keys) =================
def create_dims_and_bridges(**context):
    logger.info("BƯỚC 4: TẠO DIMs & BRIDGES (với surrogate keys)")
    spark = get_spark_session("dims_bridges")
    try:
        # SCD2: read only current versions; join to get bridge columns
        df_games = spark.table("hive.silver.games").filter(col("is_current") == True).select(
            "steam_appid","developers_cleaned","genres_cleaned",
            "publishers_cleaned","categories_cleaned", "tags_cleaned"
        )
        
        # Đọc dim_game để lấy game_key
        dim_game = spark.table("hive.gold.dim_game").select("game_key", "steam_appid")

        # ── Identify games that need bridge updates ──────────────────────────
        # New games (not in any bridge yet) OR games updated since last gold run
        games_to_update = get_changed_game_keys(spark, dim_game).cache()
        n_changed = games_to_update.count()
        logger.info(f"   {n_changed:,} games cần cập nhật bridge (mới hoặc thay đổi)")

        # ── Helper: incremental lookup dim ───────────────────────────────────
        # ── Helper: incremental bridge ───────────────────────────────────────
        def _upsert_bridge(bridge_table, path, col_cleaned, val_col, dim_table,
                           sk_col, bridge_sk_col):
            """
            Incremental bridge update:
            - First run  → createOrReplace from all games.
            - Later runs → delete rows for changed games, then append new rows.
            """
            dim_tbl = spark.table(dim_table)

            if not table_exists(spark, bridge_table):
                br = explode_and_clean(df_games, col_cleaned, val_col) \
                    .join(dim_tbl, val_col, "left") \
                    .join(dim_game, "steam_appid", "left") \
                    .select("steam_appid", "game_key", sk_col).distinct()
                br = br.withColumn(
                    bridge_sk_col,
                    row_number().over(Window.orderBy("steam_appid", sk_col))
                ).select(bridge_sk_col, "game_key", sk_col)
                br.writeTo(bridge_table).using("iceberg") \
                    .tableProperty("location", path) \
                    .tableProperty("write.format.default", "parquet") \
                    .tableProperty("format-version", "2") \
                    .createOrReplace()
                logger.info(f"{bridge_table.split('.')[-1]}: {br.count():,} rows (initial)")
            elif n_changed > 0:
                games_to_update.createOrReplaceTempView("__bridge_refresh_keys__")
                spark.sql(
                    f"DELETE FROM {bridge_table} "
                    f"WHERE game_key IN (SELECT game_key FROM __bridge_refresh_keys__)"
                )
                df_sub = df_games.join(games_to_update.select("steam_appid"), "steam_appid")
                new_br = explode_and_clean(df_sub, col_cleaned, val_col) \
                    .join(dim_tbl, val_col, "left") \
                    .join(dim_game, "steam_appid", "left") \
                    .select("steam_appid", "game_key", sk_col).distinct()
                max_br_key = get_max_key(spark, bridge_table, bridge_sk_col)
                new_br = new_br.withColumn(
                    bridge_sk_col,
                    (lit(max_br_key) + row_number().over(
                        Window.orderBy("steam_appid", sk_col))).cast("int")
                ).select(bridge_sk_col, "game_key", sk_col)
                new_br.writeTo(bridge_table).append()
                logger.info(
                    f"{bridge_table.split('.')[-1]}: "
                    f"updated for {n_changed:,} games ({new_br.count():,} new rows)"
                )
            else:
                logger.info(f"{bridge_table.split('.')[-1]}: no changes, skipped")

        # ========== Developers ==========
        dev_all = explode_and_clean(
            df_games.where(col("developers_cleaned").isNotNull()),
            "developers_cleaned", "developer_name"
        ).select("developer_name").distinct()

        if not table_exists(spark, "hive.gold.dim_developer"):
            dim_dev = dev_all.withColumn("developer_sk", row_number().over(Window.orderBy("developer_name"))) \
                             .select("developer_sk", "developer_name")
            dim_dev.writeTo("hive.gold.dim_developer").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_developer") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"dim_developer: {dim_dev.count():,} rows (initial)")
        else:
            new_devs = dev_all.join(
                spark.table("hive.gold.dim_developer").select("developer_name"),
                "developer_name", "left_anti"
            )
            if new_devs.count() > 0:
                max_sk = get_max_key(spark, "hive.gold.dim_developer", "developer_sk")
                new_devs = new_devs.withColumn(
                    "developer_sk",
                    (lit(max_sk) + row_number().over(Window.orderBy("developer_name"))).cast("int")
                ).select("developer_sk", "developer_name")
                new_devs.writeTo("hive.gold.dim_developer").append()
                logger.info(f"dim_developer: {new_devs.count():,} new entries appended")
        _upsert_bridge(
            "hive.gold.bridge_game_developer", f"{Config.GOLD_PATH}/bridge_game_developer",
            "developers_cleaned", "developer_name",
            "hive.gold.dim_developer", "developer_sk", "game_dev_key"
        )

        # ========== Genres ==========
        gen_all = explode_and_clean(
            df_games.where(col("genres_cleaned").isNotNull()),
            "genres_cleaned", "genre_name"
        ).select("genre_name").distinct()

        if not table_exists(spark, "hive.gold.dim_genre"):
            dim_gen = gen_all.withColumn("genre_sk", row_number().over(Window.orderBy("genre_name"))) \
                             .select("genre_sk", "genre_name")
            dim_gen.writeTo("hive.gold.dim_genre").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_genre") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"dim_genre: {dim_gen.count():,} rows (initial)")
        else:
            new_gens = gen_all.join(
                spark.table("hive.gold.dim_genre").select("genre_name"),
                "genre_name", "left_anti"
            )
            if new_gens.count() > 0:
                max_sk = get_max_key(spark, "hive.gold.dim_genre", "genre_sk")
                new_gens = new_gens.withColumn(
                    "genre_sk",
                    (lit(max_sk) + row_number().over(Window.orderBy("genre_name"))).cast("int")
                ).select("genre_sk", "genre_name")
                new_gens.writeTo("hive.gold.dim_genre").append()
                logger.info(f"dim_genre: {new_gens.count():,} new entries appended")
        _upsert_bridge(
            "hive.gold.bridge_game_genre", f"{Config.GOLD_PATH}/bridge_game_genre",
            "genres_cleaned", "genre_name",
            "hive.gold.dim_genre", "genre_sk", "game_gen_key"
        )

        # ========== Publishers ==========
        pub_all = explode_and_clean(
            df_games.where(col("publishers_cleaned").isNotNull()),
            "publishers_cleaned", "publisher_name"
        ).select("publisher_name").distinct()

        if not table_exists(spark, "hive.gold.dim_publisher"):
            dim_pub = pub_all.withColumn("publisher_sk", row_number().over(Window.orderBy("publisher_name"))) \
                             .select("publisher_sk", "publisher_name")
            dim_pub.writeTo("hive.gold.dim_publisher").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_publisher") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"dim_publisher: {dim_pub.count():,} rows (initial)")
        else:
            new_pubs = pub_all.join(
                spark.table("hive.gold.dim_publisher").select("publisher_name"),
                "publisher_name", "left_anti"
            )
            if new_pubs.count() > 0:
                max_sk = get_max_key(spark, "hive.gold.dim_publisher", "publisher_sk")
                new_pubs = new_pubs.withColumn(
                    "publisher_sk",
                    (lit(max_sk) + row_number().over(Window.orderBy("publisher_name"))).cast("int")
                ).select("publisher_sk", "publisher_name")
                new_pubs.writeTo("hive.gold.dim_publisher").append()
                logger.info(f"dim_publisher: {new_pubs.count():,} new entries appended")
        _upsert_bridge(
            "hive.gold.bridge_game_publisher", f"{Config.GOLD_PATH}/bridge_game_publisher",
            "publishers_cleaned", "publisher_name",
            "hive.gold.dim_publisher", "publisher_sk", "game_pub_key"
        )

        # ========== Categories ==========
        cat_all = explode_and_clean(
            df_games.where(col("categories_cleaned").isNotNull()),
            "categories_cleaned", "category_name"
        ).select("category_name").distinct()

        if not table_exists(spark, "hive.gold.dim_category"):
            dim_cat = cat_all.withColumn("category_sk", row_number().over(Window.orderBy("category_name"))) \
                             .select("category_sk", "category_name")
            dim_cat.writeTo("hive.gold.dim_category").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_category") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"dim_category: {dim_cat.count():,} rows (initial)")
        else:
            new_cats = cat_all.join(
                spark.table("hive.gold.dim_category").select("category_name"),
                "category_name", "left_anti"
            )
            if new_cats.count() > 0:
                max_sk = get_max_key(spark, "hive.gold.dim_category", "category_sk")
                new_cats = new_cats.withColumn(
                    "category_sk",
                    (lit(max_sk) + row_number().over(Window.orderBy("category_name"))).cast("int")
                ).select("category_sk", "category_name")
                new_cats.writeTo("hive.gold.dim_category").append()
                logger.info(f"dim_category: {new_cats.count():,} new entries appended")
        _upsert_bridge(
            "hive.gold.bridge_game_category", f"{Config.GOLD_PATH}/bridge_game_category",
            "categories_cleaned", "category_name",
            "hive.gold.dim_category", "category_sk", "game_cate_key"
        )

        # ========== Tags ==========
        tag_all = explode_and_clean(
            df_games.where(col("tags_cleaned").isNotNull() & (col("tags_cleaned") != "")),
            "tags_cleaned", "tag_name"
        ).select("tag_name").distinct()

        if not table_exists(spark, "hive.gold.dim_tag"):
            dim_tag = tag_all.withColumn("tag_sk", row_number().over(Window.orderBy("tag_name"))) \
                             .select("tag_sk", "tag_name")
            dim_tag.writeTo("hive.gold.dim_tag").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_tag") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"dim_tag: {dim_tag.count():,} rows (initial)")
        else:
            new_tags = tag_all.join(
                spark.table("hive.gold.dim_tag").select("tag_name"),
                "tag_name", "left_anti"
            )
            if new_tags.count() > 0:
                max_sk = get_max_key(spark, "hive.gold.dim_tag", "tag_sk")
                new_tags = new_tags.withColumn(
                    "tag_sk",
                    (lit(max_sk) + row_number().over(Window.orderBy("tag_name"))).cast("int")
                ).select("tag_sk", "tag_name")
                new_tags.writeTo("hive.gold.dim_tag").append()
                logger.info(f"dim_tag: {new_tags.count():,} new entries appended")
        _upsert_bridge(
            "hive.gold.bridge_game_tag", f"{Config.GOLD_PATH}/bridge_game_tag",
            "tags_cleaned", "tag_name",
            "hive.gold.dim_tag", "tag_sk", "game_tag_key"
        )

        games_to_update.unpersist()

    finally:
        spark.stop()


# ================= 5. DIM_DETAIL_INFO =================
def create_dim_detail_info(**context):
    logger.info("BƯỚC 5: TẠO DIM_DETAIL_INFO")
    spark = get_spark_session("dim_detail_info")
    try:
        # SCD2: read only the current (active) version of each game
        df_games = spark.table("hive.silver.games").filter(col("is_current") == True)
        
        # Lấy record mới nhất per steam_appid
        window_latest = Window.partitionBy("steam_appid").orderBy(col("ts_ingested").desc())
        df_latest = df_games.withColumn("rn", row_number().over(window_latest)).filter(col("rn") == 1).drop("rn")
        
        _DETAIL_COLS = [
            "game_detail_key", "steam_appid", "steam_metacritic_score",
            "steam_price_initial", "steam_price_final", "steam_currency",
            "discount_pct", "recommendations_total",
            "age_category", "price_category", "price_vnd", "price_category_vnd"
        ]

        def _build_detail(sdf):
            return sdf.select(
                "steam_appid",
                "steam_metacritic_score",
                "steam_price_initial",
                "steam_price_final",
                lit("USD").alias("steam_currency"),
                when(col("steam_price_initial") > 0,
                     ((col("steam_price_initial") - col("steam_price_final"))
                      / col("steam_price_initial") * 100)
                ).otherwise(lit(0)).alias("discount_pct"),
                "recommendations_total",
                "age_category",
                "price_category",
                "price_vnd",
                "price_category_vnd",
            )

        new_data = _build_detail(df_latest)

        if not table_exists(spark, "hive.gold.dim_detail_info"):
            dim_detail = new_data.withColumn(
                "game_detail_key", row_number().over(Window.orderBy("steam_appid"))
            ).select(*_DETAIL_COLS)
            dim_detail.writeTo("hive.gold.dim_detail_info").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_detail_info") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"dim_detail_info: {dim_detail.count():,} rows (initial load)")
        else:
            existing = spark.table("hive.gold.dim_detail_info")
            max_key  = get_max_key(spark, "hive.gold.dim_detail_info", "game_detail_key")

            tracked_cols = [c for c in _DETAIL_COLS if c not in ("game_detail_key", "steam_appid")]
            existing_latest = (
                existing
                .withColumn("rn", row_number().over(Window.partitionBy("steam_appid").orderBy(col("game_detail_key").desc())))
                .filter(col("rn") == 1)
                .drop("rn")
            )
            existing_key_map = existing_latest.select("game_detail_key", "steam_appid")
            changed_keys = get_changed_keys(
                new_data,
                existing_latest.select("steam_appid", *tracked_cols),
                "steam_appid",
                tracked_cols,
            )

            new_recs = new_data \
                .join(existing_key_map.select("steam_appid"), "steam_appid", "left_anti") \
                .withColumn(
                    "game_detail_key",
                    (lit(max_key) + row_number().over(Window.orderBy("steam_appid"))).cast("int")
                ).select(*_DETAIL_COLS)

            changed_recs = new_data \
                .join(changed_keys, "steam_appid", "inner") \
                .join(existing_key_map, "steam_appid") \
                .select(*_DETAIL_COLS)

            upserts = changed_recs.unionByName(new_recs)
            new_recs_count = new_recs.count()
            changed_recs_count = changed_recs.count()
            if (new_recs_count + changed_recs_count) > 0:
                upserts.select("steam_appid").distinct().createOrReplaceTempView("__dim_detail_refresh_keys__")
                spark.sql("""
                    DELETE FROM hive.gold.dim_detail_info
                    WHERE steam_appid IN (SELECT steam_appid FROM __dim_detail_refresh_keys__)
                """)
                upserts.writeTo("hive.gold.dim_detail_info").append()
                logger.info(
                    f"dim_detail_info: UPSERTED — {new_recs_count:,} new, {changed_recs_count:,} updated"
                )
            else:
                logger.info("dim_detail_info: no changes detected, skipped")
    finally:
        spark.stop()

# ================= 6. DIM_REVIEW_INFO =================
def create_dim_review_info(**context):
    logger.info("BƯỚC 6: TẠO DIM_REVIEW_INFO")
    spark = get_spark_session("dim_review_info")
    try:
        # SCD2: read only current (active) reviews
        df_reviews = spark.table("hive.silver.reviews").filter(col("is_current") == True)
        
        _REVIEW_COLS = [
            "review_key", "steam_appid",
            "steam_review_total_positive", "steam_review_total_negative",
            "steam_review_total", "steam_review_positive_ratio",
            "steam_review_score_desc", "review_volume_category", "review_quality_score"
        ]

        def _build_review(sdf):
            return sdf.select(
                "steam_appid",
                "steam_review_total_positive",
                "steam_review_total_negative",
                "steam_review_total",
                "steam_review_positive_ratio",
                "steam_review_score_desc",
                "review_volume_category",
                "review_quality_score",
            )

        new_data = _build_review(df_reviews)

        if not table_exists(spark, "hive.gold.dim_review_info"):
            dim_review = new_data.withColumn(
                "review_key", row_number().over(Window.orderBy("steam_appid"))
            ).select(*_REVIEW_COLS)
            dim_review.writeTo("hive.gold.dim_review_info").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_review_info") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"dim_review_info: {dim_review.count():,} rows (initial load)")
        else:
            existing = spark.table("hive.gold.dim_review_info")
            max_key  = get_max_key(spark, "hive.gold.dim_review_info", "review_key")

            tracked_cols = [c for c in _REVIEW_COLS if c not in ("review_key", "steam_appid")]
            existing_latest = (
                existing
                .withColumn("rn", row_number().over(Window.partitionBy("steam_appid").orderBy(col("review_key").desc())))
                .filter(col("rn") == 1)
                .drop("rn")
            )
            existing_key_map = existing_latest.select("review_key", "steam_appid")
            changed_keys = get_changed_keys(
                new_data,
                existing_latest.select("steam_appid", *tracked_cols),
                "steam_appid",
                tracked_cols,
            )

            new_recs = new_data \
                .join(existing_key_map.select("steam_appid"), "steam_appid", "left_anti") \
                .withColumn(
                    "review_key",
                    (lit(max_key) + row_number().over(Window.orderBy("steam_appid"))).cast("int")
                ).select(*_REVIEW_COLS)

            changed_recs = new_data \
                .join(changed_keys, "steam_appid", "inner") \
                .join(existing_key_map, "steam_appid") \
                .select(*_REVIEW_COLS)

            upserts = changed_recs.unionByName(new_recs)
            new_recs_count = new_recs.count()
            changed_recs_count = changed_recs.count()
            if (new_recs_count + changed_recs_count) > 0:
                upserts.select("steam_appid").distinct().createOrReplaceTempView("__dim_review_refresh_keys__")
                spark.sql("""
                    DELETE FROM hive.gold.dim_review_info
                    WHERE steam_appid IN (SELECT steam_appid FROM __dim_review_refresh_keys__)
                """)
                upserts.writeTo("hive.gold.dim_review_info").append()
                logger.info(
                    f"dim_review_info: UPSERTED — {new_recs_count:,} new, {changed_recs_count:,} updated"
                )
            else:
                logger.info("dim_review_info: no changes detected, skipped")
    finally:
        spark.stop()

# ================= 7. DIM_POPULARITY_INFO =================
def create_dim_popularity_info(**context):
    logger.info("BƯỚC 7: TẠO DIM_POPULARITY_INFO")
    spark = get_spark_session("dim_popularity_info")
    try:
        # SCD2: read only current (active) popularity records
        df_pop = spark.table("hive.silver.popularity").filter(col("is_current") == True)
        
        _POP_COLS = [
            "popularity_key", "steam_appid",
            "steam_concurrent_players", "owners_low", "owners_high", "owners_midpoint",
            "steamspy_players_2weeks", "playtime_avg_hours", "playtime_median_hours",
            "popularity_category", "engagement_category"
        ]

        def _build_pop(sdf):
            return sdf.select(
                "steam_appid",
                "steam_concurrent_players",
                "owners_low",
                "owners_high",
                "owners_midpoint",
                "steamspy_players_2weeks",
                "playtime_avg_hours",
                "playtime_median_hours",
                "popularity_category",
                "engagement_category",
            )

        new_data = _build_pop(df_pop)

        if not table_exists(spark, "hive.gold.dim_popularity_info"):
            dim_pop = new_data.withColumn(
                "popularity_key", row_number().over(Window.orderBy("steam_appid"))
            ).select(*_POP_COLS)
            dim_pop.writeTo("hive.gold.dim_popularity_info").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/dim_popularity_info") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"dim_popularity_info: {dim_pop.count():,} rows (initial load)")
        else:
            existing = spark.table("hive.gold.dim_popularity_info")
            max_key  = get_max_key(spark, "hive.gold.dim_popularity_info", "popularity_key")

            tracked_cols = [c for c in _POP_COLS if c not in ("popularity_key", "steam_appid")]
            existing_latest = (
                existing
                .withColumn("rn", row_number().over(Window.partitionBy("steam_appid").orderBy(col("popularity_key").desc())))
                .filter(col("rn") == 1)
                .drop("rn")
            )
            existing_key_map = existing_latest.select("popularity_key", "steam_appid")
            changed_keys = get_changed_keys(
                new_data,
                existing_latest.select("steam_appid", *tracked_cols),
                "steam_appid",
                tracked_cols,
            )

            new_recs = new_data \
                .join(existing_key_map.select("steam_appid"), "steam_appid", "left_anti") \
                .withColumn(
                    "popularity_key",
                    (lit(max_key) + row_number().over(Window.orderBy("steam_appid"))).cast("int")
                ).select(*_POP_COLS)

            changed_recs = new_data \
                .join(changed_keys, "steam_appid", "inner") \
                .join(existing_key_map, "steam_appid") \
                .select(*_POP_COLS)

            upserts = changed_recs.unionByName(new_recs)
            new_recs_count = new_recs.count()
            changed_recs_count = changed_recs.count()
            if (new_recs_count + changed_recs_count) > 0:
                upserts.select("steam_appid").distinct().createOrReplaceTempView("__dim_pop_refresh_keys__")
                spark.sql("""
                    DELETE FROM hive.gold.dim_popularity_info
                    WHERE steam_appid IN (SELECT steam_appid FROM __dim_pop_refresh_keys__)
                """)
                upserts.writeTo("hive.gold.dim_popularity_info").append()
                logger.info(
                    f"dim_popularity_info: UPSERTED — {new_recs_count:,} new, {changed_recs_count:,} updated"
                )
            else:
                logger.info("dim_popularity_info: no changes detected, skipped")
    finally:
        spark.stop()

# ================= 8. FACT_GAME_SNAPSHOT (chỉ FKs) =================
def create_fact_game_snapshot(**context):
    logger.info("BƯỚC 8: TẠO FACT_GAME_SNAPSHOT (chỉ FKs)")
    spark = get_spark_session("fact_game_snapshot")
    try:
        # Đọc dims
        dim_game = spark.table("hive.gold.dim_game").select("game_key", "steam_appid", "release_date")
        dim_date = spark.table("hive.gold.dim_date").select("date_key", "date_id")
        dim_detail = spark.table("hive.gold.dim_detail_info").select("game_detail_key", "steam_appid")
        
        # Đọc bridges (lấy first record per game)
        bridge_dev = spark.table("hive.gold.bridge_game_developer") \
            .withColumn("rn", row_number().over(Window.partitionBy("game_key").orderBy("game_dev_key"))) \
            .filter(col("rn") == 1).drop("rn").select("game_key", "game_dev_key")
        
        bridge_gen = spark.table("hive.gold.bridge_game_genre") \
            .withColumn("rn", row_number().over(Window.partitionBy("game_key").orderBy("game_gen_key"))) \
            .filter(col("rn") == 1).drop("rn").select("game_key", "game_gen_key")
        
        bridge_pub = spark.table("hive.gold.bridge_game_publisher") \
            .withColumn("rn", row_number().over(Window.partitionBy("game_key").orderBy("game_pub_key"))) \
            .filter(col("rn") == 1).drop("rn").select("game_key", "game_pub_key")
        
        bridge_cat = spark.table("hive.gold.bridge_game_category") \
            .withColumn("rn", row_number().over(Window.partitionBy("game_key").orderBy("game_cate_key"))) \
            .filter(col("rn") == 1).drop("rn").select("game_key", "game_cate_key")
        
        bridge_tag = spark.table("hive.gold.bridge_game_tag") \
            .withColumn("rn", row_number().over(Window.partitionBy("game_key").orderBy("game_tag_key"))) \
            .filter(col("rn") == 1).drop("rn").select("game_key", "game_tag_key")

        # SCD2: read only current (active) game records from Silver
        df_games = spark.table("hive.silver.games").filter(col("is_current") == True).select("steam_appid", "ts_ingested")
        
        # JOIN để lấy tất cả FKs
        fact = df_games.join(dim_game, "steam_appid", "left") \
            .join(dim_detail.withColumnRenamed("steam_appid", "sa2"), 
                  col("steam_appid") == col("sa2"), "left").drop("sa2") \
            .withColumn("date_id", date_format(col("release_date"), "yyyyMMdd").cast("int")) \
            .join(dim_date, "date_id", "left").drop("date_id") \
            .join(bridge_dev, "game_key", "left") \
            .join(bridge_gen, "game_key", "left") \
            .join(bridge_pub, "game_key", "left") \
            .join(bridge_cat, "game_key", "left") \
            .join(bridge_tag, "game_key", "left") \
            .select(
                "game_key",
                "game_detail_key",
                "date_key",
                coalesce("game_dev_key", lit(-1)).alias("game_dev_key"),
                coalesce("game_gen_key", lit(-1)).alias("game_gen_key"),
                coalesce("game_pub_key", lit(-1)).alias("game_pub_key"),
                coalesce("game_cate_key", lit(-1)).alias("game_cate_key"),
                coalesce("game_tag_key",  lit(-1)).alias("game_tag_key"), 
                col("ts_ingested").alias("snapshot_ts")
            )
        
        if not table_exists(spark, "hive.gold.fact_game_snapshot"):
            fact.writeTo("hive.gold.fact_game_snapshot").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/fact_game_snapshot") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"fact_game_snapshot: {fact.count():,} rows (initial load)")
        else:
            fact.select("game_key").distinct().createOrReplaceTempView("__fact_game_refresh_keys__")
            spark.sql("""
                DELETE FROM hive.gold.fact_game_snapshot
                WHERE game_key IN (SELECT game_key FROM __fact_game_refresh_keys__)
            """)
            fact.writeTo("hive.gold.fact_game_snapshot").append()
            logger.info(f"fact_game_snapshot: UPSERTED ({fact.count():,} source rows processed)")
    finally:
        spark.stop()

# ================= 9. FACT_REVIEW_SNAPSHOT (chỉ FKs) =================
def create_fact_review_snapshot(**context):
    logger.info("BƯỚC 9: TẠO FACT_REVIEW_SNAPSHOT (chỉ FKs)")
    spark = get_spark_session("fact_review_snapshot")
    try:
        dim_game = spark.table("hive.gold.dim_game").select("game_key", "steam_appid")
        dim_date = spark.table("hive.gold.dim_date").select("date_key", "date_id")
        dim_review = spark.table("hive.gold.dim_review_info").select("review_key", "steam_appid")
        
        # SCD2: read only current (active) review records from Silver
        df_reviews = spark.table("hive.silver.reviews").filter(col("is_current") == True).select("steam_appid", "ts_ingested")
        
        # JOIN để lấy FKs
        fact = df_reviews.join(dim_game, "steam_appid", "left") \
            .join(dim_review.withColumnRenamed("steam_appid", "sa2"), 
                  col("steam_appid") == col("sa2"), "left").drop("sa2") \
            .withColumn("date_id", date_format(col("ts_ingested"), "yyyyMMdd").cast("int")) \
            .join(dim_date, "date_id", "left").drop("date_id") \
            .select(
                "game_key",
                "date_key",
                "review_key",
                col("ts_ingested").alias("snapshot_ts")
            )
        
        if not table_exists(spark, "hive.gold.fact_review_snapshot"):
            fact.writeTo("hive.gold.fact_review_snapshot").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/fact_review_snapshot") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"fact_review_snapshot: {fact.count():,} rows (initial load)")
        else:
            fact.select("game_key").distinct().createOrReplaceTempView("__fact_review_refresh_keys__")
            spark.sql("""
                DELETE FROM hive.gold.fact_review_snapshot
                WHERE game_key IN (SELECT game_key FROM __fact_review_refresh_keys__)
            """)
            fact.writeTo("hive.gold.fact_review_snapshot").append()
            logger.info(f"fact_review_snapshot: UPSERTED ({fact.count():,} source rows processed)")
    finally:
        spark.stop()

# ================= 10. FACT_POPULARITY_SNAPSHOT (chỉ FKs) =================
def create_fact_popularity_snapshot(**context):
    logger.info("BƯỚC 10: TẠO FACT_POPULARITY_SNAPSHOT (chỉ FKs)")
    spark = get_spark_session("fact_popularity_snapshot")
    try:
        dim_game = spark.table("hive.gold.dim_game").select("game_key", "steam_appid")
        dim_date = spark.table("hive.gold.dim_date").select("date_key", "date_id")
        dim_pop = spark.table("hive.gold.dim_popularity_info").select("popularity_key", "steam_appid")
        
        # SCD2: read only current (active) popularity records from Silver
        df_pop = spark.table("hive.silver.popularity").filter(col("is_current") == True).select("steam_appid", "ts")
        
        # JOIN để lấy FKs
        fact = df_pop.join(dim_game, "steam_appid", "left") \
            .join(dim_pop.withColumnRenamed("steam_appid", "sa2"), 
                  col("steam_appid") == col("sa2"), "left").drop("sa2") \
            .withColumn("date_id", date_format(col("ts"), "yyyyMMdd").cast("int")) \
            .join(dim_date, "date_id", "left").drop("date_id") \
            .select(
                "game_key",
                "date_key",
                "popularity_key",
                col("ts").alias("snapshot_ts")
            )
        
        if not table_exists(spark, "hive.gold.fact_popularity_snapshot"):
            fact.writeTo("hive.gold.fact_popularity_snapshot").using("iceberg") \
                .tableProperty("location", f"{Config.GOLD_PATH}/fact_popularity_snapshot") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("format-version", "2").createOrReplace()
            logger.info(f"fact_popularity_snapshot: {fact.count():,} rows (initial load)")
        else:
            fact.select("game_key").distinct().createOrReplaceTempView("__fact_pop_refresh_keys__")
            spark.sql("""
                DELETE FROM hive.gold.fact_popularity_snapshot
                WHERE game_key IN (SELECT game_key FROM __fact_pop_refresh_keys__)
            """)
            fact.writeTo("hive.gold.fact_popularity_snapshot").append()
            logger.info(f"fact_popularity_snapshot: UPSERTED ({fact.count():,} source rows processed)")
    finally:
        spark.stop()

# ================= 11. VERIFY =================
def verify_gold_layer(**context):
    logger.info("BƯỚC 11: XÁC NHẬN GOLD LAYER")
    spark = get_spark_session("verify_gold")
    try:
        tables = [
            "hive.gold.dim_date",
            "hive.gold.dim_game",
            "hive.gold.dim_developer",
            "hive.gold.dim_genre",
            "hive.gold.dim_publisher",
            "hive.gold.dim_category",
            "hive.gold.dim_tag", 
            "hive.gold.dim_detail_info",
            "hive.gold.dim_review_info",
            "hive.gold.dim_popularity_info",
            "hive.gold.bridge_game_developer",
            "hive.gold.bridge_game_genre",
            "hive.gold.bridge_game_publisher",
            "hive.gold.bridge_game_category",
            "hive.gold.bridge_game_tag", 
            "hive.gold.fact_game_snapshot",
            "hive.gold.fact_review_snapshot",
            "hive.gold.fact_popularity_snapshot"
        ]
        
        results = {}
        logger.info(f"\n{'='*80}")
        logger.info("CÁC BẢNG TRONG GOLD LAYER:")
        logger.info(f"{'='*80}")
        
        for t in tables:
            try:
                cnt = spark.table(t).count()
            except Exception as e:
                raise RuntimeError(
                    f"Gold verification failed: cannot count required table {t} ({type(e).__name__})."
                ) from None
            if t == "hive.gold.dim_date" and cnt == 0:
                raise RuntimeError("Gold verification failed: required calendar hive.gold.dim_date is empty.")
            results[t] = cnt
            logger.info(f"   {t:<45} : {cnt:>10,} records")
        
        logger.info(f"{'='*80}\n")
        
        # This bounded sample JOIN is diagnostic only.
        try:
            logger.info("Test query: JOIN FactGameSnapshot với DimGame")
            q = spark.sql("""
                SELECT g.name, COUNT(f.game_key) as snaps
                FROM hive.gold.dim_game g
                LEFT JOIN hive.gold.fact_game_snapshot f ON g.game_key = f.game_key
                GROUP BY g.name
                ORDER BY snaps DESC
                LIMIT 5
            """)
            logger.info("Gold sample JOIN diagnostic returned %d rows.", q.count())
        except Exception as e:
            logger.warning("Gold sample JOIN diagnostic failed (%s).", type(e).__name__)
    finally:
        spark.stop()

    logger.info("Gold verification passed for %d required tables.", len(results))
    return results

