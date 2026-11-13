# scripts/bronze_to_silver_etl.py
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import tempfile
import shutil
from datetime import datetime

# ================= CONFIG =================
class Config:
    """Centralized configuration"""
    # MinIO/S3
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "hongphuc"
    MINIO_SECRET_KEY = "hongphuc"
    BUCKET = "lakehouse"
    
    # Paths
    BRONZE_PATH = f"s3a://{BUCKET}/bronze"
    SILVER_PATH = f"s3a://{BUCKET}/silver"
    WAREHOUSE_PATH = f"s3a://{BUCKET}/warehouse"
    
    # Hive
    HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
    
    # JARs
    JARS = "/opt/airflow/jars/hadoop-aws-3.3.2.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/jars/iceberg-spark-runtime-3.3_2.12-1.3.1.jar"
    
    # Business rules
    USD_TO_VND = 24500
    CURRENT_YEAR = datetime.now().year

    # SCD2: columns to compare for detecting changes per table
    SCD2_CHANGE_COLS = {
        'games': [
            'name_cleaned', 'steam_price_final', 'steam_price_initial',
            'required_age', 'is_free', 'steam_metacritic_score',
            'price_category', 'age_category', 'recommendations_total',
        ],
        'reviews': [
            'steam_review_positive_ratio', 'steam_review_total',
            'steam_review_score_desc', 'review_quality_score', 'review_volume_category',
        ],
        'popularity': [
            'steam_concurrent_players', 'steamspy_owners_range_cleaned',
            'playtime_avg_hours', 'owners_midpoint',
            'popularity_category', 'engagement_category', 'owners_category',
        ],
    }


# ================= UTILS =================
def get_spark_session():
    """Create configured Spark session"""
    return SparkSession.builder \
        .appName("Bronze to Silver ETL - TLTN Lakehouse") \
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


def clean_text_column(df, column, replacements={'™': '', '®': '', 'â„¢': ''}):
    """Clean text column with replacements"""
    df[f'{column}_cleaned'] = df[column].str.strip()
    for old, new in replacements.items():
        df[f'{column}_cleaned'] = df[f'{column}_cleaned'].str.replace(old, new, regex=False)
    return df[f'{column}_cleaned'].str.strip()


def ensure_numeric(df, column, dtype='int64', fill_value=0):
    """Ensure column is numeric with proper type"""
    return pd.to_numeric(df[column], errors='coerce').fillna(fill_value).astype(dtype)

def remove_duplicates(df, subset=None):
    """Remove duplicate rows from DataFrame"""
    if subset:
        df = df.drop_duplicates(subset=subset)
    else:
        df = df.drop_duplicates()
    return df


# ================= SCD TYPE 2 HELPERS =================
def _table_exists(spark, table_name):
    """Check the configured V2 catalog and propagate lookup failures."""
    catalog_name, namespace_name, name = table_name.split(".")
    catalog = spark._jsparkSession.sessionState().catalogManager().catalog(catalog_name)
    namespace = spark.sparkContext._gateway.new_array(spark._jvm.java.lang.String, 1)
    namespace[0] = namespace_name
    identifier = spark._jvm.org.apache.spark.sql.connector.catalog.Identifier.of(namespace, name)
    return catalog.tableExists(identifier)


def _add_scd2_columns(sdf, version=1):
    """Attach SCD2 metadata columns to a Spark DataFrame (for initial load)."""
    return sdf \
        .withColumn("effective_date", F.current_timestamp()) \
        .withColumn("expiry_date", F.lit(None).cast("timestamp")) \
        .withColumn("is_current", F.lit(True)) \
        .withColumn("scd_version", F.lit(version).cast("int"))


def _upsert_scd2(spark, new_sdf, table_name, key_col, change_cols):
    """Insert successors and expire changed current rows in one Iceberg commit."""
    target = spark.table(table_name)
    columns = new_sdf.columns
    expire = "__scd2_expire"
    max_version = "__scd2_max_version"
    metadata = {
        "effective_date": "current_timestamp()",
        "expiry_date": "CAST(NULL AS TIMESTAMP)",
        "is_current": "true",
        # Spark file reads mark fields nullable. The SQL expression keeps this required field non-nullable.
        "scd_version": f"CAST(COALESCE(s.{max_version}, 0) + 1 AS INT)",
    }
    if (set(columns) & (metadata.keys() | {expire, max_version})
            or set(target.columns) != set(columns) | metadata.keys()):
        raise ValueError("Silver SCD2 source columns do not match the target schema")

    current = target.filter(F.col("is_current") == True)
    joined = new_sdf.alias("s").join(current.alias("e"), key_col, "left")
    # One joined row per key requires unique input and at most one current target row.
    invalid = joined.groupBy(key_col).count().filter(
        F.col(key_col).isNull() | (F.col("count") > 1)
    )
    if invalid.limit(1).count():
        raise ValueError("Silver SCD2 requires non-null unique source keys and at most one current row per key")

    changed = F.lit(False)
    for column in change_cols:
        if column in columns:
            changed = changed | (~F.col(f"s.{column}").eqNullSafe(F.col(f"e.{column}")))

    has_current = F.col("e.is_current").isNotNull()
    selected = joined.filter((~has_current) | changed).select("s.*", has_current.alias(expire)).cache()
    try:
        insert_count = selected.count()
        if insert_count == 0:
            return 0

        # Expired-only keys from an older interrupted run resume after their latest version.
        versions = target.groupBy(key_col).agg(F.max("scd_version").alias(max_version))
        successors = selected.join(versions, key_col, "left")

        # Only expiration actions can match a current row. Every successor is an insert.
        staged = successors.withColumn(expire, F.lit(False)).unionByName(successors.filter(F.col(expire)))

        def quote(identifier):
            return "`" + identifier.replace("`", "``") + "`"

        table = ".".join(quote(part) for part in table_name.split("."))
        key = quote(key_col)
        insert_columns = ", ".join(quote(column) for column in [*columns, *metadata])
        insert_values = ", ".join([*(f"s.{quote(column)}" for column in columns), *metadata.values()])

        # Spark 3.3.2 can fail when adding metadata columns through an analyzed temp view.
        # A direct file source cuts the target lineage and avoids creating a View node.
        with tempfile.TemporaryDirectory(prefix="silver-scd2-") as directory:
            source = f"file:///{Path(directory).as_posix().lstrip('/')}/actions"
            staged.write.parquet(source)
            spark.sql(f"""
                MERGE INTO {table} AS t
                USING parquet.{quote(source)} AS s
                ON s.{expire} AND t.is_current = true AND t.{key} = s.{key}
                WHEN MATCHED THEN
                    UPDATE SET is_current = false, expiry_date = current_timestamp()
                WHEN NOT MATCHED AND NOT s.{expire} THEN
                    INSERT ({insert_columns}) VALUES ({insert_values})
            """)

        return insert_count
    finally:
        selected.unpersist()


# ================= TRANSFORMS =================
def preprocess_games(df_raw):
    """Transform games table"""
    df = df_raw.copy()
    df = remove_duplicates(df, subset='steam_appid')

    if 'steam_currency' in df.columns:
        df['steam_currency'].fillna('N/A', inplace=True)

    critical_fields = ['publishers', 'tags', 'categories', 'developers', 'genres', 'release_date', 'name']
    missing_mask = df[critical_fields].isnull().any(axis=1)
    removed_appids = df.loc[missing_mask, 'steam_appid'].tolist()

    df = df.loc[~missing_mask].copy()
    
    df['name_cleaned'] = clean_text_column(df, 'name')
    legal_terms = [
        ', Inc.', ', Inc', ' Inc.', ' Inc',
        ', Ltd.', ', Ltd', ' Ltd.', ' Ltd', 
        ', Co.', ', Co', ' Co.', ' Co',
        ', LLC.', ', LLC', ' LLC.', ' LLC',
        ', Corp.', ', Corp', ' Corp.', ' Corp',
        ', S.A.', ', SA.', ', SA', ' S.A.', ' SA.',
        ', GmbH', ' GmbH',
        ', AB', ' AB',
        ', AS', ' AS',
        ', Oy', ' Oy',
        ', SRL', ' SRL',
        ', Pvt. Ltd.', ', Pvt Ltd', ' Pvt. Ltd.', ' Pvt Ltd'
    ]
    
    for col in ['developers', 'publishers']:
        df[f'{col}_cleaned'] = df[col].str.strip()
        for term in sorted(legal_terms, key=len, reverse=True):
            df[f'{col}_cleaned'] = df[f'{col}_cleaned'].str.replace(term, '', regex=False)
        df[f'{col}_cleaned'] = df[f'{col}_cleaned'].str.strip()
    
    for col in ['genres', 'categories', 'tags']:
        df[f'{col}_cleaned'] = df[col].str.strip().str.replace(' , ', ', ', regex=False)
    
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['release_year'] = df['release_date'].dt.year
    df['game_age_years'] = Config.CURRENT_YEAR - df['release_year']
    
    df['age_category'] = pd.cut(
        df['game_age_years'].fillna(-1),
        bins=[-float('inf'), 0, 1, 3, 7, 15, float('inf')],
        labels=['Unknown', 'New', 'Recent', 'Established', 'Mature', 'Legacy']
    ).astype(str)

    df['steam_price_initial'] = pd.to_numeric(df['steam_price_initial'], errors='coerce')
    df['steam_price_final'] = pd.to_numeric(df['steam_price_final'], errors='coerce')
    
    df['steam_price_initial'] = df['steam_price_initial'] / 100
    df['steam_price_final'] = df['steam_price_final'] / 100
    
    df['price_category'] = pd.cut(
        df['steam_price_final'].fillna(0),
        bins=[-float('inf'), 0.01, 10, 30, 60, float('inf')],
        labels=['Free', 'Budget', 'Mid-range', 'Premium', 'AAA']
    ).astype(str)
    
    df['price_vnd'] = (df['steam_price_final'] * Config.USD_TO_VND).round(0).fillna(0)
    
    df['price_category_vnd'] = pd.cut(
        df['price_vnd'],
        bins=[-float('inf'), 1, 250000, 750000, 1500000, float('inf')],
        labels=['Miễn phí', 'Giá rẻ', 'Trung bình', 'Cao cấp', 'AAA']
    ).astype(str)
    
    df['required_age'] = df['required_age'].astype(str).str.replace('+', '').replace(['0', 'None', ''], 0)
    df['required_age'] = pd.to_numeric(df['required_age'], errors='coerce').fillna(0).astype('int64')
    if 'ts_ingested' in df.columns:
        df['ts_ingested'] = pd.to_datetime(df['ts_ingested'], errors='coerce')
        df['ts_ingested'] = df['ts_ingested'].fillna(pd.Timestamp.now())
    else:
        df['ts_ingested'] = pd.Timestamp.now()
    
    numeric_cols = {
        'steam_appid': 'int64',
        'steam_metacritic_score': 'float64',
        'steam_price_initial': 'float64',
        'steam_price_final': 'float64',
        'recommendations_total': 'float64',
        'release_year': 'int64',
        'game_age_years': 'int64',
        'price_vnd': 'int64'
    }
    
    for col, dtype in numeric_cols.items():
        df[col] = ensure_numeric(df, col, dtype)

    df['is_free'] = df['is_free'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False})

    return df, removed_appids


def preprocess_popularity(df_raw):
    """Transform popularity table"""
    df = df_raw.copy()
    df = remove_duplicates(df, subset='steam_appid')
    def parse_range(s):
        try:
            if pd.isna(s): return 0, 0, 0
            parts = str(s).strip().split(' .. ')
            if len(parts) == 2:
                low = int(parts[0].replace(',', ''))
                high = int(parts[1].replace(',', ''))
                return low, high, (low + high) // 2
            return 0, 0, 0
        except:
            return 0, 0, 0
    
    df[['owners_low', 'owners_high', 'owners_midpoint']] = df['steamspy_owners_range'].apply(lambda x: pd.Series(parse_range(x)))
    df['steamspy_owners_range_cleaned'] = df['steamspy_owners_range'].str.strip()
    
    df['playtime_avg_hours'] = df['steamspy_average_forever_min'] / 60
    df['playtime_median_hours'] = df['steamspy_median_forever_min'] / 60
    
    df['popularity_category'] = pd.cut(
        df['steam_concurrent_players'].fillna(0),
        bins=[-float('inf'), 0.1, 100, 1000, 10000, 100000, float('inf')],
        labels=['Inactive', 'Low', 'Moderate', 'Popular', 'Very Popular', 'Extremely Popular']
    ).astype(str)
    
    df['engagement_category'] = pd.cut(
        df['playtime_avg_hours'].fillna(0),
        bins=[-float('inf'), 0.1, 2, 10, 50, 200, float('inf')],
        labels=['No Data', 'Low Engagement', 'Casual', 'Engaged', 'Dedicated', 'Hardcore']
    ).astype(str)
    
    df['owners_category'] = pd.cut(
        df['owners_midpoint'].fillna(0),
        bins=[-float('inf'), 0.1, 1000, 10000, 100000, 1000000, 10000000, float('inf')],
        labels=['No Data', 'Niche', 'Small', 'Medium', 'Popular', 'Hit', 'Blockbuster']
    ).astype(str)
    
    numeric_cols = {
        'steam_appid': 'int64',
        'steam_concurrent_players': 'int64',
        'steamspy_players_2weeks': 'int64',
        'steamspy_average_forever_min': 'int64',
        'steamspy_median_forever_min': 'int64',
        'owners_low': 'int64',
        'owners_high': 'int64',
        'owners_midpoint': 'int64',
        'playtime_avg_hours': 'float64',
        'playtime_median_hours': 'float64'
    }
    for col, dtype in numeric_cols.items():
        df[col] = ensure_numeric(df, col, dtype)
    
    df['ts'] = pd.to_datetime(df['ts'], errors='coerce')
    return df


def preprocess_reviews(df_raw):
    """Transform reviews table"""
    df = df_raw.copy()
    df = remove_duplicates(df, subset='steam_appid')
    mask = df['steam_review_positive_ratio'].isnull() & (df['steam_review_total'] > 0)
    df.loc[mask, 'steam_review_positive_ratio'] = (
        df.loc[mask, 'steam_review_total_positive'] / df.loc[mask, 'steam_review_total']
    )
    df['steam_review_positive_ratio'].fillna(0, inplace=True)
    
    df['review_volume_category'] = pd.cut(
        df['steam_review_total'].fillna(0),
        bins=[-float('inf'), 0.1, 100, 1000, 10000, float('inf')],
        labels=['No Reviews', 'Few', 'Moderate', 'Many', 'Overwhelming']
    ).astype(str)
    
    df['review_quality_score'] = (
        df['steam_review_positive_ratio'] *
        df['steam_review_total'].apply(lambda x: min(1.0, x / 1000)) * 100
    )

# 
    if 'steam_review_score_desc' in df.columns:
        missing_desc_before = df['steam_review_score_desc'].isnull().sum()
        if missing_desc_before > 0:
            df['steam_review_score_desc'].fillna('No Reviews', inplace=True)
    
    if 'ts_ingested' in df.columns:
        df['ts_ingested'] = pd.to_datetime(df['ts_ingested'], errors='coerce')
        df['ts_ingested'] = df['ts_ingested'].fillna(pd.Timestamp.now())
    else:
        df['ts_ingested'] = pd.Timestamp.now()
    
    numeric_cols = {
        'steam_appid': 'int64',
        'steam_review_total_positive': 'int64',
        'steam_review_total_negative': 'int64',
        'steam_review_total': 'int64',
        'steam_review_positive_ratio': 'float64',
        'review_quality_score': 'float64'
    }
    
    for col, dtype in numeric_cols.items():
        df[col] = ensure_numeric(df, col, dtype)
    
    df['ts_ingested'] = pd.to_datetime(df['ts_ingested'], errors='coerce')
    return df


# ================= MAIN ETL =================
def bronze_to_silver_etl():
    """ETL Bronze to Silver with Iceberg tables"""
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    logger.info("=" * 60)
    logger.info("ETL BRONZE → SILVER START")
    logger.info("=" * 60)

    spark = get_spark_session()
    temp_dir = tempfile.mkdtemp()

    try:
        # Extract
        logger.info("Extracting from Bronze...")
        tables = {
            'games': f"{Config.BRONZE_PATH}/games.csv",
            'popularity': f"{Config.BRONZE_PATH}/popularity.csv",
            'reviews': f"{Config.BRONZE_PATH}/reviews.csv"
        }

        if not hasattr(np, 'bool'):
            np.bool = bool
        if not hasattr(np, 'object'):
            np.object = object
        raw_data = {
            name: (
                spark.read.option("header", True)
                    .option("inferSchema", True)
                    .option("quote", '"')          
                    .option("escape", '"')      
                    .option("multiLine", True)     
                    .csv(path)
                    .toPandas()
            )
            for name, path in tables.items()
        }
        logger.info(f"Extracted {sum(len(df) for df in raw_data.values()):,} rows")

        # 1) process games and get removed_appids
        games_clean, removed_appids = preprocess_games(raw_data['games'])
        reviews_clean = preprocess_reviews(raw_data['reviews'])
        popularity_clean = preprocess_popularity(raw_data['popularity'])
        if removed_appids:
            logger.info(f"removing related rows in reviews and popularity for {len(removed_appids):,} appids")
            if 'steam_appid' in reviews_clean.columns:
                reviews_clean = reviews_clean[~reviews_clean['steam_appid'].isin(removed_appids)].copy()
            if 'steam_appid' in popularity_clean.columns:
                popularity_clean = popularity_clean[~popularity_clean['steam_appid'].isin(removed_appids)].copy()

        # Transform
        logger.info("Transforming...")

        transformed_data = {
            'games': games_clean,
            'popularity': popularity_clean,
            'reviews': reviews_clean
        }

        # Convert to Spark
        for name, df in transformed_data.items():
            df.to_parquet(f"{temp_dir}/{name}.parquet", index=False)

        spark_dfs = {name: spark.read.parquet(f"file://{temp_dir}/{name}.parquet") for name in transformed_data.keys()}

        # Load
        logger.info("Writing to Silver layer (Iceberg - SCD Type 2)...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS hive.silver LOCATION '{Config.SILVER_PATH}'")

        for name, sdf in spark_dfs.items():
            table_id = f"hive.silver.{name}"
            change_cols = Config.SCD2_CHANGE_COLS[name]

            if _table_exists(spark, table_id):
                before = spark.table(table_id).count()
                inserted = _upsert_scd2(spark, sdf, table_id, "steam_appid", change_cols)
                after = spark.table(table_id).count()
                logger.info(
                    f"{name}: SCD2 UPSERT — {inserted:,} versions inserted "
                    f"(total rows {before:,} → {after:,})"
                )
            else:
                # Initial load: attach SCD2 metadata columns before creating table
                sdf_init = _add_scd2_columns(sdf)
                sdf_init.writeTo(table_id) \
                    .using("iceberg") \
                    .tableProperty("write.format.default", "parquet") \
                    .tableProperty("format-version", "2") \
                    .create()
                logger.info(f"{name}: {sdf_init.count():,} rows written (initial SCD2 load)")

        logger.info("ETL Completed Successfully!")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        spark.stop()
        logger.info("Spark stopped and temp cleaned.")


