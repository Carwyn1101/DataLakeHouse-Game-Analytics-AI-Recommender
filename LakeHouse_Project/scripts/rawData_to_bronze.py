from minio import Minio
from minio.error import S3Error
import csv
import re
import os
import pandas as pd
import io
import logging

from .bronze_appid import canonical_appid


_SILVER_COLUMNS = {
    "games.csv": {
        "name", "release_date", "is_free", "steam_metacritic_score",
        "steam_price_initial", "steam_price_final", "steam_currency",
        "developers", "publishers", "genres", "categories", "required_age",
        "recommendations_total", "tags",
    },
    "reviews.csv": {
        "steam_review_total_positive", "steam_review_total_negative",
        "steam_review_total", "steam_review_score_desc", "steam_review_positive_ratio",
    },
    "popularity.csv": {
        "steam_concurrent_players", "steamspy_owners_range", "steamspy_players_2weeks",
        "steamspy_average_forever_min", "steamspy_median_forever_min",
    },
}
# Match the default missing-value markers of the previous pandas 1.5 CSV reader.
_LEGACY_NULL_MARKERS = {
    "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan",
    "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null",
}
_ISO_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
    r"(?:[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?"
    r"(?:Z|[+-](?:[01][0-9]|2[0-3]):?[0-5][0-9])?)?"
)


def _read_bronze_csv(stream, object_name, *, source):
    """Validate CSV records and return the newest row per business key."""
    name = os.path.basename(object_name).lower()
    timestamp_column = "ts" if name == "popularity.csv" else "ts_ingested"
    side = "source" if source else "target"
    context = f"{object_name}: {side}"
    try:
        records = (row for row in csv.reader(stream, strict=True) if row)
        columns = next(records, [])
        if not columns and source:
            return columns, {}
        if any(not column.strip() or "\x00" in column for column in columns) or len(columns) != len(set(columns)):
            raise ValueError(f"{context} CSV has blank, invalid or duplicate headers")
        required = {"steam_appid", timestamp_column} | _SILVER_COLUMNS.get(name, set())
        if required.difference(columns):
            raise ValueError(f"{context} CSV lacks required columns")

        latest = {}
        conflicts = set()
        for record, values in enumerate(records, start=2):
            if len(values) != len(columns) or any("\x00" in value for value in values):
                raise ValueError(f"{context} CSV record {record} is malformed")
            row = dict(zip(columns, values))
            try:
                appid_text = canonical_appid(row["steam_appid"])
            except ValueError:
                raise ValueError(f"{context} CSV record {record} has an invalid steam_appid") from None
            timestamp_text = row[timestamp_column].strip()
            try:
                if not _ISO_TIMESTAMP.fullmatch(timestamp_text):
                    raise ValueError
                timestamp = pd.Timestamp(timestamp_text)
                if timestamp.tzinfo is not None:
                    timestamp = timestamp.tz_convert(None)
                timestamp_ns = timestamp.value
            except (ValueError, OverflowError):
                raise ValueError(f"{context} CSV record {record} has an invalid {timestamp_column}") from None

            if source:
                row = {column: "" if value in _LEGACY_NULL_MARKERS else value
                       for column, value in row.items()}
            row["steam_appid"] = appid_text
            row[timestamp_column] = str(timestamp)
            previous = latest.get(appid_text)
            if previous is not None and not source:
                raise ValueError(f"{context} CSV has duplicate steam_appid keys")
            if previous is None or timestamp_ns > previous[0]:
                latest[appid_text] = (timestamp_ns, row)
                conflicts.discard(appid_text)
            elif timestamp_ns == previous[0] and row != previous[1]:
                conflicts.add(appid_text)
        if conflicts:
            raise ValueError(f"{context} CSV has conflicting rows at the latest timestamp")
    except (csv.Error, UnicodeError):
        raise ValueError(f"{context} CSV cannot be decoded or parsed") from None
    return columns, latest


def _read_bronze_target(client, bucket, object_name):
    """Treat only a missing object reported by the SDK as an initial load."""
    try:
        response = client.get_object(bucket_name=bucket, object_name=object_name)
    except S3Error as error:
        if error.code == "NoSuchKey":
            return None
        raise
    with io.BytesIO() as payload:
        try:
            # Drain the SDK stream before parsing, including its end-of-body checks.
            for chunk in response.stream(amt=64 * 1024):
                payload.write(chunk)
        finally:
            try:
                response.close()
            finally:
                response.release_conn()
        payload.seek(0)
        with io.TextIOWrapper(payload, encoding="utf-8-sig", newline="") as stream:
            return _read_bronze_csv(stream, object_name, source=False)


def upload_to_minio(file_path, bucket, object_name):
    client = Minio(
        "minio:9000",
        access_key="hongphuc",
        secret_key="hongphuc",
        secure=False
    )
    client.fput_object(
        bucket_name=bucket,
        object_name=object_name,
        file_path=file_path
    )

def append_new_data_to_minio(file_path, bucket, object_name):
    client = Minio("minio:9000", access_key="hongphuc", secret_key="hongphuc", secure=False)
    try:
        response = client.get_object(bucket, object_name)
        df_minio = pd.read_csv(response)
        response.close()
        response.release_conn()
    except:
        df_minio = pd.DataFrame()

    try:
        df_local = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        logging.info(f"File {file_path} hoàn toàn trống, bỏ qua upload.")
        return

    if df_local.empty:
        logging.info(f"File {file_path} chỉ có header, không có dữ liệu, bỏ qua upload.")
        return

    if not df_minio.empty:
        df_append = df_local[~df_local['steam_appid'].isin(df_minio['steam_appid'])]
    else:
        df_append = df_local

    if not df_append.empty:
        df_new = pd.concat([df_minio, df_append], ignore_index=True)
        buf = io.BytesIO()
        df_new.to_csv(buf, index=False)
        buf.seek(0)
        client.put_object(bucket, object_name, buf, length=buf.getbuffer().nbytes, content_type='application/csv')

# def append_by_rowcount(file_path, bucket, object_name):
def upsert_to_minio(file_path, bucket, object_name):
    """Upsert to MinIO using timestamp-based incremental logic (SCD Type 1)."""
    client = Minio("minio:9000", access_key="hongphuc", secret_key="hongphuc", secure=False)
    with open(file_path, encoding="utf-8-sig", newline="") as stream:
        source_columns, source = _read_bronze_csv(stream, object_name, source=True)
    if not source:
        logging.info("%s | empty source, skipping write", object_name)
        return

    target = _read_bronze_target(client, bucket, object_name)
    columns, rows = target if target is not None else (source_columns, {})
    if set(columns) != set(source_columns):
        raise ValueError(f"{object_name}: source and target CSV headers differ")

    inserted = updated = conflicts = 0
    for appid, observation in source.items():
        current = rows.get(appid)
        if current is None:
            rows[appid] = observation
            inserted += 1
        elif observation[0] > current[0]:
            rows[appid] = observation
            updated += 1
        elif observation[0] == current[0] and observation[1] != current[1]:
            conflicts += 1

    if conflicts:
        logging.warning("%s | equal-timestamp conflicts retained from target=%d", object_name, conflicts)
    if not inserted and not updated:
        logging.info("%s | no changes, skipping write", object_name)
        return

    with io.StringIO(newline="") as csv_output:
        writer = csv.DictWriter(csv_output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(row for _, row in rows.values())
        payload = csv_output.getvalue().encode("utf-8")
    client.put_object(
        bucket_name=bucket, object_name=object_name, data=io.BytesIO(payload),
        length=len(payload), content_type="application/csv",
    )
    logging.info("%s | inserted=%d | updated=%d", object_name, inserted, updated)


def detect_and_upload():
    source_folder = "/opt/airflow/source_data"  # dùng cho airflow
    # source_folder = "D:\TLCN_KLTN\TLCN\data"
    bucket = "lakehouse"
    bronze_prefix = "bronze/"
    for file in os.listdir(source_folder):
        if file in _SILVER_COLUMNS:
            file_path = os.path.join(source_folder, file)
            object_name = bronze_prefix + file
            upsert_to_minio(file_path, bucket, object_name)
