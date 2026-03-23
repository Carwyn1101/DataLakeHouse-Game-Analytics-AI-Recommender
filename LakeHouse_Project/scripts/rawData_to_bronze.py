from minio import Minio
import os
import pandas as pd
import io
import logging


def _detect_ts_column(df):
    """Choose the timestamp column used for incremental compare."""
    for c in ["ts_ingested", "ts"]:
        if c in df.columns:
            return c
    return None


def _normalize_ts(series):
    """Parse mixed datetime formats to a comparable naive-UTC timestamp."""
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.tz_convert(None)


def _build_content_diff_mask(cmp_df, common_cols):
    """Return a boolean mask for rows where any shared non-key column changed."""
    if not common_cols:
        return pd.Series(False, index=cmp_df.index)

    diff_mask = pd.Series(False, index=cmp_df.index)
    for c in common_cols:
        src = cmp_df[f"{c}_src"].fillna("__NULL__").astype(str)
        tgt = cmp_df[f"{c}_tgt"].fillna("__NULL__").astype(str)
        diff_mask = diff_mask | (src != tgt)
    return diff_mask

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
    try:
        response = client.get_object(bucket, object_name)
        df_minio = pd.read_csv(response)
        response.close()
        response.release_conn()
    except Exception:
        df_minio = pd.DataFrame()

    try:
        df_local = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        logging.info(f"File {file_path} hoàn toàn trống, bỏ qua upload.")
        return

    source_read = len(df_local)
    if df_local.empty:
        logging.info(f"File {file_path} không có dữ liệu, bỏ qua upload.")
        return

    if "steam_appid" not in df_local.columns:
        logging.warning(f"{object_name}: thiếu cột steam_appid, bỏ qua incremental upsert.")
        return

    # Keep latest source record per steam_appid (newest timestamp wins).
    ts_col = _detect_ts_column(df_local)
    if ts_col:
        df_local[ts_col] = _normalize_ts(df_local[ts_col])
        df_local = df_local.sort_values(ts_col).drop_duplicates(subset=["steam_appid"], keep="last")
    else:
        df_local = df_local.drop_duplicates(subset=["steam_appid"], keep="last")

    if df_minio.empty:
        df_final = df_local
        inserted = len(df_local)
        updated_real = 0
        delta_filtered = inserted
        unchanged = source_read - delta_filtered
    else:
        if "steam_appid" not in df_minio.columns:
            # Fallback: rewrite by source if target malformed
            inserted = len(df_local)
            updated_real = 0
            delta_filtered = inserted
            unchanged = source_read - delta_filtered
            df_final = df_local
        else:
            target_ts_col = _detect_ts_column(df_minio)
            if ts_col and target_ts_col:
                df_minio[target_ts_col] = _normalize_ts(df_minio[target_ts_col])

                df_minio_latest = df_minio.drop_duplicates(subset=["steam_appid"], keep="last")
                cmp_df = df_local.merge(
                    df_minio_latest,
                    on="steam_appid",
                    how="left",
                    suffixes=("_src", "_tgt"),
                    indicator=True,
                )

                src_ts_col = f"{ts_col}_src"
                tgt_ts_col = f"{target_ts_col}_tgt"

                common_cols = [
                    c for c in df_local.columns
                    if c in df_minio_latest.columns and c != "steam_appid"
                ]
                diff_mask = _build_content_diff_mask(cmp_df, common_cols)

                is_insert = cmp_df["_merge"] == "left_only"
                ts_update = (~is_insert) & (cmp_df[src_ts_col] > cmp_df[tgt_ts_col])
                # If timestamp is equal/invalid but business columns changed, still treat as real update.
                content_update = (~is_insert) & (~ts_update) & diff_mask
                is_update = ts_update | content_update
                delta_ids = set(cmp_df.loc[is_insert | is_update, "steam_appid"].tolist())

                inserted = int(is_insert.sum())
                updated_real = int(is_update.sum())
                delta_filtered = inserted + updated_real
                unchanged = source_read - delta_filtered
            else:
                # No comparable timestamp: detect real updates by content difference.
                df_minio_latest = df_minio.drop_duplicates(subset=["steam_appid"], keep="last")
                cmp_df = df_local.merge(
                    df_minio_latest,
                    on="steam_appid",
                    how="left",
                    suffixes=("_src", "_tgt"),
                    indicator=True,
                )

                common_cols = [
                    c for c in df_local.columns
                    if c in df_minio_latest.columns and c != "steam_appid"
                ]

                diff_mask = _build_content_diff_mask(cmp_df, common_cols)

                is_insert = cmp_df["_merge"] == "left_only"
                is_update = (~is_insert) & diff_mask
                delta_ids = set(cmp_df.loc[is_insert | is_update, "steam_appid"].tolist())

                inserted = int(is_insert.sum())
                updated_real = int(is_update.sum())
                delta_filtered = inserted + updated_real
                unchanged = source_read - delta_filtered

            df_delta = df_local[df_local["steam_appid"].isin(delta_ids)]
            df_unchanged_target = df_minio[~df_minio["steam_appid"].isin(delta_ids)]
            df_final = pd.concat([df_unchanged_target, df_delta], ignore_index=True)

    buf = io.BytesIO()
    df_final.to_csv(buf, index=False)
    buf.seek(0)
    client.put_object(
        bucket, object_name, buf,
        length=buf.getbuffer().nbytes,
        content_type='application/csv'
    )
    logging.info(
        f"{object_name} | source_read={source_read} | delta_filtered={delta_filtered} | "
        f"inserted={inserted} | updated_real={updated_real} | unchanged={unchanged}"
    )

def detect_and_upload():
    source_folder = "/opt/airflow/source_data"  # dùng cho airflow
    # source_folder = "D:\TLCN_KLTN\TLCN\data"
    bucket = "lakehouse"
    bronze_prefix = "bronze/"
    uploaded_files = set()  # Giữ tên các file đã được tải lên

    for file in os.listdir(source_folder):
        if file not in uploaded_files:
            file_path = os.path.join(source_folder, file)
            object_name = bronze_prefix + file
            upsert_to_minio(file_path, bucket, object_name)
            uploaded_files.add(file)

