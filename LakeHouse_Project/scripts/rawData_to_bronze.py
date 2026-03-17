from minio import Minio
import os
import pandas as pd
import io
import logging

def upload_to_minio(file_path, bucket, object_name):
    client = Minio(
        "minio:9000",
        access_key="vansang8",
        secret_key="vansang8",
        secure=False
    )
    client.fput_object(
        bucket_name=bucket,
        object_name=object_name,
        file_path=file_path
    )

def append_new_data_to_minio(file_path, bucket, object_name):
    client = Minio("minio:9000", access_key="vansang8", secret_key="vansang8", secure=False)
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
    """Upsert data to MinIO: update existing rows by steam_appid, append new rows."""
    client = Minio("minio:9000", access_key="vansang8", secret_key="vansang8", secure=False)
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

    if df_local.empty:
        logging.info(f"File {file_path} không có dữ liệu, bỏ qua upload.")
        return

    if df_minio.empty:
        df_final = df_local
        new_count, updated_count = len(df_local), 0
    else:
        incoming_ids = set(df_local['steam_appid'].tolist())
        minio_ids    = set(df_minio['steam_appid'].tolist())
        new_ids      = incoming_ids - minio_ids
        updated_ids  = incoming_ids & minio_ids

        # Keep existing rows that are NOT being overwritten
        df_unchanged = df_minio[~df_minio['steam_appid'].isin(updated_ids)]
        # Combine unchanged rows with ALL incoming rows (covers both inserts and updates)
        df_final = pd.concat([df_unchanged, df_local], ignore_index=True)
        new_count     = len(new_ids)
        updated_count = len(updated_ids)

    buf = io.BytesIO()
    df_final.to_csv(buf, index=False)
    buf.seek(0)
    client.put_object(
        bucket, object_name, buf,
        length=buf.getbuffer().nbytes,
        content_type='application/csv'
    )
    logging.info(f"Upserted {object_name}: {new_count} new rows, {updated_count} updated rows")

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

