from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.rawData_to_bronze import detect_and_upload

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'auto_upload_bronze_to_minio',
    default_args=default_args,
    schedule='*/2 * * * *',  # \ '0 0 * * *' 00:00 hàng ngày \ '*/2 * * * *' mỗi 2 phút
    catchup=False
) as dag:
    upload_task = PythonOperator(
        task_id='detect_and_upload',
        python_callable=detect_and_upload
    )