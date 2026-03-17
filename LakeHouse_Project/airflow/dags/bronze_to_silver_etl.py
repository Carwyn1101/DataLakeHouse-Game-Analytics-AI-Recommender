# airflow/dags/bronze_to_silver_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.bronze_to_silver_etl import bronze_to_silver_etl

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bronze_to_silver_etl',
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    description='ETL from Bronze to Silver layer using Iceberg',
    tags=['etl', 'bronze-to-silver', 'iceberg']
) as dag:

    etl_task = PythonOperator(
        task_id='run_bronze_to_silver_etl',
        python_callable=bronze_to_silver_etl
    )

    etl_task
