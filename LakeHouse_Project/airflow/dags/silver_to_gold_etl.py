# dags/gold_etl_complete.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.silver_to_gold_etl import (
    create_gold_database,
    create_dim_date,
    create_dim_game,
    create_dims_and_bridges,
    create_dim_detail_info,
    create_dim_review_info,
    create_dim_popularity_info,
    create_fact_game_snapshot,
    create_fact_review_snapshot,
    create_fact_popularity_snapshot,
    verify_gold_layer
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025,10,27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'gold_layer_etl_complete',
    default_args=default_args,
    description='Gold ETL theo schema FactGameSnapshot/DimGame/bridges/DimReviewDescriptor',
    schedule=None,
    catchup=False,
    tags=['gold','dim','fact','snapshot']
) as dag:

    t_create_db = PythonOperator(task_id='create_gold_database', python_callable=create_gold_database)
    t_dim_date = PythonOperator(task_id='create_dim_date', python_callable=create_dim_date)
    t_dim_game = PythonOperator(task_id='create_dim_game', python_callable=create_dim_game)
    t_dims_bridges = PythonOperator(task_id='create_dims_and_bridges', python_callable=create_dims_and_bridges)
    t_dim_detail_info = PythonOperator(task_id='create_dim_detail_info', python_callable=create_dim_detail_info)
    t_dim_review_info = PythonOperator(task_id='create_dim_review_info', python_callable=create_dim_review_info)
    t_dim_popularity_info = PythonOperator(task_id='create_dim_popularity_info', python_callable=create_dim_popularity_info)
    t_fact_game = PythonOperator(task_id='create_fact_game_snapshot', python_callable=create_fact_game_snapshot)
    t_fact_review = PythonOperator(task_id='create_fact_review_snapshot', python_callable=create_fact_review_snapshot)
    t_fact_pop = PythonOperator(task_id='create_fact_popularity_snapshot', python_callable=create_fact_popularity_snapshot)
    t_verify = PythonOperator(task_id='verify_gold_layer', python_callable=verify_gold_layer)

    # Tạo DB trước
    t_create_db >> t_dim_date >> t_dim_game 

    # Sau dim_game thì chạy tuần tự các dim và bridge
    t_dim_game >> t_dims_bridges >> t_dim_detail_info >> t_dim_review_info >> t_dim_popularity_info


    # Fact phụ thuộc vào dims & bridges
    [t_dim_date, t_dim_game, t_dim_detail_info, t_dims_bridges] >> t_fact_game
    [t_dim_date, t_dim_game, t_dim_review_info] >> t_fact_review
    [t_dim_date, t_dim_game, t_dim_popularity_info] >> t_fact_pop

    # Verify cuối cùng
    [t_fact_game, t_fact_review, t_fact_pop] >> t_verify

