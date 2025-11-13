from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'bronze_to_silver',
    default_args=default_args,
    description='Move Bronze Parquet to Silver Layer',
    schedule_interval='@hourly',
    start_date=datetime(2025, 11, 1),
    catchup=False
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver_task',
        application='/opt/app/bronze_to_silver.py',
        conn_id='spark_default',
        verbose=True
    )
