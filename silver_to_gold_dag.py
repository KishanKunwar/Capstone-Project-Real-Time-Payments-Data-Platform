from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    'silver_to_gold',
    default_args=default_args,
    description='Load Silver data to Gold Postgres warehouse',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 1),
    catchup=False
) as dag:

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold_task',
        application='/opt/app/silver_to_gold.py',  # your Spark script path
        conn_id='spark_default',
        verbose=True
    )

