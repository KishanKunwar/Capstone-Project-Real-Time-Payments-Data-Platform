from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': True,
    'email': ['you@example.com'],  # replace with your email
}

with DAG(
    'silver_to_gold_daily',
    default_args=default_args,
    description='Daily DAG: Load Silver → Gold, run dbt, export reports, generate docs',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 1),
    catchup=False,
) as dag:

    # 1️⃣ Wait for the last Bronze → Silver run to complete
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze',
        external_dag_id='bronze_to_silver',      # hourly DAG
        external_task_id='bronze_to_silver_task',
        poke_interval=300,  # every 5 min
        timeout=3600        # max wait 1 hour
    )

    # 2️⃣ Run Silver → Gold Spark job
    silver_to_gold_task = SparkSubmitOperator(
        task_id='silver_to_gold_task',
        application='/opt/app/silver_to_gold.py',  # path to your Spark script
        conn_id='spark_default',
        verbose=True
    )

    # 3️⃣ Run dbt models
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/app/payments_dbt && dbt run'
    )

    # 4️⃣ Generate dbt docs
    generate_dbt_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command='cd /opt/app/payments_dbt && dbt docs generate'
    )

    # 5️⃣ Export Settlement Report CSV
    export_settlement_csv = BashOperator(
        task_id='export_settlement_csv',
        bash_command='cd /opt/app && python export_reports.py --report settlement'
    )

    # 6️⃣ Export Fraud Alerts CSV
    export_fraud_csv = BashOperator(
        task_id='export_fraud_csv',
        bash_command='cd /opt/app && python export_reports.py --report fraud'
    )

    # Setting dependencies
    wait_for_bronze >> silver_to_gold_task >> run_dbt_models >> generate_dbt_docs
    run_dbt_models >> [export_settlement_csv, export_fraud_csv]
