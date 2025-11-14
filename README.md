Project Overview
This project simulates a payments processing pipeline using Python, Kafka, Spark, Airflow, Postgres, and dbt. It demonstrates real-time ingestion, validation, transformation, and reporting of payment transactions, including fraud detection.
Pipeline Flow:
Python Producer → Kafka (raw/deadletter) → Spark Streaming (Bronze) → Spark Batch (Silver) → Postgres (Gold) → dbt → Daily Reports

2. Features


Simulated Payments Producer: Generates JSON payment events and publishes to Kafka topics (payments.raw, payments.deadletter).


Kafka Setup: Manages valid and invalid payment events.


Spark Jobs:


Bronze Layer: Raw + validated payments in Parquet, partitioned by date.


Silver Layer: Deduplicated, cleaned, and standardized data.


Gold Layer: Loaded into Postgres, organized into a star schema.




Airflow Orchestration:


DAG 1 (hourly): Bronze → Silver


DAG 2 (daily): Silver → Gold → dbt → Export daily CSVs




dbt Modeling: Transformation, documentation, and report generation.


Fraud Detection Rules: High amount, velocity, cross-border, blacklisted merchants, decline rate.


3. Project Structure
apache_kafka/
├── payments_producer.py        # Python script generating payment events
├── bronze_to_silver.py         # Spark job for Bronze → Silver
├── silver_to_gold.py           # Spark job for Silver → Gold
├── export_reports.py           # Script to export CSV reports
├── payments_dbt/               # dbt project
│   ├── models/
│   ├── macros/
│   └── dbt_project.yml
└── dags/                       # Airflow DAGs
    ├── bronze_to_silver_dag.py
    └── silver_to_gold_dag.py

4. Fraud Rules


| Rule                 | Description                                           | Example                                      |
|----------------------|-------------------------------------------------------|----------------------------------------------|
| High Amount          | Flag if amount > 10,000 for any merchant             | TXN amount = 15,000 → flagged               |
| Velocity             | Flag if same card_hash has > 5 transactions in 1 min | 6 transactions in 30 sec → flagged          |
| Cross-border         | Flag if same card_hash used in 2 countries within 10 min | TXN1: US, TXN2: Canada within 10 min → flagged |
| Blacklisted Merchant | Flag if merchant_id is in a blacklist table          | merchant_id = M12345 → flagged               |
| Decline Rate         | Flag if a card has > 50% declined transactions in last 10 attempts | 6/10 declined → flagged           |

5. Runbook
Prerequisites


Python 3.12, pip


Apache Kafka


Apache Spark


Postgres


Airflow


dbt


Steps


Start Kafka: Create topics payments.raw and payments.deadletter.


Run Payment Producer:


python payments_producer.py



Run Airflow:


airflow scheduler
airflow webserver



Check DAGs: bronze_to_silver (hourly), silver_to_gold (daily).


Generate Reports:


python export_reports.py --report settlement
python export_reports.py --report fraud



dbt:


cd payments_dbt
dbt run
dbt docs generate

Recovery


Re-run DAGs if a failure occurs. Airflow retries and failure alerts are configured.


Backfill data if needed using airflow dags backfill <dag_id>.


6. Outputs


Settlement Report CSV: Summarized per merchant, currency, and fees.


Fraud Alerts CSV: List of flagged transactions.


7. Architecture Diagram
![alt text](image.png)