from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum as spark_sum

spark = SparkSession.builder \
    .appName("Silver to Gold Payments") \
    .getOrCreate()

silver_path = "/opt/app/silver/payments/"
silver_df = spark.read.parquet(silver_path)

jdbc_url = "jdbc:postgresql://postgres:5432/retail"
jdbc_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

# dim_date
dim_date = silver_df.select(to_date("ts_event").alias("date")).distinct()
dim_date.write.jdbc(jdbc_url, "dim_date", mode="overwrite", properties=jdbc_props)

# dim_card
dim_card = silver_df.select("card_hash", "channel").distinct()
dim_card.write.jdbc(jdbc_url, "dim_card", mode="overwrite", properties=jdbc_props)

# dim_merchant
dim_merchant = silver_df.select("merchant_id", "mcc", "location").distinct()
dim_merchant.write.jdbc(jdbc_url, "dim_merchant", mode="overwrite", properties=jdbc_props)

#  fact_transactions
fact_transactions = silver_df.select(
    "transition_id", "ts_event", "card_hash", "merchant_id", "amount", "currency"
)
fact_transactions.write.jdbc(jdbc_url, "fact_transactions", mode="append", properties=jdbc_props)

#  fact_settlement_daily (example aggregation)
fact_settlement_daily = silver_df.groupBy(to_date("ts_event").alias("date"), "merchant_id") \
    .agg(
        count("transition_id").alias("total_transactions"),
        spark_sum("amount").alias("total_amount")
    )
fact_settlement_daily.write.jdbc(jdbc_url, "fact_settlement_daily", mode="append", properties=jdbc_props)

#  fact_fraud_signals (example: amount > 900)
fact_fraud_signals = silver_df.filter(col("amount") > 900) \
    .select("transition_id", "ts_event", "card_hash", "merchant_id", "amount")
fact_fraud_signals.write.jdbc(jdbc_url, "fact_fraud_signals", mode="append", properties=jdbc_props)
