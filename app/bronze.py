from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, current_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

# 1️⃣ Spark session
spark = (SparkSession.builder
         .appName("Capstone Real-Time Payments")
         .getOrCreate())

kafka_bootstrap = "kafka:9092"
source_topic = "payments.raw"
deadletter_topic = "payments.deadletter"

# 2️⃣ Define schema
schema = StructType([
    StructField("transition_id", StringType(), True),
    StructField("ts_event", TimestampType(), True),
    StructField("card_hash", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("mcc", IntegerType(), True),
    StructField("channel", StringType(), True),
    StructField("auth_result", StringType(), True),
    StructField("location", StringType(), True)
])

# 3️⃣ Kafka streaming read
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", kafka_bootstrap)
       .option("subscribe", source_topic)
       .option("startingOffsets", "latest")
       .load())

json_df = (raw.selectExpr("CAST(value AS STRING) AS json_str")
           .select(from_json(col("json_str"), schema).alias("data"))
           .select("data.*"))

# 4️⃣ Fraud rules
# Example: high amount (>900), blacklisted merchants, velocity check (simplified)
blacklisted_merchants = ["m999", "m888"]  # sample list

validated_df = json_df.withColumn(
    "is_valid",
    when(col("amount").isNull() | (col("amount") <= 0), lit(False))
    .when(col("currency").isNull(), lit(False))
    .when(col("auth_result").isNull(), lit(False))
    .when(col("amount") > 900, lit(False))  # high amount fraud
    .when(col("merchant_id").isin(blacklisted_merchants), lit(False))  # blacklisted merchant
    .otherwise(lit(True))
)

# 5️⃣ Split valid / invalid
valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid")
invalid_df = validated_df.filter(col("is_valid") == False).drop("is_valid")

# 6️⃣ Write invalid events to Kafka deadletter
invalid_to_kafka = (invalid_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("topic", deadletter_topic)
    .option("checkpointLocation", "/opt/app/checkpoints/deadletter_ckpt")
    .start())

# 7️⃣ Write valid events to Bronze layer in Parquet, partitioned by date
valid_to_parquet = (valid_df
    .withColumn("date", current_date())
    .writeStream
    .format("parquet")
    .option("path", "/opt/app/bronze/payments/")
    .option("checkpointLocation", "/opt/app/checkpoints/bronze_ckpt")
    .partitionBy("date")
    .start())

spark.streams.awaitAnyTermination()
