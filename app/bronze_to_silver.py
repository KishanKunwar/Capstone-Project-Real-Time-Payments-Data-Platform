from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("Bronze to Silver Payments") \
    .getOrCreate()

# Read Bronze layer
bronze_path = "/opt/app/bronze/payments/"
bronze_df = spark.read.parquet(bronze_path)

#  Deduplicate
silver_df = bronze_df.dropDuplicates(["transition_id"])

# Standardize data types
silver_df = silver_df.withColumn("amount", col("amount").cast(DoubleType()))

#  Enforce ISO currency & clean invalid fields
valid_currencies = ["USD", "EUR", "GBP", "CAD"]  # Add more as needed
silver_df = silver_df.filter(col("currency").isin(valid_currencies))

#  Add processing timestamp
silver_df = silver_df.withColumn("processed_ts", current_timestamp())

#  Write Silver layer
silver_path = "/opt/app/silver/payments/"
silver_df.write.mode("overwrite").parquet(silver_path)
