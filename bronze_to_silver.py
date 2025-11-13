from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("Bronze to Silver Payments") \
    .getOrCreate()

# 1️⃣ Read Bronze layer
bronze_path = "/opt/app/bronze/payments/"
bronze_df = spark.read.parquet(bronze_path)

# 2️⃣ Deduplicate
silver_df = bronze_df.dropDuplicates(["transition_id"])

# 3️⃣ Standardize data types
silver_df = silver_df.withColumn("amount", col("amount").cast(DoubleType()))

# 4️⃣ Enforce ISO currency & clean invalid fields
valid_currencies = ["USD", "EUR", "GBP", "CAD"]  # Add more as needed
silver_df = silver_df.filter(col("currency").isin(valid_currencies))

# 5️⃣ Add processing timestamp
silver_df = silver_df.withColumn("processed_ts", current_timestamp())

# 6️⃣ Write Silver layer
silver_path = "/opt/app/silver/payments/"
silver_df.write.mode("overwrite").parquet(silver_path)
