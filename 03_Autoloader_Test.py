# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

source_data = '/Volumes/fingaurd/source/fraud_watchlist/source_data/'

# COMMAND ----------

input_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/fingaurd/source/fraud_watchlist/schema/")
    .option("cloudFiles.inferColumnTypes", "True")
    .load(source_data)
)

# COMMAND ----------

transformed_df = input_stream.select(
    "*", 
    F.col("_metadata.file_path").alias("file_path"),
    F.current_timestamp().alias("ingestion_timestamp"),
)

# COMMAND ----------

streaming_query = (
    transformed_df.writeStream.format("delta")
    .outputMode("Append")
    .option("checkpointLocation", "/Volumes/fingaurd/source/fraud_watchlist/checkpoint/")
    .trigger(availableNow=True)
    .toTable("fingaurd.bronze.fraud_watchlist_test")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fingaurd.bronze.fraud_watchlist_test