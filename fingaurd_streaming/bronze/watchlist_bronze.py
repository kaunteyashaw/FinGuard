from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql import functions as F


@dp.table(
    name="fingaurd.bronze.brz_fraud_watchlist"
    , comment = "Watchlist raw stream data ingested from kafka"
)
def watchlist_bronze() -> DataFrame:
    source_data = '/Volumes/fingaurd/source/fraud_watchlist/source_data/'
    streaming_df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "True")
            .load(source_data)
   )
    

    parsed_streaming_df= streaming_df.select(
    F.col("watchlist_id"),
    F.col("watch_type"),
    F.col("entity_id"),
    F.col("risk_level"),
    F.col("status"),
    F.col("action"),
    F.col("reason_code"),
    F.col("effective_from"),
    F.col("reported_by"),
    F.col("reported_source"),
    F.col("country"),
    F.col("city"),
    F.col("_rescued_data"),
    F.col("_metadata.file_path").alias("source_file"),
    F.current_timestamp().alias("ingestion_timestamp")
    
    )
    return parsed_streaming_df


