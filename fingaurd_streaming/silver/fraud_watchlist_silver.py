from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql import functions as F


@dp.table(
    name="fingaurd.silver.slv_fraud_watchlist"
    , comment = "Silver stream data ingested"
)
def watchlist_silver() -> DataFrame:
    # source_data = '/Volumes/fingaurd/source/fraud_watchlist/source_data/'
    bronze_df = (
            spark.readStream.table("fingaurd.bronze.brz_fraud_watchlist")
   )
    

    cleaned_df = bronze_df.select(
    F.upper(F.col("watchlist_id")).alias("watchlist_id"),
    F.col("watch_type"),
    F.upper(F.col("entity_id")).alias("entity_id"),
    F.upper(F.col("risk_level")).alias("risk_level"),
    F.col("status"),
    F.upper(F.col("action")).alias("action"),
    F.col("reason_code"),
    F.to_timestamp(F.col("effective_from"), "dd-MM-yyyy HH:mm:ss").alias("effective_from"),
    F.col("reported_by"),
    F.col("reported_source"),
    F.col("country"),
    F.col("city"),
    F.col("source_file"),
    F.current_timestamp().alias("silver_ingestion_timestamp"),
    F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    
    )
    return cleaned_df


