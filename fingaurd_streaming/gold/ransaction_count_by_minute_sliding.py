from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
import json
# Joining Streaming Table with Static Table
@dp.table(
    name="fingaurd.gold.transaction_count_by_minute_sw"
    , comment = "n/a"
)
def transaction_count_by_minute() -> DataFrame:
    transaction_df = spark.readStream.table("fingaurd.silver.slv_transactions").withColumn("transaction_timestamp", F.col("transaction_timestamp").cast("timestamp"))

    transaction_with_watermark = transaction_df.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df = transaction_with_watermark.groupBy(F.window("transaction_timestamp", "5 minute", "1 minute")).agg(F.count("*").alias("transaction_count")).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )

    return transaction_count_df
    
