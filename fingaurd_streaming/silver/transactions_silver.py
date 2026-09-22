from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
)

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), True),
])

@dp.table(
    name="fingaurd.silver.slv_transactions"
    , comment = "Parsed and cleaned transactions data"
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table("fingaurd.bronze.brz_transactions")

    transformed_df = (
        bronze_df.select(
            F.from_json(F.col("value"), transaction_schema).alias("data"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestmp"),
            F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
            )
        ).select(
            F.col("data.*"),
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestmp"),
            F.col("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp")
        )
    return transformed_df