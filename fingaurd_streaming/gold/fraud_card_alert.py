from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
import json
# Joining Streaming Table with Static Table
@dp.table(
    name="fingaurd.gold.fraud_card_alert"
    , comment = "smms"
)
def high_val_txn_alert() -> DataFrame:
    txns = spark.readStream.table("fingaurd.silver.slv_transactions")
    txns = txns.withColumn("transaction_timestamp", F.to_timestamp("transaction_timestamp"))
    fraud_watchlist=spark.readStream.table("fingaurd.silver.slv_fraud_watchlist")
    customers = spark.read.table("fingaurd.silver.slv_customers")

    txns_with_watermark = txns.withWatermark("transaction_timestamp", "5 minutes")
    fraud_watchlist_with_watermark = fraud_watchlist.withWatermark("effective_from", "5 minutes")

    fraud_detected = (
        txns_with_watermark
        .join(fraud_watchlist_with_watermark, txns_with_watermark.card_number == fraud_watchlist_with_watermark.entity_id, "inner")
        .join(customers, txns_with_watermark.customer_id == customers.customer_id, "left")
        .select(
            F.concat_ws("-", F.lit("FRAUD"), F.col("transaction_id"), F.col("watchlist_id")).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),
            # Transaction details
            txns_with_watermark.transaction_id,
            txns_with_watermark.customer_id,
            customers.email.alias("customer_email"),
            F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
            txns_with_watermark.card_number,
            txns_with_watermark.amount,
            txns_with_watermark.currency,
            txns_with_watermark.merchant_id,
            txns_with_watermark.merchant_name,
            txns_with_watermark.merchant_category,
            txns_with_watermark.transaction_type,
            txns_with_watermark.payment_channel,
            txns_with_watermark.device_id,
            txns_with_watermark.city.alias("transaction_city"),
            txns_with_watermark.country.alias("transaction_country"),
            txns_with_watermark.transaction_timestamp,
            txns_with_watermark.is_international,
            txns_with_watermark.status.alias("transaction_status"),
            # Fraud watchlist details
            F.col("watchlist_id"),
            F.col("watch_type"),
            F.col("risk_level"),
            F.col("action"),
            F.col("reason_code"),
            F.col("effective_from").alias("watchlist_effective_from"),
            F.col("reported_by"),
            F.col("reported_source"),
            fraud_watchlist_with_watermark.city.alias("watchlist_city"),
            fraud_watchlist_with_watermark.country.alias("watchlist_country")
        
        
        )

    )
    return fraud_detected