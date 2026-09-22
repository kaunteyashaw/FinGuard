from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
import json
# Joining Streaming Table with Static Table
@dp.table(
    name="fingaurd.gold.high_val_txn_alert"
    , comment = "smms"
)
def high_val_txn_alert() -> DataFrame:
    txns = spark.readStream.table("fingaurd.silver.slv_transactions")
    customers = spark.read.table("fingaurd.silver.slv_customers")

    joined_df = (txns.join(customers, txns.customer_id == customers.customer_id, "left")\
                    .filter(F.col("amount")>F.col("transaction_limit"))\
                    .select(
                        F.concat_ws("-", F.lit("ALERT"), F.col("transaction_id")).alias("alert_id"),
                        F.lit("HIGH_VAL_TXN").alias("alert_type"),
                        F.current_timestamp().alias("alert_timestamp"),
                        txns.transaction_id, 
                        txns.customer_id,
                        customers.email.alias("customer_email"),
                        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
                        txns.amount.alias("transaction_amount"),
                        customers.transaction_limit.alias("transaction_limit"),
                        txns.currency,
                        txns.merchant_name,
                        txns.merchant_category, 
                        txns.is_international,
                        txns.transaction_type,
                        txns.payment_channel,
                        txns.city,
                        txns.country,
                        txns.transaction_timestamp,
                        txns.status

                    )
                )
    return joined_df
    


    

