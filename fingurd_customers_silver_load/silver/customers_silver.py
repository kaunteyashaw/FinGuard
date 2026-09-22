# Silver Load of Cutomer tables

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F



@dp.table(
    name="fingaurd.silver.slv_customers",
    comment = "Parsed and cleaned Customer data"
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers_silver() -> DataFrame:
    brz_df = spark.readStream.table("fingaurd.bronze.customers")

    silver_df = (
        brz_df
        .withColumn("account_open_date", F.to_date(F.col("account_open_date"), "yyyy-MM-dd"))
        .withColumn("silver_ingestion_date", F.current_timestamp())
    )

    return silver_df