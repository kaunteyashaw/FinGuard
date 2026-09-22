# Databricks notebook source
# bootstrap_servers = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'
# api_key = '7GFJSYEZO3VQPJDP'
# api_secret = 'cfltn+XuV2Z+Sfmk3EbQLddBmlF6e9mLg5YMwi/DbhdHsgvLnwvgAyFzmZz9GDLg'
# topic = 'credit_card_transactions_topic'

# COMMAND ----------

kafka_connection_json = dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")

# COMMAND ----------

import json 
kafka_config=json.loads(kafka_connection_json)
bootstrap_servers=kafka_config['bootstrap_servers']
api_key=kafka_config['api_key']
api_secret=kafka_config['api_secret']
topic=kafka_config['topic']

# COMMAND ----------

# jaas_config="kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+api_key+"\" password=\""+api_secret+"\";"
jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sample_batch = spark.read.format("kafka")\
    .option("kafka.bootstrap.servers", bootstrap_servers)\
    .option("subscribe", topic)\
    .option("kafka.security.protocol", "SASL_SSL")\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("kafka.sasl.jaas.config", jaas_config)\
    .option("startingOffsets", "earliest")\
    .load()
    

# COMMAND ----------

sample_batch.count()

# COMMAND ----------

sample_batch.display()

# COMMAND ----------


parsed_batch= sample_batch.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)
parsed_batch.display()

# COMMAND ----------

parsed_batch.write.saveAsTable("fingaurd.bronze.transaction_batch_test")

# COMMAND ----------

streaming_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", bootstrap_servers)\
    .option("subscribe", topic)\
    .option("kafka.security.protocol", "SASL_SSL")\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("kafka.sasl.jaas.config", jaas_config)\
    .option("startingOffsets", "earliest")\
    .load()
    

# COMMAND ----------

parsed_streaming_df= streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)
# parsed_batch.display()



# COMMAND ----------

# writing to a streaming table

streaming_query = parsed_streaming_df.writeStream.format("delta").outputMode("append")\
    .option("checkpointLocation", "/Volumes/fingaurd/source/transactions/checkpoint/")\
    .trigger(availableNow=True)\
    .toTable("fingaurd.bronze.transaction_streaming_test")

print("QueryID=", streaming_query.id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fingaurd.bronze.transaction_streaming_test;