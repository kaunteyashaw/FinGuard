# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `fingaurd`.`silver`.`customers` where customer_id='CUST000001'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fingaurd.bronze.slv_fraud_watchlist

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fingaurd.gold.fraud_card_alert