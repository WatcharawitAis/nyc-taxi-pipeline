# Databricks notebook source
bronze = spark.sql("SELECT * FROM biap.default.bronze_nyc_taxi_trips LIMIT 10")
display(bronze)

# COMMAND ----------

silver = spark.sql("SELECT * FROM biap.default.silver_nyc_taxi_trips LIMIT 10")
display(silver)


# COMMAND ----------

gold = spark.sql("SELECT * FROM biap.default.dayofweek LIMIT 10")
display(gold)
