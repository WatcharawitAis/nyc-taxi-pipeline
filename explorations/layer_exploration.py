# Databricks notebook source
# 1. ดึงค่าเฉพาะตัวแปรที่เราตั้งไว้ใน YAML
bronze_name = spark.conf.get("bronze_schema")
print(f"ชื่อ Schema ของชั้น Bronze คือ: {bronze_name}")

# COMMAND ----------

bronze = spark.sql("SELECT * FROM biap.default.bronze_nyc_taxi_trips LIMIT 10")
display(bronze)

# COMMAND ----------

from pyspark.sql.functions import size
silver = spark.sql("SELECT * FROM biap_dev.silver.silver_nyc_taxi_trips")
silver = silver.where((size(silver["_errors"]) > 0) | (size(silver["_warnings"]) > 0))
display(silver)

# COMMAND ----------

df.select("_errors", "_warnings").show(5, truncate=False)
df.printSchema()

# COMMAND ----------

gold = spark.sql("SELECT * FROM biap_dev.gold.day_of_week_metrics LIMIT 10")
display(gold)

