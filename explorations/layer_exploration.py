# Databricks notebook source
# 1. ดึงค่าเฉพาะตัวแปรที่เราตั้งไว้ใน YAML
bronze_name = spark.conf.get("bronze_schema")
print(f"ชื่อ Schema ของชั้น Bronze คือ: {bronze_name}")

# COMMAND ----------

bronze = spark.sql("SELECT * FROM biap.default.bronze_nyc_taxi_trips LIMIT 10")
display(bronze)

# COMMAND ----------

silver = spark.sql("SELECT * FROM biap.default.silver_nyc_taxi_trips LIMIT 10")
display(silver)


# COMMAND ----------

gold = spark.sql("SELECT * FROM biap.default.dayofweek LIMIT 10")
display(gold)

