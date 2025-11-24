# Databricks notebook source
origin_path = "/Volumes/google/play_store/resource/origin"
bronze_path = "/Volumes/google/play_store/resource/ETL/bronze"

# COMMAND ----------

df_user_reviews = spark.read.csv(f"{origin_path}/user_reviews.csv", header = True, inferSchema = True, sep = ",")

# COMMAND ----------

df_user_reviews.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{bronze_path}/user_reviews")