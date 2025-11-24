# Databricks notebook source
display(dbutils.fs.ls("/Volumes/google/play_store/resource/origin/"))

# COMMAND ----------

origin_path = "/Volumes/google/play_store/resource/origin"
bronze_path = "/Volumes/google/play_store/resource/ETL/bronze"

# COMMAND ----------

df_user_reviews = spark.read.csv(f"{origin_path}/user_reviews.csv", header = True, inferSchema = True, sep = ",")

# COMMAND ----------

display(df_user_reviews.limit(10))

# COMMAND ----------

df_user_reviews.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{bronze_path}/user_reviews")