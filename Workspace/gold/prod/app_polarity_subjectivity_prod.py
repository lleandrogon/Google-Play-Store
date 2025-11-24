# Databricks notebook source
from pyspark.sql.functions import avg, round

# COMMAND ----------

silver_path = "/Volumes/google/play_store/resource/ETL/silver"
gold_path = "/Volumes/google/play_store/resource/ETL/gold"

# COMMAND ----------

silver_map = {
    "tmp_silver_apps": f"{silver_path}/apps/",
    "tmp_silver_user_reviews": f"{silver_path}/user_reviews/"
}

for tmp_table, path in silver_map.items():
    spark.read.format("delta").load(path).createOrReplaceTempView(tmp_table)

# COMMAND ----------

apps = spark.table("tmp_silver_apps")
user_reviews = spark.table("tmp_silver_user_reviews")

# COMMAND ----------

df = apps.join(user_reviews, on = "app", how = "inner") \
    .groupBy("app") \
    .agg(
        round(avg("sentiment_polarity"), 2).alias("avg_sentiment_polarity"),
        round(avg("sentiment_subjectivity"), 2).alias("avg_sentiment_subjectivity")
    )

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{gold_path}/app_polarity_subjectivity")