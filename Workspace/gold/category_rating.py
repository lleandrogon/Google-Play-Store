# Databricks notebook source
from pyspark.sql.functions import avg, round, desc

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/google/play_store/resource/ETL/silver"))

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

display(
    apps.select("category", "rating") \
    .groupby("category") \
    .agg(
        round(avg("rating"), 2).alias("avg_rating")
    ) \
    .orderBy(desc("avg_rating"))
)

# COMMAND ----------

df = apps.select("category", "rating") \
    .groupby("category") \
    .agg(
        round(avg("rating"), 2).alias("avg_rating")
    ) \
    .orderBy(desc("avg_rating"))

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{gold_path}/category_rating")

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("google.play_store.gold_category_rating")