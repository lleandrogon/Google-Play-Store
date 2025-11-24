# Databricks notebook source
from pyspark.sql.functions import col, when, round
from pyspark.sql.types import DoubleType

# COMMAND ----------

bronze_path = "/Volumes/google/play_store/resource/ETL/bronze"
silver_path = "/Volumes/google/play_store/resource/ETL/silver"

# COMMAND ----------

bronze_map = {
    "tmp_bronze_apps": f"{bronze_path}/apps/",
    "tmp_bronze_user_reviews": f"{bronze_path}/user_reviews/"
}

for tmp_table, path in bronze_map.items():
    spark.read.format("delta").load(path).createOrReplaceTempView(tmp_table)

# COMMAND ----------

df = spark.table("tmp_bronze_user_reviews")

# COMMAND ----------

for c in df.columns:
    df = df.withColumnRenamed(c, c.lower())

# COMMAND ----------

df = df.dropna(subset = ["translated_review"])

# COMMAND ----------

df = df.replace("nan", None, subset = ["translated_review", "sentiment", "sentiment_polarity", "sentiment_subjectivity"])

df = df.dropna(subset = ["translated_review", "sentiment", "sentiment_polarity", "sentiment_subjectivity"])

# COMMAND ----------

df = df.withColumn(
    "sentiment_polarity",
    col("sentiment_polarity").try_cast(DoubleType())
)

display(df.select("sentiment_polarity").distinct().limit(50))

# COMMAND ----------

df = df.withColumn(
    "sentiment_subjectivity",
    col("sentiment_subjectivity").try_cast(DoubleType())
)

# COMMAND ----------

df = df.withColumn(
    "sentiment_polarity",
    round(col("sentiment_polarity"), 1)
)

df = df.withColumn(
    "sentiment_subjectivity",
    round(col("sentiment_subjectivity"), 1)
)

# COMMAND ----------

df = df.withColumn(
    "sentiment_type",
    when(col("sentiment_polarity") <= 0, "bad") \
    .when(col("sentiment_polarity") <= 0.5, "neutral") \
    .when(col("sentiment_polarity") <= 1, "good") \
    .otherwise("unknown")
)

# COMMAND ----------

df = df.withColumn(
    "subjectivity_level",
    when(col("sentiment_subjectivity") <= 0.3, "low") \
    .when(col("sentiment_subjectivity") <= 0.7, "medium") \
    .when(col("sentiment_subjectivity") <= 1, "high") \
    .otherwise("unknown")
)

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{silver_path}/user_reviews")