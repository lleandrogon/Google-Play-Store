# Databricks notebook source
from pyspark.sql.functions import col, when, round
from pyspark.sql.types import DoubleType

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/google/play_store/resource/ETL/bronze"))

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

display(df.limit(15))

# COMMAND ----------

for c in df.columns:
    df = df.withColumnRenamed(c, c.lower())

df.printSchema()

# COMMAND ----------

display(df.where(col("translated_review").isNull()))

# COMMAND ----------

df = df.dropna(subset = ["translated_review"])

# COMMAND ----------

display(df.where(col("translated_review").isNull()))

# COMMAND ----------

df = df.replace("nan", None, subset = ["translated_review", "sentiment", "sentiment_polarity", "sentiment_subjectivity"])

df = df.dropna(subset = ["translated_review", "sentiment", "sentiment_polarity", "sentiment_subjectivity"])

# COMMAND ----------

display(
    df.where(
        (col("translated_review") == None) |
        (col("sentiment") == None) |
        (col("sentiment_polarity") == None) |
        (col("sentiment_subjectivity") == None)
    )
)

# COMMAND ----------

display(
    df.where(
        (col("translated_review") == "nan") |
        (col("sentiment") == "nan") |
        (col("sentiment_polarity") == "nan") |
        (col("sentiment_subjectivity") == "nan")
    )
)

# COMMAND ----------

display(df.limit(15))

# COMMAND ----------

display(df.select("sentiment").distinct())

# COMMAND ----------

display(df.select("sentiment_polarity").distinct())

# COMMAND ----------

df = df.withColumn(
    "sentiment_polarity",
    col("sentiment_polarity").try_cast(DoubleType())
)

display(df.select("sentiment_polarity").distinct().limit(50))

# COMMAND ----------

display(df.where(col("sentiment_polarity").isNull()).limit(50))

# COMMAND ----------

display(df.select("sentiment_subjectivity").distinct().limit(50))

# COMMAND ----------

df = df.withColumn(
    "sentiment_subjectivity",
    col("sentiment_subjectivity").try_cast(DoubleType())
)

display(df.limit(50))

# COMMAND ----------

df = df.withColumn(
    "sentiment_polarity",
    round(col("sentiment_polarity"), 1)
)

df = df.withColumn(
    "sentiment_subjectivity",
    round(col("sentiment_subjectivity"), 1)
)

display(df.limit(50))

# COMMAND ----------

df = df.withColumn(
    "sentiment_type",
    when(col("sentiment_polarity") <= 0, "bad") \
    .when(col("sentiment_polarity") <= 0.5, "neutral") \
    .when(col("sentiment_polarity") <= 1, "good") \
    .otherwise("unknown")
)

display(df.limit(50))

# COMMAND ----------

df = df.withColumn(
    "subjectivity_level",
    when(col("sentiment_subjectivity") <= 0.3, "low") \
    .when(col("sentiment_subjectivity") <= 0.7, "medium") \
    .when(col("sentiment_subjectivity") <= 1, "high") \
    .otherwise("unknown")
)

display(df.limit(50))

# COMMAND ----------

display(df.limit(100))

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{silver_path}/user_reviews")

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("google.play_store.silver_user_reviews")