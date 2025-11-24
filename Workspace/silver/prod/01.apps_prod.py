# Databricks notebook source
from pyspark.sql.functions import col, lower, regexp_replace, when, to_date, expr
from pyspark.sql import functions as f

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

df = spark.table("tmp_bronze_apps")

# COMMAND ----------

df = df.drop("_c0")

# COMMAND ----------

for c in df.columns:
    df = df.withColumnRenamed(c, c.lower())

# COMMAND ----------

df = df.withColumn("price", regexp_replace(col("price"), "[$,]", ""))

# COMMAND ----------

df = df.withColumn("rating", col("rating").cast("double"))
df = df.withColumn("reviews", col("reviews").cast("int"))
df = df.withColumn("size", col("size").cast("double"))
df = df.withColumn("price", col("price").cast("double"))

# COMMAND ----------

df = df.withColumn("category", lower(col("category")))

# COMMAND ----------

df = df.withColumn(
    "aproximate_installs",
    regexp_replace(col("installs"), "[+,]", "")
)

df = df.withColumn(
    "aproximate_installs",
    when(col("aproximate_installs").rlike("^[0-9]+$"), col("aproximate_installs").cast("int")) \
    .otherwise(None)
)

# COMMAND ----------

df = df.withColumn(
    "last_updated",
    f.date_format(
        f.to_date("last_updated", "MMMM d, yyyy"),
        "yyyy-MM-dd"
    )
)

df = df.withColumn(
    "last_updated_date", 
    f.to_date("last_updated", "yyyy-MM-dd")
)

# COMMAND ----------

df = df.drop("last_updated")

# COMMAND ----------

df = df.withColumnRenamed("last_updated_date", "last_updated")

# COMMAND ----------

df.write.mode("overwrite") \
    .format("delta") \
    .option("inferSchema", "true") \
    .save(f"{silver_path}/apps")