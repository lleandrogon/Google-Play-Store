# Databricks notebook source
origin_path = "/Volumes/google/play_store/resource/origin"
bronze_path = "/Volumes/google/play_store/resource/ETL/bronze"

# COMMAND ----------

df_apps = spark.read.csv(f"{origin_path}/apps.csv",
                            header = True,
                            inferSchema = True,
                            sep = ",", 
                            multiLine = True, 
                            escape = '"',
                            quote = '"'
                        )

# COMMAND ----------

for c in df_apps.columns:
    df_apps = df_apps.withColumnRenamed(c, c.replace(" ", "_"))

# COMMAND ----------

df_apps.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{bronze_path}/apps")