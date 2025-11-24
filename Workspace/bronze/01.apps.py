# Databricks notebook source
display(dbutils.fs.ls("/Volumes/google/play_store/resource/origin/"))

# COMMAND ----------

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

display(df_apps.limit(10))

# COMMAND ----------

for c in df_apps.columns:
    df_apps = df_apps.withColumnRenamed(c, c.replace(" ", "_"))

# COMMAND ----------

df_apps.write.mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save(f"{bronze_path}/apps")