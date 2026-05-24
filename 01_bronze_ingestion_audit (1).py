# Databricks notebook source (Fabric Notebook)
# Project     : Logistics Domain Data Platform
# Layer       : Bronze — Raw Ingestion + Audit Columns
# Description : Reads raw files/API data landed by Fabric Pipelines,
#               adds standard audit columns, writes to Fabric Lakehouse
#               Bronze in Delta format (append-only).
#               Parameterized — one notebook handles all 5 sources.
# Author      : Abhishek Kamble
# Note        : Resource names are placeholders. Replace with actual values.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters — passed from Fabric Pipeline

# COMMAND ----------

# In Microsoft Fabric, parameters are passed via pipeline variables
# or notebook parameters widget

source_system   = spark.conf.get("pipeline.source_system",   "orders")
pipeline_run_id = spark.conf.get("pipeline.run_id",          "manual_run")
bronze_base     = spark.conf.get("pipeline.bronze_base",     "abfss://bronze@<onelake-workspace>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/")
raw_base        = spark.conf.get("pipeline.raw_base",        "abfss://raw@<onelake-workspace>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Files/")

ingestion_ts    = datetime.utcnow().isoformat()
bronze_path     = bronze_base + source_system
raw_path        = raw_base    + source_system

print(f"Source        : {source_system}")
print(f"Pipeline Run  : {pipeline_run_id}")
print(f"Ingestion TS  : {ingestion_ts}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read raw files (format depends on source_system config)

# COMMAND ----------

# Format mapping per source — in production this comes from a config dict or control table
source_format_map = {
    "orders":             "parquet",   # REST API output via Fabric Pipeline
    "shipment_tracking":  "csv",       # flat file daily drops
    "warehouse_inventory":"parquet",   # Azure SQL via Fabric Pipeline
    "carrier_master":     "parquet",   # Azure SQL weekly full load
    "master_data":        "csv"        # CSV weekly
}

read_format = source_format_map.get(source_system, "parquet")

df_raw = (
    spark.read
    .format(read_format)
    .option("header", "true")         # for CSV sources
    .option("inferSchema", "true")
    .load(raw_path)
)

print(f"Records read: {df_raw.count()}")
df_raw.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Add standard audit columns

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    .withColumn("source_system",       F.lit(source_system))
    .withColumn("pipeline_run_id",     F.lit(pipeline_run_id))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Fabric Lakehouse Bronze — Delta, append-only
# MAGIC No transformation in Bronze. Raw as received + audit columns only.

# COMMAND ----------

(
    df_bronze
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "false")
    .partitionBy("source_system")
    .save(bronze_path)
)

print(f"Bronze write complete: source={source_system}, records={df_bronze.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Audit log

# COMMAND ----------

audit_row = spark.createDataFrame([{
    "source_system":       source_system,
    "pipeline_run_id":     pipeline_run_id,
    "ingestion_timestamp": ingestion_ts,
    "records_written":     df_bronze.count(),
    "status":              "SUCCESS"
}])

(
    audit_row
    .write
    .format("delta")
    .mode("append")
    .save(bronze_base + "_audit_log/")
)

print("Audit log updated.")
