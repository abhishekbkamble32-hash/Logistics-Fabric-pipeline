# Fabric Notebook
# Project     : Logistics Domain Data Platform
# Layer       : Silver — Shipments Domain Cleanse
# Description : Parses multi-event shipment records into one clean row per event.
#               Joins with Carrier Master for enrichment.
#               Calculates transit_days = delivery_ts - pickup_ts.
# Author      : Abhishek Kamble
# Note        : Resource names are placeholders. Replace with actual values.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

bronze_shipments_path = spark.conf.get("pipeline.bronze_shipments_path", "abfss://bronze@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/shipment_tracking/")
silver_carrier_path   = spark.conf.get("pipeline.silver_carrier_path",   "abfss://silver@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/carrier_master/")
silver_shipments_path = spark.conf.get("pipeline.silver_shipments_path", "abfss://silver@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/shipments/")
load_date             = spark.conf.get("pipeline.load_date",              str(date.today()))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read Bronze shipments
# MAGIC Source schema: one row per tracking event
# MAGIC (shipment_id, event_type, event_timestamp, location, carrier_id, ...)

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_shipments_path)
print(f"Bronze shipment records: {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Parse multi-event records
# MAGIC Each shipment_id has multiple rows (one per event_type).
# MAGIC Parse into one clean row per event and enrich with timestamps.

# COMMAND ----------

# Normalise event types
event_type_map = F.create_map(
    F.lit("PICKED_UP"),    F.lit("PICKED_UP"),
    F.lit("PICKUP"),       F.lit("PICKED_UP"),
    F.lit("IN_TRANSIT"),   F.lit("IN_TRANSIT"),
    F.lit("INTRANSIT"),    F.lit("IN_TRANSIT"),
    F.lit("OUT_FOR_DEL"),  F.lit("OUT_FOR_DELIVERY"),
    F.lit("DELIVERED"),    F.lit("DELIVERED"),
    F.lit("DEL"),          F.lit("DELIVERED"),
    F.lit("FAILED_DEL"),   F.lit("FAILED_DELIVERY"),
)

df_events = df_bronze.withColumn(
    "event_type_std",
    F.coalesce(event_type_map[F.upper(F.col("event_type"))], F.lit("UNKNOWN"))
).withColumn(
    "event_timestamp", F.to_timestamp(F.col("event_timestamp"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Extract pickup and delivery timestamps per shipment
# MAGIC Used to calculate transit_days

# COMMAND ----------

df_pickup = (
    df_events
    .filter(F.col("event_type_std") == "PICKED_UP")
    .groupBy("shipment_id")
    .agg(F.min("event_timestamp").alias("pickup_ts"))
)

df_delivery = (
    df_events
    .filter(F.col("event_type_std") == "DELIVERED")
    .groupBy("shipment_id")
    .agg(F.max("event_timestamp").alias("delivery_ts"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Join with Carrier Master for enrichment

# COMMAND ----------

df_carrier = (
    spark.read.format("delta").load(silver_carrier_path)
    .select("carrier_id", "carrier_name", "carrier_type", "region")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Build clean Silver shipments — one row per event

# COMMAND ----------

df_silver_shipments = (
    df_events
    .join(df_pickup,   on="shipment_id", how="left")
    .join(df_delivery, on="shipment_id", how="left")
    .join(df_carrier,  on="carrier_id",  how="left")
    # Calculate transit_days
    .withColumn(
        "transit_days",
        F.when(
            F.col("delivery_ts").isNotNull() & F.col("pickup_ts").isNotNull(),
            F.round(
                (F.unix_timestamp("delivery_ts") - F.unix_timestamp("pickup_ts")) / 86400,
                2
            )
        ).otherwise(F.lit(None))
    )
    # Flag SLA breach (example: SLA = 5 days)
    .withColumn(
        "sla_breached",
        F.when(F.col("transit_days") > 5, F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn("load_date", F.lit(load_date))
    .select(
        "shipment_id",
        "order_id",
        "carrier_id",
        "carrier_name",
        "carrier_type",
        "region",
        "event_type_std",
        "event_timestamp",
        "location",
        "pickup_ts",
        "delivery_ts",
        "transit_days",
        "sla_breached",
        "load_date"
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to Silver — mergeSchema=true + schema version log alert

# COMMAND ----------

# Check for new columns vs previous run (schema evolution tracking)
try:
    df_existing = spark.read.format("delta").load(silver_shipments_path)
    existing_cols = set(df_existing.columns)
    new_cols = set(df_silver_shipments.columns) - existing_cols
    if new_cols:
        print(f"SCHEMA EVOLUTION ALERT: New columns detected: {new_cols}")
        # In production: trigger Slack alert here
except Exception:
    print("First write — no existing Silver shipments table.")

(
    df_silver_shipments
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"load_date = '{load_date}'")
    .option("mergeSchema", "true")
    .partitionBy("load_date")
    .save(silver_shipments_path)
)

print(f"Silver shipments written: {df_silver_shipments.count()} rows")
print(f"Delivered shipments: {df_silver_shipments.filter(F.col('event_type_std')=='DELIVERED').count()}")
print(f"SLA breaches: {df_silver_shipments.filter(F.col('sla_breached')==True).count()}")
