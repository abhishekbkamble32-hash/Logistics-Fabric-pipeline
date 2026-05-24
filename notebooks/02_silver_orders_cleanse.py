# Fabric Notebook
# Project     : Logistics Domain Data Platform
# Layer       : Silver — Orders Domain Cleanse
# Description : Deduplication, status code standardisation, mandatory field
#               validation, SLA breach flagging into at-risk table.
#               at-risk table feeds directly into Operations Power BI dashboard.
# Author      : Abhishek Kamble
# Note        : Resource names are placeholders. Replace with actual values.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, datetime

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

bronze_orders_path  = spark.conf.get("pipeline.bronze_orders_path",  "abfss://bronze@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/orders/")
silver_orders_path  = spark.conf.get("pipeline.silver_orders_path",  "abfss://silver@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/orders/")
at_risk_path        = spark.conf.get("pipeline.at_risk_path",        "abfss://silver@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/orders_at_risk/")
quarantine_path     = spark.conf.get("pipeline.quarantine_path",     "abfss://silver@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/_quarantine/orders/")
load_date           = spark.conf.get("pipeline.load_date",           str(date.today()))

SLA_HOURS = 48    # Orders older than this without delivery = at-risk

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read Bronze orders

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_orders_path)
print(f"Bronze records: {df_bronze.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Schema Validation

# COMMAND ----------

mandatory_cols = ["order_id", "customer_id", "product_id", "order_placed_at", "order_status", "warehouse_id"]

valid_condition = " AND ".join([f"{c} IS NOT NULL" for c in mandatory_cols])

df_valid      = df_bronze.filter(valid_condition)
df_quarantine = df_bronze.filter(f"NOT ({valid_condition})")

print(f"Valid: {df_valid.count()} | Quarantine: {df_quarantine.count()}")

if df_quarantine.count() > 0:
    df_quarantine \
        .withColumn("quarantine_reason", F.lit("missing_mandatory_field")) \
        .withColumn("quarantine_ts", F.current_timestamp()) \
        .write.format("delta").mode("append").save(quarantine_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Deduplication — order_id + event_timestamp

# COMMAND ----------

window_dedup = Window.partitionBy("order_id").orderBy(F.col("event_timestamp").desc())

df_deduped = (
    df_valid
    .withColumn("rn", F.row_number().over(window_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"After dedup: {df_deduped.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Status Code Standardisation
# MAGIC Source had multiple variants for the same status.
# MAGIC Mapped to one standard vocabulary.

# COMMAND ----------

status_map = {
    # SHIPPED variants
    "SHIPPED":   "SHIPPED",
    "SHP":       "SHIPPED",
    "S":         "SHIPPED",
    "DISPATCHED":"SHIPPED",
    # DELIVERED variants
    "DELIVERED": "DELIVERED",
    "DEL":       "DELIVERED",
    "D":         "DELIVERED",
    # IN_TRANSIT variants
    "IN_TRANSIT":"IN_TRANSIT",
    "INTRANSIT": "IN_TRANSIT",
    "IT":        "IN_TRANSIT",
    "IN TRANSIT":"IN_TRANSIT",
    # PENDING
    "PENDING":   "PENDING",
    "PND":       "PENDING",
    "P":         "PENDING",
    # CANCELLED
    "CANCELLED": "CANCELLED",
    "CANCELED":  "CANCELLED",
    "CAN":       "CANCELLED",
}

# Build a mapping expression
status_map_expr = F.create_map([F.lit(x) for pair in status_map.items() for x in pair])

df_standardised = (
    df_deduped
    .withColumn(
        "order_status_std",
        F.coalesce(status_map_expr[F.upper(F.col("order_status"))], F.lit("UNKNOWN"))
    )
    .withColumn("order_placed_at",   F.to_timestamp(F.col("order_placed_at")))
    .withColumn("expected_delivery", F.to_timestamp(F.col("expected_delivery")))
    .withColumn("load_date",         F.lit(load_date))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. SLA Breach Flagging
# MAGIC Orders where:
# MAGIC   - Status is NOT DELIVERED or CANCELLED
# MAGIC   - Hours since order_placed_at > SLA_HOURS threshold
# MAGIC These feed directly into the Operations dashboard at-risk table.

# COMMAND ----------

df_with_sla = df_standardised.withColumn(
    "hours_since_order",
    (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("order_placed_at"))) / 3600
)

df_at_risk = df_with_sla.filter(
    (F.col("order_status_std").isin(["PENDING", "IN_TRANSIT", "SHIPPED"]))
    & (F.col("hours_since_order") > SLA_HOURS)
).withColumn("sla_breach_flag", F.lit(True)) \
 .withColumn("at_risk_logged_at", F.current_timestamp())

print(f"At-risk orders: {df_at_risk.count()}")

# Write at-risk orders to separate table for Operations dashboard
(
    df_at_risk
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"load_date = '{load_date}'")
    .save(at_risk_path)
)

# Add sla_breach_flag to main silver orders
df_silver = df_with_sla.withColumn(
    "sla_breach_flag",
    (F.col("order_status_std").isin(["PENDING", "IN_TRANSIT", "SHIPPED"]))
    & (F.col("hours_since_order") > SLA_HOURS)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to Silver

# COMMAND ----------

(
    df_silver
    .drop("hours_since_order")
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"load_date = '{load_date}'")
    .option("mergeSchema", "true")
    .partitionBy("load_date")
    .save(silver_orders_path)
)

print(f"Silver orders written: {df_silver.count()} rows, load_date={load_date}")
print(f"At-risk orders flagged: {df_at_risk.count()}")
