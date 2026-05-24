# Fabric Notebook
# Project     : Logistics Domain Data Platform
# Layer       : Gold — Star Schema + KPI Tables into Fabric Warehouse
# Description : Builds fact_shipments, fact_orders, fact_inventory (daily snapshot).
#               Loads all dimension tables. Creates pre-aggregated KPI tables
#               used by 4 Power BI dashboards.
# Author      : Abhishek Kamble
# Note        : Resource names are placeholders. Replace with actual values.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import date
import pandas as pd

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

silver_base = spark.conf.get("pipeline.silver_base", "abfss://silver@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/")
gold_base   = spark.conf.get("pipeline.gold_base",   "abfss://gold@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/")
load_date   = spark.conf.get("pipeline.load_date",   str(date.today()))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read Silver tables

# COMMAND ----------

df_orders    = spark.read.format("delta").load(silver_base + "orders/")
df_shipments = spark.read.format("delta").load(silver_base + "shipments/")
df_inventory = spark.read.format("delta").load(silver_base + "inventory/")
df_customer  = spark.read.format("delta").load(silver_base + "dim_customer/").filter(F.col("is_current") == True)
df_product   = spark.read.format("delta").load(silver_base + "dim_product/").filter(F.col("is_current") == True)
df_carrier   = spark.read.format("delta").load(silver_base + "carrier_master/")
df_warehouse = spark.read.format("delta").load(silver_base + "dim_warehouse/")

print("All Silver tables loaded.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. dim_date

# COMMAND ----------

def build_dim_date(start="2020-01-01", end="2030-12-31"):
    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({
        "date_key":    dates.strftime("%Y%m%d").astype(int),
        "full_date":   dates.date,
        "year":        dates.year,
        "quarter":     dates.quarter,
        "month":       dates.month,
        "month_name":  dates.month_name(),
        "week":        dates.isocalendar().week.values,
        "day_of_week": dates.day_name(),
        "is_weekend":  dates.weekday >= 5
    })
    return spark.createDataFrame(df)

df_dim_date = build_dim_date()
df_dim_date.write.format("delta").mode("overwrite").save(gold_base + "dim_date/")
print("dim_date written.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. fact_shipments
# MAGIC One row per shipment event — carrier, origin, destination, SLA flag, transit_days

# COMMAND ----------

df_fact_shipments = (
    df_shipments
    .filter(f"load_date = '{load_date}'")
    .join(df_carrier.select("carrier_id", "carrier_name", "carrier_type", "region"), on="carrier_id", how="left")
    .join(df_product.select("product_id", "product_sk"), on="product_id", how="left")
    .join(df_customer.select("customer_id", "customer_sk"), on="customer_id", how="left")
    .withColumn("date_key", F.date_format(F.col("event_timestamp"), "yyyyMMdd").cast("int"))
    .select(
        "shipment_id",
        "order_id",
        "carrier_id",
        "carrier_name",
        "carrier_type",
        "region",
        "customer_sk",
        "product_sk",
        "date_key",
        "event_type_std",
        "event_timestamp",
        "pickup_ts",
        "delivery_ts",
        "transit_days",
        "sla_breached",
        "load_date"
    )
)

df_fact_shipments.write.format("delta").mode("overwrite") \
    .option("replaceWhere", f"load_date = '{load_date}'") \
    .partitionBy("load_date") \
    .save(gold_base + "fact_shipments/")

print(f"fact_shipments: {df_fact_shipments.count()} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. fact_orders
# MAGIC One row per order — customer, product, value, status

# COMMAND ----------

df_fact_orders = (
    df_orders
    .filter(f"load_date = '{load_date}'")
    .join(df_customer.select("customer_id", "customer_sk"),  on="customer_id",  how="left")
    .join(df_product.select("product_id", "product_sk"),     on="product_id",   how="left")
    .join(df_warehouse.select("warehouse_id", "warehouse_sk"), on="warehouse_id", how="left")
    .withColumn("date_key", F.date_format(F.col("order_placed_at"), "yyyyMMdd").cast("int"))
    .withColumn("order_value", F.col("quantity") * F.col("unit_price"))
    .select(
        "order_id",
        "customer_sk",
        "product_sk",
        "warehouse_sk",
        "date_key",
        "quantity",
        "unit_price",
        "order_value",
        "order_status_std",
        "sla_breach_flag",
        "load_date"
    )
)

df_fact_orders.write.format("delta").mode("overwrite") \
    .option("replaceWhere", f"load_date = '{load_date}'") \
    .partitionBy("load_date") \
    .save(gold_base + "fact_orders/")

print(f"fact_orders: {df_fact_orders.count()} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. fact_inventory — Daily Snapshot
# MAGIC Kept as snapshot (not current state) so stock trends are trackable over time.

# COMMAND ----------

df_fact_inventory = (
    df_inventory
    .filter(f"load_date = '{load_date}'")
    .join(df_product.select("product_id", "product_sk"),       on="product_id",   how="left")
    .join(df_warehouse.select("warehouse_id", "warehouse_sk"), on="warehouse_id", how="left")
    .withColumn("date_key", F.date_format(F.col("snapshot_date"), "yyyyMMdd").cast("int"))
    .withColumn("net_movement", F.col("units_received") - F.col("units_dispatched"))
    .withColumn("stock_vs_threshold", F.col("stock_on_hand") - F.col("reorder_threshold"))
    .withColumn("below_reorder", F.col("stock_on_hand") < F.col("reorder_threshold"))
    .select(
        "product_sk", "warehouse_sk", "date_key",
        "stock_on_hand", "reorder_threshold", "units_received", "units_dispatched",
        "net_movement", "stock_vs_threshold", "below_reorder", "load_date"
    )
)

df_fact_inventory.write.format("delta").mode("overwrite") \
    .option("replaceWhere", f"load_date = '{load_date}'") \
    .partitionBy("load_date") \
    .save(gold_base + "fact_inventory/")

print(f"fact_inventory: {df_fact_inventory.count()} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Pre-aggregated KPI Tables
# MAGIC Power BI dashboards hit these — not raw fact tables.

# COMMAND ----------

# KPI 1: Daily SLA compliance rate per carrier and region
df_kpi_sla = (
    df_fact_shipments
    .filter(F.col("event_type_std") == "DELIVERED")
    .join(df_dim_date.select("date_key", "year", "month"), on="date_key", how="left")
    .groupBy("year", "month", "carrier_id", "carrier_name", "region")
    .agg(
        F.count("shipment_id").alias("total_deliveries"),
        F.sum(F.when(F.col("sla_breached") == False, 1).otherwise(0)).alias("on_time_deliveries"),
        F.avg("transit_days").alias("avg_transit_days")
    )
    .withColumn("sla_compliance_rate",
        F.round(F.col("on_time_deliveries") / F.col("total_deliveries") * 100, 2))
)

df_kpi_sla.write.format("delta").mode("overwrite").save(gold_base + "kpi_sla_compliance/")

# KPI 2: Carrier performance scorecard
df_kpi_carrier = (
    df_kpi_sla
    .groupBy("carrier_id", "carrier_name")
    .agg(
        F.avg("sla_compliance_rate").alias("avg_on_time_rate"),
        F.avg("avg_transit_days").alias("avg_transit_days"),
        F.sum("total_deliveries").alias("total_deliveries")
    )
    .withColumn("performance_rank",
        F.rank().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.orderBy(F.col("avg_on_time_rate").desc())
        )
    )
)

df_kpi_carrier.write.format("delta").mode("overwrite").save(gold_base + "kpi_carrier_scorecard/")

# KPI 3: Warehouse stock health vs reorder threshold
df_kpi_stock = (
    df_fact_inventory
    .filter(f"load_date = '{load_date}'")
    .join(df_warehouse.select("warehouse_sk", "warehouse_name", "region"), on="warehouse_sk", how="left")
    .join(df_product.select("product_sk", "product_name", "category"), on="product_sk", how="left")
    .groupBy("warehouse_name", "region", "category")
    .agg(
        F.sum("stock_on_hand").alias("total_stock"),
        F.sum("reorder_threshold").alias("total_threshold"),
        F.sum(F.when(F.col("below_reorder") == True, 1).otherwise(0)).alias("sku_below_reorder")
    )
)

df_kpi_stock.write.format("delta").mode("overwrite").save(gold_base + "kpi_warehouse_stock/")

# KPI 4: Fulfilment rate by region
df_kpi_fulfilment = (
    df_fact_orders
    .join(df_warehouse.select("warehouse_sk", "region"), on="warehouse_sk", how="left")
    .join(df_dim_date.select("date_key", "year", "month"), on="date_key", how="left")
    .groupBy("year", "month", "region")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum(F.when(F.col("order_status_std") == "DELIVERED", 1).otherwise(0)).alias("fulfilled_orders"),
        F.sum("order_value").alias("total_order_value")
    )
    .withColumn("fulfilment_rate",
        F.round(F.col("fulfilled_orders") / F.col("total_orders") * 100, 2))
    .withColumn("revenue_at_risk",
        F.col("total_order_value") - 
        F.col("total_order_value") * (F.col("fulfilment_rate") / 100))
)

df_kpi_fulfilment.write.format("delta").mode("overwrite").save(gold_base + "kpi_fulfilment_rate/")

print("All 4 KPI tables written.")
print(f"load_date={load_date} Gold load complete.")
