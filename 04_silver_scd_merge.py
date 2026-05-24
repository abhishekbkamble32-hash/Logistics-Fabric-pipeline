# Fabric Notebook
# Project     : Logistics Domain Data Platform
# Layer       : Silver — SCD Type 2 MERGE (Customer + Product dimensions)
#               SCD Type 1 — Carrier Master (simple overwrite)
# Description : Delta MERGE implementation for dimension management.
#               Customer and Product use Type 2 (full history).
#               Carrier Master uses Type 1 (latest state only).
# Author      : Abhishek Kamble
# Note        : Resource names are placeholders. Replace with actual values.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

silver_base = spark.conf.get("pipeline.silver_base", "abfss://silver@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/")
bronze_base = spark.conf.get("pipeline.bronze_base", "abfss://bronze@<onelake>.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/")
load_date   = spark.conf.get("pipeline.load_date",   str(date.today()))

dim_customer_path = silver_base + "dim_customer/"
dim_product_path  = silver_base + "dim_product/"
carrier_path      = silver_base + "carrier_master/"

# COMMAND ----------
# MAGIC %md
# MAGIC ## ── SCD TYPE 2 — dim_customer ──────────────────────────────────
# MAGIC Tracked attributes: city, loyalty_tier
# MAGIC When these change → expire old record, insert new.

# COMMAND ----------

df_customer_src = (
    spark.read.format("delta").load(bronze_base + "master_data/")
    .filter(F.col("entity_type") == "customer")
)

# Deduplicate source on customer_id
window_spec = Window.partitionBy("customer_id").orderBy(F.col("ingestion_timestamp").desc())
df_customer_src = (
    df_customer_src
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select("customer_id", "customer_name", "city", "loyalty_tier", "email")
)

# ── Initialise if not exists ──
try:
    DeltaTable.forPath(spark, dim_customer_path)
    customer_exists = True
except Exception:
    customer_exists = False

if not customer_exists:
    df_init = df_customer_src \
        .withColumn("customer_sk", F.monotonically_increasing_id()) \
        .withColumn("start_date",  F.lit(load_date).cast("date")) \
        .withColumn("end_date",    F.lit(None).cast("date")) \
        .withColumn("is_current",  F.lit(True))
    df_init.write.format("delta").mode("overwrite").save(dim_customer_path)
    print(f"dim_customer initialised: {df_init.count()} rows")
else:
    delta_customer = DeltaTable.forPath(spark, dim_customer_path)
    tracked = ["city", "loyalty_tier"]
    change_cond = " OR ".join([f"tgt.{c} <> src.{c}" for c in tracked])

    # Pass 1 — expire changed records
    delta_customer.alias("tgt").merge(
        df_customer_src.alias("src"),
        "tgt.customer_id = src.customer_id AND tgt.is_current = true"
    ).whenMatchedUpdate(
        condition=change_cond,
        set={"is_current": F.lit(False), "end_date": F.lit(load_date).cast("date")}
    ).execute()

    # Pass 2 — insert new records for changed/new customers
    df_current = delta_customer.toDF().filter(F.col("is_current") == True)
    df_new = df_customer_src.join(df_current.select("customer_id"), on="customer_id", how="left_anti")
    df_expired = delta_customer.toDF() \
        .filter((F.col("is_current") == False) & (F.col("end_date") == load_date)) \
        .select("customer_id") \
        .join(df_customer_src, on="customer_id", how="inner")

    df_insert = df_new.union(df_expired).dropDuplicates(["customer_id"]) \
        .withColumn("customer_sk", F.monotonically_increasing_id()) \
        .withColumn("start_date",  F.lit(load_date).cast("date")) \
        .withColumn("end_date",    F.lit(None).cast("date")) \
        .withColumn("is_current",  F.lit(True))

    if df_insert.count() > 0:
        df_insert.write.format("delta").mode("append").save(dim_customer_path)
        print(f"dim_customer: {df_insert.count()} new/updated records inserted")
    else:
        print("dim_customer: no changes detected")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ── SCD TYPE 2 — dim_product ──────────────────────────────────
# MAGIC Tracked attributes: category, unit_price
# MAGIC Same two-pass MERGE pattern as dim_customer.

# COMMAND ----------

df_product_src = (
    spark.read.format("delta").load(bronze_base + "master_data/")
    .filter(F.col("entity_type") == "product")
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("product_id").orderBy(F.col("ingestion_timestamp").desc())
    ))
    .filter(F.col("rn") == 1).drop("rn")
    .select("product_id", "product_name", "category", "sub_category", "unit_price")
)

try:
    DeltaTable.forPath(spark, dim_product_path)
    product_exists = True
except Exception:
    product_exists = False

if not product_exists:
    df_product_src \
        .withColumn("product_sk", F.monotonically_increasing_id()) \
        .withColumn("start_date", F.lit(load_date).cast("date")) \
        .withColumn("end_date",   F.lit(None).cast("date")) \
        .withColumn("is_current", F.lit(True)) \
        .write.format("delta").mode("overwrite").save(dim_product_path)
    print("dim_product initialised")
else:
    delta_product = DeltaTable.forPath(spark, dim_product_path)
    product_tracked = ["category", "unit_price"]
    product_change_cond = " OR ".join([f"tgt.{c} <> src.{c}" for c in product_tracked])

    delta_product.alias("tgt").merge(
        df_product_src.alias("src"),
        "tgt.product_id = src.product_id AND tgt.is_current = true"
    ).whenMatchedUpdate(
        condition=product_change_cond,
        set={"is_current": F.lit(False), "end_date": F.lit(load_date).cast("date")}
    ).execute()

    df_product_current = delta_product.toDF().filter(F.col("is_current") == True)
    df_product_new = df_product_src.join(df_product_current.select("product_id"), on="product_id", how="left_anti")
    df_product_expired = delta_product.toDF() \
        .filter((F.col("is_current") == False) & (F.col("end_date") == load_date)) \
        .select("product_id").join(df_product_src, on="product_id", how="inner")

    df_product_insert = df_product_new.union(df_product_expired).dropDuplicates(["product_id"]) \
        .withColumn("product_sk", F.monotonically_increasing_id()) \
        .withColumn("start_date", F.lit(load_date).cast("date")) \
        .withColumn("end_date",   F.lit(None).cast("date")) \
        .withColumn("is_current", F.lit(True))

    if df_product_insert.count() > 0:
        df_product_insert.write.format("delta").mode("append").save(dim_product_path)
        print(f"dim_product: {df_product_insert.count()} new/updated records")
    else:
        print("dim_product: no changes detected")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ── SCD TYPE 1 — Carrier Master ───────────────────────────────
# MAGIC Simple overwrite — keep latest state only.
# MAGIC Under 1,000 rows, rarely changes. Incremental adds complexity with no benefit.

# COMMAND ----------

df_carrier_src = (
    spark.read.format("delta").load(bronze_base + "carrier_master/")
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("carrier_id").orderBy(F.col("ingestion_timestamp").desc())
    ))
    .filter(F.col("rn") == 1).drop("rn")
    .select("carrier_id", "carrier_name", "carrier_type", "region", "contact_email")
    .withColumn("last_updated", F.lit(load_date).cast("date"))
)

df_carrier_src.write.format("delta").mode("overwrite").save(carrier_path)
print(f"Carrier Master (SCD Type 1) overwritten: {df_carrier_src.count()} rows")
