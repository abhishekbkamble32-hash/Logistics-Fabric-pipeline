# Schema Version Log — Logistics Domain Data Platform

## Purpose
`mergeSchema=true` is enabled on all Silver Delta writes to absorb new columns automatically.
Any new column detected triggers a **Slack alert** so the team is never surprised by silent schema changes.

This file tracks all schema evolution events manually for audit and review.

---

## Format

```
| Date       | Table              | Change Type    | Columns Added / Modified | Detected By         | Action Taken          |
```

---

## Log

| Date       | Table                   | Change Type    | Columns Added / Modified       | Detected By              | Action Taken                          |
|------------|-------------------------|----------------|-------------------------------|--------------------------|---------------------------------------|
| 2024-03-01 | silver.shipment_tracking | Column Added   | `carrier_zone` VARCHAR(50)    | Schema evolution alert   | Accepted — added to Gold DDL          |
| 2024-04-15 | silver.orders           | Column Added   | `promo_code` VARCHAR(100)     | Schema evolution alert   | Accepted — added to fact_orders DDL  |
| 2024-05-10 | silver.orders           | Column Added   | `customer_segment` VARCHAR(50)| Schema evolution alert   | Accepted — added to dim_customer     |
| 2024-06-22 | silver.carrier_master   | Column Added   | `contact_phone` VARCHAR(20)   | Schema evolution alert   | Accepted — SCD Type 1 overwrite      |

---

## How Schema Evolution Alert Works

In notebook `03_silver_shipments_parse.py`:

```python
# Check for new columns vs previous run
df_existing  = spark.read.format("delta").load(silver_shipments_path)
existing_cols = set(df_existing.columns)
new_cols      = set(df_new.columns) - existing_cols

if new_cols:
    print(f"SCHEMA EVOLUTION ALERT: New columns detected: {new_cols}")
    # In production: trigger Slack alert via webhook
```

---

## Rules

1. All new columns must be logged here within 24 hours of detection.
2. New columns that affect Gold layer DDL require a DDL update in `fabric_warehouse_ddl.sql`.
3. Breaking changes (column type changes, column removals) require explicit team review before merge.
4. Log is reviewed in weekly data engineering standup.
