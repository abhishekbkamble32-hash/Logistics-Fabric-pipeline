# Logistics Domain Data Platform

> **Microsoft Fabric End-to-End | Medallion Architecture | Fabric Pipelines · Fabric Lakehouse · Fabric Warehouse · Delta Lake · Fabric Lineage View**

---

## Problem Statement

A US-based logistics client managed order fulfilment, shipment tracking, and warehouse inventory across multiple distribution centres — but across **five completely separate systems**. Operations managers couldn't answer basic questions like *"Which orders are at risk of missing SLA?"* or *"Which warehouse is short on this SKU?"* without manually joining data from three systems in Excel.

The client needed a centralised, real-time supply chain view from order placement to delivery, with historical depth to analyse carrier performance and SLA compliance. This was a **full Microsoft Fabric end-to-end implementation** — no ADF, everything native in Fabric.

---

## Architecture Overview

```
[OMS REST API]          ─┐
[Shipment Flat Files]   ─┤
[Warehouse Azure SQL]   ─┤──► Fabric Pipelines (Parameterized, Watermark-based)
[Carrier Master SQL]    ─┤         │
[Master Data CSVs]      ─┘         ▼
                             Fabric Lakehouse Bronze (Delta, append-only)
                                      │
                             Fabric Notebooks (PySpark — Parameterized)
                                      │
                             Fabric Lakehouse Silver (Delta, mergeSchema=true)
                                      │
                             Fabric Notebooks (PySpark)
                                      │
                             Fabric Warehouse Gold (Star Schema + KPI Tables)
                                      │
                             Power BI (DirectQuery) ──► 4 Dashboards
                                      │
                         Fabric Lineage View (End-to-end dependency map)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Microsoft Fabric Pipelines |
| Storage & Table Format | Fabric Lakehouse + Delta Lake |
| Transformation | Fabric Notebooks (PySpark) |
| Serving | Fabric Warehouse (T-SQL) |
| Lineage & Governance | Fabric Lineage View |
| Reporting | Power BI (DirectQuery) |
| Version Control | GitHub |

---

## Data Flow — Medallion Architecture

### 🥉 Bronze Layer — Raw Ingestion

All 5 sources orchestrated entirely through **Fabric Pipelines** — no external orchestration tool:

| Source | Method | Frequency | Reason |
|---|---|---|---|
| Order Management System | REST API | **Every 4 hours** | Needed at-risk orders before SLA breach — daily too late |
| Shipment Tracking | Flat files | Daily | Fabric flat file connector |
| Warehouse Inventory | Azure SQL | Daily | Standard daily refresh |
| Carrier Master | Azure SQL | Weekly (full load) | Under 1,000 rows, rarely changes — incremental adds complexity with no benefit |
| Master Data (products, customers) | CSVs | Weekly | Low change frequency |

- All records landed in **Fabric Lakehouse Bronze** in **Delta format**, append-only
- Audit columns on every record: `ingestion_timestamp`, `pipeline_run_id`, `source_system`
- No transformation at Bronze — raw as received

### 🥈 Silver Layer — Cleanse & Enrich

**Parameterized Fabric Notebooks** — same PySpark code handled multiple domains by accepting `domain_name` and config as parameters, keeping logic reusable across sources:

**Orders:**
- Deduplicated on `order_id + event_timestamp`
- Standardised status codes (`'SHIPPED'`, `'SHP'`, `'S'` → single standard vocabulary)
- Validated mandatory fields; orders breaching SLA threshold flagged into a separate **at-risk table** feeding directly into the Operations dashboard

**Shipments:**
- Source had multi-event records (one row per event: picked up, in transit, delivered)
- Parsed to one clean row per event, joined with Carrier Master for enrichment
- Calculated `transit_days` as delivery minus pickup timestamp

**Inventory:**
- Calculated net stock movements: `opening + receipts - dispatches`

**Dimension Management:**
- **SCD Type 1** for Carrier Master — simple overwrite
- **SCD Type 2** for Customer and Product dimensions via **Delta MERGE** — full historical tracking

**Schema Evolution:**
- `mergeSchema=true` on all Delta writes to absorb new columns automatically
- Maintained a **schema version log** — any new column triggered a Slack alert; no silent schema changes

### 🥇 Gold Layer — Aggregate & Serve

Loaded into **Fabric Warehouse** — full T-SQL support required because operations analysts wrote ad-hoc SQL, not just consuming fixed Power BI reports.

**Fact Tables:**

| Table | Granularity |
|---|---|
| `fact_shipments` | One row per shipment event — carrier, origin, destination, SLA flag, `transit_days` |
| `fact_orders` | One row per order — customer, product, value, status |
| `fact_inventory` | Daily snapshot per SKU per warehouse — kept as snapshot (not current state) so stock trends are trackable over time |

**Dimension Tables:** `dim_customer`, `dim_product`, `dim_carrier`, `dim_warehouse`, `dim_date`

**Pre-aggregated KPI Tables:**
- Daily SLA compliance rate per carrier and region
- Carrier performance scorecard ranked by on-time rate and average transit days
- Warehouse stock health vs reorder threshold
- Fulfilment rate by region

---

## Reporting & Governance

**Power BI Dashboards (4) via DirectQuery on Fabric Warehouse:**

| Dashboard | Key Metrics |
|---|---|
| Operations | Real-time order status, at-risk orders (red alerts), SLA breach alerts |
| Warehouse | Stock levels, low-stock alerts, inbound/outbound movement |
| Carrier Performance | On-time rate, average transit days by carrier and zone |
| Management | Fulfilment rate, revenue at risk, daily order volume |

> The **Carrier Performance dashboard** directly enabled contract renegotiations — the client used it as objective evidence in supplier discussions, shifting volume away from underperforming carriers.

**Governance:**
- **Fabric Lineage View** — full dependency map from pipeline to report
- **Monitoring Hub** reviewed every morning before standup
- **Retry logic:** 3 retries, 2-minute interval on all pipeline activities
- **Fault tolerance** on Copy Activities — bad rows redirected to error log table; pipeline never failed on a single bad record
- **Access control:** Operations team → Gold only | Engineers → Silver

---

## Key Outcomes

| Metric | Before | After |
|---|---|---|
| Systems unified | 5 silos | **Single platform** |
| Pipeline performance | Baseline | **+25%** |
| SLA reporting | Manual Excel | **Fully automated** |
| Ops team posture | Reactive | **Proactive** (real-time SLA alerts) |
| Carrier contracts | Gut-feel decisions | **Data-driven renegotiation** |

---

## Repository Structure

```
logistics-fabric-pipeline/
├── README.md
├── fabric_pipelines/
│   └── pipeline_config_template.json        # Fabric Pipeline export (sanitized)
├── notebooks/
│   ├── bronze_ingestion_audit.py            # Audit column injection + Delta write
│   ├── silver_orders_cleanse.py             # Dedup, status normalisation, SLA flag
│   ├── silver_shipments_parse.py            # Multi-event parse + transit_days calc
│   ├── silver_inventory_movements.py        # Net stock movement calculation
│   ├── silver_scd_type2_merge.py            # Delta MERGE SCD Type 2 (Customer/Product)
│   └── gold_facts_dims_load.py              # Star schema + KPI table load
├── sql/
│   ├── fabric_warehouse_ddl.sql             # All fact and dimension DDL
│   ├── gold_kpi_views.sql                   # Carrier scorecard, SLA compliance views
│   └── adhoc_analyst_queries.sql            # Sample analyst queries on Gold layer
├── config/
│   └── schema_version_log.md               # Schema evolution tracking
└── docs/
    └── architecture_diagram.png
```

---

## Author

**Abhishek Kamble** | Azure Data Engineer | Microsoft DP-700 Certified  
📧 abhishekbkamble32@gmail.com | 📍 Pune, India  
[LinkedIn](#) · [GitHub](#)
