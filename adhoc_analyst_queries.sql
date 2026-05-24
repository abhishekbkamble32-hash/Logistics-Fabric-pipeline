-- ============================================================
-- Project : Logistics Domain Data Platform
-- Layer   : Gold — Sample Analyst Queries (Fabric Warehouse)
-- Desc    : Ad-hoc T-SQL queries on Gold layer.
--           Operations analysts query Fabric Warehouse directly —
--           full T-SQL support was required for this reason.
-- Author  : Abhishek Kamble
-- ============================================================

-- ============================================================
-- 1. Orders at risk of SLA breach (Operations Dashboard)
-- ============================================================

SELECT
    fo.order_id,
    dc.customer_name,
    dc.city,
    dp.product_name,
    dw.warehouse_name,
    dw.region,
    dd.full_date           AS order_date,
    fo.order_value,
    fo.order_status_std
FROM fact_orders fo
JOIN dim_customer  dc ON fo.customer_sk   = dc.customer_sk  AND dc.is_current = 1
JOIN dim_product   dp ON fo.product_sk    = dp.product_sk   AND dp.is_current = 1
JOIN dim_warehouse dw ON fo.warehouse_sk  = dw.warehouse_sk
JOIN dim_date      dd ON fo.date_key      = dd.date_key
WHERE fo.sla_breach_flag = 1
  AND fo.order_status_std NOT IN ('DELIVERED', 'CANCELLED')
ORDER BY fo.order_value DESC;

-- ============================================================
-- 2. Carrier performance comparison — on-time rate + avg transit days
-- ============================================================

SELECT
    carrier_name,
    avg_on_time_rate,
    avg_transit_days,
    total_deliveries,
    performance_rank,
    CASE
        WHEN avg_on_time_rate >= 95 THEN 'GREEN'
        WHEN avg_on_time_rate >= 85 THEN 'AMBER'
        ELSE                             'RED'
    END AS performance_status
FROM kpi_carrier_scorecard
ORDER BY performance_rank;

-- ============================================================
-- 3. Warehouse stock below reorder threshold (Warehouse Dashboard)
-- ============================================================

SELECT
    fi.warehouse_sk,
    dw.warehouse_name,
    dw.region,
    dp.product_name,
    dp.category,
    fi.stock_on_hand,
    fi.reorder_threshold,
    fi.stock_vs_threshold,
    fi.units_received,
    fi.units_dispatched
FROM fact_inventory fi
JOIN dim_product   dp ON fi.product_sk   = dp.product_sk   AND dp.is_current = 1
JOIN dim_warehouse dw ON fi.warehouse_sk = dw.warehouse_sk
WHERE fi.below_reorder = 1
  AND fi.load_date = CAST(GETDATE() AS DATE)
ORDER BY fi.stock_vs_threshold ASC;

-- ============================================================
-- 4. Monthly fulfilment rate by region
-- ============================================================

SELECT
    year,
    month,
    region,
    total_orders,
    fulfilled_orders,
    fulfilment_rate,
    revenue_at_risk
FROM kpi_fulfilment_rate
ORDER BY year DESC, month DESC, region;

-- ============================================================
-- 5. Shipment transit time analysis — carrier vs SLA
-- ============================================================

SELECT
    carrier_name,
    region,
    COUNT(shipment_id)                                          AS total_shipments,
    AVG(CAST(transit_days AS FLOAT))                           AS avg_transit_days,
    SUM(CASE WHEN sla_breached = 0 THEN 1 ELSE 0 END)         AS on_time,
    SUM(CASE WHEN sla_breached = 1 THEN 1 ELSE 0 END)         AS breached,
    ROUND(
        100.0 * SUM(CASE WHEN sla_breached = 0 THEN 1 ELSE 0 END)
        / COUNT(shipment_id), 2
    )                                                           AS on_time_pct
FROM fact_shipments
WHERE event_type_std = 'DELIVERED'
GROUP BY carrier_name, region
ORDER BY on_time_pct ASC;

-- ============================================================
-- 6. Daily order volume — last 30 days (Management Dashboard)
-- ============================================================

SELECT
    dd.full_date,
    dd.day_of_week,
    COUNT(fo.order_id)       AS order_count,
    SUM(fo.order_value)      AS total_order_value,
    SUM(fo.quantity)         AS total_units
FROM fact_orders fo
JOIN dim_date dd ON fo.date_key = dd.date_key
WHERE dd.full_date >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
GROUP BY dd.full_date, dd.day_of_week
ORDER BY dd.full_date DESC;

-- ============================================================
-- 7. Inventory turnover by category
-- ============================================================

SELECT
    dp.category,
    SUM(fi.units_dispatched)    AS total_dispatched,
    AVG(CAST(fi.stock_on_hand AS FLOAT)) AS avg_stock,
    ROUND(
        1.0 * SUM(fi.units_dispatched)
        / NULLIF(AVG(CAST(fi.stock_on_hand AS FLOAT)), 0),
        4
    ) AS turnover_ratio
FROM fact_inventory fi
JOIN dim_product dp ON fi.product_sk = dp.product_sk AND dp.is_current = 1
GROUP BY dp.category
ORDER BY turnover_ratio DESC;
