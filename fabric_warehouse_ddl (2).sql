-- ============================================================
-- Project : Logistics Domain Data Platform
-- Layer   : Gold — Fabric Warehouse DDL
-- Desc    : Full T-SQL DDL for all fact tables, dimension tables,
--           and KPI tables. Fabric Warehouse supports full T-SQL
--           — required because operations analysts write ad-hoc SQL.
-- Author  : Abhishek Kamble
-- ============================================================

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE dim_customer (
    customer_sk    BIGINT        NOT NULL,
    customer_id    VARCHAR(50)   NOT NULL,
    customer_name  VARCHAR(200)  NULL,
    email          VARCHAR(200)  NULL,
    city           VARCHAR(100)  NULL,
    loyalty_tier   VARCHAR(50)   NULL,
    start_date     DATE          NOT NULL,
    end_date       DATE          NULL,
    is_current     BIT           NOT NULL DEFAULT 1
);

CREATE TABLE dim_product (
    product_sk    BIGINT         NOT NULL,
    product_id    VARCHAR(50)    NOT NULL,
    product_name  VARCHAR(300)   NULL,
    category      VARCHAR(100)   NULL,
    sub_category  VARCHAR(100)   NULL,
    unit_price    DECIMAL(10,2)  NULL,
    start_date    DATE           NOT NULL,
    end_date      DATE           NULL,
    is_current    BIT            NOT NULL DEFAULT 1
);

CREATE TABLE dim_carrier (
    carrier_id     VARCHAR(50)   NOT NULL PRIMARY KEY,
    carrier_name   VARCHAR(200)  NULL,
    carrier_type   VARCHAR(100)  NULL,   -- Express / Standard / Freight
    region         VARCHAR(100)  NULL,
    contact_email  VARCHAR(200)  NULL,
    last_updated   DATE          NULL
);

CREATE TABLE dim_warehouse (
    warehouse_sk    BIGINT        NOT NULL,
    warehouse_id    VARCHAR(50)   NOT NULL,
    warehouse_name  VARCHAR(200)  NULL,
    city            VARCHAR(100)  NULL,
    region          VARCHAR(100)  NULL,
    country         VARCHAR(100)  NULL,
    is_active       BIT           NOT NULL DEFAULT 1
);

CREATE TABLE dim_date (
    date_key     INT           NOT NULL PRIMARY KEY,
    full_date    DATE          NOT NULL,
    year         INT           NOT NULL,
    quarter      INT           NOT NULL,
    month        INT           NOT NULL,
    month_name   VARCHAR(20)   NOT NULL,
    week         INT           NOT NULL,
    day_of_week  VARCHAR(15)   NOT NULL,
    is_weekend   BIT           NOT NULL
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- fact_shipments: one row per shipment event
CREATE TABLE fact_shipments (
    shipment_id      VARCHAR(50)    NOT NULL,
    order_id         VARCHAR(50)    NOT NULL,
    carrier_id       VARCHAR(50)    NOT NULL,
    carrier_name     VARCHAR(200)   NULL,
    carrier_type     VARCHAR(100)   NULL,
    region           VARCHAR(100)   NULL,
    customer_sk      BIGINT         NOT NULL,
    product_sk       BIGINT         NOT NULL,
    date_key         INT            NOT NULL,
    event_type_std   VARCHAR(50)    NOT NULL,
    event_timestamp  DATETIME2      NULL,
    pickup_ts        DATETIME2      NULL,
    delivery_ts      DATETIME2      NULL,
    transit_days     DECIMAL(8,2)   NULL,
    sla_breached     BIT            NOT NULL DEFAULT 0,
    load_date        DATE           NOT NULL
);

-- fact_orders: one row per order
CREATE TABLE fact_orders (
    order_id          VARCHAR(50)    NOT NULL,
    customer_sk       BIGINT         NOT NULL,
    product_sk        BIGINT         NOT NULL,
    warehouse_sk      BIGINT         NOT NULL,
    date_key          INT            NOT NULL,
    quantity          INT            NOT NULL,
    unit_price        DECIMAL(10,2)  NOT NULL,
    order_value       DECIMAL(14,2)  NOT NULL,
    order_status_std  VARCHAR(50)    NOT NULL,
    sla_breach_flag   BIT            NOT NULL DEFAULT 0,
    load_date         DATE           NOT NULL
);

-- fact_inventory: daily snapshot per SKU per warehouse
-- Kept as snapshot (not current state) so stock trends trackable over time
CREATE TABLE fact_inventory (
    product_sk          BIGINT        NOT NULL,
    warehouse_sk        BIGINT        NOT NULL,
    date_key            INT           NOT NULL,
    stock_on_hand       INT           NOT NULL,
    reorder_threshold   INT           NOT NULL,
    units_received      INT           NOT NULL DEFAULT 0,
    units_dispatched    INT           NOT NULL DEFAULT 0,
    net_movement        INT           NOT NULL,
    stock_vs_threshold  INT           NOT NULL,
    below_reorder       BIT           NOT NULL DEFAULT 0,
    load_date           DATE          NOT NULL
);

-- ============================================================
-- KPI TABLES (pre-aggregated — Power BI hits these)
-- ============================================================

CREATE TABLE kpi_sla_compliance (
    year                 INT            NOT NULL,
    month                INT            NOT NULL,
    carrier_id           VARCHAR(50)    NOT NULL,
    carrier_name         VARCHAR(200)   NULL,
    region               VARCHAR(100)   NULL,
    total_deliveries     INT            NOT NULL,
    on_time_deliveries   INT            NOT NULL,
    avg_transit_days     DECIMAL(8,2)   NULL,
    sla_compliance_rate  DECIMAL(5,2)   NOT NULL
);

CREATE TABLE kpi_carrier_scorecard (
    carrier_id          VARCHAR(50)    NOT NULL,
    carrier_name        VARCHAR(200)   NULL,
    avg_on_time_rate    DECIMAL(5,2)   NOT NULL,
    avg_transit_days    DECIMAL(8,2)   NULL,
    total_deliveries    INT            NOT NULL,
    performance_rank    INT            NOT NULL
);

CREATE TABLE kpi_warehouse_stock (
    warehouse_name     VARCHAR(200)   NOT NULL,
    region             VARCHAR(100)   NOT NULL,
    category           VARCHAR(100)   NOT NULL,
    total_stock        INT            NOT NULL,
    total_threshold    INT            NOT NULL,
    sku_below_reorder  INT            NOT NULL
);

CREATE TABLE kpi_fulfilment_rate (
    year              INT            NOT NULL,
    month             INT            NOT NULL,
    region            VARCHAR(100)   NOT NULL,
    total_orders      INT            NOT NULL,
    fulfilled_orders  INT            NOT NULL,
    total_order_value DECIMAL(16,2)  NOT NULL,
    fulfilment_rate   DECIMAL(5,2)   NOT NULL,
    revenue_at_risk   DECIMAL(16,2)  NOT NULL
);
