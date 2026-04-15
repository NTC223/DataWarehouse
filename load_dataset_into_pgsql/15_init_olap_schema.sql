-- ============================================================
-- init_olap_schema.sql
-- Khởi tạo schema `olap` và 24 bảng cuboid
-- Chạy 1 lần trước khi DAG dwh_to_olap chạy lần đầu
-- ============================================================

CREATE SCHEMA IF NOT EXISTS olap;

SET search_path TO olap;
-- ============================================================
-- CUBE 1: Sales (Fact_Sales)
-- Measures: total_quantity, total_amount
-- ============================================================

-- Dim 0 – ALL
CREATE TABLE IF NOT EXISTS olap.olap_sales_all (
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 1 – by Time
CREATE TABLE IF NOT EXISTS olap.olap_sales_by_time (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 1 – by Product
CREATE TABLE IF NOT EXISTS olap.olap_sales_by_product (
    product_key      INT,
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 1 – by Customer type
CREATE TABLE IF NOT EXISTS olap.olap_sales_by_customer (
    customer_type    VARCHAR(50),
    customer_key     INT,
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 1 – by Location (state, city)
CREATE TABLE IF NOT EXISTS olap.olap_sales_by_location (
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 2 – Time × Product
CREATE TABLE IF NOT EXISTS olap.olap_sales_time_product (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    product_key      INT,
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 2 – Time × Customer
CREATE TABLE IF NOT EXISTS olap.olap_sales_time_customer (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    customer_type    VARCHAR(50),
    customer_key     INT,
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 2 – Time × Location
CREATE TABLE IF NOT EXISTS olap.olap_sales_time_location (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 2 – Product × Customer
CREATE TABLE IF NOT EXISTS olap.olap_sales_product_customer (
    product_key      INT,
    customer_type    VARCHAR(50),
    customer_key     INT,
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 2 – Product × Location
CREATE TABLE IF NOT EXISTS olap.olap_sales_product_location (
    product_key      INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 2 – Customer × Location
CREATE TABLE IF NOT EXISTS olap.olap_sales_customer_location (
    customer_type    VARCHAR(50),
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 3 – Time × Product × Customer
CREATE TABLE IF NOT EXISTS olap.olap_sales_time_product_customer (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    product_key      INT,
    customer_type    VARCHAR(50),
    customer_key     INT,
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 3 – Time × Product × Location
CREATE TABLE IF NOT EXISTS olap.olap_sales_time_product_location (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    product_key      INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 3 – Time × Customer × Location
CREATE TABLE IF NOT EXISTS olap.olap_sales_time_customer_location (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    customer_type    VARCHAR(50),
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 3 – Product × Customer × Location
CREATE TABLE IF NOT EXISTS olap.olap_sales_product_customer_location (
    product_key      INT,
    customer_type    VARCHAR(50),
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- Dim 4 – BASE (full combination)
CREATE TABLE IF NOT EXISTS olap.olap_sales_base (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    product_key      INT,
    customer_key    INT,
    customer_type    VARCHAR(50),
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    total_amount     NUMERIC(18,2)
);

-- ============================================================
-- CUBE 2: Inventory (Fact_Inventory)
-- Measures: total_quantity_on_hand
-- ============================================================

-- Dim 0 – ALL
CREATE TABLE IF NOT EXISTS olap.olap_inv_all (
    total_quantity_on_hand   BIGINT
);

-- Dim 1 – by Time
CREATE TABLE IF NOT EXISTS olap.olap_inv_by_time (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    total_quantity_on_hand   BIGINT
);

-- Dim 1 – by Product
CREATE TABLE IF NOT EXISTS olap.olap_inv_by_product (
    product_key              INT,
    total_quantity_on_hand   BIGINT
);

-- Dim 1 – by Store
CREATE TABLE IF NOT EXISTS olap.olap_inv_by_store (
    store_key                INT,
    total_quantity_on_hand   BIGINT
);

-- Dim 1 – by Location
CREATE TABLE IF NOT EXISTS olap.olap_inv_by_location (
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 2 – Time × Product
CREATE TABLE IF NOT EXISTS olap.olap_inv_time_product (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    product_key              INT,
    total_quantity_on_hand   BIGINT
);

-- Dim 2 – Time × Store
CREATE TABLE IF NOT EXISTS olap.olap_inv_time_store (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    store_key                INT,
    total_quantity_on_hand   BIGINT
);

-- Dim 2 – Time × Location
CREATE TABLE IF NOT EXISTS olap.olap_inv_time_location (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 2 – Product × Store
CREATE TABLE IF NOT EXISTS olap.olap_inv_product_store (
    product_key              INT,
    store_key                INT,
    total_quantity_on_hand   BIGINT
);

-- Dim 2 – Product × Location
CREATE TABLE IF NOT EXISTS olap.olap_inv_product_location (
    product_key              INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 2 – Store × Location
CREATE TABLE IF NOT EXISTS olap.olap_inv_store_location (
    store_key                INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 3 – Time × Product × Store
CREATE TABLE IF NOT EXISTS olap.olap_inv_time_product_store (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    product_key              INT,
    store_key                INT,
    total_quantity_on_hand   BIGINT
);

-- Dim 3 – Time × Product × Location
CREATE TABLE IF NOT EXISTS olap.olap_inv_time_product_location (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    product_key              INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 3 – Time × Store × Location
CREATE TABLE IF NOT EXISTS olap.olap_inv_time_store_location (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    store_key                INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 3 – Product × Store × Location
CREATE TABLE IF NOT EXISTS olap.olap_inv_product_store_location (
    product_key              INT,
    store_key                INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 4 – BASE (full combination)
CREATE TABLE IF NOT EXISTS olap.olap_inv_base (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    product_key              INT,
    store_key                INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);
