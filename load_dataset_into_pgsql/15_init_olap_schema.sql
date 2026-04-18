-- ============================================================
-- init_olap_schema.sql
-- Khởi tạo schema `olap` và các bảng cuboid
-- Chạy 1 lần trước khi DAG dwh_to_olap chạy lần đầu
-- ============================================================
--
-- define cube sales [time, customer, product]:
--     total_quantity = sum(quantity_ordered), sum_amount = sum(total_amount)
--
-- define dimension time as (time_key, month, quarter, year)
-- define dimension customer as (customer_key, customer_name, customer_type,
--     first_order_date, location(location_key, city_name, state, office_address))
-- define dimension product as (product_key, description, size, weight)
--
-- define cube inventory [time, product, store]:
--     total_quantity_on_hand = sum(quantity_on_hand)
--
-- define dimension time as time in cube sales
-- define dimension product as product in cube sales
-- define dimension store as (store_key, phone_number,
--     location(location_key, city_name, state, office_address))
-- ============================================================

CREATE SCHEMA IF NOT EXISTS olap;

SET search_path TO olap;

-- ============================================================
-- CUBE 1: Sales (Fact_Sales)
-- Dimensions: time, customer, product
-- Measures: total_quantity = sum(quantity_ordered),
--           sum_amount   = sum(total_amount)
-- Note: location (state, city) is a sub-attribute of customer,
--       NOT an independent dimension.
-- ============================================================

-- Dim 0 – ALL (Apex)
CREATE TABLE IF NOT EXISTS olap.olap_sales_all (
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- Dim 1 – by Time
CREATE TABLE IF NOT EXISTS olap.olap_sales_by_time (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- Dim 1 – by Customer
-- (customer_key, customer_type are stored; location attrs state/city come from customer hierarchy)
CREATE TABLE IF NOT EXISTS olap.olap_sales_by_customer_info (
    customer_key     INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_by_customer_loc (
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- Dim 1 – by Product
CREATE TABLE IF NOT EXISTS olap.olap_sales_by_product (
    product_key      INT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- Dim 2 – Time × Customer
CREATE TABLE IF NOT EXISTS olap.olap_sales_time_customer_info (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    customer_key     INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_time_customer_loc (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- Dim 2 – Time × Product
CREATE TABLE IF NOT EXISTS olap.olap_sales_time_product (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    product_key      INT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- Dim 2 – Customer × Product
CREATE TABLE IF NOT EXISTS olap.olap_sales_customer_product_info (
    customer_key     INT,
    customer_type    VARCHAR(50),
    product_key      INT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_customer_product_loc (
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    product_key      INT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- Dim 3 – BASE: Time × Customer × Product (full combination)
CREATE TABLE IF NOT EXISTS olap.olap_sales_base_info (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    customer_key     INT,
    customer_type    VARCHAR(50),
    product_key      INT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_base_loc (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    product_key      INT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- ============================================================
-- CUBE 2: Inventory (Fact_Inventory)
-- Dimensions: time, product, store
-- Measures: total_quantity_on_hand = sum(quantity_on_hand)
-- Note: location (state, city) is a sub-attribute of store,
--       NOT an independent dimension.
-- ============================================================

-- Dim 0 – ALL (Apex)
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
-- (store_key stored; location attrs state/city come from store hierarchy)
CREATE TABLE IF NOT EXISTS olap.olap_inv_by_store (
    store_key                INT,
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
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 2 – Product × Store
CREATE TABLE IF NOT EXISTS olap.olap_inv_product_store (
    product_key              INT,
    store_key                INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- Dim 3 – BASE: Time × Product × Store (full combination)
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
