-- ============================================================
-- 16_init_olap_granularity.sql
-- Tạo các bảng cuboid granularity (rollup theo từng cấp độ
-- của mỗi dimension).  Chạy sau 15_init_olap_schema.sql.
-- ============================================================

SET search_path TO olap;

-- ============================================================
-- CUBE 1: Sales  –  Time rollups
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_sales_by_quarter (
    year             SMALLINT,
    quarter          SMALLINT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_by_year (
    year             SMALLINT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- ============================================================
-- CUBE 1  –  Customer rollups (single-dim)
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_sales_by_customer_type (
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_by_city (
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_by_state (
    state            VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- ============================================================
-- CUBE 1  –  Time × Product rollups
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_product (
    year             SMALLINT,
    quarter          SMALLINT,
    product_key      INT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_year_product (
    year             SMALLINT,
    product_key      INT,
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- ============================================================
-- CUBE 1  –  Time × Customer rollups  (12 bảng)
-- ============================================================

-- month × customer_type
CREATE TABLE IF NOT EXISTS olap.olap_sales_month_customer_type (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- month × city
CREATE TABLE IF NOT EXISTS olap.olap_sales_month_city (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- month × state
CREATE TABLE IF NOT EXISTS olap.olap_sales_month_state (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    state            VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- quarter × customer_key
CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_customer_info (
    year             SMALLINT,
    quarter          SMALLINT,
    customer_key     INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_customer_loc (
    year             SMALLINT,
    quarter          SMALLINT,
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- quarter × customer_type
CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_customer_type (
    year             SMALLINT,
    quarter          SMALLINT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- quarter × city
CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_city (
    year             SMALLINT,
    quarter          SMALLINT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- quarter × state
CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_state (
    year             SMALLINT,
    quarter          SMALLINT,
    state            VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- year × customer_key
CREATE TABLE IF NOT EXISTS olap.olap_sales_year_customer_info (
    year             SMALLINT,
    customer_key     INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_year_customer_loc (
    year             SMALLINT,
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- year × customer_type
CREATE TABLE IF NOT EXISTS olap.olap_sales_year_customer_type (
    year             SMALLINT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- year × city
CREATE TABLE IF NOT EXISTS olap.olap_sales_year_city (
    year             SMALLINT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- year × state
CREATE TABLE IF NOT EXISTS olap.olap_sales_year_state (
    year             SMALLINT,
    state            VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- ============================================================
-- CUBE 1  –  Product × Customer rollups
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_sales_product_customer_type (
    product_key      INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_product_city (
    product_key      INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_product_state (
    product_key      INT,
    state            VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- ============================================================
-- CUBE 1  –  Time × Product × Customer rollups  (12 bảng)
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_sales_month_product_customer_type (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    product_key      INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_month_product_city (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    product_key      INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_month_product_state (
    year             SMALLINT,
    quarter          SMALLINT,
    month            SMALLINT,
    product_key      INT,
    state            VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_product_customer_info (
    year             SMALLINT,
    quarter          SMALLINT,
    product_key      INT,
    customer_key     INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_product_customer_loc (
    year             SMALLINT,
    quarter          SMALLINT,
    product_key      INT,
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_product_customer_type (
    year             SMALLINT,
    quarter          SMALLINT,
    product_key      INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_product_city (
    year             SMALLINT,
    quarter          SMALLINT,
    product_key      INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_quarter_product_state (
    year             SMALLINT,
    quarter          SMALLINT,
    product_key      INT,
    state            VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_year_product_customer_info (
    year             SMALLINT,
    product_key      INT,
    customer_key     INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_year_product_customer_loc (
    year             SMALLINT,
    product_key      INT,
    customer_key     INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_year_product_customer_type (
    year             SMALLINT,
    product_key      INT,
    customer_type    VARCHAR(50),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_year_product_city (
    year             SMALLINT,
    product_key      INT,
    state            VARCHAR(100),
    city             VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

CREATE TABLE IF NOT EXISTS olap.olap_sales_year_product_state (
    year             SMALLINT,
    product_key      INT,
    state            VARCHAR(100),
    total_quantity   BIGINT,
    sum_amount       NUMERIC(18,2)
);

-- ============================================================
-- CUBE 2: Inventory  –  Time rollups
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_inv_by_quarter (
    year                     SMALLINT,
    quarter                  SMALLINT,
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_by_year (
    year                     SMALLINT,
    total_quantity_on_hand   BIGINT
);

-- ============================================================
-- CUBE 2  –  Store rollups (single-dim)
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_inv_by_city (
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_by_state (
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- ============================================================
-- CUBE 2  –  Time × Product rollups
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_inv_quarter_product (
    year                     SMALLINT,
    quarter                  SMALLINT,
    product_key              INT,
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_year_product (
    year                     SMALLINT,
    product_key              INT,
    total_quantity_on_hand   BIGINT
);

-- ============================================================
-- CUBE 2  –  Time × Store rollups  (8 bảng)
-- ============================================================

-- month × city
CREATE TABLE IF NOT EXISTS olap.olap_inv_month_city (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- month × state
CREATE TABLE IF NOT EXISTS olap.olap_inv_month_state (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- quarter × store_key
CREATE TABLE IF NOT EXISTS olap.olap_inv_quarter_store (
    year                     SMALLINT,
    quarter                  SMALLINT,
    store_key                INT,
    city                     VARCHAR(100),
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- quarter × city
CREATE TABLE IF NOT EXISTS olap.olap_inv_quarter_city (
    year                     SMALLINT,
    quarter                  SMALLINT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- quarter × state
CREATE TABLE IF NOT EXISTS olap.olap_inv_quarter_state (
    year                     SMALLINT,
    quarter                  SMALLINT,
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- year × store_key
CREATE TABLE IF NOT EXISTS olap.olap_inv_year_store (
    year                     SMALLINT,
    store_key                INT,
    city                     VARCHAR(100),
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- year × city
CREATE TABLE IF NOT EXISTS olap.olap_inv_year_city (
    year                     SMALLINT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- year × state
CREATE TABLE IF NOT EXISTS olap.olap_inv_year_state (
    year                     SMALLINT,
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- ============================================================
-- CUBE 2  –  Product × Store rollups
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_inv_product_city (
    product_key              INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_product_state (
    product_key              INT,
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

-- ============================================================
-- CUBE 2  –  Time × Product × Store rollups  (8 bảng)
-- ============================================================

CREATE TABLE IF NOT EXISTS olap.olap_inv_month_product_city (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    product_key              INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_month_product_state (
    year                     SMALLINT,
    quarter                  SMALLINT,
    month                    SMALLINT,
    product_key              INT,
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_quarter_product_store (
    year                     SMALLINT,
    quarter                  SMALLINT,
    product_key              INT,
    store_key                INT,
    city                     VARCHAR(100),
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_quarter_product_city (
    year                     SMALLINT,
    quarter                  SMALLINT,
    product_key              INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_quarter_product_state (
    year                     SMALLINT,
    quarter                  SMALLINT,
    product_key              INT,
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_year_product_store (
    year                     SMALLINT,
    product_key              INT,
    store_key                INT,
    city                     VARCHAR(100),
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_year_product_city (
    year                     SMALLINT,
    product_key              INT,
    state                    VARCHAR(100),
    city                     VARCHAR(100),
    total_quantity_on_hand   BIGINT
);

CREATE TABLE IF NOT EXISTS olap.olap_inv_year_product_state (
    year                     SMALLINT,
    product_key              INT,
    state                    VARCHAR(100),
    total_quantity_on_hand   BIGINT
);
