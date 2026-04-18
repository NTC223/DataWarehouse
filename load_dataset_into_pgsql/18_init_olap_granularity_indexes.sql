-- ============================================================
-- 17_init_olap_granularity_indexes.sql
-- B-Tree indexes cho 56 bảng cuboid granularity mới
-- (chạy sau 16_init_olap_granularity.sql)
--
-- Quy tắc đặt tên:
--   idx_<table>_<nhóm_cột>
-- ============================================================

-- ============================================================
-- CUBE 1: Sales  –  Time rollups
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_c1_by_quarter_time
    ON olap.olap_sales_by_quarter (year, quarter);

CREATE INDEX IF NOT EXISTS idx_c1_by_year_time
    ON olap.olap_sales_by_year (year);

-- ============================================================
-- CUBE 1  –  Customer rollups (single-dim)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_c1_by_ctype_type
    ON olap.olap_sales_by_customer_type (customer_type);

CREATE INDEX IF NOT EXISTS idx_c1_by_city_city
    ON olap.olap_sales_by_city (city);

CREATE INDEX IF NOT EXISTS idx_c1_by_state_state
    ON olap.olap_sales_by_state (state);

-- ============================================================
-- CUBE 1  –  Time × Product rollups
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_c1_qprod_time
    ON olap.olap_sales_quarter_product (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_qprod_prod
    ON olap.olap_sales_quarter_product (product_key);

CREATE INDEX IF NOT EXISTS idx_c1_yprod_time
    ON olap.olap_sales_year_product (year);
CREATE INDEX IF NOT EXISTS idx_c1_yprod_prod
    ON olap.olap_sales_year_product (product_key);

-- ============================================================
-- CUBE 1  –  Time × Customer rollups
-- ============================================================

-- month × customer_type
CREATE INDEX IF NOT EXISTS idx_c1_m_ctype_time
    ON olap.olap_sales_month_customer_type (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c1_m_ctype_type
    ON olap.olap_sales_month_customer_type (customer_type);

-- month × city
CREATE INDEX IF NOT EXISTS idx_c1_m_city_time
    ON olap.olap_sales_month_city (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c1_m_city_city
    ON olap.olap_sales_month_city (city);

-- month × state
CREATE INDEX IF NOT EXISTS idx_c1_m_state_time
    ON olap.olap_sales_month_state (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c1_m_state_state
    ON olap.olap_sales_month_state (state);

-- quarter × customer_key (_info & _loc)
CREATE INDEX IF NOT EXISTS idx_c1_q_cust_info_time
    ON olap.olap_sales_quarter_customer_info (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_cust_info_key
    ON olap.olap_sales_quarter_customer_info (customer_key);
CREATE INDEX IF NOT EXISTS idx_c1_q_cust_info_type
    ON olap.olap_sales_quarter_customer_info (customer_type);

CREATE INDEX IF NOT EXISTS idx_c1_q_cust_loc_time
    ON olap.olap_sales_quarter_customer_loc (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_cust_loc_key
    ON olap.olap_sales_quarter_customer_loc (customer_key);
CREATE INDEX IF NOT EXISTS idx_c1_q_cust_loc_loc
    ON olap.olap_sales_quarter_customer_loc (state, city);

-- quarter × customer_type
CREATE INDEX IF NOT EXISTS idx_c1_q_ctype_time
    ON olap.olap_sales_quarter_customer_type (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_ctype_type
    ON olap.olap_sales_quarter_customer_type (customer_type);

-- quarter × city
CREATE INDEX IF NOT EXISTS idx_c1_q_city_time
    ON olap.olap_sales_quarter_city (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_city_city
    ON olap.olap_sales_quarter_city (city);

-- quarter × state
CREATE INDEX IF NOT EXISTS idx_c1_q_state_time
    ON olap.olap_sales_quarter_state (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_state_state
    ON olap.olap_sales_quarter_state (state);

-- year × customer_key (_info & _loc)
CREATE INDEX IF NOT EXISTS idx_c1_y_cust_info_time
    ON olap.olap_sales_year_customer_info (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_cust_info_key
    ON olap.olap_sales_year_customer_info (customer_key);
CREATE INDEX IF NOT EXISTS idx_c1_y_cust_info_type
    ON olap.olap_sales_year_customer_info (customer_type);

CREATE INDEX IF NOT EXISTS idx_c1_y_cust_loc_time
    ON olap.olap_sales_year_customer_loc (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_cust_loc_key
    ON olap.olap_sales_year_customer_loc (customer_key);
CREATE INDEX IF NOT EXISTS idx_c1_y_cust_loc_loc
    ON olap.olap_sales_year_customer_loc (state, city);

-- year × customer_type
CREATE INDEX IF NOT EXISTS idx_c1_y_ctype_time
    ON olap.olap_sales_year_customer_type (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_ctype_type
    ON olap.olap_sales_year_customer_type (customer_type);

-- year × city
CREATE INDEX IF NOT EXISTS idx_c1_y_city_time
    ON olap.olap_sales_year_city (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_city_city
    ON olap.olap_sales_year_city (city);

-- year × state
CREATE INDEX IF NOT EXISTS idx_c1_y_state_time
    ON olap.olap_sales_year_state (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_state_state
    ON olap.olap_sales_year_state (state);

-- ============================================================
-- CUBE 1  –  Product × Customer rollups
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_c1_prod_ctype_prod
    ON olap.olap_sales_product_customer_type (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_prod_ctype_type
    ON olap.olap_sales_product_customer_type (customer_type);

CREATE INDEX IF NOT EXISTS idx_c1_prod_city_prod
    ON olap.olap_sales_product_city (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_prod_city_city
    ON olap.olap_sales_product_city (city);

CREATE INDEX IF NOT EXISTS idx_c1_prod_state_prod
    ON olap.olap_sales_product_state (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_prod_state_state
    ON olap.olap_sales_product_state (state);

-- ============================================================
-- CUBE 1  –  Time × Product × Customer rollups
-- ============================================================

-- month × product × customer_type
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_ctype_time
    ON olap.olap_sales_month_product_customer_type (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_ctype_prod
    ON olap.olap_sales_month_product_customer_type (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_ctype_type
    ON olap.olap_sales_month_product_customer_type (customer_type);

-- month × product × city
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_city_time
    ON olap.olap_sales_month_product_city (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_city_prod
    ON olap.olap_sales_month_product_city (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_city_city
    ON olap.olap_sales_month_product_city (city);

-- month × product × state
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_state_time
    ON olap.olap_sales_month_product_state (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_state_prod
    ON olap.olap_sales_month_product_state (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_m_prod_state_state
    ON olap.olap_sales_month_product_state (state);

-- quarter × product × customer_key (_info & _loc)
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_cust_info_time
    ON olap.olap_sales_quarter_product_customer_info (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_cust_info_prod
    ON olap.olap_sales_quarter_product_customer_info (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_cust_info_key
    ON olap.olap_sales_quarter_product_customer_info (customer_key);

CREATE INDEX IF NOT EXISTS idx_c1_q_prod_cust_loc_time
    ON olap.olap_sales_quarter_product_customer_loc (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_cust_loc_prod
    ON olap.olap_sales_quarter_product_customer_loc (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_cust_loc_key
    ON olap.olap_sales_quarter_product_customer_loc (customer_key);

-- quarter × product × customer_type
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_ctype_time
    ON olap.olap_sales_quarter_product_customer_type (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_ctype_prod
    ON olap.olap_sales_quarter_product_customer_type (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_ctype_type
    ON olap.olap_sales_quarter_product_customer_type (customer_type);

-- quarter × product × city
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_city_time
    ON olap.olap_sales_quarter_product_city (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_city_prod
    ON olap.olap_sales_quarter_product_city (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_city_city
    ON olap.olap_sales_quarter_product_city (city);

-- quarter × product × state
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_state_time
    ON olap.olap_sales_quarter_product_state (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_state_prod
    ON olap.olap_sales_quarter_product_state (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_q_prod_state_state
    ON olap.olap_sales_quarter_product_state (state);

-- year × product × customer_key (_info & _loc)
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_cust_info_time
    ON olap.olap_sales_year_product_customer_info (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_cust_info_prod
    ON olap.olap_sales_year_product_customer_info (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_cust_info_key
    ON olap.olap_sales_year_product_customer_info (customer_key);

CREATE INDEX IF NOT EXISTS idx_c1_y_prod_cust_loc_time
    ON olap.olap_sales_year_product_customer_loc (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_cust_loc_prod
    ON olap.olap_sales_year_product_customer_loc (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_cust_loc_key
    ON olap.olap_sales_year_product_customer_loc (customer_key);

-- year × product × customer_type
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_ctype_time
    ON olap.olap_sales_year_product_customer_type (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_ctype_prod
    ON olap.olap_sales_year_product_customer_type (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_ctype_type
    ON olap.olap_sales_year_product_customer_type (customer_type);

-- year × product × city
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_city_time
    ON olap.olap_sales_year_product_city (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_city_prod
    ON olap.olap_sales_year_product_city (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_city_city
    ON olap.olap_sales_year_product_city (city);

-- year × product × state
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_state_time
    ON olap.olap_sales_year_product_state (year);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_state_prod
    ON olap.olap_sales_year_product_state (product_key);
CREATE INDEX IF NOT EXISTS idx_c1_y_prod_state_state
    ON olap.olap_sales_year_product_state (state);

-- ============================================================
-- CUBE 2: Inventory  –  Time rollups
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_c2_by_quarter_time
    ON olap.olap_inv_by_quarter (year, quarter);

CREATE INDEX IF NOT EXISTS idx_c2_by_year_time
    ON olap.olap_inv_by_year (year);

-- ============================================================
-- CUBE 2  –  Store rollups (single-dim)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_c2_by_city_city
    ON olap.olap_inv_by_city (city);

CREATE INDEX IF NOT EXISTS idx_c2_by_state_state
    ON olap.olap_inv_by_state (state);

-- ============================================================
-- CUBE 2  –  Time × Product rollups
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_c2_q_prod_time
    ON olap.olap_inv_quarter_product (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_prod
    ON olap.olap_inv_quarter_product (product_key);

CREATE INDEX IF NOT EXISTS idx_c2_y_prod_time
    ON olap.olap_inv_year_product (year);
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_prod
    ON olap.olap_inv_year_product (product_key);

-- ============================================================
-- CUBE 2  –  Time × Store rollups
-- ============================================================

-- month × city
CREATE INDEX IF NOT EXISTS idx_c2_m_city_time
    ON olap.olap_inv_month_city (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c2_m_city_city
    ON olap.olap_inv_month_city (city);

-- month × state
CREATE INDEX IF NOT EXISTS idx_c2_m_state_time
    ON olap.olap_inv_month_state (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c2_m_state_state
    ON olap.olap_inv_month_state (state);

-- quarter × store_key
CREATE INDEX IF NOT EXISTS idx_c2_q_store_time
    ON olap.olap_inv_quarter_store (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c2_q_store_store
    ON olap.olap_inv_quarter_store (store_key);
CREATE INDEX IF NOT EXISTS idx_c2_q_store_loc
    ON olap.olap_inv_quarter_store (state, city);

-- quarter × city
CREATE INDEX IF NOT EXISTS idx_c2_q_city_time
    ON olap.olap_inv_quarter_city (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c2_q_city_city
    ON olap.olap_inv_quarter_city (city);

-- quarter × state
CREATE INDEX IF NOT EXISTS idx_c2_q_state_time
    ON olap.olap_inv_quarter_state (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c2_q_state_state
    ON olap.olap_inv_quarter_state (state);

-- year × store_key
CREATE INDEX IF NOT EXISTS idx_c2_y_store_time
    ON olap.olap_inv_year_store (year);
CREATE INDEX IF NOT EXISTS idx_c2_y_store_store
    ON olap.olap_inv_year_store (store_key);
CREATE INDEX IF NOT EXISTS idx_c2_y_store_loc
    ON olap.olap_inv_year_store (state, city);

-- year × city
CREATE INDEX IF NOT EXISTS idx_c2_y_city_time
    ON olap.olap_inv_year_city (year);
CREATE INDEX IF NOT EXISTS idx_c2_y_city_city
    ON olap.olap_inv_year_city (city);

-- year × state
CREATE INDEX IF NOT EXISTS idx_c2_y_state_time
    ON olap.olap_inv_year_state (year);
CREATE INDEX IF NOT EXISTS idx_c2_y_state_state
    ON olap.olap_inv_year_state (state);

-- ============================================================
-- CUBE 2  –  Product × Store rollups
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_c2_prod_city_prod
    ON olap.olap_inv_product_city (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_prod_city_city
    ON olap.olap_inv_product_city (city);

CREATE INDEX IF NOT EXISTS idx_c2_prod_state_prod
    ON olap.olap_inv_product_state (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_prod_state_state
    ON olap.olap_inv_product_state (state);

-- ============================================================
-- CUBE 2  –  Time × Product × Store rollups
-- ============================================================

-- month × product × city
CREATE INDEX IF NOT EXISTS idx_c2_m_prod_city_time
    ON olap.olap_inv_month_product_city (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c2_m_prod_city_prod
    ON olap.olap_inv_month_product_city (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_m_prod_city_city
    ON olap.olap_inv_month_product_city (city);

-- month × product × state
CREATE INDEX IF NOT EXISTS idx_c2_m_prod_state_time
    ON olap.olap_inv_month_product_state (year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_c2_m_prod_state_prod
    ON olap.olap_inv_month_product_state (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_m_prod_state_state
    ON olap.olap_inv_month_product_state (state);

-- quarter × product × store_key
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_store_time
    ON olap.olap_inv_quarter_product_store (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_store_prod
    ON olap.olap_inv_quarter_product_store (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_store_store
    ON olap.olap_inv_quarter_product_store (store_key);

-- quarter × product × city
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_city_time
    ON olap.olap_inv_quarter_product_city (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_city_prod
    ON olap.olap_inv_quarter_product_city (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_city_city
    ON olap.olap_inv_quarter_product_city (city);

-- quarter × product × state
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_state_time
    ON olap.olap_inv_quarter_product_state (year, quarter);
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_state_prod
    ON olap.olap_inv_quarter_product_state (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_q_prod_state_state
    ON olap.olap_inv_quarter_product_state (state);

-- year × product × store_key
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_store_time
    ON olap.olap_inv_year_product_store (year);
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_store_prod
    ON olap.olap_inv_year_product_store (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_store_store
    ON olap.olap_inv_year_product_store (store_key);

-- year × product × city
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_city_time
    ON olap.olap_inv_year_product_city (year);
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_city_prod
    ON olap.olap_inv_year_product_city (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_city_city
    ON olap.olap_inv_year_product_city (city);

-- year × product × state
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_state_time
    ON olap.olap_inv_year_product_state (year);
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_state_prod
    ON olap.olap_inv_year_product_state (product_key);
CREATE INDEX IF NOT EXISTS idx_c2_y_prod_state_state
    ON olap.olap_inv_year_product_state (state);
