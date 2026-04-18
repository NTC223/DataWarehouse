-- ============================================================
-- 16_init_olap_indexes.sql
-- Creates B-Tree indexes for all Cuboid Tables.
-- Ensures fast slicing, dicing, and drill-down aggregations.
--
-- CUBE 1: sales [time, customer, product]  → 8 cuboids
-- CUBE 2: inventory [time, product, store] → 8 cuboids
-- ============================================================

-- ============================================================
-- CUBE 1: Sales
-- ============================================================

-- olap_sales_all  → no index needed (single-row table)

-- Indexes for olap_sales_by_time
CREATE INDEX idx_olap_sales_by_time_time ON olap.olap_sales_by_time (year, quarter, month);

-- Indexes for olap_sales_by_customer_info and _loc
CREATE INDEX idx_olap_sales_by_cust_info_cust ON olap.olap_sales_by_customer_info (customer_key);
CREATE INDEX idx_olap_sales_by_cust_info_type ON olap.olap_sales_by_customer_info (customer_type);
CREATE INDEX idx_olap_sales_by_cust_loc_cust  ON olap.olap_sales_by_customer_loc  (customer_key);
CREATE INDEX idx_olap_sales_by_cust_loc_loc   ON olap.olap_sales_by_customer_loc  (state, city);

-- Indexes for olap_sales_by_product
CREATE INDEX idx_olap_sales_by_product_prod ON olap.olap_sales_by_product (product_key);

-- Indexes for olap_sales_time_customer_info and _loc
CREATE INDEX idx_olap_sales_time_cust_info_time ON olap.olap_sales_time_customer_info (year, quarter, month);
CREATE INDEX idx_olap_sales_time_cust_info_cust ON olap.olap_sales_time_customer_info (customer_key);
CREATE INDEX idx_olap_sales_time_cust_info_type ON olap.olap_sales_time_customer_info (customer_type);
CREATE INDEX idx_olap_sales_time_cust_loc_time  ON olap.olap_sales_time_customer_loc  (year, quarter, month);
CREATE INDEX idx_olap_sales_time_cust_loc_cust  ON olap.olap_sales_time_customer_loc  (customer_key);
CREATE INDEX idx_olap_sales_time_cust_loc_loc   ON olap.olap_sales_time_customer_loc  (state, city);

-- Indexes for olap_sales_time_product
CREATE INDEX idx_olap_sales_time_product_time ON olap.olap_sales_time_product (year, quarter, month);
CREATE INDEX idx_olap_sales_time_product_prod ON olap.olap_sales_time_product (product_key);

-- Indexes for olap_sales_customer_product_info and _loc
CREATE INDEX idx_olap_sales_cust_prod_info_cust ON olap.olap_sales_customer_product_info (customer_key);
CREATE INDEX idx_olap_sales_cust_prod_info_type ON olap.olap_sales_customer_product_info (customer_type);
CREATE INDEX idx_olap_sales_cust_prod_info_prod ON olap.olap_sales_customer_product_info (product_key);
CREATE INDEX idx_olap_sales_cust_prod_loc_cust  ON olap.olap_sales_customer_product_loc  (customer_key);
CREATE INDEX idx_olap_sales_cust_prod_loc_loc   ON olap.olap_sales_customer_product_loc  (state, city);
CREATE INDEX idx_olap_sales_cust_prod_loc_prod  ON olap.olap_sales_customer_product_loc  (product_key);

-- Indexes for olap_sales_base_info and _loc
CREATE INDEX idx_olap_sales_base_info_time ON olap.olap_sales_base_info (year, quarter, month);
CREATE INDEX idx_olap_sales_base_info_cust ON olap.olap_sales_base_info (customer_key);
CREATE INDEX idx_olap_sales_base_info_type ON olap.olap_sales_base_info (customer_type);
CREATE INDEX idx_olap_sales_base_info_prod ON olap.olap_sales_base_info (product_key);
CREATE INDEX idx_olap_sales_base_loc_time  ON olap.olap_sales_base_loc  (year, quarter, month);
CREATE INDEX idx_olap_sales_base_loc_cust  ON olap.olap_sales_base_loc  (customer_key);
CREATE INDEX idx_olap_sales_base_loc_loc   ON olap.olap_sales_base_loc  (state, city);
CREATE INDEX idx_olap_sales_base_loc_prod  ON olap.olap_sales_base_loc  (product_key);

-- ============================================================
-- CUBE 2: Inventory
-- ============================================================

-- olap_inv_all → no index needed (single-row table)

-- Indexes for olap_inv_by_time
CREATE INDEX idx_olap_inv_by_time_time ON olap.olap_inv_by_time (year, quarter, month);

-- Indexes for olap_inv_by_product
CREATE INDEX idx_olap_inv_by_product_prod ON olap.olap_inv_by_product (product_key);

-- Indexes for olap_inv_by_store
CREATE INDEX idx_olap_inv_by_store_store ON olap.olap_inv_by_store (store_key);
CREATE INDEX idx_olap_inv_by_store_loc   ON olap.olap_inv_by_store (state, city);

-- Indexes for olap_inv_time_product
CREATE INDEX idx_olap_inv_time_product_time ON olap.olap_inv_time_product (year, quarter, month);
CREATE INDEX idx_olap_inv_time_product_prod ON olap.olap_inv_time_product (product_key);

-- Indexes for olap_inv_time_store
CREATE INDEX idx_olap_inv_time_store_time  ON olap.olap_inv_time_store (year, quarter, month);
CREATE INDEX idx_olap_inv_time_store_store ON olap.olap_inv_time_store (store_key);
CREATE INDEX idx_olap_inv_time_store_loc   ON olap.olap_inv_time_store (state, city);

-- Indexes for olap_inv_product_store
CREATE INDEX idx_olap_inv_product_store_prod  ON olap.olap_inv_product_store (product_key);
CREATE INDEX idx_olap_inv_product_store_store ON olap.olap_inv_product_store (store_key);
CREATE INDEX idx_olap_inv_product_store_loc   ON olap.olap_inv_product_store (state, city);

-- Indexes for olap_inv_base
CREATE INDEX idx_olap_inv_base_time  ON olap.olap_inv_base (year, quarter, month);
CREATE INDEX idx_olap_inv_base_prod  ON olap.olap_inv_base (product_key);
CREATE INDEX idx_olap_inv_base_store ON olap.olap_inv_base (store_key);
CREATE INDEX idx_olap_inv_base_loc   ON olap.olap_inv_base (state, city);
