-- ============================================================
-- 16_init_olap_indexes.sql
-- Creates B-Tree and Composite Indexes for all Cuboid Tables.
-- Ensures fast slicing, dicing, and drill-down aggregations.
-- ============================================================

-- Indexes for olap_sales_by_time
CREATE INDEX idx_olap_sales_by_time_time ON olap.olap_sales_by_time (year, quarter, month);

-- Indexes for olap_sales_by_product
CREATE INDEX idx_olap_sales_by_product_prod ON olap.olap_sales_by_product (product_key);

-- Indexes for olap_sales_by_customer
CREATE INDEX idx_olap_sales_by_customer_cust ON olap.olap_sales_by_customer (customer_type, customer_key);

-- Indexes for olap_sales_by_location
CREATE INDEX idx_olap_sales_by_location_loc ON olap.olap_sales_by_location (state, city);

-- Indexes for olap_sales_time_product
CREATE INDEX idx_olap_sales_time_product_time ON olap.olap_sales_time_product (year, quarter, month);
CREATE INDEX idx_olap_sales_time_product_prod ON olap.olap_sales_time_product (product_key);

-- Indexes for olap_sales_time_customer
CREATE INDEX idx_olap_sales_time_customer_time ON olap.olap_sales_time_customer (year, quarter, month);
CREATE INDEX idx_olap_sales_time_customer_cust ON olap.olap_sales_time_customer (customer_type, customer_key);

-- Indexes for olap_sales_time_location
CREATE INDEX idx_olap_sales_time_location_time ON olap.olap_sales_time_location (year, quarter, month);
CREATE INDEX idx_olap_sales_time_location_loc ON olap.olap_sales_time_location (state, city);

-- Indexes for olap_sales_product_customer
CREATE INDEX idx_olap_sales_product_customer_cust ON olap.olap_sales_product_customer (customer_type, customer_key);
CREATE INDEX idx_olap_sales_product_customer_prod ON olap.olap_sales_product_customer (product_key);

-- Indexes for olap_sales_product_location
CREATE INDEX idx_olap_sales_product_location_loc ON olap.olap_sales_product_location (state, city);
CREATE INDEX idx_olap_sales_product_location_prod ON olap.olap_sales_product_location (product_key);

-- Indexes for olap_sales_customer_location
CREATE INDEX idx_olap_sales_customer_location_cust ON olap.olap_sales_customer_location (customer_type, customer_key);
CREATE INDEX idx_olap_sales_customer_location_loc ON olap.olap_sales_customer_location (state, city);

-- Indexes for olap_sales_time_product_customer
CREATE INDEX idx_olap_sales_time_product_customer_time ON olap.olap_sales_time_product_customer (year, quarter, month);
CREATE INDEX idx_olap_sales_time_product_customer_cust ON olap.olap_sales_time_product_customer (customer_type, customer_key);
CREATE INDEX idx_olap_sales_time_product_customer_prod ON olap.olap_sales_time_product_customer (product_key);

-- Indexes for olap_sales_time_product_location
CREATE INDEX idx_olap_sales_time_product_location_time ON olap.olap_sales_time_product_location (year, quarter, month);
CREATE INDEX idx_olap_sales_time_product_location_loc ON olap.olap_sales_time_product_location (state, city);
CREATE INDEX idx_olap_sales_time_product_location_prod ON olap.olap_sales_time_product_location (product_key);

-- Indexes for olap_sales_time_customer_location
CREATE INDEX idx_olap_sales_time_customer_location_time ON olap.olap_sales_time_customer_location (year, quarter, month);
CREATE INDEX idx_olap_sales_time_customer_location_cust ON olap.olap_sales_time_customer_location (customer_type, customer_key);
CREATE INDEX idx_olap_sales_time_customer_location_loc ON olap.olap_sales_time_customer_location (state, city);

-- Indexes for olap_sales_product_customer_location
CREATE INDEX idx_olap_sales_product_customer_location_cust ON olap.olap_sales_product_customer_location (customer_type, customer_key);
CREATE INDEX idx_olap_sales_product_customer_location_loc ON olap.olap_sales_product_customer_location (state, city);
CREATE INDEX idx_olap_sales_product_customer_location_prod ON olap.olap_sales_product_customer_location (product_key);

-- Indexes for olap_sales_base
CREATE INDEX idx_olap_sales_base_time ON olap.olap_sales_base (year, quarter, month);
CREATE INDEX idx_olap_sales_base_cust ON olap.olap_sales_base (customer_type, customer_key);
CREATE INDEX idx_olap_sales_base_loc ON olap.olap_sales_base (state, city);
CREATE INDEX idx_olap_sales_base_prod ON olap.olap_sales_base (product_key);

-- Indexes for olap_inv_by_time
CREATE INDEX idx_olap_inv_by_time_time ON olap.olap_inv_by_time (year, quarter, month);

-- Indexes for olap_inv_by_product
CREATE INDEX idx_olap_inv_by_product_prod ON olap.olap_inv_by_product (product_key);

-- Indexes for olap_inv_by_store
CREATE INDEX idx_olap_inv_by_store_store ON olap.olap_inv_by_store (store_key);

-- Indexes for olap_inv_time_product
CREATE INDEX idx_olap_inv_time_product_time ON olap.olap_inv_time_product (year, quarter, month);
CREATE INDEX idx_olap_inv_time_product_prod ON olap.olap_inv_time_product (product_key);

-- Indexes for olap_inv_time_store
CREATE INDEX idx_olap_inv_time_store_time ON olap.olap_inv_time_store (year, quarter, month);
CREATE INDEX idx_olap_inv_time_store_store ON olap.olap_inv_time_store (store_key);

-- Indexes for olap_inv_product_store
CREATE INDEX idx_olap_inv_product_store_prod ON olap.olap_inv_product_store (product_key);
CREATE INDEX idx_olap_inv_product_store_store ON olap.olap_inv_product_store (store_key);

-- Indexes for olap_inv_base
CREATE INDEX idx_olap_inv_base_time ON olap.olap_inv_base (year, quarter, month);
CREATE INDEX idx_olap_inv_base_prod ON olap.olap_inv_base (product_key);
CREATE INDEX idx_olap_inv_base_store ON olap.olap_inv_base (store_key);
CREATE INDEX idx_olap_inv_base_loc ON olap.olap_inv_base (state, city);

-- Indexes for olap_inv_by_location
CREATE INDEX idx_olap_inv_by_location_loc ON olap.olap_inv_by_location (state, city);

-- Indexes for olap_inv_time_location
CREATE INDEX idx_olap_inv_time_location_time ON olap.olap_inv_time_location (year, quarter, month);
CREATE INDEX idx_olap_inv_time_location_loc ON olap.olap_inv_time_location (state, city);

-- Indexes for olap_inv_product_location
CREATE INDEX idx_olap_inv_product_location_prod ON olap.olap_inv_product_location (product_key);
CREATE INDEX idx_olap_inv_product_location_loc ON olap.olap_inv_product_location (state, city);

-- Indexes for olap_inv_store_location
CREATE INDEX idx_olap_inv_store_location_store ON olap.olap_inv_store_location (store_key);
CREATE INDEX idx_olap_inv_store_location_loc ON olap.olap_inv_store_location (state, city);

-- Indexes for olap_inv_time_product_store
CREATE INDEX idx_olap_inv_time_product_store_time ON olap.olap_inv_time_product_store (year, quarter, month);
CREATE INDEX idx_olap_inv_time_product_store_prod ON olap.olap_inv_time_product_store (product_key);
CREATE INDEX idx_olap_inv_time_product_store_store ON olap.olap_inv_time_product_store (store_key);

-- Indexes for olap_inv_time_product_location
CREATE INDEX idx_olap_inv_time_product_location_time ON olap.olap_inv_time_product_location (year, quarter, month);
CREATE INDEX idx_olap_inv_time_product_location_prod ON olap.olap_inv_time_product_location (product_key);
CREATE INDEX idx_olap_inv_time_product_location_loc ON olap.olap_inv_time_product_location (state, city);

-- Indexes for olap_inv_time_store_location
CREATE INDEX idx_olap_inv_time_store_location_time ON olap.olap_inv_time_store_location (year, quarter, month);
CREATE INDEX idx_olap_inv_time_store_location_store ON olap.olap_inv_time_store_location (store_key);
CREATE INDEX idx_olap_inv_time_store_location_loc ON olap.olap_inv_time_store_location (state, city);

-- Indexes for olap_inv_product_store_location
CREATE INDEX idx_olap_inv_product_store_location_prod ON olap.olap_inv_product_store_location (product_key);
CREATE INDEX idx_olap_inv_product_store_location_store ON olap.olap_inv_product_store_location (store_key);
CREATE INDEX idx_olap_inv_product_store_location_loc ON olap.olap_inv_product_store_location (state, city);
