-- ============================================================
-- File 12: Indexes trên IDB, DWH và Metadata
-- ============================================================

-- ─────────────────────────────────────────────
-- IDB Indexes (tăng tốc ETL join & watermark lookup)
-- ─────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_idb_customer_city
    ON idb.Customer(city_id);

CREATE INDEX IF NOT EXISTS idx_idb_store_city
    ON idb.Store(city_id);

CREATE INDEX IF NOT EXISTS idx_idb_order_customer
    ON idb."Order"(customer_id);

CREATE INDEX IF NOT EXISTS idx_idb_order_date
    ON idb."Order"(order_date);

CREATE INDEX IF NOT EXISTS idx_idb_orderproduct_order
    ON idb.OrderProduct(order_id);

CREATE INDEX IF NOT EXISTS idx_idb_orderproduct_product
    ON idb.OrderProduct(product_id);

CREATE INDEX IF NOT EXISTS idx_idb_orderproduct_time
    ON idb.OrderProduct(last_updated_time);

CREATE INDEX IF NOT EXISTS idx_idb_stocked_store
    ON idb.StockedProduct(store_id);

CREATE INDEX IF NOT EXISTS idx_idb_stocked_product
    ON idb.StockedProduct(product_id);

CREATE INDEX IF NOT EXISTS idx_idb_stocked_time
    ON idb.StockedProduct(last_updated_time);

CREATE INDEX IF NOT EXISTS idx_idb_repoffice_time
    ON idb.RepresentativeOffice(last_updated_time);

CREATE INDEX IF NOT EXISTS idx_idb_product_time
    ON idb.Product(last_updated_time);

-- ─────────────────────────────────────────────
-- DWH Indexes (tăng tốc OLAP query & drill down)
-- ─────────────────────────────────────────────

-- Fact_Sales: FK indexes
CREATE INDEX IF NOT EXISTS idx_dwh_fact_sales_time
    ON dwh.Fact_Sales(time_key);

CREATE INDEX IF NOT EXISTS idx_dwh_fact_sales_product
    ON dwh.Fact_Sales(product_key);

CREATE INDEX IF NOT EXISTS idx_dwh_fact_sales_customer
    ON dwh.Fact_Sales(customer_key);

-- Fact_Inventory: FK indexes
CREATE INDEX IF NOT EXISTS idx_dwh_fact_inv_time
    ON dwh.Fact_Inventory(time_key);

CREATE INDEX IF NOT EXISTS idx_dwh_fact_inv_store
    ON dwh.Fact_Inventory(store_key);

CREATE INDEX IF NOT EXISTS idx_dwh_fact_inv_product
    ON dwh.Fact_Inventory(product_key);

-- Dim_Location: hỗ trợ drill up state → city
CREATE INDEX IF NOT EXISTS idx_dwh_dimloc_state
    ON dwh.Dim_Location(state);

CREATE INDEX IF NOT EXISTS idx_dwh_dimloc_city
    ON dwh.Dim_Location(city);

-- Dim_Time: hỗ trợ roll up/drill down year → quarter → month
CREATE INDEX IF NOT EXISTS idx_dwh_dimtime_year
    ON dwh.Dim_Time(year);

CREATE INDEX IF NOT EXISTS idx_dwh_dimtime_quarter
    ON dwh.Dim_Time(year, quarter);

CREATE INDEX IF NOT EXISTS idx_dwh_dimtime_month
    ON dwh.Dim_Time(year, quarter, month);

-- Dim_Customer: filter theo customer_type
CREATE INDEX IF NOT EXISTS idx_dwh_dimcust_type
    ON dwh.Dim_Customer(customer_type);

CREATE INDEX IF NOT EXISTS idx_dwh_dimcust_location
    ON dwh.Dim_Customer(location_key);

-- Dim_Store: join với location
CREATE INDEX IF NOT EXISTS idx_dwh_dimstore_location
    ON dwh.Dim_Store(location_key);

-- Dim_Product: filter theo size
CREATE INDEX IF NOT EXISTS idx_dwh_dimprod_size
    ON dwh.Dim_Product(size);

-- ─────────────────────────────────────────────
-- Metadata Indexes
-- ─────────────────────────────────────────────

-- warehouse_structure: lookup theo schema + type, parent tree
CREATE INDEX IF NOT EXISTS idx_meta_ws_schema_type
    ON metadata.warehouse_structure(schema_name, object_type);

CREATE INDEX IF NOT EXISTS idx_meta_ws_parent
    ON metadata.warehouse_structure(parent_id);

-- data_lineage: tra cứu nhanh theo data_object
CREATE INDEX IF NOT EXISTS idx_meta_lineage_object
    ON metadata.data_lineage(data_object);

-- data_dictionary: filter theo pipeline + source table
CREATE INDEX IF NOT EXISTS idx_meta_dict_pipeline_source
    ON metadata.data_dictionary(pipeline_stage, source_table);

-- etl_log: xem log mới nhất theo pipeline
CREATE INDEX IF NOT EXISTS idx_meta_etl_log_pipeline_time
    ON metadata.etl_log(pipeline_stage, run_time DESC);

-- etl_log: filter theo job + status (debug failed jobs)
CREATE INDEX IF NOT EXISTS idx_meta_etl_log_job_status
    ON metadata.etl_log(job_name, status);
