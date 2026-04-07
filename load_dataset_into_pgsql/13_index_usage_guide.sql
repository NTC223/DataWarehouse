-- ============================================================
-- Cách xem, export và query sử dụng Index đã tạo
-- ============================================================

-- ─────────────────────────────────────────────
-- 1. XEM TẤT CẢ INDEX HIỆN CÓ (idb, dwh, metadata)
-- ─────────────────────────────────────────────
SELECT
    schemaname      AS schema_name,
    tablename       AS table_name,
    indexname       AS index_name,
    indexdef        AS index_definition
FROM pg_indexes
WHERE schemaname IN ('idb', 'dwh', 'metadata')
ORDER BY schemaname, tablename, indexname;


-- ─────────────────────────────────────────────
-- 2. EXPORT CẤU TRÚC INDEX RA FILE (chạy từ terminal)
-- ─────────────────────────────────────────────
-- Cách 1: pg_dump chỉ lấy schema (indexes + DDL), không lấy data
--   docker exec pgsql pg_dump -U admin -s -n idb -n dwh -n metadata postgres > index_export.sql
--
-- Cách 2: Chỉ lấy đúng phần CREATE INDEX
--   docker exec pgsql psql -U admin -d postgres -c "
--     SELECT indexdef || ';' FROM pg_indexes
--     WHERE schemaname IN ('idb','dwh','metadata')
--     ORDER BY schemaname, tablename;
--   " -t > index_definitions.sql


-- ─────────────────────────────────────────────
-- 3. KIỂM TRA INDEX CÓ ĐƯỢC DÙNG KHÔNG (EXPLAIN)
-- ─────────────────────────────────────────────

-- Ví dụ 1: Fact_Sales theo time_key → idx_dwh_fact_sales_time
EXPLAIN ANALYZE
SELECT product_key, SUM(total_amount) AS revenue
FROM dwh.Fact_Sales
WHERE time_key >= 20240101 AND time_key <= 20241231
GROUP BY product_key
ORDER BY revenue DESC;

-- Ví dụ 2: Drill down Dim_Time Year → Quarter → Month
EXPLAIN ANALYZE
SELECT t.year, t.quarter, t.month, SUM(f.total_amount)
FROM dwh.Fact_Sales f
JOIN dwh.Dim_Time t ON f.time_key = t.time_key
WHERE t.year = 2024
GROUP BY t.year, t.quarter, t.month
ORDER BY t.year, t.quarter, t.month;

-- Ví dụ 3: Roll up Geographic (City → State)
EXPLAIN ANALYZE
SELECT l.state, l.city, SUM(f.total_amount) AS revenue
FROM dwh.Fact_Sales f
JOIN dwh.Dim_Customer c ON f.customer_key = c.customer_key
JOIN dwh.Dim_Location l ON c.location_key  = l.location_key
WHERE l.state = 'Hà Nội'
GROUP BY l.state, l.city
ORDER BY revenue DESC;

-- Ví dụ 4: Slice & Dice — customer_type + time
EXPLAIN ANALYZE
SELECT
    c.customer_type,
    t.year,
    t.quarter,
    COUNT(DISTINCT f.customer_key) AS num_customers,
    SUM(f.total_amount)             AS revenue
FROM dwh.Fact_Sales f
JOIN dwh.Dim_Customer c ON f.customer_key = c.customer_key
JOIN dwh.Dim_Time     t ON f.time_key     = t.time_key
WHERE c.customer_type IN ('Tourist', 'Both')
  AND t.year = 2024
GROUP BY c.customer_type, t.year, t.quarter
ORDER BY t.quarter;


-- ─────────────────────────────────────────────
-- 4. XEM INDEX SIZE
-- ─────────────────────────────────────────────
SELECT
    n.nspname                                       AS schema_name,
    t.relname                                       AS table_name,
    i.relname                                       AS index_name,
    pg_size_pretty(pg_relation_size(ix.indexrelid)) AS index_size
FROM pg_index ix
JOIN pg_class t  ON t.oid  = ix.indrelid
JOIN pg_class i  ON i.oid  = ix.indexrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname IN ('idb', 'dwh', 'metadata')
ORDER BY pg_relation_size(ix.indexrelid) DESC;


-- ─────────────────────────────────────────────
-- 5. ANALYZE để cập nhật pg_stats
-- ─────────────────────────────────────────────
ANALYZE dwh.Fact_Sales;
ANALYZE dwh.Fact_Inventory;
ANALYZE dwh.Dim_Time;
ANALYZE dwh.Dim_Location;
ANALYZE dwh.Dim_Customer;
ANALYZE dwh.Dim_Store;
ANALYZE dwh.Dim_Product;


-- ─────────────────────────────────────────────
-- 6. QUERY METADATA (ví dụ sử dụng metadata indexes)
-- ─────────────────────────────────────────────

-- 6a. Xem cấu trúc warehouse (hierarchy tree)
SELECT
    ws.id,
    ws.object_type,
    ws.object_name,
    ws.schema_name,
    p.object_name  AS parent_name,
    ws.hierarchy_level,
    ws.columns_info->>'column_name' AS mapped_column
FROM metadata.warehouse_structure ws
LEFT JOIN metadata.warehouse_structure p ON ws.parent_id = p.id
ORDER BY ws.schema_name NULLS LAST, ws.parent_id NULLS FIRST, ws.hierarchy_level NULLS FIRST;

-- 6b. Xem lineage + trạng thái data freshness
SELECT
    dl.data_object,
    dl.source_system,
    dl.source_object,
    dl.data_currency,
    dl.last_refreshed,
    CASE
        WHEN dl.last_refreshed IS NULL THEN 'never loaded'
        WHEN dl.last_refreshed < CURRENT_TIMESTAMP - INTERVAL '2 days' THEN 'stale (>2d)'
        ELSE 'fresh'
    END AS freshness
FROM metadata.data_lineage dl
ORDER BY dl.data_object;

-- 6c. Xem ETL log gần nhất theo pipeline
SELECT pipeline_stage, job_name, status, rows_inserted, rows_updated, query_time_ms, run_time
FROM metadata.etl_log
ORDER BY run_time DESC
LIMIT 20;

-- 6d. Xem data dictionary mapping theo pipeline
SELECT pipeline_stage, source_schema, source_table, source_column,
       target_schema, target_table, target_column, transformation_rule
FROM metadata.data_dictionary
WHERE pipeline_stage = 'source_to_idb'
ORDER BY source_table, source_column;
