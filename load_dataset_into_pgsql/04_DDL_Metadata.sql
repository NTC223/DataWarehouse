-- ============================================================
-- File 04: DDL cho toàn bộ hệ thống Metadata (unified schema)
-- Gộp tất cả metadata vào 1 schema duy nhất: metadata
-- ============================================================

CREATE SCHEMA IF NOT EXISTS metadata;
SET search_path TO metadata;

-- ── 1. Cấu trúc kho + Hierarchy (self-referencing) ──────────
CREATE TABLE warehouse_structure (
    id              serial PRIMARY KEY,
    object_type     varchar(50)  NOT NULL,   -- 'schema','table','dimension','fact','hierarchy','hierarchy_level'
    object_name     varchar(100) NOT NULL,
    schema_name     varchar(50),             -- 'idb', 'dwh'
    parent_id       int REFERENCES warehouse_structure(id),
    description     text,
    columns_info    jsonb,                   -- JSON chi tiết cột / column_name cho hierarchy_level
    hierarchy_level int,                     -- NULL nếu không phải hierarchy, 1 = coarsest, N = finest
    created_at      timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at      timestamp DEFAULT CURRENT_TIMESTAMP
);

-- ── 2. Data Lineage (thay thế operational_metadata) ─────────
CREATE TABLE data_lineage (
    id                  serial PRIMARY KEY,
    data_object         varchar(100) NOT NULL,    -- 'idb.Customer', 'dwh.Fact_Sales'
    source_system       varchar(50),              -- 'MySQL', 'PostgreSQL', 'IDB'
    source_object       varchar(255),             -- 'Customer', 'idb.OrderProduct + idb.Order'
    transformation_path text,                     -- full lineage path
    data_currency       varchar(20) DEFAULT 'active',  -- 'active', 'stale', 'archived'
    refresh_frequency   varchar(50) DEFAULT 'daily',
    last_refreshed      timestamp,                -- auto-updated bởi ETL DAG
    created_at          timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at          timestamp DEFAULT CURRENT_TIMESTAMP
);

-- ── 3. Data Dictionary (gộp source→idb + idb→dwh) ──────────
CREATE TABLE data_dictionary (
    mapping_id          serial PRIMARY KEY,
    pipeline_stage      varchar(20) NOT NULL,     -- 'source_to_idb', 'idb_to_dwh'
    source_schema       varchar(50),              -- NULL (MySQL), 'sales_source', 'idb'
    source_table        varchar(100),
    source_column       varchar(100),
    source_data_type    varchar(50),
    target_schema       varchar(50),              -- 'idb', 'dwh'
    target_table        varchar(100),
    target_column       varchar(100),
    target_data_type    varchar(50),
    transformation_rule text,
    is_active           boolean DEFAULT TRUE
);

-- ── 4. ETL Watermark (gộp 2 bảng cũ, PK composite) ─────────
CREATE TABLE etl_watermark (
    pipeline_stage  varchar(20)  NOT NULL,     -- 'source_to_idb', 'idb_to_dwh'
    table_name      varchar(100) NOT NULL,
    last_load_time  timestamp,
    PRIMARY KEY (pipeline_stage, table_name)
);

-- ── 5. ETL Log (gộp 2 bảng cũ) ─────────────────────────────
CREATE TABLE etl_log (
    log_id          serial PRIMARY KEY,
    pipeline_stage  varchar(20) NOT NULL,      -- 'source_to_idb', 'idb_to_dwh'
    job_name        varchar(100),
    status          varchar(20),               -- 'SUCCESS', 'FAILED', 'RUNNING'
    rows_inserted   int,
    rows_updated    int,
    query_time_ms   numeric(10,2),
    error_message   text,
    run_time        timestamp DEFAULT CURRENT_TIMESTAMP
);

-- ── 6. Summarization Algorithm ──────────────────────────────
CREATE TABLE summarization_algorithm (
    id              serial PRIMARY KEY,
    algorithm_name  varchar(100),
    target_table    varchar(100),
    description     text,
    formula         text,
    created_at      timestamp DEFAULT CURRENT_TIMESTAMP
);

-- ── 7. Performance Metadata (UPSERT-ready) ──────────────────
CREATE TABLE performance_metadata (
    id              serial PRIMARY KEY,
    object_name     varchar(100) NOT NULL,
    object_type     varchar(50),
    schema_name     varchar(50)  NOT NULL,
    row_count       bigint,
    avg_query_time_ms numeric(10,2),
    last_analyzed   timestamp,
    index_info      text,
    notes           text,
    captured_at     timestamp DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_perf_schema_object UNIQUE (schema_name, object_name)
);

-- ── 8. Business Metadata ────────────────────────────────────
CREATE TABLE business_metadata (
    id                serial PRIMARY KEY,
    term_name         varchar(100),
    definition        text,
    data_owner        varchar(100),
    data_steward      varchar(100),
    sensitivity_level varchar(20),    -- 'public', 'internal', 'confidential'
    related_tables    text,
    charging_policy   text,
    created_at        timestamp DEFAULT CURRENT_TIMESTAMP
);

-- ── Trigger: auto-update updated_at ─────────────────────────
CREATE OR REPLACE FUNCTION metadata.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_warehouse_structure_updated
    BEFORE UPDATE ON warehouse_structure
    FOR EACH ROW EXECUTE FUNCTION metadata.set_updated_at();

CREATE TRIGGER trg_data_lineage_updated
    BEFORE UPDATE ON data_lineage
    FOR EACH ROW EXECUTE FUNCTION metadata.set_updated_at();
