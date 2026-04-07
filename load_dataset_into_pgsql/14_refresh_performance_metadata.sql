-- ============================================================
-- File 14: Function refresh_performance_metadata()
-- Đo thời gian SELECT COUNT(*) thực tế trên từng bảng IDB/DWH
-- và UPSERT vào metadata.performance_metadata.
--
-- Cách dùng:
--   SELECT metadata.refresh_performance_metadata();
--   (được gọi tự động sau mỗi lần ETL thành công)
-- ============================================================

SET search_path TO metadata;

CREATE OR REPLACE FUNCTION metadata.refresh_performance_metadata()
RETURNS void
LANGUAGE plpgsql
SET search_path TO metadata, public
AS $$
DECLARE
    rec       RECORD;
    t_start   timestamp;
    t_end     timestamp;
    q_ms      numeric(10,2);
    dummy_cnt bigint;
BEGIN
    FOR rec IN
        SELECT
            t.oid,
            t.relname   AS obj,
            t.relkind,
            n.nspname   AS sch,
            COALESCE(s.n_live_tup, t.reltuples::bigint, 0) AS rcount,
            s.last_analyze,
            (
                SELECT string_agg(i.relname, ', ' ORDER BY i.relname)
                FROM pg_index ix
                JOIN pg_class i ON i.oid = ix.indexrelid
                WHERE ix.indrelid = t.oid
                  AND NOT ix.indisprimary
            ) AS idx_info
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = t.oid
        WHERE n.nspname IN ('idb', 'dwh')
          AND t.relkind IN ('r', 'm', 'v')
        ORDER BY n.nspname, t.relname
    LOOP
        -- Đo thời gian SELECT COUNT(*) thực tế
        t_start := clock_timestamp();
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', rec.sch, rec.obj) INTO dummy_cnt;
        t_end   := clock_timestamp();
        q_ms    := round(EXTRACT(EPOCH FROM (t_end - t_start)) * 1000, 2);

        -- UPSERT: insert mới hoặc update nếu đã tồn tại
        INSERT INTO metadata.performance_metadata
            (object_name, object_type, schema_name, row_count,
             avg_query_time_ms, last_analyzed, index_info, notes, captured_at)
        VALUES (
            rec.obj,
            CASE rec.relkind
                WHEN 'r' THEN 'table'
                WHEN 'm' THEN 'materialized_view'
                ELSE 'view'
            END,
            rec.sch,
            rec.rcount,
            q_ms,
            rec.last_analyze,
            rec.idx_info,
            'auto-refreshed by metadata.refresh_performance_metadata()',
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (schema_name, object_name) DO UPDATE SET
            object_type       = EXCLUDED.object_type,
            row_count         = EXCLUDED.row_count,
            avg_query_time_ms = EXCLUDED.avg_query_time_ms,
            last_analyzed     = EXCLUDED.last_analyzed,
            index_info        = EXCLUDED.index_info,
            notes             = EXCLUDED.notes,
            captured_at       = EXCLUDED.captured_at;
    END LOOP;

    RAISE NOTICE 'performance_metadata refreshed: % rows',
        (SELECT COUNT(*) FROM metadata.performance_metadata);
END;
$$;

-- Chạy lần đầu để khởi tạo dữ liệu ngay khi init
SELECT metadata.refresh_performance_metadata();
