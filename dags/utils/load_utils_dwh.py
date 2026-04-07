from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import register_adapter, AsIs
import pandas as pd
import numpy as np
import traceback
import time

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)

def load_dimension_or_fact(target_table, extract_sql, target_columns,
                           pk_conflict_action="DO NOTHING"):

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn_pg = pg_hook.get_conn()
    cursor_pg = conn_pg.cursor()

    try:
        # ── Lấy watermark ────────────────────────────────────
        cursor_pg.execute(
            "SELECT last_load_time FROM metadata.etl_watermark "
            "WHERE pipeline_stage = 'idb_to_dwh' AND table_name = %s",
            (target_table,)
        )
        result = cursor_pg.fetchone()
        last_runtime = result[0] if result else '1970-01-01 00:00:00'

        # ── Extract từ IDB ───────────────────────────────────
        formatted_sql = extract_sql.format(last_runtime=last_runtime)
        t0 = time.time()
        df = pg_hook.get_pandas_df(formatted_sql)
        etl_extract_ms = round((time.time() - t0) * 1000, 2)  # thời gian ETL extract (ms)

        rows_inserted = 0
        rows_updated  = 0
        if not df.empty:
            df = df.where(pd.notnull(df), None)
            records = [tuple(x) for x in df.to_numpy()]

            col_names    = ', '.join(target_columns)
            placeholders = ', '.join(['%s'] * len(target_columns))

            # ✅ RETURNING xmax để phân biệt insert thật vs ON CONFLICT update:
            # xmax = 0 → row mới (inserted), xmax != 0 → row đã có (updated)
            insert_query = (
                f"INSERT INTO dwh.{target_table} ({col_names}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT {pk_conflict_action} "
                f"RETURNING (xmax = 0) AS is_inserted;"
            )
            for record in records:
                cursor_pg.execute(insert_query, record)
                result_row = cursor_pg.fetchone()
                if result_row is None:              # DO NOTHING → row bị bỏ qua
                    pass
                elif result_row[0]:                 # xmax=0 → inserted
                    rows_inserted += 1
                else:                               # xmax!=0 → updated
                    rows_updated  += 1

        # ── Cập nhật watermark ───────────────────────────────
        cursor_pg.execute(
            "INSERT INTO metadata.etl_watermark (pipeline_stage, table_name, last_load_time) "
            "VALUES ('idb_to_dwh', %s, CURRENT_TIMESTAMP) "
            "ON CONFLICT (pipeline_stage, table_name) DO UPDATE SET last_load_time = CURRENT_TIMESTAMP;",
            (target_table,)
        )

        # ── Ghi log SUCCESS ──────────────────────────────────
        cursor_pg.execute(
            "INSERT INTO metadata.etl_log "
            "(pipeline_stage, job_name, status, rows_inserted, rows_updated, query_time_ms, error_message) "
            "VALUES ('idb_to_dwh', %s, 'SUCCESS', %s, %s, %s, NULL);",
            (f'Load_{target_table}_To_DWH', rows_inserted, rows_updated, etl_extract_ms)
        )

        # ── Cập nhật data_lineage.last_refreshed ─────────────
        cursor_pg.execute(
            "UPDATE metadata.data_lineage "
            "SET last_refreshed = CURRENT_TIMESTAMP, data_currency = 'active' "
            "WHERE data_object = %s",
            (f'dwh.{target_table}',)
        )

        conn_pg.commit()

        # ── Refresh performance_metadata sau mỗi lần load ────
        try:
            cursor_pg.execute("SELECT metadata.refresh_performance_metadata();")
            conn_pg.commit()
        except Exception:
            pass

    except Exception as e:
        conn_pg.rollback()

        error_msg = traceback.format_exc()[:500]
        try:
            cursor_pg.execute(
                "INSERT INTO metadata.etl_log "
                "(pipeline_stage, job_name, status, rows_inserted, rows_updated, query_time_ms, error_message) "
                "VALUES ('idb_to_dwh', %s, 'FAILED', 0, 0, NULL, %s);",
                (f'Load_{target_table}_To_DWH', error_msg)
            )
            conn_pg.commit()
        except Exception:
            pass

        raise

    finally:
        cursor_pg.close()
        conn_pg.close()