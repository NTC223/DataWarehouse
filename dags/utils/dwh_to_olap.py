from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import register_adapter, AsIs
import pandas as pd
import numpy as np
import traceback
import time

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)


def load_olap_cuboid(cuboid_table: str, extract_sql: str, target_columns: list):
    """
    Load một cuboid table vào schema `olap`.

    Strategy: TRUNCATE + INSERT (full refresh).
    Bởi vì OLAP cuboid là aggregated data (GROUP BY), TRUNCATE + INSERT
    là an toàn nhất để tránh double-counting.

    Args:
        cuboid_table  : Tên bảng trong schema olap (VD: 'olap_sales_all')
        extract_sql   : SQL truy vấn từ schema dwh để tính aggregation
        target_columns: Danh sách tên cột tương ứng với SELECT của extract_sql
    """

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn_pg = pg_hook.get_conn()
    cursor_pg = conn_pg.cursor()

    try:
        # ── Extract từ DWH ───────────────────────────────────
        t0 = time.time()
        df = pg_hook.get_pandas_df(extract_sql)
        etl_extract_ms = round((time.time() - t0) * 1000, 2)

        rows_inserted = 0

        # ── TRUNCATE rồi INSERT (full refresh) ───────────────
        cursor_pg.execute(f"TRUNCATE TABLE olap.{cuboid_table};")

        if not df.empty:
            df = df.where(pd.notnull(df), None)
            records = [tuple(x) for x in df.to_numpy()]

            col_names    = ', '.join(target_columns)
            placeholders = ', '.join(['%s'] * len(target_columns))

            insert_query = (
                f"INSERT INTO olap.{cuboid_table} ({col_names}) "
                f"VALUES ({placeholders});"
            )
            cursor_pg.executemany(insert_query, records)
            rows_inserted = len(records)

        # ── Cập nhật watermark ───────────────────────────────
        cursor_pg.execute(
            "INSERT INTO metadata.etl_watermark (pipeline_stage, table_name, last_load_time) "
            "VALUES ('dwh_to_olap', %s, CURRENT_TIMESTAMP) "
            "ON CONFLICT (pipeline_stage, table_name) DO UPDATE SET last_load_time = CURRENT_TIMESTAMP;",
            (cuboid_table,)
        )

        # ── Ghi log SUCCESS ──────────────────────────────────
        cursor_pg.execute(
            "INSERT INTO metadata.etl_log "
            "(pipeline_stage, job_name, status, rows_inserted, rows_updated, query_time_ms, error_message) "
            "VALUES ('dwh_to_olap', %s, 'SUCCESS', %s, 0, %s, NULL);",
            (f'Load_{cuboid_table}_To_OLAP', rows_inserted, etl_extract_ms)
        )

        # ── Cập nhật data_lineage.last_refreshed ─────────────
        cursor_pg.execute(
            "UPDATE metadata.data_lineage "
            "SET last_refreshed = CURRENT_TIMESTAMP, data_currency = 'active' "
            "WHERE data_object = %s",
            (f'olap.{cuboid_table}',)
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
                "VALUES ('dwh_to_olap', %s, 'FAILED', 0, 0, NULL, %s);",
                (f'Load_{cuboid_table}_To_OLAP', error_msg)
            )
            conn_pg.commit()
        except Exception:
            pass

        raise

    finally:
        cursor_pg.close()
        conn_pg.close()
