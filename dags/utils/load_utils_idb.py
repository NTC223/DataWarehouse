from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import traceback
import time

def load_table(source_conn_id, target_table, extract_sql, target_columns,
               pk_conflict_action="DO NOTHING"):

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    if source_conn_id == 'mysql_default':
        source_hook = MySqlHook(mysql_conn_id=source_conn_id)
    else:
        source_hook = PostgresHook(postgres_conn_id=source_conn_id)

    conn_pg = pg_hook.get_conn()
    cursor_pg = conn_pg.cursor()

    try:
        # ── Lấy watermark ────────────────────────────────────
        cursor_pg.execute(
            "SELECT last_load_time FROM metadata.etl_watermark "
            "WHERE pipeline_stage = 'source_to_idb' AND table_name = %s",
            (target_table,)
        )
        result = cursor_pg.fetchone()
        last_runtime = result[0] if result else '1970-01-01 00:00:00'

        # ── Extract từ nguồn ─────────────────────────────────
        formatted_sql = extract_sql.format(last_runtime=last_runtime)
        
        t0 = time.time()
        source_df = source_hook.get_pandas_df(formatted_sql)
        etl_extract_ms = round((time.time() - t0) * 1000, 2)  # thời gian ETL extract (ms)

        rows_inserted = 0
        rows_updated  = 0
        if not source_df.empty:
            source_df = source_df.where(pd.notnull(source_df), None)
            records = [tuple(x) for x in source_df.to_numpy()]

            col_names    = ', '.join(target_columns)
            placeholders = ', '.join(['%s'] * len(target_columns))

            # ✅ Xử lý bảng "Order" cần quoted vì là reserved word
            tbl = f'"Order"' if target_table == 'Order' else target_table

            # ✅ Dùng RETURNING xmax để phân biệt INSERT thật vs ON CONFLICT UPDATE
            # xmax = 0 → row mới (inserted), xmax != 0 → row đã tồn tại (updated)
            insert_query = (
                f"INSERT INTO idb.{tbl} ({col_names}) "
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
            "VALUES ('source_to_idb', %s, CURRENT_TIMESTAMP) "
            "ON CONFLICT (pipeline_stage, table_name) DO UPDATE SET last_load_time = CURRENT_TIMESTAMP;",
            (target_table,)
        )

        # ── Ghi log SUCCESS ──────────────────────────────────
        cursor_pg.execute(
            "INSERT INTO metadata.etl_log "
            "(pipeline_stage, job_name, status, rows_inserted, rows_updated, query_time_ms, error_message) "
            "VALUES ('source_to_idb', %s, 'SUCCESS', %s, %s, %s, NULL);",
            (f'Load_{target_table}_To_IDB', rows_inserted, rows_updated, etl_extract_ms)
        )

        # ── Cập nhật data_lineage.last_refreshed ─────────────
        cursor_pg.execute(
            "UPDATE metadata.data_lineage "
            "SET last_refreshed = CURRENT_TIMESTAMP, data_currency = 'active' "
            "WHERE data_object = %s",
            (f'idb.{target_table}',)
        )

        conn_pg.commit()

        # ── ANALYZE để cập nhật row_count thực tế cho performance_metadata ──
        tbl_analyze = f'"Order"' if target_table == 'Order' else target_table
        try:
            cursor_pg.execute(f'ANALYZE idb.{tbl_analyze};')
            conn_pg.commit()
        except Exception:
            pass

        # ── Refresh performance_metadata ─────────────────────
        try:
            cursor_pg.execute('SELECT metadata.refresh_performance_metadata();')
            conn_pg.commit()
        except Exception:
            pass   # không fail task nếu refresh lỗi

    except Exception as e:
        conn_pg.rollback()      # ✅ rollback ngay khi lỗi, tránh dirty data

        # ✅ Ghi log FAILED với đầy đủ traceback để debug
        error_msg = traceback.format_exc()[:500]    # giới hạn 500 ký tự
        try:
            cursor_pg.execute(
                "INSERT INTO metadata.etl_log "
                "(pipeline_stage, job_name, status, rows_inserted, rows_updated, query_time_ms, error_message) "
                "VALUES ('source_to_idb', %s, 'FAILED', 0, 0, NULL, %s);",
                (f'Load_{target_table}_To_IDB', error_msg)
            )
            conn_pg.commit()
        except Exception:
            pass    # nếu ghi log cũng lỗi thì bỏ qua, raise lỗi gốc

        raise   # ✅ raise để Airflow task biết là FAILED, không bị mark SUCCESS

    finally:
        cursor_pg.close()
        conn_pg.close()