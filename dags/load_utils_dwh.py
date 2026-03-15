from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)

def load_dimension_or_fact(target_table, extract_sql, target_columns, is_fact=False):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn_pg = pg_hook.get_conn()
    cursor_pg = conn_pg.cursor()
    cursor_pg.execute(f"SELECT last_load_time FROM metadata_idb_to_dwh.etl_watermark WHERE table_name = '{target_table}'")
    result = cursor_pg.fetchone()
    last_runtime = result[0] if result else '1970-01-01 00:00:00'
    formatted_sql = extract_sql.format(last_runtime=last_runtime)
    df = pg_hook.get_pandas_df(formatted_sql)
    rows_inserted = 0
    if not df.empty:
        df = df.where(pd.notnull(df), None)
        records = [tuple(x) for x in df.to_numpy()]
        col_names = ', '.join(target_columns)
        placeholders = ', '.join(['%s'] * len(target_columns))
        insert_query = f"INSERT INTO dwh.{target_table} ({col_names}) VALUES ({placeholders});"
        from psycopg2.extras import execute_batch
        execute_batch(cursor_pg, insert_query, records)
        rows_inserted = len(records)
        cursor_pg.execute(f"INSERT INTO metadata_idb_to_dwh.etl_watermark (table_name, last_load_time) VALUES ('{target_table}', CURRENT_TIMESTAMP) ON CONFLICT (table_name) DO UPDATE SET last_load_time = CURRENT_TIMESTAMP;")
    cursor_pg.execute(f"INSERT INTO metadata_idb_to_dwh.etl_log (job_name, status, rows_inserted, error_message) VALUES ('Load_{target_table}_To_DWH', 'SUCCESS', {rows_inserted}, NULL);")
    conn_pg.commit()
    cursor_pg.close()
    conn_pg.close()
