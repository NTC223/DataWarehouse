from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

def load_table(source_conn_id, target_table, extract_sql, target_columns, pk_conflict_action="DO NOTHING"):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    source_hook = MySqlHook(mysql_conn_id='mysql_default') if source_conn_id == 'mysql_default' else PostgresHook(postgres_conn_id='postgres_default')
    conn_pg = pg_hook.get_conn()
    cursor_pg = conn_pg.cursor()
    cursor_pg.execute(f"SELECT last_load_time FROM metadata_source_to_idb.etl_watermark WHERE table_name = '{target_table}'")
    result = cursor_pg.fetchone()
    last_runtime = result[0] if result else '1970-01-01 00:00:00'
    formatted_sql = extract_sql.format(last_runtime=last_runtime)
    source_df = source_hook.get_pandas_df(formatted_sql)
    rows_inserted = 0
    if not source_df.empty:
        source_df = source_df.where(pd.notnull(source_df), None)
        records = [tuple(x) for x in source_df.to_numpy()]
        col_names = ', '.join(target_columns)
        placeholders = ', '.join(['%s'] * len(target_columns))
        tbl = f'"{target_table}"' if target_table == 'Order' else target_table
        insert_query = f'INSERT INTO idb.{tbl} ({col_names}) VALUES ({placeholders}) ON CONFLICT {pk_conflict_action};'
        from psycopg2.extras import execute_batch
        execute_batch(cursor_pg, insert_query, records)
        rows_inserted = len(records)
        cursor_pg.execute(f"INSERT INTO metadata_source_to_idb.etl_watermark (table_name, last_load_time) VALUES ('{target_table}', CURRENT_TIMESTAMP) ON CONFLICT (table_name) DO UPDATE SET last_load_time = CURRENT_TIMESTAMP;")
    cursor_pg.execute(f"INSERT INTO metadata_source_to_idb.etl_log (job_name, status, rows_inserted, error_message) VALUES ('Load_{target_table}_To_IDB', 'SUCCESS', {rows_inserted}, NULL);")
    conn_pg.commit()
    cursor_pg.close()
    conn_pg.close()
