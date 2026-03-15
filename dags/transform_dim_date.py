from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def transform_dim_date():
    """
    Sinh tự động dữ liệu cho bảng Dim_Date (Calendar Dimension).
    Tạo một dải ngày liên tục từ năm 2010 đến năm 2030.
    Kiểm tra nếu đã có dữ liệu trong Dim_Date thì bỏ qua (chỉ chạy một lần).
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn_pg = pg_hook.get_conn()
    cursor_pg = conn_pg.cursor()

    # Kiểm tra xem Dim_Date đã có dữ liệu chưa
    cursor_pg.execute("SELECT COUNT(*) FROM dwh.dim_date;")
    count = cursor_pg.fetchone()[0]

    if count > 0:
        print(f"Dim_Date already has {count} rows. Skipping generation.")
        cursor_pg.close()
        conn_pg.close()
        return

    # Sinh dải ngày từ 2010-01-01 đến 2030-12-31
    dates = pd.date_range(start='2010-01-01', end='2030-12-31', freq='D')

    records = []
    for d in dates:
        date_key = int(d.strftime('%Y%m%d'))  # Format YYYYMMDD
        quarter = (d.month - 1) // 3 + 1
        records.append((date_key, d.date(), d.day, d.month, quarter, d.year))

    from psycopg2.extras import execute_batch
    insert_query = """
        INSERT INTO dwh.dim_date (date_key, full_date, day, month, quarter, year)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_key) DO NOTHING;
    """
    execute_batch(cursor_pg, insert_query, records)

    cursor_pg.execute(f"""
        INSERT INTO metadata_idb_to_dwh.etl_log (job_name, status, rows_inserted, error_message)
        VALUES ('Generate_Dim_Date', 'SUCCESS', {len(records)}, NULL);
    """)
    conn_pg.commit()
    cursor_pg.close()
    conn_pg.close()
    print(f"Successfully generated {len(records)} rows for Dim_Date (2010–2030).")
