# Sinh Dim_Time từ tất cả các ngày đặt hàng
EXTRACT_SQL = """
    SELECT DISTINCT
        TO_CHAR(order_date, 'YYYYMMDD')::int   AS time_key,
        EXTRACT(MONTH   FROM order_date)::int  AS month,
        EXTRACT(QUARTER FROM order_date)::int  AS quarter,
        EXTRACT(YEAR    FROM order_date)::int  AS year
    FROM idb."Order"
    WHERE order_date > '{last_runtime}'
"""
TARGET_COLUMNS = ['time_key', 'month', 'quarter', 'year']
TARGET_TABLE   = 'Dim_Time'