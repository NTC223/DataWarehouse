# Sinh Dim_Time đảm bảo luôn có tháng hiện tại (ngay cả khi không có đơn hàng) và các tháng từ đơn hàng
EXTRACT_SQL = """
    SELECT * FROM (
        SELECT DISTINCT
            TO_CHAR(order_date, 'YYYYMM')::int     AS time_key,
            EXTRACT(MONTH   FROM order_date)::int  AS month,
            EXTRACT(QUARTER FROM order_date)::int  AS quarter,
            EXTRACT(YEAR    FROM order_date)::int  AS year
        FROM idb."Order"
        WHERE order_date > '{last_runtime}'
        UNION
        SELECT 
            TO_CHAR(CURRENT_DATE, 'YYYYMM')::int    AS time_key,
            EXTRACT(MONTH   FROM CURRENT_DATE)::int AS month,
            EXTRACT(QUARTER FROM CURRENT_DATE)::int AS quarter,
            EXTRACT(YEAR    FROM CURRENT_DATE)::int AS year
    ) sub
    ORDER BY time_key
"""
TARGET_COLUMNS = ['time_key', 'month', 'quarter', 'year']
TARGET_TABLE   = 'Dim_Time'