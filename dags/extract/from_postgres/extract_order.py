TARGET_TABLE = 'Order'
TARGET_COLUMNS = ['order_id', 'order_date', 'customer_id']
SOURCE_CONN_ID = 'postgres_default'

EXTRACT_SQL = """
    SELECT order_id, order_date, customer_id
    FROM sales_source."Order"
    WHERE order_date >= '{last_runtime}'
"""