TARGET_TABLE = 'Order'
TARGET_COLUMNS = ['order_id', 'customer_id', 'order_date']
SOURCE_CONN_ID = 'postgres_default'

EXTRACT_SQL = """
    SELECT order_id, customer_id, order_date
    FROM sales_source."Order"
    WHERE order_date >= '{last_runtime}'
"""