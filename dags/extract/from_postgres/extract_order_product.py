TARGET_TABLE = 'OrderProduct'
TARGET_COLUMNS = ['order_id', 'product_id', 'ordered_quantity', 'ordered_price', 'last_updated_time']
SOURCE_CONN_ID = 'postgres_default'

EXTRACT_SQL = """
    SELECT order_id, product_id, ordered_quantity, ordered_price, time
    FROM sales_source.OrderProduct
    WHERE time >= '{last_runtime}'
"""