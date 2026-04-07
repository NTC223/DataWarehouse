TARGET_TABLE = 'StockedProduct'
TARGET_COLUMNS = ['store_id', 'product_id', 'stock_quantity', 'last_updated_time']
SOURCE_CONN_ID = 'postgres_default'

EXTRACT_SQL = """
    SELECT store_id, product_id, stock_quantity, time
    FROM sales_source.StockedProduct
    WHERE time >= '{last_runtime}'
"""