TARGET_TABLE = 'Product'
TARGET_COLUMNS = ['product_id', 'description', 'size', 'weight', 'price', 'last_updated_time']
SOURCE_CONN_ID = 'postgres_default'

EXTRACT_SQL = """
    SELECT product_id, description, UPPER(size), ROUND(weight, 2), ROUND(price, 2), time
    FROM sales_source.Product
    WHERE time >= '{last_runtime}'
"""