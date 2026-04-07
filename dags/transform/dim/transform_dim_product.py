EXTRACT_SQL = """
    SELECT
        product_id      AS product_key,
        description,
        size,
        weight
    FROM idb.Product
    WHERE last_updated_time > '{last_runtime}'
"""
TARGET_COLUMNS = ['product_key', 'description', 'size', 'weight']
TARGET_TABLE   = 'Dim_Product'
