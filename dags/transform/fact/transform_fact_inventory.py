EXTRACT_SQL = """
    SELECT
        (SELECT TO_CHAR(MAX(last_updated_time), 'YYYYMM')::int FROM idb.StockedProduct) AS time_key,
        sp.store_id                                      AS store_key,
        sp.product_id                                    AS product_key,
        sp.stock_quantity                                AS quantity_on_hand
    FROM idb.StockedProduct sp
    ORDER BY time_key, store_key, product_key
"""
TARGET_COLUMNS = ['time_key', 'store_key', 'product_key', 'quantity_on_hand']
TARGET_TABLE   = 'Fact_Inventory'