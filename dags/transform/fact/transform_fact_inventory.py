EXTRACT_SQL = """
    SELECT
        TO_CHAR(sp.last_updated_time, 'YYYYMMDD')::int  AS time_key,
        sp.store_id                                      AS store_key,
        sp.product_id                                    AS product_key,
        sp.stock_quantity                                AS quantity_on_hand
    FROM idb.StockedProduct sp
    WHERE sp.last_updated_time > '{last_runtime}'
"""
TARGET_COLUMNS = ['time_key', 'store_key', 'product_key', 'quantity_on_hand']
TARGET_TABLE   = 'Fact_Inventory'