from load_utils_dwh import load_dimension_or_fact
def transform_fact_inventory():
    load_dimension_or_fact('Fact_Inventory', "SELECT CAST(TO_CHAR(sp.restock_time, 'YYYYMMDD') AS INTEGER) as date_key, ds.store_key, dp.product_key, sp.stock_quantity FROM idb.StockedProduct sp JOIN dwh.Dim_Store ds ON sp.store_id = ds.store_id JOIN dwh.Dim_Product dp ON sp.product_id = dp.product_id WHERE sp.restock_time >= '{last_runtime}'", ['date_key', 'store_key', 'product_key', 'stock_quantity'], True)
