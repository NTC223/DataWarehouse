from load_utils_idb import load_table
def extract_stocked_product():
    load_table('postgres_default', 'StockedProduct', "SELECT store_id, product_id, stock_quantity, time FROM sales_source.StockedProduct WHERE time >= '{last_runtime}'", ['store_id', 'product_id', 'stock_quantity', 'restock_time'], '(store_id, product_id) DO UPDATE SET stock_quantity = EXCLUDED.stock_quantity')
