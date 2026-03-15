from load_utils_idb import load_table
def extract_product():
    load_table('postgres_default', 'Product', "SELECT product_id, description, UPPER(size), ROUND(weight, 2), ROUND(price, 2), time FROM sales_source.Product WHERE time >= '{last_runtime}'", ['product_id', 'description', 'size', 'weight', 'price', 'created_time'], '(product_id) DO NOTHING')
