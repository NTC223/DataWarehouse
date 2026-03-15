from load_utils_idb import load_table
def extract_order_product():
    load_table('postgres_default', 'OrderProduct', "SELECT order_id, product_id, ordered_quantity, ordered_price, time FROM sales_source.OrderProduct WHERE time >= '{last_runtime}'", ['order_id', 'product_id', 'ordered_quantity', 'ordered_price', 'added_time'], '(order_id, product_id) DO NOTHING')
