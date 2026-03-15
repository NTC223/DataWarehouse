from load_utils_idb import load_table
def extract_order():
    load_table('postgres_default', 'Order', "SELECT order_id, order_date, customer_id FROM sales_source.\"Order\" WHERE order_date >= '{last_runtime}'", ['order_id', 'order_date', 'customer_id'], '(order_id) DO NOTHING')
