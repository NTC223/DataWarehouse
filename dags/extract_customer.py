from load_utils_idb import load_table
def extract_customer():
    load_table('mysql_default', 'Customer', "SELECT customer_id, TRIM(UPPER(customer_name)), city_id, IFNULL(first_order_date, CURRENT_DATE) FROM Customer WHERE first_order_date >= '{last_runtime}'", ['customer_id', 'customer_name', 'city_id', 'first_order_date'], '(customer_id) DO NOTHING')
