from load_utils_idb import load_table
def extract_store():
    load_table('postgres_default', 'Store', "SELECT store_id, COALESCE(phone_number, 'Unknown'), time, city_id FROM sales_source.Store WHERE time >= '{last_runtime}'", ['store_id', 'phone_number', 'opening_time', 'city_id'], '(store_id) DO NOTHING')
