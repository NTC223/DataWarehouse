from load_utils_dwh import load_dimension_or_fact
def transform_dim_store():
    load_dimension_or_fact('Dim_Store', "SELECT s.store_id, s.phone_number, l.city_key FROM idb.Store s JOIN dwh.Dim_Location l ON s.city_id = l.city_id WHERE s.opening_time >= '{last_runtime}'", ['store_id', 'phone_number', 'city_key'], False)
