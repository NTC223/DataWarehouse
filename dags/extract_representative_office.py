from load_utils_idb import load_table
def extract_representative_office():
    load_table('postgres_default', 'RepresentativeOffice', "SELECT city_id, city_name, office_address, state, time FROM sales_source.RepresentativeOffice WHERE time >= '{last_runtime}'", ['city_id', 'city_name', 'office_address', 'state', 'established_time'], '(city_id) DO NOTHING')
