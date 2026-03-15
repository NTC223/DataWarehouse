from load_utils_dwh import load_dimension_or_fact
def transform_dim_location():
    load_dimension_or_fact('Dim_Location', "SELECT city_id, city_name, state, office_address FROM idb.RepresentativeOffice WHERE established_time >= '{last_runtime}'", ['city_id', 'city_name', 'state', 'office_address'], False)
