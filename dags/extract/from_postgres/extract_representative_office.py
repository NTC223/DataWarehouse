TARGET_TABLE = 'RepresentativeOffice'
TARGET_COLUMNS = ['city_id', 'city_name', 'office_address', 'state', 'last_updated_time']
SOURCE_CONN_ID = 'postgres_default'

EXTRACT_SQL = """
    SELECT city_id, city_name, office_address, state, time
    FROM sales_source.RepresentativeOffice
    WHERE time >= '{last_runtime}'
"""