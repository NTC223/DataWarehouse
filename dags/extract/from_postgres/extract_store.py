TARGET_TABLE = 'Store'
TARGET_COLUMNS = ['store_id', 'phone_number', 'last_updated_time', 'city_id']
SOURCE_CONN_ID = 'postgres_default'

EXTRACT_SQL = """
    SELECT store_id, phone_number, time, city_id
    FROM sales_source.Store
    WHERE time >= '{last_runtime}'
"""