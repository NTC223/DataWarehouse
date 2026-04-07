EXTRACT_SQL = """
    SELECT
        city_id         AS location_key,
        city_name       AS city,
        state,
        office_address
    FROM idb.RepresentativeOffice
    WHERE last_updated_time > '{last_runtime}'
"""
TARGET_COLUMNS = ['location_key', 'city', 'state', 'office_address']
TARGET_TABLE   = 'Dim_Location'