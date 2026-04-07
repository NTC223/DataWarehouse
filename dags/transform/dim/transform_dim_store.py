EXTRACT_SQL = """
    SELECT
        s.store_id                                       AS store_key,
        s.phone_number,
        l.city_id                                        AS location_key
    FROM idb.Store s
    JOIN idb.RepresentativeOffice l ON s.city_id = l.city_id
    WHERE s.last_updated_time > '{last_runtime}'
"""
TARGET_COLUMNS = ['store_key', 'phone_number', 'location_key']
TARGET_TABLE   = 'Dim_Store'