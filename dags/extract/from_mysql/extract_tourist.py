TARGET_TABLE = 'TouristCustomer'
TARGET_COLUMNS = ['customer_id', 'tour_guide', 'last_updated_time']
SOURCE_CONN_ID = 'mysql_default'

EXTRACT_SQL = """
    SELECT customer_id, tour_guide, time
    FROM TouristCustomer
    WHERE time >= '{last_runtime}'
"""