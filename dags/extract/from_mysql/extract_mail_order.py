TARGET_TABLE = 'MailOrderCustomer'
TARGET_COLUMNS = ['customer_id', 'postal_address', 'last_updated_time']
SOURCE_CONN_ID = 'mysql_default'

EXTRACT_SQL = """
    SELECT customer_id, postal_address, time
    FROM MailOrderCustomer
    WHERE time >= '{last_runtime}'
"""