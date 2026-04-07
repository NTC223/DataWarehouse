TARGET_TABLE = 'Customer'
TARGET_COLUMNS = ['customer_id', 'customer_name', 'city_id', 'first_order_date']
SOURCE_CONN_ID = 'mysql_default'

EXTRACT_SQL = """
    SELECT customer_id, customer_name, city_id, first_order_date
    FROM Customer
    WHERE first_order_date >= '{last_runtime}'
"""