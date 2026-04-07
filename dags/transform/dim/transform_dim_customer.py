# JOIN 3 bảng để xác định customer_type
EXTRACT_SQL = """
    SELECT
        c.customer_id       AS customer_key,
        c.customer_name,
        CASE
            WHEN t.customer_id IS NOT NULL AND m.customer_id IS NOT NULL THEN 'Both'
            WHEN t.customer_id IS NOT NULL THEN 'Tourist'
            WHEN m.customer_id IS NOT NULL THEN 'MailOrder'
            ELSE 'Unknown'
        END                 AS customer_type,
        c.first_order_date,
        c.city_id           AS location_key
    FROM idb.Customer c
    LEFT JOIN idb.TouristCustomer   t ON c.customer_id = t.customer_id
    LEFT JOIN idb.MailOrderCustomer m ON c.customer_id = m.customer_id
    WHERE c.first_order_date > '{last_runtime}'
"""
TARGET_COLUMNS = ['customer_key', 'customer_name', 'customer_type', 'first_order_date', 'location_key']
TARGET_TABLE   = 'Dim_Customer'