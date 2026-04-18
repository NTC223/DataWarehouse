EXTRACT_SQL = """
    SELECT
        TO_CHAR(o.order_date, 'YYYYMM')::int          AS time_key,
        op.product_id                                    AS product_key,
        o.customer_id                                    AS customer_key,
        SUM(op.ordered_quantity)                         AS quantity_ordered,
        SUM(op.ordered_quantity * op.ordered_price)      AS total_amount
    FROM idb.OrderProduct op
    JOIN idb."Order" o ON op.order_id = o.order_id
    WHERE op.last_updated_time > '{last_runtime}'
    GROUP BY TO_CHAR(o.order_date, 'YYYYMM')::int, op.product_id, o.customer_id
    ORDER BY time_key, product_key, customer_key
"""
TARGET_COLUMNS = ['time_key', 'product_key', 'customer_key', 'quantity_ordered', 'total_amount']
TARGET_TABLE   = 'Fact_Sales'