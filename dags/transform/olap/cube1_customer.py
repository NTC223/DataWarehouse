# Cuboid: olap_sales_by_customer
EXTRACT_SQL = """\
SELECT
    dc.customer_type, fs.customer_key,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount) AS sum_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_type, fs.customer_key
"""
TARGET_COLUMNS = ['customer_type', 'customer_key', 'total_quantity', 'sum_amount']
TARGET_TABLE   = 'olap_sales_by_customer'
