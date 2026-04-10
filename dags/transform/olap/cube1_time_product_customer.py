# Cuboid: olap_sales_time_product_customer
EXTRACT_SQL = """\
SELECT
    dt.year, dt.quarter, dt.month,
    fs.product_key,
    dc.customer_type, fs.customer_key,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount)     AS total_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Time dt ON fs.time_key = dt.time_key
JOIN dwh.Dim_Customer dc ON fs.customer_key = dc.customer_key
GROUP BY dt.year, dt.quarter, dt.month, fs.product_key, dc.customer_type, fs.customer_key
"""
TARGET_COLUMNS = ['year', 'quarter', 'month', 'product_key', 'customer_type', 'customer_key', 'total_quantity', 'total_amount']
TARGET_TABLE   = 'olap_sales_time_product_customer'
