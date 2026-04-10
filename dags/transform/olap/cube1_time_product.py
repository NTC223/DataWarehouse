# Cuboid: olap_sales_time_product
EXTRACT_SQL = """\
SELECT
    dt.year, dt.quarter, dt.month,
    fs.product_key,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount)     AS total_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Time dt ON fs.time_key = dt.time_key
GROUP BY dt.year, dt.quarter, dt.month, fs.product_key
"""
TARGET_COLUMNS = ['year', 'quarter', 'month', 'product_key', 'total_quantity', 'total_amount']
TARGET_TABLE   = 'olap_sales_time_product'
