# Cuboid: olap_sales_time_product_location
EXTRACT_SQL = """\
SELECT
    dt.year, dt.quarter, dt.month,
    fs.product_key,
    dl.state, dl.city,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount)     AS total_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Time dt ON fs.time_key = dt.time_key
JOIN dwh.Dim_Customer dc ON fs.customer_key = dc.customer_key
JOIN dwh.Dim_Location dl ON dc.location_key = dl.location_key
GROUP BY dt.year, dt.quarter, dt.month, fs.product_key, dl.state, dl.city
"""
TARGET_COLUMNS = ['year', 'quarter', 'month', 'product_key', 'state', 'city', 'total_quantity', 'total_amount']
TARGET_TABLE   = 'olap_sales_time_product_location'
