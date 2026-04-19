# Cuboid: olap_sales_time_location
EXTRACT_SQL = """\
SELECT
    dt.year, dt.quarter, dt.month,
    dl.state, dl.city,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount) AS sum_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Time dt ON fs.time_key = dt.time_key
JOIN dwh.Dim_Customer dc ON fs.customer_key = dc.customer_key
JOIN dwh.Dim_Location dl ON dc.location_key = dl.location_key
GROUP BY dt.year, dt.quarter, dt.month, dl.state, dl.city
"""
TARGET_COLUMNS = ['year', 'quarter', 'month', 'state', 'city', 'total_quantity', 'sum_amount']
TARGET_TABLE   = 'olap_sales_time_location'
