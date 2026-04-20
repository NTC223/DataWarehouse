# Cuboid: olap_sales_customer_location
EXTRACT_SQL = """\
SELECT
    dc.customer_type, fs.customer_key,
    dl.state, dl.city,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount) AS sum_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Customer dc ON fs.customer_key = dc.customer_key
JOIN dwh.Dim_Location dl ON dc.location_key = dl.location_key
GROUP BY dc.customer_type, fs.customer_key, dl.state, dl.city
"""
TARGET_COLUMNS = ['customer_type', 'customer_key', 'state', 'city', 'total_quantity', 'sum_amount']
TARGET_TABLE   = 'olap_sales_customer_location'
