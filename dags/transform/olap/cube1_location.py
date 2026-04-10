# Cuboid: olap_sales_by_location
EXTRACT_SQL = """\
SELECT
    dl.state, dl.city,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount)     AS total_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Customer dc ON fs.customer_key = dc.customer_key
JOIN dwh.Dim_Location dl ON dc.location_key = dl.location_key
GROUP BY dl.state, dl.city
"""
TARGET_COLUMNS = ['state', 'city', 'total_quantity', 'total_amount']
TARGET_TABLE   = 'olap_sales_by_location'
