# Cuboid: olap_sales_product_customer
EXTRACT_SQL = """\
SELECT
    fs.product_key,
    dc.customer_type, fs.customer_key,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount)     AS total_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Customer dc ON fs.customer_key = dc.customer_key
GROUP BY fs.product_key, dc.customer_type, fs.customer_key
"""
TARGET_COLUMNS = ['product_key', 'customer_type', 'customer_key', 'total_quantity', 'total_amount']
TARGET_TABLE   = 'olap_sales_product_customer'
