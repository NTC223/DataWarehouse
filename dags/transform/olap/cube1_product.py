# Cuboid: olap_sales_by_product
EXTRACT_SQL = """\
SELECT
    fs.product_key,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount)     AS total_amount
FROM dwh.Fact_Sales fs
GROUP BY fs.product_key
"""
TARGET_COLUMNS = ['product_key', 'total_quantity', 'total_amount']
TARGET_TABLE   = 'olap_sales_by_product'
