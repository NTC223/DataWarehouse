# Cuboid: olap_sales_all
EXTRACT_SQL = """\
SELECT
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount)     AS total_amount
FROM dwh.Fact_Sales fs
"""
TARGET_COLUMNS = ['total_quantity', 'total_amount']
TARGET_TABLE   = 'olap_sales_all'
