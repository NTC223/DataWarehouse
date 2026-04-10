# Cuboid: olap_sales_by_time
EXTRACT_SQL = """\
SELECT
    dt.year, dt.quarter, dt.month,
    SUM(fs.quantity_ordered) AS total_quantity,
    SUM(fs.total_amount)     AS total_amount
FROM dwh.Fact_Sales fs
JOIN dwh.Dim_Time dt ON fs.time_key = dt.time_key
GROUP BY dt.year, dt.quarter, dt.month
"""
TARGET_COLUMNS = ['year', 'quarter', 'month', 'total_quantity', 'total_amount']
TARGET_TABLE   = 'olap_sales_by_time'
