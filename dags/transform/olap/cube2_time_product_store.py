# Cuboid: olap_inv_time_product_store
EXTRACT_SQL = """\
SELECT
    dt.year,
    dt.quarter,
    dt.month,
    fi.product_key,
    fi.store_key,
    SUM(fi.quantity_on_hand) AS total_quantity_on_hand
FROM dwh.Fact_Inventory fi
JOIN dwh.Dim_Time dt ON fi.time_key = dt.time_key
GROUP BY dt.year, dt.quarter, dt.month, fi.product_key, fi.store_key
"""
TARGET_COLUMNS = ['year', 'quarter', 'month', 'product_key', 'store_key', 'total_quantity_on_hand']
TARGET_TABLE   = 'olap_inv_time_product_store'
