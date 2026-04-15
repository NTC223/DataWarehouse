# Cuboid: olap_inv_time_product_location
EXTRACT_SQL = """\
SELECT
    dt.year,
    dt.quarter,
    dt.month,
    fi.product_key,
    dl.state,
    dl.city,
    SUM(fi.quantity_on_hand) AS total_quantity_on_hand
FROM dwh.Fact_Inventory fi
JOIN dwh.Dim_Time dt ON fi.time_key = dt.time_key
JOIN dwh.Dim_Store ds ON fi.store_key = ds.store_key
JOIN dwh.Dim_Location dl ON ds.location_key = dl.location_key
GROUP BY dt.year, dt.quarter, dt.month, fi.product_key, dl.state, dl.city
"""
TARGET_COLUMNS = ['year', 'quarter', 'month', 'product_key', 'state', 'city', 'total_quantity_on_hand']
TARGET_TABLE   = 'olap_inv_time_product_location'
