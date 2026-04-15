# Cuboid: olap_inv_store_location
EXTRACT_SQL = """\
SELECT
    fi.store_key,
    dl.state,
    dl.city,
    SUM(fi.quantity_on_hand) AS total_quantity_on_hand
FROM dwh.Fact_Inventory fi
JOIN dwh.Dim_Store ds ON fi.store_key = ds.store_key
JOIN dwh.Dim_Location dl ON ds.location_key = dl.location_key
GROUP BY fi.store_key, dl.state, dl.city
"""
TARGET_COLUMNS = ['store_key', 'state', 'city', 'total_quantity_on_hand']
TARGET_TABLE   = 'olap_inv_store_location'
