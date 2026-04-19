# Cuboid: olap_inv_by_store
EXTRACT_SQL = """
\
SELECT
    fi.store_key,
    SUM(fi.quantity_on_hand) AS total_quantity_on_hand
FROM dwh.Fact_Inventory fi
GROUP BY fi.store_key
"""
TARGET_COLUMNS = ['store_key', 'total_quantity_on_hand']
TARGET_TABLE   = 'olap_inv_by_store'
