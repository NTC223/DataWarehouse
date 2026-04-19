# Cuboid: olap_inv_all
EXTRACT_SQL = """
\
SELECT
    SUM(fi.quantity_on_hand) AS total_quantity_on_hand
FROM dwh.Fact_Inventory fi
"""
TARGET_COLUMNS = ['total_quantity_on_hand']
TARGET_TABLE   = 'olap_inv_all'
