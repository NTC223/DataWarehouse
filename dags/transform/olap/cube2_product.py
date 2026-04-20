# Cuboid: olap_inv_by_product
EXTRACT_SQL = """
\
SELECT
    fi.product_key,
    SUM(fi.quantity_on_hand) AS total_quantity_on_hand
FROM dwh.Fact_Inventory fi
GROUP BY fi.product_key
"""
TARGET_COLUMNS = ['product_key', 'total_quantity_on_hand']
TARGET_TABLE   = 'olap_inv_by_product'
