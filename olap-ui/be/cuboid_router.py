"""
cuboid_router.py — Maps OLAP dimension/level combinations to cuboid table names.

Contains the exhaustive mapping of all 80 cuboids (48 Sales + 32 Inventory)
and the logic to resolve which cuboid to query based on active dimensions.
"""

from models import DimensionLevel

# ═══════════════════════════════════════════════════════════════
# Cuboid entry format:
#   (time_level, dim2_level, product_level) → {
#       'table':    str,                  # table name in schema olap
#       'dim_cols': list[str],            # dimension columns
#       'measures': list[str],            # measure columns
#   }
#
# Levels:
#   time:     None | 'year' | 'quarter' | 'month'
#   customer: None | 'state' | 'city' | 'customer_type' |
#             'customer_info' | 'customer_loc'
#   store:    None | 'state' | 'city' | 'store'
#   product:  None | 'product'
# ═══════════════════════════════════════════════════════════════

_SALES_MEASURES = ['total_quantity', 'sum_amount']
_INV_MEASURES = ['total_quantity_on_hand']


# ── SALES CUBE (48 cuboids) ──────────────────────────────────

SALES_CUBOIDS: dict[tuple, dict] = {
    # ── Dim-0: ALL ──
    (None, None, None): {
        'table': 'olap_sales_all',
        'dim_cols': [],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-1: Time only ──
    ('month', None, None): {
        'table': 'olap_sales_by_time',
        'dim_cols': ['year', 'quarter', 'month'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', None, None): {
        'table': 'olap_sales_by_quarter',
        'dim_cols': ['year', 'quarter'],
        'measures': _SALES_MEASURES,
    },
    ('year', None, None): {
        'table': 'olap_sales_by_year',
        'dim_cols': ['year'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-1: Customer only ──
    (None, 'customer_info', None): {
        'table': 'olap_sales_by_customer_info',
        'dim_cols': ['customer_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    (None, 'customer_loc', None): {
        'table': 'olap_sales_by_customer_loc',
        'dim_cols': ['customer_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    (None, 'customer_type', None): {
        'table': 'olap_sales_by_customer_type',
        'dim_cols': ['customer_type'],
        'measures': _SALES_MEASURES,
    },
    (None, 'city', None): {
        'table': 'olap_sales_by_city',
        'dim_cols': ['state', 'city'],
        'measures': _SALES_MEASURES,
    },
    (None, 'state', None): {
        'table': 'olap_sales_by_state',
        'dim_cols': ['state'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-1: Product only ──
    (None, None, 'product'): {
        'table': 'olap_sales_by_product',
        'dim_cols': ['product_key'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-2: Time × Product ──
    ('month', None, 'product'): {
        'table': 'olap_sales_time_product',
        'dim_cols': ['year', 'quarter', 'month', 'product_key'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', None, 'product'): {
        'table': 'olap_sales_quarter_product',
        'dim_cols': ['year', 'quarter', 'product_key'],
        'measures': _SALES_MEASURES,
    },
    ('year', None, 'product'): {
        'table': 'olap_sales_year_product',
        'dim_cols': ['year', 'product_key'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-2: Time(month) × Customer ──
    ('month', 'customer_info', None): {
        'table': 'olap_sales_time_customer_info',
        'dim_cols': ['year', 'quarter', 'month', 'customer_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('month', 'customer_loc', None): {
        'table': 'olap_sales_time_customer_loc',
        'dim_cols': ['year', 'quarter', 'month', 'customer_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('month', 'customer_type', None): {
        'table': 'olap_sales_month_customer_type',
        'dim_cols': ['year', 'quarter', 'month', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('month', 'city', None): {
        'table': 'olap_sales_month_city',
        'dim_cols': ['year', 'quarter', 'month', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('month', 'state', None): {
        'table': 'olap_sales_month_state',
        'dim_cols': ['year', 'quarter', 'month', 'state'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-2: Time(quarter) × Customer ──
    ('quarter', 'customer_info', None): {
        'table': 'olap_sales_quarter_customer_info',
        'dim_cols': ['year', 'quarter', 'customer_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', 'customer_loc', None): {
        'table': 'olap_sales_quarter_customer_loc',
        'dim_cols': ['year', 'quarter', 'customer_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', 'customer_type', None): {
        'table': 'olap_sales_quarter_customer_type',
        'dim_cols': ['year', 'quarter', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', 'city', None): {
        'table': 'olap_sales_quarter_city',
        'dim_cols': ['year', 'quarter', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', 'state', None): {
        'table': 'olap_sales_quarter_state',
        'dim_cols': ['year', 'quarter', 'state'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-2: Time(year) × Customer ──
    ('year', 'customer_info', None): {
        'table': 'olap_sales_year_customer_info',
        'dim_cols': ['year', 'customer_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('year', 'customer_loc', None): {
        'table': 'olap_sales_year_customer_loc',
        'dim_cols': ['year', 'customer_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('year', 'customer_type', None): {
        'table': 'olap_sales_year_customer_type',
        'dim_cols': ['year', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('year', 'city', None): {
        'table': 'olap_sales_year_city',
        'dim_cols': ['year', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('year', 'state', None): {
        'table': 'olap_sales_year_state',
        'dim_cols': ['year', 'state'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-2: Product × Customer ──
    (None, 'customer_info', 'product'): {
        'table': 'olap_sales_customer_product_info',
        'dim_cols': ['customer_key', 'customer_type', 'product_key'],
        'measures': _SALES_MEASURES,
    },
    (None, 'customer_loc', 'product'): {
        'table': 'olap_sales_customer_product_loc',
        'dim_cols': ['customer_key', 'state', 'city', 'product_key'],
        'measures': _SALES_MEASURES,
    },
    (None, 'customer_type', 'product'): {
        'table': 'olap_sales_product_customer_type',
        'dim_cols': ['product_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    (None, 'city', 'product'): {
        'table': 'olap_sales_product_city',
        'dim_cols': ['product_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    (None, 'state', 'product'): {
        'table': 'olap_sales_product_state',
        'dim_cols': ['product_key', 'state'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-3: Time(month) × Product × Customer ──
    ('month', 'customer_info', 'product'): {
        'table': 'olap_sales_base_info',
        'dim_cols': ['year', 'quarter', 'month', 'product_key', 'customer_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('month', 'customer_loc', 'product'): {
        'table': 'olap_sales_base_loc',
        'dim_cols': ['year', 'quarter', 'month', 'product_key', 'customer_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('month', 'customer_type', 'product'): {
        'table': 'olap_sales_month_product_customer_type',
        'dim_cols': ['year', 'quarter', 'month', 'product_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('month', 'city', 'product'): {
        'table': 'olap_sales_month_product_city',
        'dim_cols': ['year', 'quarter', 'month', 'product_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('month', 'state', 'product'): {
        'table': 'olap_sales_month_product_state',
        'dim_cols': ['year', 'quarter', 'month', 'product_key', 'state'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-3: Time(quarter) × Product × Customer ──
    ('quarter', 'customer_info', 'product'): {
        'table': 'olap_sales_quarter_product_customer_info',
        'dim_cols': ['year', 'quarter', 'product_key', 'customer_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', 'customer_loc', 'product'): {
        'table': 'olap_sales_quarter_product_customer_loc',
        'dim_cols': ['year', 'quarter', 'product_key', 'customer_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', 'customer_type', 'product'): {
        'table': 'olap_sales_quarter_product_customer_type',
        'dim_cols': ['year', 'quarter', 'product_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', 'city', 'product'): {
        'table': 'olap_sales_quarter_product_city',
        'dim_cols': ['year', 'quarter', 'product_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('quarter', 'state', 'product'): {
        'table': 'olap_sales_quarter_product_state',
        'dim_cols': ['year', 'quarter', 'product_key', 'state'],
        'measures': _SALES_MEASURES,
    },

    # ── Dim-3: Time(year) × Product × Customer ──
    ('year', 'customer_info', 'product'): {
        'table': 'olap_sales_year_product_customer_info',
        'dim_cols': ['year', 'product_key', 'customer_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('year', 'customer_loc', 'product'): {
        'table': 'olap_sales_year_product_customer_loc',
        'dim_cols': ['year', 'product_key', 'customer_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('year', 'customer_type', 'product'): {
        'table': 'olap_sales_year_product_customer_type',
        'dim_cols': ['year', 'product_key', 'customer_type'],
        'measures': _SALES_MEASURES,
    },
    ('year', 'city', 'product'): {
        'table': 'olap_sales_year_product_city',
        'dim_cols': ['year', 'product_key', 'state', 'city'],
        'measures': _SALES_MEASURES,
    },
    ('year', 'state', 'product'): {
        'table': 'olap_sales_year_product_state',
        'dim_cols': ['year', 'product_key', 'state'],
        'measures': _SALES_MEASURES,
    },
}


# ── INVENTORY CUBE (32 cuboids) ──────────────────────────────

INVENTORY_CUBOIDS: dict[tuple, dict] = {
    # ── Dim-0: ALL ──
    (None, None, None): {
        'table': 'olap_inv_all',
        'dim_cols': [],
        'measures': _INV_MEASURES,
    },

    # ── Dim-1: Time only ──
    ('month', None, None): {
        'table': 'olap_inv_by_time',
        'dim_cols': ['year', 'quarter', 'month'],
        'measures': _INV_MEASURES,
    },
    ('quarter', None, None): {
        'table': 'olap_inv_by_quarter',
        'dim_cols': ['year', 'quarter'],
        'measures': _INV_MEASURES,
    },
    ('year', None, None): {
        'table': 'olap_inv_by_year',
        'dim_cols': ['year'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-1: Product only ──
    (None, None, 'product'): {
        'table': 'olap_inv_by_product',
        'dim_cols': ['product_key'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-1: Store only ──
    (None, 'store', None): {
        'table': 'olap_inv_by_store',
        'dim_cols': ['store_key', 'city', 'state'],
        'measures': _INV_MEASURES,
    },
    (None, 'city', None): {
        'table': 'olap_inv_by_city',
        'dim_cols': ['state', 'city'],
        'measures': _INV_MEASURES,
    },
    (None, 'state', None): {
        'table': 'olap_inv_by_state',
        'dim_cols': ['state'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-2: Time × Product ──
    ('month', None, 'product'): {
        'table': 'olap_inv_time_product',
        'dim_cols': ['year', 'quarter', 'month', 'product_key'],
        'measures': _INV_MEASURES,
    },
    ('quarter', None, 'product'): {
        'table': 'olap_inv_quarter_product',
        'dim_cols': ['year', 'quarter', 'product_key'],
        'measures': _INV_MEASURES,
    },
    ('year', None, 'product'): {
        'table': 'olap_inv_year_product',
        'dim_cols': ['year', 'product_key'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-2: Time(month) × Store ──
    ('month', 'store', None): {
        'table': 'olap_inv_time_store',
        'dim_cols': ['year', 'quarter', 'month', 'store_key', 'city', 'state'],
        'measures': _INV_MEASURES,
    },
    ('month', 'city', None): {
        'table': 'olap_inv_month_city',
        'dim_cols': ['year', 'quarter', 'month', 'state', 'city'],
        'measures': _INV_MEASURES,
    },
    ('month', 'state', None): {
        'table': 'olap_inv_month_state',
        'dim_cols': ['year', 'quarter', 'month', 'state'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-2: Time(quarter) × Store ──
    ('quarter', 'store', None): {
        'table': 'olap_inv_quarter_store',
        'dim_cols': ['year', 'quarter', 'store_key', 'city', 'state'],
        'measures': _INV_MEASURES,
    },
    ('quarter', 'city', None): {
        'table': 'olap_inv_quarter_city',
        'dim_cols': ['year', 'quarter', 'state', 'city'],
        'measures': _INV_MEASURES,
    },
    ('quarter', 'state', None): {
        'table': 'olap_inv_quarter_state',
        'dim_cols': ['year', 'quarter', 'state'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-2: Time(year) × Store ──
    ('year', 'store', None): {
        'table': 'olap_inv_year_store',
        'dim_cols': ['year', 'store_key', 'city', 'state'],
        'measures': _INV_MEASURES,
    },
    ('year', 'city', None): {
        'table': 'olap_inv_year_city',
        'dim_cols': ['year', 'state', 'city'],
        'measures': _INV_MEASURES,
    },
    ('year', 'state', None): {
        'table': 'olap_inv_year_state',
        'dim_cols': ['year', 'state'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-2: Product × Store ──
    (None, 'store', 'product'): {
        'table': 'olap_inv_product_store',
        'dim_cols': ['product_key', 'store_key', 'city', 'state'],
        'measures': _INV_MEASURES,
    },
    (None, 'city', 'product'): {
        'table': 'olap_inv_product_city',
        'dim_cols': ['product_key', 'state', 'city'],
        'measures': _INV_MEASURES,
    },
    (None, 'state', 'product'): {
        'table': 'olap_inv_product_state',
        'dim_cols': ['product_key', 'state'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-3: Time(month) × Product × Store ──
    ('month', 'store', 'product'): {
        'table': 'olap_inv_base',
        'dim_cols': ['year', 'quarter', 'month', 'product_key', 'store_key', 'city', 'state'],
        'measures': _INV_MEASURES,
    },
    ('month', 'city', 'product'): {
        'table': 'olap_inv_month_product_city',
        'dim_cols': ['year', 'quarter', 'month', 'product_key', 'state', 'city'],
        'measures': _INV_MEASURES,
    },
    ('month', 'state', 'product'): {
        'table': 'olap_inv_month_product_state',
        'dim_cols': ['year', 'quarter', 'month', 'product_key', 'state'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-3: Time(quarter) × Product × Store ──
    ('quarter', 'store', 'product'): {
        'table': 'olap_inv_quarter_product_store',
        'dim_cols': ['year', 'quarter', 'product_key', 'store_key', 'city', 'state'],
        'measures': _INV_MEASURES,
    },
    ('quarter', 'city', 'product'): {
        'table': 'olap_inv_quarter_product_city',
        'dim_cols': ['year', 'quarter', 'product_key', 'state', 'city'],
        'measures': _INV_MEASURES,
    },
    ('quarter', 'state', 'product'): {
        'table': 'olap_inv_quarter_product_state',
        'dim_cols': ['year', 'quarter', 'product_key', 'state'],
        'measures': _INV_MEASURES,
    },

    # ── Dim-3: Time(year) × Product × Store ──
    ('year', 'store', 'product'): {
        'table': 'olap_inv_year_product_store',
        'dim_cols': ['year', 'product_key', 'store_key', 'city', 'state'],
        'measures': _INV_MEASURES,
    },
    ('year', 'city', 'product'): {
        'table': 'olap_inv_year_product_city',
        'dim_cols': ['year', 'product_key', 'state', 'city'],
        'measures': _INV_MEASURES,
    },
    ('year', 'state', 'product'): {
        'table': 'olap_inv_year_product_state',
        'dim_cols': ['year', 'product_key', 'state'],
        'measures': _INV_MEASURES,
    },
}


# ── Cube metadata ─────────────────────────────────────────────

CUBE_META = {
    'sales': {
        'name': 'sales',
        'display_name': 'Sales Cube',
        'fact_table': 'dwh.Fact_Sales',
        'measures': [
            {'name': 'total_quantity', 'display': 'Total Quantity', 'agg': 'SUM'},
            {'name': 'sum_amount', 'display': 'Total Amount ($)', 'agg': 'SUM'},
        ],
        'dimensions': [
            {
                'name': 'time',
                'display': 'Time',
                'levels': [
                    {'level': 'year', 'display': 'Year', 'columns': ['year']},
                    {'level': 'quarter', 'display': 'Quarter', 'columns': ['year', 'quarter']},
                    {'level': 'month', 'display': 'Month', 'columns': ['year', 'quarter', 'month']},
                ],
                'hierarchy': ['year', 'quarter', 'month'],
            },
            {
                'name': 'customer',
                'display': 'Customer',
                'levels': [
                    {'level': 'state', 'display': 'State', 'columns': ['state']},
                    {'level': 'city', 'display': 'City', 'columns': ['state', 'city']},
                    {'level': 'customer_type', 'display': 'Customer Type', 'columns': ['customer_type']},
                    {'level': 'customer_info', 'display': 'Customer (Info)', 'columns': ['customer_key', 'customer_type']},
                    {'level': 'customer_loc', 'display': 'Customer (Location)', 'columns': ['customer_key', 'state', 'city']},
                ],
                'hierarchy': ['state', 'city', 'customer_type', 'customer_info'],
            },
            {
                'name': 'product',
                'display': 'Product',
                'levels': [
                    {'level': 'product', 'display': 'Product', 'columns': ['product_key']},
                ],
                'hierarchy': ['product'],
            },
        ],
        'cuboid_map': SALES_CUBOIDS,
    },
    'inventory': {
        'name': 'inventory',
        'display_name': 'Inventory Cube',
        'fact_table': 'dwh.Fact_Inventory',
        'measures': [
            {'name': 'total_quantity_on_hand', 'display': 'Total Qty On Hand', 'agg': 'SUM'},
        ],
        'dimensions': [
            {
                'name': 'time',
                'display': 'Time',
                'levels': [
                    {'level': 'year', 'display': 'Year', 'columns': ['year']},
                    {'level': 'quarter', 'display': 'Quarter', 'columns': ['year', 'quarter']},
                    {'level': 'month', 'display': 'Month', 'columns': ['year', 'quarter', 'month']},
                ],
                'hierarchy': ['year', 'quarter', 'month'],
            },
            {
                'name': 'store',
                'display': 'Store',
                'levels': [
                    {'level': 'state', 'display': 'State', 'columns': ['state']},
                    {'level': 'city', 'display': 'City', 'columns': ['state', 'city']},
                    {'level': 'store', 'display': 'Store', 'columns': ['store_key', 'city', 'state']},
                ],
                'hierarchy': ['state', 'city', 'store'],
            },
            {
                'name': 'product',
                'display': 'Product',
                'levels': [
                    {'level': 'product', 'display': 'Product', 'columns': ['product_key']},
                ],
                'hierarchy': ['product'],
            },
        ],
        'cuboid_map': INVENTORY_CUBOIDS,
    },
}


# ── Drill-Through SQL templates ──────────────────────────────

DRILL_THROUGH_SQL = {
    'sales': {
        'base_sql': """
            SELECT fs.time_key, dt.year, dt.quarter, dt.month,
                   fs.product_key, dp.description AS product_desc,
                   dp.size AS product_size, dp.weight AS product_weight,
                   fs.customer_key, dc.customer_name, dc.customer_type,
                   dl.state, dl.city,
                   fs.quantity_ordered, fs.total_amount
            FROM dwh.Fact_Sales fs
            JOIN dwh.Dim_Time dt ON fs.time_key = dt.time_key
            JOIN dwh.Dim_Product dp ON fs.product_key = dp.product_key
            JOIN dwh.Dim_Customer dc ON fs.customer_key = dc.customer_key
            LEFT JOIN dwh.Dim_Location dl ON dc.location_key = dl.location_key
        """,
        'filter_column_map': {
            'year': 'dt.year',
            'quarter': 'dt.quarter',
            'month': 'dt.month',
            'product_key': 'fs.product_key',
            'customer_key': 'fs.customer_key',
            'customer_type': 'dc.customer_type',
            'state': 'dl.state',
            'city': 'dl.city',
        },
    },
    'inventory': {
        'base_sql': """
            SELECT fi.time_key, dt.year, dt.quarter, dt.month,
                   fi.product_key, dp.description AS product_desc,
                   dp.size AS product_size, dp.weight AS product_weight,
                   fi.store_key, ds.phone_number AS store_phone,
                   dl.state, dl.city,
                   fi.quantity_on_hand
            FROM dwh.Fact_Inventory fi
            JOIN dwh.Dim_Time dt ON fi.time_key = dt.time_key
            JOIN dwh.Dim_Product dp ON fi.product_key = dp.product_key
            JOIN dwh.Dim_Store ds ON fi.store_key = ds.store_key
            LEFT JOIN dwh.Dim_Location dl ON ds.location_key = dl.location_key
        """,
        'filter_column_map': {
            'year': 'dt.year',
            'quarter': 'dt.quarter',
            'month': 'dt.month',
            'product_key': 'fi.product_key',
            'store_key': 'fi.store_key',
            'state': 'dl.state',
            'city': 'dl.city',
        },
    },
}


# ── Dimension values lookup config ───────────────────────────

DIM_VALUES_CONFIG = {
    'sales': {
        'year':          {'table': 'dwh.Dim_Time',     'column': 'year'},
        'quarter':       {'table': 'dwh.Dim_Time',     'column': 'quarter'},
        'month':         {'table': 'dwh.Dim_Time',     'column': 'month'},
        'customer_key':  {'table': 'dwh.Dim_Customer',  'column': 'customer_key'},
        'customer_type': {'table': 'dwh.Dim_Customer',  'column': 'customer_type'},
        'customer_name': {'table': 'dwh.Dim_Customer',  'column': 'customer_name'},
        'state':         {'table': 'dwh.Dim_Location',  'column': 'state'},
        'city':          {'table': 'dwh.Dim_Location',  'column': 'city'},
        'product_key':   {'table': 'dwh.Dim_Product',   'column': 'product_key'},
        'description':   {'table': 'dwh.Dim_Product',   'column': 'description'},
    },
    'inventory': {
        'year':          {'table': 'dwh.Dim_Time',     'column': 'year'},
        'quarter':       {'table': 'dwh.Dim_Time',     'column': 'quarter'},
        'month':         {'table': 'dwh.Dim_Time',     'column': 'month'},
        'store_key':     {'table': 'dwh.Dim_Store',    'column': 'store_key'},
        'state':         {'table': 'dwh.Dim_Location', 'column': 'state'},
        'city':          {'table': 'dwh.Dim_Location', 'column': 'city'},
        'product_key':   {'table': 'dwh.Dim_Product',  'column': 'product_key'},
        'description':   {'table': 'dwh.Dim_Product',  'column': 'description'},
    },
}


# ═══════════════════════════════════════════════════════════════
# ROUTER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def _extract_key(cube: str, dimensions: list[DimensionLevel]) -> tuple:
    """
    Convert a list of DimensionLevel objects into the cuboid lookup key.
    Returns a tuple: (time_level, dim2_level, product_level)
    """
    time_level = None
    dim2_level = None   # customer (sales) or store (inventory)
    product_level = None

    for d in dimensions:
        if d.dimension == 'time':
            time_level = d.level
        elif d.dimension in ('customer', 'store'):
            dim2_level = d.level
        elif d.dimension == 'product':
            product_level = d.level

    return (time_level, dim2_level, product_level)


def resolve_cuboid(cube: str, dimensions: list[DimensionLevel]) -> dict:
    """
    Given a cube name and list of active dimensions with levels,
    resolve the matching cuboid.

    Returns:
        dict with keys: 'table', 'dim_cols', 'measures'

    Raises:
        ValueError if no matching cuboid exists for the combination.
    """
    cuboid_map = CUBE_META[cube]['cuboid_map']
    key = _extract_key(cube, dimensions)

    if key not in cuboid_map:
        raise ValueError(
            f"No cuboid found for cube='{cube}' with dimension key={key}. "
            f"Valid keys: {list(cuboid_map.keys())}"
        )

    return cuboid_map[key]


def resolve_drill_across(
    source_cube: str,
    dimensions: list[DimensionLevel],
) -> tuple[dict, dict, list[str]]:
    """
    Resolve cuboids for drill-across: find matching cuboid in BOTH cubes
    using only shared dimensions (time, product).

    Returns:
        (sales_cuboid, inventory_cuboid, join_columns)
    """
    # Extract only shared dimensions (time and product)
    shared_dims = [d for d in dimensions if d.dimension in ('time', 'product')]

    # For each cube, we need the key with only shared dimensions
    sales_key = _extract_key('sales', shared_dims)
    inv_key = _extract_key('inventory', shared_dims)

    sales_cuboid = SALES_CUBOIDS.get(sales_key)
    inv_cuboid = INVENTORY_CUBOIDS.get(inv_key)

    if not sales_cuboid or not inv_cuboid:
        raise ValueError(
            f"Cannot drill across with shared dimensions {shared_dims}. "
            f"Sales key={sales_key} found={sales_cuboid is not None}, "
            f"Inventory key={inv_key} found={inv_cuboid is not None}"
        )

    # Determine join columns (intersection of dim_cols)
    join_cols = [c for c in sales_cuboid['dim_cols'] if c in inv_cuboid['dim_cols']]

    return sales_cuboid, inv_cuboid, join_cols