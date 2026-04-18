# =============================================================
# olap_cuboid_defs.py
# Tập trung định nghĩa TẤT CẢ cuboids của cả 2 cube.
# Mỗi entry: {'table': str, 'sql': str, 'columns': list[str]}
# =============================================================

# ── helpers ──────────────────────────────────────────────────
_FS  = "dwh.Fact_Sales fs"
_FI  = "dwh.Fact_Inventory fi"
_DT_S  = "JOIN dwh.Dim_Time     dt  ON fs.time_key     = dt.time_key"
_DT_I  = "JOIN dwh.Dim_Time     dt  ON fi.time_key     = dt.time_key"
_DC    = "JOIN dwh.Dim_Customer dc  ON fs.customer_key = dc.customer_key"
_DL_C  = "JOIN dwh.Dim_Location dl  ON dc.location_key = dl.location_key"
_DS    = "JOIN dwh.Dim_Store    ds  ON fi.store_key    = ds.store_key"
_DL_S  = "JOIN dwh.Dim_Location dl  ON ds.location_key = dl.location_key"

# ── shorthands for SELECT / GROUP BY blocks ──────────────────
_M  = "dt.year, dt.quarter, dt.month"   # month level (includes q & y)
_Q  = "dt.year, dt.quarter"             # quarter level
_Y  = "dt.year"                         # year level
_CK_INFO = "fs.customer_key, dc.customer_type"                   # customer_key + info
_CK_LOC  = "fs.customer_key, dl.state, dl.city"                  # customer_key + location
_CT = "dc.customer_type"
_CI = "dl.state, dl.city"  # city level → always include state to disambiguate
_ST = "dl.state"
_PK = "fs.product_key"
_SK = "fi.store_key, dl.city, dl.state" # store_key level (inv)
_CI2= "dl.state, dl.city"  # same for Inventory cube
_ST2= "dl.state"

_SALES_M = "SUM(fs.quantity_ordered) AS total_quantity, SUM(fs.total_amount) AS sum_amount"
_INV_M   = "SUM(fi.quantity_on_hand) AS total_quantity_on_hand"


# =============================================================
# CUBE 1 – Sales  (48 cuboids)
# =============================================================

CUBE1 = [

    # ── Dim-0: ALL ───────────────────────────────────────────
    {
        'table': 'olap_sales_all',
        'sql': f"SELECT {_SALES_M} FROM {_FS}",
        'columns': ['total_quantity', 'sum_amount'],
    },

    # ── Dim-1: Time only ─────────────────────────────────────
    {
        'table': 'olap_sales_by_time',
        'sql': f"SELECT {_M}, {_SALES_M} FROM {_FS} {_DT_S} GROUP BY {_M}",
        'columns': ['year','quarter','month','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_by_quarter',
        'sql': f"SELECT {_Q}, {_SALES_M} FROM {_FS} {_DT_S} GROUP BY {_Q}",
        'columns': ['year','quarter','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_by_year',
        'sql': f"SELECT {_Y}, {_SALES_M} FROM {_FS} {_DT_S} GROUP BY {_Y}",
        'columns': ['year','total_quantity','sum_amount'],
    },

    # ── Dim-1: Customer only ─────────────────────────────────
    {
        'table': 'olap_sales_by_customer_info',
        'sql': (f"SELECT {_CK_INFO}, {_SALES_M} FROM {_FS} {_DC} "
                f"GROUP BY {_CK_INFO}"),
        'columns': ['customer_key','customer_type','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_by_customer_loc',
        'sql': (f"SELECT {_CK_LOC}, {_SALES_M} FROM {_FS} {_DC} {_DL_C} "
                f"GROUP BY {_CK_LOC}"),
        'columns': ['customer_key','state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_by_customer_type',
        'sql': (f"SELECT {_CT}, {_SALES_M} FROM {_FS} {_DC} GROUP BY {_CT}"),
        'columns': ['customer_type','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_by_city',
        'sql': (f"SELECT {_CI}, {_SALES_M} FROM {_FS} {_DC} {_DL_C} GROUP BY {_CI}"),
        'columns': ['state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_by_state',
        'sql': (f"SELECT {_ST}, {_SALES_M} FROM {_FS} {_DC} {_DL_C} GROUP BY {_ST}"),
        'columns': ['state','total_quantity','sum_amount'],
    },

    # ── Dim-1: Product only ───────────────────────────────────
    {
        'table': 'olap_sales_by_product',
        'sql': f"SELECT {_PK}, {_SALES_M} FROM {_FS} GROUP BY {_PK}",
        'columns': ['product_key','total_quantity','sum_amount'],
    },

    # ── Dim-2: Time × Product ────────────────────────────────
    {
        'table': 'olap_sales_time_product',
        'sql': (f"SELECT {_M}, {_PK}, {_SALES_M} FROM {_FS} {_DT_S} "
                f"GROUP BY {_M}, {_PK}"),
        'columns': ['year','quarter','month','product_key','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_product',
        'sql': (f"SELECT {_Q}, {_PK}, {_SALES_M} FROM {_FS} {_DT_S} "
                f"GROUP BY {_Q}, {_PK}"),
        'columns': ['year','quarter','product_key','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_product',
        'sql': (f"SELECT {_Y}, {_PK}, {_SALES_M} FROM {_FS} {_DT_S} "
                f"GROUP BY {_Y}, {_PK}"),
        'columns': ['year','product_key','total_quantity','sum_amount'],
    },

    # ── Dim-2: Time × Customer (month level) ─────────────────
    {
        'table': 'olap_sales_time_customer_info',
        'sql': (f"SELECT {_M}, {_CK_INFO}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_M}, {_CK_INFO}"),
        'columns': ['year','quarter','month','customer_key','customer_type',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_time_customer_loc',
        'sql': (f"SELECT {_M}, {_CK_LOC}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_M}, {_CK_LOC}"),
        'columns': ['year','quarter','month','customer_key','state','city',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_month_customer_type',
        'sql': (f"SELECT {_M}, {_CT}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_M}, {_CT}"),
        'columns': ['year','quarter','month','customer_type','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_month_city',
        'sql': (f"SELECT {_M}, {_CI}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_M}, {_CI}"),
        'columns': ['year','quarter','month','state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_month_state',
        'sql': (f"SELECT {_M}, {_ST}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_M}, {_ST}"),
        'columns': ['year','quarter','month','state','total_quantity','sum_amount'],
    },

    # ── Dim-2: Time × Customer (quarter level) ───────────────
    {
        'table': 'olap_sales_quarter_customer_info',
        'sql': (f"SELECT {_Q}, {_CK_INFO}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_Q}, {_CK_INFO}"),
        'columns': ['year','quarter','customer_key','customer_type',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_customer_loc',
        'sql': (f"SELECT {_Q}, {_CK_LOC}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Q}, {_CK_LOC}"),
        'columns': ['year','quarter','customer_key','state','city',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_customer_type',
        'sql': (f"SELECT {_Q}, {_CT}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_Q}, {_CT}"),
        'columns': ['year','quarter','customer_type','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_city',
        'sql': (f"SELECT {_Q}, {_CI}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Q}, {_CI}"),
        'columns': ['year','quarter','state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_state',
        'sql': (f"SELECT {_Q}, {_ST}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Q}, {_ST}"),
        'columns': ['year','quarter','state','total_quantity','sum_amount'],
    },

    # ── Dim-2: Time × Customer (year level) ─────────────────
    {
        'table': 'olap_sales_year_customer_info',
        'sql': (f"SELECT {_Y}, {_CK_INFO}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_Y}, {_CK_INFO}"),
        'columns': ['year','customer_key','customer_type',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_customer_loc',
        'sql': (f"SELECT {_Y}, {_CK_LOC}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Y}, {_CK_LOC}"),
        'columns': ['year','customer_key','state','city',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_customer_type',
        'sql': (f"SELECT {_Y}, {_CT}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_Y}, {_CT}"),
        'columns': ['year','customer_type','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_city',
        'sql': (f"SELECT {_Y}, {_CI}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Y}, {_CI}"),
        'columns': ['year','state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_state',
        'sql': (f"SELECT {_Y}, {_ST}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Y}, {_ST}"),
        'columns': ['year','state','total_quantity','sum_amount'],
    },

    # ── Dim-2: Product × Customer ────────────────────────────
    {
        'table': 'olap_sales_customer_product_info',
        'sql': (f"SELECT {_CK_INFO}, {_PK}, {_SALES_M} FROM {_FS} {_DC} "
                f"GROUP BY {_CK_INFO}, {_PK}"),
        'columns': ['customer_key','customer_type','product_key',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_customer_product_loc',
        'sql': (f"SELECT {_CK_LOC}, {_PK}, {_SALES_M} FROM {_FS} {_DC} {_DL_C} "
                f"GROUP BY {_CK_LOC}, {_PK}"),
        'columns': ['customer_key','state','city','product_key',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_product_customer_type',
        'sql': (f"SELECT {_PK}, {_CT}, {_SALES_M} FROM {_FS} {_DC} "
                f"GROUP BY {_PK}, {_CT}"),
        'columns': ['product_key','customer_type','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_product_city',
        'sql': (f"SELECT {_PK}, {_CI}, {_SALES_M} FROM {_FS} {_DC} {_DL_C} "
                f"GROUP BY {_PK}, {_CI}"),
        'columns': ['product_key','state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_product_state',
        'sql': (f"SELECT {_PK}, {_ST}, {_SALES_M} FROM {_FS} {_DC} {_DL_C} "
                f"GROUP BY {_PK}, {_ST}"),
        'columns': ['product_key','state','total_quantity','sum_amount'],
    },

    # ── Dim-3: Time × Product × Customer (month) ─────────────
    {
        'table': 'olap_sales_base_info',
        'sql': (f"SELECT {_M}, {_PK}, {_CK_INFO}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_M}, {_PK}, {_CK_INFO}"),
        'columns': ['year','quarter','month','product_key','customer_key','customer_type',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_base_loc',
        'sql': (f"SELECT {_M}, {_PK}, {_CK_LOC}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_M}, {_PK}, {_CK_LOC}"),
        'columns': ['year','quarter','month','product_key','customer_key','state','city',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_month_product_customer_type',
        'sql': (f"SELECT {_M}, {_PK}, {_CT}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_M}, {_PK}, {_CT}"),
        'columns': ['year','quarter','month','product_key','customer_type',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_month_product_city',
        'sql': (f"SELECT {_M}, {_PK}, {_CI}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_M}, {_PK}, {_CI}"),
        'columns': ['year','quarter','month','product_key','state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_month_product_state',
        'sql': (f"SELECT {_M}, {_PK}, {_ST}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_M}, {_PK}, {_ST}"),
        'columns': ['year','quarter','month','product_key','state','total_quantity','sum_amount'],
    },

    # ── Dim-3: Time × Product × Customer (quarter) ───────────
    {
        'table': 'olap_sales_quarter_product_customer_info',
        'sql': (f"SELECT {_Q}, {_PK}, {_CK_INFO}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_Q}, {_PK}, {_CK_INFO}"),
        'columns': ['year','quarter','product_key','customer_key','customer_type',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_product_customer_loc',
        'sql': (f"SELECT {_Q}, {_PK}, {_CK_LOC}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Q}, {_PK}, {_CK_LOC}"),
        'columns': ['year','quarter','product_key','customer_key','state','city',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_product_customer_type',
        'sql': (f"SELECT {_Q}, {_PK}, {_CT}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_Q}, {_PK}, {_CT}"),
        'columns': ['year','quarter','product_key','customer_type','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_product_city',
        'sql': (f"SELECT {_Q}, {_PK}, {_CI}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Q}, {_PK}, {_CI}"),
        'columns': ['year','quarter','product_key','state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_quarter_product_state',
        'sql': (f"SELECT {_Q}, {_PK}, {_ST}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Q}, {_PK}, {_ST}"),
        'columns': ['year','quarter','product_key','state','total_quantity','sum_amount'],
    },

    # ── Dim-3: Time × Product × Customer (year) ──────────────
    {
        'table': 'olap_sales_year_product_customer_info',
        'sql': (f"SELECT {_Y}, {_PK}, {_CK_INFO}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_Y}, {_PK}, {_CK_INFO}"),
        'columns': ['year','product_key','customer_key','customer_type',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_product_customer_loc',
        'sql': (f"SELECT {_Y}, {_PK}, {_CK_LOC}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Y}, {_PK}, {_CK_LOC}"),
        'columns': ['year','product_key','customer_key','state','city',
                    'total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_product_customer_type',
        'sql': (f"SELECT {_Y}, {_PK}, {_CT}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} "
                f"GROUP BY {_Y}, {_PK}, {_CT}"),
        'columns': ['year','product_key','customer_type','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_product_city',
        'sql': (f"SELECT {_Y}, {_PK}, {_CI}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Y}, {_PK}, {_CI}"),
        'columns': ['year','product_key','state','city','total_quantity','sum_amount'],
    },
    {
        'table': 'olap_sales_year_product_state',
        'sql': (f"SELECT {_Y}, {_PK}, {_ST}, {_SALES_M} FROM {_FS} {_DT_S} {_DC} {_DL_C} "
                f"GROUP BY {_Y}, {_PK}, {_ST}"),
        'columns': ['year','product_key','state','total_quantity','sum_amount'],
    },
]  # end CUBE1  (48 entries)


# =============================================================
# CUBE 2 – Inventory  (32 cuboids)
# =============================================================

# store-level helpers (Inventory)
_SK_COLS = "fi.store_key, dl.city, dl.state"
_PI   = "fi.product_key"


def _build_inv_snapshot_sql(select_clause, group_clause=""):
    # Determine time group columns
    time_cols = []
    if "dt.year" in select_clause: time_cols.append("dt.year")
    if "dt.quarter" in select_clause: time_cols.append("dt.quarter")
    if "dt.month" in select_clause: time_cols.append("dt.month")
    
    partition = "fi.product_key, fi.store_key"
    if time_cols:
        partition += ", " + ", ".join(time_cols)
        
    subquery = (
        f"SELECT DISTINCT ON ({partition}) "
        f"  fi.quantity_on_hand, fi.product_key, fi.store_key, "
        f"  dt.year, dt.quarter, dt.month, dl.city, dl.state "
        f"FROM dwh.Fact_Inventory fi "
        f"JOIN dwh.Dim_Time dt ON fi.time_key = dt.time_key "
        f"JOIN dwh.Dim_Store ds ON fi.store_key = ds.store_key "
        f"JOIN dwh.Dim_Location dl ON ds.location_key = dl.location_key "
        f"ORDER BY {partition}, fi.time_key DESC"
    )
    
    # Strip aliases for the outer select/group
    outer_select = select_clause.replace("fi.", "").replace("dt.", "").replace("dl.", "").replace("SUM(quantity_on_hand)", "SUM(base.quantity_on_hand)")
    outer_group = group_clause.replace("fi.", "").replace("dt.", "").replace("dl.", "")
    
    group_str = f" GROUP BY {outer_group}" if outer_group else ""
    return f"SELECT {outer_select} FROM ({subquery}) base{group_str}"

CUBE2 = [
    { 'table': 'olap_inv_all', 'sql': _build_inv_snapshot_sql(f"{_INV_M}"), 'columns': ['total_quantity_on_hand'] },
    
    # Dim-1: Time only
    { 'table': 'olap_inv_by_time', 'sql': _build_inv_snapshot_sql(f"{_M}, {_INV_M}", f"{_M}"), 'columns': ['year','quarter','month','total_quantity_on_hand'] },
    { 'table': 'olap_inv_by_quarter', 'sql': _build_inv_snapshot_sql(f"{_Q}, {_INV_M}", f"{_Q}"), 'columns': ['year','quarter','total_quantity_on_hand'] },
    { 'table': 'olap_inv_by_year', 'sql': _build_inv_snapshot_sql(f"{_Y}, {_INV_M}", f"{_Y}"), 'columns': ['year','total_quantity_on_hand'] },
    
    # Dim-1: Product only
    { 'table': 'olap_inv_by_product', 'sql': _build_inv_snapshot_sql(f"{_PI}, {_INV_M}", f"{_PI}"), 'columns': ['product_key','total_quantity_on_hand'] },
    
    # Dim-1: Store only
    { 'table': 'olap_inv_by_store', 'sql': _build_inv_snapshot_sql(f"{_SK_COLS}, {_INV_M}", f"{_SK_COLS}"), 'columns': ['store_key','city','state','total_quantity_on_hand'] },
    { 'table': 'olap_inv_by_city', 'sql': _build_inv_snapshot_sql(f"{_CI2}, {_INV_M}", f"{_CI2}"), 'columns': ['state','city','total_quantity_on_hand'] },
    { 'table': 'olap_inv_by_state', 'sql': _build_inv_snapshot_sql(f"{_ST2}, {_INV_M}", f"{_ST2}"), 'columns': ['state','total_quantity_on_hand'] },
    
    # Dim-2: Time x Product
    { 'table': 'olap_inv_time_product', 'sql': _build_inv_snapshot_sql(f"{_M}, {_PI}, {_INV_M}", f"{_M}, {_PI}"), 'columns': ['year','quarter','month','product_key','total_quantity_on_hand'] },
    { 'table': 'olap_inv_quarter_product', 'sql': _build_inv_snapshot_sql(f"{_Q}, {_PI}, {_INV_M}", f"{_Q}, {_PI}"), 'columns': ['year','quarter','product_key','total_quantity_on_hand'] },
    { 'table': 'olap_inv_year_product', 'sql': _build_inv_snapshot_sql(f"{_Y}, {_PI}, {_INV_M}", f"{_Y}, {_PI}"), 'columns': ['year','product_key','total_quantity_on_hand'] },
    
    # Dim-2: Time x Store
    { 'table': 'olap_inv_time_store', 'sql': _build_inv_snapshot_sql(f"{_M}, {_SK_COLS}, {_INV_M}", f"{_M}, {_SK_COLS}"), 'columns': ['year','quarter','month','store_key','city','state','total_quantity_on_hand'] },
    { 'table': 'olap_inv_month_city', 'sql': _build_inv_snapshot_sql(f"{_M}, {_CI2}, {_INV_M}", f"{_M}, {_CI2}"), 'columns': ['year','quarter','month','state','city','total_quantity_on_hand'] },
    { 'table': 'olap_inv_month_state', 'sql': _build_inv_snapshot_sql(f"{_M}, {_ST2}, {_INV_M}", f"{_M}, {_ST2}"), 'columns': ['year','quarter','month','state','total_quantity_on_hand'] },
    
    { 'table': 'olap_inv_quarter_store', 'sql': _build_inv_snapshot_sql(f"{_Q}, {_SK_COLS}, {_INV_M}", f"{_Q}, {_SK_COLS}"), 'columns': ['year','quarter','store_key','city','state','total_quantity_on_hand'] },
    { 'table': 'olap_inv_quarter_city', 'sql': _build_inv_snapshot_sql(f"{_Q}, {_CI2}, {_INV_M}", f"{_Q}, {_CI2}"), 'columns': ['year','quarter','state','city','total_quantity_on_hand'] },
    { 'table': 'olap_inv_quarter_state', 'sql': _build_inv_snapshot_sql(f"{_Q}, {_ST2}, {_INV_M}", f"{_Q}, {_ST2}"), 'columns': ['year','quarter','state','total_quantity_on_hand'] },
    
    { 'table': 'olap_inv_year_store', 'sql': _build_inv_snapshot_sql(f"{_Y}, {_SK_COLS}, {_INV_M}", f"{_Y}, {_SK_COLS}"), 'columns': ['year','store_key','city','state','total_quantity_on_hand'] },
    { 'table': 'olap_inv_year_city', 'sql': _build_inv_snapshot_sql(f"{_Y}, {_CI2}, {_INV_M}", f"{_Y}, {_CI2}"), 'columns': ['year','state','city','total_quantity_on_hand'] },
    { 'table': 'olap_inv_year_state', 'sql': _build_inv_snapshot_sql(f"{_Y}, {_ST2}, {_INV_M}", f"{_Y}, {_ST2}"), 'columns': ['year','state','total_quantity_on_hand'] },
    
    # Dim-2: Product x Store
    { 'table': 'olap_inv_product_store', 'sql': _build_inv_snapshot_sql(f"{_PI}, {_SK_COLS}, {_INV_M}", f"{_PI}, {_SK_COLS}"), 'columns': ['product_key','store_key','city','state','total_quantity_on_hand'] },
    { 'table': 'olap_inv_product_city', 'sql': _build_inv_snapshot_sql(f"{_PI}, {_CI2}, {_INV_M}", f"{_PI}, {_CI2}"), 'columns': ['product_key','state','city','total_quantity_on_hand'] },
    { 'table': 'olap_inv_product_state', 'sql': _build_inv_snapshot_sql(f"{_PI}, {_ST2}, {_INV_M}", f"{_PI}, {_ST2}"), 'columns': ['product_key','state','total_quantity_on_hand'] },
    
    # Dim-3: Time x Product x Store
    { 'table': 'olap_inv_base', 'sql': _build_inv_snapshot_sql(f"{_M}, {_PI}, {_SK_COLS}, {_INV_M}", f"{_M}, {_PI}, {_SK_COLS}"), 'columns': ['year','quarter','month','product_key','store_key','city','state','total_quantity_on_hand'] },
    { 'table': 'olap_inv_month_product_city', 'sql': _build_inv_snapshot_sql(f"{_M}, {_PI}, {_CI2}, {_INV_M}", f"{_M}, {_PI}, {_CI2}"), 'columns': ['year','quarter','month','product_key','state','city','total_quantity_on_hand'] },
    { 'table': 'olap_inv_month_product_state', 'sql': _build_inv_snapshot_sql(f"{_M}, {_PI}, {_ST2}, {_INV_M}", f"{_M}, {_PI}, {_ST2}"), 'columns': ['year','quarter','month','product_key','state','total_quantity_on_hand'] },
    
    { 'table': 'olap_inv_quarter_product_store', 'sql': _build_inv_snapshot_sql(f"{_Q}, {_PI}, {_SK_COLS}, {_INV_M}", f"{_Q}, {_PI}, {_SK_COLS}"), 'columns': ['year','quarter','product_key','store_key','city','state','total_quantity_on_hand'] },
    { 'table': 'olap_inv_quarter_product_city', 'sql': _build_inv_snapshot_sql(f"{_Q}, {_PI}, {_CI2}, {_INV_M}", f"{_Q}, {_PI}, {_CI2}"), 'columns': ['year','quarter','product_key','state','city','total_quantity_on_hand'] },
    { 'table': 'olap_inv_quarter_product_state', 'sql': _build_inv_snapshot_sql(f"{_Q}, {_PI}, {_ST2}, {_INV_M}", f"{_Q}, {_PI}, {_ST2}"), 'columns': ['year','quarter','product_key','state','total_quantity_on_hand'] },
    
    { 'table': 'olap_inv_year_product_store', 'sql': _build_inv_snapshot_sql(f"{_Y}, {_PI}, {_SK_COLS}, {_INV_M}", f"{_Y}, {_PI}, {_SK_COLS}"), 'columns': ['year','product_key','store_key','city','state','total_quantity_on_hand'] },
    { 'table': 'olap_inv_year_product_city', 'sql': _build_inv_snapshot_sql(f"{_Y}, {_PI}, {_CI2}, {_INV_M}", f"{_Y}, {_PI}, {_CI2}"), 'columns': ['year','product_key','state','city','total_quantity_on_hand'] },
    { 'table': 'olap_inv_year_product_state', 'sql': _build_inv_snapshot_sql(f"{_Y}, {_PI}, {_ST2}, {_INV_M}", f"{_Y}, {_PI}, {_ST2}"), 'columns': ['year','product_key','state','total_quantity_on_hand'] },
]  # end CUBE2  (32 entries)


# ── Convenience mapping ───────────────────────────────────────
ALL_CUBOIDS = {
    'cube1': CUBE1,
    'cube2': CUBE2,
}
