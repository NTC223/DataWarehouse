"""
================================================================================
OLAP_ROUTER.PY - Cuboid Router Algorithm
================================================================================
Module chứa thuật toán Cuboid Router - Tự động chọn bảng Cuboid tối ưu nhất
dựa trên các dimensions được yêu cầu.

Logic cốt lõi (đồng bộ `schema olap.sql`):
  B1 — Time prefix: filter/cột month → month_; quarter → quarter_; year → year_; còn lại → full/none.
  B2 — Sales: ưu tiên granule khách customer_key > city > state > customer_type (tie-break).
  B3 — Chọn bảng `olap_sales_*` / `olap_inv_*` có tập cột nhỏ nhất mà vẫn chứa đủ pivot + filter.
  Fallback Sales: `olap_sales_base_loc`; Inventory: `olap_inv_base`.

Author: Data Warehouse Team
================================================================================
"""

from typing import List, Dict, Set, Tuple, Optional, Any, FrozenSet
from dataclasses import dataclass
import logging

# Import database module
from app.database import execute_query, execute_query_paginated, OLAP_SCHEMA
from app.services.olap_schema_tables import (
    INVENTORY_MEASURE_COLUMNS,
    INVENTORY_OLAP_TABLE_COLUMNS,
    SALES_MEASURE_COLUMNS,
    SALES_OLAP_TABLE_COLUMNS,
)

# Cấu hình logging
logger = logging.getLogger(__name__)

# Hằng số định danh cột ảo dùng cho Global Row-Total Sort (Window Function)
ROW_TOTAL_KEY = "__row_total__"


# ==============================================================================
# DATA CLASSES - Định nghĩa cấu trúc dữ liệu
# ==============================================================================
@dataclass
class CuboidMetadata:
    """
    Metadata của một bảng Cuboid.
    
    Attributes:
        table_name: Tên bảng trong database
        dimensions: Set các dimension mà bảng này chứa
        columns: List các cột cụ thể trong bảng
        measure_columns: List các cột measure (total_quantity, sum_amount)
        priority: Độ ưu tiên (số cột ít hơn = ưu tiên cao hơn)
    """
    table_name: str
    dimensions: Set[str]
    columns: List[str]
    measure_columns: List[str]
    priority: int = 0
    
    def __post_init__(self):
        """Tính độ ưu tiên sau khi khởi tạo."""
        self.priority = len(self.columns)


@dataclass
class RoutingResult:
    """
    Kết quả của thuật toán Cuboid Routing.
    
    Attributes:
        selected_table: Tên bảng được chọn
        dimensions: Các dimension được yêu cầu
        sql: Câu SQL được sinh ra
        reason: Lý do chọn bảng này
    """
    selected_table: str
    dimensions: Set[str]
    sql: str
    reason: str


# ==============================================================================
# DIMENSION MAPPING — không còn dimension trừu tượng "location"
# (state/city thuộc chiều khách hàng / cửa hàng theo schema olap.sql)
# ==============================================================================
DIMENSION_COLUMNS = {
    "time": ["year", "quarter", "month"],
    "product": ["product_key"],
    "customer": ["customer_type", "customer_key", "state", "city"],
    "store": ["store_key"],
}

# Inventory: địa lý gắn với cửa hàng (schema olap_inv_*)
INVENTORY_DIMENSION_COLUMNS = {
    "time": ["year", "quarter", "month"],
    "product": ["product_key"],
    "store": ["store_key", "state", "city"],
}


def _collect_needed_columns(
    required_columns: List[str],
    required_filters: Optional[Dict[str, Any]] = None,
) -> Set[str]:
    need = set(required_columns or [])
    for k, v in (required_filters or {}).items():
        if v is None or v == "" or v == "All":
            continue
        need.add(k)
    return need


def infer_time_prefix_family(
    filters: Optional[Dict[str, Any]], needed: Set[str]
) -> str:
    """
    B1: month_ > quarter_ > year_ > full (by_time / time_* / base_*) > none.
    Dựa trên filter ràng buộc và/hoặc cột pivot yêu cầu.
    """
    rf = filters or {}

    def _active(key: str) -> bool:
        v = rf.get(key)
        return v is not None and v != "" and v != "All"

    if _active("month") or "month" in needed:
        return "month"
    if _active("quarter") or "quarter" in needed:
        return "quarter"
    if _active("year") or "year" in needed:
        return "year"
    if needed & {"year", "quarter", "month"}:
        return "full"
    return "none"


def _sales_table_time_family(table: str) -> str:
    if table.startswith("olap_sales_month"):
        return "month"
    if table.startswith("olap_sales_quarter") or table == "olap_sales_by_quarter":
        return "quarter"
    if table.startswith("olap_sales_year") or table == "olap_sales_by_year":
        return "year"
    if "base_" in table or "_time_" in table or table == "olap_sales_by_time":
        return "full"
    return "none"


def _inv_table_time_family(table: str) -> str:
    if table.startswith("olap_inv_month"):
        return "month"
    if table.startswith("olap_inv_quarter") or table == "olap_inv_by_quarter":
        return "quarter"
    if table.startswith("olap_inv_year") or table == "olap_inv_by_year":
        return "year"
    if table in ("olap_inv_by_time", "olap_inv_base") or table.startswith("olap_inv_time"):
        return "full"
    return "none"


def _sales_customer_granule_rank(table: str, needed: Set[str]) -> int:
    """
    B2: Ưu tiên chiều khách — customer_key > city > state > customer_type (rank thấp hơn = tốt hơn).
    Chỉ dùng khi tie-break cùng số cột.
    """
    if not needed.intersection({"customer_key", "city", "state", "customer_type"}):
        return 0
    if "customer_key" in needed:
        if "_customer_loc" in table or "base_loc" in table or "_product_customer_loc" in table:
            return 1
        if "_customer_info" in table or "base_info" in table or "_product_customer_info" in table:
            return 0
        if "customer_product_loc" in table:
            return 4
        if "customer_product_info" in table:
            return 3
        if "by_customer_loc" in table:
            return 6
        if "by_customer_info" in table:
            return 5
        return 20
    if "city" in needed:
        return 10 if ("_city" in table or "_loc" in table) else 30
    if "state" in needed:
        return 15 if ("_state" in table or "_city" in table or "_loc" in table) else 35
    if "customer_type" in needed:
        return 25 if "customer_type" in table else 40
    return 0


def _inv_store_granule_rank(table: str, needed: Set[str]) -> int:
    """Inventory: store_key + địa lý — ưu tiên bảng có store_key khi cần."""
    if "store_key" not in needed:
        return 0
    if "product_store" in table or "_product_store" in table:
        return 0
    if "time_store" in table or "year_store" in table or "quarter_store" in table or "by_store" in table:
        return 1
    if table == "olap_inv_base":
        return 2
    return 10


def pick_optimal_olap_table(
    catalog: Dict[str, FrozenSet[str]],
    needed: Set[str],
    base_table: str,
    filters: Optional[Dict[str, Any]],
    time_family_fn,
    rank_fn=None,
) -> Tuple[str, str]:
    """
    B3: chọn bảng nhỏ nhất (ít cột) mà vẫn là superset của mọi cột pivot/filter,
    tie-break theo khớp B1 (prefix time) rồi B2 (granule rank).
    """
    family = infer_time_prefix_family(filters, needed)
    family_order = {"none": 0, "year": 1, "quarter": 2, "month": 3, "full": 4}
    tgt = family_order.get(family, 0)
    scored: List[Tuple[Tuple, str]] = []
    for tname, cols in catalog.items():
        if not needed <= cols:
            continue
        tf = time_family_fn(tname)
        dist = abs(family_order.get(tf, 0) - tgt)
        cr = rank_fn(tname, needed) if rank_fn else 0
        key = (len(cols), dist, cr, tname)
        scored.append((key, tname))
    if not scored:
        return base_table, f"Fallback {base_table} — không có bảng chứa đủ cột {sorted(needed)}"
    scored.sort(key=lambda x: x[0])
    chosen = scored[0][1]
    reason = (
        f"B1 time_family={family}, B2+B3 -> {chosen} "
        f"(minimal |cols|={len(catalog[chosen])} among {len(scored)} candidates)"
    )
    return chosen, reason


# ==============================================================================
# CUBOID ROUTER - Thuật toán chọn bảng tối ưu
# ==============================================================================
class CuboidRouter:
    """
    Bộ định tuyến Cuboid Sales — chọn bảng theo `schema olap.sql`:
    prefix month_/quarter_/year_, không dùng dimension "location".
    """

    def __init__(self):
        self.catalog = SALES_OLAP_TABLE_COLUMNS
        self.base_table = "olap_sales_base_loc"
        logger.info(f"✅ [CuboidRouter] Đã khởi tạo với {len(self.catalog)} bảng OLAP Sales")
    
    def map_columns_to_dimensions(self, columns: List[str]) -> Set[str]:
        """
        Ánh xạ từ danh sách cột sang các dimension.
        
        Args:
            columns: List tên cột (VD: ["year", "product_key"])
        
        Returns:
            Set các dimension (VD: {"time", "product"})
        """
        dimensions = set()
        
        for col in columns:
            for dim, dim_cols in DIMENSION_COLUMNS.items():
                if col in dim_cols:
                    dimensions.add(dim)
                    break
        
        return dimensions
    
    def find_optimal_cuboid(
        self,
        required_columns: List[str],
        required_filters: Dict[str, Any] = None,
    ) -> RoutingResult:
        needed = _collect_needed_columns(required_columns, required_filters)
        required_dims = self.map_columns_to_dimensions(list(needed))
        table, reason = pick_optimal_olap_table(
            self.catalog,
            needed,
            self.base_table,
            required_filters,
            _sales_table_time_family,
            _sales_customer_granule_rank,
        )
        logger.info(f"✅ [CuboidRouter] Chọn bảng: {table} — {reason}")
        return RoutingResult(
            selected_table=table,
            dimensions=required_dims,
            sql="",
            reason=reason,
        )
    
    def build_pivot_query(
        self,
        rows: List[str],
        columns: List[str],
        measures: List[str],
        filters: Dict[str, Any],
        limit: int = 5000,
        page: int = 1,
        page_size: int = 50,
        sort_column: str = None,
        sort_order: str = "asc"
    ) -> Tuple[str, Tuple, str]:
        """
        Xây dựng câu SQL cho Pivot Table.
        
        Args:
            rows: List cột cho hàng (VD: ["year", "quarter"])
            columns: List cột cho cột (VD: ["customer_type"])
            measures: List measure cần tính (VD: ["sum_amount"])
            filters: Dict điều kiện filter
            limit: Giới hạn số bản ghi (mặc định 5000)
        
        Returns:
            Tuple (sql_string, params)
        
        Example:
            sql, params = router.build_pivot_query(
                rows=["year", "quarter"],
                columns=["customer_type"],
                measures=["sum_amount"],
                filters={"state": "California"}
            )
        """
        # Tất cả các cột cần thiết
        all_columns = rows + columns
        
        # Tìm bảng tối ưu
        routing = self.find_optimal_cuboid(all_columns, filters)
        table_name = f"{OLAP_SCHEMA}.{routing.selected_table}"
        
        # Xây dựng SELECT clause
        select_cols = rows + columns
        select_clause = ", ".join(select_cols) if select_cols else "1"
        
        # Xây dựng measure clause
        measure_clause = ", ".join([f"SUM({m}) as {m}" for m in measures])
        
        # Xây dựng WHERE clause
        where_conditions = []
        params = []
        
        for col, value in filters.items():
            if value is not None and value != "All" and value != "":
                where_conditions.append(f"{col} = %s")
                params.append(value)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Xây dựng GROUP BY clause
        group_by_cols = rows + columns
        group_by_clause = ""
        if group_by_cols:
            group_by_clause = "GROUP BY " + ", ".join(group_by_cols)
        
        # OFFSET / LIMIT
        offset = (page - 1) * page_size
        safe_sort_order = "DESC" if str(sort_order).lower() == "desc" else "ASC"

        # ── BƯỚC 1: ĐẾM TỔNG SỐ DÒNG (Unique Rows) ──────────────────────────────
        # Thu thập các điều kiện WHERE cơ bản
        base_conditions = []
        for col, value in filters.items():
            if value is not None and value != "All" and value != "":
                base_conditions.append(f"{col} = %s")
        
        base_where = "WHERE " + " AND ".join(base_conditions) if base_conditions else ""
        
        count_sql = f"""
            SELECT COUNT(*) AS total_rows FROM (
                SELECT {", ".join(rows) if rows else "1"}
                FROM {table_name}
                {base_where}
                GROUP BY {", ".join(rows) if rows else "1"}
            ) AS _count_unique_rows
        """.strip()

        # ── BƯỚC 2: TÌM DANH SÁCH ID HÀNG CHO TRANG NÀY ──────────────────────────
        is_global_total_sort = (sort_column == ROW_TOTAL_KEY and len(rows) > 0)
        valid_sort_cols = set(rows + columns + measures) | {ROW_TOTAL_KEY}
        main_measure = measures[0] if measures else "sum_amount"
        
        # Subquery ID hàng
        if rows:
            if is_global_total_sort:
                row_ids_subq = f"""
                    SELECT {", ".join(rows)}
                    FROM {table_name}
                    {base_where}
                    GROUP BY {", ".join(rows)}
                    ORDER BY SUM({main_measure}) {safe_sort_order}
                    LIMIT {page_size} OFFSET {offset}
                """
            elif sort_column in rows:
                row_ids_subq = f"""
                    SELECT DISTINCT {", ".join(rows)}
                    FROM {table_name}
                    {base_where}
                    ORDER BY {sort_column} {safe_sort_order}
                    LIMIT {page_size} OFFSET {offset}
                """
            else:
                row_ids_subq = f"""
                    SELECT DISTINCT {", ".join(rows)}
                    FROM {table_name}
                    {base_where}
                    ORDER BY {", ".join(rows)}
                    LIMIT {page_size} OFFSET {offset}
                """
        else:
            row_ids_subq = "SELECT 1 LIMIT 1"

        # ── BƯỚC 3: TRUY VẤN DATA CHI TIẾT ────────────────────────────────────────
        # Kết hợp điều kiện Filter và điều kiện Phân trang
        final_conditions = [c for c in base_conditions]
        if rows:
            row_tuple = f"({', '.join(rows)})"
            final_conditions.append(f"{row_tuple} IN ({row_ids_subq})")
        
        final_where = "WHERE " + " AND ".join(final_conditions) if final_conditions else ""

        # Base data query
        base_query = f"""
            SELECT {select_clause}, {measure_clause}
            FROM {table_name}
            {final_where}
            {group_by_clause}
        """

        partition_cols = ", ".join(rows) if rows else "1"
        data_sql = f"""
            SELECT *, SUM({main_measure}) OVER (PARTITION BY {partition_cols}) AS {ROW_TOTAL_KEY}
            FROM ({base_query}) AS _raw_data
        """

        # Final Sort
        if is_global_total_sort:
            data_sql += f" ORDER BY {ROW_TOTAL_KEY} {safe_sort_order}, {', '.join(rows) if rows else '1'}"
        elif sort_column in valid_sort_cols:
            data_sql += f" ORDER BY {sort_column} {safe_sort_order}"
        elif rows:
            data_sql += f" ORDER BY {', '.join(rows)}"

        logger.debug(f"[CuboidRouter] Final Data SQL: {data_sql}")
        
        # Sửa lỗi: Nếu có rows (Nested Query), params xuất hiện 2 lần (chính + subquery)
        # count_sql luôn chỉ dùng 1 lần params
        count_params = tuple(params)
        data_params = tuple(list(params) + list(params)) if rows else count_params

        return data_sql.strip(), data_params, count_sql, count_params



    
    def build_raw_data_query(
        self,
        columns: List[str],
        filters: Dict[str, Any],
        page: int = 1,
        page_size: int = 100
    ) -> Tuple[str, Tuple]:
        """
        Xây dựng câu SQL cho Raw Data với phân trang.
        
        Args:
            columns: List cột cần SELECT (empty = tất cả)
            filters: Dict điều kiện filter
            page: Số trang
            page_size: Kích thước trang
        
        Returns:
            Tuple (sql_string, params)
        """
        # Raw data luôn dùng bảng base
        table_name = f"{OLAP_SCHEMA}.{self.base_table}"
        
        # SELECT clause
        if columns:
            select_clause = ", ".join(columns)
        else:
            select_clause = "*"
        
        # WHERE clause
        where_conditions = []
        params = []
        
        for col, value in filters.items():
            if value is not None and value != "All" and value != "":
                where_conditions.append(f"{col} = %s")
                params.append(value)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # LIMIT và OFFSET
        offset = (page - 1) * page_size
        limit_clause = f"LIMIT {page_size} OFFSET {offset}"
        
        # Ghép câu SQL
        main_measure = "sum_amount"
        sql = f"""
            SELECT {select_clause}
            FROM {table_name}
            {where_clause}
            ORDER BY {main_measure} DESC
            {limit_clause}
        """.strip()
        
        return sql, tuple(params)
    
    def get_cuboid_stats(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "total_cuboids": len(self.catalog),
            "by_dimension_count": {},
            "cuboids": [],
        }
        for tname, cols in self.catalog.items():
            dims = self.map_columns_to_dimensions(
                [c for c in cols if c not in SALES_MEASURE_COLUMNS]
            )
            dim_count = len(dims)
            stats["by_dimension_count"][dim_count] = (
                stats["by_dimension_count"].get(dim_count, 0) + 1
            )
            stats["cuboids"].append(
                {
                    "table": tname,
                    "dimensions": list(dims),
                    "columns": sorted(cols),
                    "priority": len(cols),
                }
            )
        return stats


# ==============================================================================
# SINGLETON INSTANCE
# ==============================================================================
# Khởi tạo singleton instance để tái sử dụng
router = CuboidRouter()


# ==============================================================================
# PUBLIC API - Các hàm sử dụng bên ngoài
# ==============================================================================
def route_and_query_pivot(
    rows: List[str],
    columns: List[str],
    measures: List[str],
    filters: Dict[str, Any],
    limit: int = 5000,
    cube: str = "sales",
    page: int = 1,
    page_size: int = 50,
    sort_column: str = None,
    sort_order: str = "asc"
) -> Dict[str, Any]:
    """
    Thực hiện định tuyến và truy vấn Pivot Table với Pagination & Sorting.

    Args:
        rows, columns, measures, filters: Cấu hình OLAP
        limit: (legacy, không dùng nữa)
        cube: 'sales' | 'inventory'
        page: Số trang (bắt đầu từ 1)
        page_size: Số dòng mỗi trang
        sort_column: Tên cột cần sort (phải nằm trong whitelist rows+columns+measures)
        sort_order: 'asc' | 'desc'

    Returns:
        Dict chứa kết quả, metadata và pagination info
    """
    try:
        # Chọn router dựa trên cube
        active_router = inventory_router if cube == "inventory" else router

        # Xây dựng query - unpack 4-tuple (data_sql, data_params, count_sql, count_params)
        data_sql, data_params, count_sql, count_params = active_router.build_pivot_query(
            rows, columns, measures, filters,
            limit=limit, page=page, page_size=page_size,
            sort_column=sort_column, sort_order=sort_order
        )
        logger.info(f"[route_and_query_pivot] Data SQL: {data_sql}")
        logger.info(f"[route_and_query_pivot] Count SQL: {count_sql}")
        logger.info(f"[route_and_query_pivot] Data Params: {data_params}")
        logger.info(f"[route_and_query_pivot] Count Params: {count_params}")

        # Thực thi count query để lấy total_rows
        count_result = execute_query(count_sql, count_params)
        total_rows = int(count_result[0]["total_rows"]) if count_result else 0
        total_pages = max(1, (total_rows + page_size - 1) // page_size)

        # Thực thi data query
        results = execute_query(data_sql, data_params)
        logger.info(f"[route_and_query_pivot] Found {len(results)} rows (page {page}/{total_pages}, total={total_rows})")
        if results:
            logger.info(f"[route_and_query_pivot] First row: {results[0]}")

        # Tìm bảng được chọn để trả về metadata
        all_columns = rows + columns
        routing = active_router.find_optimal_cuboid(all_columns, filters)

        return {
            "success": True,
            "data": results,
            "metadata": {
                "selected_table": routing.selected_table,
                "reason": routing.reason,
                "dimensions_used": list(routing.dimensions),
                "rows": rows,
                "columns": columns,
                "measures": measures,
                "filters": filters,
                "row_count": len(results),
                "total_rows": total_rows,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "sort_column": sort_column,
                "sort_order": sort_order
            }
        }
        
    except Exception as e:
        logger.error(f"❌ [CuboidRouter] Lỗi truy vấn pivot: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "data": []
        }


def route_and_query_raw(
    columns: List[str],
    filters: Dict[str, Any],
    page: int = 1,
    page_size: int = 100,
    cube: str = "sales"  # ✏️ Thêm cube parameter
) -> Dict[str, Any]:
    """
    Thực hiện truy vấn Raw Data với phân trang.
    
    Args:
        columns: List cột cần SELECT
        filters: Dict điều kiện filter
        page: Số trang
        page_size: Kích thước trang
        cube: Loại cube ('sales' | 'inventory')
    
    Returns:
        Dict chứa kết quả và thông tin phân trang
    """
    try:
        # Chọn router dựa trên cube
        active_router = inventory_router if cube == "inventory" else router
        
        # Xây dựng query
        sql, params = active_router.build_raw_data_query(columns, filters, page, page_size)
        
        # Thực thi query phân trang
        result = execute_query_paginated(sql, params, page, page_size)
        
        return {
            "success": True,
            "data": result["data"],
            "pagination": {
                "page": result["page"],
                "page_size": result["page_size"],
                "total": result["total"],
                "total_pages": result["total_pages"]
            }
        }
        
    except Exception as e:
        logger.error(f"❌ [CuboidRouter] Lỗi truy vấn raw data: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "data": [],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0
            }
        }


def get_router_info() -> Dict[str, Any]:
    """
    Lấy thông tin về Cuboid Router.
    
    Returns:
        Dict chứa thông tin về router và các cuboid
    """
    return {
        "router_status": "active",
        "cuboid_stats": router.get_cuboid_stats(),
        "dimension_mapping": DIMENSION_COLUMNS
    }


# ==============================================================================
# INVENTORY CUBOID ROUTER - Router riêng cho Inventory Cube
# ==============================================================================
class InventoryCuboidRouter:
    """
    Bộ định tuyến Cuboid Inventory (Cube 2) — month_/quarter_/year_ theo schema olap.sql.
    Địa lý (state/city) là thuộc tính store/product, không dùng dimension "location".
    """

    def __init__(self):
        self.catalog = INVENTORY_OLAP_TABLE_COLUMNS
        self.base_table = "olap_inv_base"
        logger.info(f"✅ [InventoryCuboidRouter] Đã khởi tạo với {len(self.catalog)} bảng OLAP Inventory")
    
    def map_columns_to_dimensions(self, columns: List[str]) -> Set[str]:
        """Ánh xạ cột -> dimension cho Inventory (state/city thuộc store)."""
        dimensions = set()
        for col in columns:
            for dim, dim_cols in INVENTORY_DIMENSION_COLUMNS.items():
                if col in dim_cols:
                    dimensions.add(dim)
                    break
        return dimensions
    
    def find_optimal_cuboid(
        self,
        required_columns: List[str],
        required_filters: Dict[str, Any] = None,
    ) -> RoutingResult:
        needed = _collect_needed_columns(required_columns, required_filters)
        required_dims = self.map_columns_to_dimensions(list(needed))
        table, reason = pick_optimal_olap_table(
            self.catalog,
            needed,
            self.base_table,
            required_filters,
            _inv_table_time_family,
            _inv_store_granule_rank,
        )
        logger.info(f"✅ [InventoryCuboidRouter] Chọn bảng: {table} — {reason}")
        return RoutingResult(
            selected_table=table,
            dimensions=required_dims,
            sql="",
            reason=reason,
        )
    
    def build_inventory_query(
        self,
        columns: List[str],
        filters: Dict[str, Any],
        group_by: List[str] = None
    ) -> Tuple[str, Tuple]:
        """Xây dựng câu SQL cho Inventory query."""
        all_columns = columns + (group_by or [])
        
        routing = self.find_optimal_cuboid(all_columns, filters)
        table_name = f"{OLAP_SCHEMA}.{routing.selected_table}"
        
        # SELECT clause
        select_cols = columns if columns else ["*"]
        if group_by:
            select_clause = ", ".join(group_by) + ", SUM(total_quantity_on_hand) as total_quantity_on_hand"
        else:
            select_clause = ", ".join(select_cols)
        
        # WHERE clause
        where_conditions = []
        params = []
        
        for col, value in filters.items():
            if value is not None and value != "All" and value != "":
                where_conditions.append(f"{col} = %s")
                params.append(value)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # GROUP BY clause
        group_by_clause = ""
        if group_by:
            group_by_clause = "GROUP BY " + ", ".join(group_by)
        
        # ORDER BY clause
        order_by_clause = ""
        if group_by:
            order_by_clause = "ORDER BY " + ", ".join(group_by)
        
        sql = f"""
            SELECT {select_clause}
            FROM {table_name}
            {where_clause}
            {group_by_clause}
            {order_by_clause}
        """.strip()
        
        return sql, tuple(params)
    
    def build_pivot_query(
        self,
        rows: List[str],
        columns: List[str],
        measures: List[str],
        filters: Dict[str, Any],
        limit: int = 5000,
        page: int = 1,
        page_size: int = 50,
        sort_column: str = None,
        sort_order: str = "asc"
    ) -> Tuple[str, Tuple, str]:
        """Xây dựng câu SQL cho Pivot Table Inventory."""
        all_columns = rows + columns

        routing = self.find_optimal_cuboid(all_columns, filters)
        table_name = f"{OLAP_SCHEMA}.{routing.selected_table}"

        offset = (page - 1) * page_size
        safe_sort_order = "DESC" if str(sort_order).lower() == "desc" else "ASC"

        # SELECT clause
        select_cols = rows + columns
        select_clause = ", ".join(select_cols) if select_cols else "1"

        # Measure clause - Inventory chỉ có total_quantity_on_hand
        valid_measures = ["total_quantity_on_hand"]
        measures_to_use = [m for m in measures if m in valid_measures] if measures else []
        if not measures_to_use:
            measures_to_use = valid_measures
        measure_clause = ", ".join([f"SUM({m}) as {m}" for m in measures_to_use])

        # WHERE clause
        where_conditions = []
        params = []
        for col, value in filters.items():
            if value is not None and value != "All" and value != "":
                where_conditions.append(f"{col} = %s")
                params.append(value)
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        # GROUP BY clause
        group_by_clause = "GROUP BY " + ", ".join(select_cols) if select_cols else ""

        # ── BƯỚC 1: ĐẾM TỔNG SỐ DÒNG (Unique Rows) ──────────────────────────────
        base_conditions = []
        for col, value in filters.items():
            if value is not None and value != "All" and value != "":
                base_conditions.append(f"{col} = %s")
        
        base_where = "WHERE " + " AND ".join(base_conditions) if base_conditions else ""
        
        count_sql = f"""
            SELECT COUNT(*) AS total_rows FROM (
                SELECT {", ".join(rows) if rows else "1"}
                FROM {table_name}
                {base_where}
                GROUP BY {", ".join(rows) if rows else "1"}
            ) AS _count_unique_rows
        """.strip()

        # ── BƯỚC 2: TÌM DANH SÁCH ID HÀNG CHO TRANG NÀY ──────────────────────────
        is_global_total_sort = (sort_column == ROW_TOTAL_KEY and len(rows) > 0)
        valid_sort_cols = set(rows + columns + [m for m in measures_to_use]) | {ROW_TOTAL_KEY}
        main_measure = measures_to_use[0]
        
        if rows:
            if is_global_total_sort:
                row_ids_subq = f"""
                    SELECT {", ".join(rows)}
                    FROM {table_name}
                    {base_where}
                    GROUP BY {", ".join(rows)}
                    ORDER BY SUM({main_measure}) {safe_sort_order}
                    LIMIT {page_size} OFFSET {offset}
                """
            elif sort_column in rows:
                row_ids_subq = f"""
                    SELECT DISTINCT {", ".join(rows)}
                    FROM {table_name}
                    {base_where}
                    ORDER BY {sort_column} {safe_sort_order}
                    LIMIT {page_size} OFFSET {offset}
                """
            else:
                row_ids_subq = f"""
                    SELECT DISTINCT {", ".join(rows)}
                    FROM {table_name}
                    {base_where}
                    ORDER BY {", ".join(rows)}
                    LIMIT {page_size} OFFSET {offset}
                """
        else:
            row_ids_subq = "SELECT 1 LIMIT 1"

        # ── BƯỚC 3: TRUY VẤN DATA CHI TIẾT ────────────────────────────────────────
        final_conditions = [c for c in base_conditions]
        if rows:
            row_tuple = f"({', '.join(rows)})"
            final_conditions.append(f"{row_tuple} IN ({row_ids_subq})")
        
        final_where = "WHERE " + " AND ".join(final_conditions) if final_conditions else ""

        base_query = f"""
            SELECT {select_clause}, {measure_clause}
            FROM {table_name}
            {final_where}
            {group_by_clause}
        """

        partition_cols = ", ".join(rows) if rows else "1"
        data_sql = f"""
            SELECT *, SUM({main_measure}) OVER (PARTITION BY {partition_cols}) AS {ROW_TOTAL_KEY}
            FROM ({base_query}) AS _raw_data
        """

        if is_global_total_sort:
            data_sql += f" ORDER BY {ROW_TOTAL_KEY} {safe_sort_order}, {', '.join(rows) if rows else '1'}"
        elif sort_column in valid_sort_cols:
            data_sql += f" ORDER BY {sort_column} {safe_sort_order}"
        elif rows:
            data_sql += f" ORDER BY {', '.join(rows)}"

        # Sửa lỗi: Nếu có rows (Nested Query), params xuất hiện 2 lần
        count_params = tuple(params)
        data_params = tuple(list(params) + list(params)) if rows else count_params

        return data_sql.strip(), data_params, count_sql, count_params




    
    def build_raw_data_query(
        self,
        columns: List[str],
        filters: Dict[str, Any],
        page: int = 1,
        page_size: int = 100
    ) -> Tuple[str, Tuple]:
        """Xây dựng câu SQL cho Raw Data Inventory."""
        table_name = f"{OLAP_SCHEMA}.{self.base_table}"
        
        # SELECT clause
        if columns:
            select_clause = ", ".join(columns)
        else:
            select_clause = "*"
        
        # WHERE clause
        where_conditions = []
        params = []
        
        for col, value in filters.items():
            if value is not None and value != "All" and value != "":
                where_conditions.append(f"{col} = %s")
                params.append(value)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # LIMIT và OFFSET
        offset = (page - 1) * page_size
        limit_clause = f"LIMIT {page_size} OFFSET {offset}"
        
        main_measure = "total_quantity_on_hand"
        sql = f"""
            SELECT {select_clause}
            FROM {table_name}
            {where_clause}
            ORDER BY {main_measure} DESC
            {limit_clause}
        """.strip()
        
        return sql, tuple(params)


# Khởi tạo singleton instance cho Inventory Router
inventory_router = InventoryCuboidRouter()


# ==============================================================================
# INVENTORY PUBLIC API
# ==============================================================================
def route_and_query_inventory(
    columns: List[str],
    filters: Dict[str, Any],
    group_by: List[str] = None
) -> Dict[str, Any]:
    """
    Thực hiện định tuyến và truy vấn Inventory.
    
    Args:
        columns: List cột cần SELECT
        filters: Dict điều kiện filter
        group_by: List cột để GROUP BY
    
    Returns:
        Dict chứa kết quả và metadata
    """
    try:
        sql, params = inventory_router.build_inventory_query(columns, filters, group_by)
        
        results = execute_query(sql, params)
        
        all_columns = columns + (group_by or [])
        routing = inventory_router.find_optimal_cuboid(all_columns, filters)
        
        return {
            "success": True,
            "data": results,
            "metadata": {
                "selected_table": routing.selected_table,
                "reason": routing.reason,
                "dimensions_used": list(routing.dimensions),
                "row_count": len(results)
            }
        }
        
    except Exception as e:
        logger.error(f"❌ [InventoryCuboidRouter] Lỗi truy vấn: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "data": []
        }


def get_inventory_router_info() -> Dict[str, Any]:
    """Lấy thông tin về Inventory Cuboid Router."""
    stats: Dict[str, Any] = {
        "total_cuboids": len(inventory_router.catalog),
        "by_dimension_count": {},
        "cuboids": [],
    }
    for tname, cols in inventory_router.catalog.items():
        dims = inventory_router.map_columns_to_dimensions(
            [c for c in cols if c not in INVENTORY_MEASURE_COLUMNS]
        )
        dim_count = len(dims)
        stats["by_dimension_count"][dim_count] = (
            stats["by_dimension_count"].get(dim_count, 0) + 1
        )
        stats["cuboids"].append(
            {
                "table": tname,
                "dimensions": list(dims),
                "columns": sorted(cols),
                "priority": len(cols),
            }
        )
    return {
        "router_status": "active",
        "cube": "Inventory (Cube 2)",
        "dimensions": ["time", "product", "store"],
        "cuboid_stats": stats,
    }


# ==============================================================================
# TEST
# ==============================================================================
if __name__ == "__main__":
    print("🧪 [TEST] Cuboid Router Test")
    print("=" * 50)
    
    # Test 1: Tìm cuboid cho query đơn giản
    print("\n[Test 1] Query: year + sum_amount")
    result = router.find_optimal_cuboid(["year"])
    print(f"  Selected: {result.selected_table}")
    print(f"  Reason: {result.reason}")
    
    # Test 2: Tìm cuboid cho query phức tạp
    print("\n[Test 2] Query: year + product_key + customer_type")
    result = router.find_optimal_cuboid(["year", "product_key", "customer_type"])
    print(f"  Selected: {result.selected_table}")
    print(f"  Reason: {result.reason}")
    
    # Test 3: Build pivot query
    print("\n[Test 3] Build Pivot Query")
    sql, params, csql, cparams = router.build_pivot_query(
        rows=["year", "quarter"],
        columns=["customer_type"],
        measures=["sum_amount"],
        filters={"state": "California"},
    )
    print(f"  SQL:\n{sql}")
    print(f"  Params: {params}")
    
    # Test 4: Stats
    print("\n[Test 4] Cuboid Stats")
    stats = router.get_cuboid_stats()
    print(f"  Total cuboids: {stats['total_cuboids']}")
    print(f"  By dimension count: {stats['by_dimension_count']}")
