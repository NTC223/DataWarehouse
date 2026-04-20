"""
================================================================================
INVENTORY.PY - API cho luồng Inventory (Drill-Across OLAP)
================================================================================
Cung cấp các endpoint cho Inventory Dashboard:
  - /analysis/:productId: Phân tích chi tiết 1 sản phẩm (gộp Sales + Inventory)
  - /scatter-data: Scatter plot cho toàn bộ sản phẩm
  - /overview: Tổng quan tồn kho

Drill-Across: Gộp dữ liệu từ Cube 1 (Sales) và Cube 2 (Inventory)
================================================================================
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from enum import Enum
import logging

from app.database import execute_query, OLAP_SCHEMA
from app.services.olap_router import (
    route_and_query_inventory,
    get_inventory_router_info,
    inventory_router,
    pick_optimal_olap_table,
    _inv_store_granule_rank,
    _inv_table_time_family,
    _sales_customer_granule_rank,
    _sales_table_time_family,
)
from app.services.olap_schema_tables import (
    INVENTORY_OLAP_TABLE_COLUMNS,
    SALES_OLAP_TABLE_COLUMNS,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# ==============================================================================
# REQUEST/RESPONSE MODELS
# ==============================================================================
class TimeLevel(str, Enum):
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class StoreFilter(BaseModel):
    """Filter cửa hàng / địa lý tồn kho (khớp payload FE `store`)."""
    state: Optional[str] = "All"
    city: Optional[str] = "All"
    store_key: Optional[str] = "All"


class InventoryFilter(BaseModel):
    """Model cho filter Inventory (query params — tương đương StoreFilter phẳng)."""
    city: Optional[str] = None
    state: Optional[str] = None
    store_key: Optional[int] = None
    year: Optional[int] = None
    quarter: Optional[int] = None
    month: Optional[int] = None


class ProductAnalysisResponse(BaseModel):
    """Response cho phân tích sản phẩm."""
    product_key: int
    product_name: str
    time_level: str
    data: List[Dict[str, Any]]


class ScatterDataPoint(BaseModel):
    """Điểm dữ liệu cho scatter plot."""
    product_key: int
    product_name: str
    sales: float  # x-axis: total_quantity (Sales)
    inventory: float  # y-axis: total_quantity_on_hand (Inventory)
    coverage_ratio: float
    status: str  # "optimal" | "overstock" | "understock"


class ScatterDataResponse(BaseModel):
    """Response cho scatter plot."""
    city: Optional[str]
    data: List[ScatterDataPoint]
    summary: Dict[str, int]


class InventoryOverviewResponse(BaseModel):
    """Response cho overview tồn kho."""
    total_inventory: int
    total_stores: int
    total_products: int
    avg_inventory_per_store: float


class CityRiskRankingPoint(BaseModel):
    """Điểm dữ liệu cho city risk ranking."""
    city: str
    state: Optional[str] = None
    overstock_count: int  # Số sản phẩm Đọng vốn trong city này
    overstock_score: float  # Tổng overstock score = sum((inventory / sales) với overstock items)
    understock_count: int  # Số sản phẩm Đứt gãy
    understock_score: float  # Tổng understock score = sum((inventory / sales) với understock items)
    avg_inventory: float
    total_sales: float


class CitiesRiskRankingResponse(BaseModel):
    """Response cho city risk ranking."""
    overstock_cities: List[CityRiskRankingPoint]  # Top N cities by overstock
    understock_cities: List[CityRiskRankingPoint]  # Top N cities by understock
    total_cities: int


# ==============================================================================
# HELPER FUNCTIONS - Query Cube Sales và Inventory
# ==============================================================================
def query_cube_sales(
    product_key: int = None,
    city: str = None,
    state: str = None,
    year: int = None,
    quarter: int = None,
    month: int = None,
    group_by_time: str = "month"
) -> List[Dict]:
    """
    Query Cube 1 (Sales) để lấy total_quantity và sum_amount.
    
    Args:
        product_key: ID sản phẩm (optional)
        city: Thành phố (optional)
        state: Bang (optional)
        year, quarter, month: Filter thờigian
        group_by_time: 'month', 'quarter', hoặc 'year'
    
    Returns:
        List các bản ghi với time_key, total_quantity, sum_amount
    """
    need = set()
    flat: Dict[str, Any] = {}
    if product_key is not None:
        need.add("product_key")
        flat["product_key"] = product_key
    if city:
        need.add("city")
        flat["city"] = city
    if state:
        need.add("state")
        flat["state"] = state
    if year is not None:
        need.add("year")
        flat["year"] = year
    if quarter is not None:
        need.add("quarter")
        flat["quarter"] = quarter
    if month is not None:
        need.add("month")
        flat["month"] = month
    if group_by_time == "month":
        need.update({"year", "quarter", "month"})
    elif group_by_time == "quarter":
        need.update({"year", "quarter"})
    else:
        need.add("year")
    table, _ = pick_optimal_olap_table(
        SALES_OLAP_TABLE_COLUMNS,
        need,
        "olap_sales_base_loc",
        flat,
        _sales_table_time_family,
        _sales_customer_granule_rank,
    )
    
    # Xây dựng SELECT và GROUP BY
    if group_by_time == "month":
        time_select = "year, quarter, month"
        time_group = "year, quarter, month"
    elif group_by_time == "quarter":
        time_select = "year, quarter"
        time_group = "year, quarter"
    else:  # year
        time_select = "year"
        time_group = "year"
    
    # WHERE conditions
    conditions = []
    params = []
    
    if product_key:
        conditions.append("product_key = %s")
        params.append(product_key)
    if city:
        conditions.append("city = %s")
        params.append(city)
    if state:
        conditions.append("state = %s")
        params.append(state)
    if year:
        conditions.append("year = %s")
        params.append(year)
    if quarter:
        conditions.append("quarter = %s")
        params.append(quarter)
    if month:
        conditions.append("month = %s")
        params.append(month)
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    sql = f"""
        SELECT 
            {time_select},
            SUM(total_quantity) as total_quantity,
            SUM(sum_amount) as sum_amount
        FROM {OLAP_SCHEMA}.{table}
        {where_clause}
        GROUP BY {time_group}
        ORDER BY {time_group}
    """.strip()
    
    return execute_query(sql, tuple(params) if params else None)


def query_cube_inventory(
    product_key: int = None,
    city: str = None,
    state: str = None,
    store_key: int = None,
    year: int = None,
    quarter: int = None,
    month: int = None,
    group_by_time: str = "month"
) -> List[Dict]:
    """
    Query Cube 2 (Inventory) để lấy total_quantity_on_hand.
    
    Hỗ trợ: Time, Product, Store (state/city trên bảng cuboid, không còn cube «location» riêng)
    
    Args:
        product_key: ID sản phẩm (optional)
        city: Thành phố (optional)
        state: Bang/Tỉnh (optional)
        store_key: ID cửa hàng (optional)
        year, quarter, month: Filter thời gian
        group_by_time: 'month', 'quarter', hoặc 'year'
    
    Returns:
        List các bản ghi với time_key, total_quantity_on_hand
    """
    need = set()
    flat: Dict[str, Any] = {}
    if product_key is not None:
        need.add("product_key")
        flat["product_key"] = product_key
    if store_key is not None:
        need.add("store_key")
        flat["store_key"] = store_key
    if state:
        need.add("state")
        flat["state"] = state
    if city:
        need.add("city")
        flat["city"] = city
    if year is not None:
        need.add("year")
        flat["year"] = year
    if quarter is not None:
        need.add("quarter")
        flat["quarter"] = quarter
    if month is not None:
        need.add("month")
        flat["month"] = month
    if group_by_time == "month":
        need.update({"year", "quarter", "month"})
    elif group_by_time == "quarter":
        need.update({"year", "quarter"})
    else:
        need.add("year")
    table, _ = pick_optimal_olap_table(
        INVENTORY_OLAP_TABLE_COLUMNS,
        need,
        "olap_inv_base",
        flat,
        _inv_table_time_family,
        _inv_store_granule_rank,
    )
    
    # Xây dựng SELECT và GROUP BY
    if group_by_time == "month":
        time_select = "year, quarter, month"
        time_group = "year, quarter, month"
    elif group_by_time == "quarter":
        time_select = "year, quarter"
        time_group = "year, quarter"
    else:  # year
        time_select = "year"
        time_group = "year"
    
    # WHERE conditions
    conditions = []
    params = []
    
    if product_key:
        conditions.append("product_key = %s")
        params.append(product_key)
    if store_key:
        conditions.append("store_key = %s")
        params.append(store_key)
    if state:
        conditions.append("state = %s")
        params.append(state)
    if city:
        conditions.append("city = %s")
        params.append(city)
    if year:
        conditions.append("year = %s")
        params.append(year)
    if quarter:
        conditions.append("quarter = %s")
        params.append(quarter)
    if month:
        conditions.append("month = %s")
        params.append(month)
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    sql = f"""
        SELECT 
            {time_select},
            SUM(total_quantity_on_hand) as total_quantity_on_hand
        FROM {OLAP_SCHEMA}.{table}
        {where_clause}
        GROUP BY {time_group}
        ORDER BY {time_group}
    """.strip()
    
    return execute_query(sql, tuple(params) if params else None)


def merge_sales_inventory(
    sales_data: List[Dict],
    inventory_data: List[Dict],
    group_by_time: str
) -> List[Dict]:
    """
    Gộp dữ liệu Sales và Inventory theo time_key.
    Sử dụng Full Outer Join logic.

    Coverage Ratio (Độ đáp ứng) - Forward-looking:
        coverage_ratio[T] = total_quantity_on_hand[T] / total_quantity[T+1]
    
    - Nếu T là kỳ cuối cùng (không có T+1): coverage_ratio = None
    - Nếu total_quantity[T+1] == 0 hoặc None: coverage_ratio = None

    Args:
        sales_data: Dữ liệu từ Cube Sales
        inventory_data: Dữ liệu từ Cube Inventory
        group_by_time: 'month', 'quarter', hoặc 'year'

    Returns:
        List các bản ghi đã gộp, sắp xếp tăng dần theo thời gian
    """
    def make_time_key(row: Dict) -> str:
        if group_by_time == "month":
            return f"{row['year']}-{row['quarter']}-{row['month']}"
        elif group_by_time == "quarter":
            return f"{row['year']}-{row['quarter']}"
        return str(row['year'])

    # Tạo dictionary cho lookup
    sales_dict = {make_time_key(row): row for row in sales_data}
    inventory_dict = {make_time_key(row): row for row in inventory_data}

    # Lấy tất cả keys và sắp xếp tăng dần theo thời gian
    all_keys = sorted(set(sales_dict.keys()) | set(inventory_dict.keys()))

    # Bước 1: Gộp dữ liệu thô vào mảng trung gian
    merged = []
    for key in all_keys:
        sales = sales_dict.get(key, {})
        inventory = inventory_dict.get(key, {})

        total_qty = float(sales.get('total_quantity', 0) or 0)
        qty_on_hand = float(inventory.get('total_quantity_on_hand', 0) or 0)

        merged.append({
            'time_key': key,
            'year': sales.get('year') or inventory.get('year'),
            'quarter': sales.get('quarter') or inventory.get('quarter'),
            'month': sales.get('month') or inventory.get('month'),
            'total_quantity': total_qty,
            'sum_amount': float(sales.get('sum_amount', 0) or 0),
            'total_quantity_on_hand': qty_on_hand,
            'coverage_ratio': None  # Sẽ được tính ở bước 2
        })

    # Bước 2: Tính coverage_ratio và Dịch chuyển Sales hiển thị sang T+1 (Forward-looking)
    for i in range(len(merged)):
        qty_on_hand_current = merged[i]['total_quantity_on_hand']

        # Quy định: Nếu không có kỳ tiếp theo → gán 0 (tránh nhầm lẫn với kỳ hiện tại T)
        if i + 1 >= len(merged):
            merged[i]['coverage_ratio'] = 0.0
            merged[i]['total_quantity'] = 0.0
            merged[i]['sum_amount'] = 0.0
            continue

        qty_ordered_next = merged[i + 1]['total_quantity']
        sum_amount_next = merged[i + 1]['sum_amount']

        # Nếu kỳ tiếp theo không có doanh số (0 hoặc None) → tránh chia 0
        if not qty_ordered_next:
            merged[i]['coverage_ratio'] = 0.0
        else:
            merged[i]['coverage_ratio'] = round(qty_on_hand_current / qty_ordered_next, 2)
            
        # Cập nhật giá trị Sales của bản ghi hiện tại thành giá trị của kỳ sau (T+1)
        # Điều này giúp thanh biểu đồ "Sales kỳ sau" khớp với phép tính và nhãn
        merged[i]['total_quantity'] = qty_ordered_next
        merged[i]['sum_amount'] = sum_amount_next

    return merged



# ==============================================================================
# API ENDPOINTS
# ==============================================================================
@router.get("/overview", response_model=InventoryOverviewResponse)
async def get_inventory_overview(
    city: Optional[str] = None,
    state: Optional[str] = None,
    store_key: Optional[int] = None,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    month: Optional[int] = None
):
    """
    Lấy tổng quan tồn kho.
    Hỗ trợ filter theo location (city, state).
    """
    try:
        # Xây dựng WHERE conditions
        conditions = []
        params = []
        
        if city:
            conditions.append("city = %s")
            params.append(city)
        if state:
            conditions.append("state = %s")
            params.append(state)
        if store_key is not None:
            conditions.append("store_key = %s")
            params.append(store_key)
        if year:
            conditions.append("year = %s")
            params.append(year)
        if quarter:
            conditions.append("quarter = %s")
            params.append(quarter)
        if month:
            conditions.append("month = %s")
            params.append(month)
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        has_filters = bool(conditions)
        # Có filter: dùng base (đủ time × product × store × state/city); không filter: cuboid tối giản
        if has_filters:
            source_table = "olap_inv_base"
            store_source = "olap_inv_base"
            product_source = "olap_inv_base"
        else:
            source_table = "olap_inv_all"
            store_source = "olap_inv_by_store"
            product_source = "olap_inv_by_product"
        
        # Tổng tồn kho
        sql_total = f"""
            SELECT SUM(total_quantity_on_hand) as total
            FROM {OLAP_SCHEMA}.{source_table}
            {where_clause}
        """
        total_result = execute_query(sql_total, tuple(params) if params else None)
        total_inventory = int(total_result[0]['total'] or 0)
        
        # Số lượng stores
        sql_stores = f"""
            SELECT COUNT(DISTINCT store_key) as count
            FROM {OLAP_SCHEMA}.{store_source}
            {where_clause}
        """
        stores_result = execute_query(sql_stores, tuple(params) if params else None)
        total_stores = int(stores_result[0]['count'] or 0)
        
        # Số lượng products
        sql_products = f"""
            SELECT COUNT(DISTINCT product_key) as count
            FROM {OLAP_SCHEMA}.{product_source}
            {where_clause}
        """
        products_result = execute_query(sql_products, tuple(params) if params else None)
        total_products = int(products_result[0]['count'] or 0)
        
        avg_inventory = total_inventory / total_stores if total_stores > 0 else 0
        
        return InventoryOverviewResponse(
            total_inventory=total_inventory,
            total_stores=total_stores,
            total_products=total_products,
            avg_inventory_per_store=round(avg_inventory, 2)
        )
        
    except Exception as e:
        logger.error(f"❌ [Inventory Overview] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/{product_id}", response_model=ProductAnalysisResponse)
async def get_product_analysis(
    product_id: int,
    city: Optional[str] = None,
    state: Optional[str] = None,
    store_key: Optional[int] = None,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    month: Optional[int] = None,
    time_level: TimeLevel = TimeLevel.MONTH
):
    """
    Phân tích chi tiết 1 sản phẩm - Drill-Across Sales và Inventory.
    """
    try:
        logger.info(f"[Inventory Analysis] Product: {product_id}, City: {city}, Time: {time_level.value}")
        
        # Xác định tham số lọc cho Sales - LUÔN DÙNG T+1 (Kỳ sau)
        sales_year = year
        sales_quarter = quarter
        sales_month = month
        
        if time_level == TimeLevel.MONTH and month and year:
            sales_month = month + 1
            if sales_month > 12:
                sales_month = 1
                sales_year = year + 1
            sales_quarter = (sales_month - 1) // 3 + 1
            logger.info(f"[Inventory Analysis] Shifted Month for T+1: {sales_year}-M{sales_month}")
        elif time_level == TimeLevel.QUARTER and quarter and year:
            sales_quarter = quarter + 1
            if sales_quarter > 4:
                sales_quarter = 1
                sales_year = year + 1
            logger.info(f"[Inventory Analysis] Shifted Quarter for T+1: {sales_year}-Q{sales_quarter}")
        elif time_level == TimeLevel.YEAR and year:
            sales_year = year + 1
            logger.info(f"[Inventory Analysis] Shifted Year for T+1: {sales_year}")

        # Query song song 2 Cube
        sales_data = query_cube_sales(
            product_key=product_id,
            city=city,
            state=state,
            year=sales_year,
            quarter=sales_quarter,
            month=sales_month,
            group_by_time=time_level.value
        )
        
        inventory_data = query_cube_inventory(
            product_key=product_id,
            city=city,
            state=state,
            store_key=store_key,
            year=year,
            quarter=quarter,
            month=month,
            group_by_time=time_level.value
        )
        
        # Gộp dữ liệu cho trường hợp lọc một kỳ cụ thể (Month/Quarter/Year)
        # Ta cần hiển thị Inventory của kỳ [T] và Sales của kỳ [T+1] trên cùng một nhãn thời gian của kỳ [T]
        is_single_filter = (
            (time_level == TimeLevel.MONTH and month and year) or
            (time_level == TimeLevel.QUARTER and quarter and year) or
            (time_level == TimeLevel.YEAR and year)
        )

        if is_single_filter:
            inv_row = inventory_data[0] if inventory_data else None
            sales_row = sales_data[0] if sales_data else None
            
            # Tạo nhãn thời gian cho kỳ [T]
            if time_level == TimeLevel.MONTH:
                time_key = f"{year}-{quarter}-{month}"
            elif time_level == TimeLevel.QUARTER:
                time_key = f"{year}-{quarter}"
            else:
                time_key = str(year)
                
            merged_data = [{
                'time_key': time_key,
                'year': year,
                'quarter': quarter,
                'month': month,
                'total_quantity': float(sales_row['total_quantity'] if sales_row else 0),
                'sum_amount': float(sales_row['sum_amount'] if sales_row else 0),
                'total_quantity_on_hand': float(inv_row['total_quantity_on_hand'] if inv_row else 0),
                'coverage_ratio': 0.0
            }]
            
            # Tính ratio: Inventory[T] / Sales[T+1]
            q_inv = merged_data[0]['total_quantity_on_hand']
            q_sales_next = merged_data[0]['total_quantity']
            if q_sales_next > 0:
                merged_data[0]['coverage_ratio'] = round(q_inv / q_sales_next, 2)
        else:
            # Trường hợp xem trend (không filter tháng cụ thể hoặc xem theo Quý/Năm)
            merged_data = merge_sales_inventory(sales_data, inventory_data, time_level.value)
        
        return ProductAnalysisResponse(
            product_key=product_id,
            product_name=f"Product {product_id}",
            time_level=time_level.value,
            data=merged_data
        )
        
    except Exception as e:
        logger.error(f"❌ [Inventory Analysis] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scatter-data", response_model=ScatterDataResponse)
async def get_scatter_data(
    city: Optional[str] = Query(None, description="Lọc theo thành phố"),
    state: Optional[str] = Query(None, description="Lọc theo bang"),
    store_key: Optional[int] = Query(None, description="Lọc theo cửa hàng"),
    year: Optional[int] = Query(None, description="Lọc theo năm"),
    quarter: Optional[int] = Query(None, description="Lọc theo quý"),
    month: Optional[int] = Query(None, description="Lọc theo tháng")
):
    """
    Lấy dữ liệu cho Scatter Plot - phân tích rủi ro tồn kho.
    
    Trả về mảng các điểm với:
    - x: Sales (total_quantity)
    - y: Inventory (total_quantity_on_hand)
    - status: optimal | overstock | understock (dựa trên đường chéo y=x)
    """
    try:
        logger.info(f"[Scatter Data] City: {city}, State: {state}")
        
        need_sales = {"product_key"}
        flat_sales: Dict[str, Any] = {}
        if city:
            need_sales.add("city")
            flat_sales["city"] = city
        if state:
            need_sales.add("state")
            flat_sales["state"] = state
        if year is not None:
            need_sales.add("year")
            flat_sales["year"] = year
        if quarter is not None:
            need_sales.add("quarter")
            flat_sales["quarter"] = quarter
        if month is not None:
            need_sales.add("month")
            flat_sales["month"] = month
        sales_table, _ = pick_optimal_olap_table(
            SALES_OLAP_TABLE_COLUMNS,
            need_sales,
            "olap_sales_base_loc",
            flat_sales,
            _sales_table_time_family,
            _sales_customer_granule_rank,
        )

        need_inv = {"product_key"}
        flat_inv: Dict[str, Any] = {}
        if city:
            need_inv.add("city")
            flat_inv["city"] = city
        if state:
            need_inv.add("state")
            flat_inv["state"] = state
        if store_key is not None:
            need_inv.add("store_key")
            flat_inv["store_key"] = store_key
        if year is not None:
            need_inv.add("year")
            flat_inv["year"] = year
        if quarter is not None:
            need_inv.add("quarter")
            flat_inv["quarter"] = quarter
        if month is not None:
            need_inv.add("month")
            flat_inv["month"] = month
        inv_table, _ = pick_optimal_olap_table(
            INVENTORY_OLAP_TABLE_COLUMNS,
            need_inv,
            "olap_inv_base",
            flat_inv,
            _inv_table_time_family,
            _inv_store_granule_rank,
        )

        # Xây dựng target period cho Sales (Dự báo T+1 cho tất cả các cấp)
        target_sales_year = year
        target_sales_quarter = quarter
        target_sales_month = month
        is_forward_looking = False
        
        # Nếu người dùng có filter thời gian cụ thể, ta mới dịch chuyển target_sales
        if time_level := (TimeLevel.MONTH if month else TimeLevel.QUARTER if quarter else TimeLevel.YEAR if year else None):
            is_forward_looking = True
            if time_level == TimeLevel.MONTH:
                target_sales_month = month + 1
                if target_sales_month > 12:
                    target_sales_month = 1
                    target_sales_year = year + 1
            elif time_level == TimeLevel.QUARTER:
                target_sales_quarter = quarter + 1
                if target_sales_quarter > 4:
                    target_sales_quarter = 1
                    target_sales_year = year + 1
            elif time_level == TimeLevel.YEAR:
                target_sales_year = year + 1
            
            logger.info(f"[Scatter Data] Forward-looking {time_level.value}: Sales Target Period {target_sales_year}-{target_sales_quarter or ''}-{target_sales_month or ''}")
        
        # Query Sales
        sales_conditions = []
        sales_params = []
        if city:
            sales_conditions.append("city = %s")
            sales_params.append(city)
        if state:
            sales_conditions.append("state = %s")
            sales_params.append(state)
        
        if is_forward_looking:
            if year: # Nếu có filter year
                sales_conditions.append("year = %s")
                sales_params.append(target_sales_year)
            if quarter: # Nếu có filter quarter
                sales_conditions.append("quarter = %s")
                sales_params.append(target_sales_quarter)
            if month: # Nếu có filter month
                sales_conditions.append("month = %s")
                sales_params.append(target_sales_month)
        else:
            if year:
                sales_conditions.append("year = %s")
                sales_params.append(year)
            if quarter:
                sales_conditions.append("quarter = %s")
                sales_params.append(quarter)
            if month:
                sales_conditions.append("month = %s")
                sales_params.append(month)
        
        sales_where = "WHERE " + " AND ".join(sales_conditions) if sales_conditions else ""
        
        # Query Sales
        sql_sales = f"""
            SELECT 
                product_key,
                SUM(total_quantity) as total_quantity,
                SUM(sum_amount) as sum_amount
            FROM {OLAP_SCHEMA}.{sales_table}
            {sales_where}
            GROUP BY product_key
        """.strip()
        
        sales_results = execute_query(sql_sales, tuple(sales_params) if sales_params else None)
        
        # Query Inventory (Luôn là kỳ hiện tại)
        inv_conditions = []
        inv_params = []
        
        if city:
            inv_conditions.append("city = %s")
            inv_params.append(city)
        if state:
            inv_conditions.append("state = %s")
            inv_params.append(state)
        if store_key is not None:
            inv_conditions.append("store_key = %s")
            inv_params.append(store_key)
        if year:
            inv_conditions.append("year = %s")
            inv_params.append(year)
        if quarter:
            inv_conditions.append("quarter = %s")
            inv_params.append(quarter)
        if month:
            inv_conditions.append("month = %s")
            inv_params.append(month)
        
        inv_where = "WHERE " + " AND ".join(inv_conditions) if inv_conditions else ""
        
        sql_inventory = f"""
            SELECT 
                product_key,
                SUM(total_quantity_on_hand) as total_quantity_on_hand
            FROM {OLAP_SCHEMA}.{inv_table}
            {inv_where}
            GROUP BY product_key
        """.strip()
        
        inventory_results = execute_query(sql_inventory, tuple(inv_params) if inv_params else None)
        
        # Tạo dictionary cho lookup và lấy tất cả product_keys
        sales_dict = {row['product_key']: float(row['total_quantity'] or 0) for row in sales_results}
        inventory_dict = {row['product_key']: float(row['total_quantity_on_hand'] or 0) for row in inventory_results}
        
        all_product_keys = set(sales_dict.keys()) | set(inventory_dict.keys())
        
        # Xây dựng scatter data
        scatter_data = []
        status_count = {"optimal": 0, "overstock": 0, "understock": 0}
        
        for product_key in all_product_keys:
            sales_qty = sales_dict.get(product_key, 0)
            inv_qty = inventory_dict.get(product_key, 0)
            
            # Tính coverage ratio và status
            if sales_qty > 0:
                coverage_ratio = inv_qty / sales_qty
            else:
                coverage_ratio = 999 if inv_qty > 0 else 0
            
            # Phân loại status dựa trên đường chéo y=x
            if inv_qty > sales_qty * 1.2:
                status = "overstock"
            elif inv_qty < sales_qty * 0.8:
                status = "understock"
            else:
                status = "optimal"
            
            status_count[status] += 1
            
            scatter_data.append(ScatterDataPoint(
                product_key=product_key,
                product_name=f"Product {product_key}",
                sales=sales_qty,
                inventory=inv_qty,
                coverage_ratio=round(coverage_ratio, 2),
                status=status
            ))
        
        return ScatterDataResponse(
            city=city,
            data=scatter_data,
            summary=status_count
        )
        
    except Exception as e:
        logger.error(f"❌ [Scatter Data] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/router-info")
async def get_inventory_cuboid_info():
    """
    Lấy thông tin về Inventory Cuboid Router.
    """
    return get_inventory_router_info()


@router.get("/products")
async def get_inventory_products(
    page: int = 1,
    page_size: int = 50
):
    """
    Lấy danh sách sản phẩm có trong Inventory.
    """
    try:
        offset = (page - 1) * page_size
        
        sql = f"""
            SELECT 
                product_key,
                SUM(total_quantity_on_hand) as total_inventory
            FROM {OLAP_SCHEMA}.olap_inv_by_product
            GROUP BY product_key
            ORDER BY product_key
            LIMIT %s OFFSET %s
        """
        
        results = execute_query(sql, (page_size, offset))
        
        return {
            "products": [
                {
                    "product_key": row["product_key"],
                    "product_name": f"Product {row['product_key']}",
                    "total_inventory": int(row["total_inventory"] or 0)
                }
                for row in results
            ],
            "page": page,
            "page_size": page_size
        }
        
    except Exception as e:
        logger.error(f"❌ [Inventory Products] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cities-risk-ranking", response_model=CitiesRiskRankingResponse)
async def get_cities_risk_ranking(
    year: Optional[int] = Query(None, description="Lọc theo năm"),
    quarter: Optional[int] = Query(None, description="Lọc theo quý"),
    month: Optional[int] = Query(None, description="Lọc theo tháng"),
    limit: int = Query(3, description="Số cities để trả về cho mỗi category")
):
    """
    Lấy top cities theo rủi ro (overstock/understock).
    Trả về top N cities có Đọng vốn nhất và top N cities có Đứt gãy nhất.
    """
    try:
        logger.info(f"[Cities Risk Ranking] Year: {year}, Quarter: {quarter}, Month: {month}")
        
        need_city = {"city", "state", "product_key"}
        flat_ct: Dict[str, Any] = {}
        if year is not None:
            need_city.add("year")
            flat_ct["year"] = year
        if quarter is not None:
            need_city.add("quarter")
            flat_ct["quarter"] = quarter
        if month is not None:
            need_city.add("month")
            flat_ct["month"] = month
        sales_table, _ = pick_optimal_olap_table(
            SALES_OLAP_TABLE_COLUMNS,
            need_city,
            "olap_sales_base_loc",
            flat_ct,
            _sales_table_time_family,
            _sales_customer_granule_rank,
        )
        inv_table, _ = pick_optimal_olap_table(
            INVENTORY_OLAP_TABLE_COLUMNS,
            need_city,
            "olap_inv_base",
            flat_ct,
            _inv_table_time_family,
            _inv_store_granule_rank,
        )
        
        # Xây dựng WHERE conditions
        conditions = []
        params = []
        
        if year:
            conditions.append("year = %s")
            params.append(year)
        if quarter:
            conditions.append("quarter = %s")
            params.append(quarter)
        if month:
            conditions.append("month = %s")
            params.append(month)
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        # Query Sales data by City + Product
        sql_sales = f"""
            SELECT 
                city,
                state,
                product_key,
                SUM(total_quantity) as total_quantity,
                SUM(sum_amount) as sum_amount
            FROM {OLAP_SCHEMA}.{sales_table}
            {where_clause}
            GROUP BY city, state, product_key
        """.strip()
        
        sales_results = execute_query(sql_sales, tuple(params) if params else None)
        
        # Query Inventory data by City + Product
        sql_inventory = f"""
            SELECT 
                city,
                state,
                product_key,
                SUM(total_quantity_on_hand) as total_quantity_on_hand
            FROM {OLAP_SCHEMA}.{inv_table}
            {where_clause}
            GROUP BY city, state, product_key
        """.strip()
        
        inventory_results = execute_query(sql_inventory, tuple(params) if params else None)
        
        # Aggregate by city
        # Structure: { "city_name": { "city": str, "state": str, "products": [{ "product_key", "sales", "inventory" }] } }
        city_data = {}
        
        # Populate sales data
        for row in sales_results:
            city_key = row['city']
            if city_key not in city_data:
                city_data[city_key] = {
                    "city": row['city'],
                    "state": row.get('state'),
                    "products": {}
                }
            
            product_key = row['product_key']
            if product_key not in city_data[city_key]["products"]:
                city_data[city_key]["products"][product_key] = {
                    "product_key": product_key,
                    "sales": 0,
                    "inventory": 0
                }
            
            city_data[city_key]["products"][product_key]["sales"] = float(row['total_quantity'] or 0)
        
        # Populate inventory data
        for row in inventory_results:
            city_key = row['city']
            if city_key not in city_data:
                city_data[city_key] = {
                    "city": row['city'],
                    "state": row.get('state'),
                    "products": {}
                }
            
            product_key = row['product_key']
            if product_key not in city_data[city_key]["products"]:
                city_data[city_key]["products"][product_key] = {
                    "product_key": product_key,
                    "sales": 0,
                    "inventory": 0
                }
            
            city_data[city_key]["products"][product_key]["inventory"] = float(row['total_quantity_on_hand'] or 0)
        
        # Tính toán overstock/understock scores cho mỗi city
        city_scores = []
        
        for city_key, city_info in city_data.items():
            overstock_count = 0
            overstock_sum = 0
            understock_count = 0
            understock_sum = 0
            total_inventory = 0
            total_sales = 0
            
            for product_key, prod_info in city_info["products"].items():
                sales = prod_info["sales"]
                inventory = prod_info["inventory"]
                
                total_inventory += inventory
                total_sales += sales
                
                # Phân loại status dựa trên đường chéo y=x
                if inventory > sales * 1.2:  # Overstock
                    overstock_count += 1
                    # Score = coverage ratio (inventory / sales)
                    if sales > 0:
                        overstock_sum += inventory / sales
                    else:
                        overstock_sum += inventory / 1 if inventory > 0 else 0
                
                elif inventory < sales * 0.8:  # Understock
                    understock_count += 1
                    # Score = inverse coverage (sales / inventory) để cao càng tốt
                    if inventory > 0:
                        understock_sum += sales / inventory
                    else:
                        understock_sum += sales / 1 if sales > 0 else 0
            
            # Tính trung bình (không tính nếu count = 0)
            overstock_score = round(overstock_sum / overstock_count, 2) if overstock_count > 0 else 0
            understock_score = round(understock_sum / understock_count, 2) if understock_count > 0 else 0
            
            avg_inventory = total_inventory / len(city_info["products"]) if city_info["products"] else 0
            
            city_scores.append(CityRiskRankingPoint(
                city=city_info["city"],
                state=city_info["state"],
                overstock_count=overstock_count,
                overstock_score=overstock_score,
                understock_count=understock_count,
                understock_score=understock_score,
                avg_inventory=round(avg_inventory, 2),
                total_sales=round(total_sales, 2)
            ))
        
        # Sort và lấy top N
        overstock_cities = sorted(
            [c for c in city_scores if c.overstock_count > 0],
            key=lambda x: x.overstock_score,
            reverse=True
        )[:limit]
        
        understock_cities = sorted(
            [c for c in city_scores if c.understock_count > 0],
            key=lambda x: x.understock_score,
            reverse=True
        )[:limit]
        
        return CitiesRiskRankingResponse(
            overstock_cities=overstock_cities,
            understock_cities=understock_cities,
            total_cities=len(city_scores)
        )
        
    except Exception as e:
        logger.error(f"❌ [Cities Risk Ranking] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stores")
async def get_inventory_stores():
    """
    Lấy danh sách cửa hàng.
    """
    try:
        sql = f"""
            SELECT 
                store_key,
                SUM(total_quantity_on_hand) as total_inventory
            FROM {OLAP_SCHEMA}.olap_inv_by_store
            GROUP BY store_key
            ORDER BY store_key
        """
        
        results = execute_query(sql)
        
        return {
            "stores": [
                {
                    "store_key": row["store_key"],
                    "store_name": f"Store {row['store_key']}",
                    "total_inventory": int(row["total_inventory"] or 0)
                }
                for row in results
            ]
        }
        
    except Exception as e:
        logger.error(f"❌ [Inventory Stores] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
