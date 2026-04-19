"""
================================================================================
DASHBOARD.PY - API cho Tab Dashboard (Executive View)
================================================================================
Cung cấp các endpoint cho Dashboard Sales:
  - /overview: 3 KPI cards (Tổng doanh thu, Tổng SP, Giá trị TB)
  - /trend: Biểu đồ xu hướng với drill-down/roll-up
  - /top-products: Top 5 và Bottom 5 sản phẩm
  - /customer-segment: Phân khúc khách hàng (pie chart)

Author: Data Warehouse Team
================================================================================
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Set, Tuple
from enum import Enum
import logging

from app.database import execute_query, OLAP_SCHEMA
from app.services.olap_schema_tables import SALES_OLAP_TABLE_COLUMNS
from app.services.olap_router import (
    pick_optimal_olap_table,
    _sales_customer_granule_rank,
    _sales_table_time_family,
)

# Cấu hình logging
logger = logging.getLogger(__name__)

# Khởi tạo router
router = APIRouter()


# ==============================================================================
# REQUEST/RESPONSE MODELS
# ==============================================================================
class TimeFilter(BaseModel):
    """Model cho filter thờigian."""
    year: Optional[str] = "All"      # "All" hoặc "2024"
    quarter: Optional[str] = "All"   # "All" hoặc "1", "2", "3", "4"
    month: Optional[str] = "All"     # "All" hoặc "1"-"12"


class CustomerFilter(BaseModel):
    """Filter khách hàng (địa lý + phân loại + mã KH), khớp payload FE `customer`."""
    state: Optional[str] = "All"
    city: Optional[str] = "All"
    customer_type: Optional[str] = "All"  # "All", "Tourist", "MailOrder", ...
    customer_key: Optional[str] = "All"   # "All" hoặc mã KH dạng chuỗi


class DashboardFilter(BaseModel):
    """Model cho toàn bộ filter của Dashboard."""
    time: TimeFilter = TimeFilter()
    customer: CustomerFilter = CustomerFilter()
    product_key: Optional[str] = "All"    # "All" hoặc product_key cụ thể


class TrendLevel(str, Enum):
    """Enum cho level của trend chart."""
    YEAR = "year"
    QUARTER = "quarter"
    MONTH = "month"


class TrendRequest(BaseModel):
    """Model cho request trend chart."""
    filter: DashboardFilter
    level: TrendLevel = TrendLevel.YEAR  # Level hiện tại của drill-down


class OverviewResponse(BaseModel):
    """Model cho response overview KPI."""
    total_revenue: float
    total_quantity: int
    average_price: float


class TrendData(BaseModel):
    """Model cho dữ liệu trend."""
    label: str      # VD: "2024", "Q1", "Jan"
    value: float    # Doanh thu
    quantity: int   # Số lượng


class TrendResponse(BaseModel):
    """Model cho response trend chart."""
    level: str
    data: List[TrendData]


class ProductRanking(BaseModel):
    """Model cho xếp hạng sản phẩm."""
    product_key: int
    product_name: str  # Sẽ được join từ bảng dim_product
    sum_amount: float
    total_quantity: int


class TopProductsResponse(BaseModel):
    """Model cho response top products."""
    top_5: List[ProductRanking]
    bottom_5: List[ProductRanking]


class CustomerSegment(BaseModel):
    """Model cho phân khúc khách hàng."""
    customer_type: str
    sum_amount: float
    percentage: float


class CustomerSegmentResponse(BaseModel):
    """Model cho response customer segment."""
    data: List[CustomerSegment]


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def build_filter_conditions(filter_data: DashboardFilter) -> tuple:
    """
    Xây dựng điều kiện WHERE từ filter.
    
    Returns:
        Tuple (where_clause, params, active_dimensions)
    """
    conditions = []
    params = []
    active_dimensions = set()
    
    # Time filters
    if filter_data.time.year and filter_data.time.year != "All":
        conditions.append("year = %s")
        params.append(int(filter_data.time.year))
        active_dimensions.add("time")
    
    if filter_data.time.quarter and filter_data.time.quarter != "All":
        conditions.append("quarter = %s")
        params.append(int(filter_data.time.quarter))
        active_dimensions.add("time")
    
    if filter_data.time.month and filter_data.time.month != "All":
        conditions.append("month = %s")
        params.append(int(filter_data.time.month))
        active_dimensions.add("time")
    
    c = filter_data.customer
    if c.state and c.state != "All":
        conditions.append("state = %s")
        params.append(c.state)
        active_dimensions.add("customer")

    if c.city and c.city != "All":
        conditions.append("city = %s")
        params.append(c.city)
        active_dimensions.add("customer")

    if c.customer_type and c.customer_type != "All":
        conditions.append("customer_type = %s")
        params.append(c.customer_type)
        active_dimensions.add("customer")

    if c.customer_key and c.customer_key != "All":
        conditions.append("customer_key = %s")
        params.append(int(c.customer_key))
        active_dimensions.add("customer")
    
    # Product filter
    if filter_data.product_key and filter_data.product_key != "All":
        conditions.append("product_key = %s")
        params.append(int(filter_data.product_key))
        active_dimensions.add("product")
    
    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)
    
    return where_clause, tuple(params), active_dimensions


def _dashboard_filter_needed_columns_and_flat(
    filter_data: DashboardFilter,
    *,
    include_product_if_filtered: bool = True,
) -> Tuple[Set[str], Dict[str, Any]]:
    """Cột OLAP cần có + dict filter phẳng (cho infer_time_prefix_family)."""
    need: Set[str] = set()
    flat: Dict[str, Any] = {}
    t = filter_data.time
    if t.year and t.year != "All":
        need.add("year")
        flat["year"] = int(t.year)
    if t.quarter and t.quarter != "All":
        need.add("quarter")
        flat["quarter"] = int(t.quarter)
    if t.month and t.month != "All":
        need.add("month")
        flat["month"] = int(t.month)
    c = filter_data.customer
    if c.state and c.state != "All":
        need.add("state")
        flat["state"] = c.state
    if c.city and c.city != "All":
        need.add("city")
        flat["city"] = c.city
    if c.customer_type and c.customer_type != "All":
        need.add("customer_type")
        flat["customer_type"] = c.customer_type
    if c.customer_key and c.customer_key != "All":
        need.add("customer_key")
        flat["customer_key"] = int(c.customer_key)
    if include_product_if_filtered and filter_data.product_key and filter_data.product_key != "All":
        need.add("product_key")
        flat["product_key"] = int(filter_data.product_key)
    return need, flat


def select_sales_olap_table_for_dashboard(
    filter_data: DashboardFilter,
    extra_columns: Optional[Set[str]] = None,
    *,
    include_product_if_filtered: bool = True,
) -> str:
    """
    Chọn bảng `olap_sales_*` khớp schema mới (month_/quarter_/year_) dựa trên filter + cột GROUP BY.
    """
    need, flat = _dashboard_filter_needed_columns_and_flat(
        filter_data, include_product_if_filtered=include_product_if_filtered
    )
    if extra_columns:
        need |= set(extra_columns)
    table, _ = pick_optimal_olap_table(
        SALES_OLAP_TABLE_COLUMNS,
        need,
        "olap_sales_base_loc",
        flat,
        _sales_table_time_family,
        _sales_customer_granule_rank,
    )
    return table


# ==============================================================================
# API ENDPOINTS
# ==============================================================================
@router.post("/overview", response_model=OverviewResponse)
async def get_overview(filter_data: DashboardFilter):
    """
    Lấy 3 KPI cards: Tổng doanh thu, Tổng SP, Giá trị TB.
    
    Logic:
    - Dựa vào filter để chọn bảng cuboid phù hợp
    - Tính SUM(sum_amount) và SUM(total_quantity)
    - ASP = sum_amount / total_quantity
    """
    try:
        # Xây dựng điều kiện filter
        where_clause, params, _active_dims = build_filter_conditions(filter_data)
        table_name = select_sales_olap_table_for_dashboard(filter_data)
        full_table_name = f"{OLAP_SCHEMA}.{table_name}"
        
        logger.info(f"[Overview] Sử dụng bảng: {table_name}")
        
        # Xây dựng query
        sql = f"""
            SELECT 
                COALESCE(SUM(sum_amount), 0) as total_revenue,
                COALESCE(SUM(total_quantity), 0) as total_quantity
            FROM {full_table_name}
            {where_clause}
        """.strip()
        
        # Thực thi query
        result = execute_query(sql, params, fetch_all=False)
        
        total_revenue = float(result[0]["total_revenue"])
        total_quantity = int(result[0]["total_quantity"])
        
        # Tính ASP (Average Selling Price)
        average_price = total_revenue / total_quantity if total_quantity > 0 else 0
        
        return OverviewResponse(
            total_revenue=total_revenue,
            total_quantity=total_quantity,
            average_price=round(average_price, 2)
        )
        
    except Exception as e:
        logger.error(f"❌ [Overview] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trend", response_model=TrendResponse)
async def get_trend(request: TrendRequest):
    """
    Lấy dữ liệu xu hướng cho biểu đồ đường.
    
    Logic:
    - Dựa vào level (year/quarter/month) để chọn cột GROUP BY
    - Drill-down: year -> quarter -> month
    - Roll-up: month -> quarter -> year
    """
    try:
        filter_data = request.filter
        level = request.level
        
        # Xây dựng điều kiện filter
        where_clause, params, _dims = build_filter_conditions(filter_data)
        
        # Xác định cột GROUP BY dựa vào level
        if level == TrendLevel.YEAR:
            group_col = "year"
            order_col = "year"
        elif level == TrendLevel.QUARTER:
            group_col = "quarter"
            # Nếu có filter year, group theo quarter
            if filter_data.time.year and filter_data.time.year != "All":
                order_col = "quarter"
            else:
                # Nếu không có filter year, group theo year, quarter
                group_col = "year, quarter"
                order_col = "year, quarter"
        else:  # MONTH
            group_col = "month"
            # Nếu có filter quarter, group theo month
            if filter_data.time.quarter and filter_data.time.quarter != "All":
                order_col = "month"
            elif filter_data.time.year and filter_data.time.year != "All":
                # Nếu có filter year, group theo quarter, month
                group_col = "quarter, month"
                order_col = "quarter, month"
            else:
                # Nếu không có filter, group theo year, quarter, month
                group_col = "year, quarter, month"
                order_col = "year, quarter, month"
        
        extra_cols = {c.strip() for c in group_col.replace(" ", "").split(",") if c.strip()}
        table_name = select_sales_olap_table_for_dashboard(filter_data, extra_columns=extra_cols)
        full_table_name = f"{OLAP_SCHEMA}.{table_name}"
        
        logger.info(f"[Trend] Level: {level}, Table: {table_name}")
        
        # Xây dựng query
        select_cols = group_col
        sql = f"""
            SELECT 
                {select_cols},
                SUM(sum_amount) as value,
                SUM(total_quantity) as quantity
            FROM {full_table_name}
            {where_clause}
            GROUP BY {group_col}
            ORDER BY {order_col}
        """.strip()
        
        # Thực thi query
        results = execute_query(sql, params)
        
        # Format dữ liệu trả về
        trend_data = []
        for row in results:
            if level == TrendLevel.YEAR:
                label = str(row["year"])
            elif level == TrendLevel.QUARTER:
                if "," in group_col:
                    label = f"{row['year']}-Q{row['quarter']}"
                else:
                    label = f"Q{row['quarter']}"
            else:  # MONTH
                if "," in group_col:
                    if "quarter" in group_col:
                        label = f"Q{row['quarter']}-M{row['month']}"
                    else:
                        label = f"{row['year']}-M{row['month']}"
                else:
                    label = f"M{row['month']}"
            
            trend_data.append(TrendData(
                label=label,
                value=float(row["value"]),
                quantity=int(row["quantity"])
            ))
        
        return TrendResponse(level=level.value, data=trend_data)
        
    except Exception as e:
        logger.error(f"❌ [Trend] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/top-products", response_model=TopProductsResponse)
async def get_top_products(filter_data: DashboardFilter):
    """
    Lấy Top 5 và Bottom 5 sản phẩm theo doanh thu.
    
    Logic:
    - Chỉ trả về kết quả nếu product_key = "All"
    - Roll-up theo product_key
    - Sắp xếp DESC cho top 5, ASC cho bottom 5
    """
    try:
        # Kiểm tra nếu đã filter sản phẩm cụ thể
        if filter_data.product_key and filter_data.product_key != "All":
            return TopProductsResponse(top_5=[], bottom_5=[])
        
        # Xây dựng điều kiện filter (không bao gồm product)
        conditions = []
        params = []
        
        if filter_data.time.year and filter_data.time.year != "All":
            conditions.append("year = %s")
            params.append(int(filter_data.time.year))
        
        if filter_data.time.quarter and filter_data.time.quarter != "All":
            conditions.append("quarter = %s")
            params.append(int(filter_data.time.quarter))
        
        if filter_data.time.month and filter_data.time.month != "All":
            conditions.append("month = %s")
            params.append(int(filter_data.time.month))
        
        c = filter_data.customer
        if c.state and c.state != "All":
            conditions.append("state = %s")
            params.append(c.state)

        if c.city and c.city != "All":
            conditions.append("city = %s")
            params.append(c.city)

        if c.customer_type and c.customer_type != "All":
            conditions.append("customer_type = %s")
            params.append(c.customer_type)

        if c.customer_key and c.customer_key != "All":
            conditions.append("customer_key = %s")
            params.append(int(c.customer_key))

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        table_name = select_sales_olap_table_for_dashboard(
            filter_data,
            extra_columns={"product_key"},
            include_product_if_filtered=False,
        )
        full_table_name = f"{OLAP_SCHEMA}.{table_name}"

        logger.info(f"[TopProducts] Table: {table_name}")

        # Query Top 5
        sql_top = f"""
            SELECT 
                product_key,
                SUM(sum_amount) as sum_amount,
                SUM(total_quantity) as total_quantity
            FROM {full_table_name}
            {where_clause}
            GROUP BY product_key
            ORDER BY sum_amount DESC
            LIMIT 5
        """.strip()

        # Query Bottom 5
        sql_bottom = f"""
            SELECT 
                product_key,
                SUM(sum_amount) as sum_amount,
                SUM(total_quantity) as total_quantity
            FROM {full_table_name}
            {where_clause}
            GROUP BY product_key
            ORDER BY sum_amount ASC
            LIMIT 5
        """.strip()
        
        # Thực thi queries
        top_results = execute_query(sql_top, tuple(params))
        bottom_results = execute_query(sql_bottom, tuple(params))
        
        # Format kết quả
        top_5 = [
            ProductRanking(
                product_key=int(row["product_key"]),
                product_name=f"Product {row['product_key']}",  # TODO: Join với dim_product
                sum_amount=float(row["sum_amount"]),
                total_quantity=int(row["total_quantity"])
            )
            for row in top_results
        ]

        bottom_5 = [
            ProductRanking(
                product_key=int(row["product_key"]),
                product_name=f"Product {row['product_key']}",
                sum_amount=float(row["sum_amount"]),
                total_quantity=int(row["total_quantity"])
            )
            for row in bottom_results
        ]
        
        return TopProductsResponse(top_5=top_5, bottom_5=bottom_5)
        
    except Exception as e:
        logger.error(f"❌ [TopProducts] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/customer-segment", response_model=CustomerSegmentResponse)
async def get_customer_segment(filter_data: DashboardFilter):
    """
    Lấy phân khúc khách hàng cho biểu đồ tròn.
    
    Logic:
    - Chỉ trả về nếu customer_type = "All"
    - Tính tổng doanh thu theo customer_type
    - Tính phần trăm
    """
    try:
        c = filter_data.customer
        if (c.customer_type and c.customer_type != "All") or (c.customer_key and c.customer_key != "All"):
            return CustomerSegmentResponse(data=[])
        # Cuboid OLAP không có bảng vừa state/city vừa customer_type (không join Dim)
        if (c.state and c.state != "All") or (c.city and c.city != "All"):
            logger.warning(
                "[CustomerSegment] Bỏ qua khi lọc state/city — không có bảng aggregate state×customer_type"
            )
            return CustomerSegmentResponse(data=[])

        conditions = []
        params = []

        if filter_data.time.year and filter_data.time.year != "All":
            conditions.append("year = %s")
            params.append(int(filter_data.time.year))

        if filter_data.time.quarter and filter_data.time.quarter != "All":
            conditions.append("quarter = %s")
            params.append(int(filter_data.time.quarter))

        if filter_data.time.month and filter_data.time.month != "All":
            conditions.append("month = %s")
            params.append(int(filter_data.time.month))

        if c.state and c.state != "All":
            conditions.append("state = %s")
            params.append(c.state)

        if c.city and c.city != "All":
            conditions.append("city = %s")
            params.append(c.city)

        if filter_data.product_key and filter_data.product_key != "All":
            conditions.append("product_key = %s")
            params.append(int(filter_data.product_key))
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        table_name = select_sales_olap_table_for_dashboard(
            filter_data, extra_columns={"customer_type"}
        )
        full_table_name = f"{OLAP_SCHEMA}.{table_name}"
        
        logger.info(f"[CustomerSegment] Table: {table_name}")
        
        # Query
        sql = f"""
            SELECT 
                customer_type,
                SUM(sum_amount) as sum_amount
            FROM {full_table_name}
            {where_clause}
            GROUP BY customer_type
            ORDER BY sum_amount DESC
        """.strip()

        results = execute_query(sql, tuple(params))

        total = sum(float(row["sum_amount"]) for row in results)

        segments = [
            CustomerSegment(
                customer_type=row["customer_type"],
                sum_amount=float(row["sum_amount"]),
                percentage=round(float(row["sum_amount"]) / total * 100, 2) if total > 0 else 0
            )
            for row in results
        ]
        
        return CustomerSegmentResponse(data=segments)
        
    except Exception as e:
        logger.error(f"❌ [CustomerSegment] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
