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


class CustomerRanking(BaseModel):
    """Model cho xếp hạng khách hàng."""
    customer_key: int
    customer_name: str
    customer_type: str
    sum_amount: float
    total_quantity: int
    order_count: int
    avg_order_value: float


class TopCustomersResponse(BaseModel):
    """Model cho response top customers."""
    top_5: List[CustomerRanking]


class CustomerTransactionLine(BaseModel):
    """Model cho một dòng giao dịch chi tiết của khách hàng."""
    period: str  # VD: "2024-03"
    product_name: str
    quantity_ordered: int
    total_amount: float


class PaginationInfo(BaseModel):
    """Model cho thông tin phân trang."""
    total_records: int
    total_pages: int
    current_page: int
    page_size: int


class CustomerDrillThroughResponse(BaseModel):
    """Model cho response drill-through khách hàng (gộp Hover & Modal)."""
    customer_info: Dict[str, Any]
    summary: Dict[str, Any]
    transactions: List[CustomerTransactionLine]
    pagination: PaginationInfo


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


@router.post("/top-customers", response_model=TopCustomersResponse)
async def get_top_customers(filter_data: DashboardFilter):
    """
    Lấy Top 5 khách hàng doanh thu cao nhất.
    Sử dụng bảng OLAP để tối ưu hiệu năng và JOIN với Dim_Customer để lấy tên.
    """
    try:
        # Xây dựng điều kiện filter với alias rõ ràng.
        # Lưu ý: customer_type lọc qua dim_customer (c) để tránh phụ thuộc cột customer_type trong OLAP cuboid location.
        conditions: List[str] = []
        params: List[Any] = []

        # Time filters (OLAP agg có các cột time)
        if filter_data.time.year and filter_data.time.year != "All":
            conditions.append("agg.year = %s")
            params.append(int(filter_data.time.year))
        if filter_data.time.quarter and filter_data.time.quarter != "All":
            conditions.append("agg.quarter = %s")
            params.append(int(filter_data.time.quarter))
        if filter_data.time.month and filter_data.time.month != "All":
            conditions.append("agg.month = %s")
            params.append(int(filter_data.time.month))

        # Customer location filters (state/city nằm ở OLAP location cuboid)
        cflt = filter_data.customer
        if cflt.state and cflt.state != "All":
            conditions.append("agg.state = %s")
            params.append(cflt.state)
        if cflt.city and cflt.city != "All":
            conditions.append("agg.city = %s")
            params.append(cflt.city)

        # customer_type lọc qua dimension
        if cflt.customer_type and cflt.customer_type != "All":
            conditions.append("c.customer_type = %s")
            params.append(cflt.customer_type)

        # customer_key nếu filter 1 khách cụ thể (OLAP có customer_key)
        if cflt.customer_key and cflt.customer_key != "All":
            conditions.append("agg.customer_key = %s")
            params.append(int(cflt.customer_key))

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Chọn bảng OLAP: bỏ qua customer_type khi chọn cuboid vì customer_type lọc qua dim_customer.
        filter_for_table = DashboardFilter(
            time=filter_data.time,
            customer=CustomerFilter(
                state=filter_data.customer.state,
                city=filter_data.customer.city,
                customer_type="All",
                customer_key=filter_data.customer.customer_key,
            ),
            product_key="All",  # top customers không group theo product
        )

        table_name = select_sales_olap_table_for_dashboard(
            filter_for_table, extra_columns={"customer_key"}
        )
        full_table_name = f"{OLAP_SCHEMA}.{table_name}"

        logger.info(f"[TopCustomers] Sử dụng bảng: {table_name}")

        # SQL lấy Top 5 khách hàng
        # JOIN với dwh.dim_customer để có thông tin định danh
        sql = f"""
            SELECT 
                agg.customer_key,
                c.customer_name,
                c.customer_type,
                SUM(agg.sum_amount) as sum_amount,
                SUM(agg.total_quantity) as total_quantity
            FROM {full_table_name} agg
            JOIN dwh.dim_customer c ON agg.customer_key = c.customer_key
            {where_clause}
            GROUP BY agg.customer_key, c.customer_name, c.customer_type
            ORDER BY sum_amount DESC
            LIMIT 5
        """.strip()

        # Thực thi query
        results = execute_query(sql, tuple(params))

        # Vì OLAP cuboid hiện tại có thể không có COUNT(order), 
        # chúng ta sẽ giả lập order_count = total_quantity hoặc để tạm.
        # Hoặc nếu cần chính xác, có thể query thêm từ fact_sales (nhưng tốn resource hơn).
        
        top_5 = [
            CustomerRanking(
                customer_key=int(row["customer_key"]),
                customer_name=row["customer_name"],
                customer_type=row["customer_type"],
                sum_amount=float(row["sum_amount"]),
                total_quantity=int(row["total_quantity"]),
                order_count=0,  # Sẽ cập nhật sau nếu cần thiết
                avg_order_value=float(row["sum_amount"])  # Tạm thời là tổng
            )
            for row in results
        ]

        return TopCustomersResponse(top_5=top_5)

    except Exception as e:
        logger.error(f"❌ [TopCustomers] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/customer-drill-through/{customer_key}", response_model=CustomerDrillThroughResponse)
async def get_customer_drill_through(
    customer_key: int,
    year: Optional[str] = "All",
    quarter: Optional[str] = "All",
    month: Optional[str] = "All",
    state: Optional[str] = "All",
    city: Optional[str] = "All",
    customer_type: Optional[str] = "All",
    page: int = 1,
    page_size: int = 5
):
    """
    Endpoint Drill-through duy nhất:
    - Trả về thông tin khách hàng
    - Trả về thống kê tổng quát (Summary) dựa trên filter
    - Trả về danh sách giao dịch chi tiết (Transactions) có phân trang
    """
    try:
        # 1. Lấy thông tin khách hàng từ Dim_Customer
        customer_sql = """
            SELECT 
                c.customer_key, c.customer_name, c.customer_type, c.first_order_date,
                l.city, l.state
            FROM dwh.dim_customer c
            LEFT JOIN dwh.dim_location l ON c.location_key = l.location_key
            WHERE c.customer_key = %s
        """
        cust_res = execute_query(customer_sql, (customer_key,), fetch_all=False)
        if not cust_res:
            raise HTTPException(status_code=404, detail="Không tìm thấy khách hàng")
        
        customer_info = dict(cust_res[0])
        customer_info["location"] = f"{customer_info['city']}, {customer_info['state']}"
        customer_info["first_order_date"] = str(customer_info["first_order_date"])

        # 2. Xây dựng điều kiện WHERE cho giao dịch
        conditions = ["f.customer_key = %s"]
        params = [customer_key]

        if year != "All":
            conditions.append("t.year = %s")
            params.append(int(year))
        if quarter != "All":
            conditions.append("t.quarter = %s")
            params.append(int(quarter))
        if month != "All":
            conditions.append("t.month = %s")
            params.append(int(month))
        # customer_type/state/city là thuộc tính của Customer (dim_customer -> dim_location)
        if customer_type != "All":
            conditions.append("c.customer_type = %s")
            params.append(customer_type)
        if state != "All":
            conditions.append("l.state = %s")
            params.append(state)
        # Lưu ý: l.city có thể conflict nếu join nhiều, nhưng ở đây dùng prefix rõ ràng
        if city != "All":
            conditions.append("l.city = %s")
            params.append(city)

        where_clause = "WHERE " + " AND ".join(conditions)

        # 3. Lấy tổng số bản ghi để phân trang
        count_sql = f"""
            SELECT COUNT(*) as total
            FROM dwh.fact_sales f
            JOIN dwh.dim_time t ON f.time_key = t.time_key
            JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
            LEFT JOIN dwh.dim_location l ON c.location_key = l.location_key
            {where_clause}
        """
        # Lưu ý: state/city ở đây là location của Customer (đúng theo sidebar Sales).
        
        total_res = execute_query(count_sql, tuple(params), fetch_all=False)
        total_records = int(total_res[0]["total"])
        total_pages = (total_records + page_size - 1) // page_size

        # 4. Lấy Summary Stats
        summary_sql = f"""
            SELECT 
                COALESCE(SUM(f.total_amount), 0) as total_revenue,
                COALESCE(SUM(f.quantity_ordered), 0) as total_quantity,
                COUNT(*) as order_count,
                AVG(f.total_amount) as avg_amount,
                MAX(t.year || '-' || LPAD(t.month::text, 2, '0')) as last_order
            FROM dwh.fact_sales f
            JOIN dwh.dim_time t ON f.time_key = t.time_key
            JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
            LEFT JOIN dwh.dim_location l ON c.location_key = l.location_key
            {where_clause}
        """
        summ_res = execute_query(summary_sql, tuple(params), fetch_all=False)
        summary = {
            "total_revenue": float(summ_res[0]["total_revenue"]),
            "total_quantity": int(summ_res[0]["total_quantity"]),
            "order_count": int(summ_res[0]["order_count"]),
            "avg_amount_per_order": float(summ_res[0]["avg_amount"] or 0),
            "last_order_date": summ_res[0]["last_order"] or "N/A"
        }

        # 5. Lấy danh sách giao dịch chi tiết (phân trang)
        offset = (page - 1) * page_size
        
        transactions_sql = f"""
            SELECT
                t.year || '-' || LPAD(t.month::text, 2, '0') as period,
                p.description as product_name,
                f.quantity_ordered,
                f.total_amount
            FROM dwh.fact_sales f
            JOIN dwh.dim_time t ON f.time_key = t.time_key
            JOIN dwh.dim_product p ON f.product_key = p.product_key
            JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
            LEFT JOIN dwh.dim_location l ON c.location_key = l.location_key
            {where_clause}
            ORDER BY f.time_key DESC, p.description ASC
            LIMIT %s OFFSET %s
        """
        trans_params = params + [page_size, offset]
        trans_results = execute_query(transactions_sql, tuple(trans_params))

        transactions = [
            CustomerTransactionLine(
                period=row["period"],
                product_name=row["product_name"],
                quantity_ordered=int(row["quantity_ordered"]),
                total_amount=float(row["total_amount"])
            )
            for row in trans_results
        ]

        return CustomerDrillThroughResponse(
            customer_info=customer_info,
            summary=summary,
            transactions=transactions,
            pagination=PaginationInfo(
                total_records=total_records,
                total_pages=total_pages,
                current_page=page,
                page_size=page_size
            )
        )

    except Exception as e:
        logger.error(f"❌ [CustomerDrillThrough] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
