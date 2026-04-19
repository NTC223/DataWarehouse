"""
================================================================================
FILTERS.PY - API cho khởi tạo dữ liệu Filter
================================================================================
Cung cấp các endpoint để lấy dữ liệu cho các dropdown filter:
  - Time: Danh sách năm, quý, tháng
  - Location: Danh sách state, city
  - Customer: Danh sách customer_type
  - Product: Danh sách sản phẩm

Author: Data Warehouse Team
================================================================================
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import logging

from app.database import execute_query, OLAP_SCHEMA

# Cấu hình logging
logger = logging.getLogger(__name__)

# Khởi tạo router
router = APIRouter()


# ==============================================================================
# RESPONSE MODELS
# ==============================================================================
class TimeOptions(BaseModel):
    """Model cho options thờigian."""
    years: List[int]
    quarters: List[int]
    months: List[int]


class LocationOptions(BaseModel):
    """Model cho options vị trí."""
    states: List[str]
    cities_by_state: Dict[str, List[str]]


class CustomerFilter(BaseModel):
    """Bộ lọc khách hàng (khớp `globalFilter.customer` từ FE)."""
    state: str = "All"
    city: str = "All"
    customer_type: str = "All"
    customer_key: str = "All"


class CustomerOptions(BaseModel):
    """Model cho options khách hàng."""
    customer_types: List[str]


class ProductInfo(BaseModel):
    """Model cho thông tin sản phẩm."""
    product_key: int
    product_name: str


class ProductOptions(BaseModel):
    """Model cho options sản phẩm."""
    products: List[ProductInfo]


class FilterInitResponse(BaseModel):
    """Model cho response khởi tạo filter."""
    time: TimeOptions
    location: LocationOptions
    customer: CustomerOptions
    product: ProductOptions


class CascadingFilterResponse(BaseModel):
    """Model cho response filter liên kết."""
    quarters: List[int]
    months: List[int]


# ==============================================================================
# API ENDPOINTS
# ==============================================================================
@router.get("/init", response_model=FilterInitResponse)
async def get_filter_init():
    """
    Lấy tất cả dữ liệu filter để khởi tạo dropdowns.
    
    Returns:
        Toàn bộ dữ liệu cho các filter:
        - Time: Danh sách năm, quý, tháng có trong dữ liệu
        - Location: Danh sách state và city
        - Customer: Danh sách customer_type
        - Product: Danh sách sản phẩm (key, name)
    """
    try:
        logger.info("[Filters] Đang lấy dữ liệu filter khởi tạo...")
        
        # ===== Lấy dữ liệu Time =====
        time_sql = f"""
            SELECT DISTINCT year, quarter, month
            FROM {OLAP_SCHEMA}.olap_sales_by_time
            ORDER BY year DESC, quarter, month
        """
        time_results = execute_query(time_sql)
        
        years = sorted(list(set([int(r["year"]) for r in time_results])), reverse=True)
        quarters = [1, 2, 3, 4]  # Fixed
        months = list(range(1, 13))  # Fixed
        
        # ===== Lấy dữ liệu Location =====
        location_sql = f"""
            SELECT DISTINCT state, city
            FROM {OLAP_SCHEMA}.olap_sales_by_city
            WHERE state IS NOT NULL
            ORDER BY state, city
        """
        location_results = execute_query(location_sql)
        
        states = sorted(list(set([r["state"] for r in location_results if r["state"]])))
        cities_by_state = {}
        for row in location_results:
            state = row["state"]
            city = row["city"]
            if state and city:
                if state not in cities_by_state:
                    cities_by_state[state] = []
                cities_by_state[state].append(city)
        
        # Sort cities trong mỗi state
        for state in cities_by_state:
            cities_by_state[state] = sorted(cities_by_state[state])
        
        # ===== Lấy dữ liệu Customer =====
        customer_sql = f"""
            SELECT DISTINCT customer_type
            FROM {OLAP_SCHEMA}.olap_sales_by_customer_type
            WHERE customer_type IS NOT NULL
            ORDER BY customer_type
        """
        customer_results = execute_query(customer_sql)
        customer_types = [r["customer_type"] for r in customer_results if r["customer_type"]]
        
        # ===== Lấy dữ liệu Product =====
        product_sql = f"""
            SELECT DISTINCT product_key
            FROM {OLAP_SCHEMA}.olap_sales_by_product
            WHERE product_key IS NOT NULL
            ORDER BY product_key
        """
        product_results = execute_query(product_sql)
        
        products = [
            ProductInfo(
                product_key=int(r["product_key"]),
                product_name=f"Product {r['product_key']}"  # TODO: Join với dim_product
            )
            for r in product_results
        ]
        
        # ===== Build response =====
        response = FilterInitResponse(
            time=TimeOptions(
                years=years,
                quarters=quarters,
                months=months
            ),
            location=LocationOptions(
                states=states,
                cities_by_state=cities_by_state
            ),
            customer=CustomerOptions(
                customer_types=customer_types
            ),
            product=ProductOptions(
                products=products
            )
        )
        
        logger.info(f"[Filters] Đã lấy {len(years)} năm, {len(states)} states, {len(products)} products")
        
        return response
        
    except Exception as e:
        logger.error(f"❌ [Filters] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/time/cascading")
async def get_cascading_time_filters(
    year: Optional[int] = None,
    quarter: Optional[int] = None
):
    """
    Lấy filter thờigian liên kết (cascading).
    
    Args:
        year: Năm đã chọn (optional)
        quarter: Quý đã chọn (optional)
    
    Returns:
        Danh sách quarters và months phù hợp với filter đã chọn
    
    Example:
        - year=2024 -> trả về quarters có trong 2024, months có trong 2024
        - year=2024, quarter=1 -> trả về months có trong Q1/2024
    """
    try:
        conditions = []
        params = []
        
        if year:
            conditions.append("year = %s")
            params.append(year)
        
        if quarter:
            conditions.append("quarter = %s")
            params.append(quarter)
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        sql = f"""
            SELECT DISTINCT quarter, month
            FROM {OLAP_SCHEMA}.olap_sales_by_time
            {where_clause}
            ORDER BY quarter, month
        """
        
        results = execute_query(sql, tuple(params) if params else None)
        
        quarters = sorted(list(set([int(r["quarter"]) for r in results])))
        months = sorted(list(set([int(r["month"]) for r in results])))
        
        return CascadingFilterResponse(quarters=quarters, months=months)
        
    except Exception as e:
        logger.error(f"❌ [CascadingTime] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/location/cities")
async def get_cities_by_state(state: str):
    """
    Lấy danh sách city theo state đã chọn.
    
    Args:
        state: Tên bang đã chọn
    
    Returns:
        Danh sách các thành phố thuộc bang đó
    """
    try:
        sql = f"""
            SELECT DISTINCT city
            FROM {OLAP_SCHEMA}.olap_sales_by_city
            WHERE state = %s AND city IS NOT NULL
            ORDER BY city
        """
        
        results = execute_query(sql, (state,))
        cities = [r["city"] for r in results if r["city"]]
        
        return {"state": state, "cities": cities}
        
    except Exception as e:
        logger.error(f"❌ [CitiesByState] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/products/search")
async def search_products(
    query: str,
    limit: int = 10
):
    """
    Tìm kiếm sản phẩm (autocomplete).
    
    Args:
        query: Từ khóa tìm kiếm
        limit: Số kết quả tối đa
    
    Returns:
        Danh sách sản phẩm khớp với từ khóa
    """
    try:
        # Tìm kiếm theo product_key (vì chưa có tên sản phẩm)
        sql = f"""
            SELECT DISTINCT product_key
            FROM {OLAP_SCHEMA}.olap_sales_by_product
            WHERE CAST(product_key AS TEXT) LIKE %s
            ORDER BY product_key
            LIMIT %s
        """
        
        search_pattern = f"%{query}%"
        results = execute_query(sql, (search_pattern, limit))
        
        products = [
            {
                "product_key": int(r["product_key"]),
                "product_name": f"Product {r['product_key']}"
            }
            for r in results
        ]
        
        return {"query": query, "results": products}
        
    except Exception as e:
        logger.error(f"❌ [SearchProducts] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/time/years")
async def get_years():
    """Lấy danh sách các năm có dữ liệu."""
    try:
        sql = f"""
            SELECT DISTINCT year
            FROM {OLAP_SCHEMA}.olap_sales_by_time
            ORDER BY year DESC
        """
        results = execute_query(sql)
        years = [int(r["year"]) for r in results]
        
        return {"years": years}
        
    except Exception as e:
        logger.error(f"❌ [Years] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/location/states")
async def get_states():
    """Lấy danh sách các bang."""
    try:
        sql = f"""
            SELECT DISTINCT state
            FROM {OLAP_SCHEMA}.olap_sales_by_city
            WHERE state IS NOT NULL
            ORDER BY state
        """
        results = execute_query(sql)
        states = [r["state"] for r in results if r["state"]]
        
        return {"states": states}
        
    except Exception as e:
        logger.error(f"❌ [States] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/customer/types")
async def get_customer_types():
    """Lấy danh sách các loại khách hàng."""
    try:
        sql = f"""
            SELECT DISTINCT customer_type
            FROM {OLAP_SCHEMA}.olap_sales_by_customer_type
            WHERE customer_type IS NOT NULL
            ORDER BY customer_type
        """
        results = execute_query(sql)
        customer_types = [r["customer_type"] for r in results if r["customer_type"]]
        
        return {"customer_types": customer_types}
        
    except Exception as e:
        logger.error(f"❌ [CustomerTypes] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/customers/search")
async def search_customers(
    q: str = "",
    state: Optional[str] = None,
    city: Optional[str] = None,
    customer_type: Optional[str] = None,
    limit: int = 20,
):
    """
    Autocomplete customer_key (Sales). Params state / city / customer_type lọc giao (AND).
    Giá trị 'All' hoặc rỗng = không áp dụng điều kiện đó.
    """
    try:
        limit = max(1, min(limit, 100))
        conditions = ["customer_key IS NOT NULL"]
        params: List[Any] = []

        if state and state != "All":
            conditions.append("state = %s")
            params.append(state)
        if city and city != "All":
            conditions.append("city = %s")
            params.append(city)
        if customer_type and customer_type != "All":
            conditions.append("customer_type = %s")
            params.append(customer_type)
        if q and q.strip():
            conditions.append("CAST(customer_key AS TEXT) ILIKE %s")
            params.append(f"%{q.strip()}%")

        where_sql = " AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT customer_key
            FROM {OLAP_SCHEMA}.olap_sales_by_customer_loc
            WHERE {where_sql}
            ORDER BY customer_key
            LIMIT %s
        """
        params.append(limit)
        rows = execute_query(sql, tuple(params))
        results = [{"customer_key": int(r["customer_key"])} for r in rows if r.get("customer_key") is not None]
        return {"results": results}
    except Exception as e:
        logger.error(f"❌ [SearchCustomers] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stores/search")
async def search_stores(
    q: str = "",
    state: Optional[str] = None,
    city: Optional[str] = None,
    limit: int = 20,
):
    """
    Autocomplete store_key (Inventory). Params state / city lọc giao (AND).
    """
    try:
        limit = max(1, min(limit, 100))
        conditions = ["store_key IS NOT NULL"]
        params: List[Any] = []

        if state and state != "All":
            conditions.append("state = %s")
            params.append(state)
        if city and city != "All":
            conditions.append("city = %s")
            params.append(city)
        if q and q.strip():
            conditions.append("CAST(store_key AS TEXT) ILIKE %s")
            params.append(f"%{q.strip()}%")

        where_sql = " AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT store_key
            FROM {OLAP_SCHEMA}.olap_inv_product_store
            WHERE {where_sql}
            ORDER BY store_key
            LIMIT %s
        """
        params.append(limit)
        rows = execute_query(sql, tuple(params))
        results = [{"store_key": int(r["store_key"])} for r in rows if r.get("store_key") is not None]
        return {"results": results}
    except Exception as e:
        logger.error(f"❌ [SearchStores] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/products")
async def get_products(
    page: int = 1,
    page_size: int = 100
):
    """
    Lấy danh sách sản phẩm có phân trang.
    
    Args:
        page: Số trang
        page_size: Kích thước trang
    """
    try:
        offset = (page - 1) * page_size
        
        # Đếm tổng số
        count_sql = f"""
            SELECT COUNT(DISTINCT product_key) as total
            FROM {OLAP_SCHEMA}.olap_sales_by_product
        """
        count_result = execute_query(count_sql)
        total = count_result[0]["total"]
        
        # Lấy dữ liệu
        sql = f"""
            SELECT DISTINCT product_key
            FROM {OLAP_SCHEMA}.olap_sales_by_product
            ORDER BY product_key
            LIMIT %s OFFSET %s
        """
        results = execute_query(sql, (page_size, offset))
        
        products = [
            {
                "product_key": int(r["product_key"]),
                "product_name": f"Product {r['product_key']}"
            }
            for r in results
        ]
        
        total_pages = (total + page_size - 1) // page_size
        
        return {
            "products": products,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages
            }
        }
        
    except Exception as e:
        logger.error(f"❌ [Products] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
