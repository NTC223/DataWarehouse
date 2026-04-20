"""
================================================================================
OLAP_CUBES.PY - Tiêu chuẩn API OLAP cho Cubes (Sales & Inventory)
================================================================================
Cung cấp các endpoint OLAP chuẩn:
  - GET /api/cubes: Danh sách cubes với metadata
  - GET /api/cubes/{cube}/dimensions: Chi tiết cấu trúc chiều
  - POST /api/cubes/{cube}/query: Truy vấn OLAP chính (sử dụng /api/olap/explore)
  - POST /api/cubes/{cube}/drill-through: Truy vấn Fact table
  - POST /api/cubes/drill-across: Kết hợp dữ liệu từ 2 cubes
  - GET /api/metadata/dim-values: Danh sách giá trị chiều (Filter Panel)
  - GET /api/health: Kiểm tra trạng thái database

Author: Data Warehouse Team
================================================================================
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import logging
from enum import Enum

from app.database import execute_query, execute_query_paginated, test_connection, OLAP_SCHEMA
from app.services.olap_router import route_and_query_pivot, route_and_query_raw

# Cấu hình logging
logger = logging.getLogger(__name__)

# Khởi tạo router
router = APIRouter()


# ==============================================================================
# ENUMS & CONSTANTS
# ==============================================================================
class CubeEnum(str, Enum):
    """Các cube khả dụng trong hệ thống."""
    SALES = "Sales"
    INVENTORY = "Inventory"


# Metadata về các cubes
CUBE_METADATA = {
    "Sales": {
        "name": "Sales",
        "description": "Cube doanh số bán hàng chi tiết theo chiều thời gian, địa lý, sản phẩm, khách hàng",
        "measures": ["sum_amount", "total_quantity"],
        "dimensions": ["year", "quarter", "month", "customer_key", "customer_type", "state", "city", "product_key"],
        "hierarchies": {
            "time": ["year", "quarter", "month"],
            "location": ["state", "city"],
            "customer": ["customer_type", "customer_key"],
            "product": ["product_key"]
        }
    },
    "Inventory": {
        "name": "Inventory",
        "description": "Cube tồn kho theo cửa hàng, sản phẩm",
        "measures": ["total_quantity_on_hand"],
        "dimensions": ["store_key", "product_key"],
        "hierarchies": {
            "store": ["store_key"],
            "product": ["product_key"]
        }
    }
}


# ==============================================================================
# REQUEST/RESPONSE MODELS
# ==============================================================================
class CubeInfo(BaseModel):
    """Metadata của một cube."""
    name: str
    description: str
    measures: List[str]
    dimensions: List[str]
    hierarchies: Dict[str, List[str]]


class CubesListResponse(BaseModel):
    """Response cho GET /api/cubes."""
    cubes: List[CubeInfo]
    count: int


class DimensionInfo(BaseModel):
    """Thông tin một chiều."""
    name: str
    type: str
    description: str


class DimensionsResponse(BaseModel):
    """Response cho GET /api/cubes/{cube}/dimensions."""
    dimensions: List[DimensionInfo]


class OLAPQueryRequest(BaseModel):
    """Request cho POST /api/cubes/{cube}/query."""
    rows: List[str] = Field(..., description="Chiều làm hàng")
    columns: List[str] = Field(default=[], description="Chiều làm cột")
    measures: List[str] = Field(default=["sum_amount"], description="Thước đo")
    filters: Dict[str, Any] = Field(default={}, description="Bộ lọc")
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=500)


class OLAPQueryResponse(BaseModel):
    """Response cho POST /api/cubes/{cube}/query."""
    data: List[Dict[str, Any]]
    pagination: Dict[str, Any]
    cuboid_used: str


class DrillThroughRequest(BaseModel):
    """Request cho POST /api/cubes/{cube}/drill-through."""
    filters: Dict[str, Any]
    columns: Optional[List[str]] = None
    page: int = Field(1, ge=1)
    page_size: int = Field(100, ge=1, le=500)


class DimensionValuesResponse(BaseModel):
    """Response cho GET /api/metadata/dim-values."""
    values: List[Any]
    pagination: Dict[str, Any]


class HealthCheckResponse(BaseModel):
    """Response cho GET /api/health."""
    status: str
    database_connection: bool
    service: str
    version: str


# ==============================================================================
# ENDPOINT 1: GET /api/cubes - Danh sách cubes
# ==============================================================================
@router.get(
    "",
    response_model=CubesListResponse,
    summary="Lấy danh sách cubes",
    description="Trả về danh sách các cube (Sales, Inventory) kèm metadata"
)
async def get_cubes():
    """Danh sách tất cả cubes có sẵn."""
    try:
        cubes = [
            CubeInfo(**metadata)
            for metadata in CUBE_METADATA.values()
        ]
        return CubesListResponse(
            cubes=cubes,
            count=len(cubes)
        )
    except Exception as e:
        logger.error(f"❌ Lỗi lấy danh sách cubes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# ENDPOINT 2: GET /api/cubes/{cube}/dimensions - Chi tiết chiều
# ==============================================================================
@router.get(
    "/{cube}/dimensions",
    response_model=DimensionsResponse,
    summary="Lấy chi tiết cấu trúc chiều",
    description="Trả về chi tiết cấu trúc chiều dữ liệu của cube"
)
async def get_cube_dimensions(cube: CubeEnum):
    """Chi tiết cấu trúc chiều của cube."""
    try:
        if cube.value not in CUBE_METADATA:
            raise HTTPException(status_code=404, detail=f"Cube '{cube.value}' không tồn tại")
        
        metadata = CUBE_METADATA[cube.value]
        
        # Ánh xạ kiểu dữ liệu cho mỗi chiều
        dim_types = {
            "year": "integer", "quarter": "integer", "month": "integer",
            "customer_key": "integer", "customer_type": "string",
            "state": "string", "city": "string", "product_key": "integer",
            "store_key": "integer"
        }
        
        dim_descriptions = {
            "year": "Năm", "quarter": "Quý", "month": "Tháng",
            "customer_key": "Mã khách hàng", "customer_type": "Loại khách hàng",
            "state": "Tỉnh/Thành phố", "city": "Thành phố",
            "product_key": "Mã sản phẩm", "store_key": "Mã cửa hàng"
        }
        
        dimensions = [
            DimensionInfo(
                name=d,
                type=dim_types.get(d, "string"),
                description=dim_descriptions.get(d, d)
            )
            for d in metadata["dimensions"]
        ]
        
        return DimensionsResponse(dimensions=dimensions)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Lỗi lấy dimensions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# ENDPOINT 3: POST /api/cubes/{cube}/query - Truy vấn OLAP
# ==============================================================================
@router.post(
    "/{cube}/query",
    response_model=OLAPQueryResponse,
    summary="Truy vấn OLAP chính",
    description="Endpoint truy vấn OLAP: nhận chiều/mức và bộ lọc, trả về kết quả phân trang"
)
async def olap_query(cube: CubeEnum, request: OLAPQueryRequest):
    """Truy vấn OLAP với cuboid routing tự động."""
    try:
        if cube.value not in CUBE_METADATA:
            raise HTTPException(status_code=404, detail=f"Cube '{cube.value}' không tồn tại")
        
        # Gọi hàm route_and_query_pivot từ olap_router
        result = route_and_query_pivot(
            rows=request.rows,
            columns=request.columns,
            measures=request.measures,
            filters=request.filters,
            page=request.page,
            page_size=request.page_size,
            cube=cube.value.lower()
        )
        
        if not result.get("success"):
            raise HTTPException(status_code=400, detail=result.get("error", "Truy vấn thất bại"))
        
        return OLAPQueryResponse(
            data=result.get("data", []),
            pagination=result.get("pagination", {}),
            cuboid_used=result.get("cuboid_used", "unknown")
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Lỗi truy vấn OLAP: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# ENDPOINT 4: POST /api/cubes/{cube}/drill-through
# ==============================================================================
@router.post(
    "/{cube}/drill-through",
    summary="Drill-through chi tiết",
    description="Truy vấn trực tiếp bảng Fact để lấy dữ liệu giao dịch chi tiết"
)
async def drill_through(cube: CubeEnum, request: DrillThroughRequest):
    """Drill-through: truy vấn bảng Fact để lấy dữ liệu chi tiết."""
    try:
        if cube.value not in CUBE_METADATA:
            raise HTTPException(status_code=404, detail=f"Cube '{cube.value}' không tồn tại")
        
        if not request.filters:
            raise HTTPException(status_code=400, detail="filters không được để trống")
        
        # Xác định bảng Fact
        fact_table = "dwh.fact_sales" if cube.value == "Sales" else "dwh.fact_inventory"
        
        # Xây dựng câu SELECT
        if request.columns:
            select_cols = ", ".join(request.columns)
        else:
            select_cols = "*"
        
        sql = f"SELECT {select_cols} FROM {fact_table}"
        
        # Xây dựng WHERE clause
        where_clauses = []
        params = []
        
        for key, value in request.filters.items():
            if value and value != "All":
                where_clauses.append(f"{key} = %s")
                params.append(value)
        
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        
        # Thực thi query với phân trang
        result = execute_query_paginated(
            sql,
            params=tuple(params) if params else None,
            page=request.page,
            page_size=request.page_size
        )
        
        return {
            "data": result["data"],
            "pagination": {
                "page": result["page"],
                "page_size": result["page_size"],
                "total": result["total"],
                "total_pages": result["total_pages"]
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Lỗi drill-through: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# ENDPOINT 5: POST /api/cubes/drill-across
# ==============================================================================
@router.post(
    "/drill-across",
    summary="Kết hợp dữ liệu từ 2 cubes",
    description="Kết hợp dữ liệu từ Sales cube và Inventory cube trên chiều chung"
)
async def drill_across(
    dimensions: List[str] = Query(..., description="Chiều dùng chung"),
    measures_sales: List[str] = Query(default=["sum_amount"], description="Measures Sales"),
    measures_inventory: List[str] = Query(default=["total_quantity_on_hand"], description="Measures Inventory"),
    filters: Optional[Dict[str, Any]] = Query(None, description="Bộ lọc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500)
):
    """Drill-across: kết hợp dữ liệu từ Sales + Inventory trên chiều chung."""
    try:
        if not dimensions:
            raise HTTPException(status_code=400, detail="dimensions không được để trống")
        
        # Xây dựng query từ Sales
        sales_cols = ", ".join(dimensions + measures_sales)
        sales_sql = f"SELECT {sales_cols} FROM {OLAP_SCHEMA}.olap_sales_base_info"
        
        # Xây dựng query từ Inventory
        inventory_cols = ", ".join(dimensions + measures_inventory)
        inventory_sql = f"SELECT {inventory_cols} FROM {OLAP_SCHEMA}.olap_inventory"
        
        # Xây dựng WHERE clause
        where_clause = ""
        params = []
        
        if filters:
            where_clauses = []
            for key, value in filters.items():
                if value and value != "All":
                    where_clauses.append(f"{key} = %s")
                    params.append(value)
            if where_clauses:
                where_clause = " WHERE " + " AND ".join(where_clauses)
        
        # Kết hợp 2 queries
        join_condition = " AND ".join([f"s.{d} = i.{d}" for d in dimensions])
        
        sql = f"""
            SELECT 
                {', '.join([f's.{d}' for d in dimensions])},
                {', '.join([f's.{m} as sales_{m}' for m in measures_sales])},
                {', '.join([f'i.{m} as inventory_{m}' for m in measures_inventory])}
            FROM ({sales_sql}) s
            FULL OUTER JOIN ({inventory_sql}) i
            ON {join_condition}
            {where_clause}
        """
        
        result = execute_query_paginated(
            sql,
            params=tuple(params) if params else None,
            page=page,
            page_size=page_size
        )
        
        return {
            "data": result["data"],
            "pagination": {
                "page": result["page"],
                "page_size": result["page_size"],
                "total": result["total"],
                "total_pages": result["total_pages"]
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Lỗi drill-across: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# ENDPOINT 6: GET /api/metadata/dim-values - Giá trị chiều
# ==============================================================================
@router.get(
    "/metadata/dim-values",
    response_model=DimensionValuesResponse,
    summary="Lấy danh sách giá trị chiều",
    description="Trả về danh sách giá trị phân biệt của một chiều (cho Filter Panel)"
)
async def get_dimension_values(
    cube: str = Query(..., description="Tên cube (Sales, Inventory)"),
    dimension: str = Query(..., description="Tên chiều"),
    search: Optional[str] = Query(None, description="Tìm kiếm (partial match)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500)
):
    """Danh sách giá trị phân biệt của một chiều."""
    try:
        if cube not in CUBE_METADATA:
            raise HTTPException(status_code=404, detail=f"Cube '{cube}' không tồn tại")
        
        if dimension not in CUBE_METADATA[cube]["dimensions"]:
            raise HTTPException(status_code=404, detail=f"Chiều '{dimension}' không tồn tại trong cube '{cube}'")
        
        # Xác định bảng OLAP
        table = f"{OLAP_SCHEMA}.olap_sales_base_info" if cube == "Sales" else f"{OLAP_SCHEMA}.olap_inventory"
        
        # Xây dựng query
        sql = f"SELECT DISTINCT {dimension} FROM {table} WHERE {dimension} IS NOT NULL"
        params = []
        
        if search:
            sql += f" AND CAST({dimension} AS VARCHAR) LIKE %s"
            params.append(f"%{search}%")
        
        sql += f" ORDER BY {dimension}"
        
        # Thực thi query với phân trang
        result = execute_query_paginated(
            sql,
            params=tuple(params) if params else None,
            page=page,
            page_size=page_size
        )
        
        return DimensionValuesResponse(
            values=[row[dimension] for row in result["data"]],
            pagination={
                "page": result["page"],
                "page_size": result["page_size"],
                "total": result["total"],
                "total_pages": result["total_pages"]
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Lỗi lấy giá trị chiều: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# ENDPOINT 7: GET /api/health - Health check
# ==============================================================================
@router.get(
    "/health",
    response_model=HealthCheckResponse,
    summary="Kiểm tra trạng thái",
    description="Kiểm tra trạng thái kết nối database"
)
async def health_check():
    """Kiểm tra trạng thái của API."""
    try:
        db_connected = test_connection()
        status = "healthy" if db_connected else "degraded"
        
        return HealthCheckResponse(
            status=status,
            database_connection=db_connected,
            service="Data Warehouse OLAP API",
            version="1.0.0"
        )
    except Exception as e:
        logger.error(f"❌ Lỗi health check: {str(e)}")
        return HealthCheckResponse(
            status="unhealthy",
            database_connection=False,
            service="Data Warehouse OLAP API",
            version="1.0.0"
        )
