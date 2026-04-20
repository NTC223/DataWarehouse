"""
================================================================================
OLAP_EXPLORER.PY - API cho Tab OLAP Explorer (Pivot Table)
================================================================================
Cung cấp các endpoint cho OLAP Explorer:
  - /explore: Pivot table với Cuboid Routing
  - /raw-data: Dữ liệu thô với phân trang

Hỗ trợ 4 phép toán OLAP:
  - Slice: Lọc dữ liệu theo 1 chiều
  - Dice: Lọc dữ liệu theo nhiều chiều
  - Pivot: Xoay trục (đổi rows <-> columns)
  - Drill-down: Khoan sâu dữ liệu

Author: Data Warehouse Team
================================================================================
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import logging

from app.services.olap_router import (
    route_and_query_pivot,
    route_and_query_raw,
    get_router_info,
    router as cuboid_router
)

# Cấu hình logging
logger = logging.getLogger(__name__)

# Khởi tạo router
router = APIRouter()


# ==============================================================================
# REQUEST/RESPONSE MODELS
# ==============================================================================
class ExploreRequest(BaseModel):
    """
    Model cho request explore (Pivot Table).
    
    Example:
        {
            "rows": ["year", "quarter"],
            "columns": ["customer_type"],
            "measures": ["sum_amount"],
            "filters": {"state": "California"},
            "limit": 5000
        }
    """
    rows: List[str] = Field(
        default=[],
        description="List các cột cho hàng (VD: ['year', 'quarter'])"
    )
    columns: List[str] = Field(
        default=[],
        description="List các cột cho cột (VD: ['customer_type'])"
    )
    measures: List[str] = Field(
        default=["sum_amount"],
        description="List các measure cần tính (VD: ['sum_amount', 'total_quantity', 'total_quantity_on_hand'])"
    )
    filters: Dict[str, Any] = Field(
        default={},
        description="Dict các điều kiện filter (VD: {'state': 'California'})"
    )
    limit: int = Field(
        default=5000,
        ge=1,
        le=10000,
        description="Giới hạn số bản ghi (legacy, mặc định 5000)"
    )
    page: int = Field(default=1, ge=1, description="Số trang (bắt đầu từ 1)")
    page_size: int = Field(default=50, ge=10, le=500, description="Số dòng mỗi trang")
    sort_column: Optional[str] = Field(default=None, description="Tên cột cần sắp xếp")
    sort_order: str = Field(default="asc", description="Chiều sắp xếp: 'asc' hoặc 'desc'")
    cube: str = Field(
        default="sales",
        description="Cube OLAP: 'sales' hoặc 'inventory'",
    )


class RawDataRequest(BaseModel):
    """
    Model cho request raw data.
    
    Example:
        {
            "columns": ["year", "product_key", "sum_amount"],
            "filters": {"year": 2024},
            "page": 1,
            "page_size": 100
        }
    """
    columns: List[str] = Field(
        default=[],
        description="List các cột cần SELECT (empty = tất cả)"
    )
    filters: Dict[str, Any] = Field(
        default={},
        description="Dict các điều kiện filter"
    )
    page: int = Field(
        default=1,
        ge=1,
        description="Số trang (bắt đầu từ 1)"
    )
    page_size: int = Field(
        default=100,
        ge=10,
        le=500,
        description="Kích thước trang (10-500)"
    )
    cube: str = Field(
        default="sales",
        description="Cube OLAP: 'sales' hoặc 'inventory'",
    )


class PivotCell(BaseModel):
    """Model cho một ô trong pivot table."""
    row_key: str
    col_key: str
    value: float


class ExploreResponse(BaseModel):
    """Model cho response explore."""
    success: bool
    data: List[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]] = None
    pagination: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class RawDataResponse(BaseModel):
    """Model cho response raw data."""
    success: bool
    data: List[Dict[str, Any]]
    pagination: Dict[str, Any]
    error: Optional[str] = None


class RouterInfoResponse(BaseModel):
    """Model cho response router info."""
    router_status: str
    cuboid_stats: Dict[str, Any]
    dimension_mapping: Dict[str, List[str]]


# ==============================================================================
# AVAILABLE FIELDS - Các trường có thể sử dụng
# ==============================================================================
AVAILABLE_FIELDS = {
    "dimensions": {
        "time": {
            "columns": ["year", "quarter", "month"],
            "description": "Thời gian (Năm, Quý, Tháng)"
        },
        "product": {
            "columns": ["product_key"],
            "description": "Sản phẩm"
        },
        "customer": {
            "columns": ["customer_type", "customer_key", "state", "city"],
            "description": "Khách hàng (loại, mã KH, bang, thành phố); không dùng dimension location tách riêng",
        },
        "store": {
            "columns": ["store_key", "state", "city"],
            "description": "Cửa hàng (Inventory): store_key và địa lý",
        },
    },
    "measures": {
        "sum_amount": {
            "description": "Tổng doanh thu (Sales)",
            "format": "currency"
        },
        "total_quantity": {
            "description": "Tổng số lượng bán (Sales)",
            "format": "number"
        },
        "total_quantity_on_hand": {
            "description": "Tổng tồn kho (Inventory)",
            "format": "number"
        },
    }
}


# ==============================================================================
# API ENDPOINTS
# ==============================================================================
@router.post("/explore", response_model=ExploreResponse)
async def explore(request: ExploreRequest):
    """
    Endpoint chính cho Pivot Table - Thực hiện Cuboid Routing.
    
    ## Logic xử lý:
    1. Nhận cấu hình rows, columns, measures, filters
    2. Sử dụng CuboidRouter để tìm bảng tối ưu
    3. Sinh câu SQL phù hợp
    4. Thực thi query và trả về kết quả
    
    ## 4 Phép toán OLAP:
    - **Slice**: Đặt filter để lọc 1 chiều (VD: {"state": "California"})
    - **Dice**: Đặt nhiều filter để lọc nhiều chiều
    - **Pivot**: Đổi chỗ rows và columns
    - **Drill-down**: Thêm cột vào rows (VD: year -> quarter -> month)
    
    ## Example:
    ```json
    {
        "rows": ["year", "quarter"],
        "columns": ["customer_type"],
        "measures": ["sum_amount"],
        "filters": {"state": "California"},
        "limit": 5000
    }
    ```
    """
    try:
        logger.info(f"[Explore] Request: rows={request.rows}, columns={request.columns}")
        
        # Validate fields
        all_fields = request.rows + request.columns + list(request.filters.keys())
        valid_columns = []
        for dim in AVAILABLE_FIELDS["dimensions"].values():
            valid_columns.extend(dim["columns"])
        
        invalid_fields = [f for f in all_fields if f not in valid_columns]
        if invalid_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Các trường không hợp lệ: {invalid_fields}. "
                       f"Các trường hợp lệ: {valid_columns}"
            )
        
        # Validate measures
        valid_measures = list(AVAILABLE_FIELDS["measures"].keys())
        invalid_measures = [m for m in request.measures if m not in valid_measures]
        if invalid_measures:
            raise HTTPException(
                status_code=400,
                detail=f"Các measure không hợp lệ: {invalid_measures}. "
                       f"Các measure hợp lệ: {valid_measures}"
            )
        
        # Thực hiện routing và query
        result = route_and_query_pivot(
            rows=request.rows,
            columns=request.columns,
            measures=request.measures,
            filters=request.filters,
            limit=request.limit,
            cube=request.cube,
            page=request.page,
            page_size=request.page_size,
            sort_column=request.sort_column,
            sort_order=request.sort_order
        )

        # Inject pagination info vào response
        if result.get("success") and result.get("metadata"):
            result["pagination"] = {
                "page": result["metadata"].get("page", 1),
                "page_size": result["metadata"].get("page_size", 50),
                "total_rows": result["metadata"].get("total_rows", len(result["data"])),
                "total_pages": result["metadata"].get("total_pages", 1),
            }

        return ExploreResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [Explore] Lỗi: {str(e)}")
        return ExploreResponse(
            success=False,
            data=[],
            metadata={},
            error=str(e)
        )


@router.post("/raw-data", response_model=RawDataResponse)
async def get_raw_data(request: RawDataRequest):
    """
    Lấy dữ liệu thô (Raw Data) với phân trang.
    
    ## Logic:
    - Luôn query từ bảng `olap_sales_base_loc` (không dùng Cuboid Routing)
    - Hỗ trợ phân trang với LIMIT và OFFSET
    - Có thể chọn cột cụ thể hoặc lấy tất cả
    
    ## Example:
    ```json
    {
        "columns": ["year", "product_key", "customer_type", "sum_amount"],
        "filters": {"year": 2024, "state": "California"},
        "page": 1,
        "page_size": 100
    }
    ```
    """
    try:
        logger.info(f"[RawData] Request: page={request.page}, page_size={request.page_size}")
        
        # Validate columns nếu có
        if request.columns:
            valid_columns = []
            for dim in AVAILABLE_FIELDS["dimensions"].values():
                valid_columns.extend(dim["columns"])
            valid_columns.extend(list(AVAILABLE_FIELDS["measures"].keys()))
            
            invalid_cols = [c for c in request.columns if c not in valid_columns]
            if invalid_cols:
                raise HTTPException(
                    status_code=400,
                    detail=f"Các cột không hợp lệ: {invalid_cols}"
                )
        
        # Thực hiện query
        result = route_and_query_raw(
            columns=request.columns,
            filters=request.filters,
            page=request.page,
            page_size=request.page_size,
            cube=request.cube,
        )
        
        return RawDataResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [RawData] Lỗi: {str(e)}")
        return RawDataResponse(
            success=False,
            data=[],
            pagination={
                "page": request.page,
                "page_size": request.page_size,
                "total": 0,
                "total_pages": 0
            },
            error=str(e)
        )


@router.get("/fields")
async def get_available_fields():
    """
    Lấy danh sách các trường có thể sử dụng trong Pivot Table.
    
    Returns:
        Dict chứa:
        - dimensions: Các dimension và cột tương ứng
        - measures: Các measure có thể tính
    """
    return {
        "success": True,
        "data": AVAILABLE_FIELDS
    }


@router.get("/router-info", response_model=RouterInfoResponse)
async def get_cuboid_router_info():
    """
    Lấy thông tin về Cuboid Router.
    
    Returns:
        Thông tin về:
        - Trạng thái router
        - Thống kê các cuboid
        - Mapping dimension -> columns
    """
    try:
        info = get_router_info()
        return RouterInfoResponse(**info)
    except Exception as e:
        logger.error(f"❌ [RouterInfo] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/swap-axes")
async def swap_axes(request: ExploreRequest):
    """
    Xoay trục (Pivot) - Đổi chỗ rows và columns.
    
    Đây là helper endpoint để thực hiện phép toán Pivot nhanh.
    """
    try:
        new_request = request.model_copy(
            update={"rows": request.columns, "columns": request.rows}
        )
        return await explore(new_request)
        
    except Exception as e:
        logger.error(f"❌ [SwapAxes] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/drill-down")
async def drill_down(
    current_rows: List[str],
    new_field: str,
    request: ExploreRequest
):
    """
    Drill-down - Thêm một cột mới vào rows.
    
    Args:
        current_rows: Danh sách rows hiện tại
        new_field: Cột mới cần thêm (drill-down)
        request: Request gốc
    
    Example:
        - Drill-down từ year -> quarter: current_rows=["year"], new_field="quarter"
        - Kết quả: rows=["year", "quarter"]
    """
    try:
        # Thêm field mới vào rows
        new_rows = current_rows + [new_field]
        
        new_request = request.model_copy(update={"rows": new_rows})
        return await explore(new_request)
        
    except Exception as e:
        logger.error(f"❌ [DrillDown] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/roll-up")
async def roll_up(
    current_rows: List[str],
    request: ExploreRequest
):
    """
    Roll-up - Bỏ cột cuối cùng trong rows (đi ngược lên).
    
    Args:
        current_rows: Danh sách rows hiện tại
        request: Request gốc
    
    Example:
        - Roll-up từ year,quarter -> year: current_rows=["year", "quarter"]
        - Kết quả: rows=["year"]
    """
    try:
        # Bỏ cột cuối cùng
        if len(current_rows) > 0:
            new_rows = current_rows[:-1]
        else:
            new_rows = []
        
        new_request = request.model_copy(update={"rows": new_rows})
        return await explore(new_request)
        
    except Exception as e:
        logger.error(f"❌ [RollUp] Lỗi: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
