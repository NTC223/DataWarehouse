"""
================================================================================
MAIN.PY - FastAPI Application Entry Point
================================================================================
File khởi tạo ứng dụng FastAPI chính cho hệ thống Data Warehouse Dashboard.
Cung cấp API cho 2 tab chính:
  1. Dashboard (Executive View) - Báo cáo Sales
  2. OLAP Explorer - Interactive Pivot Table

Author: Data Warehouse Team
Version: 1.0.0
================================================================================
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Import các router
from app.routers import dashboard, olap_explorer, filters, inventory


# ==============================================================================
# LIFESPAN EVENT HANDLER - Quản lý vòng đởi ứng dụng
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handler cho các sự kiện startup và shutdown của ứng dụng.
    - Startup: Khởi tạo kết nối pool, load metadata
    - Shutdown: Đóng kết nối, dọn dẹp tài nguyên
    """
    # ===== STARTUP =====
    print("🚀 [STARTUP] Đang khởi tạo Data Warehouse Dashboard API...")
    print("✅ [STARTUP] Khởi tạo thành công!")
    
    yield  # Ứng dụng chạy ở đây
    
    # ===== SHUTDOWN =====
    print("🛑 [SHUTDOWN] Đang đóng ứng dụng...")
    print("✅ [SHUTDOWN] Đã dọn dẹp tài nguyên!")


# ==============================================================================
# KHỞI TẠO FASTAPI APP
# ==============================================================================
app = FastAPI(
    title="Data Warehouse Dashboard API",
    description="""
    API cho hệ thống Data Warehouse với khả năng:
    - Dashboard Sales với KPI cards, trend charts, drill-down
    - Dashboard Inventory với Drill-Across analysis
    - OLAP Explorer với Pivot Table và Cuboid Routing
    
    ## Các endpoint chính:
    
    ### Dashboard Sales
    - `POST /api/dashboard/overview` - Lấy 3 KPI cards
    - `POST /api/dashboard/trend` - Lấy dữ liệu xu hướng (drill-down)
    - `POST /api/dashboard/top-products` - Top 5/Bottom 5 sản phẩm
    - `POST /api/dashboard/customer-segment` - Phân khúc khách hàng
    
    ### Dashboard Inventory
    - `GET /api/inventory/overview` - Tổng quan tồn kho
    - `GET /api/inventory/analysis/{product_id}` - Phân tích Drill-Across
    - `GET /api/inventory/scatter-data` - Scatter plot rủi ro tồn kho
    - `GET /api/inventory/products` - Danh sách sản phẩm
    - `GET /api/inventory/stores` - Danh sách cửa hàng
    
    ### OLAP Explorer
    - `POST /api/olap/explore` - Pivot table với Cuboid Routing
    - `POST /api/olap/raw-data` - Dữ liệu thô có phân trang
    
    ### Filters
    - `GET /api/filters/init` - Lấy tất cả dữ liệu filter khởi tạo
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)


# ==============================================================================
# CORS MIDDLEWARE - Cho phép gọi API từ Frontend
# ==============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cho phép tất cả origins (trong production nên giới hạn)
    allow_credentials=True,
    allow_methods=["*"],  # Cho phép tất cả methods
    allow_headers=["*"],  # Cho phép tất cả headers
)


# ==============================================================================
# ĐĂNG KÝ ROUTERS
# ==============================================================================
# Router cho Dashboard (Tab 1)
app.include_router(
    dashboard.router,
    prefix="/api/dashboard",
    tags=["Dashboard - Báo cáo Sales"]
)

# Router cho OLAP Explorer (Tab 2)
app.include_router(
    olap_explorer.router,
    prefix="/api/olap",
    tags=["OLAP Explorer - Pivot Table"]
)

# Router cho Filters (Khởi tạo dữ liệu filter)
app.include_router(
    filters.router,
    prefix="/api/filters",
    tags=["Filters - Dữ liệu bộ lọc"]
)

# Router cho Inventory (Drill-Across Analysis)
app.include_router(
    inventory.router,
    prefix="/api/inventory",
    tags=["Inventory - Phân tích tồn kho"]
)


# ==============================================================================
# HEALTH CHECK ENDPOINT
# ==============================================================================
@app.get("/health", tags=["Health Check"])
async def health_check():
    """
    Endpoint kiểm tra trạng thái hoạt động của API.
    """
    return {
        "status": "healthy",
        "service": "Data Warehouse Dashboard API",
        "version": "1.0.0"
    }


@app.get("/", tags=["Root"])
async def root():
    """
    Endpoint gốc - Redirect đến documentation.
    """
    return {
        "message": "Data Warehouse Dashboard API",
        "documentation": "/docs",
        "health_check": "/health"
    }


# ==============================================================================
# MAIN - Chạy ứng dụng (chỉ dùng khi chạy trực tiếp)
# ==============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Tự động reload khi code thay đổi (chỉ dùng trong dev)
        log_level="info"
    )
