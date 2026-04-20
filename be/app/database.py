"""
================================================================================
DATABASE.PY - Kết nối PostgreSQL bằng psycopg2
================================================================================
Module quản lý kết nối đến PostgreSQL sử dụng psycopg2 (Raw SQL).
KHÔNG sử dụng ORM (SQLAlchemy) theo yêu cầu dự án.

Cung cấp:
  - Connection pool để tái sử dụng kết nối
  - Hàm execute_query để thực thi câu lệnh SELECT
  - Hàm get_connection để lấy connection trực tiếp

Author: Data Warehouse Team
================================================================================
"""

import os
import psycopg2
from psycopg2 import pool, extras
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Tuple
import logging

# Load biến môi trường từ file .env
load_dotenv()

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==============================================================================
# DATABASE CONFIGURATION - Đọc từ file .env
# ==============================================================================
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin")
}

# Schema OLAP mặc định (tên bảng: olap_sales_month_* / quarter_* / year_* — xem `schema olap.sql`)
OLAP_SCHEMA = "olap"


# ==============================================================================
# CONNECTION POOL - Quản lý kết nối hiệu quả
# ==============================================================================
class DatabasePool:
    """
    Singleton class quản lý connection pool đến PostgreSQL.
    Sử dụng pattern Singleton để đảm bảo chỉ có một pool duy nhất.
    """
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance
    
    def _initialize_pool(self):
        """Khởi tạo connection pool."""
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=5,      # Số kết nối tối thiểu
                maxconn=20,     # Số kết nối tối đa
                **DB_CONFIG
            )
            logger.info(f"✅ [DB] Connection pool khởi tạo thành công: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        except Exception as e:
            logger.error(f"❌ [DB] Lỗi khởi tạo connection pool: {str(e)}")
            raise
    
    def get_connection(self):
        """Lấy một connection từ pool."""
        try:
            return self._pool.getconn()
        except Exception as e:
            logger.error(f"❌ [DB] Lỗi lấy connection từ pool: {str(e)}")
            raise
    
    def put_connection(self, conn):
        """Trả connection về pool."""
        try:
            self._pool.putconn(conn)
        except Exception as e:
            logger.error(f"❌ [DB] Lỗi trả connection về pool: {str(e)}")
    
    def close_all(self):
        """Đóng tất cả connections trong pool."""
        try:
            self._pool.closeall()
            logger.info("✅ [DB] Đã đóng tất cả connections")
        except Exception as e:
            logger.error(f"❌ [DB] Lỗi đóng connections: {str(e)}")


# Khởi tạo singleton instance
db_pool = DatabasePool()


# ==============================================================================
# PUBLIC API - Các hàm sử dụng bên ngoài
# ==============================================================================
def get_connection():
    """
    Lấy một database connection từ pool.
    
    Returns:
        psycopg2 connection object
    
    Example:
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM olap.olap_sales_all")
            results = cursor.fetchall()
        finally:
            release_connection(conn)
    """
    return db_pool.get_connection()


def release_connection(conn):
    """
    Trả connection về pool sau khi sử dụng.
    
    Args:
        conn: Connection cần trả về
    """
    db_pool.put_connection(conn)


def execute_query(
    sql: str,
    params: Optional[Tuple] = None,
    fetch_all: bool = True
) -> List[Dict[str, Any]]:
    """
    Thực thi câu lệnh SELECT và trả về kết quả.
    
    Args:
        sql: Câu lệnh SQL cần thực thi
        params: Các tham số cho câu lệnh (tuple)
        fetch_all: True = fetch all, False = fetch one
    
    Returns:
        List[Dict]: Danh sách các bản ghi dưới dạng dictionary
    
    Example:
        results = execute_query(
            "SELECT * FROM olap.olap_sales_by_time WHERE year = %s",
            (2024,)
        )
    """
    conn = None
    cursor = None
    
    try:
        conn = db_pool.get_connection()
        # Sử dụng RealDictCursor để trả về dạng dictionary
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Log câu query (chỉ trong development)
        logger.debug(f"[SQL] {sql}")
        if params:
            logger.debug(f"[PARAMS] {params}")
        
        # Thực thi query
        cursor.execute(sql, params)
        
        # Fetch kết quả
        if fetch_all:
            results = cursor.fetchall()
        else:
            result = cursor.fetchone()
            results = [result] if result else []
        
        # Chuyển đổi từ RealDictRow sang list of dict
        return [dict(row) for row in results]
        
    except Exception as e:
        logger.error(f"❌ [DB] Lỗi thực thi query: {str(e)}")
        logger.error(f"[SQL] {sql}")
        logger.error(f"[PARAMS] {params}")
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.put_connection(conn)


def execute_query_paginated(
    sql: str,
    params: Optional[Tuple] = None,
    page: int = 1,
    page_size: int = 100
) -> Dict[str, Any]:
    """
    Thực thi câu lệnh SELECT với phân trang.
    
    Args:
        sql: Câu lệnh SQL gốc (KHÔNG có LIMIT/OFFSET)
        params: Các tham số cho câu lệnh
        page: Số trang (bắt đầu từ 1)
        page_size: Số bản ghi mỗi trang
    
    Returns:
        Dict chứa:
            - data: Danh sách bản ghi
            - page: Trang hiện tại
            - page_size: Kích thước trang
            - total: Tổng số bản ghi
            - total_pages: Tổng số trang
    
    Example:
        result = execute_query_paginated(
            "SELECT * FROM olap.olap_sales_base WHERE year = %s",
            (2024,),
            page=1,
            page_size=50
        )
    """
    conn = None
    cursor = None
    
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Đếm tổng số bản ghi
        count_sql = f"SELECT COUNT(*) FROM ({sql}) AS count_query"
        cursor.execute(count_sql, params)
        total = cursor.fetchone()["count"]
        
        # Thêm LIMIT và OFFSET
        paginated_sql = f"{sql} LIMIT %s OFFSET %s"
        offset = (page - 1) * page_size
        
        if params:
            query_params = params + (page_size, offset)
        else:
            query_params = (page_size, offset)
        
        cursor.execute(paginated_sql, query_params)
        data = [dict(row) for row in cursor.fetchall()]
        
        total_pages = (total + page_size - 1) // page_size
        
        return {
            "data": data,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages
        }
        
    except Exception as e:
        logger.error(f"❌ [DB] Lỗi thực thi query phân trang: {str(e)}")
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.put_connection(conn)


def test_connection() -> bool:
    """
    Kiểm tra kết nối đến database.
    
    Returns:
        bool: True nếu kết nối thành công, False nếu thất bại
    """
    try:
        result = execute_query("SELECT 1 as test", fetch_all=False)
        return result[0]["test"] == 1
    except Exception as e:
        logger.error(f"❌ [DB] Kiểm tra kết nối thất bại: {str(e)}")
        return False


# ==============================================================================
# UTILITY FUNCTIONS - Các hàm tiện ích
# ==============================================================================
def build_where_clause(
    filters: Dict[str, Any],
    column_mapping: Dict[str, str]
) -> Tuple[str, Tuple]:
    """
    Xây dựng mệnh đề WHERE từ dictionary filters.
    
    Args:
        filters: Dict chứa các điều kiện lọc
        column_mapping: Dict ánh xạ tên filter -> tên cột trong DB
    
    Returns:
        Tuple (where_clause, params)
    
    Example:
        filters = {"year": 2024, "state": "California"}
        mapping = {"year": "year", "state": "state"}
        where_clause, params = build_where_clause(filters, mapping)
        # where_clause = "WHERE year = %s AND state = %s"
        # params = (2024, "California")
    """
    conditions = []
    params = []
    
    for filter_key, filter_value in filters.items():
        # Bỏ qua các giá trị "All" hoặc None
        if filter_value is None or filter_value == "All" or filter_value == "":
            continue
        
        # Lấy tên cột tương ứng
        column = column_mapping.get(filter_key)
        if not column:
            continue
        
        conditions.append(f"{column} = %s")
        params.append(filter_value)
    
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)
    else:
        where_clause = ""
    
    return where_clause, tuple(params)


def sanitize_identifier(identifier: str) -> str:
    """
    Làm sạch tên cột/bảng để tránh SQL injection.
    Chỉ cho phép các ký tự alphanumeric và dấu gạch dưới.
    
    Args:
        identifier: Tên cột/bảng cần làm sạch
    
    Returns:
        str: Tên đã được làm sạch
    """
    import re
    # Chỉ giữ lại alphanumeric và gạch dưới
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', identifier)
    return sanitized


# ==============================================================================
# INITIALIZATION - Kiểm tra kết nối khi import module
# ==============================================================================
if __name__ == "__main__":
    # Test kết nối
    print("🔄 [TEST] Đang kiểm tra kết nối database...")
    if test_connection():
        print("✅ [TEST] Kết nối database thành công!")
        
        # Test query đơn giản
        result = execute_query("SELECT COUNT(*) as count FROM olap.olap_sales_all")
        print(f"📊 [TEST] Số bản ghi trong olap_sales_all: {result[0]['count']}")
    else:
        print("❌ [TEST] Kết nối database thất bại!")
