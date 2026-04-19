"""
db.py — Database connection pool for OLAP Web UI backend.
Uses psycopg2 ThreadedConnectionPool for thread-safe connection reuse
(FastAPI runs sync handlers in a threadpool).
Reads configuration from environment variables.
"""

import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager

# ── Configuration from environment ────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin")
DB_NAME = os.getenv("DB_NAME", "postgres")

# ── Connection pool ──────────────────────────────────────────
_pool: ThreadedConnectionPool | None = None


def init_pool(minconn: int = 2, maxconn: int = 10):
    """Initialize the connection pool. Called once at app startup."""
    global _pool
    _pool = ThreadedConnectionPool(
        minconn,
        maxconn,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME,
    )


def close_pool():
    """Close all connections in the pool. Called at app shutdown."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


@contextmanager
def get_conn():
    """
    Context manager that yields a database connection from the pool.
    Automatically returns the connection to the pool when done.
    On exception, rolls back the transaction.
    """
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_pool() first.")

    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def execute_query(sql: str, params: tuple | list | None = None) -> tuple[list[str], list[list]]:
    """
    Execute a SELECT query and return (column_names, rows).

    Args:
        sql:    SQL query string with %s placeholders
        params: Tuple/list of parameter values (or None)

    Returns:
        Tuple of (column_names: list[str], rows: list[list])
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            rows = [list(row) for row in cur.fetchall()]
            return columns, rows


def execute_query_dict(sql: str, params: tuple | list | None = None) -> list[dict]:
    """
    Execute a SELECT query and return list of dicts.

    Args:
        sql:    SQL query string with %s placeholders
        params: Tuple/list of parameter values (or None)

    Returns:
        List of dicts, each dict maps column_name → value
    """
    columns, rows = execute_query(sql, params)
    return [dict(zip(columns, row)) for row in rows]
