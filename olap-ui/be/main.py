"""
main.py — FastAPI application for the OLAP Web UI backend.

Provides REST endpoints for:
  - Listing cubes and their dimensions
  - Querying OLAP cuboids (drill-down, roll-up, slice, dice)
  - Drill-across between Sales and Inventory cubes
  - Drill-through to DWH fact table detail records
  - Fetching dimension values for filters (with pagination/search)
"""

import os
import time
from typing import Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from db import init_pool, close_pool, execute_query
from models import (
    OLAPQueryRequest,
    OLAPQueryResponse,
    DrillThroughRequest,
    DrillThroughResponse,
    DrillAcrossRequest,
    DrillAcrossResponse,
    DimValuesResponse,
    CubeInfo,
    FilterCondition,
)
from cuboid_router import (
    CUBE_META,
    resolve_cuboid,
    resolve_drill_across,
    DRILL_THROUGH_SQL,
    DIM_VALUES_CONFIG,
)


# ── App lifecycle ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB pool on startup, close on shutdown."""
    init_pool(minconn=2, maxconn=10)
    yield
    close_pool()


app = FastAPI(
    title="OLAP Explorer API",
    description="REST API for OLAP operations on Sales & Inventory cubes",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────
_cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:80").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════
# HELPER: Build WHERE clause from filters
# ═══════════════════════════════════════════════════════════════

def _build_where(
    filters: list[FilterCondition],
    valid_columns: list[str],
    column_prefix: str = "",
    column_map: dict[str, str] | None = None,
) -> tuple[str, list]:
    """
    Build a parameterized WHERE clause from filter conditions.

    Args:
        filters:       List of FilterCondition objects
        valid_columns: List of allowed column names (prevent SQL injection)
        column_prefix: Optional table prefix (e.g., 't.')
        column_map:    Optional mapping of filter column names to SQL column refs

    Returns:
        (where_clause_str, params_list)
        where_clause_str starts with ' WHERE ...' or is '' if no filters.
    """
    if not filters:
        return "", []

    clauses = []
    params = []

    for f in filters:
        # Determine actual SQL column reference
        if column_map and f.column in column_map:
            col_ref = column_map[f.column]
        elif f.column in valid_columns:
            col_ref = f"{column_prefix}{f.column}" if column_prefix else f.column
        else:
            continue  # skip invalid columns silently

        if f.operator == "eq" and len(f.values) == 1:
            clauses.append(f"{col_ref} = %s")
            params.append(f.values[0])
        elif f.operator == "in" and len(f.values) == 1:
            clauses.append(f"{col_ref} = %s")
            params.append(f.values[0])
        elif f.operator == "in" and len(f.values) > 1:
            placeholders = ", ".join(["%s"] * len(f.values))
            clauses.append(f"{col_ref} IN ({placeholders})")
            params.extend(f.values)
        elif f.operator == "between" and len(f.values) == 2:
            clauses.append(f"{col_ref} BETWEEN %s AND %s")
            params.extend(f.values[:2])

    if not clauses:
        return "", []

    return " WHERE " + " AND ".join(clauses), params


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════


# ── 1. List cubes ─────────────────────────────────────────────

@app.get("/api/cubes", response_model=list[CubeInfo])
def list_cubes():
    """Return metadata about all available cubes."""
    result = []
    for key, meta in CUBE_META.items():
        result.append(CubeInfo(
            name=meta['name'],
            display_name=meta['display_name'],
            fact_table=meta['fact_table'],
            measures=meta['measures'],
            dimensions=[
                {
                    'name': d['name'],
                    'display': d['display'],
                    'levels': d['levels'],
                    'hierarchy': d['hierarchy'],
                }
                for d in meta['dimensions']
            ],
        ))
    return result


# ── 2. Get cube dimensions ───────────────────────────────────

@app.get("/api/cubes/{cube}/dimensions")
def get_cube_dimensions(cube: str):
    """Return dimensions and hierarchy levels for a specific cube."""
    if cube not in CUBE_META:
        raise HTTPException(status_code=404, detail=f"Cube '{cube}' not found")

    return {
        'cube': cube,
        'display_name': CUBE_META[cube]['display_name'],
        'measures': CUBE_META[cube]['measures'],
        'dimensions': CUBE_META[cube]['dimensions'],
    }


# ── 3. Main OLAP query ───────────────────────────────────────

@app.post("/api/cubes/{cube}/query", response_model=OLAPQueryResponse)
def query_cube(cube: str, req: OLAPQueryRequest):
    """
    Main OLAP query endpoint.
    Resolves the correct cuboid based on active dimensions,
    applies filters (slice/dice), and returns results.
    """
    if cube not in CUBE_META:
        raise HTTPException(status_code=404, detail=f"Cube '{cube}' not found")

    try:
        cuboid = resolve_cuboid(cube, req.dimensions)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    table_name = cuboid['table']
    dim_cols = cuboid['dim_cols']
    measure_cols = cuboid['measures']
    all_cols = dim_cols + measure_cols

    # Build WHERE clause
    where_clause, where_params = _build_where(req.filters, all_cols)
    params = list(where_params)

    # Build ORDER BY
    order_clause = ""
    if req.sort_by and req.sort_by in all_cols:
        direction = "ASC" if req.sort_order == "asc" else "DESC"
        order_clause = f" ORDER BY {req.sort_by} {direction}"
    elif dim_cols:
        order_clause = f" ORDER BY {', '.join(dim_cols)}"

    # Get total count
    count_sql = f"SELECT COUNT(*) FROM olap.{table_name}{where_clause}"
    try:
        _, count_result = execute_query(count_sql, where_params if where_params else None)
        total_count = count_result[0][0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Count query failed: {str(e)}")

    # Build parameterized LIMIT/OFFSET (limit always set, default 500)
    limit_clause = " LIMIT %s OFFSET %s"
    params.extend([req.limit, req.offset])

    sql = f"SELECT * FROM olap.{table_name}{where_clause}{order_clause}{limit_clause}"

    t0 = time.time()
    try:
        columns, rows = execute_query(sql, params if params else None)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    query_time_ms = round((time.time() - t0) * 1000, 2)

    # Determine actual dim/measure columns from result
    actual_dim_cols = [c for c in columns if c in dim_cols]
    actual_measure_cols = [c for c in columns if c in measure_cols]

    # Serialize Decimal/numeric values to float for JSON
    serialized_rows = []
    for row in rows:
        serialized_rows.append([
            float(v) if hasattr(v, 'as_tuple') else v  # Decimal → float
            for v in row
        ])

    return OLAPQueryResponse(
        columns=columns,
        dimension_columns=actual_dim_cols,
        measure_columns=actual_measure_cols,
        rows=serialized_rows,
        total_count=total_count,
        has_more=req.offset + len(serialized_rows) < total_count,
        cuboid_used=table_name,
        query_time_ms=query_time_ms,
    )


# ── 4. Drill-through ─────────────────────────────────────────

@app.post("/api/cubes/{cube}/drill-through", response_model=DrillThroughResponse)
def drill_through(cube: str, req: DrillThroughRequest):
    """
    Drill-through: query the DWH fact table directly for detail records.
    Returns individual fact rows with all dimension attributes joined.
    """
    if cube not in DRILL_THROUGH_SQL:
        raise HTTPException(status_code=404, detail=f"Cube '{cube}' not found")

    dt_config = DRILL_THROUGH_SQL[cube]
    base_sql = dt_config['base_sql'].strip()
    col_map = dt_config['filter_column_map']

    # Build WHERE using the column mapping
    where_clause, where_params = _build_where(
        req.filters,
        valid_columns=list(col_map.keys()),
        column_map=col_map,
    )
    params = list(where_params)

    # Get total count
    count_sql = f"SELECT COUNT(*) FROM ({base_sql}{where_clause}) sub"
    try:
        _, count_result = execute_query(count_sql, where_params if where_params else None)
        total_count = count_result[0][0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Drill-through count query failed: {str(e)}")

    sql = f"{base_sql}{where_clause} ORDER BY 1 LIMIT %s OFFSET %s"
    params.extend([req.limit, req.offset])

    t0 = time.time()
    try:
        columns, rows = execute_query(sql, params if params else None)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Drill-through failed: {str(e)}")
    query_time_ms = round((time.time() - t0) * 1000, 2)

    serialized_rows = []
    for row in rows:
        serialized_rows.append([
            float(v) if hasattr(v, 'as_tuple') else
            v.isoformat() if hasattr(v, 'isoformat') else v
            for v in row
        ])

    return DrillThroughResponse(
        columns=columns,
        rows=serialized_rows,
        total_count=total_count,
        has_more=req.offset + len(serialized_rows) < total_count,
        query_time_ms=query_time_ms,
    )


# ── 5. Drill-across ──────────────────────────────────────────

@app.post("/api/cubes/drill-across", response_model=DrillAcrossResponse)
def drill_across(req: DrillAcrossRequest):
    """
    Drill-across: join Sales and Inventory cubes on shared dimensions
    (Time and/or Product). Returns combined measures from both cubes.
    """
    try:
        sales_cuboid, inv_cuboid, join_cols = resolve_drill_across(
            req.source_cube, req.dimensions
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not join_cols:
        raise HTTPException(
            status_code=400,
            detail="No shared dimension columns to join on. "
                   "Drill-across requires at least Time or Product dimension."
        )

    s_table = sales_cuboid['table']
    i_table = inv_cuboid['table']

    # Build JOIN condition
    join_condition = " AND ".join([f"s.{c} = i.{c}" for c in join_cols])

    # Select all dim cols from sales side + measures from both
    s_dim_cols = ", ".join([f"s.{c}" for c in sales_cuboid['dim_cols']])
    s_measures = ", ".join([f"s.{m}" for m in sales_cuboid['measures']])
    i_measures = ", ".join([f"i.{m}" for m in inv_cuboid['measures']])

    select_parts = [s_dim_cols, s_measures, i_measures]
    select_clause = ", ".join([p for p in select_parts if p])

    # Build WHERE for filters
    all_valid = sales_cuboid['dim_cols'] + sales_cuboid['measures']
    where_clause, where_params = _build_where(req.filters, all_valid, column_prefix="s.")
    params = list(where_params)

    # Build COUNT sql
    count_sql = (
        f"SELECT COUNT(*) "
        f"FROM olap.{s_table} s "
        f"FULL OUTER JOIN olap.{i_table} i ON {join_condition}"
        f"{where_clause}"
    )
    try:
        _, count_result = execute_query(count_sql, where_params if where_params else None)
        total_count = count_result[0][0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Drill-across count query failed: {str(e)}")

    # Build ORDER BY
    order_clause = ""
    if req.sort_by:
        direction = "ASC" if req.sort_order == "asc" else "DESC"
        order_clause = f" ORDER BY s.{req.sort_by} {direction}"
    elif join_cols:
        order_clause = f" ORDER BY {', '.join([f's.{c}' for c in join_cols])}"

    limit_clause = " LIMIT %s OFFSET %s"
    params.extend([req.limit, req.offset])

    sql = (
        f"SELECT {select_clause} "
        f"FROM olap.{s_table} s "
        f"FULL OUTER JOIN olap.{i_table} i ON {join_condition}"
        f"{where_clause}{order_clause}{limit_clause}"
    )

    t0 = time.time()
    try:
        columns, rows = execute_query(sql, params if params else None)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Drill-across failed: {str(e)}")
    query_time_ms = round((time.time() - t0) * 1000, 2)

    serialized_rows = []
    for row in rows:
        serialized_rows.append([
            float(v) if hasattr(v, 'as_tuple') else v
            for v in row
        ])

    return DrillAcrossResponse(
        columns=columns,
        rows=serialized_rows,
        total_count=total_count,
        has_more=req.offset + len(serialized_rows) < total_count,
        sales_cuboid=s_table,
        inventory_cuboid=i_table,
        query_time_ms=query_time_ms,
    )


# ── 6. Dimension values (with search + pagination) ───────────

@app.get("/api/metadata/dim-values", response_model=DimValuesResponse)
def get_dim_values(
    cube: str = Query("sales", description="Cube name"),
    dimension: str = Query(..., description="Dimension name (e.g., 'year', 'customer_type')"),
    column: str = Query(..., description="Column name to fetch values for"),
    search: str | None = Query(None, description="Search text (ILIKE filter)"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Fetch distinct values for a dimension column.
    Supports text search and pagination for large dimensions.
    """
    if cube not in DIM_VALUES_CONFIG:
        raise HTTPException(status_code=404, detail=f"Cube '{cube}' not found")

    config = DIM_VALUES_CONFIG[cube]
    if column not in config:
        raise HTTPException(
            status_code=400,
            detail=f"Column '{column}' not available for cube '{cube}'. "
                   f"Available: {list(config.keys())}"
        )

    table = config[column]['table']
    col = config[column]['column']

    # Count total
    count_params: list = []
    count_sql = f"SELECT COUNT(DISTINCT {col}) FROM {table}"
    if search:
        count_sql += f" WHERE CAST({col} AS TEXT) ILIKE %s"
        count_params.append(f"%{search}%")

    _, count_rows = execute_query(count_sql, count_params if count_params else None)
    total = count_rows[0][0] if count_rows else 0

    # Fetch values
    fetch_params: list = []
    fetch_sql = f"SELECT DISTINCT {col} FROM {table}"
    if search:
        fetch_sql += f" WHERE CAST({col} AS TEXT) ILIKE %s"
        fetch_params.append(f"%{search}%")
    fetch_sql += f" ORDER BY {col} LIMIT %s OFFSET %s"
    fetch_params.extend([limit, offset])

    _, value_rows = execute_query(fetch_sql, fetch_params if fetch_params else None)
    values = [row[0] for row in value_rows]

    return DimValuesResponse(
        values=values,
        total=total,
        limit=limit,
        offset=offset,
    )


# ── 7. Health check ──────────────────────────────────────────

@app.get("/api/health")
def health_check():
    """Health check endpoint."""
    try:
        execute_query("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": str(e)}
