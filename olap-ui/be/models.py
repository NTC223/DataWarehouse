"""
models.py — Pydantic request/response models for the OLAP API.
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal, Any


# ── Request Models ────────────────────────────────────────────

class DimensionLevel(BaseModel):
    """
    Describes one active dimension and its current hierarchy level.

    dimension: 'time', 'customer', 'product', 'store'
    level:
      - Time:     'year', 'quarter', 'month'
      - Customer: 'customer_info', 'customer_loc', 'customer_type', 'city', 'state'
      - Product:  'product'
      - Store:    'store', 'city', 'state'
    """
    dimension: str
    level: str


class FilterCondition(BaseModel):
    """
    A filter on a specific column.
    Used for both Slice (single value) and Dice (multiple values).
    """
    column: str
    operator: Literal["eq", "in", "between"] = "in"
    values: list[Any]


class OLAPQueryRequest(BaseModel):
    """
    Main query request for OLAP operations.
    The backend resolves the appropriate cuboid based on dimensions.
    """
    cube: Literal["sales", "inventory"]
    dimensions: list[DimensionLevel] = Field(default_factory=list)
    filters: list[FilterCondition] = Field(default_factory=list)
    sort_by: Optional[str] = None
    sort_order: Literal["asc", "desc"] = "asc"
    limit: int = 500
    offset: int = 0


class DrillThroughRequest(BaseModel):
    """
    Drill-through request — queries the DWH fact table directly
    for detailed records.
    """
    cube: Literal["sales", "inventory"]
    filters: list[FilterCondition] = Field(default_factory=list)
    limit: int = 100
    offset: int = 0


class DrillAcrossRequest(BaseModel):
    """
    Drill-across request — joins two cubes on shared dimensions.
    source_cube is the cube being viewed; we join in the other cube.
    """
    source_cube: Literal["sales", "inventory"]
    dimensions: list[DimensionLevel] = Field(default_factory=list)
    filters: list[FilterCondition] = Field(default_factory=list)
    sort_by: Optional[str] = None
    sort_order: Literal["asc", "desc"] = "asc"
    limit: int = 500
    offset: int = 0


class DimValuesParams(BaseModel):
    """
    Query parameters for fetching distinct dimension values.
    Supports search text and pagination for performance.
    """
    dimension: str
    column: str
    cube: Literal["sales", "inventory"] = "sales"
    search: Optional[str] = None
    limit: int = 50
    offset: int = 0


# ── Response Models ───────────────────────────────────────────

class OLAPQueryResponse(BaseModel):
    """Response for a standard OLAP query."""
    columns: list[str]
    dimension_columns: list[str]
    measure_columns: list[str]
    rows: list[list[Any]]
    total_count: int
    has_more: bool = False
    cuboid_used: str
    query_time_ms: float


class DrillThroughResponse(BaseModel):
    """Response for drill-through queries."""
    columns: list[str]
    rows: list[list[Any]]
    total_count: int
    has_more: bool = False
    query_time_ms: float


class DrillAcrossResponse(BaseModel):
    """Response for drill-across queries."""
    columns: list[str]
    rows: list[list[Any]]
    total_count: int
    has_more: bool = False
    sales_cuboid: str
    inventory_cuboid: str
    query_time_ms: float


class DimValuesResponse(BaseModel):
    """Response for dimension value lookup."""
    values: list[Any]
    total: int
    limit: int
    offset: int


class CubeInfo(BaseModel):
    """Metadata about a cube."""
    name: str
    display_name: str
    fact_table: str
    measures: list[dict[str, str]]
    dimensions: list[dict[str, Any]]


class ErrorResponse(BaseModel):
    """Standard error response."""
    detail: str
