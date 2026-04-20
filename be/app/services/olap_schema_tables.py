"""Bảng OLAP + tập cột — đồng bộ với `schema olap.sql` trong repo."""
from __future__ import annotations

from typing import Dict, FrozenSet

SALES_MEASURE_COLUMNS: FrozenSet[str] = frozenset({"total_quantity", "sum_amount"})
INVENTORY_MEASURE_COLUMNS: FrozenSet[str] = frozenset({"total_quantity_on_hand"})

SALES_OLAP_TABLE_COLUMNS: Dict[str, FrozenSet[str]] = {
    "olap_sales_all": frozenset() | SALES_MEASURE_COLUMNS,
    "olap_sales_base_info": frozenset(
        {"year", "quarter", "month", "customer_key", "customer_type", "product_key"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_base_loc": frozenset(
        {"year", "quarter", "month", "customer_key", "state", "city", "product_key"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_by_city": frozenset({"state", "city"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_by_customer_info": frozenset({"customer_key", "customer_type"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_by_customer_loc": frozenset({"customer_key", "state", "city"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_by_customer_type": frozenset({"customer_type"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_by_product": frozenset({"product_key"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_by_quarter": frozenset({"year", "quarter"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_by_state": frozenset({"state"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_by_time": frozenset({"year", "quarter", "month"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_by_year": frozenset({"year"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_customer_product_info": frozenset(
        {"customer_key", "customer_type", "product_key"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_customer_product_loc": frozenset(
        {"customer_key", "state", "city", "product_key"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_month_city": frozenset({"year", "quarter", "month", "state", "city"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_month_customer_type": frozenset(
        {"year", "quarter", "month", "customer_type"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_month_product_city": frozenset(
        {"year", "quarter", "month", "product_key", "state", "city"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_month_product_customer_type": frozenset(
        {"year", "quarter", "month", "product_key", "customer_type"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_month_product_state": frozenset(
        {"year", "quarter", "month", "product_key", "state"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_month_state": frozenset({"year", "quarter", "month", "state"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_product_city": frozenset({"product_key", "state", "city"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_product_customer_type": frozenset({"product_key", "customer_type"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_product_state": frozenset({"product_key", "state"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_city": frozenset({"year", "quarter", "state", "city"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_customer_info": frozenset(
        {"year", "quarter", "customer_key", "customer_type"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_customer_loc": frozenset(
        {"year", "quarter", "customer_key", "state", "city"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_customer_type": frozenset({"year", "quarter", "customer_type"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_product": frozenset({"year", "quarter", "product_key"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_product_city": frozenset(
        {"year", "quarter", "product_key", "state", "city"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_product_customer_info": frozenset(
        {"year", "quarter", "product_key", "customer_key", "customer_type"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_product_customer_loc": frozenset(
        {"year", "quarter", "product_key", "customer_key", "state", "city"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_product_customer_type": frozenset(
        {"year", "quarter", "product_key", "customer_type"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_product_state": frozenset({"year", "quarter", "product_key", "state"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_quarter_state": frozenset({"year", "quarter", "state"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_time_customer_info": frozenset(
        {"year", "quarter", "month", "customer_key", "customer_type"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_time_customer_loc": frozenset(
        {"year", "quarter", "month", "customer_key", "state", "city"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_time_product": frozenset({"year", "quarter", "month", "product_key"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_year_city": frozenset({"year", "state", "city"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_year_customer_info": frozenset({"year", "customer_key", "customer_type"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_year_customer_loc": frozenset({"year", "customer_key", "state", "city"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_year_customer_type": frozenset({"year", "customer_type"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_year_product": frozenset({"year", "product_key"}) | SALES_MEASURE_COLUMNS,
    "olap_sales_year_product_city": frozenset({"year", "product_key", "state", "city"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_year_product_customer_info": frozenset(
        {"year", "product_key", "customer_key", "customer_type"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_year_product_customer_loc": frozenset(
        {"year", "product_key", "customer_key", "state", "city"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_year_product_customer_type": frozenset(
        {"year", "product_key", "customer_type"}
    )
    | SALES_MEASURE_COLUMNS,
    "olap_sales_year_product_state": frozenset({"year", "product_key", "state"})
    | SALES_MEASURE_COLUMNS,
    "olap_sales_year_state": frozenset({"year", "state"}) | SALES_MEASURE_COLUMNS,
}

INVENTORY_OLAP_TABLE_COLUMNS: Dict[str, FrozenSet[str]] = {
    "olap_inv_all": frozenset() | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_base": frozenset(
        {"year", "quarter", "month", "product_key", "store_key", "state", "city"}
    )
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_by_city": frozenset({"state", "city"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_by_product": frozenset({"product_key"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_by_quarter": frozenset({"year", "quarter"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_by_state": frozenset({"state"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_by_store": frozenset({"store_key", "state", "city"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_by_time": frozenset({"year", "quarter", "month"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_by_year": frozenset({"year"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_month_city": frozenset({"year", "quarter", "month", "state", "city"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_month_product_city": frozenset(
        {"year", "quarter", "month", "product_key", "state", "city"}
    )
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_month_product_state": frozenset(
        {"year", "quarter", "month", "product_key", "state"}
    )
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_month_state": frozenset({"year", "quarter", "month", "state"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_product_city": frozenset({"product_key", "state", "city"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_product_state": frozenset({"product_key", "state"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_product_store": frozenset({"product_key", "store_key", "state", "city"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_quarter_city": frozenset({"year", "quarter", "state", "city"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_quarter_product": frozenset({"year", "quarter", "product_key"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_quarter_product_city": frozenset(
        {"year", "quarter", "product_key", "state", "city"}
    )
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_quarter_product_state": frozenset({"year", "quarter", "product_key", "state"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_quarter_product_store": frozenset(
        {"year", "quarter", "product_key", "store_key", "city", "state"}
    )
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_quarter_state": frozenset({"year", "quarter", "state"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_quarter_store": frozenset({"year", "quarter", "store_key", "city", "state"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_time_product": frozenset({"year", "quarter", "month", "product_key"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_time_store": frozenset({"year", "quarter", "month", "store_key", "state", "city"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_year_city": frozenset({"year", "state", "city"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_year_product": frozenset({"year", "product_key"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_year_product_city": frozenset({"year", "product_key", "state", "city"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_year_product_state": frozenset({"year", "product_key", "state"})
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_year_product_store": frozenset(
        {"year", "product_key", "store_key", "city", "state"}
    )
    | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_year_state": frozenset({"year", "state"}) | INVENTORY_MEASURE_COLUMNS,
    "olap_inv_year_store": frozenset({"year", "store_key", "city", "state"})
    | INVENTORY_MEASURE_COLUMNS,
}
