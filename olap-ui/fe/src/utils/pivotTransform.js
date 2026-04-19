/**
 * pivotTransform.js — Client-side utility to transform flat query results
 * into a pivot-table-friendly structure (swap rows ↔ columns).
 */

/**
 * Format a numeric value for display.
 * @param {number|null} value
 * @param {string} columnName - used to determine formatting
 */
export function formatValue(value, columnName) {
  if (value === null || value === undefined) return '—';

  if (typeof value === 'number') {
    if (columnName.includes('amount') || columnName.includes('sum_amount')) {
      return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
      }).format(value);
    }
    return new Intl.NumberFormat('en-US').format(value);
  }

  return String(value);
}

/**
 * Build a descriptive label for a dimension level.
 */
export function buildDimLabel(dimension, level) {
  const labels = {
    time: { year: 'Year', quarter: 'Quarter', month: 'Month' },
    customer: {
      state: 'State',
      city: 'City',
      customer_type: 'Customer Type',
      customer_info: 'Customer (Info)',
      customer_loc: 'Customer (Location)',
    },
    product: { product: 'Product' },
    store: {
      state: 'State',
      city: 'City',
      store: 'Store',
    },
  };
  return labels[dimension]?.[level] || `${dimension}/${level}`;
}

/**
 * Build a descriptive label for a row value from dimension columns.
 * @param {object} row - flat row object { year: 2026, quarter: 1, ... }
 * @param {string[]} dimCols - dimension column names
 */
export function buildRowLabel(row, dimCols) {
  return dimCols.map(col => row[col] ?? '—').join(' · ');
}

/**
 * Transform flat tabular data into a pivoted structure.
 *
 * @param {string[]} columns - all column names
 * @param {any[][]} rows - all data rows
 * @param {string[]} rowDims - column names to put on rows
 * @param {string[]} colDims - column names to put on columns
 * @param {string[]} measures - measure column names
 *
 * @returns {{
 *   pivotHeaders: string[],
 *   pivotRows: any[][],
 *   rowLabels: string[],
 * }}
 */
export function pivotData(columns, rows, rowDims, colDims, measures) {
  if (!colDims || colDims.length === 0) {
    // No pivot — just return data as-is organized by rowDims + measures
    return {
      pivotHeaders: [...rowDims, ...measures],
      pivotRows: rows.map(row => {
        const obj = {};
        columns.forEach((c, i) => { obj[c] = row[i]; });
        return [...rowDims.map(d => obj[d]), ...measures.map(m => obj[m])];
      }),
      rowLabels: rowDims,
    };
  }

  // Build row-indexed structure
  const rowMap = new Map();
  const colValuesSet = new Set();

  rows.forEach(row => {
    const obj = {};
    columns.forEach((c, i) => { obj[c] = row[i]; });

    const rowKey = rowDims.map(d => obj[d] ?? '').join('|||');
    const colKey = colDims.map(d => obj[d] ?? '').join('|||');

    colValuesSet.add(colKey);

    if (!rowMap.has(rowKey)) {
      rowMap.set(rowKey, { dimValues: rowDims.map(d => obj[d]), measures: {} });
    }
    const entry = rowMap.get(rowKey);
    entry.measures[colKey] = measures.map(m => obj[m]);
  });

  const colValues = [...colValuesSet].sort();

  // Build headers: rowDims... then for each colValue: measure1, measure2...
  const pivotHeaders = [...rowDims];
  colValues.forEach(cv => {
    const label = cv.replace(/\|\|\|/g, ' · ');
    measures.forEach(m => {
      pivotHeaders.push(`${label} — ${m}`);
    });
  });

  // Build rows
  const pivotRows = [];
  for (const [, entry] of rowMap) {
    const row = [...entry.dimValues];
    colValues.forEach(cv => {
      const vals = entry.measures[cv] || measures.map(() => null);
      row.push(...vals);
    });
    pivotRows.push(row);
  }

  return { pivotHeaders, pivotRows, rowLabels: rowDims };
}

/**
 * Sort rows by a specific column index.
 */
export function sortRows(rows, colIndex, direction = 'asc') {
  return [...rows].sort((a, b) => {
    const va = a[colIndex];
    const vb = b[colIndex];
    if (va === null || va === undefined) return 1;
    if (vb === null || vb === undefined) return -1;
    if (typeof va === 'number' && typeof vb === 'number') {
      return direction === 'asc' ? va - vb : vb - va;
    }
    const cmp = String(va).localeCompare(String(vb));
    return direction === 'asc' ? cmp : -cmp;
  });
}

/**
 * Determine column display name from raw column name.
 */
export function formatColumnName(col) {
  const map = {
    year: 'Year',
    quarter: 'Quarter',
    month: 'Month',
    product_key: 'Product',
    customer_key: 'Customer',
    customer_type: 'Cust. Type',
    store_key: 'Store',
    state: 'State',
    city: 'City',
    total_quantity: 'Qty Sold',
    sum_amount: 'Revenue',
    total_quantity_on_hand: 'Qty On Hand',
    product_desc: 'Product Desc.',
    product_size: 'Size',
    product_weight: 'Weight',
    customer_name: 'Customer Name',
    store_phone: 'Phone',
    quantity_ordered: 'Qty Ordered',
    total_amount: 'Amount',
    quantity_on_hand: 'Qty On Hand',
    time_key: 'Time Key',
  };
  return map[col] || col.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}
