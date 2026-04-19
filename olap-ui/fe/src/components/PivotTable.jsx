import React, { useState, useMemo } from 'react';
import { formatValue, formatColumnName, sortRows } from '../utils/pivotTransform';

export default function PivotTable({
  columns,
  rows,
  dimensionColumns,
  measureColumns,
  cuboidUsed = '',
  totalCount = 0,
  queryTimeMs,
  onCellClick,
  loading,
  hasMore = false,
  onLoadMore,
}) {
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState('asc');

  const handleSort = (colIndex) => {
    if (sortCol === colIndex) {
      setSortDir(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortCol(colIndex);
      setSortDir('asc');
    }
  };

  const sortedRows = useMemo(() => {
    if (sortCol === null) return rows;
    return sortRows(rows, sortCol, sortDir);
  }, [rows, sortCol, sortDir]);

  if (loading) {
    return (
      <div className="pivot-container">
        <div className="table-toolbar">
          <div className="table-title">
            <span>📊</span> Loading...
          </div>
        </div>
        <div className="loading-skeleton">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="skeleton-row" style={{ animationDelay: `${i * 0.1}s` }} />
          ))}
        </div>
      </div>
    );
  }

  if (!columns || columns.length === 0) {
    return (
      <div className="pivot-container">
        <div className="empty-state">
          <span className="empty-state-icon">📊</span>
          <div className="empty-state-title">No Data</div>
          <div className="empty-state-desc">
            Select dimensions from the sidebar and click a query operation to explore your data warehouse.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="pivot-container">
      <div className="table-toolbar">
        <div className="table-title">
          <span>📊</span>
          Query Results
        </div>
        <div className="table-meta">
          <div className="table-meta-item">
            <span className="meta-label">Cuboid:</span>
            <span className="meta-value">{cuboidUsed || '—'}</span>
          </div>
          <div className="table-meta-item">
            <span className="meta-label">Rows:</span>
            <span className="meta-value">{totalCount?.toLocaleString() || 0}</span>
          </div>
          <div className="table-meta-item">
            <span className="meta-label">Time:</span>
            <span className="meta-value">{queryTimeMs || 0}ms</span>
          </div>
        </div>
      </div>

      <div className="pivot-table-wrapper">
        <table className="pivot-table">
          <thead>
            <tr>
              {columns.map((col, i) => {
                const isMeasure = measureColumns?.includes(col);
                const isSorted = sortCol === i;
                return (
                  <th
                    key={col}
                    className={`${isMeasure ? 'measure-col' : ''} ${isSorted ? 'sorted' : ''}`}
                    onClick={() => handleSort(i)}
                  >
                    {formatColumnName(col)}
                    {isSorted && (
                      <span className="sort-arrow">
                        {sortDir === 'asc' ? '▲' : '▼'}
                      </span>
                    )}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row, ri) => (
              <tr key={ri}>
                {row.map((val, ci) => {
                  const col = columns[ci];
                  const isMeasure = measureColumns?.includes(col);
                  const isDim = dimensionColumns?.includes(col);
                  const isClickable = isDim && onCellClick;

                  return (
                    <td
                      key={ci}
                      className={`${isMeasure ? 'measure-cell' : ''} ${isDim ? 'dim-cell' : ''} ${isClickable ? 'clickable' : ''}`}
                      onClick={isClickable ? () => onCellClick(col, val, row, columns) : undefined}
                    >
                      {isMeasure ? formatValue(val, col) : (val ?? '—')}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ── LOAD MORE SECTION ────────────────────────────── */}
      <div className="table-footer" style={{ padding: 'var(--sp-md)', textAlign: 'center', borderTop: '1px solid var(--border)' }}>
        <div style={{ marginBottom: 'var(--sp-sm)', fontSize: '0.85rem', color: 'var(--text-muted)' }}>
          Showing {rows.length.toLocaleString()} of {totalCount ? totalCount.toLocaleString() : '0'} rows
        </div>
        {hasMore && (
          <button 
            className="ops-btn" 
            onClick={onLoadMore}
            disabled={loading}
            style={{ margin: '0.5rem auto 1rem', display: 'block', minWidth: 140, padding: '0.5rem 1rem' }}
          >
            {loading ? 'Loading...' : 'Load More (500)'}
          </button>
        )}
      </div>
    </div>
  );
}
