import React, { useState, useEffect, useCallback } from 'react';
import { fetchDimValues } from '../api/olap';

export default function FilterPanel({
  cube,
  activeDimensions,
  filters,
  onAddFilter,
  onRemoveFilter,
  dimensionsMeta,
}) {
  // Only show filter options for active dimensions
  const filterableCols = [];
  activeDimensions.forEach(ad => {
    const dimMeta = dimensionsMeta.find(d => d.name === ad.dimension);
    if (!dimMeta) return;
    const lvl = dimMeta.levels.find(l => l.level === ad.level);
    if (!lvl) return;
    lvl.columns.forEach(col => {
      if (!filterableCols.find(f => f.column === col)) {
        filterableCols.push({ column: col, dimension: ad.dimension, level: ad.level });
      }
    });
  });

  return (
    <div className="sidebar-section">
      <div className="sidebar-section-title">Filters (Slice / Dice)</div>
      {filterableCols.length === 0 && (
        <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', padding: '8px 0' }}>
          Enable a dimension to add filters
        </div>
      )}
      {filterableCols.map(fc => (
        <FilterGroup
          key={fc.column}
          cube={cube}
          column={fc.column}
          dimension={fc.dimension}
          currentFilter={filters.find(f => f.column === fc.column)}
          onAddFilter={onAddFilter}
          onRemoveFilter={onRemoveFilter}
        />
      ))}
    </div>
  );
}

function FilterGroup({ cube, column, dimension, currentFilter, onAddFilter, onRemoveFilter }) {
  const [search, setSearch] = useState('');
  const [values, setValues] = useState([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [selectedValues, setSelectedValues] = useState(currentFilter?.values || []);

  const loadValues = useCallback(async (searchText) => {
    setLoading(true);
    try {
      const res = await fetchDimValues({
        cube,
        dimension,
        column,
        search: searchText || '',
        limit: 50,
        offset: 0,
      });
      setValues(res.values);
    } catch (err) {
      console.error('Failed to load dim values:', err);
    } finally {
      setLoading(false);
    }
  }, [cube, dimension, column]);

  useEffect(() => {
    if (expanded) {
      loadValues(search);
    }
  }, [expanded, search, loadValues]);

  useEffect(() => {
    setSelectedValues(currentFilter?.values || []);
  }, [currentFilter]);

  const handleToggleValue = (val) => {
    const newSelected = selectedValues.includes(val)
      ? selectedValues.filter(v => v !== val)
      : [...selectedValues, val];
    setSelectedValues(newSelected);

    if (newSelected.length > 0) {
      onAddFilter({ column, operator: 'in', values: newSelected });
    } else {
      onRemoveFilter(column);
    }
  };

  const hasFilter = selectedValues.length > 0;

  return (
    <div className="filter-group" id={`filter-${column}`}>
      <div className="filter-group-header">
        <div
          className="filter-group-title"
          onClick={() => setExpanded(!expanded)}
          style={{ cursor: 'pointer' }}
        >
          <span>{expanded ? '▾' : '▸'}</span>
          <span>{column.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}</span>
          {hasFilter && (
            <span className="badge info">{selectedValues.length}</span>
          )}
        </div>
        {hasFilter && (
          <button
            className="filter-remove-btn"
            onClick={() => {
              setSelectedValues([]);
              onRemoveFilter(column);
            }}
            title="Clear filter"
          >
            ✕
          </button>
        )}
      </div>

      {expanded && (
        <>
          <input
            type="text"
            className="filter-search-input"
            placeholder={`Search ${column}...`}
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
          <div className="filter-values-list">
            {loading && <div className="text-muted" style={{ padding: '8px', fontSize: '0.78rem' }}>Loading...</div>}
            {!loading && values.length === 0 && (
              <div className="text-muted" style={{ padding: '8px', fontSize: '0.78rem' }}>No values found</div>
            )}
            {!loading && values.map(val => (
              <label
                key={String(val)}
                className={`filter-value-item ${selectedValues.includes(val) ? 'selected' : ''}`}
              >
                <input
                  type="checkbox"
                  checked={selectedValues.includes(val)}
                  onChange={() => handleToggleValue(val)}
                />
                <span>{String(val)}</span>
              </label>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
