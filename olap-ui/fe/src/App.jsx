import React, { useReducer, useEffect, useCallback, useState } from 'react';
import CubeSelector from './components/CubeSelector';
import DimensionPanel from './components/DimensionPanel';
import FilterPanel from './components/FilterPanel';
import OperationToolbar from './components/OperationToolbar';
import PivotTable from './components/PivotTable';
import ChartPanel from './components/ChartPanel';
import QueryHistory from './components/QueryHistory';
import { fetchCubes, queryOLAP, drillThrough, drillAcross } from './api/olap';
import { buildDimLabel, pivotData } from './utils/pivotTransform';

// ═══════════════════════════════════════════════════════════════
// State management with useReducer
// ═══════════════════════════════════════════════════════════════

const initialState = {
  // Cube metadata
  cubesMeta: [],
  cube: 'sales',

  // Active dimensions and filters
  dimensions: [],
  filters: [],

  // Query result
  data: null,
  loading: false,
  error: null,

  // Navigation history (stack for drill-down/roll-up)
  history: [],

  // View mode
  viewMode: 'both',

  // Drill-through modal
  drillThroughData: null,
  drillThroughLoading: false,
  drillThroughPage: 0,
  drillThroughHasMore: false,
  drillThroughTotalCount: 0,

  // Drill-across data
  drillAcrossData: null,

  // Pagination
  page: 0,
  hasMore: false,
  pageSize: 500,
};

function reducer(state, action) {
  switch (action.type) {
    case 'SET_CUBES_META':
      return { ...state, cubesMeta: action.payload };

    case 'SET_CUBE':
      return {
        ...state,
        cube: action.payload,
        dimensions: [],
        filters: [],
        data: null,
        history: [],
        drillAcrossData: null,
        error: null,
        page: 0,
        hasMore: false,
      };

    case 'SET_DIMENSIONS':
      return { ...state, dimensions: action.payload };

    case 'SET_FILTERS':
      return { ...state, filters: action.payload };

    case 'QUERY_START':
      return { ...state, loading: true, error: null };

    case 'QUERY_SUCCESS':
      return { 
        ...state, 
        loading: false, 
        data: action.payload, 
        error: null,
        page: 0,
        hasMore: action.payload.has_more,
      };

    case 'QUERY_ERROR':
      return { ...state, loading: false, error: action.payload };

    case 'LOAD_MORE_SUCCESS':
      return {
        ...state,
        loading: false,
        data: {
          ...action.payload,
          rows: [...state.data.rows, ...action.payload.rows],
        },
        page: state.page + 1,
        hasMore: action.payload.has_more,
      };

    case 'PUSH_HISTORY':
      return {
        ...state,
        history: [...state.history, action.payload],
      };

    case 'JUMP_TO_HISTORY': {
      const idx = action.payload;
      const entry = state.history[idx];
      return {
        ...state,
        cube: entry.cube,
        dimensions: entry.dimensions,
        filters: entry.filters,
        history: state.history.slice(0, idx),
        drillAcrossData: null,
        page: 0,
        hasMore: false,
      };
    }

    case 'SET_VIEW_MODE':
      return { ...state, viewMode: action.payload };

    case 'DRILL_THROUGH_START':
      return { ...state, drillThroughLoading: true };

    case 'DRILL_THROUGH_SUCCESS':
      return { 
        ...state, 
        drillThroughLoading: false, 
        drillThroughData: action.payload,
        drillThroughPage: 0,
        drillThroughHasMore: action.payload.has_more,
        drillThroughTotalCount: action.payload.total_count,
      };

    case 'LOAD_MORE_DRILL_THROUGH':
      return {
        ...state,
        drillThroughLoading: false,
        drillThroughData: {
          ...action.payload,
          rows: [...state.drillThroughData.rows, ...action.payload.rows],
        },
        drillThroughPage: state.drillThroughPage + 1,
        drillThroughHasMore: action.payload.has_more,
      };

    case 'DRILL_THROUGH_CLOSE':
      return { 
        ...state, 
        drillThroughData: null, 
        drillThroughLoading: false,
        drillThroughPage: 0,
        drillThroughHasMore: false,
      };

    case 'DRILL_ACROSS_SUCCESS':
      return { ...state, drillAcrossData: action.payload, loading: false };

    case 'DRILL_ACROSS_CLEAR':
      return { ...state, drillAcrossData: null };

    default:
      return state;
  }
}

// ═══════════════════════════════════════════════════════════════
// Hierarchy helpers (client-side)
// ═══════════════════════════════════════════════════════════════

const TIME_HIERARCHY = ['year', 'quarter', 'month'];
const CUSTOMER_HIERARCHY_INFO = ['state', 'city', 'customer_type', 'customer_info'];
const CUSTOMER_HIERARCHY_LOC = ['state', 'city', 'customer_loc'];
const STORE_HIERARCHY = ['state', 'city', 'store'];

function getHierarchyDown(dimension, currentLevel) {
  let hier;
  if (dimension === 'time') hier = TIME_HIERARCHY;
  else if (dimension === 'customer') {
    if (['customer_info', 'customer_type'].includes(currentLevel)) {
      hier = CUSTOMER_HIERARCHY_INFO;
    } else {
      hier = CUSTOMER_HIERARCHY_LOC;
    }
  }
  else if (dimension === 'store') hier = STORE_HIERARCHY;
  else return null;

  const idx = hier.indexOf(currentLevel);
  return idx >= 0 && idx < hier.length - 1 ? hier[idx + 1] : null;
}

function getHierarchyUp(dimension, currentLevel) {
  let hier;
  if (dimension === 'time') hier = TIME_HIERARCHY;
  else if (dimension === 'customer') {
    if (['customer_info', 'customer_type'].includes(currentLevel)) {
      hier = CUSTOMER_HIERARCHY_INFO;
    } else {
      hier = CUSTOMER_HIERARCHY_LOC;
    }
  }
  else if (dimension === 'store') hier = STORE_HIERARCHY;
  else return null;

  const idx = hier.indexOf(currentLevel);
  return idx > 0 ? hier[idx - 1] : null;
}

// ═══════════════════════════════════════════════════════════════
// App Component
// ═══════════════════════════════════════════════════════════════

export default function App() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const {
    cubesMeta, cube, dimensions, filters,
    data, loading, error, history,
    viewMode, drillThroughData, drillThroughLoading,
    drillAcrossData, page, hasMore, pageSize,
    drillThroughPage, drillThroughHasMore, drillThroughTotalCount,
  } = state;

  // ── Load cube metadata ──────────────────────────────────
  useEffect(() => {
    fetchCubes()
      .then(cubes => dispatch({ type: 'SET_CUBES_META', payload: cubes }))
      .catch(err => dispatch({ type: 'QUERY_ERROR', payload: err.message }));
  }, []);

  // ── Get current cube metadata ───────────────────────────
  const currentCubeMeta = cubesMeta.find(c => c.name === cube);
  const dimensionsMeta = currentCubeMeta?.dimensions || [];

  // ── Execute query ───────────────────────────────────────
  const executeQuery = useCallback(async (dims = dimensions, fils = filters) => {
    dispatch({ type: 'QUERY_START' });
    dispatch({ type: 'DRILL_ACROSS_CLEAR' });
    try {
      const result = await queryOLAP(cube, {
        dimensions: dims,
        filters: fils,
        limit: pageSize,
        offset: 0,
      });
      dispatch({ type: 'QUERY_SUCCESS', payload: result });
    } catch (err) {
      dispatch({ type: 'QUERY_ERROR', payload: err.message });
    }
  }, [cube, dimensions, filters, pageSize]);

  // ── Auto-query when dimensions or filters change ────────
  useEffect(() => {
    // Only auto-query if we have cube metadata loaded
    if (cubesMeta.length > 0) {
      executeQuery();
    }
  }, [cube, dimensions, filters, cubesMeta.length]);

  // ── Cube selection ──────────────────────────────────────
  const handleSelectCube = (cubeName) => {
    dispatch({ type: 'SET_CUBE', payload: cubeName });
  };

  // ── Dimension toggle ────────────────────────────────────
  const handleToggleDimension = (dimName, defaultLevel) => {
    const exists = dimensions.find(d => d.dimension === dimName);
    if (exists) {
      const newDims = dimensions.filter(d => d.dimension !== dimName);
      dispatch({ type: 'SET_DIMENSIONS', payload: newDims });
      // Also remove related filters
      const dimMeta = dimensionsMeta.find(d => d.name === dimName);
      if (dimMeta) {
        const dimCols = dimMeta.levels.flatMap(l => l.columns);
        const newFilters = filters.filter(f => !dimCols.includes(f.column));
        dispatch({ type: 'SET_FILTERS', payload: newFilters });
      }
    } else {
      const newDims = [...dimensions, { dimension: dimName, level: defaultLevel }];
      dispatch({ type: 'SET_DIMENSIONS', payload: newDims });
    }
  };

  // ── Change dimension level ──────────────────────────────
  const handleChangeLevel = (dimName, newLevel) => {
    const newDims = dimensions.map(d =>
      d.dimension === dimName ? { ...d, level: newLevel } : d
    );
    dispatch({ type: 'SET_DIMENSIONS', payload: newDims });
  };

  // ── Filter operations ──────────────────────────────────
  const handleAddFilter = (filter) => {
    const newFilters = filters.filter(f => f.column !== filter.column);
    newFilters.push(filter);
    dispatch({ type: 'SET_FILTERS', payload: newFilters });
  };

  const handleRemoveFilter = (column) => {
    dispatch({ type: 'SET_FILTERS', payload: filters.filter(f => f.column !== column) });
  };

  // ── DRILL DOWN (all drillable dimensions at once) ────────
  const handleDrillDown = () => {
    // Check if ANY dimension can drill down
    const hasDrillable = dimensions.some(d => getHierarchyDown(d.dimension, d.level));
    if (!hasDrillable) return;

    // Push current state to history
    dispatch({
      type: 'PUSH_HISTORY',
      payload: { cube, dimensions: [...dimensions], filters: [...filters] },
    });

    // Drill ALL drillable dimensions one level down
    const newDims = dimensions.map(d => {
      const nextLevel = getHierarchyDown(d.dimension, d.level);
      return nextLevel ? { ...d, level: nextLevel } : d;
    });
    dispatch({ type: 'SET_DIMENSIONS', payload: newDims });
  };

  // ── ROLL UP ─────────────────────────────────────────────
  const handleRollUp = () => {
    // If we have history, pop from it (restores exact previous state)
    if (history.length > 0) {
      dispatch({ type: 'JUMP_TO_HISTORY', payload: history.length - 1 });
      return;
    }

    // No history — roll ALL rollable dimensions up one level.
    // Do NOT push to history (no prior state to return to).
    const hasRollable = dimensions.some(d => getHierarchyUp(d.dimension, d.level));
    if (!hasRollable) return;

    const newDims = dimensions.map(d => {
      const upLevel = getHierarchyUp(d.dimension, d.level);
      return upLevel ? { ...d, level: upLevel } : d;
    });
    dispatch({ type: 'SET_DIMENSIONS', payload: newDims });
  };

  // ── PIVOT ───────────────────────────────────────────────
  const [pivoted, setPivoted] = useState(false);
  const handlePivot = () => {
    setPivoted(prev => !prev);
  };

  // ── DRILL ACROSS ────────────────────────────────────────
  const handleDrillAcross = async () => {
    const sharedDims = dimensions.filter(d => d.dimension === 'time' || d.dimension === 'product');
    if (sharedDims.length === 0) return;

    dispatch({ type: 'QUERY_START' });
    try {
      const result = await drillAcross({
        source_cube: cube,
        dimensions: sharedDims,
        filters,
      });
      dispatch({ type: 'DRILL_ACROSS_SUCCESS', payload: result });
    } catch (err) {
      dispatch({ type: 'QUERY_ERROR', payload: err.message });
    }
  };

  // ── DRILL THROUGH ───────────────────────────────────────
  const handleDrillThrough = async () => {
    dispatch({ type: 'DRILL_THROUGH_START' });
    try {
      const result = await drillThrough(cube, {
        filters,
        limit: 500,
        offset: 0,
      });
      dispatch({ type: 'DRILL_THROUGH_SUCCESS', payload: result });
    } catch (err) {
      dispatch({ type: 'QUERY_ERROR', payload: err.message });
      dispatch({ type: 'DRILL_THROUGH_CLOSE' });
    }
  };

  const handleDrillThroughLoadMore = async () => {
    if (!drillThroughHasMore || drillThroughLoading) return;
    dispatch({ type: 'DRILL_THROUGH_START' });
    try {
      const nextPage = drillThroughPage + 1;
      const result = await drillThrough(cube, {
        filters,
        limit: 500,
        offset: nextPage * 500,
      });
      dispatch({ type: 'LOAD_MORE_DRILL_THROUGH', payload: result });
    } catch (err) {
      dispatch({ type: 'QUERY_ERROR', payload: err.message });
    }
  };

  // ── LOAD MORE ───────────────────────────────────────────
  const handleLoadMore = useCallback(async () => {
    if (!hasMore || loading) return;
    dispatch({ type: 'QUERY_START' });
    try {
      const result = await queryOLAP(cube, {
        dimensions,
        filters,
        limit: pageSize,
        offset: (page + 1) * pageSize,
      });
      dispatch({ type: 'LOAD_MORE_SUCCESS', payload: result });
    } catch (err) {
      dispatch({ type: 'QUERY_ERROR', payload: err.message });
    }
  }, [cube, dimensions, filters, page, hasMore, loading, pageSize]);

  // ── CELL CLICK (Drill Down into specific value) ─────────
  const handleCellClick = (column, value, row, allColumns) => {
    if (value === null || value === undefined || value === '—') return;

    // Push current state to history
    dispatch({
      type: 'PUSH_HISTORY',
      payload: { cube, dimensions: [...dimensions], filters: [...filters] },
    });

    // Add filter for the clicked value
    const newFilter = { column, operator: 'eq', values: [value] };
    const newFilters = [...filters.filter(f => f.column !== column), newFilter];

    // Find which dimension this column belongs to and drill down
    let newDims = [...dimensions];
    for (const dim of dimensions) {
      const dimMeta = dimensionsMeta.find(d => d.name === dim.dimension);
      if (!dimMeta) continue;
      const lvl = dimMeta.levels.find(l => l.level === dim.level);
      if (!lvl) continue;
      if (lvl.columns.includes(column)) {
        const nextLevel = getHierarchyDown(dim.dimension, dim.level);
        if (nextLevel) {
          newDims = newDims.map(d =>
            d.dimension === dim.dimension ? { ...d, level: nextLevel } : d
          );
        }
        break;
      }
    }

    dispatch({ type: 'SET_DIMENSIONS', payload: newDims });
    dispatch({ type: 'SET_FILTERS', payload: newFilters });
  };

  // ── History navigation ──────────────────────────────────
  const handleJumpToHistory = (idx) => {
    dispatch({ type: 'JUMP_TO_HISTORY', payload: idx });
  };

  // ── Capability checks ──────────────────────────────────
  const canDrillDown = dimensions.some(d => getHierarchyDown(d.dimension, d.level));
  const canRollUp = history.length > 0 || dimensions.some(d => getHierarchyUp(d.dimension, d.level));
  const canPivot = dimensions.length >= 2;
  const canDrillAcross = dimensions.some(d => d.dimension === 'time' || d.dimension === 'product');
  const canDrillThrough = data && data.rows && data.rows.length > 0;

  // ── Determine which data to show ────────────────────────
  const displayData = drillAcrossData || data;
  const displayColumns = displayData?.columns || [];
  const displayRows = displayData?.rows || [];
  const displayDimCols = displayData?.dimension_columns || displayColumns.filter(c =>
    !['total_quantity', 'sum_amount', 'total_quantity_on_hand'].includes(c)
  );
  const displayMeasureCols = displayData?.measure_columns || displayColumns.filter(c =>
    ['total_quantity', 'sum_amount', 'total_quantity_on_hand'].includes(c)
  );

  // ── Pivot logic ─────────────────────────────────────────
  let finalColumns = displayColumns;
  let finalRows = displayRows;
  let finalDimCols = displayDimCols;
  let finalMeasureCols = displayMeasureCols;

  if (pivoted && displayDimCols.length >= 2 && displayRows.length > 0) {
    // Dùng dim cuối làm column dimension (pivot axis)
    // Các dim còn lại làm row dimensions
    const rowDims = displayDimCols.slice(0, -1);
    const colDims = displayDimCols.slice(-1);

    const { pivotHeaders, pivotRows } = pivotData(
      displayColumns,
      displayRows,
      rowDims,
      colDims,
      displayMeasureCols
    );
    finalColumns = pivotHeaders;
    finalRows = pivotRows;
    finalDimCols = rowDims;
    finalMeasureCols = pivotHeaders.filter(h => !rowDims.includes(h));
  }

  // ── Breadcrumb ──────────────────────────────────────────
  const breadcrumbItems = [
    { label: cube === 'sales' ? '💰 Sales' : '📦 Inventory', idx: -1 },
    ...history.map((entry, i) => ({
      label: entry.dimensions.length === 0
        ? 'ALL'
        : entry.dimensions.map(d => buildDimLabel(d.dimension, d.level)).join(' × '),
      idx: i,
    })),
    {
      label: dimensions.length === 0
        ? 'ALL'
        : dimensions.map(d => buildDimLabel(d.dimension, d.level)).join(' × '),
      idx: null,
      active: true,
    },
  ];

  return (
    <div className="app-layout">
      {/* ── Header ──────────────────────────────────────── */}
      <header className="app-header">
        <div className="logo-group">
          <div className="logo-icon">◆</div>
          <div>
            <div className="logo-text">OLAP Explorer</div>
            <div className="logo-subtitle">Data Warehouse Analytics</div>
          </div>
        </div>

        <div className="breadcrumb" id="breadcrumb">
          {breadcrumbItems.map((item, i) => (
            <React.Fragment key={i}>
              {i > 0 && <span className="breadcrumb-separator">›</span>}
              <span
                className={`breadcrumb-item ${item.active ? 'active' : ''}`}
                onClick={item.idx !== null && !item.active ? () => handleJumpToHistory(item.idx) : undefined}
              >
                {item.label}
              </span>
            </React.Fragment>
          ))}
        </div>

        <CubeSelector
          cubes={cubesMeta}
          activeCube={cube}
          onSelect={handleSelectCube}
        />
      </header>

      {/* ── Sidebar ─────────────────────────────────────── */}
      <aside className="app-sidebar">
        <DimensionPanel
          dimensions={dimensionsMeta}
          activeDimensions={dimensions}
          onToggleDimension={handleToggleDimension}
          onChangeLevel={handleChangeLevel}
        />

        <FilterPanel
          cube={cube}
          activeDimensions={dimensions}
          filters={filters}
          onAddFilter={handleAddFilter}
          onRemoveFilter={handleRemoveFilter}
          dimensionsMeta={dimensionsMeta}
        />

        <QueryHistory
          history={history}
          onJumpTo={handleJumpToHistory}
        />
      </aside>

      {/* ── Main Content ────────────────────────────────── */}
      <main className="app-main">
        <OperationToolbar
          onDrillDown={handleDrillDown}
          onRollUp={handleRollUp}
          onPivot={handlePivot}
          onDrillAcross={handleDrillAcross}
          onDrillThrough={handleDrillThrough}
          canDrillDown={canDrillDown}
          canRollUp={canRollUp}
          canPivot={canPivot}
          canDrillAcross={canDrillAcross}
          canDrillThrough={canDrillThrough}
          activeCube={cube}
          viewMode={viewMode}
          onViewModeChange={(mode) => dispatch({ type: 'SET_VIEW_MODE', payload: mode })}
        />

        {error && (
          <div style={{
            background: 'rgba(244, 63, 94, 0.1)',
            border: '1px solid rgba(244, 63, 94, 0.3)',
            borderRadius: 'var(--radius-md)',
            padding: 'var(--sp-md) var(--sp-lg)',
            color: 'var(--accent-rose)',
            fontSize: '0.85rem',
          }}>
            ⚠ {error}
          </div>
        )}

        {drillAcrossData && (
          <div style={{
            background: 'rgba(99, 102, 241, 0.1)',
            border: '1px solid rgba(99, 102, 241, 0.3)',
            borderRadius: 'var(--radius-md)',
            padding: 'var(--sp-sm) var(--sp-lg)',
            color: 'var(--accent-indigo)',
            fontSize: '0.82rem',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}>
            <span>
              ↔ Drill Across: <strong>{drillAcrossData.sales_cuboid}</strong> ⟷ <strong>{drillAcrossData.inventory_cuboid}</strong>
            </span>
            <button
              onClick={() => dispatch({ type: 'DRILL_ACROSS_CLEAR' })}
              style={{
                background: 'none', border: 'none', color: 'var(--accent-indigo)',
                cursor: 'pointer', fontSize: '0.9rem',
              }}
            >
              ✕ Clear
            </button>
          </div>
        )}

        {(viewMode === 'table' || viewMode === 'both') && (
          <PivotTable
            columns={finalColumns}
            rows={finalRows}
            dimensionColumns={finalDimCols}
            measureColumns={finalMeasureCols}
            cuboidUsed={drillAcrossData
              ? `${drillAcrossData.sales_cuboid} ⟷ ${drillAcrossData.inventory_cuboid}`
              : data?.cuboid_used
            }
            totalCount={displayData?.total_count}
            hasMore={hasMore}
            onLoadMore={handleLoadMore}
            queryTimeMs={displayData?.query_time_ms}
            onCellClick={handleCellClick}
            loading={loading}
          />
        )}

        {(viewMode === 'chart' || viewMode === 'both') && (
          <ChartPanel
            columns={finalColumns}
            rows={finalRows}
            dimensionColumns={finalDimCols}
            measureColumns={finalMeasureCols}
          />
        )}
      </main>

      {/* ── Footer ──────────────────────────────────────── */}
      <footer className="app-footer">
        <span>OLAP Explorer v1.0 — Data Warehouse Analytics</span>
        <span>
          {pivoted && <span className="badge warning" style={{ marginRight: 8 }}>PIVOTED</span>}
          {data?.cuboid_used && (
            <span className="badge info">Cuboid: {data.cuboid_used}</span>
          )}
        </span>
      </footer>

      {/* ── Drill-Through Modal ─────────────────────────── */}
      {(drillThroughData || drillThroughLoading) && (
        <div className="modal-overlay" onClick={() => dispatch({ type: 'DRILL_THROUGH_CLOSE' })}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <div className="modal-title">
                🔍 Drill Through — {cube === 'sales' ? 'Fact_Sales' : 'Fact_Inventory'} Detail Records
              </div>
              <button
                className="modal-close"
                onClick={() => dispatch({ type: 'DRILL_THROUGH_CLOSE' })}
              >
                ✕
              </button>
            </div>
            <div className="modal-body">
              {drillThroughLoading && (
                <div className="loading-skeleton">
                  {Array.from({ length: 5 }).map((_, i) => (
                    <div key={i} className="skeleton-row" style={{ animationDelay: `${i * 0.1}s` }} />
                  ))}
                </div>
              )}
              {drillThroughData && (
                <PivotTable
                  columns={drillThroughData.columns}
                  rows={drillThroughData.rows}
                  dimensionColumns={[]}
                  measureColumns={[]}
                  totalCount={drillThroughTotalCount}
                  queryTimeMs={drillThroughData.query_time_ms}
                  loading={false}
                  hasMore={drillThroughHasMore}
                  onLoadMore={handleDrillThroughLoadMore}
                />
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
