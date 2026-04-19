import React from 'react';

export default function OperationToolbar({
  onDrillDown,
  onRollUp,
  onPivot,
  onDrillAcross,
  onDrillThrough,
  canDrillDown,
  canRollUp,
  canPivot,
  canDrillAcross,
  canDrillThrough,
  activeCube,
  viewMode,
  onViewModeChange,
}) {
  return (
    <div className="ops-toolbar">
      <button
        className="ops-btn drill-down"
        onClick={onDrillDown}
        disabled={!canDrillDown}
        title="Drill Down — navigate to finer granularity"
        id="btn-drill-down"
      >
        <span className="ops-icon">⬇</span>
        Drill Down
      </button>

      <button
        className="ops-btn roll-up"
        onClick={onRollUp}
        disabled={!canRollUp}
        title="Roll Up — navigate to coarser granularity"
        id="btn-roll-up"
      >
        <span className="ops-icon">⬆</span>
        Roll Up
      </button>

      <div className="ops-divider" />

      <button
        className="ops-btn pivot"
        onClick={onPivot}
        disabled={!canPivot}
        title="Pivot: xoay dimension cuối cùng thành column headers"
        id="btn-pivot"
      >
        <span className="ops-icon">🔄</span>
        Pivot
      </button>

      <div className="ops-divider" />

      <button
        className="ops-btn drill-across"
        onClick={onDrillAcross}
        disabled={!canDrillAcross}
        title={`Drill Across — compare with ${activeCube === 'sales' ? 'Inventory' : 'Sales'} cube`}
        id="btn-drill-across"
      >
        <span className="ops-icon">↔</span>
        Drill Across
      </button>

      <button
        className="ops-btn drill-through"
        onClick={onDrillThrough}
        disabled={!canDrillThrough}
        title="Drill Through — view detail fact records"
        id="btn-drill-through"
      >
        <span className="ops-icon">🔍</span>
        Drill Through
      </button>

      <div style={{ flex: 1 }} />

      <div className="view-toggle">
        <button
          className={`view-toggle-btn ${viewMode === 'table' ? 'active' : ''}`}
          onClick={() => onViewModeChange('table')}
          id="view-table"
        >
          📊 Table
        </button>
        <button
          className={`view-toggle-btn ${viewMode === 'chart' ? 'active' : ''}`}
          onClick={() => onViewModeChange('chart')}
          id="view-chart"
        >
          📈 Chart
        </button>
        <button
          className={`view-toggle-btn ${viewMode === 'both' ? 'active' : ''}`}
          onClick={() => onViewModeChange('both')}
          id="view-both"
        >
          ⊞ Both
        </button>
      </div>
    </div>
  );
}
