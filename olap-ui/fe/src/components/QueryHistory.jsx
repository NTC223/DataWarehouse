import React from 'react';

export default function QueryHistory({ history, onJumpTo }) {
  if (!history || history.length === 0) {
    return (
      <div className="sidebar-section">
        <div className="sidebar-section-title">Navigation History</div>
        <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', padding: '8px 0' }}>
          No navigation history yet. Use Drill Down / Roll Up to navigate.
        </div>
      </div>
    );
  }

  return (
    <div className="sidebar-section">
      <div className="sidebar-section-title">Navigation History ({history.length})</div>
      <div className="history-list">
        {history.map((entry, i) => {
          const desc = buildDescription(entry);
          return (
            <div
              key={i}
              className="history-item"
              onClick={() => onJumpTo(i)}
              title={`Jump to: ${desc}`}
            >
              <span className="history-index">{i + 1}</span>
              <span className="history-desc">{desc}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function buildDescription(entry) {
  const parts = [];
  const cubeName = entry.cube === 'sales' ? '💰 Sales' : '📦 Inventory';
  parts.push(cubeName);

  if (entry.dimensions.length === 0) {
    parts.push('ALL');
  } else {
    entry.dimensions.forEach(d => {
      parts.push(`${d.dimension}(${d.level})`);
    });
  }

  if (entry.filters.length > 0) {
    const filterDesc = entry.filters.map(f =>
      `${f.column}=${f.values.length > 1 ? `[${f.values.length} vals]` : f.values[0]}`
    ).join(', ');
    parts.push(`| ${filterDesc}`);
  }

  return parts.join(' · ');
}
