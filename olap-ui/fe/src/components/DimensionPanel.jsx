import React from 'react';

const DIM_ICONS = {
  time: '🕐',
  customer: '👤',
  product: '📦',
  store: '🏪',
};

export default function DimensionPanel({
  dimensions,
  activeDimensions,
  onToggleDimension,
  onChangeLevel,
}) {
  return (
    <div className="sidebar-section">
      <div className="sidebar-section-title">Dimensions</div>
      {dimensions.map(dim => {
        const active = activeDimensions.find(d => d.dimension === dim.name);
        const isActive = !!active;
        const currentLevel = active?.level || dim.levels[0]?.level;

        return (
          <div
            key={dim.name}
            className={`dim-card ${isActive ? 'active' : ''}`}
            id={`dim-card-${dim.name}`}
          >
            <div className="dim-card-header">
              <div className="dim-card-name">
                <span className="dim-icon">{DIM_ICONS[dim.name] || '📐'}</span>
                {dim.display}
              </div>
              <button
                className={`dim-card-toggle ${isActive ? 'on' : ''}`}
                onClick={() => onToggleDimension(dim.name, dim.levels[0]?.level)}
                title={isActive ? 'Remove dimension' : 'Add dimension'}
                id={`dim-toggle-${dim.name}`}
              />
            </div>

            {isActive && (
              <div className="dim-levels">
                {dim.levels.map(lvl => (
                  <button
                    key={lvl.level}
                    className={`dim-level-chip ${currentLevel === lvl.level ? 'active' : ''}`}
                    onClick={() => onChangeLevel(dim.name, lvl.level)}
                    id={`dim-level-${dim.name}-${lvl.level}`}
                  >
                    {lvl.display}
                  </button>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
