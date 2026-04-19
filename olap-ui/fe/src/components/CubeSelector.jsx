import React from 'react';

const CUBE_ICONS = {
  sales: '💰',
  inventory: '📦',
};

export default function CubeSelector({ cubes, activeCube, onSelect }) {
  return (
    <div className="cube-selector">
      {cubes.map(cube => (
        <button
          key={cube.name}
          id={`cube-btn-${cube.name}`}
          className={`cube-btn ${activeCube === cube.name ? 'active' : ''}`}
          onClick={() => onSelect(cube.name)}
        >
          <span className="cube-icon">{CUBE_ICONS[cube.name] || '📊'}</span>
          {cube.display_name}
        </button>
      ))}
    </div>
  );
}
