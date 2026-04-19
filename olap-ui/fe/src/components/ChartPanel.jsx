import React, { useRef, useEffect, useMemo, useState } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';
import { formatColumnName } from '../utils/pivotTransform';

ChartJS.register(
  CategoryScale, LinearScale,
  BarElement, LineElement, PointElement,
  Title, Tooltip, Legend, Filler
);

const CHART_COLORS = [
  'rgba(59, 130, 246, 0.8)',  // blue
  'rgba(139, 92, 246, 0.8)',  // purple
  'rgba(6, 182, 212, 0.8)',   // cyan
  'rgba(16, 185, 129, 0.8)',  // green
  'rgba(245, 158, 11, 0.8)',  // amber
  'rgba(244, 63, 94, 0.8)',   // rose
  'rgba(99, 102, 241, 0.8)',  // indigo
];

const CHART_BG_COLORS = [
  'rgba(59, 130, 246, 0.15)',
  'rgba(139, 92, 246, 0.15)',
  'rgba(6, 182, 212, 0.15)',
  'rgba(16, 185, 129, 0.15)',
  'rgba(245, 158, 11, 0.15)',
  'rgba(244, 63, 94, 0.15)',
  'rgba(99, 102, 241, 0.15)',
];

export default function ChartPanel({ columns, rows, dimensionColumns, measureColumns }) {
  const [chartType, setChartType] = useState('bar');

  const chartData = useMemo(() => {
    if (!columns || !rows || rows.length === 0 || !dimensionColumns || !measureColumns) {
      return null;
    }

    // Use the first dimension column as labels
    const labelColIndex = columns.indexOf(dimensionColumns[0]);
    if (labelColIndex === -1) return null;

    // Build label from all dimension columns for better readability
    const dimIndices = dimensionColumns.map(d => columns.indexOf(d)).filter(i => i !== -1);
    const labels = rows.map(row =>
      dimIndices.map(i => row[i] ?? '—').join(' · ')
    );

    // Limit to 50 data points for readability
    const maxPoints = 50;
    const limitedLabels = labels.slice(0, maxPoints);
    const limitedRows = rows.slice(0, maxPoints);

    const datasets = measureColumns.map((measure, mi) => {
      const colIndex = columns.indexOf(measure);
      if (colIndex === -1) return null;

      return {
        label: formatColumnName(measure),
        data: limitedRows.map(row => row[colIndex] ?? 0),
        backgroundColor: chartType === 'line'
          ? CHART_BG_COLORS[mi % CHART_BG_COLORS.length]
          : CHART_COLORS[mi % CHART_COLORS.length],
        borderColor: CHART_COLORS[mi % CHART_COLORS.length],
        borderWidth: chartType === 'line' ? 2 : 1,
        borderRadius: chartType === 'bar' ? 4 : 0,
        fill: chartType === 'line',
        tension: 0.3,
        pointRadius: chartType === 'line' ? 3 : 0,
        pointHoverRadius: 6,
      };
    }).filter(Boolean);

    return { labels: limitedLabels, datasets };
  }, [columns, rows, dimensionColumns, measureColumns, chartType]);

  const options = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: 'index',
      intersect: false,
    },
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: '#94a3b8',
          font: { family: "'Inter', sans-serif", size: 12 },
          padding: 16,
          usePointStyle: true,
          pointStyleWidth: 8,
        },
      },
      tooltip: {
        backgroundColor: 'rgba(17, 24, 39, 0.95)',
        titleColor: '#f1f5f9',
        bodyColor: '#94a3b8',
        borderColor: 'rgba(100, 130, 200, 0.2)',
        borderWidth: 1,
        padding: 12,
        titleFont: { family: "'Inter', sans-serif", size: 13, weight: '600' },
        bodyFont: { family: "'JetBrains Mono', monospace", size: 12 },
        callbacks: {
          label: (ctx) => {
            const val = ctx.parsed?.y ?? ctx.raw;
            const formatted = typeof val === 'number'
              ? new Intl.NumberFormat('en-US').format(val)
              : val;
            return ` ${ctx.dataset.label}: ${formatted}`;
          },
        },
      },
    },
    scales: {
      x: {
        grid: { color: 'rgba(100, 130, 200, 0.06)' },
        ticks: {
          color: '#64748b',
          font: { family: "'Inter', sans-serif", size: 11 },
          maxRotation: 45,
        },
      },
      y: {
        grid: { color: 'rgba(100, 130, 200, 0.06)' },
        ticks: {
          color: '#64748b',
          font: { family: "'JetBrains Mono', monospace", size: 11 },
          callback: (val) => new Intl.NumberFormat('en-US', { notation: 'compact' }).format(val),
        },
      },
    },
  }), []);

  if (!chartData) {
    return (
      <div className="chart-container">
        <div className="empty-state" style={{ padding: '48px' }}>
          <span className="empty-state-icon">📈</span>
          <div className="empty-state-title">No Data to Chart</div>
          <div className="empty-state-desc">Run a query to see chart visualization.</div>
        </div>
      </div>
    );
  }

  const ChartComponent = chartType === 'line' ? Line : Bar;

  return (
    <div className="chart-container">
      <div className="chart-header">
        <span style={{ fontWeight: 600, fontSize: '0.95rem' }}>
          📈 Visualization
        </span>
        <div className="chart-type-btns">
          <button
            className={`chart-type-btn ${chartType === 'bar' ? 'active' : ''}`}
            onClick={() => setChartType('bar')}
          >
            Bar
          </button>
          <button
            className={`chart-type-btn ${chartType === 'line' ? 'active' : ''}`}
            onClick={() => setChartType('line')}
          >
            Line
          </button>
        </div>
      </div>
      <div className="chart-canvas-wrapper">
        <ChartComponent data={chartData} options={options} />
      </div>
    </div>
  );
}
