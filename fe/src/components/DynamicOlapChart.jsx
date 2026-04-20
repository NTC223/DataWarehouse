/**
 * DynamicOlapChart.jsx
 * Component vẽ biểu đồ động dựa trên cấu hình OLAP (rows/columns).
 * - Dữ liệu ĐÃ ĐƯỢC BE sort giảm dần theo measure (KHÔNG tự sort lại).
 * - Rule engine chọn chart: Line / Horizontal Bar / Heatmap.
 */
import React, { useMemo } from 'react'
import ReactECharts from 'echarts-for-react'

// ─── Helper: Làm phẳng nhiều chiều thành 1 nhãn ─────────────────────────────
function makeLabel(row, keys) {
  if (!keys || keys.length === 0) return 'Tất cả'
  return keys.map(k => {
    const val = row[k]
    return val === null || val === undefined ? '(Trống)' : val
  }).join(' - ')
}

// ─── Helper: Format tiền tệ VND ────────────────────────────────────────────────
function formatCurrency(val) {
  if (val == null || isNaN(val)) return '0'
  if (Math.abs(val) >= 1_000_000_000) return (val / 1_000_000_000).toFixed(1) + 'B'
  if (Math.abs(val) >= 1_000_000) return (val / 1_000_000).toFixed(1) + 'M'
  if (Math.abs(val) >= 1_000) return (val / 1_000).toFixed(1) + 'K'
  return val.toLocaleString('vi-VN')
}

// ─── Helper: Detect time dimension ─────────────────────────────────────────────
const TIME_KEYWORDS = ['year', 'quarter', 'month']

function isTimeField(field) {
  return TIME_KEYWORDS.some(k => String(field).toLowerCase().includes(k))
}

function hasTimeDimension(fields) {
  return fields.some(f => isTimeField(f))
}

// ─── Main Component ─────────────────────────────────────────────────────────────
export default function DynamicOlapChart({ data, activeRows, activeColumns, currentPage = 1 }) {
  // ── Bước 3.1: Swap axes nếu không có Row ────────────────────────────────────
  // Extract column names từ objects { id, column, name }
  const { rows, columns } = useMemo(() => {
    let r = (activeRows || []).map(f => f.column || f.id)
    let c = (activeColumns || []).map(f => f.column || f.id)
    if (r.length === 0 && c.length > 0) {
      r = c
      c = []
    }
    return { rows: r, columns: c }
  }, [activeRows, activeColumns])

  // ── Empty State ───────────────────────────────────────────────────────────────
  if (!data || data.length === 0) {
    return (
      <div style={{ height: '500px', width: '100%' }} className="flex items-center justify-center bg-gray-50 rounded-lg border border-gray-200">
        <p className="text-gray-400 text-sm">Không có dữ liệu để hiển thị biểu đồ.</p>
      </div>
    )
  }

  // ── Xác định measure column ─────────────────────────────────────────────────
  const MEASURE_COL = 'total_amount'
  const measureKey = Object.keys(data[0] || {}).find(
    k => k === MEASURE_COL || k.toLowerCase().includes('amount') || k.toLowerCase().includes('quantity')
  ) || MEASURE_COL

  // ── Bước 3.2: Flatten labels ───────────────────────────────────────────────
  // rows[] → nhãn trục Y (bar/line)
  // columns[] → nhãn trục X (bar/line) hoặc series (heatmap)
  const rowLabels = rows.length > 0
    ? data.map(d => makeLabel(d, rows))
    : []

  const colLabels = columns.length > 0
    ? [...new Set(data.map(d => makeLabel(d, columns)))].sort()
    : []

  // ── RULE ENGINE ──────────────────────────────────────────────────────────────
  const hasColumns = columns.length > 0

  // Base option chung
  const baseTitle = `Phân tích Top ${data.length} tổ hợp cao nhất (Trang ${currentPage})`

  let option = {}

  if (!hasColumns) {
    // ─── TRƯỜNG HỢP 1: 1 trục (Row, KHÔNG có Column) ───────────────────────
    const useLine = hasTimeDimension(rows)

    const sortedData = data // ĐÃ SORT SẴN từ BE, không sort lại

    if (useLine) {
      // ── Line Chart ──────────────────────────────────────────────────────────
      option = {
        title: { text: baseTitle, left: 'center', textStyle: { fontSize: 14, fontWeight: 'bold', color: '#1f2937' } },
        tooltip: {
          trigger: 'axis',
          backgroundColor: 'rgba(255, 255, 255, 0.95)',
          borderColor: '#84cc16',
          borderWidth: 1,
          textStyle: { color: '#374151' },
          formatter: (params) => {
            const p = params[0]
            return `<b>${p.name}</b><br/>Giá trị: <b>${formatCurrency(p.value)}</b>`
          }
        },
        grid: { left: 70, right: 40, top: 60, bottom: 100, containLabel: true },
        xAxis: {
          type: 'category',
          data: rowLabels,
          boundaryGap: false,
          axisLine: { lineStyle: { color: '#d1d5db' } },
          axisLabel: {
            rotate: 35,
            fontSize: 11,
            color: '#6b7280',
            interval: 'auto',
            width: 100,
            overflow: 'truncate'
          },
          axisTick: { show: false }
        },
        yAxis: {
          type: 'value',
          scale: true,
          axisLabel: { formatter: v => formatCurrency(v), color: '#6b7280', fontSize: 11 },
          splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } },
          axisLine: { show: false }
        },
        series: [{
          type: 'line',
          data: sortedData.map(d => d[measureKey]),
          smooth: 0.4,
          lineStyle: { width: 4, color: '#65a30d' },
          itemStyle: { color: '#65a30d' },
          areaStyle: {
            color: {
              type: 'linear',
              x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(101, 163, 13, 0.35)' },
                { offset: 1, color: 'rgba(101, 163, 13, 0.02)' }
              ]
            }
          },
          emphasis: {
            focus: 'series',
            lineStyle: { width: 5 },
            itemStyle: { shadowBlur: 10, shadowColor: 'rgba(101, 163, 13, 0.4)' }
          },
          symbol: 'circle',
          symbolSize: 8,
          showSymbol: false,
          endLabel: { show: false }
        }],
        dataZoom: [
          { type: 'slider', xAxisIndex: 0, start: 0, end: 100, height: 22, bottom: 15, borderColor: '#e5e7eb', fillerColor: 'rgba(132, 204, 22, 0.15)', handleStyle: { color: '#65a30d' } },
          { type: 'inside', xAxisIndex: 0, start: 0, end: 100 }
        ]
      }
    } else {
      // ── Vertical Bar Chart ────────────────────────────────────────────────
      option = {
        title: { text: baseTitle, left: 'center', textStyle: { fontSize: 14, fontWeight: 'bold', color: '#1f2937' } },
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'shadow' },
          backgroundColor: 'rgba(255, 255, 255, 0.95)',
          borderColor: '#84cc16',
          borderWidth: 1,
          textStyle: { color: '#374151' },
          formatter: (params) => {
            const p = params[0]
            return `<b>${p.name}</b><br/>Giá trị: <b>${formatCurrency(p.value)}</b>`
          }
        },
        grid: { left: 70, right: 40, top: 60, bottom: 100, containLabel: true },
        xAxis: {
          type: 'category',
          data: rowLabels,
          axisLine: { lineStyle: { color: '#d1d5db' } },
          axisLabel: {
            rotate: 35,
            fontSize: 11,
            color: '#6b7280',
            interval: 'auto',
            width: 100,
            overflow: 'truncate'
          },
          axisTick: { show: false }
        },
        yAxis: {
          type: 'value',
          axisLabel: { formatter: v => formatCurrency(v), color: '#6b7280', fontSize: 11 },
          splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } },
          axisLine: { show: false }
        },
        series: [{
          type: 'bar',
          data: sortedData.map(d => d[measureKey]),
          barMaxWidth: 45,
          barRadius: [6, 6, 0, 0],
          itemStyle: {
            borderRadius: [6, 6, 0, 0],
            color: {
              type: 'linear',
              x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: '#a3e635' },
                { offset: 1, color: '#365314' }
              ]
            }
          },
          emphasis: {
            focus: 'series',
            itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.3)' }
          },
          label: {
            show: true,
            position: 'top',
            formatter: (p) => formatCurrency(p.value),
            fontSize: 10,
            color: '#4b5563'
          }
        }],
        dataZoom: [
          { type: 'slider', xAxisIndex: 0, start: 0, end: 100, height: 22, bottom: 15, borderColor: '#e5e7eb', fillerColor: 'rgba(132, 204, 22, 0.15)', handleStyle: { color: '#65a30d' } },
          { type: 'inside', xAxisIndex: 0, start: 0, end: 100 }
        ]
      }
    }

  } else {
    // ─── TRƯỜNG HỢP 2: Đa trục chéo (Có Row VÀ Column) → Bubble Chart ─────
    const yLabels = [...new Set(rowLabels)].reverse()
    const xLabels = colLabels

    const values = data.map(d => d[measureKey])
    const minVal = Math.min(...values)
    const maxVal = Math.max(...values)
    const heatmapData = []

    for (const rowLabel of yLabels) {
      const rowItems = data.filter(d => makeLabel(d, rows) === rowLabel)
      for (const item of rowItems) {
        const xLabel = makeLabel(item, columns)
        const xIdx = xLabels.indexOf(xLabel)
        const yIdx = yLabels.indexOf(rowLabel)
        if (xIdx >= 0 && yIdx >= 0) {
          heatmapData.push([xIdx, yIdx, item[measureKey]])
        }
      }
    }

    // Giới hạn 100 bubbles để chart còn đọc được
    const MAX_BUBBLES = 100
    const sortedData = heatmapData.sort((a, b) => b[2] - a[2]).slice(0, MAX_BUBBLES)

    // Tính kích thước bubble tối thiểu để nhìn thấy
    const bubbleMin = Math.max(12, Math.sqrt(minVal) / 200)
    const bubbleMax = Math.min(60, Math.sqrt(maxVal) / 25)

    option = {
      title: { text: baseTitle, left: 'center', textStyle: { fontSize: 14, fontWeight: 'bold', color: '#1f2937' } },
      tooltip: {
        trigger: 'item',
        backgroundColor: 'rgba(255,255,255,0.95)',
        borderColor: '#84cc16',
        borderWidth: 1,
        textStyle: { color: '#374151' },
        formatter: (params) => {
          const x = xLabels[params.value[0]] || ''
          const y = yLabels[params.value[1]] || ''
          const v = formatCurrency(params.value[2])
          return `<b>${y}</b> × <b>${x}</b><br/>Giá trị: <b>${v}</b>`
        }
      },
      grid: { left: 200, right: 50, top: 60, bottom: 80, containLabel: true },
      xAxis: {
        type: 'category',
        data: xLabels,
        splitArea: { show: false },
        axisLabel: { rotate: 30, fontSize: 11, color: '#6b7280' },
        axisLine: { lineStyle: { color: '#d1d5db' } },
        axisTick: { show: false }
      },
      yAxis: {
        type: 'category',
        data: yLabels,
        splitArea: { show: false },
        splitLine: { lineStyle: { color: '#f3f4f6', type: 'dashed' } },
        axisLabel: {
          fontSize: 11,
          color: '#4b5563',
          width: 180,
          overflow: 'truncate',
          margin: 15
        },
        axisLine: { show: false }
      },
      visualMap: {
        min: minVal,
        max: maxVal,
        calculable: true,
        orient: 'horizontal',
        left: 'center',
        bottom: 5,
        itemWidth: 15,
        itemHeight: 120,
        textStyle: { color: '#6b7280', fontSize: 11 },
        inRange: {
          symbolSize: [bubbleMin, bubbleMax],
          color: ['#ecfccb', '#a3e635', '#65a30d', '#3f6212', '#1a2e05']
        },
        formatter: (val) => formatCurrency(val)
      },
      series: [{
        type: 'scatter',
        data: sortedData,
        label: { show: false },
        symbolKeepAspect: false,
        itemStyle: {
          opacity: 0.85,
          borderColor: '#ffffff',
          borderWidth: 2
        },
        emphasis: {
          scale: 1.4,
          itemStyle: {
            shadowBlur: 15,
            shadowColor: 'rgba(101, 163, 13, 0.5)',
            borderWidth: 2,
            borderColor: '#84cc16'
          }
        }
      }]
    }
  }

  return (
    <div style={{ height: '500px', width: '100%' }}>
      <ReactECharts
        option={option}
        style={{ height: '100%', width: '100%' }}
        opts={{ renderer: 'canvas' }}
        notMerge={true}
      />
    </div>
  )
}
