/**
 * ============================================================================
 * COMBOCHART.JSX - Combo Chart cho Inventory Analysis
 * ============================================================================
 * Biểu đồ kết hợp cho phân tích chi tiết 1 sản phẩm:
 * - Trục X: Thờigian (Tháng/Quý/Năm)
 * - Trục Y trái: Bar chart cho Sales và Inventory
 * - Trục Y phải: Line chart cho Độ đáp ứng (Coverage Ratio)
 * ============================================================================
 */

import React from 'react'
import ReactECharts from 'echarts-for-react'
import { formatCurrency, formatNumber } from '../stores/store'

function ComboChart({ data, productName, timeLevel = 'month' }) {
  if (!data || data.length === 0) {
    return (
      <div className="h-80 flex items-center justify-center bg-gray-50 rounded-lg">
        <p className="text-gray-500">Không có dữ liệu</p>
      </div>
    )
  }

  // Format time label dựa vào timeLevel
  const formatTimeLabel = (item) => {
    if (timeLevel === 'month') {
      return `${item.year}-Q${item.quarter}-M${item.month}`
    } else if (timeLevel === 'quarter') {
      return `${item.year}-Q${item.quarter}`
    }
    return `${item.year}`
  }

  const labels = data.map(formatTimeLabel)
  const salesData = data.map(d => d.total_quantity)
  const inventoryData = data.map(d => d.total_quantity_on_hand)
  const coverageData = data.map(d => d.coverage_ratio * 100) // Convert to percentage

  const option = {
    title: {
      text: `Phân tích: ${productName}`,
      left: 'center',
      textStyle: {
        fontSize: 16,
        fontWeight: 'bold'
      }
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      },
      formatter: function (params) {
        let result = `<strong>${params[0].axisValue}</strong><br/>`
        params.forEach(param => {
          if (param.seriesName === 'Độ đáp ứng') {
            result += `${param.marker} ${param.seriesName}: ${param.value.toFixed(1)}%<br/>`
          } else {
            result += `${param.marker} ${param.seriesName}: ${formatNumber(param.value)}<br/>`
          }
        })
        return result
      }
    },
    legend: {
      data: ['Đã bán', 'Tổng lượng hàng còn lại', 'Độ đáp ứng'],
      bottom: 0
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: labels,
      axisPointer: {
        type: 'shadow'
      },
      axisLabel: {
        rotate: labels.length > 6 ? 45 : 0
      }
    },
    yAxis: [
      {
        type: 'value',
        name: 'Số lượng',
        position: 'left',
        axisLabel: {
          formatter: value => formatNumber(value)
        }
      },
      {
        type: 'value',
        name: 'Độ đáp ứng (%)',
        position: 'right',
        axisLabel: {
          formatter: '{value}%'
        },
        splitLine: {
          show: false
        }
      }
    ],
    series: [
      {
        name: 'Đã bán',
        type: 'bar',
        data: salesData,
        itemStyle: {
          color: '#799351'
        },
        barGap: '10%'
      },
      {
        name: 'Tổng lượng hàng còn lại',
        type: 'bar',
        data: inventoryData,
        itemStyle: {
          color: '#BED393'
        }
      },
      {
        name: 'Độ đáp ứng',
        type: 'line',
        yAxisIndex: 1,
        data: coverageData,
        itemStyle: {
          color: '#F4A261'
        },
        lineStyle: {
          width: 3
        },
        symbol: 'circle',
        symbolSize: 8,
        markLine: {
          data: [
            {
              yAxis: 100,
              label: {
                formatter: '100% (Cân bằng)'
              },
              lineStyle: {
                color: '#E76F51',
                type: 'dashed'
              }
            }
          ]
        }
      }
    ]
  }

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
      <ReactECharts option={option} style={{ height: '400px' }} />

      {/* Legend giải thích */}
      <div className="mt-4 grid grid-cols-3 gap-4 text-sm">
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 bg-[#799351] rounded"></div>
          <span className="text-gray-600">
            <strong>Đã bán (kỳ sau):</strong> Số lượng đã bán ra
          </span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 bg-[#BED393] rounded"></div>
          <span className="text-gray-600">
            <strong>Còn lại (hiện tại):</strong> Lượng hàng còn lại cuối kỳ
          </span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-1 bg-[#F4A261]"></div>
          <span className="text-gray-600">
            <strong>Độ đáp ứng:</strong> Hàng còn lại / Bán kỳ sau × 100%
          </span>
        </div>
      </div>
    </div>
  )
}

export default ComboChart
