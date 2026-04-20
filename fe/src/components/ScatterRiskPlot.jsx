/**
 * ============================================================================
 * SCATTERRISKPLOT.JSX - Scatter Plot cho phân tích rủi ro tồn kho
 * ============================================================================
 * Biểu đồ phân tán hiển thị toàn bộ sản phẩm:
 * - X-axis: Sales (total_quantity)
 * - Y-axis: Inventory (total_quantity_on_hand)
 * - Đường chéo y=x: Tỷ lệ 1:1 (cân bằng)
 * - Phân vùng màu:
 *   + Trên đường chéo (Cam): Overstock - Tồn kho dư thừa
 *   + Dưới đường chéo (Đỏ): Understock - Thiếu hàng
 *   + Sát đường chéo (Xanh): Optimal - Cân bằng
 * ============================================================================
 */

import React from 'react'
import ReactECharts from 'echarts-for-react'
import { formatNumber } from '../stores/store'

function ScatterRiskPlot({ data, summary, city, onPointClick, timeLevel = 'month' }) {
  if (!data || data.length === 0) {
    return (
      <div className="h-96 flex items-center justify-center bg-gray-50 rounded-lg">
        <p className="text-gray-500">Không có dữ liệu</p>
      </div>
    )
  }

  // Phân loại data theo status
  const optimalData = data.filter(d => d.status === 'optimal').map(d => [d.sales, d.inventory, d.product_name, d.coverage_ratio])
  const overstockData = data.filter(d => d.status === 'overstock').map(d => [d.sales, d.inventory, d.product_name, d.coverage_ratio])
  const understockData = data.filter(d => d.status === 'understock').map(d => [d.sales, d.inventory, d.product_name, d.coverage_ratio])

  // Tìm max để vẽ đường chéo
  const maxValue = Math.max(
    ...data.map(d => d.sales),
    ...data.map(d => d.inventory)
  ) * 1.1

  const option = {
    title: {
      text: `Biểu đồ Độ đáp ứng theo Sản phẩm${city ? ` - ${city}` : ''}`,
      subtext: `Tổng: ${data.length} sản phẩm | Optimal: ${summary?.optimal || 0} | Overstock: ${summary?.overstock || 0} | Understock: ${summary?.understock || 0}`,
      left: 'center',
      textStyle: {
        fontSize: 16,
        fontWeight: 'bold'
      }
    },
    tooltip: {
      trigger: 'item',
      formatter: function (params) {
        const productName = params.data[2]
        const coverage = params.data[3]
        const salesLabel = 'Bán (kỳ sau)'
        return `
          <strong>${productName}</strong><br/>
          ${salesLabel}: ${formatNumber(params.data[0])}<br/>
          Tồn kho hiện tại: ${formatNumber(params.data[1])}<br/>
          Độ đáp ứng: ${(coverage * 100).toFixed(1)}%
        `
      }
    },
    legend: {
      data: ['Optimal', 'Overstock', 'Understock'],
      bottom: 0
    },
    grid: {
      left: '8%',
      right: '8%',
      bottom: '15%',
      top: '20%',
      containLabel: true
    },
    xAxis: {
      type: 'value',
      name: 'Lượng bán (kỳ sau)',
      nameLocation: 'middle',
      nameGap: 30,
      axisLabel: {
        formatter: value => formatNumber(value)
      },
      splitLine: {
        lineStyle: {
          type: 'dashed'
        }
      }
    },
    yAxis: {
      type: 'value',
      name: 'Lượng hàng hiện tại',
      nameLocation: 'middle',
      nameGap: 50,
      axisLabel: {
        formatter: value => formatNumber(value)
      },
      splitLine: {
        lineStyle: {
          type: 'dashed'
        }
      }
    },
    series: [
      // Đường chéo y=x (tỷ lệ 1:1)
      {
        name: 'Tỷ lệ 1:1',
        type: 'line',
        data: [[0, 0], [maxValue, maxValue]],
        lineStyle: {
          color: '#666',
          type: 'dashed',
          width: 2
        },
        symbol: 'none',
        silent: true,
        tooltip: {
          show: false
        }
      },
      // Optimal (Xanh lá)
      {
        name: 'Optimal',
        type: 'scatter',
        data: optimalData,
        symbolSize: 12,
        itemStyle: {
          color: '#799351'
        },
        emphasis: {
          focus: 'series',
          itemStyle: {
            borderColor: '#000',
            borderWidth: 2
          }
        }
      },
      // Overstock (Cam)
      {
        name: 'Overstock',
        type: 'scatter',
        data: overstockData,
        symbolSize: 12,
        itemStyle: {
          color: '#F4A261'
        },
        emphasis: {
          focus: 'series',
          itemStyle: {
            borderColor: '#000',
            borderWidth: 2
          }
        }
      },
      // Understock (Đỏ)
      {
        name: 'Understock',
        type: 'scatter',
        data: understockData,
        symbolSize: 12,
        itemStyle: {
          color: '#E76F51'
        },
        emphasis: {
          focus: 'series',
          itemStyle: {
            borderColor: '#000',
            borderWidth: 2
          }
        }
      }
    ]
  }

  const onChartClick = (params) => {
    if (params.componentType === 'series' && params.seriesName !== 'Tỷ lệ 1:1') {
      const productName = params.data[2]
      const productKey = data.find(d => d.product_name === productName)?.product_key
      if (productKey && onPointClick) {
        onPointClick(productKey)
      }
    }
  }

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
      <ReactECharts
        option={option}
        style={{ height: '500px' }}
        onEvents={{
          click: onChartClick
        }}
      />

      {/* Legend giải thích */}
      <div className="mt-4 grid grid-cols-3 gap-4 text-sm">
        <div className="bg-green-50 rounded-lg p-3">
          <div className="flex items-center gap-2 mb-1">
            <div className="w-3 h-3 bg-[#799351] rounded-full"></div>
            <span className="font-semibold text-[#799351]">Tối ưu (Optimal)</span>
          </div>
          <p className="text-green-600 text-xs">
            Độ đáp ứng từ 80% – 120%<br />
            Lượng hàng vừa đủ cho kỳ sau
          </p>
        </div>

        <div className="bg-orange-50 rounded-lg p-3">
          <div className="flex items-center gap-2 mb-1">
            <div className="w-3 h-3 bg-[#F4A261] rounded-full"></div>
            <span className="font-semibold text-[#F4A261]">Dư thừa (Overstock)</span>
          </div>
          <p className="text-orange-600 text-xs">
            Độ đáp ứng &gt; 120%<br />
            Hàng còn lại vượt quá nhu cầu kỳ sau
          </p>
        </div>

        <div className="bg-red-50 rounded-lg p-3">
          <div className="flex items-center gap-2 mb-1">
            <div className="w-3 h-3 bg-[#E76F51] rounded-full"></div>
            <span className="font-semibold text-[#E76F51]">Thiếu hụt (Understock)</span>
          </div>
          <p className="text-red-600 text-xs">
            Độ đáp ứng &lt; 80%<br />
            Hàng không đủ đáp ứng nhu cầu kỳ sau
          </p>
        </div>
      </div>

      <p className="mt-3 text-xs text-gray-400 text-center">
        💡 Click vào điểm để xem phân tích chi tiết sản phẩm
      </p>
    </div>
  )
}

export default ScatterRiskPlot
