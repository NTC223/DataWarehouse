/**
 * ============================================================================
 * DASHBOARD.JSX - Dashboard Component (Tab 1)
 * ============================================================================
 * Component hiển thị Dashboard với:
 * - 3 KPI Cards (Tổng doanh thu, Tổng SP, Giá trị TB)
 * - Trend Chart (Line chart với drill-down/roll-up)
 * - Customer Segment Chart (Pie chart - ẩn khi filter customer)
 * - Top/Bottom Products Tables (ẩn khi filter product)
 * ============================================================================
 */

import React, { useEffect } from 'react'
import ReactECharts from 'echarts-for-react'
import { useDashboardStore, formatCurrency, formatNumber, selectIsProductFiltered, selectIsCustomerFiltered } from '../stores/store'
import LoadingSpinner from './LoadingSpinner'

function Dashboard() {
  const {
    data,
    loading,
    drillDownLevel,
    handleChartDrillDown,
    handleChartRollUp,
    globalFilter,
    updateTopLevelFilter
  } = useDashboardStore()

  const isProductFiltered = selectIsProductFiltered({ globalFilter })
  const isCustomerFiltered = selectIsCustomerFiltered({ globalFilter })

  // ============================================================================
  // KPI CARDS
  // ============================================================================
  const KPICards = () => {
    if (!data.overview) return null

    const { total_revenue, total_quantity, average_price } = data.overview

    return (
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
        {/* Total Revenue */}
        <div className="kpi-card card-hover border-l-4 border-[#799351]">
          <p className="kpi-label">Tổng doanh thu</p>
          <p className="kpi-value text-[#799351]">
            {formatCurrency(total_revenue)}
          </p>
        </div>

        {/* Total Quantity */}
        <div className="kpi-card card-hover border-l-4 border-[#F4A261]">
          <p className="kpi-label">Tổng sản phẩm đã bán</p>
          <p className="kpi-value text-[#F4A261]">
            {formatNumber(total_quantity)}
          </p>
        </div>

        {/* Average Price */}
        <div className="kpi-card card-hover border-l-4 border-[#E76F51]">
          <p className="kpi-label">Giá bán trung bình (ASP)</p>
          <p className="kpi-value text-[#E76F51]">
            {formatCurrency(average_price)}
          </p>
        </div>
      </div>
    )
  }

  // ============================================================================
  // TREND CHART (Line Chart với Drill-down)
  // ============================================================================
  const TrendChart = () => {
    if (loading.trend) {
      return (
        <div className="chart-container h-96 flex items-center justify-center">
          <LoadingSpinner size="lg" />
        </div>
      )
    }

    if (!data.trend?.data?.length) {
      return (
        <div className="chart-container h-96 flex items-center justify-center">
          <p className="text-gray-500">Không có dữ liệu</p>
        </div>
      )
    }

    const chartData = data.trend.data
    const labels = chartData.map(d => d.label)
    const values = chartData.map(d => d.value)
    const quantities = chartData.map(d => d.quantity)

    const option = {
      title: {
        text: `Xu hướng doanh thu theo ${drillDownLevel === 'year' ? 'Năm' : drillDownLevel === 'quarter' ? 'Quý' : 'Tháng'}`,
        left: 'center',
        textStyle: {
          fontSize: 16,
          fontWeight: 'bold'
        }
      },
      tooltip: {
        trigger: 'axis',
        formatter: function (params) {
          const value = params[0].value
          const quantity = quantities[params[0].dataIndex]
          return `
            <div style="padding: 10px;">
              <strong>${params[0].name}</strong><br/>
              Doanh thu: ${formatCurrency(value)}<br/>
              Số lượng: ${formatNumber(quantity)}
            </div>
          `
        }
      },
      legend: {
        data: ['Doanh thu'],
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
        axisLabel: {
          rotate: labels.length > 10 ? 45 : 0
        }
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: (value) => {
            if (value >= 1000000) {
              return (value / 1000000).toFixed(0) + 'M'
            }
            if (value >= 1000) {
              return (value / 1000).toFixed(0) + 'K'
            }
            return value
          }
        }
      },
      series: [
        {
          name: 'Doanh thu',
          type: 'line',
          data: values,
          smooth: true,
          symbol: 'circle',
          symbolSize: 8,
          lineStyle: {
            width: 3,
            color: '#799351'
          },
          itemStyle: {
            color: '#799351'
          },
          areaStyle: {
            color: {
              type: 'linear',
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(121, 147, 81, 0.3)' },
                { offset: 1, color: 'rgba(121, 147, 81, 0.05)' }
              ]
            }
          }
        }
      ]
    }

    const onChartClick = (params) => {
      if (drillDownLevel !== 'month') {
        handleChartDrillDown(params.name)
      }
    }

    return (
      <div className="chart-container">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-800">
            📈 Xu hướng doanh thu
          </h3>
          <div className="flex items-center gap-2">
            {drillDownLevel !== 'year' && (
              <button
                onClick={handleChartRollUp}
                className="btn btn-secondary text-sm"
              >
                ⬆ Quay lại cấp trên
              </button>
            )}
            <span className="text-sm text-gray-500">
              Cấp độ: {drillDownLevel === 'year' ? 'Năm' : drillDownLevel === 'quarter' ? 'Quý' : 'Tháng'}
            </span>
          </div>
        </div>
        <ReactECharts
          option={option}
          style={{ height: '350px' }}
          onEvents={{
            click: onChartClick
          }}
        />
        <p className="text-xs text-gray-400 mt-2 text-center">
          {drillDownLevel !== 'month' ? 'Click vào điểm dữ liệu để drill-down' : 'Đã ở cấp độ chi tiết nhất'}
        </p>
      </div>
    )
  }

  // ============================================================================
  // CUSTOMER SEGMENT CHART (Pie Chart)
  // ============================================================================
  const CustomerSegmentChart = () => {
    // Ẩn nếu đã filter customer type cụ thể
    if (isCustomerFiltered) return null

    if (!data.customerSegment?.data?.length) return null

    const chartData = data.customerSegment.data.map(item => ({
      name: item.customer_type === 'Tourist' ? 'KH Du lịch' :
        item.customer_type === 'MailOrder' ? 'KH Bưu điện' : 'Cả hai',
      value: item.sum_amount,
      percentage: item.percentage
    }))

    const option = {
      title: {
        text: 'Phân khúc khách hàng',
        left: 'center',
        textStyle: {
          fontSize: 14,
          fontWeight: 'bold'
        }
      },
      tooltip: {
        trigger: 'item',
        formatter: (params) => {
          return `
            <strong>${params.name}</strong><br/>
            Doanh thu: ${formatCurrency(params.value)}<br/>
            Tỷ lệ: ${params.percent}%
          `
        }
      },
      legend: {
        orient: 'vertical',
        right: '5%',
        top: 'center'
      },
      series: [
        {
          name: 'Phân khúc',
          type: 'pie',
          radius: ['40%', '70%'],
          center: ['40%', '50%'],
          avoidLabelOverlap: false,
          itemStyle: {
            borderRadius: 10,
            borderColor: '#fff',
            borderWidth: 2
          },
          label: {
            show: false
          },
          emphasis: {
            label: {
              show: true,
              fontSize: 14,
              fontWeight: 'bold'
            }
          },
          data: chartData,
          color: ['#799351', '#F4A261', '#E76F51']
        }
      ]
    }

    return (
      <div className="chart-container">
        <ReactECharts option={option} style={{ height: '300px' }} />
      </div>
    )
  }

  // ============================================================================
  // TOP/BOTTOM PRODUCTS TABLES
  // ============================================================================
  const ProductRankings = () => {
    // Ẩn nếu đã filter product cụ thể
    if (isProductFiltered) return null

    if (!data.topProducts) return null

    const { top_5, bottom_5 } = data.topProducts

    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top 5 */}
        <div className="chart-container">
          <h3 className="text-lg font-semibold text-gray-800 mb-4">
            🏆 Top 5 sản phẩm doanh thu cao
          </h3>
          <div className="table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th className="w-16">#</th>
                  <th>Sản phẩm</th>
                  <th className="text-right">Doanh thu</th>
                  <th className="text-right">Số lượng</th>
                </tr>
              </thead>
              <tbody>
                {top_5.map((product, index) => (
                  <tr
                    key={product.product_key}
                    onClick={() => updateTopLevelFilter('product_key', product.product_key.toString())}
                    className="cursor-pointer hover:bg-green-50 transition-colors group"
                    title="Click để lọc Dashboard theo sản phẩm này"
                  >
                    <td>
                      <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-yellow-100 group-hover:bg-yellow-200 text-yellow-700 text-sm font-bold transition-colors">
                        {index + 1}
                      </span>
                    </td>
                    <td>{product.product_name}</td>
                    <td className="text-right font-medium text-green-600">
                      {formatCurrency(product.sum_amount)}
                    </td>
                    <td className="text-right">
                      {formatNumber(product.total_quantity)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Bottom 5 */}
        <div className="chart-container">
          <h3 className="text-lg font-semibold text-gray-800 mb-4">
            📉 Top 5 sản phẩm doanh thu thấp
          </h3>
          <div className="table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th className="w-16">#</th>
                  <th>Sản phẩm</th>
                  <th className="text-right">Doanh thu</th>
                  <th className="text-right">Số lượng</th>
                </tr>
              </thead>
              <tbody>
                {bottom_5.map((product, index) => (
                  <tr
                    key={product.product_key}
                    onClick={() => updateTopLevelFilter('product_key', product.product_key.toString())}
                    className="cursor-pointer hover:bg-red-50 transition-colors group"
                    title="Click để lọc Dashboard theo sản phẩm này"
                  >
                    <td>
                      <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-red-100 group-hover:bg-red-200 text-red-700 text-sm font-bold transition-colors">
                        {index + 1}
                      </span>
                    </td>
                    <td>{product.product_name}</td>
                    <td className="text-right font-medium text-red-600">
                      {formatCurrency(product.sum_amount)}
                    </td>
                    <td className="text-right">
                      {formatNumber(product.total_quantity)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    )
  }

  // ============================================================================
  // RENDER
  // ============================================================================
  return (
    <div className="space-y-6 animate-fade-in">
      {/* KPI Cards */}
      <KPICards />

      {/* Main Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Trend Chart - Chiếm 2/3 */}
        <div className="lg:col-span-2">
          <TrendChart />
        </div>

        {/* Customer Segment - Chiếm 1/3 */}
        <div>
          <CustomerSegmentChart />
        </div>
      </div>

      {/* Product Rankings */}
      <ProductRankings />

      {/* Info Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-blue-50 rounded-lg p-4">
          <h4 className="font-semibold text-blue-800 mb-2">💡 Mẹo sử dụng</h4>
          <p className="text-sm text-blue-600">
            Click vào biểu đồ xu hướng để drill-down từ Năm → Quý → Tháng.
            Sử dụng nút "Quay lại cấp trên" để roll-up.
          </p>
        </div>

        <div className="bg-green-50 rounded-lg p-4">
          <h4 className="font-semibold text-green-800 mb-2">📊 Dữ liệu</h4>
          <p className="text-sm text-green-600">
            Dữ liệu được cập nhật từ hệ thống OLAP với độ trễ tối thiểu.
            Tất cả số liệu đều được pre-aggregated để đảm bảo tốc độ.
          </p>
        </div>

        <div className="bg-purple-50 rounded-lg p-4">
          <h4 className="font-semibold text-purple-800 mb-2">🔍 OLAP Explorer</h4>
          <p className="text-sm text-purple-600">
            Chuyển sang tab "OLAP Explorer" để tự do khám phá dữ liệu
            với Pivot Table và các phép toán OLAP.
          </p>
        </div>
      </div>
    </div>
  )
}

export default Dashboard
