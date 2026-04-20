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
    updateTopLevelFilter,
    fetchDrillThrough
  } = useDashboardStore()

  const [modalOpen, setModalOpen] = React.useState(false)
  const [modalData, setModalData] = React.useState(null)
  const [modalLoading, setModalLoading] = React.useState(false)
  const [selectedCustKey, setSelectedCustKey] = React.useState(null)

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
  const Top5ProductsTable = () => {
    if (isProductFiltered || !data.topProducts) return null
    const { top_5 } = data.topProducts
    return (
      <div className="chart-container">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">🏆 Top 5 sản phẩm cao</h3>
        <div className="table-container">
          <table className="data-table">
            <thead>
              <tr>
                <th className="w-16">#</th>
                <th>Sản phẩm</th>
                <th className="text-right">Doanh thu</th>
              </tr>
            </thead>
            <tbody>
              {top_5.map((product, index) => (
                <tr key={product.product_key} onClick={() => updateTopLevelFilter('product_key', product.product_key.toString())} className="cursor-pointer hover:bg-green-50 transition-colors">
                  <td>{index + 1}</td>
                  <td>{product.product_name}</td>
                  <td className="text-right font-medium text-green-600">{formatCurrency(product.sum_amount)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )
  }

  const Bottom5ProductsTable = () => {
    if (isProductFiltered || !data.topProducts) return null
    const { bottom_5 } = data.topProducts
    return (
      <div className="chart-container">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">📉 Top 5 sản phẩm thấp</h3>
        <div className="table-container">
          <table className="data-table">
            <thead>
              <tr>
                <th className="w-16">#</th>
                <th>Sản phẩm</th>
                <th className="text-right">Doanh thu</th>
              </tr>
            </thead>
            <tbody>
              {bottom_5.map((product, index) => (
                <tr key={product.product_key} onClick={() => updateTopLevelFilter('product_key', product.product_key.toString())} className="cursor-pointer hover:bg-red-50 transition-colors">
                  <td>{index + 1}</td>
                  <td>{product.product_name}</td>
                  <td className="text-right font-medium text-red-600">{formatCurrency(product.sum_amount)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )
  }

  // ============================================================================
  // TOP CUSTOMERS TABLE (Ranking + Drill-through)
  // ============================================================================
  const TopCustomersTable = () => {
    if (loading.topCustomers) {
      return (
        <div className="chart-container h-80 flex items-center justify-center">
          <LoadingSpinner size="lg" />
        </div>
      )
    }

    if (!data.topCustomers?.top_5?.length) return null

    const handleRowClick = async (customerKey) => {
      setSelectedCustKey(customerKey)
      setModalOpen(true)
      setModalLoading(true)
      try {
        const result = await fetchDrillThrough(customerKey, 1, 15) // Page 1, size 15 for modal init
        setModalData(result)
      } catch (err) {
        console.error(err)
      } finally {
        setModalLoading(false)
      }
    }

    return (
      <div className="chart-container relative">
        <h3 className="text-lg font-semibold text-gray-800 mb-4 flex items-center gap-2">
          👤 Top 5 khách hàng doanh thu cao
          
        </h3>
        <div className="table-container">
          <table className="data-table">
            <thead>
              <tr>
                <th className="w-16">#</th>
                <th>Khách hàng</th>
                <th>Loại</th>
                <th className="text-right">Doanh thu</th>
                <th className="text-right">Sản phẩm</th>
              </tr>
            </thead>
            <tbody>
              {data.topCustomers.top_5.map((customer, index) => (
                <tr
                  key={customer.customer_key}
                  onClick={() => handleRowClick(customer.customer_key)}
                  className="cursor-pointer hover:bg-blue-50 transition-colors group"
                >
                  <td>
                    <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-blue-100 group-hover:bg-blue-200 text-blue-700 text-sm font-bold transition-colors">
                      {index + 1}
                    </span>
                  </td>
                  <td className="font-medium">{customer.customer_name}</td>
                  <td>
                    <span className={`px-2 py-1 rounded-full text-xs ${
                      customer.customer_type === 'Tourist' ? 'bg-orange-100 text-orange-700' : 'bg-purple-100 text-purple-700'
                    }`}>
                      {customer.customer_type}
                    </span>
                  </td>
                  <td className="text-right font-bold text-blue-600">
                    {formatCurrency(customer.sum_amount)}
                  </td>
                  <td className="text-right">
                    {formatNumber(customer.total_quantity)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )
  }

  // ============================================================================
  // DRILL-THROUGH MODAL
  // ============================================================================
  const CustomerDrillThroughModal = () => {
    if (!modalOpen) return null

    const handleClose = () => {
      setModalOpen(false)
      setModalData(null)
    }

    const changePage = async (page) => {
      setModalLoading(true)
      try {
        const result = await fetchDrillThrough(selectedCustKey, page, 15)
        setModalData(result)
      } catch (err) {
        console.error(err)
      } finally {
        setModalLoading(false)
      }
    }

    return (
      <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/50 backdrop-blur-sm animate-in fade-in duration-300">
        <div className="bg-white rounded-2xl shadow-2xl w-full max-w-5xl max-h-[90vh] overflow-hidden flex flex-col scale-100 animate-in zoom-in-95 duration-300">
          {/* Header */}
          <div className="bg-gradient-to-r from-[#799351] to-[#588157] p-6 text-white flex justify-between items-center">
            <div>
              <h2 className="text-2xl font-bold">{modalData?.customer_info?.customer_name || 'Đang tải...'}</h2>
              <p className="opacity-80 flex items-center gap-2 mt-1">
                <span>📍 {modalData?.customer_info?.location}</span>
                <span>•</span>
                <span>🏷️ {modalData?.customer_info?.customer_type}</span>
                <span>•</span>
                <span>📅 Khách hàng từ: {modalData?.customer_info?.first_order_date}</span>
              </p>
            </div>
            <button 
              onClick={handleClose}
              className="p-2 hover:bg-white/20 rounded-full transition-colors text-2xl"
            >
              ✕
            </button>
          </div>

          <div className="flex-1 overflow-y-auto p-6">
            {!modalData && modalLoading ? (
               <div className="h-64 flex items-center justify-center">
                  <LoadingSpinner size="lg" />
               </div>
            ) : modalData ? (
              <div className="space-y-6">
                {/* Stats Grid */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="bg-[#799351]/10 p-4 rounded-xl border border-[#799351]/20">
                    <p className="text-xs text-[#588157] font-bold uppercase mb-1">Tổng doanh thu</p>
                    <p className="text-xl font-black text-[#3A5A40]">{formatCurrency(modalData.summary.total_revenue)}</p>
                  </div>
                  <div className="bg-[#F4A261]/10 p-4 rounded-xl border border-[#F4A261]/20">
                    <p className="text-xs text-[#F4A261] font-bold uppercase mb-1">Số lượng sản phẩm đã mua</p>
                    <p className="text-xl font-black text-[#6B4E16]">{formatNumber(modalData.summary.total_quantity)}</p>
                  </div>
                  <div className="bg-[#E76F51]/10 p-4 rounded-xl border border-[#E76F51]/20">
                    <p className="text-xs text-[#E76F51] font-bold uppercase mb-1">Tổng số dòng giao dịch</p>
                    <p className="text-xl font-black text-[#7A2E1E]">{modalData.summary.order_count}</p>
                  </div>
                  <div className="bg-gray-50 p-4 rounded-xl border border-gray-100">
                    <p className="text-xs text-gray-600 font-bold uppercase mb-1">Giao dịch cuối cùng</p>
                    <p className="text-xl font-black text-gray-900">{modalData.summary.last_order_date}</p>
                  </div>
                </div>

                {/* History Table */}
                <div className="border rounded-xl overflow-hidden">
                  <div className="bg-gray-50 px-4 py-3 border-b flex justify-between items-center">
                    <h4 className="font-bold text-gray-700">Lịch sử giao dịch chi tiết</h4>
                    <span className="text-sm text-gray-500">Hiển thị {modalData.transactions.length} / {modalData.pagination.total_records} bản ghi</span>
                  </div>
                  <div className="relative">
                    {modalLoading && (
                      <div className="absolute inset-0 bg-white/60 backdrop-blur-[1px] z-10 flex items-center justify-center">
                        <LoadingSpinner size="md" />
                      </div>
                    )}
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50 text-gray-600 border-b">
                        <tr>
                          <th className="px-4 py-3 text-left">Thời gian</th>
                          <th className="px-4 py-3 text-left">Sản phẩm</th>
                          <th className="px-4 py-3 text-right">Số lượng</th>
                          <th className="px-4 py-3 text-right">Giá trị</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y">
                        {modalData.transactions.map((t, i) => (
                          <tr key={i} className="hover:bg-gray-50 transition-colors">
                            <td className="px-4 py-3 font-medium text-gray-900">{t.period}</td>
                            <td className="px-4 py-3 text-gray-600">{t.product_name}</td>
                            <td className="px-4 py-3 text-right text-gray-600">{formatNumber(t.quantity_ordered)}</td>
                            <td className="px-4 py-3 text-right font-bold text-gray-900">{formatCurrency(t.total_amount)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Pagination */}
                <div className="flex justify-between items-center py-4">
                  <button 
                    disabled={modalData.pagination.current_page === 1 || modalLoading}
                    onClick={() => changePage(modalData.pagination.current_page - 1)}
                    className="btn btn-secondary px-4 py-2 disabled:opacity-30"
                  >
                    ← Trước
                  </button>
                  <div className="flex gap-2">
                    {Array.from({ length: Math.min(5, modalData.pagination.total_pages) }, (_, i) => {
                      const p = modalData.pagination.current_page <= 3 
                        ? i + 1 
                        : Math.min(modalData.pagination.current_page - 2 + i, modalData.pagination.total_pages - 4 + i > 0 ? modalData.pagination.total_pages - 4 + i : 1 + i);
                      
                      return p <= modalData.pagination.total_pages ? (
                        <button
                          key={p}
                          onClick={() => changePage(p)}
                          className={`w-10 h-10 rounded-lg font-bold transition-all ${
                            modalData.pagination.current_page === p 
                            ? 'bg-[#799351] text-white shadow-lg shadow-[#799351]/25 scale-110' 
                            : 'bg-white border text-gray-600 hover:border-[#799351]/40'
                          }`}
                        >
                          {p}
                        </button>
                      ) : null
                    })}
                  </div>
                  <button 
                    disabled={modalData.pagination.current_page === modalData.pagination.total_pages || modalLoading}
                    onClick={() => changePage(modalData.pagination.current_page + 1)}
                    className="btn btn-secondary px-4 py-2 disabled:opacity-30"
                  >
                    Sau →
                  </button>
                </div>
              </div>
            ) : (
                <div className="text-center py-12 text-gray-400 italic">Không có dữ liệu cho các tiêu chí lọc này</div>
            )}
          </div>
          
          {/* Footer */}
          {/* <div className="bg-gray-50 border-t p-4 px-6 flex justify-between items-center">
            <p className="text-sm text-gray-500 italic">
              * Dữ liệu drill-through được truy xuất trực tiếp từ Fact_Sales theo bộ lọc hiện tại.
            </p>
            <div className="flex gap-3">
               <button 
                onClick={() => {
                  updateTopLevelFilter('customer_key', selectedCustKey.toString())
                  handleClose()
                }}
                className="bg-[#799351]/15 text-[#3A5A40] px-4 py-2 rounded-lg font-bold hover:bg-[#799351]/25 transition-colors"
              >
                  Lọc toàn Dashboard theo khách này
               </button>
               <button onClick={handleClose} className="btn btn-primary px-8 py-2">Đóng</button>
            </div>
          </div> */}
        </div>
      </div>
    )
  }

  // ============================================================================
  // RENDER
  // ============================================================================
  return (
    <div className="space-y-6 animate-fade-in">
      {/* Modals */}
      <CustomerDrillThroughModal />

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

      {/* Rankings Section */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <Top5ProductsTable />
        <Bottom5ProductsTable />
        <TopCustomersTable />
      </div>

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
