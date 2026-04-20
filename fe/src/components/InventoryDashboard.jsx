/**
 * ============================================================================
 * INVENTORYDASHBOARD.JSX - Inventory Dashboard Component
 * ============================================================================
 * Dashboard cho phân tích tồn kho với:
 * - Overview Cards: Tổng tồn kho, số cửa hàng, số sản phẩm
 * - Scatter Risk Plot: Phân tích rủi ro toàn bộ sản phẩm
 * - Combo Chart: Phân tích chi tiết 1 sản phẩm (Drill-Across)
 * - Product Selector: Chọn sản phẩm để phân tích
 * ============================================================================
 */

import React, { useState, useEffect } from 'react'
import inventoryService from '../services/inventoryService'
import ComboChart from './ComboChart'
import ScatterRiskPlot from './ScatterRiskPlot'
import LoadingSpinner from './LoadingSpinner'
import { formatNumber, useDashboardStore } from '../stores/store'

function InventoryDashboard({ filters }) {
  // Store
  const { updateFilterSection } = useDashboardStore()

  // State
  const [overview, setOverview] = useState(null)
  const [scatterData, setScatterData] = useState(null)
  const [cityRiskData, setCityRiskData] = useState(null)
  const [selectedProduct, setSelectedProduct] = useState(null)
  const [productAnalysis, setProductAnalysis] = useState(null)
  const [products, setProducts] = useState([])
  const [productSearch, setProductSearch] = useState('')
  const [isDropdownOpen, setIsDropdownOpen] = useState(false)
  const [loading, setLoading] = useState({
    overview: false,
    scatter: false,
    cityRisk: false,
    analysis: false,
    products: false
  })

  // Xác định mức độ chi tiết biểu đồ (time level) kế thừa từ Global Filter
  const derivedTimeLevel = () => {
    if (filters.time?.year === 'All') return 'year'
    if (filters.time?.quarter === 'All') return 'quarter'
    return 'month'
  }

  const storeKeyFromFilter = () => {
    const sk = filters.store?.store_key
    if (sk === 'All' || sk == null || sk === '') return null
    const n = typeof sk === 'string' ? parseInt(sk, 10) : sk
    return Number.isFinite(n) ? n : null
  }

  // Fetch dữ liệu khi component mount hoặc filters thay đổi
  useEffect(() => {
    fetchOverview()
    fetchScatterData()
    fetchCityRiskRanking()
    fetchProducts()
  }, [
    filters.store?.city,
    filters.store?.state,
    filters.store?.store_key,
    filters.time?.year,
    filters.time?.quarter,
    filters.time?.month
  ])

  // Fetch product analysis khi selectedProduct thay đổi
  useEffect(() => {
    if (selectedProduct) {
      fetchProductAnalysis(selectedProduct)
      // Cập nhật text trong thanh search nếu click từ biểu đồ chứ không phải gõ
      const prod = products.find(p => p.product_key === selectedProduct)
      if (prod) {
        setProductSearch(prod.product_name)
      }
    }
  }, [selectedProduct, filters.store?.city, filters.store?.state, filters.store?.store_key, filters.time?.year, filters.time?.quarter, filters.time?.month, products])

  const fetchOverview = async () => {
    try {
      setLoading(prev => ({ ...prev, overview: true }))
      const city = filters.store?.city === 'All' ? null : filters.store?.city
      const state = filters.store?.state === 'All' ? null : filters.store?.state
      const year = filters.time?.year === 'All' ? null : parseInt(filters.time?.year)
      const quarter = filters.time?.quarter === 'All' ? null : parseInt(filters.time?.quarter)
      const month = filters.time?.month === 'All' ? null : parseInt(filters.time?.month)
      const store_key = storeKeyFromFilter()
      const data = await inventoryService.getOverview(city, state, year, quarter, month, store_key)
      setOverview(data)
    } catch (error) {
      console.error('Error fetching overview:', error)
    } finally {
      setLoading(prev => ({ ...prev, overview: false }))
    }
  }

  const fetchScatterData = async () => {
    try {
      setLoading(prev => ({ ...prev, scatter: true }))
      const city = filters.store?.city === 'All' ? null : filters.store?.city
      const state = filters.store?.state === 'All' ? null : filters.store?.state
      const year = filters.time?.year === 'All' ? null : parseInt(filters.time?.year)
      const quarter = filters.time?.quarter === 'All' ? null : parseInt(filters.time?.quarter)
      const month = filters.time?.month === 'All' ? null : parseInt(filters.time?.month)
      const store_key = storeKeyFromFilter()

      const data = await inventoryService.getScatterData({
        city,
        state,
        store_key,
        year,
        quarter,
        month
      })
      setScatterData(data)
    } catch (error) {
      console.error('Error fetching scatter data:', error)
    } finally {
      setLoading(prev => ({ ...prev, scatter: false }))
    }
  }

  const fetchProducts = async () => {
    try {
      setLoading(prev => ({ ...prev, products: true }))
      const data = await inventoryService.getProducts(1, 100)
      setProducts(data.products || [])
    } catch (error) {
      console.error('Error fetching products:', error)
    } finally {
      setLoading(prev => ({ ...prev, products: false }))
    }
  }

  const fetchCityRiskRanking = async () => {
    try {
      setLoading(prev => ({ ...prev, cityRisk: true }))
      const year = filters.time?.year === 'All' ? null : parseInt(filters.time?.year)
      const quarter = filters.time?.quarter === 'All' ? null : parseInt(filters.time?.quarter)
      const month = filters.time?.month === 'All' ? null : parseInt(filters.time?.month)
      
      const data = await inventoryService.getCitiesRiskRanking({
        year,
        quarter,
        month,
        limit: 3
      })
      setCityRiskData(data)
    } catch (error) {
      console.error('Error fetching city risk ranking:', error)
    } finally {
      setLoading(prev => ({ ...prev, cityRisk: false }))
    }
  }

  const fetchProductAnalysis = async (productId) => {
    try {
      setLoading(prev => ({ ...prev, analysis: true }))
      const city = filters.store?.city === 'All' ? null : filters.store?.city
      const state = filters.store?.state === 'All' ? null : filters.store?.state
      const year = filters.time?.year === 'All' ? null : parseInt(filters.time?.year)
      const quarter = filters.time?.quarter === 'All' ? null : parseInt(filters.time?.quarter)
      const month = filters.time?.month === 'All' ? null : parseInt(filters.time?.month)
      const store_key = storeKeyFromFilter()

      const data = await inventoryService.getAnalysis(productId, {
        city,
        state,
        store_key,
        year,
        quarter,
        month,
        time_level: derivedTimeLevel()
      })
      setProductAnalysis(data)
    } catch (error) {
      console.error('Error fetching product analysis:', error)
    } finally {
      setLoading(prev => ({ ...prev, analysis: false }))
    }
  }

  // KPI Cards
  const KPICards = () => {
    if (!overview) return null

    return (
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="kpi-card card-hover border-l-4 border-[#799351] bg-white transform transition-all duration-300 hover:-translate-y-1 hover:shadow-[0_10px_20px_rgba(121,147,81,0.15)] relative overflow-hidden group">
          <div className="absolute top-0 right-0 p-4 opacity-5 group-hover:opacity-10 transition-opacity">📦</div>
          <p className="kpi-label">Tổng Lượng Hàng Còn Lại</p>
          <p className="kpi-value text-[#799351] tracking-tight">
            {formatNumber(overview.total_inventory)}
          </p>
        </div>

        <div className="kpi-card card-hover border-l-4 border-[#A3B18A] bg-white transform transition-all duration-300 hover:-translate-y-1 hover:shadow-[0_10px_20px_rgba(163,177,138,0.15)] relative overflow-hidden group">
          <div className="absolute top-0 right-0 p-4 opacity-5 group-hover:opacity-10 transition-opacity">🏪</div>
          <p className="kpi-label">Số cửa hàng</p>
          <p className="kpi-value text-[#A3B18A] tracking-tight">
            {formatNumber(overview.total_stores)}
          </p>
        </div>

        <div className="kpi-card card-hover border-l-4 border-[#F4A261] bg-white transform transition-all duration-300 hover:-translate-y-1 hover:shadow-[0_10px_20px_rgba(244,162,97,0.15)] relative overflow-hidden group">
          <div className="absolute top-0 right-0 p-4 opacity-5 group-hover:opacity-10 transition-opacity">🛍️</div>
          <p className="kpi-label">Số sản phẩm</p>
          <p className="kpi-value text-[#F4A261] tracking-tight">
            {formatNumber(overview.total_products)}
          </p>
        </div>

        <div className="kpi-card card-hover border-l-4 border-[#E76F51] bg-white transform transition-all duration-300 hover:-translate-y-1 hover:shadow-[0_10px_20px_rgba(231,111,81,0.15)] relative overflow-hidden group">
          <div className="absolute top-0 right-0 p-4 opacity-5 group-hover:opacity-10 transition-opacity">📈</div>
          <p className="kpi-label">TB tồn kho/CH</p>
          <p className="kpi-value text-[#E76F51] tracking-tight">
            {formatNumber(overview.avg_inventory_per_store)}
          </p>
        </div>
      </div>
    )
  }

  // Khung lọc danh sách sản phẩm theo text
  const filteredProducts = products.filter(p =>
    p.product_name.toLowerCase().includes((productSearch || '').toLowerCase()) ||
    p.product_key.toString().includes(productSearch || '')
  ).slice(0, 50)

  return (
    <div className="space-y-6 animate-fade-in">


      {/* Extremes Tables - 4 Column Layout */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {/* Col 1: Top 5 Dư Thừa (Over-ordered) */}
        <div className="bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden">
          <div className="bg-orange-50 px-4 py-2 border-b border-orange-100">
            <h4 className="font-bold text-orange-800 flex items-center gap-2 text-sm">
              <span>⚠️</span> Top 5 Dư Thừa
            </h4>
          </div>
          <div className="overflow-x-auto">
            {loading.scatter ? (
              <div className="h-32 flex items-center justify-center text-gray-400">
                <span className="text-xs">⏳ Đang tải...</span>
              </div>
            ) : scatterData?.data && scatterData.data.length > 0 ? (
              <table className="w-full text-xs">
                <thead>
                  <tr className="bg-gray-50 text-gray-600 uppercase text-[11px]">
                    <th className="px-3 py-1.5 text-left">Sản phẩm</th>
                    <th className="px-3 py-1.5 text-right">Còn lại</th>
                    <th className="px-3 py-1.5 text-right">Bán {derivedTimeLevel() === 'month' ? '(kỳ sau)' : '(cùng kỳ)'}</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {[...scatterData.data]
                    .sort((a, b) => (b.inventory - a.inventory) || (a.sales - b.sales))
                    .slice(0, 5)
                    .map((item, idx) => (
                      <tr key={idx} className="hover:bg-orange-50 transition-colors cursor-pointer" onClick={() => setSelectedProduct(item.product_key)}>
                        <td className="px-3 py-2 font-medium text-gray-700 truncate text-[11px]">{item.product_name}</td>
                        <td className="px-3 py-2 text-right text-orange-600 font-bold text-[11px]">{formatNumber(item.inventory)}</td>
                        <td className="px-3 py-2 text-right text-gray-500 text-[11px]">{formatNumber(item.sales)}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            ) : (
              <div className="h-32 flex items-center justify-center text-gray-400">
                <span className="text-xs">Không có dữ liệu</span>
              </div>
            )}
          </div>
        </div>

        {/* Col 2: Top 3 Thành phố Đọng Vốn (Over-ordered) */}
        {cityRiskData?.overstock_cities && cityRiskData.overstock_cities.length > 0 ? (
          <div className="bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden group">
            <div className="bg-orange-50 px-4 py-2 border-b border-orange-100">
              <h4 className="font-bold text-orange-800 flex items-center gap-2 text-sm">
                <span>🏙️</span> Top 3 TP Đọng Vốn
                <div className="relative inline-block ml-auto">
                  <span className="text-gray-400 cursor-help text-xs font-normal">ℹ️</span>
                  <div className="absolute bottom-full right-0 mb-2 w-56 bg-gray-800 text-white text-[10px] rounded px-3 py-2 hidden group-hover:block whitespace-normal z-50">
                    <strong>Chỉ số Đọng Vốn:</strong> Trung bình Tồn/Bán
                    <br/>
                    VD: Chỉ số 10.7 = Tồn cao hơn Bán 10.7 lần<br/>
                    → Phí vốn lớn, cần giảm nhập
                  </div>
                </div>
              </h4>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="bg-gray-50 text-gray-600 uppercase text-[11px]">
                    <th className="px-3 py-1.5 text-left">Thành phố</th>
                    <th className="px-3 py-1.5 text-right">SL</th>
                    <th className="px-3 py-1.5 text-right">Chỉ số</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {cityRiskData.overstock_cities.map((city, idx) => (
                    <tr 
                      key={idx} 
                      className="hover:bg-orange-50 transition-colors cursor-pointer" 
                      onClick={() => updateFilterSection('store', { city: city.city, state: city.state || 'All' })}
                    >
                      <td className="px-3 py-2 font-medium text-gray-700 truncate text-[11px]">{city.city}</td>
                      <td className="px-3 py-2 text-right text-orange-600 font-bold text-[11px]">{city.overstock_count}</td>
                      <td className="px-3 py-2 text-right text-orange-600 text-[11px]">{city.overstock_score}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden">
            <div className="bg-orange-50 px-4 py-2 border-b border-orange-100">
              <h4 className="font-bold text-orange-800 flex items-center gap-2 text-sm">
                <span>🏙️</span> Top 3 TP Đọng Vốn
              </h4>
            </div>
            <div className="h-32 flex items-center justify-center text-gray-400">
              <span className="text-xs">Không có dữ liệu</span>
            </div>
          </div>
        )}

        {/* Col 3: Top 3 Thành phố Đứt Gãy (Critical Low) */}
        {cityRiskData?.understock_cities && cityRiskData.understock_cities.length > 0 ? (
          <div className="bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden group">
            <div className="bg-red-50 px-4 py-2 border-b border-red-100">
              <h4 className="font-bold text-red-800 flex items-center gap-2 text-sm">
                <span>🏙️</span> Top 3 TP Đứt Gãy
                <div className="relative inline-block ml-auto">
                  <span className="text-gray-400 cursor-help text-xs font-normal">ℹ️</span>
                  <div className="absolute bottom-full right-0 mb-2 w-56 bg-gray-800 text-white text-[10px] rounded px-3 py-2 hidden group-hover:block whitespace-normal z-50">
                    <strong>Chỉ số Đứt Gãy:</strong> Trung bình Bán/Tồn
                    <br/>
                    VD: Chỉ số 50.09 = Bán cao hơn Tồn 50x<br/>
                    → Nguy cơ đứt gãy lớn, tồn sát 0
                  </div>
                </div>
              </h4>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="bg-gray-50 text-gray-600 uppercase text-[11px]">
                    <th className="px-3 py-1.5 text-left">Thành phố</th>
                    <th className="px-3 py-1.5 text-right">SL</th>
                    <th className="px-3 py-1.5 text-right">Chỉ số</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {cityRiskData.understock_cities.map((city, idx) => (
                    <tr 
                      key={idx} 
                      className="hover:bg-red-50 transition-colors cursor-pointer" 
                      onClick={() => updateFilterSection('store', { city: city.city, state: city.state || 'All' })}
                    >
                      <td className="px-3 py-2 font-medium text-gray-700 truncate text-[11px]">{city.city}</td>
                      <td className="px-3 py-2 text-right text-red-600 font-bold text-[11px]">{city.understock_count}</td>
                      <td className="px-3 py-2 text-right text-red-600 text-[11px]">{city.understock_score}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div className="bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden">
            <div className="bg-red-50 px-4 py-2 border-b border-red-100">
              <h4 className="font-bold text-red-800 flex items-center gap-2 text-sm">
                <span>🏙️</span> Top 3 TP Đứt Gãy
              </h4>
            </div>
            <div className="h-32 flex items-center justify-center text-gray-400">
              <span className="text-xs">Không có dữ liệu</span>
            </div>
          </div>
        )}

        {/* Col 4: Top 5 Nguy Cơ Thiếu Hụt (Critical Low) */}
        <div className="bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden">
          <div className="bg-red-50 px-4 py-2 border-b border-red-100">
            <h4 className="font-bold text-red-800 flex items-center gap-2 text-sm">
              <span>🚨</span> Top 5 Nguy Cơ Thiếu Hụt
            </h4>
          </div>
          <div className="overflow-x-auto">
            {loading.scatter ? (
              <div className="h-32 flex items-center justify-center text-gray-400">
                <span className="text-xs">⏳ Đang tải...</span>
              </div>
            ) : scatterData?.data && scatterData.data.length > 0 ? (
              <table className="w-full text-xs">
                <thead>
                  <tr className="bg-gray-50 text-gray-600 uppercase text-[11px]">
                    <th className="px-3 py-1.5 text-left">Sản phẩm</th>
                    <th className="px-3 py-1.5 text-right">Còn lại</th>
                    <th className="px-3 py-1.5 text-right">Bán {derivedTimeLevel() === 'month' ? '(kỳ sau)' : '(cùng kỳ)'}</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {[...scatterData.data]
                    .sort((a, b) => (a.inventory - b.inventory) || (b.sales - a.sales))
                    .slice(0, 5)
                    .map((item, idx) => (
                      <tr key={idx} className="hover:bg-red-50 transition-colors cursor-pointer" onClick={() => setSelectedProduct(item.product_key)}>
                        <td className="px-3 py-2 font-medium text-gray-700 truncate text-[11px]">{item.product_name}</td>
                        <td className="px-3 py-2 text-right text-red-600 font-bold text-[11px]">{formatNumber(item.inventory)}</td>
                        <td className="px-3 py-2 text-right text-blue-600 font-bold text-[11px]">{formatNumber(item.sales)}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            ) : (
              <div className="h-32 flex items-center justify-center text-gray-400">
                <span className="text-xs">Không có dữ liệu</span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Scatter Risk Plot */}
      <div className="bg-white rounded-xl shadow-lg shadow-gray-200/40 border border-gray-100 p-5 transform transition-all duration-300 hover:shadow-xl hover:shadow-gray-200/60">
        <div className="flex items-center justify-between mb-4 border-b border-gray-100 pb-3">
          <h3 className="text-lg font-bold text-[#3A5A40] flex items-center gap-2">
            <span>🗺️</span> Bản đồ Độ đáp ứng theo Sản phẩm
          </h3>
          <button
            onClick={fetchScatterData}
            disabled={loading.scatter}
            className="btn bg-[#f1f5f9] hover:bg-[#e2e8f0] text-gray-700 text-sm disabled:opacity-50 font-medium px-4 py-2 rounded-lg transition-colors shadow-sm"
          >
            {loading.scatter ? '⏳' : '🔄'} Làm mới
          </button>
        </div>

        {loading.scatter ? (
          <div className="h-96 flex items-center justify-center">
            <LoadingSpinner size="lg" />
          </div>
        ) : (
          <ScatterRiskPlot
            data={scatterData?.data}
            summary={scatterData?.summary}
            city={scatterData?.city}
            timeLevel={derivedTimeLevel()}
            onPointClick={setSelectedProduct}
          />
        )}
      </div>

      {/* Product Analysis Section */}
      <div className="bg-white rounded-xl shadow-lg shadow-gray-200/40 border border-gray-100 p-5 transform transition-all duration-300 hover:shadow-xl hover:shadow-gray-200/60">
        <div className="flex flex-wrap items-center justify-between gap-4 mb-4 border-b border-gray-100 pb-3">
          <h3 className="text-lg font-bold text-[#3A5A40] flex items-center gap-2">
            <span>🔍</span> Phân tích chi tiết sản phẩm
          </h3>

          <div className="flex items-center gap-3">
            {/* Searchable Product Selector */}
            <div className="relative w-64 z-20">
              <input
                type="text"
                placeholder="🔍 Tìm tên hoặc ID sản phẩm..."
                value={productSearch}
                onChange={(e) => {
                  setProductSearch(e.target.value)
                  setIsDropdownOpen(true)
                }}
                onFocus={() => setIsDropdownOpen(true)}
                onBlur={() => setTimeout(() => setIsDropdownOpen(false), 200)}
                className="w-full px-3 py-2 border border-blue-300 rounded-lg text-sm shadow-sm focus:ring-2 focus:ring-blue-500 outline-none font-medium text-blue-900"
              />
              {isDropdownOpen && filteredProducts.length > 0 && (
                <div className="absolute top-full left-0 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-xl max-h-60 overflow-y-auto">
                  {filteredProducts.map(p => (
                    <div
                      key={p.product_key}
                      onClick={() => {
                        setSelectedProduct(p.product_key)
                        setProductSearch(p.product_name)
                        setIsDropdownOpen(false)
                      }}
                      className="px-3 py-2 text-sm text-gray-700 hover:bg-blue-50 hover:text-blue-700 cursor-pointer border-b border-gray-100 last:border-0 font-medium transition-colors"
                    >
                      {p.product_name}
                    </div>
                  ))}
                  {filteredProducts.length === 50 && (
                    <div className="text-xs text-center text-gray-400 p-2 italic">Chỉ hiển thị 50 kết quả đầu tiên</div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>

        {selectedProduct ? (
          loading.analysis ? (
            <div className="h-80 flex items-center justify-center">
              <LoadingSpinner size="lg" />
            </div>
          ) : productAnalysis ? (
            <ComboChart
              data={productAnalysis.data}
              productName={productAnalysis.product_name}
              timeLevel={derivedTimeLevel()}
            />
          ) : null
        ) : (
          <div className="h-80 flex items-center justify-center bg-gray-50 rounded-lg">
            <div className="text-center text-gray-500">
              <p className="text-lg mb-2">📦 Chọn sản phẩm để phân tích</p>
              <p className="text-sm">Hoặc click vào điểm trong biểu đồ scatter ở trên</p>
            </div>
          </div>
        )}
      </div>

      {/* Info Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
        <div className="bg-gradient-to-br from-[#F8FDF4] to-white rounded-xl p-5 border border-[#C0DAAA] shadow-sm transform transition-all duration-200 hover:-translate-y-1 hover:shadow-md">
          <h4 className="font-bold text-[#588157] mb-2 flex items-center gap-2"><span>🔗</span> Drill-Across</h4>
          <p className="text-sm text-[#3A5A40]/80">
            Kết hợp dữ liệu từ Cube Sales và Cube Inventory để có cái nhìn toàn diện
            về mối quan hệ giữa bán hàng và tồn kho.
          </p>
        </div>

        <div className="bg-gradient-to-br from-[#799351]/10 to-white rounded-xl p-5 border border-[#799351]/20 shadow-sm transform transition-all duration-200 hover:-translate-y-1 hover:shadow-md">
          <h4 className="font-bold text-[#799351] mb-2 flex items-center gap-2"><span>📊</span> Độ đáp ứng (Coverage Ratio)</h4>
          <p className="text-sm text-[#3A5A40]/80">
            Độ đáp ứng = Lượng hàng {derivedTimeLevel() === 'month' ? 'hiện tại / Số lượng bán ra của kỳ tiếp theo' : 'hiện tại / Số lượng bán ra của cùng kỳ'}. Chỉ số này cho biết khả năng đáp ứng nhu cầu {derivedTimeLevel() === 'month' ? 'tương lai' : 'hiện tại'} từ lượng hàng tồn kho.
          </p>
        </div>

        <div className="bg-gradient-to-br from-[#F4A261]/10 to-white rounded-xl p-5 border border-[#F4A261]/30 shadow-sm transform transition-all duration-200 hover:-translate-y-1 hover:shadow-md">
          <h4 className="font-bold text-[#D68C45] mb-2 flex items-center gap-2"><span>⚠️</span> Phân loại rủi ro</h4>
          <div className="text-sm text-[#3A5A40]/80 space-y-2 mt-1">
            <span className="block"><strong className="text-[#F4A261]">Dư thừa (Overstock):</strong> Lượng hàng còn lại vượt quá 120% nhu cầu {derivedTimeLevel() === 'month' ? 'của tháng sau' : 'cùng kỳ'}. Cần xem xét giảm nhập.</span>
            <span className="block"><strong className="text-[#E76F51]">Nguy cơ thiếu hụt (Understock):</strong> Lượng hàng không đủ đáp ứng nhu cầu {derivedTimeLevel() === 'month' ? 'tháng sau' : 'cùng kỳ'} (Độ đáp ứng &lt; 80%). Cần nhập thêm.</span>
            <span className="block"><strong className="text-[#799351]">Tối ưu (Optimal):</strong> Lượng hàng vừa đủ, dao động từ 80% - 120%.</span>
          </div>
        </div>
      </div>
    </div>
  )
}

export default InventoryDashboard
