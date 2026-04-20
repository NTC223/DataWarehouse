import React, { useState, useEffect, useCallback, useRef } from 'react'
import axios from 'axios'
import { useDashboardStore, useOlapStore, formatCurrency } from '../stores/store'

const api = axios.create({
  baseURL: '/api',
  timeout: 15000
})

/**
 * Autocomplete tìm customer_key hoặc store_key; gọi API kèm filter cha (state, city, type).
 */
function KeyAutocomplete({
  label,
  placeholder,
  value,
  disabled,
  buildParams,
  fetchUrl,
  idKey,
  formatLabel
}) {
  const [q, setQ] = useState('')
  const [open, setOpen] = useState(false)
  const [results, setResults] = useState([])
  const wrapRef = useRef(null)

  useEffect(() => {
    if (!open || disabled) {
      setResults([])
      return
    }
    const t = setTimeout(async () => {
      if (!q.trim()) {
        setResults([])
        return
      }
      try {
        const { data } = await api.get(fetchUrl, {
          params: { q: q.trim(), limit: 15, ...buildParams() }
        })
        setResults(Array.isArray(data?.results) ? data.results : [])
      } catch {
        setResults([])
      }
    }, 280)
    return () => clearTimeout(t)
  }, [q, open, disabled, fetchUrl, buildParams])

  useEffect(() => {
    const onDoc = (e) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    return () => document.removeEventListener('mousedown', onDoc)
  }, [])

  const selected = value && value !== 'All'

  return (
    <div className="relative mt-2" ref={wrapRef}>
      <label className="block text-xs font-semibold text-gray-600 mb-1">{label}</label>
      <input
        type="text"
        placeholder={placeholder}
        value={open ? q : selected ? String(value) : ''}
        disabled={disabled}
        onChange={(e) => {
          setQ(e.target.value)
          setOpen(true)
        }}
        onFocus={() => {
          setOpen(true)
          if (!selected) setQ('')
        }}
        className="w-full px-3 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351] disabled:bg-gray-100"
      />
      {open && results.length > 0 && (
        <div className="absolute z-30 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-44 overflow-auto">
          {results.map((row) => {
            const id = row[idKey]
            return (
              <button
                key={id}
                type="button"
                className="w-full px-3 py-2 text-left text-xs hover:bg-gray-100"
                onClick={() => {
                  formatLabel.onSelect(id)
                  setQ('')
                  setOpen(false)
                }}
              >
                {formatLabel.display(row)}
              </button>
            )
          })}
        </div>
      )}
    </div>
  )
}

function Sidebar() {
  const dashboardStore = useDashboardStore()
  const olapStore = useOlapStore()
  const activeCube = olapStore.activeCube

  const {
    activeTab,
    globalFilter,
    data,
    updateFilterSection,
    updateTopLevelFilter,
    resetFilter
  } = dashboardStore

  const isExplorer = activeTab === 'explorer'

  // Thống nhất 1 nguồn dữ liệu: DashboardStore
  const currentFilters = globalFilter
  const onUpdateSection = updateFilterSection
  const onUpdateTopLevel = updateTopLevelFilter
  const onReset = resetFilter

  // [SYNC LOGIC] Tự động fetch data cho Explorer khi filter thay đổi
  useEffect(() => {
    if (activeTab === 'explorer' && (olapStore.fields.rows.length > 0 || olapStore.fields.columns.length > 0)) {
      olapStore.fetchPivotData(1)
    }
  }, [globalFilter, activeTab])

  const [productSearch, setProductSearch] = useState('')
  const [filteredProducts, setFilteredProducts] = useState([])

  const filterOptions = data.filterOptions

  const getFilterValue = (section, key) => {
    if (section === 'time') return currentFilters.time?.[key] ?? 'All'
    if (section === 'customer') return currentFilters.customer?.[key] ?? 'All'
    if (section === 'store') return currentFilters.store?.[key] ?? 'All'
    return 'All'
  }

  const getTopLevelValue = (key) => {
    if (key === 'customer_type') return currentFilters.customer?.customer_type ?? 'All'
    if (key === 'customer_key') return String(currentFilters.customer?.customer_key ?? 'All')
    if (key === 'store_key') return String(currentFilters.store?.store_key ?? 'All')
    return currentFilters[key] ?? 'All'
  }

  const showSalesCustomerBlock =
    activeTab === 'dashboard' || (isExplorer && activeCube === 'sales')
  const showInventoryStoreBlock =
    activeTab === 'inventory' || (isExplorer && activeCube === 'inventory')

  const geoStateForCities = (() => {
    if (activeTab === 'inventory') return globalFilter.store?.state ?? 'All'
    return globalFilter.customer?.state ?? 'All'
  })()

  useEffect(() => {
    if (filterOptions?.product?.products && productSearch) {
      const filtered = filterOptions.product.products.filter(p =>
        p.product_key.toString().includes(productSearch) ||
        p.product_name.toLowerCase().includes(productSearch.toLowerCase())
      )
      setFilteredProducts(filtered.slice(0, 10))
    } else {
      setFilteredProducts([])
    }
  }, [productSearch, filterOptions])

  const getAvailableQuarters = () => {
    const year = getFilterValue('time', 'year')
    if (!filterOptions?.time) return [1, 2, 3, 4]
    if (year === 'All') return [1, 2, 3, 4]
    return [1, 2, 3, 4]
  }

  const getAvailableMonths = () => {
    const year = getFilterValue('time', 'year')
    const quarter = getFilterValue('time', 'quarter')

    if (!filterOptions?.time) return Array.from({ length: 12 }, (_, i) => i + 1)
    if (year === 'All') return Array.from({ length: 12 }, (_, i) => i + 1)

    if (quarter === '1') return [1, 2, 3]
    if (quarter === '2') return [4, 5, 6]
    if (quarter === '3') return [7, 8, 9]
    if (quarter === '4') return [10, 11, 12]

    return Array.from({ length: 12 }, (_, i) => i + 1)
  }

  const getAvailableCities = () => {
    const st = geoStateForCities
    if (!filterOptions?.location) return []
    if (st === 'All') return []
    return filterOptions.location.cities_by_state[st] || []
  }

  const customerSearchParams = useCallback(() => {
    const c = globalFilter.customer || {}
    return {
      state: c.state === 'All' ? undefined : c.state,
      city: c.city === 'All' ? undefined : c.city,
      customer_type: c.customer_type === 'All' ? undefined : c.customer_type
    }
  }, [globalFilter])

  const storeSearchParams = useCallback(() => {
    const s = globalFilter.store || {}
    return {
      state: s.state === 'All' ? undefined : s.state,
      city: s.city === 'All' ? undefined : s.city
    }
  }, [globalFilter])

  if (!filterOptions) {
    return (
      <aside className="fixed left-0 top-0 h-full w-72 bg-[#C0DAAA] border-r border-gray-200 overflow-y-auto">
        <div className="p-6">
          <div className="animate-pulse">
            <div className="h-8 bg-gray-200 rounded w-3/4 mb-6"></div>
            <div className="space-y-4">
              <div className="h-4 bg-gray-200 rounded w-1/2"></div>
              <div className="h-10 bg-gray-200 rounded"></div>
            </div>
          </div>
        </div>
      </aside>
    )
  }

  const timeYear = getFilterValue('time', 'year')
  const timeStateAll = timeYear === 'All' && !isExplorer

  return (
    <aside className="fixed left-0 top-0 h-full w-72 bg-[#C0DAAA] border-r border-gray-200 overflow-y-auto z-20">
      <div className="p-5 flex flex-col h-full space-y-6">
        <div className='flex items-center justify-center'>
          <h2 className="text-lg font-bold text-gray-900 mb-8">📊 Bộ lọc {isExplorer ? '(OLAP)' : ''}</h2>
        </div>

        <div>
          <h3 className="text-xs font-bold text-gray-700 uppercase tracking-wide mb-3">
            🕐 Thời gian
          </h3>
          <div className="grid grid-cols-2 gap-3 mb-3">
            <div>
              <label className="block text-xs font-semibold text-gray-600 mb-1">Năm</label>
              <select
                value={getFilterValue('time', 'year')}
                onChange={(e) => onUpdateSection('time', {
                  year: e.target.value,
                  quarter: 'All',
                  month: 'All'
                })}
                className="w-full px-2 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351]"
              >
                <option value="All">Tất cả năm</option>
                {filterOptions.time?.years.map(year => (
                  <option key={year} value={year.toString()}>{year}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs font-semibold text-gray-600 mb-1">Quý</label>
              <select
                value={getFilterValue('time', 'quarter')}
                onChange={(e) => onUpdateSection('time', {
                  quarter: e.target.value,
                  month: 'All'
                })}
                disabled={timeStateAll}
                className="w-full px-2 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351] disabled:bg-gray-100"
              >
                <option value="All">Tất cả</option>
                {getAvailableQuarters().map(q => (
                  <option key={q} value={q.toString()}>Quý {q}</option>
                ))}
              </select>
            </div>
          </div>

          <div>
            <label className="block text-xs font-semibold text-gray-600 mb-1">Tháng</label>
            <select
              value={getFilterValue('time', 'month')}
              onChange={(e) => onUpdateSection('time', { month: e.target.value })}
              disabled={timeStateAll}
              className="w-full px-2 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351] disabled:bg-gray-100"
            >
              <option value="All">Tất cả tháng</option>
              {getAvailableMonths().map(m => (
                <option key={m} value={m.toString()}>Tháng {m}</option>
              ))}
            </select>
          </div>
        </div>

        {showSalesCustomerBlock && (
          <div>
            <h3 className="text-xs font-bold text-gray-700 uppercase tracking-wide mb-3">
              👥 Khách hàng
            </h3>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs font-semibold text-gray-600 mb-1">Bang/State</label>
                <select
                  value={getFilterValue('customer', 'state')}
                  onChange={(e) => {
                    const newState = e.target.value
                    const newCity = 'All'
                    const newCustomerType = 'All'
                    const newCustomerKey = 'All'
                    onUpdateSection('customer', { state: newState, city: newCity })
                    onUpdateTopLevel('customer_type', newCustomerType)
                    onUpdateTopLevel('customer_key', newCustomerKey)
                  }}
                  className="w-full px-2 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351]"
                >
                  <option value="All">Tất cả</option>
                  {filterOptions.location?.states.map(st => (
                    <option key={st} value={st}>{st}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-xs font-semibold text-gray-600 mb-1">Thành phố</label>
                <select
                  value={getFilterValue('customer', 'city')}
                  onChange={(e) => {
                    onUpdateSection('customer', { city: e.target.value })
                    onUpdateTopLevel('customer_key', 'All')
                  }}
                  disabled={getFilterValue('customer', 'state') === 'All'}
                  className="w-full px-2 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351] disabled:bg-gray-100"
                >
                  <option value="All">Tất cả</option>
                  {getAvailableCities().map(city => (
                    <option key={city} value={city}>{city}</option>
                  ))}
                </select>
              </div>
            </div>

            <div className="flex flex-wrap gap-x-4 gap-y-3 mt-3">
              <label className="flex items-center">
                <input
                  type="radio"
                  name="customer_type"
                  value="All"
                  checked={getTopLevelValue('customer_type') === 'All'}
                  onChange={(e) => {
                    onUpdateTopLevel('customer_type', e.target.value)
                  }}
                  className="w-3.5 h-3.5 text-[#799351]"
                />
                <span className="ml-1.5 text-xs text-gray-700">Tất cả</span>
              </label>
              {filterOptions.customer?.customer_types.map(type => (
                <label key={type} className="flex items-center">
                  <input
                    type="radio"
                    name="customer_type"
                    value={type}
                    checked={getTopLevelValue('customer_type') === type}
                    onChange={(e) => {
                      onUpdateSection('customer', { state: 'All', city: 'All' })
                      onUpdateTopLevel('customer_type', e.target.value)
                      onUpdateTopLevel('customer_key', 'All')
                    }}
                    className="w-3.5 h-3.5 text-[#799351]"
                  />
                  <span className="ml-1.5 text-xs text-gray-700">
                    {type === 'Tourist' ? 'Du lịch' : type === 'Both' ? 'Cả hai' : 'Bưu điện'}
                  </span>
                </label>
              ))}
            </div>

            <KeyAutocomplete
              label="Mã khách (Customer Key)"
              placeholder="Gõ để tìm..."
              value={getTopLevelValue('customer_key')}
              disabled={false}
              buildParams={customerSearchParams}
              fetchUrl="/filters/customers/search"
              idKey="customer_key"
              formatLabel={{
                display: (row) => `KH #${row.customer_key}`,
                onSelect: (id) => {
                  onUpdateSection('customer', { state: 'All', city: 'All' })
                  onUpdateTopLevel('customer_type', 'All')
                  onUpdateTopLevel('customer_key', String(id))
                }
              }}
            />
            {getTopLevelValue('customer_key') !== 'All' && (
              <div className="mt-1 flex items-center justify-between bg-[#EAF1EB] px-3 py-1 rounded-lg">
                <span className="text-xs font-medium text-[#799351]">Đang chọn: {getTopLevelValue('customer_key')}</span>
                <button type="button" onClick={() => onUpdateTopLevel('customer_key', 'All')} className="text-[#799351] text-xs">✕</button>
              </div>
            )}
          </div>
        )}

        {showInventoryStoreBlock && (
          <div>
            <h3 className="text-xs font-bold text-gray-700 uppercase tracking-wide mb-3">
              🏪 Cửa hàng
            </h3>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs font-semibold text-gray-600 mb-1">Bang/State</label>
                <select
                  value={getFilterValue('store', 'state')}
                  onChange={(e) => {
                    onUpdateSection('store', { state: e.target.value, city: 'All' })
                  }}
                  className="w-full px-2 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351]"
                >
                  <option value="All">Tất cả</option>
                  {filterOptions.location?.states.map(st => (
                    <option key={st} value={st}>{st}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-xs font-semibold text-gray-600 mb-1">Thành phố</label>
                <select
                  value={getFilterValue('store', 'city')}
                  onChange={(e) => onUpdateSection('store', { city: e.target.value })}
                  disabled={getFilterValue('store', 'state') === 'All'}
                  className="w-full px-2 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351] disabled:bg-gray-100"
                >
                  <option value="All">Tất cả</option>
                  {getAvailableCities().map(city => (
                    <option key={`s-${city}`} value={city}>{city}</option>
                  ))}
                </select>
              </div>
            </div>

            <KeyAutocomplete
              label="Mã cửa hàng (Store Key)"
              placeholder="Gõ để tìm..."
              value={getTopLevelValue('store_key')}
              disabled={false}
              buildParams={storeSearchParams}
              fetchUrl="/filters/stores/search"
              idKey="store_key"
              formatLabel={{
                display: (row) => `CH #${row.store_key}`,
                onSelect: (id) => onUpdateTopLevel('store_key', String(id))
              }}
            />
            {getTopLevelValue('store_key') !== 'All' && (
              <div className="mt-1 flex items-center justify-between bg-[#EAF1EB] px-3 py-1 rounded-lg">
                <span className="text-xs font-medium text-[#799351]">Đang chọn: {getTopLevelValue('store_key')}</span>
                <button type="button" onClick={() => onUpdateTopLevel('store_key', 'All')} className="text-[#799351] text-xs">✕</button>
              </div>
            )}
          </div>
        )}

        {activeTab !== 'inventory' && (
          <div className="flex-1">
            <h3 className="text-xs font-bold text-gray-700 uppercase tracking-wide mb-3">
              📦 Sản phẩm
            </h3>
            <div className="relative">
              <input
                type="text"
                placeholder="Tìm tên hoặc mã..."
                value={productSearch}
                onChange={(e) => setProductSearch(e.target.value)}
                className="w-full px-3 py-1.5 border border-gray-300 rounded-lg text-xs focus:ring-1 focus:ring-[#799351]"
              />
              {filteredProducts.length > 0 && (
                <div className="absolute z-10 w-full mt-1 bg-white border rounded-lg shadow-lg max-h-48 overflow-auto">
                  {filteredProducts.map(p => (
                    <button
                      key={p.product_key}
                      type="button"
                      onClick={() => {
                        onUpdateTopLevel('product_key', p.product_key.toString())
                        setProductSearch('')
                        setFilteredProducts([])
                      }}
                      className="w-full px-3 py-2 text-left text-xs hover:bg-gray-100"
                    >
                      {p.product_name}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {getTopLevelValue('product_key') !== 'All' && (
              <div className="mt-2 flex items-center justify-between bg-[#EAF1EB] px-3 py-1.5 rounded-lg">
                <span className="text-xs font-medium text-[#799351]">ID: {getTopLevelValue('product_key')}</span>
                <button type="button" onClick={() => onUpdateTopLevel('product_key', 'All')} className="text-[#799351]">✕</button>
              </div>
            )}
          </div>
        )}

        <div className="pt-2">
          <button
            type="button"
            onClick={onReset}
            className="w-full py-2 bg-gray-100 text-gray-700 rounded-lg text-xs font-bold hover:bg-gray-200"
          >
            🔄 Đặt lại bộ lọc
          </button>
        </div>
      </div>
    </aside>
  )
}

export default Sidebar
