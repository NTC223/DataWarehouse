/**
 * ============================================================================
 * STORES/STORE.JS - Zustand Global State Management
 * ============================================================================
 * File quản lý global state cho toàn bộ ứng dụng Dashboard.
 * 
 * State chính:
 * - globalFilter: Bộ lọc toàn cục (Time, Customer hierarchy, Store hierarchy, Product)
 * - activeTab: Tab đang active (dashboard | explorer)
 * - loading: Trạng thái loading
 * - data: Dữ liệu cache từ API
 * 
 * Actions:
 * - updateFilter: Cập nhật filter
 * - resetFilter: Reset filter về mặc định
 * - setActiveTab: Chuyển tab
 * - fetchOverview: Lấy dữ liệu overview
 * - fetchTrend: Lấy dữ liệu trend
 * - fetchTopProducts: Lấy top/bottom products
 * - fetchCustomerSegment: Lấy phân khúc khách hàng
 * ============================================================================
 */

import { create } from 'zustand'
import { devtools, persist } from 'zustand/middleware'
import axios from 'axios'

// Cấu hình API client
const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// ============================================================================
// INITIAL STATE & CASCADING FILTER HELPERS
// ============================================================================
const ALL = 'All'

const initialFilterState = {
  time: {
    year: ALL,
    quarter: ALL,
    month: ALL
  },
  /** Sales: địa lý + loại KH + mã KH (hierarchy trong dimension Customer) */
  customer: {
    state: ALL,
    city: ALL,
    customer_type: ALL,
    customer_key: ALL
  },
  /** Inventory: địa lý + mã cửa hàng (hierarchy trong dimension Store) */
  store: {
    state: ALL,
    city: ALL,
    store_key: ALL
  },
  product_key: ALL
}

/**
 * Cascading reset Sales: đổi state → reset city + customer_key;
 * đổi city hoặc customer_type → reset customer_key.
 */
function mergeCustomer(prev, patch) {
  const base = prev ?? { ...initialFilterState.customer }
  const stateChanged = patch.state !== undefined && patch.state !== base.state
  const next = { ...base, ...patch }
  if (stateChanged) {
    next.city = ALL
    next.customer_key = ALL
  } else {
    const cityChanged = patch.city !== undefined && patch.city !== base.city
    const typeChanged =
      patch.customer_type !== undefined && patch.customer_type !== base.customer_type
    if (cityChanged || typeChanged) {
      next.customer_key = ALL
    }
  }
  return next
}

/**
 * Cascading reset Inventory: đổi state → reset city + store_key;
 * đổi city → reset store_key.
 */
function mergeStore(prev, patch) {
  const base = prev ?? { ...initialFilterState.store }
  const stateChanged = patch.state !== undefined && patch.state !== base.state
  const next = { ...base, ...patch }
  if (stateChanged) {
    next.city = ALL
    next.store_key = ALL
  } else {
    const cityChanged = patch.city !== undefined && patch.city !== base.city
    if (cityChanged) {
      next.store_key = ALL
    }
  }
  return next
}

function mergeGlobalFilter(prev, patch) {
  const next = { ...prev }
  if (patch.time !== undefined) {
    next.time = { ...(prev.time ?? initialFilterState.time), ...patch.time }
  }
  if (patch.customer !== undefined) {
    next.customer = mergeCustomer(prev.customer, patch.customer)
  }
  if (patch.store !== undefined) {
    next.store = mergeStore(prev.store, patch.store)
  }
  if (patch.product_key !== undefined) {
    next.product_key = patch.product_key
  }
  return next
}

/** Map globalFilter → payload backend DashboardFilter (time + customer + product_key). */
export function globalFilterToDashboardPayload(globalFilter) {
  const c = globalFilter?.customer ?? initialFilterState.customer
  const time = globalFilter?.time ?? initialFilterState.time
  return {
    time,
    customer: {
      state: c.state ?? 'All',
      city: c.city ?? 'All',
      customer_type: c.customer_type ?? 'All',
      customer_key: c.customer_key ?? 'All'
    },
    product_key: globalFilter?.product_key ?? 'All'
  }
}
const OLAP_SALES_AVAILABLE_FIELDS = [
  { id: 'year', name: 'Năm', category: 'time', column: 'year' },
  { id: 'quarter', name: 'Quý', category: 'time', column: 'quarter' },
  { id: 'month', name: 'Tháng', category: 'time', column: 'month' },
  { id: 'product_key', name: 'Sản phẩm', category: 'product', column: 'product_key' },
  { id: 'customer_type', name: 'Loại KH', category: 'customer', column: 'customer_type' },
  { id: 'customer_key', name: 'Mã KH', category: 'customer', column: 'customer_key' },
  { id: 'state', name: 'Bang', category: 'customer', column: 'state' },
  { id: 'city', name: 'Thành phố', category: 'customer', column: 'city' }
]

const OLAP_INVENTORY_AVAILABLE_FIELDS = [
  { id: 'year', name: 'Năm', category: 'time', column: 'year' },
  { id: 'quarter', name: 'Quý', category: 'time', column: 'quarter' },
  { id: 'month', name: 'Tháng', category: 'time', column: 'month' },
  { id: 'product_key', name: 'Sản phẩm', category: 'product', column: 'product_key' },
  { id: 'store_key', name: 'Cửa hàng', category: 'store', column: 'store_key' },
  { id: 'state', name: 'Bang', category: 'store', column: 'state' },
  { id: 'city', name: 'Thành phố', category: 'store', column: 'city' }
]

// ============================================================================
// OLAP HIERARCHY CONFIG
// ============================================================================
// Mỗi dimension có axis ('rows' | 'columns' | null) và danh sách fields đang chọn.
// Khi chọn/bỏ chọn field → tự derive rows/columns fields gửi backend.
// customer_type và customer_location: mutual exclusion (chỉ chọn 1 trong 2).

export const CUBE_HIERARCHIES = {
  sales: [
    {
      dimensionId: 'time',
      dimensionLabel: '🕐 Thời gian',
      hierarchies: [
        {
          id: 'time',
          label: 'Thời gian',
          fields: [
            { id: 'year', name: 'Năm', column: 'year' },
            { id: 'quarter', name: 'Quý', column: 'quarter' },
            { id: 'month', name: 'Tháng', column: 'month' }
          ]
        }
      ]
    },
    {
      dimensionId: 'product',
      dimensionLabel: '📦 Sản phẩm',
      hierarchies: [
        {
          id: 'product',
          label: 'Sản phẩm',
          fields: [{ id: 'product_key', name: 'Sản phẩm', column: 'product_key' }]
        }
      ]
    },
    {
      dimensionId: 'customer',
      dimensionLabel: '👥 Khách hàng',
      hierarchies: [
        {
          id: 'customer_type',
          label: 'Loại KH',
          fields: [
            { id: 'customer_type', name: 'Loại KH', column: 'customer_type' },
            { id: 'customer_key', name: 'Mã KH', column: 'customer_key' }
          ]
        },
        {
          id: 'customer_location',
          label: 'Địa lý KH',
          fields: [
            { id: 'state', name: 'Bang', column: 'state' },
            { id: 'city', name: 'Thành phố', column: 'city' },
            { id: 'customer_key', name: 'Mã KH', column: 'customer_key' }
          ]
        }
      ]
    }
  ],
  inventory: [
    {
      dimensionId: 'time',
      dimensionLabel: '🕐 Thời gian',
      hierarchies: [
        {
          id: 'time',
          label: 'Thời gian',
          fields: [
            { id: 'year', name: 'Năm', column: 'year' },
            { id: 'quarter', name: 'Quý', column: 'quarter' },
            { id: 'month', name: 'Tháng', column: 'month' }
          ]
        }
      ]
    },
    {
      dimensionId: 'product',
      dimensionLabel: '📦 Sản phẩm',
      hierarchies: [
        {
          id: 'product',
          label: 'Sản phẩm',
          fields: [{ id: 'product_key', name: 'Sản phẩm', column: 'product_key' }]
        }
      ]
    },
    {
      dimensionId: 'store',
      dimensionLabel: '🏪 Cửa hàng',
      hierarchies: [
        {
          id: 'store',
          label: 'Cửa hàng',
          fields: [
            { id: 'state', name: 'Bang', column: 'state' },
            { id: 'city', name: 'Thành phố', column: 'city' },
            { id: 'store_key', name: 'Mã cửa hàng', column: 'store_key' }
          ]
        }
      ]
    }
  ]
}

// Map fieldId → hierarchyId (dùng để resolve mutex)
const FIELD_TO_HIERARCHY = {}
for (const cube of Object.values(CUBE_HIERARCHIES)) {
  for (const dim of cube) {
    for (const h of dim.hierarchies) {
      for (const f of h.fields) {
        FIELD_TO_HIERARCHY[f.id] = h.id
      }
    }
  }
}

// Mutex pairs: hierarchyId ↔ hierarchyId
const HIERARCHY_MUTEX = {
  customer_type: 'customer_location',
  customer_location: 'customer_type'
}

// Lấy tất cả field ids thuộc một hierarchy
function getFieldsOfHierarchy(cube, hierarchyId) {
  const dims = CUBE_HIERARCHIES[cube] ?? []
  for (const dim of dims) {
    for (const h of dim.hierarchies) {
      if (h.id === hierarchyId) return h.fields.map(f => f.id)
    }
  }
  return []
}

// Map fieldId → dimensionId (auto-generated từ CUBE_HIERARCHIES)
const FIELD_TO_DIMENSION = {}
for (const [cube, dims] of Object.entries(CUBE_HIERARCHIES)) {
  for (const dim of dims) {
    for (const h of dim.hierarchies) {
      for (const f of h.fields) {
        FIELD_TO_DIMENSION[f.id] = dim.dimensionId
      }
    }
  }
}

// Lấy filter từ globalFilter, chỉ gửi field thuộc hierarchy đang active trong olapState.
// Giải quyết conflict: nếu user dùng customer_type hierarchy → bỏ state/city,
// nếu user dùng customer_location hierarchy → bỏ customer_type.
function getOlapFiltersFromGlobalFilter(cube, olapState, globalFilter) {
  const cubeState = olapState[cube] ?? {}
  const result = {}

  for (const [dimId, dimState] of Object.entries(cubeState)) {
    const selected = Array.from(dimState.selected ?? [])
    if (selected.length === 0) continue

    const dimValue = globalFilter[dimId]
    if (!dimValue) continue

    for (const fieldId of selected) {
      if (fieldId in dimValue) {
        const val = dimValue[fieldId]
        if (val !== 'All') result[fieldId] = val
      }
    }
  }

  return result
}

// Resolve selected fieldIds → rows[] và columns[] field objects
function deriveRowsAndColumns(cube, olapStateCube) {
  const dims = CUBE_HIERARCHIES[cube] ?? []
  const rows = []
  const cols = []

  for (const dim of dims) {
    const dimState = olapStateCube[dim.dimensionId]
    if (!dimState) continue
    const { axis, selected } = dimState
    if (!selected || selected.size === 0) continue

    for (const fieldId of selected) {
      for (const h of dim.hierarchies) {
        const field = h.fields.find(f => f.id === fieldId)
        if (field) {
          const target = axis === 'columns' ? cols : rows
          if (!target.find(f => f.id === field.id)) {
            target.push(field)
          }
          break
        }
      }
    }
  }

  return { rows, cols }
}

// Lấy dimensionId chứa một fieldId
function getFieldDimensionId(cube, fieldId) {
  const dims = CUBE_HIERARCHIES[cube] ?? []
  for (const dim of dims) {
    for (const h of dim.hierarchies) {
      if (h.fields.find(f => f.id === fieldId)) {
        return dim.dimensionId
      }
    }
  }
  return null
}

// Lấy danh sách fieldIds thuộc một dimension
function getFieldIdsOfDimension(cube, dimensionId) {
  const dims = CUBE_HIERARCHIES[cube] ?? []
  if (dimensionId === null) {
    return dims.flatMap(d => d.hierarchies.flatMap(h => h.fields.map(f => f.id)))
  }
  const dim = dims.find(d => d.dimensionId === dimensionId)
  if (!dim) return []
  return dim.hierarchies.flatMap(h => h.fields.map(f => f.id))
}

const initialDataState = {
  overview: null,
  trend: null,
  topProducts: null,
  customerSegment: null,
  topCustomers: null,
  filterOptions: null
}

// ============================================================================
// STORE DEFINITION
// ============================================================================
export const useDashboardStore = create(
  devtools(
    persist(
      (set, get) => ({
        // ===== STATE =====
        globalFilter: { ...initialFilterState },
        activeTab: 'dashboard', // 'dashboard' | 'explorer'
        loading: {
          overview: false,
          trend: false,
          topProducts: false,
          customerSegment: false,
          topCustomers: false,
          filters: false,
          explore: false
        },
        data: { ...initialDataState },
        drillDownLevel: 'year', // 'year' | 'quarter' | 'month'
        error: null,

        // ===== ACTIONS - FILTER MANAGEMENT =====

        /**
         * Cập nhật toàn bộ filter
         * @param {Object} newFilter - Filter mới
         */
        updateFilter: (newFilter) => {
          set((state) => ({
            globalFilter: mergeGlobalFilter(state.globalFilter, newFilter)
          }))
          const { fetchAllDashboardData } = get()
          fetchAllDashboardData()
        },

        /**
         * Cập nhật một phần của filter (time | customer | store) kèm cascading reset.
         * @param {string} section - 'time' | 'customer' | 'store'
         * @param {Object} values - Giá trị mới
         */
        updateFilterSection: (section, values) => {
          set((state) => {
            const gf = state.globalFilter
            if (section === 'time') {
              return {
                globalFilter: {
                  ...gf,
                  time: { ...gf.time, ...values }
                }
              }
            }
            if (section === 'customer') {
              return {
                globalFilter: {
                  ...gf,
                  customer: mergeCustomer(gf.customer, values)
                }
              }
            }
            if (section === 'store') {
              return {
                globalFilter: {
                  ...gf,
                  store: mergeStore(gf.store, values)
                }
              }
            }
            return { globalFilter: gf }
          })
          const { fetchAllDashboardData } = get()
          fetchAllDashboardData()
        },

        /**
         * Cập nhật một field cụ thể trong filter (cascading cho customer / store).
         * @param {string} section - 'time' | 'customer' | 'store'
         * @param {string} field - Tên field
         * @param {any} value - Giá trị mới
         */
        updateFilterField: (section, field, value) => {
          set((state) => {
            const gf = state.globalFilter
            if (section === 'time') {
              return {
                globalFilter: {
                  ...gf,
                  time: { ...gf.time, [field]: value }
                }
              }
            }
            if (section === 'customer') {
              return {
                globalFilter: {
                  ...gf,
                  customer: mergeCustomer(gf.customer, { [field]: value })
                }
              }
            }
            if (section === 'store') {
              return {
                globalFilter: {
                  ...gf,
                  store: mergeStore(gf.store, { [field]: value })
                }
              }
            }
            return { globalFilter: gf }
          })
          const { fetchAllDashboardData } = get()
          fetchAllDashboardData()
        },

        /**
         * Cập nhật filter cấp cao: product_key; customer_type / customer_key / store_key (nested).
         * @param {string} field - Tên field
         * @param {any} value - Giá trị mới
         */
        updateTopLevelFilter: (field, value) => {
          set((state) => {
            const gf = state.globalFilter
            if (field === 'product_key') {
              return { globalFilter: { ...gf, product_key: value } }
            }
            if (field === 'customer_type') {
              return {
                globalFilter: {
                  ...gf,
                  customer: mergeCustomer(gf.customer, { customer_type: value })
                }
              }
            }
            if (field === 'customer_key') {
              return {
                globalFilter: {
                  ...gf,
                  customer: { ...gf.customer, customer_key: value }
                }
              }
            }
            if (field === 'store_key') {
              return {
                globalFilter: {
                  ...gf,
                  store: { ...gf.store, store_key: value }
                }
              }
            }
            return {
              globalFilter: {
                ...gf,
                [field]: value
              }
            }
          })
          const { fetchAllDashboardData } = get()
          fetchAllDashboardData()
        },

        /**
         * Reset filter về mặc định
         */
        resetFilter: () => {
          set({ globalFilter: { ...initialFilterState } })
          const { fetchAllDashboardData } = get()
          fetchAllDashboardData()
        },

        /**
         * Set drill-down level
         * @param {string} level - 'year' | 'quarter' | 'month'
         */
        setDrillDownLevel: (level) => {
          set({ drillDownLevel: level })
          const { fetchTrend } = get()
          fetchTrend()
        },

        // ===== ACTIONS - TAB MANAGEMENT =====

        /**
         * Chuyển đổi tab
         * @param {string} tab - 'dashboard' | 'explorer'
         */
        setActiveTab: (tab) => {
          set({ activeTab: tab })
        },

        // ===== ACTIONS - LOADING & ERROR =====

        setLoading: (key, value) => {
          set((state) => ({
            loading: { ...state.loading, [key]: value }
          }))
        },

        setError: (error) => {
          set({ error })
        },

        clearError: () => {
          set({ error: null })
        },

        // ===== ACTIONS - API CALLS =====

        /**
         * Fetch tất cả dữ liệu dashboard
         */
        fetchAllDashboardData: async () => {
          const { fetchOverview, fetchTrend, fetchTopProducts, fetchCustomerSegment, fetchTopCustomers } = get()

          await Promise.all([
            fetchOverview(),
            fetchTrend(),
            fetchTopProducts(),
            fetchCustomerSegment(),
            fetchTopCustomers()
          ])
        },

        /**
         * Fetch dữ liệu overview (3 KPI cards)
         */
        fetchOverview: async () => {
          const { globalFilter, setLoading } = get()

          try {
            setLoading('overview', true)

            const response = await api.post('/dashboard/overview', globalFilterToDashboardPayload(globalFilter))

            set((state) => ({
              data: { ...state.data, overview: response.data }
            }))
          } catch (error) {
            console.error('Error fetching overview:', error)
            set({ error: 'Không thể tải dữ liệu overview' })
          } finally {
            setLoading('overview', false)
          }
        },

        /**
         * Fetch dữ liệu trend (biểu đồ đường)
         */
        fetchTrend: async () => {
          const { globalFilter, drillDownLevel, setLoading } = get()

          try {
            setLoading('trend', true)

            const response = await api.post('/dashboard/trend', {
              filter: globalFilterToDashboardPayload(globalFilter),
              level: drillDownLevel
            })

            set((state) => ({
              data: { ...state.data, trend: response.data }
            }))
          } catch (error) {
            console.error('Error fetching trend:', error)
            set({ error: 'Không thể tải dữ liệu trend' })
          } finally {
            setLoading('trend', false)
          }
        },

        /**
         * Fetch top/bottom products
         */
        fetchTopProducts: async () => {
          const { globalFilter, setLoading } = get()

          try {
            setLoading('topProducts', true)

            const response = await api.post('/dashboard/top-products', globalFilterToDashboardPayload(globalFilter))

            set((state) => ({
              data: { ...state.data, topProducts: response.data }
            }))
          } catch (error) {
            console.error('Error fetching top products:', error)
            set({ error: 'Không thể tải dữ liệu top products' })
          } finally {
            setLoading('topProducts', false)
          }
        },

        /**
         * Fetch customer segment (pie chart)
         */
        fetchCustomerSegment: async () => {
          const { globalFilter, setLoading } = get()

          try {
            setLoading('customerSegment', true)

            const response = await api.post('/dashboard/customer-segment', globalFilterToDashboardPayload(globalFilter))

            set((state) => ({
              data: { ...state.data, customerSegment: response.data }
            }))
          } catch (error) {
            console.error('Error fetching customer segment:', error)
            set({ error: 'Không thể tải dữ liệu customer segment' })
          } finally {
            setLoading('customerSegment', false)
          }
        },

        /**
         * Fetch dữ liệu Top 5 khách hàng doanh thu cao nhất
         */
        fetchTopCustomers: async () => {
          const { globalFilter, setLoading } = get()

          try {
            setLoading('topCustomers', true)

            const response = await api.post('/dashboard/top-customers', globalFilterToDashboardPayload(globalFilter))

            set((state) => ({
              data: { ...state.data, topCustomers: response.data }
            }))
          } catch (error) {
            console.error('Error fetching top customers:', error)
            set({ error: 'Không thể tải dữ liệu top customers' })
          } finally {
            setLoading('topCustomers', false)
          }
        },

        /**
         * Fetch drill-through chi tiết khách hàng (gọi từ Dashboard.jsx)
         * @param {number} customerKey
         * @param {number} page
         * @param {number} pageSize
         */
        fetchDrillThrough: async (customerKey, page = 1, pageSize = 15) => {
          const { globalFilter } = get()
          const { year, quarter, month } = globalFilter.time || {}
          const { state, city, customer_type } = globalFilter.customer || {}

          const params = new URLSearchParams()
          params.append('year', year || 'All')
          params.append('quarter', quarter || 'All')
          params.append('month', month || 'All')
          params.append('state', state || 'All')
          params.append('city', city || 'All')
          params.append('customer_type', customer_type || 'All')
          params.append('page', page.toString())
          params.append('page_size', pageSize.toString())

          const response = await api.get(`/dashboard/customer-drill-through/${customerKey}?${params.toString()}`)
          return response.data
        },

        /**
         * Fetch filter options (khởi tạo)
         */
        fetchFilterOptions: async () => {
          const { setLoading } = get()

          try {
            setLoading('filters', true)

            const response = await api.get('/filters/init')

            set((state) => ({
              data: { ...state.data, filterOptions: response.data }
            }))
          } catch (error) {
            console.error('Error fetching filter options:', error)
            set({ error: 'Không thể tải dữ liệu filter' })
          } finally {
            setLoading('filters', false)
          }
        },

        /**
         * Drill-down từ chart
         * @param {string} value - Giá trị được click (VD: '2024', 'Q1')
         */
        handleChartDrillDown: (value) => {
          const { drillDownLevel, globalFilter, setDrillDownLevel, updateFilterSection } = get()

          if (drillDownLevel === 'year') {
            // Drill-down từ year -> quarter
            updateFilterSection('time', { year: value })
            setDrillDownLevel('quarter')
          } else if (drillDownLevel === 'quarter') {
            // Drill-down từ quarter -> month
            const quarterMap = { 'Q1': 1, 'Q2': 2, 'Q3': 3, 'Q4': 4 }
            const quarterNum = quarterMap[value] || value.replace('Q', '')
            updateFilterSection('time', { quarter: quarterNum.toString() })
            setDrillDownLevel('month')
          }
        },

        /**
         * Roll-up (quay lại cấp trên)
         */
        handleChartRollUp: () => {
          const { drillDownLevel, globalFilter, setDrillDownLevel, updateFilterSection } = get()

          if (drillDownLevel === 'month') {
            // Roll-up từ month -> quarter
            updateFilterSection('time', { month: 'All' })
            setDrillDownLevel('quarter')
          } else if (drillDownLevel === 'quarter') {
            // Roll-up từ quarter -> year
            updateFilterSection('time', { quarter: 'All', month: 'All' })
            setDrillDownLevel('year')
          }
        }
      }),
      {
        name: 'dashboard-storage',
        partialize: (state) => ({
          globalFilter: state.globalFilter,
          activeTab: state.activeTab
        })
      }
    )
  )
)

// ============================================================================
// OLAP EXPLORER STORE - State riêng cho Tab OLAP Explorer
// ============================================================================
export const useOlapStore = create(
  devtools(
    (set, get) => ({
      // ===== STATE =====

      // olapState[cube][dimensionId] = { axis: 'rows'|'columns'|null, selected: Set<fieldId> }
      // selected = Set of fieldIds đang được tick
      olapState: {
        sales: {
          time:     { axis: null, selected: new Set() },
          product:  { axis: null, selected: new Set() },
          customer: { axis: null, selected: new Set() }
        },
        inventory: {
          time:   { axis: null, selected: new Set() },
          product:{ axis: null, selected: new Set() },
          store:  { axis: null, selected: new Set() }
        }
      },

      // legacy fields (rows/columns/available/measures) — derive từ olapState
      fields: {
        available: [...OLAP_SALES_AVAILABLE_FIELDS],
        rows: [],
        columns: [],
        measures: ['sum_amount']
      },

      pivotData: null,
      rawData: null,
      rawDataPagination: {
        page: 1,
        page_size: 100,
        total: 0,
        total_pages: 0
      },
      pivotPagination: {
        page: 1,
        page_size: 50,
        total_rows: 0,
        total_pages: 1
      },
      pivotSort: {
        sort_column: null,
        sort_order: 'asc'
      },

      loading: {
        pivot: false,
        raw: false
      },

      error: null,
      viewMode: 'pivot', // 'pivot' | 'chart' | 'raw'
      activeCube: 'sales', // 'sales' | 'inventory'

      // ===== ACTIONS =====

      /**
       * Set active cube (Sales | Inventory) — reset olapState khi chuyển cube
       */
      setActiveCube: (cube) => {
        const fresh = {
          time:    { axis: null, selected: new Set() },
          product: { axis: null, selected: new Set() },
          [cube === 'sales' ? 'customer' : 'store']: { axis: null, selected: new Set() }
        }
        set({
          activeCube: cube,
          olapState: { ...get().olapState, [cube]: fresh },
          fields: {
            ...get().fields,
            available: cube === 'inventory'
              ? [...OLAP_INVENTORY_AVAILABLE_FIELDS]
              : [...OLAP_SALES_AVAILABLE_FIELDS],
            rows: [],
            columns: [],
            measures: cube === 'inventory' ? ['total_quantity_on_hand'] : ['sum_amount']
          },
          pivotData: null
        })
      },

      /**
       * Đặt axis cho một dimension (Hàng / Cột / null).
       * axis = null → uncheck tất cả fields trong dimension đó.
       */
      setDimensionAxis: (cube, dimensionId, axis) => {
        set((state) => {
          const cubeState = state.olapState[cube]
          if (!cubeState) return {}

          if (axis === null) {
            return {
              olapState: {
                ...state.olapState,
                [cube]: {
                  ...cubeState,
                  [dimensionId]: { axis: null, selected: new Set() }
                }
              }
            }
          }

          return {
            olapState: {
              ...state.olapState,
              [cube]: {
                ...cubeState,
                [dimensionId]: { ...cubeState[dimensionId], axis }
              }
            }
          }
        })
        get()._recomputeRowsColumns()
      },

      /**
       * Chọn / bỏ chọn một field cụ thể.
       * - Nếu field chưa chọn → thêm vào selected (sau mutex check)
       * - Nếu đã chọn → bỏ khỏi selected
       * - Enforce mutex: customer_type ↔ customer_location
       */
      toggleField: (cube, fieldId) => {
        const dimId = getFieldDimensionId(cube, fieldId)
        const hId = FIELD_TO_HIERARCHY[fieldId]
        const mutexPartner = HIERARCHY_MUTEX[hId]

        set((state) => {
          const cubeState = state.olapState[cube]
          if (!cubeState) return {}
          const dimState = cubeState[dimId]
          const newSelected = new Set(dimState.selected)

          if (newSelected.has(fieldId)) {
            newSelected.delete(fieldId)
          } else {
            newSelected.add(fieldId)

            // Enforce mutex: nếu chọn field thuộc mutex group → bỏ hết fields thuộc partner
            if (mutexPartner) {
              const partnerFieldIds = getFieldsOfHierarchy(cube, mutexPartner)
              partnerFieldIds.forEach(fid => newSelected.delete(fid))
            }
          }

          return {
            olapState: {
              ...state.olapState,
              [cube]: {
                ...cubeState,
                [dimId]: { ...dimState, selected: newSelected }
              }
            }
          }
        })
        get()._recomputeRowsColumns()
      },

      /**
       * Chọn tất cả fields của một dimension.
       */
      selectAllDimension: (cube, dimensionId) => {
        const fieldIds = getFieldIdsOfDimension(cube, dimensionId)
        set((state) => {
          const cubeState = state.olapState[cube]
          if (!cubeState) return {}
          const dimState = cubeState[dimensionId]
          const newSelected = new Set(dimState.selected)
          fieldIds.forEach(id => newSelected.add(id))
          return {
            olapState: {
              ...state.olapState,
              [cube]: {
                ...cubeState,
                [dimensionId]: { ...dimState, selected: newSelected }
              }
            }
          }
        })
        get()._recomputeRowsColumns()
      },

      /**
       * Bỏ chọn tất cả fields của một dimension.
       */
      deselectAllDimension: (cube, dimensionId) => {
        set((state) => {
          const cubeState = state.olapState[cube]
          if (!cubeState) return {}
          const dimState = cubeState[dimensionId]
          return {
            olapState: {
              ...state.olapState,
              [cube]: {
                ...cubeState,
                [dimensionId]: { ...dimState, selected: new Set() }
              }
            }
          }
        })
        get()._recomputeRowsColumns()
      },

      /**
       * Derive fields.rows/columns từ olapState hiện tại.
       * Gọi mỗi khi olapState thay đổi.
       */
      _recomputeRowsColumns: () => {
        const { activeCube, olapState, fields } = get()
        const { rows, cols } = deriveRowsAndColumns(activeCube, olapState[activeCube] ?? {})

        const prevRows = fields.rows.map(r => r.id).sort()
        const prevCols = fields.columns.map(c => c.id).sort()
        const newRows = rows.map(r => r.id).sort()
        const newCols = cols.map(c => c.id).sort()

        if (JSON.stringify(prevRows) !== JSON.stringify(newRows) ||
            JSON.stringify(prevCols) !== JSON.stringify(newCols)) {
          set((state) => ({
            fields: { ...state.fields, rows, columns: cols }
          }))
        }
      },

      /**
       * Thêm field vào rows (legacy — giữ cho backward compat, điều hướng sang toggleField)
       * @deprecated
       */
      addToRows: (fieldOrId) => {
        const fieldId = typeof fieldOrId === 'string' ? fieldOrId : fieldOrId.id
        get().toggleField(get().activeCube, fieldId)
      },

      /**
       * Thêm field vào columns (legacy)
       * @deprecated
       */
      addToColumns: (fieldOrId) => {
        const fieldId = typeof fieldOrId === 'string' ? fieldOrId : fieldOrId.id
        get().toggleField(get().activeCube, fieldId)
      },

      /**
       * Xóa field khỏi rows (legacy)
       * @deprecated
       */
      removeFromRows: (fieldId) => {
        get().toggleField(get().activeCube, fieldId)
      },

      /**
       * Xóa field khỏi columns (legacy)
       * @deprecated
       */
      removeFromColumns: (fieldId) => {
        get().toggleField(get().activeCube, fieldId)
      },

      /**
       * Swap axes (hoán đổi rows ↔ columns trong olapState)
       */
      swapAxes: () => {
        set((state) => {
          const cube = state.activeCube
          const cubeState = state.olapState[cube]
          const nextCube = {}
          for (const [dimId, dimState] of Object.entries(cubeState)) {
            const newAxis = dimState.axis === 'rows' ? 'columns'
              : dimState.axis === 'columns' ? 'rows'
              : null
            nextCube[dimId] = { ...dimState, axis: newAxis }
          }
          return {
            olapState: { ...state.olapState, [cube]: nextCube }
          }
        })
        get()._recomputeRowsColumns()
      },

      /**
       * Clear tất cả chọn lọc
       */
      clearAll: () => {
        const cube = get().activeCube
        const fresh = {
          time:    { axis: null, selected: new Set() },
          product: { axis: null, selected: new Set() },
          [cube === 'sales' ? 'customer' : 'store']: { axis: null, selected: new Set() }
        }
        set((state) => ({
          olapState: { ...state.olapState, [cube]: fresh },
          fields: { ...state.fields, rows: [], columns: [] },
          pivotData: null
        }))
      },

      /**
       * Xóa field khỏi rows
       */
      removeFromRows: (fieldId) => {
        set((state) => ({
          fields: {
            ...state.fields,
            rows: state.fields.rows.filter(r => r.id !== fieldId)
          }
        }))
      },

      /**
       * Xóa field khỏi columns
       */
      removeFromColumns: (fieldId) => {
        set((state) => ({
          fields: {
            ...state.fields,
            columns: state.fields.columns.filter(c => c.id !== fieldId)
          }
        }))
      },

      /**
       * Clear tất cả
       */
      clearAll: () => {
        const { activeCube } = get()
        const freshOlapState = () => ({
          time:    { axis: null, selected: new Set() },
          product: { axis: null, selected: new Set() },
          [activeCube === 'sales' ? 'customer' : 'store']: { axis: null, selected: new Set() }
        })
        set((state) => ({
          fields: {
            ...state.fields,
            rows: [],
            columns: []
          },
          olapState: {
            ...state.olapState,
            [activeCube]: freshOlapState()
          },
          pivotData: null
        }))
      },

      /**
       * Set measures
       */
      setMeasures: (measures) => {
        set((state) => ({
          fields: {
            ...state.fields,
            measures: measures
          }
        }))
      },

      /**
       * Set view mode
       */
      setViewMode: (mode) => {
        set({ viewMode: mode })
      },

      /**
       * Set pivot page
       */
      setPivotPage: (page) => {
        set(state => ({ pivotPagination: { ...state.pivotPagination, page } }))
        get().fetchPivotData(page)
      },

      /**
       * Set pivot page size
       */
      setPivotPageSize: (page_size) => {
        set(state => ({ pivotPagination: { ...state.pivotPagination, page_size, page: 1 } }))
        get().fetchPivotData(1)
      },

      /**
       * Set pivot sort
       */
      setPivotSort: (sort_column, sort_order) => {
        set(state => ({
          pivotSort: { sort_column, sort_order },
          pivotPagination: { ...state.pivotPagination, page: 1 }
        }))
        get().fetchPivotData(1)
      },

      /**
       * Fetch pivot data
       */
      fetchPivotData: async (page = null) => {
        const { fields, activeCube, pivotPagination, pivotSort } = get()

        if (fields.rows.length === 0 && fields.columns.length === 0) {
          set({ pivotData: null })
          return
        }

        // Dùng page truyền vào nếu có, kôn lấy từ state
        // Đảm bảo page là number (tránh nhận nhầm React Event object)
        const cleanPage = (typeof page === 'number') ? page : null
        const currentPage = cleanPage || pivotPagination.page || 1

        try {
          set({ loading: { ...get().loading, pivot: true }, error: null })

          const { globalFilter } = useDashboardStore.getState()
          const filters = getOlapFiltersFromGlobalFilter(activeCube, get().olapState, globalFilter)

          const requestBody = {
            cube: activeCube,
            rows: fields.rows.map(r => r.column),
            columns: fields.columns.map(c => c.column),
            measures: fields.measures,
            filters,
            limit: 5000,
            page: currentPage,
            page_size: pivotPagination.page_size,
            sort_column: pivotSort.sort_column,
            sort_order: pivotSort.sort_order
          }

          const response = await api.post('/olap/explore', requestBody)

          if (response.data.success) {
            set({
              pivotData: response.data,
              pivotPagination: {
                page: response.data.pagination?.page || currentPage,
                page_size: response.data.pagination?.page_size || pivotPagination.page_size,
                total_rows: response.data.pagination?.total_rows || response.data.data.length,
                total_pages: response.data.pagination?.total_pages || 1
              }
            })
          } else {
            set({ error: response.data.error || 'Lỗi không xác định' })
          }
        } catch (error) {
          console.error('Error fetching pivot data:', error)
          set({ error: 'Không thể tải dữ liệu pivot' })
        } finally {
          set({ loading: { ...get().loading, pivot: false } })
        }
      },

      /**
       * Fetch raw data
       */
      fetchRawData: async (page = 1) => {
        const { fields, activeCube, rawDataPagination } = get()

        try {
          set({ loading: { ...get().loading, raw: true }, error: null })

          const { globalFilter } = useDashboardStore.getState()
          const filters = getOlapFiltersFromGlobalFilter(activeCube, get().olapState, globalFilter)

          const requestBody = {
            cube: activeCube,
            columns: [],
            filters,
            page: page,
            page_size: rawDataPagination.page_size
          }

          const response = await api.post('/olap/raw-data', requestBody)

          if (response.data.success) {
            set({
              rawData: response.data.data,
              rawDataPagination: response.data.pagination
            })
          } else {
            set({ error: response.data.error || 'Lỗi không xác định' })
          }
        } catch (error) {
          console.error('Error fetching raw data:', error)
          set({ error: 'Không thể tải dữ liệu thô' })
        } finally {
          set({ loading: { ...get().loading, raw: false } })
        }
      },

      /**
       * Drill-down trong pivot
       */
      drillDown: (fieldId) => {
        const { fields, addToRows } = get()

        // Thêm field vào rows để drill-down
        addToRows(fieldId)

        // Fetch lại data
        const { fetchPivotData } = get()
        fetchPivotData()
      }
    })
  )
)

// ============================================================================
// SELECTORS - Helper functions để lấy dữ liệu từ store
// ============================================================================

/**
 * Kiểm tra xem có đang filter product cụ thể không
 */
export const selectIsProductFiltered = (state) => {
  return state.globalFilter.product_key !== 'All'
}

/**
 * Kiểm tra xem có đang filter customer type cụ thể không
 */
export const selectIsCustomerFiltered = (state) => {
  return state.globalFilter.customer?.customer_type !== 'All'
}

/**
 * Format số tiền
 */
export const formatCurrency = (value) => {
  if (value === null || value === undefined) return '-'
  return new Intl.NumberFormat('vi-VN', {
    style: 'currency',
    currency: 'VND',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  }).format(value)
}

/**
 * Format số lượng
 */
export const formatNumber = (value) => {
  if (value === null || value === undefined) return '-'
  return new Intl.NumberFormat('vi-VN').format(value)
}
