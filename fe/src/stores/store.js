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

/**
 * Làm phẳng filters gửi OLAP explore/raw: bỏ bọc location/customer/store/time lồng nhau.
 */
function flattenExploreFilters(filters) {
  if (!filters || typeof filters !== 'object') return {}
  const out = { ...filters }
  if (out.location && typeof out.location === 'object') {
    const loc = out.location
    if (loc.state != null) out.state = loc.state
    if (loc.city != null) out.city = loc.city
    delete out.location
  }
  if (out.customer && typeof out.customer === 'object') {
    Object.assign(out, out.customer)
    delete out.customer
  }
  if (out.store && typeof out.store === 'object') {
    Object.assign(out, out.store)
    delete out.store
  }
  if (out.time && typeof out.time === 'object') {
    Object.assign(out, out.time)
    delete out.time
  }
  return out
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

const initialDataState = {
  overview: null,
  trend: null,
  topProducts: null,
  customerSegment: null,
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
          const { fetchOverview, fetchTrend, fetchTopProducts, fetchCustomerSegment } = get()

          await Promise.all([
            fetchOverview(),
            fetchTrend(),
            fetchTopProducts(),
            fetchCustomerSegment()
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
      fields: {
        available: [...OLAP_SALES_AVAILABLE_FIELDS],
        rows: [],
        columns: [],
        filters: {},
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
      viewMode: 'pivot', // 'pivot' | 'raw'
      activeCube: 'sales', // 'sales' | 'inventory'

      // ===== ACTIONS =====

      /**
       * Set active cube (Sales | Inventory)
       */
      setActiveCube: (cube) => {
        set({
          activeCube: cube,
          fields: {
            ...get().fields,
            available:
              cube === 'inventory'
                ? [...OLAP_INVENTORY_AVAILABLE_FIELDS]
                : [...OLAP_SALES_AVAILABLE_FIELDS],
            rows: [],
            columns: [],
            measures: cube === 'inventory' ? ['total_quantity_on_hand'] : ['sum_amount'],
            filters: {}
          },
          pivotData: null
        })
      },

      /**
       * Thêm field vào rows
       */
      addToRows: (fieldOrId) => {
        const { fields } = get()
        // Support both field object and fieldId string for backward compatibility
        const field = typeof fieldOrId === 'string'
          ? fields.available.find(f => f.id === fieldOrId)
          : fieldOrId

        if (field && !fields.rows.find(r => r.id === field.id)) {
          set((state) => ({
            fields: {
              ...state.fields,
              rows: [...state.fields.rows, field]
            }
          }))
        }
      },

      /**
       * Thêm field vào columns
       */
      addToColumns: (fieldOrId) => {
        const { fields } = get()
        // Support both field object and fieldId string for backward compatibility
        const field = typeof fieldOrId === 'string'
          ? fields.available.find(f => f.id === fieldOrId)
          : fieldOrId

        if (field && !fields.columns.find(c => c.id === field.id)) {
          set((state) => ({
            fields: {
              ...state.fields,
              columns: [...state.fields.columns, field]
            }
          }))
        }
      },

      /**
       * Thêm filter
       */
      addFilter: (fieldId, value) => {
        set((state) => ({
          fields: {
            ...state.fields,
            filters: {
              ...state.fields.filters,
              [fieldId]: value
            }
          }
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
       * Xóa filter
       */
      removeFilter: (fieldId) => {
        set((state) => {
          const newFilters = { ...state.fields.filters }
          delete newFilters[fieldId]
          return {
            fields: {
              ...state.fields,
              filters: newFilters
            }
          }
        })
      },

      /**
       * Xoay trục (Swap rows và columns)
       */
      swapAxes: () => {
        set((state) => ({
          fields: {
            ...state.fields,
            rows: state.fields.columns,
            columns: state.fields.rows
          }
        }))
      },

      /**
       * Clear tất cả
       */
      clearAll: () => {
        set((state) => ({
          fields: {
            ...state.fields,
            rows: [],
            columns: [],
            filters: {}
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
       * Update filter section (dành cho cascading dropdowns ở Sidebar)
       */
      updateFilterSection: (section, updates) => {
        set((state) => ({
          fields: {
            ...state.fields,
            filters: {
              ...state.fields.filters,
              ...updates
            }
          },
          pivotPagination: { ...state.pivotPagination, page: 1 }
        }))
        const { fields } = get()
        if (fields.rows.length > 0 || fields.columns.length > 0) {
          get().fetchPivotData(1)
        }
      },

      /**
       * Update top-level filter (dành cho Radio/Search ở Sidebar)
       */
      updateTopLevelFilter: (key, value) => {
        set((state) => ({
          fields: {
            ...state.fields,
            filters: {
              ...state.fields.filters,
              [key]: value
            }
          },
          pivotPagination: { ...state.pivotPagination, page: 1 }
        }))
        const { fields } = get()
        if (fields.rows.length > 0 || fields.columns.length > 0) {
          get().fetchPivotData(1)
        }
      },

      /**
       * Reset bộ lọc OLAP
       */
      resetFilters: () => {
        set((state) => ({
          fields: {
            ...state.fields,
            filters: {}
          },
          pivotPagination: { ...state.pivotPagination, page: 1 }
        }))
        const { fields } = get()
        if (fields.rows.length > 0 || fields.columns.length > 0) {
          get().fetchPivotData(1)
        }
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

          const requestBody = {
            cube: activeCube,
            rows: fields.rows.map(r => r.column),
            columns: fields.columns.map(c => c.column),
            measures: fields.measures,
            filters: flattenExploreFilters(fields.filters),
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
        const { fields, activeCube, rawDataPagination, setLoading } = get()

        try {
          set({ loading: { ...get().loading, raw: true }, error: null })

          const requestBody = {
            cube: activeCube,  // ✏️ Thêm cube parameter
            columns: [],
            filters: flattenExploreFilters(fields.filters),
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
