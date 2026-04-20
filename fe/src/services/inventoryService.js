/**
 * ============================================================================
 * SERVICES/INVENTORYSERVICE.JS - Inventory API Service
 * ============================================================================
 * Service để gọi các API liên quan đến Inventory.
 */

import axios from 'axios'

const API_BASE = '/api/inventory'

const inventoryService = {
  /**
   * Lấy tổng quan tồn kho
   */
  async getOverview(city = null, state = null, year = null, quarter = null, month = null, store_key = null) {
    const params = {}
    if (city) params.city = city
    if (state) params.state = state
    if (year) params.year = year
    if (quarter) params.quarter = quarter
    if (month) params.month = month
    if (store_key != null && store_key !== '') params.store_key = store_key

    const response = await axios.get(`${API_BASE}/overview`, { params })
    return response.data
  },

  /**
   * Phân tích chi tiết 1 sản phẩm (Drill-Across)
   * @param {number} productId - ID sản phẩm
   * @param {Object} filters - { city, state, year, quarter, month, time_level }
   */
  async getAnalysis(productId, filters = {}) {
    const params = {}
    if (filters.city) params.city = filters.city
    if (filters.state) params.state = filters.state
    if (filters.store_key != null && filters.store_key !== '') params.store_key = filters.store_key
    if (filters.year) params.year = filters.year
    if (filters.quarter) params.quarter = filters.quarter
    if (filters.month) params.month = filters.month
    if (filters.time_level) params.time_level = filters.time_level

    const response = await axios.get(`${API_BASE}/analysis/${productId}`, { params })
    return response.data
  },

  /**
   * Lấy dữ liệu cho Scatter Plot
   * @param {Object} filters - { city, state, year, quarter, month }
   */
  async getScatterData(filters = {}) {
    const params = {}
    if (filters.city) params.city = filters.city
    if (filters.state) params.state = filters.state
    if (filters.store_key != null && filters.store_key !== '') params.store_key = filters.store_key
    if (filters.year) params.year = filters.year
    if (filters.quarter) params.quarter = filters.quarter
    if (filters.month) params.month = filters.month

    const response = await axios.get(`${API_BASE}/scatter-data`, { params })
    return response.data
  },

  /**
   * Lấy danh sách sản phẩm
   */
  async getProducts(page = 1, pageSize = 50) {
    const response = await axios.get(`${API_BASE}/products`, {
      params: { page, page_size: pageSize }
    })
    return response.data
  },

  /**
   * Lấy top cities theo rủi ro (overstock/understock)
   * @param {Object} filters - { year, quarter, month, limit }
   */
  async getCitiesRiskRanking(filters = {}) {
    const params = {}
    if (filters.year) params.year = filters.year
    if (filters.quarter) params.quarter = filters.quarter
    if (filters.month) params.month = filters.month
    if (filters.limit) params.limit = filters.limit
    
    const response = await axios.get(`${API_BASE}/cities-risk-ranking`, { params })
    return response.data
  },

  /**
   * Lấy danh sách cửa hàng
   */
  async getStores() {
    const response = await axios.get(`${API_BASE}/stores`)
    return response.data
  },

  /**
   * Lấy thông tin Inventory Cuboid Router
   */
  async getRouterInfo() {
    const response = await axios.get(`${API_BASE}/router-info`)
    return response.data
  }
}

export default inventoryService
