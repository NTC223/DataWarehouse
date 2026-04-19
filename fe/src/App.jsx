/**
 * ============================================================================
 * APP.JSX - Main Application Component
 * ============================================================================
 * Component chính của ứng dụng Dashboard.
 * Quản lý layout chính và điều hướng giữa các tab.
 * 
 * Layout:
 * - Sidebar: Chứa các filter và navigation
 * - Main Content: Hiển thị nội dung tab đang active
 * ============================================================================
 */

import React, { useEffect } from 'react'
import { useDashboardStore } from './stores/store'
import Sidebar from './components/Sidebar'
import Dashboard from './components/Dashboard'
import InventoryDashboard from './components/InventoryDashboard'
import OlapExplorer from './components/OlapExplorer'
import LoadingSpinner from './components/LoadingSpinner'

function App() {
  // Lấy state và actions từ store
  const {
    activeTab,
    setActiveTab,
    globalFilter,
    fetchFilterOptions,
    fetchAllDashboardData,
    loading
  } = useDashboardStore()

  // Khởi tạo dữ liệu khi component mount
  useEffect(() => {
    const init = async () => {
      await fetchFilterOptions()
      await fetchAllDashboardData()
    }
    init()
  }, [])

  return (
    <div className="min-h-screen bg-gray-50 flex">
      {/* Sidebar - Filter và Navigation */}
      <Sidebar />

      {/* Main Content */}
      <main className="flex-1 ml-72 min-w-0 overflow-x-hidden flex flex-col">
        {/* Header */}
        <header className="bg-[#C0DAAA] shadow-sm border-b border-gray-200 sticky top-0 z-10">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between">
              <div>
                <h1 className="text-2xl font-bold text-gray-900">
                  Data Warehouse Dashboard
                </h1>
                <p className="text-sm text-gray-500 mt-1">
                  Hệ thống báo cáo và phân tích dữ liệu bán hàng
                </p>
              </div>

              {/* Tab Navigation */}
              <div className="flex bg-gray-100 rounded-lg p-1">
                <button
                  onClick={() => setActiveTab('dashboard')}
                  className={`px-4 py-2 rounded-md text-sm font-medium transition-all ${activeTab === 'dashboard'
                      ? 'bg-white text-[#799351] shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                    }`}
                >
                  📊 Sales
                </button>
                <button
                  onClick={() => setActiveTab('inventory')}
                  className={`px-4 py-2 rounded-md text-sm font-medium transition-all ${activeTab === 'inventory'
                      ? 'bg-white text-[#799351] shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                    }`}
                >
                  📦 Inventory
                </button>
                <button
                  onClick={() => setActiveTab('explorer')}
                  className={`px-4 py-2 rounded-md text-sm font-medium transition-all ${activeTab === 'explorer'
                      ? 'bg-white text-[#799351] shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                    }`}
                >
                  🔍 OLAP Explorer
                </button>
              </div>
            </div>
          </div>
        </header>

        {/* Content Area */}
        <div className="p-6">
          {/* Loading Overlay */}
          {(loading.overview || loading.trend) && (
            <div className="fixed inset-0 bg-black/20 flex items-center justify-center z-50">
              <LoadingSpinner />
            </div>
          )}

          {/* Tab Content */}
          {activeTab === 'dashboard' && <Dashboard />}
          {activeTab === 'inventory' && <InventoryDashboard filters={globalFilter} />}
          {activeTab === 'explorer' && <OlapExplorer />}
        </div>

      </main>
    </div>
  )
}

export default App
