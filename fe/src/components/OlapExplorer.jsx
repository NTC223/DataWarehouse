/**
 * ============================================================================
 * OLAPEXPLORER.JSX - OLAP Explorer Component (Tab 2)
 * ============================================================================
 * Component hiển thị OLAP Explorer với:
 * - Field Picker: Chọn fields cho Rows, Columns, Filters
 * - Pivot Table: Hiển thị dữ liệu 2 chiều
 * - Raw Data View: Dữ liệu thô với phân trang
 * - Toolbar: Swap axes, Export, Clear
 * 
 * Hỗ trợ 4 phép toán OLAP:
 * - Slice: Lọc dữ liệu
 * - Dice: Lọc nhiều chiều
 * - Pivot: Xoay trục
 * - Drill-down: Khoan sâu dữ liệu
 * ============================================================================
 */

import React, { useState, useEffect } from 'react'
import { useOlapStore, formatCurrency, formatNumber } from '../stores/store'
import LoadingSpinner from './LoadingSpinner'

function OlapExplorer() {
  const {
    fields,
    pivotData,
    rawData,
    rawDataPagination,
    pivotPagination,
    pivotSort,
    loading,
    error,
    viewMode,
    activeCube,
    setActiveCube,
    addToRows,
    addToColumns,
    removeFromRows,
    removeFromColumns,
    swapAxes,
    clearAll,
    setViewMode,
    setMeasures,
    fetchPivotData,
    fetchRawData,
    setPivotPage,
    setPivotPageSize,
    setPivotSort
  } = useOlapStore()


  // Fetch data khi fields thay đổi - luôn reset về trang 1
  useEffect(() => {
    if (viewMode === 'pivot' && (fields.rows.length > 0 || fields.columns.length > 0)) {
      fetchPivotData(1)
    }
  }, [fields.rows, fields.columns, fields.measures, fields.filters, viewMode, activeCube])

  useEffect(() => {
    if (viewMode === 'raw') {
      fetchRawData(1)
    }
  }, [viewMode, fields.filters, activeCube])

  // ============================================================================
  // FIELD PICKER COMPONENT
  // ============================================================================
  const renderFieldPicker = () => {
    // Fields khác nhau cho Sales Cube và Inventory Cube
    const salesFields = [
      { id: 'year', name: 'Năm', category: 'time', column: 'year' },
      { id: 'quarter', name: 'Quý', category: 'time', column: 'quarter' },
      { id: 'month', name: 'Tháng', category: 'time', column: 'month' },
      { id: 'product_key', name: 'Sản phẩm', category: 'product', column: 'product_key' },
      { id: 'customer_type', name: 'Loại KH', category: 'customer', column: 'customer_type' },
      { id: 'customer_key', name: 'Mã KH', category: 'customer', column: 'customer_key' },
      { id: 'state', name: 'Bang', category: 'customer', column: 'state' },
      { id: 'city', name: 'Thành phố', category: 'customer', column: 'city' }
    ]

    const inventoryFields = [
      { id: 'year', name: 'Năm', category: 'time', column: 'year' },
      { id: 'quarter', name: 'Quý', category: 'time', column: 'quarter' },
      { id: 'month', name: 'Tháng', category: 'time', column: 'month' },
      { id: 'product_key', name: 'Sản phẩm', category: 'product', column: 'product_key' },
      { id: 'store_key', name: 'Cửa hàng', category: 'store', column: 'store_key' },
      { id: 'state', name: 'Bang', category: 'store', column: 'state' },
      { id: 'city', name: 'Thành phố', category: 'store', column: 'city' }
    ]

    const availableFields = activeCube === 'sales' ? salesFields : inventoryFields


    // Group fields by category
    const groupedFields = availableFields.reduce((acc, field) => {
      if (!acc[field.category]) {
        acc[field.category] = []
      }
      acc[field.category].push(field)
      return acc
    }, {})

    const categoryLabels = {
      time: '🕐 Thời gian',
      product: '📦 Sản phẩm',
      customer: '👥 Khách hàng',
      store: '🏪 Cửa hàng'
    }

    return (
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-800">
            📋 Chọn trường dữ liệu
          </h3>

          {/* Cube Switcher */}
          <div className="flex bg-gray-100 rounded-lg p-1">
            <button
              onClick={() => setActiveCube('sales')}
              className={`px-3 py-1 rounded-md text-sm font-medium transition-all ${activeCube === 'sales'
                ? 'bg-white text-blue-600 shadow-sm'
                : 'text-gray-600 hover:text-gray-900'
                }`}
            >
              📊 Sales Cube
            </button>
            <button
              onClick={() => setActiveCube('inventory')}
              className={`px-3 py-1 rounded-md text-sm font-medium transition-all ${activeCube === 'inventory'
                ? 'bg-white text-green-600 shadow-sm'
                : 'text-gray-600 hover:text-gray-900'
                }`}
            >
              📦 Inventory Cube
            </button>
          </div>
        </div>

        {/* Cube Info */}
        <div className="mb-4 p-3 bg-blue-50 rounded-lg text-sm">
          {activeCube === 'sales' ? (
            <p className="text-blue-700">
              <strong>📊 Sales Cube:</strong> Dimensions: Time, Product, Customer (gồm state, city, loại KH, mã KH) |
              Measures: total_quantity, sum_amount
            </p>
          ) : (
            <p className="text-green-700">
              <strong>📦 Inventory Cube:</strong> Dimensions: Time, Product, Store (gồm state, city, store_key) |
              Measures: total_quantity_on_hand
            </p>
          )}
        </div>

        {/* Available Fields - Compact Redesign */}
        <div className="space-y-3">
          {Object.entries(groupedFields).map(([category, categoryFields]) => (
            <div key={category} className="flex flex-col sm:flex-row sm:items-center gap-y-2 pb-3 border-b border-gray-100 last:border-0 last:pb-0">
              <h4 className="text-sm font-medium text-gray-500 w-32 shrink-0">
                {categoryLabels[category]}
              </h4>
              <div className="flex flex-wrap items-center gap-2">
                {categoryFields.map(field => {
                  const isInRows = fields.rows.find(r => r.id === field.id)
                  const isInCols = fields.columns.find(c => c.id === field.id)
                  const isUsed = isInRows || isInCols

                  return (
                    <div key={field.id} className={`inline-flex items-center text-xs border rounded-md shadow-sm divide-x overflow-hidden transition-all ${isUsed ? 'border-gray-200 opacity-60' : 'border-gray-200 hover:border-gray-300'}`}>
                      <div className="px-2 py-1.5 font-medium bg-gray-100 text-gray-700 min-w-[70px] text-center">
                        {field.name}
                      </div>
                      <button
                        onClick={() => addToRows(field)}
                        disabled={isUsed}
                        className={`px-2 py-1.5 font-medium transition-colors ${isInRows ? 'bg-blue-100 text-blue-700' : 'bg-white text-gray-600 hover:bg-blue-50 hover:text-blue-600'} disabled:cursor-not-allowed`}
                        title="Thêm vào Hàng"
                      >
                        Hàng ↓
                      </button>
                      <button
                        onClick={() => addToColumns(field)}
                        disabled={isUsed}
                        className={`px-2 py-1.5 font-medium transition-colors ${isInCols ? 'bg-green-100 text-green-700' : 'bg-white text-gray-600 hover:bg-green-50 hover:text-green-600'} disabled:cursor-not-allowed`}
                        title="Thêm vào Cột"
                      >
                        Cột →
                      </button>
                    </div>
                  )
                })}
              </div>
            </div>
          ))}
        </div>


        {/* Measure Selector */}
        <div className="mt-4 pt-3 border-t border-gray-200">
          <div className="flex flex-col sm:flex-row sm:items-center gap-4">
            <h4 className="text-sm font-medium text-gray-500 whitespace-nowrap w-32 shrink-0">
              📊 Độ đo (Measures)
            </h4>
            <div className="flex flex-wrap items-center gap-2">
              {activeCube === 'sales' ? (
                <>
                  <label className="inline-flex items-center text-xs cursor-pointer">
                    <input
                      type="radio"
                      name="measure-sales"
                      checked={fields.measures[0] === 'total_quantity'}
                      onChange={() => setMeasures(['total_quantity'])}
                      className="w-4 h-4 rounded border-gray-300 cursor-pointer"
                    />
                    <span className="ml-2 px-3 py-1.5 bg-blue-50 border border-blue-200 rounded text-gray-700 font-medium">
                      📦 Tổng qty
                    </span>
                  </label>
                  <label className="inline-flex items-center text-xs cursor-pointer">
                    <input
                      type="radio"
                      name="measure-sales"
                      checked={fields.measures[0] === 'sum_amount'}
                      onChange={() => setMeasures(['sum_amount'])}
                      className="w-4 h-4 rounded border-gray-300 cursor-pointer"
                    />
                    <span className="ml-2 px-3 py-1.5 bg-green-50 border border-green-200 rounded text-gray-700 font-medium">
                      💰 Tổng tiền
                    </span>
                  </label>
                </>
              ) : (
                <div className="px-3 py-1.5 bg-orange-50 border border-orange-200 rounded text-gray-700 font-medium text-xs">
                  📦 Tồn kho (total_quantity_on_hand)
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    )
  }

  // ============================================================================
  // DROP ZONES COMPONENT
  // ============================================================================
  const renderDropZones = () => {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Rows Zone */}
        <div className="drop-zone">
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-sm font-semibold text-gray-700">
              📥 Hàng (Rows)
            </h4>
            {fields.rows.length > 0 && (
              <button
                onClick={() => fields.rows.forEach(r => removeFromRows(r.id))}
                className="text-xs text-red-500 hover:text-red-700"
              >
                Xóa tất cả
              </button>
            )}
          </div>
          <div className="flex flex-wrap gap-2 min-h-[40px]">
            {fields.rows.length === 0 ? (
              <p className="text-sm text-gray-400 italic">Kéo trường vào đây</p>
            ) : (
              fields.rows.map((field, index) => (
                <div
                  key={field.id}
                  className="flex items-center gap-1 bg-blue-100 text-blue-800 px-3 py-1 rounded-full text-sm"
                >
                  <span>{index + 1}. {field.name}</span>
                  <button
                    onClick={() => removeFromRows(field.id)}
                    className="text-blue-600 hover:text-blue-800 ml-1"
                  >
                    ✕
                  </button>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Columns Zone */}
        <div className="drop-zone">
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-sm font-semibold text-gray-700">
              📤 Cột (Columns)
            </h4>
            {fields.columns.length > 0 && (
              <button
                onClick={() => fields.columns.forEach(c => removeFromColumns(c.id))}
                className="text-xs text-red-500 hover:text-red-700"
              >
                Xóa tất cả
              </button>
            )}
          </div>
          <div className="flex flex-wrap gap-2 min-h-[40px]">
            {fields.columns.length === 0 ? (
              <p className="text-sm text-gray-400 italic">Kéo trường vào đây</p>
            ) : (
              fields.columns.map((field, index) => (
                <div
                  key={field.id}
                  className="flex items-center gap-1 bg-green-100 text-green-800 px-3 py-1 rounded-full text-sm"
                >
                  <span>{index + 1}. {field.name}</span>
                  <button
                    onClick={() => removeFromColumns(field.id)}
                    className="text-green-600 hover:text-green-800 ml-1"
                  >
                    ✕
                  </button>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    )
  }

  // ============================================================================
  // TOOLBAR COMPONENT
  // ============================================================================
  const renderToolbar = () => {
    return (
      <div className="flex flex-wrap items-center gap-3 bg-gray-50 p-3 rounded-lg">
        {/* View Mode Toggle */}
        <div className="flex bg-white rounded-lg border border-gray-200">
          <button
            onClick={() => setViewMode('pivot')}
            className={`px-4 py-2 text-sm font-medium rounded-l-lg ${viewMode === 'pivot'
              ? 'bg-blue-600 text-white'
              : 'text-gray-600 hover:bg-gray-100'
              }`}
          >
            📊 Pivot Table
          </button>

        </div>

        <div className="w-px h-6 bg-gray-300"></div>

        {/* Actions */}
        <button
          onClick={swapAxes}
          disabled={fields.rows.length === 0 && fields.columns.length === 0}
          className="btn btn-secondary text-sm disabled:opacity-50"
          title="Xoay trục (Swap Rows & Columns)"
        >
          🔄 Xoay trục
        </button>

        <button
          onClick={clearAll}
          className="btn btn-danger text-sm"
          title="Xóa tất cả lựa chọn"
        >
          🗑️ Xóa tất cả
        </button>

        <button
          onClick={() => viewMode === 'pivot' ? fetchPivotData() : fetchRawData(1)}
          disabled={loading.pivot || loading.raw}
          className="btn btn-primary text-sm disabled:opacity-50"
        >
          {loading.pivot || loading.raw ? '⏳ Đang tải...' : '🔄 Làm mới'}
        </button>


        {/* Info */}
        {pivotData?.metadata && (
          <div className="ml-auto text-sm text-gray-500">
            Bảng: <span className="font-medium text-blue-600">
              {pivotData.metadata.selected_table}
            </span>
            {' | '}
            {pivotData.metadata.row_count} bản ghi
          </div>
        )}
      </div>
    )
  }

  // ============================================================================
  // PIVOT TABLE COMPONENT
  // ============================================================================
  const renderPivotTable = () => {
    if (loading.pivot) {
      return (
        <div className="h-96 flex items-center justify-center">
          <LoadingSpinner size="lg" />
        </div>
      )
    }

    if (error) {
      return (
        <div className="h-96 flex items-center justify-center">
          <div className="text-center">
            <p className="text-red-500 mb-2">❌ {error}</p>
            <button
              onClick={() => fetchPivotData()}
              className="btn btn-primary text-sm"
            >
              Thử lại
            </button>
          </div>
        </div>
      )
    }

    if (!pivotData?.data || pivotData.data.length === 0) {
      return (
        <div className="h-96 flex items-center justify-center">
          <div className="text-center text-gray-500">
            <p className="text-lg mb-2">📊 Chưa có dữ liệu</p>
            <p className="text-sm">Hãy chọn các trường dữ liệu để xem Pivot Table</p>
          </div>
        </div>
      )
    }

    const data = pivotData.data;
    const rowFields = fields.rows;
    const colFields = fields.columns;
    const measure = fields.measures[0] || 'sum_amount';

    // Xử lý dữ liệu Pivot 2D thực sự
    const colValues = colFields.length > 0
      ? [...new Set(data.map(d => colFields.map(c => d[c.column] || 'N/A').join(' | ')))]
          .sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }))
      : [];

    const pivotMatrix = [];

    // Build pivotMatrix
    if (colFields.length === 0) {
      pivotMatrix.push(...data);
    } else {
      const groupMap = {};
      data.forEach(row => {
        const rowKey = rowFields.length > 0
          ? rowFields.map(rf => row[rf.column]).join(' | ')
          : 'Total';
        const colKey = colFields.map(cf => row[cf.column] || 'N/A').join(' | ');
        if (!groupMap[rowKey]) {
          groupMap[rowKey] = {};
          rowFields.forEach(rf => { groupMap[rowKey][rf.column] = row[rf.column]; });
        }
        groupMap[rowKey][colKey] = row[measure];
      });
      pivotMatrix.push(...Object.values(groupMap));
    }

    const formatVal = (val) => {
      if (val === undefined || val === null || val === 0) return '-';
      return measure.includes('amount') ? formatCurrency(val) : formatNumber(val);
    };

    return (
      <div className="w-full overflow-x-auto overflow-y-auto border border-[#bacfa0]/50 rounded-lg max-h-[600px] shadow-inner">
        <table className="w-full text-sm text-left min-w-max relative">
          <thead className="sticky top-0 z-10 shadow-[0_1px_2px_rgba(0,0,0,0.1)]">
            <tr>
              {/* Headers cho Rows - click to sort */}
              {rowFields.map(field => {
                const isSorted = pivotSort.sort_column === field.column
                const nextOrder = isSorted && pivotSort.sort_order === 'asc' ? 'desc' : 'asc'
                return (
                  <th
                    key={field.id}
                    className="text-left bg-[#bacfa0] border-b border-[#a1b886] p-2 font-semibold text-[#30401d] cursor-pointer select-none hover:bg-[#a8c28e] transition-colors"
                    onClick={() => setPivotSort(field.column, nextOrder)}
                    title={`Sắp xếp theo ${field.name}`}
                  >
                    <span className="flex items-center gap-1">
                      {field.name}
                      {isSorted ? (pivotSort.sort_order === 'asc' ? ' ↑' : ' ↓') : ' ⇅'}
                    </span>
                  </th>
                )
              })}

              {/* Headers cho Measure / Columns - click to sort */}
              {colFields.length === 0 ? (
                <th
                  className="text-right bg-[#bacfa0] border-b border-[#a1b886] p-2 font-semibold text-[#30401d] cursor-pointer select-none hover:bg-[#a8c28e] transition-colors"
                  onClick={() => setPivotSort(measure, pivotSort.sort_column === measure && pivotSort.sort_order === 'asc' ? 'desc' : 'asc')}
                >
                  <span className="flex items-center justify-end gap-1">
                    {measure === 'sum_amount' ? 'Tổng doanh thu' : measure === 'total_quantity_on_hand' ? 'Tồn kho' : 'Tổng số lượng'}
                    {pivotSort.sort_column === measure ? (pivotSort.sort_order === 'asc' ? ' ↑' : ' ↓') : ' ⇅'}
                  </span>
                </th>
              ) : (
                colValues.map(colVal => (
                  <th key={colVal} className="text-right bg-[#bacfa0] border-b border-[#a1b886] p-2 font-semibold text-[#30401d]">
                    {colVal}
                  </th>
                ))
              )}

              {/* Cột Tổng dòng ngang - click to sort (Server-side) */}
              {colFields.length > 0 && (
                <th
                  className="text-right bg-[#9cb879] border-b border-[#82a356] p-2 font-bold text-[#223011] cursor-pointer select-none hover:bg-[#88a85e] transition-colors"
                  onClick={() => {
                    const isTotalSorted = pivotSort.sort_column === '__row_total__';
                    const nextOrder = isTotalSorted && pivotSort.sort_order === 'asc' ? 'desc' : 'asc';
                    setPivotSort('__row_total__', nextOrder);
                  }}
                  title="Sắp xếp theo tổng dòng (toàn bộ hệ thống)"
                >
                  <span className="flex items-center justify-end gap-1">
                    Tổng Tất Cả
                    {pivotSort.sort_column === '__row_total__' 
                      ? (pivotSort.sort_order === 'asc' ? ' ↑' : ' ↓') 
                      : ' ⇅'}
                  </span>
                </th>
              )}
            </tr>
          </thead>
          <tbody>
            {pivotMatrix.map((row, idx) => {
              let rowTotal = 0;
              return (
                <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                  {/* Row values */}
                  {rowFields.map(rf => (
                    <td key={rf.id} className="p-2 text-left font-medium text-gray-700 bg-gray-50/50">
                      {row[rf.column]}
                    </td>
                  ))}

                  {/* Measure values */}
                  {colFields.length === 0 ? (
                    <td className="p-2 text-right text-blue-700 font-medium whitespace-nowrap">
                      {formatVal(row[measure])}
                    </td>
                  ) : (
                    colValues.map(colVal => {
                      const val = parseFloat(row[colVal]) || 0;
                      rowTotal += val;
                      return (
                        <td key={colVal} className="p-2 text-right text-gray-700 whitespace-nowrap">
                          {formatVal(val)}
                        </td>
                      );
                    })
                  )}

                  {/* O Tổng cộng cuối dòng */}
                  {colFields.length > 0 && (
                    <td className="p-2 text-right font-bold text-orange-700 bg-orange-50/50 whitespace-nowrap">
                      {formatVal(rowTotal)}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>

        {/* Pivot Pagination Bar */}
        <div className="flex flex-wrap items-center justify-between mt-4 px-1 gap-3">
          {/* Left: Tổng số dòng + page size */}
          <div className="flex items-center gap-3 text-sm text-gray-600">
            <span>
              {pivotPagination.total_rows > 0
                ? `Hiển thị ${(pivotPagination.page - 1) * pivotPagination.page_size + 1}–${Math.min(pivotPagination.page * pivotPagination.page_size, pivotPagination.total_rows)} trong ${pivotPagination.total_rows.toLocaleString()} đối tượng`
                : `${pivotData?.data?.length || 0} dòng`
              }
            </span>
            <label className="flex items-center gap-1">
              Hiển:
              <select
                value={pivotPagination.page_size}
                onChange={(e) => setPivotPageSize(Number(e.target.value))}
                className="ml-1 border border-gray-300 rounded px-2 py-0.5 text-sm focus:outline-none focus:ring-1 focus:ring-[#799351]"
              >
                {[25, 50, 100, 200].map(n => <option key={n} value={n}>{n}/trang</option>)}
              </select>
            </label>
          </div>

          {/* Right: Pagination buttons */}
          {pivotPagination.total_pages > 1 && (
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPivotPage(1)}
                disabled={pivotPagination.page === 1 || loading.pivot}
                className="px-2 py-1 text-sm border border-gray-300 rounded hover:bg-gray-100 disabled:opacity-40"
                title="Trang đầu"
              >«</button>
              <button
                onClick={() => setPivotPage(pivotPagination.page - 1)}
                disabled={pivotPagination.page === 1 || loading.pivot}
                className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-100 disabled:opacity-40"
              >← Trước</button>
              <span className="text-sm text-gray-700 font-medium px-1">
                Trang {pivotPagination.page} / {pivotPagination.total_pages}
              </span>
              <button
                onClick={() => setPivotPage(pivotPagination.page + 1)}
                disabled={pivotPagination.page >= pivotPagination.total_pages || loading.pivot}
                className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-100 disabled:opacity-40"
              >Sau →</button>
              <button
                onClick={() => setPivotPage(pivotPagination.total_pages)}
                disabled={pivotPagination.page >= pivotPagination.total_pages || loading.pivot}
                className="px-2 py-1 text-sm border border-gray-300 rounded hover:bg-gray-100 disabled:opacity-40"
                title="Trang cuối"
              >»</button>
            </div>
          )}
        </div>

        {/* Metadata */}
        {/* {pivotData.metadata && (
          <div className="mt-4 p-3 bg-gray-50 rounded-lg text-sm">
            <p className="text-gray-600">
              <strong>Thuật toán Cuboid Router:</strong>{' '}
              {pivotData.metadata.reason}
            </p>
            <p className="text-gray-500 mt-1">
              Dimensions: {pivotData.metadata.dimensions_used.join(', ')}
            </p>
          </div>
        )} */}
      </div>
    )
  }

  // ============================================================================
  // RAW DATA TABLE COMPONENT
  // ============================================================================
  const renderRawDataTable = () => {
    if (loading.raw) {
      return (
        <div className="h-96 flex items-center justify-center">
          <LoadingSpinner size="lg" />
        </div>
      )
    }

    if (!rawData || rawData.length === 0) {
      return (
        <div className="h-96 flex items-center justify-center">
          <p className="text-gray-500">Không có dữ liệu</p>
        </div>
      )
    }

    const columns = Object.keys(rawData[0])

    return (
      <div className="w-full flex flex-col">
        <div className="w-full overflow-x-auto overflow-y-auto border border-gray-200 rounded-lg max-h-[600px] shadow-inner">
          <table className="w-full data-table min-w-max relative">
            <thead className="sticky top-0 z-10 bg-gray-100 shadow-[0_1px_2px_rgba(0,0,0,0.1)]">
              <tr>
                {columns.map(col => (
                  <th key={col} className="whitespace-nowrap">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rawData.map((row, idx) => (
                <tr key={idx}>
                  {columns.map(col => (
                    <td key={col} className="whitespace-nowrap">
                      {col.includes('amount')
                        ? formatCurrency(row[col])
                        : col.includes('quantity')
                          ? formatNumber(row[col])
                          : row[col]
                      }
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div className="flex items-center justify-between mt-4 px-4">
          <div className="text-sm text-gray-500">
            Hiển thị {(rawDataPagination.page - 1) * rawDataPagination.page_size + 1} -{' '}
            {Math.min(rawDataPagination.page * rawDataPagination.page_size, rawDataPagination.total)}{' '}
            của {rawDataPagination.total} bản ghi
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => fetchRawData(rawDataPagination.page - 1)}
              disabled={rawDataPagination.page === 1 || loading.raw}
              className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-100 disabled:opacity-50"
            >
              ← Trước
            </button>

            <span className="text-sm text-gray-600">
              Trang {rawDataPagination.page} / {rawDataPagination.total_pages}
            </span>

            <button
              onClick={() => fetchRawData(rawDataPagination.page + 1)}
              disabled={rawDataPagination.page === rawDataPagination.total_pages || loading.raw}
              className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-100 disabled:opacity-50"
            >
              Sau →
            </button>
          </div>
        </div>
      </div>
    )
  }

  // ============================================================================
  // MAIN RENDER
  // ============================================================================
  return (
    <div className="space-y-6 animate-fade-in">
      {/* Title */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">
            🔍 OLAP Explorer
          </h2>
          <p className="text-sm text-gray-500 mt-1">
            Khám phá dữ liệu với Pivot Table và các phép toán OLAP
          </p>
        </div>
      </div>

      {/* Field Picker */}
      {renderFieldPicker()}

      {/* Drop Zones */}
      {renderDropZones()}

      {/* Toolbar */}
      {renderToolbar()}

      {/* Content */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 w-full overflow-hidden">
        <div className="p-4 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-800">
            {viewMode === 'pivot' ? '📊 Pivot Table' : '📄 Dữ liệu thô'}
          </h3>
        </div>
        <div className="p-4 w-full overflow-x-hidden">
          {viewMode === 'pivot' ? renderPivotTable() : renderRawDataTable()}
        </div>
      </div>

      {/* Instructions */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-blue-50 rounded-lg p-4">
          <h4 className="font-semibold text-blue-800 mb-2">🔪 Slice</h4>
          <p className="text-sm text-blue-600">
            Lọc dữ liệu theo một chiều bằng cách thêm điều kiện filter.
          </p>
        </div>

        <div className="bg-green-50 rounded-lg p-4">
          <h4 className="font-semibold text-green-800 mb-2">🎲 Dice</h4>
          <p className="text-sm text-green-600">
            Lọc dữ liệu theo nhiều chiều cùng lúc để tạo sub-cube.
          </p>
        </div>

        <div className="bg-purple-50 rounded-lg p-4">
          <h4 className="font-semibold text-purple-800 mb-2">🔄 Pivot</h4>
          <p className="text-sm text-purple-600">
            Xoay trục để đổi chỗ hàng và cột, nhìn dữ liệu từ góc độ khác.
          </p>
        </div>

        <div className="bg-orange-50 rounded-lg p-4">
          <h4 className="font-semibold text-orange-800 mb-2">⬇️ Drill-down</h4>
          <p className="text-sm text-orange-600">
            Khoan sâu dữ liệu bằng cách thêm chiều chi tiết hơn vào rows.
          </p>
        </div>
      </div>
    </div>
  )
}

export default OlapExplorer
