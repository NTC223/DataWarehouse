/**
 * ============================================================================
 * OLAPEXPLORER.JSX - OLAP Explorer Component (Tab 2)
 * ============================================================================
 * Giao diện hierarchy picker:
 *   ☑ 🕐 Thời gian      [Hàng ▼]
 *       ├─  [year]  [quarter]  [month]
 *   ☑ 👥 Khách hàng    [Hàng ▼]
 *       ├─  [customer_type]  [customer_key]      ← Loại KH
 *       └─  [state]  [city]  [customer_key]     ← Địa lý KH (mutex)
 *
 * Logic:
 * - Dimension checkbox: check → select all fields; uncheck → deselect all
 * - Field chip: click → toggle
 * - Axis dropdown: Hàng / Cột / ─
 * - Mutex: customer_type ↔ customer_location fields
 * ============================================================================
 */

import React, { useEffect } from 'react'
import { useOlapStore, formatCurrency, formatNumber, CUBE_HIERARCHIES } from '../stores/store'
import LoadingSpinner from './LoadingSpinner'
import DynamicOlapChart from './DynamicOlapChart'

// ─── Axis Dropdown ────────────────────────────────────────────────────────────
function AxisDropdown({ currentAxis, onChange }) {
  return (
    <select
      value={currentAxis ?? ''}
      onChange={(e) => onChange(e.target.value || null)}
      className={`text-xs border rounded px-2 py-1 font-medium transition-colors ${
        currentAxis === 'rows' ? 'border-green-400 bg-green-50 text-green-700' :
        currentAxis === 'columns' ? 'border-green-400 bg-green-50 text-green-700' :
        'border-gray-300 bg-white text-gray-400'
      } focus:outline-none focus:ring-1 focus:ring-green-400`}
    >
      <option value="">Chọn</option>
      <option value="rows">Hàng ↓</option>
      <option value="columns">Cột →</option>
    </select>
  )
}

// ─── Field Chip ───────────────────────────────────────────────────────────────
function FieldChip({ field, isSelected, isMutexBlocked, onToggle }) {
  return (
    <button
      onClick={() => !isMutexBlocked && onToggle()}
      disabled={isMutexBlocked}
      className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium border transition-all ${
        isSelected
          ? 'bg-green-500 border-green-500 text-white shadow-sm'
          : isMutexBlocked
          ? 'opacity-30 cursor-not-allowed border-gray-200 text-gray-400 bg-gray-50'
          : 'border-gray-200 bg-white text-gray-600 hover:border-green-300 hover:text-green-600 hover:bg-green-50'
      }`}
      title={isMutexBlocked ? 'Không khả dụng (mutex)' : (isSelected ? 'Bỏ chọn' : 'Chọn')}
    >
      {field.name}
    </button>
  )
}

// ─── Dimension Group ─────────────────────────────────────────────────────────
function DimensionGroup({ cube, dim, dimState, onAxisChange, onToggleField, onSelectAll, onDeselectAll }) {
  const selected = dimState?.selected ?? new Set()
  const allFieldIds = dim.hierarchies.flatMap(h => h.fields.map(f => f.id))
  const allSelected = allFieldIds.every(id => selected.has(id))
  const someSelected = allFieldIds.some(id => selected.has(id))

  // Kiểm tra field có bị block bởi mutex không
  const isFieldMutexBlocked = (fieldId) => {
    if (selected.has(fieldId)) return false
    // Nếu đã chọn field thuộc mutex group → field thuộc group kia bị blocked
    const hId = (() => {
      for (const h of dim.hierarchies) {
        if (h.fields.find(f => f.id === fieldId)) return h.id
      }
      return null
    })()
    const MUTEX = { customer_type: 'customer_location', customer_location: 'customer_type' }
    const partnerH = MUTEX[hId]
    if (!partnerH) return false
    const partnerDim = CUBE_HIERARCHIES[cube]?.find(d => d.dimensionId === dim.dimensionId)
    if (!partnerDim) return false
    const partnerHObj = partnerDim.hierarchies.find(h => h.id === partnerH)
    if (!partnerHObj) return false
    return partnerHObj.fields.some(f => selected.has(f.id))
  }

  const handleHeaderToggle = () => {
    if (allSelected) {
      onDeselectAll()
    } else {
      onSelectAll()
    }
  }

  return (
    <div className="bg-gray-50 rounded-lg border border-gray-200 p-3">
      {/* Header row */}
      <div className="flex items-center gap-2 mb-2">

        {/* Checkbox: ☑ all / ◑ some / ☐ none */}
        <span
          onClick={handleHeaderToggle}
          className={`w-4 h-4 rounded border flex items-center justify-center shrink-0 text-xs font-bold cursor-pointer transition-colors ${
            allSelected
              ? 'bg-green-500 border-green-500 text-white'
              : someSelected
              ? 'bg-green-200 border-green-400 text-green-700'
              : 'border-gray-300 bg-white'
          }`}
          title={allSelected ? 'Bỏ chọn tất cả' : 'Chọn tất cả'}
        >
          {allSelected ? '✓' : someSelected ? '◑' : ''}
        </span>

        {/* Label */}
        <span
          onClick={handleHeaderToggle}
          className={`text-sm font-semibold flex-1 cursor-pointer transition-colors ${
            allSelected
              ? 'font-bold text-green-700'
              : someSelected
              ? 'font-semibold text-green-600'
              : 'text-gray-700 hover:text-green-600'
          }`}
          title={allSelected ? 'Bỏ chọn tất cả' : 'Chọn tất cả'}
        >
          {dim.dimensionLabel}
        </span>

        {/* Axis */}
        <AxisDropdown
          currentAxis={dimState?.axis ?? null}
          onChange={(axis) => onAxisChange(cube, dim.dimensionId, axis)}
        />
      </div>

      {/* Hierarchy sections */}
      <div className="flex flex-col gap-1.5">
        {dim.hierarchies.map((h) => (
          <div key={h.id}>
            {/* Hierarchy label */}
            <div className="text-xs text-gray-400 mb-1">{h.label}</div>
            {/* Field chips */}
            <div className="flex flex-wrap items-center gap-1">
              {h.fields.map((field) => (
                <FieldChip
                  key={field.id}
                  field={field}
                  isSelected={selected.has(field.id)}
                  isMutexBlocked={isFieldMutexBlocked(field.id)}
                  onToggle={() => onToggleField(cube, field.id)}
                />
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function OlapExplorer() {
  const {
    fields,
    olapState,
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
    toggleField,
    selectAllDimension,
    deselectAllDimension,
    setDimensionAxis,
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
    if (fields.rows.length > 0 || fields.columns.length > 0) {
      fetchPivotData(1)
    }
  }, [fields.rows, fields.columns, fields.measures, activeCube])

  useEffect(() => {
    if (viewMode === 'raw') {
      fetchRawData(1)
    }
  }, [viewMode, activeCube])

  // ============================================================================
  // FIELD PICKER COMPONENT
  // ============================================================================
  const renderFieldPicker = () => {
    const cube = activeCube
    const dimensions = CUBE_HIERARCHIES[cube] ?? []
    const cubeState = olapState[cube] ?? {}

    return (
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-800">📋 Chọn trường dữ liệu</h3>

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

        {/* Dimension columns — horizontal layout */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          {dimensions.map((dim) => (
            <DimensionGroup
              key={dim.dimensionId}
              cube={cube}
              dim={dim}
              dimState={cubeState[dim.dimensionId]}
              onAxisChange={setDimensionAxis}
              onToggleField={toggleField}
              onSelectAll={(dimId) => selectAllDimension(cube, dimId)}
              onDeselectAll={(dimId) => deselectAllDimension(cube, dimId)}
            />
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
                      📦 Tổng slg
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
  // CHART COMPONENT
  // ============================================================================
  const renderChart = () => {
    if (loading.pivot) {
      return (
        <div className="h-[500px] flex items-center justify-center">
          <LoadingSpinner size="lg" />
        </div>
      )
    }

    if (!pivotData?.data || pivotData.data.length === 0) {
      return (
        <div className="h-[500px] flex items-center justify-center">
          <div className="text-center text-gray-500">
            <p className="text-lg mb-2">📈 Chưa có dữ liệu</p>
            <p className="text-sm">Hãy chọn các trường dữ liệu để xem biểu đồ</p>
          </div>
        </div>
      )
    }

    const page = pivotPagination.page || 1

    return (
      <div className="w-full">
        <DynamicOlapChart
          data={pivotData.data}
          activeRows={fields.rows}
          activeColumns={fields.columns}
          currentPage={page}
        />
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

      {/* Toolbar */}
      {renderToolbar()}

      {/* Content */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 w-full overflow-hidden">
        <div className="p-4 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-800">
            📊 Pivot Table
          </h3>
        </div>
        <div className="p-4 w-full overflow-x-hidden">
          {renderPivotTable()}
        </div>
      </div>

      {/* Chart */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 w-full overflow-hidden">
        <div className="p-4 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-800">
            📈 Biểu đồ
          </h3>
        </div>
        <div className="p-4 w-full overflow-x-hidden">
          {renderChart()}
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
