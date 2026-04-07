SET search_path TO metadata;

-- ── Schemas ──────────────────────────────────────────────────
INSERT INTO warehouse_structure (object_type, object_name, schema_name, description) VALUES
('schema', 'idb', 'idb', 'Integrated Database — tầng trung gian tích hợp 2 nguồn'),
('schema', 'dwh', 'dwh', 'Data Warehouse — tầng phân tích Star Schema');

-- ── IDB Tables ──────────────────────────────────────────────
INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description, columns_info) VALUES
('table', 'RepresentativeOffice', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Văn phòng đại diện / thành phố',
 '{"city_id":"int","city_name":"varchar","office_address":"varchar","state":"varchar","last_updated_time":"date"}'),
('table', 'Customer', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Khách hàng từ MySQL Source 1',
 '{"customer_id":"int","customer_name":"varchar","city_id":"int","first_order_date":"date"}'),
('table', 'TouristCustomer', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Khách hàng loại Tourist',
 '{"customer_id":"int","tour_guide":"varchar","last_updated_time":"date"}'),
('table', 'MailOrderCustomer', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Khách hàng loại MailOrder',
 '{"customer_id":"int","postal_address":"varchar","last_updated_time":"date"}'),
('table', 'Store', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Cửa hàng từ PostgreSQL Source 2',
 '{"store_id":"int","phone_number":"varchar","city_id":"int","last_updated_time":"date"}'),
('table', 'Product', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Sản phẩm từ PostgreSQL Source 2',
 '{"product_id":"int","description":"varchar","size":"varchar","weight":"decimal","price":"decimal","last_updated_time":"date"}'),
('table', 'Order', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Đơn hàng từ PostgreSQL Source 2',
 '{"order_id":"int","order_date":"date","customer_id":"int"}'),
('table', 'OrderProduct', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Chi tiết đơn hàng',
 '{"order_id":"int","product_id":"int","ordered_quantity":"int","ordered_price":"decimal","last_updated_time":"date"}'),
('table', 'StockedProduct', 'idb',
 (SELECT id FROM warehouse_structure WHERE object_name='idb' AND object_type='schema'),
 'Tồn kho',
 '{"store_id":"int","product_id":"int","stock_quantity":"int","last_updated_time":"date"}');

-- ── DWH Dimensions & Facts ──────────────────────────────────
INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description, columns_info) VALUES
('dimension', 'Dim_Time', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='dwh' AND object_type='schema'),
 'Chiều thời gian',
 '{"time_key":"int","month":"int","quarter":"int","year":"int"}'),
('dimension', 'Dim_Location', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='dwh' AND object_type='schema'),
 'Chiều địa điểm',
 '{"location_key":"int","city":"varchar","state":"varchar","office_address":"varchar"}'),
('dimension', 'Dim_Store', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='dwh' AND object_type='schema'),
 'Chiều cửa hàng',
 '{"store_key":"int","phone_number":"varchar","location_key":"int"}'),
('dimension', 'Dim_Customer', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='dwh' AND object_type='schema'),
 'Chiều khách hàng',
 '{"customer_key":"int","customer_name":"varchar","customer_type":"varchar","first_order_date":"date","location_key":"int"}'),
('dimension', 'Dim_Product', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='dwh' AND object_type='schema'),
 'Chiều sản phẩm',
 '{"product_key":"int","description":"varchar","size":"varchar","weight":"decimal"}'),
('fact', 'Fact_Sales', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='dwh' AND object_type='schema'),
 'Bảng fact doanh thu',
 '{"time_key":"int","product_key":"int","customer_key":"int","quantity_ordered":"int","total_amount":"decimal"}'),
('fact', 'Fact_Inventory', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='dwh' AND object_type='schema'),
 'Bảng fact tồn kho',
 '{"time_key":"int","store_key":"int","product_key":"int","quantity_on_hand":"int"}');

-- ═════════════════════════════════════════════════════════════
-- HIERARCHY DEFINITIONS (nhúng vào warehouse_structure)
-- Dùng cho OLAP Roll Up / Drill Down sau này
-- ═════════════════════════════════════════════════════════════

-- ── Hierarchy 1: Dim_Time — Calendar Hierarchy ──────────────
INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description)
VALUES ('hierarchy', 'Calendar Hierarchy', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Dim_Time' AND object_type='dimension'),
 'Roll up: Month → Quarter → Year | Drill down: Year → Quarter → Month');

INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description, columns_info, hierarchy_level) VALUES
('hierarchy_level', 'Year', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Calendar Hierarchy' AND object_type='hierarchy'),
 'Cấp cao nhất: tổng hợp theo năm', '{"column_name":"year"}', 1),
('hierarchy_level', 'Quarter', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Calendar Hierarchy' AND object_type='hierarchy'),
 'Cấp 2: tổng hợp theo quý (Q1-Q4)', '{"column_name":"quarter"}', 2),
('hierarchy_level', 'Month', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Calendar Hierarchy' AND object_type='hierarchy'),
 'Cấp thấp nhất: tổng hợp theo tháng (1-12)', '{"column_name":"month"}', 3);

-- ── Hierarchy 2: Dim_Location — Geographic Hierarchy ────────
INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description)
VALUES ('hierarchy', 'Geographic Hierarchy', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Dim_Location' AND object_type='dimension'),
 'Roll up: City → State | Drill down: State → City');

INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description, columns_info, hierarchy_level) VALUES
('hierarchy_level', 'State (Location)', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Geographic Hierarchy' AND object_type='hierarchy'),
 'Cấp cao nhất: tổng hợp theo bang/tỉnh', '{"column_name":"state"}', 1),
('hierarchy_level', 'City (Location)', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Geographic Hierarchy' AND object_type='hierarchy'),
 'Cấp thấp nhất: tổng hợp theo thành phố', '{"column_name":"city"}', 2);

-- ── Hierarchy 3: Dim_Customer — Customer Type Hierarchy ─────
INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description)
VALUES ('hierarchy', 'Customer Type Hierarchy', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Dim_Customer' AND object_type='dimension'),
 'Roll up: Customer → Customer Type | Drill down: Customer Type → Customer');

INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description, columns_info, hierarchy_level) VALUES
('hierarchy_level', 'Customer Type', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Customer Type Hierarchy' AND object_type='hierarchy'),
 'Nhóm: Tourist / MailOrder / Both', '{"column_name":"customer_type"}', 1),
('hierarchy_level', 'Customer', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Customer Type Hierarchy' AND object_type='hierarchy'),
 'Cá thể từng khách hàng', '{"column_name":"customer_name"}', 2);

-- ── Hierarchy 4: Dim_Store — Store Location Hierarchy ───────
INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description)
VALUES ('hierarchy', 'Store Location Hierarchy', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Dim_Store' AND object_type='dimension'),
 'Roll up: Store → City → State | Drill down: State → City → Store');

INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description, columns_info, hierarchy_level) VALUES
('hierarchy_level', 'State (Store)', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Store Location Hierarchy' AND object_type='hierarchy'),
 'Cấp cao: bang của cửa hàng (qua Dim_Location)', '{"column_name":"location_key → Dim_Location.state"}', 1),
('hierarchy_level', 'City (Store)', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Store Location Hierarchy' AND object_type='hierarchy'),
 'Cấp giữa: thành phố của cửa hàng (qua Dim_Location)', '{"column_name":"location_key → Dim_Location.city"}', 2),
('hierarchy_level', 'Store', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Store Location Hierarchy' AND object_type='hierarchy'),
 'Cấp thấp nhất: từng cửa hàng', '{"column_name":"store_key"}', 3);

-- ── Hierarchy 5: Dim_Product — Product Hierarchy ────────────
INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description)
VALUES ('hierarchy', 'Product Hierarchy', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Dim_Product' AND object_type='dimension'),
 'Nhóm theo kích cỡ → từng sản phẩm (có thể mở rộng nếu có category)');

INSERT INTO warehouse_structure (object_type, object_name, schema_name, parent_id, description, columns_info, hierarchy_level) VALUES
('hierarchy_level', 'Size', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Product Hierarchy' AND object_type='hierarchy'),
 'Nhóm theo kích cỡ: S/M/L/XL/XXL/Freesize', '{"column_name":"size"}', 1),
('hierarchy_level', 'Product', 'dwh',
 (SELECT id FROM warehouse_structure WHERE object_name='Product Hierarchy' AND object_type='hierarchy'),
 'Từng mặt hàng cụ thể', '{"column_name":"description"}', 2);
