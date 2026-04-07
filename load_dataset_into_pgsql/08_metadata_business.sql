SET search_path TO metadata;

INSERT INTO business_metadata (term_name, definition, data_owner, data_steward, sensitivity_level, related_tables, charging_policy) VALUES
('Total Amount',      'Tổng doanh thu = ordered_quantity * ordered_price, chưa trừ chiết khấu', 'Finance Dept',    'Data Team', 'internal',      'Fact_Sales',               'Chỉ Finance được xem chi tiết từng đơn'),
('Customer Type',     'Tourist: mua tại quầy qua tour guide. MailOrder: mua qua bưu điện',      'Sales Dept',      'Data Team', 'internal',      'Dim_Customer',             'Public nội bộ'),
('Quantity On Hand',  'Số lượng tồn kho thực tế tại từng cửa hàng tại thời điểm ghi nhận',      'Warehouse Dept',  'Data Team', 'internal',      'Fact_Inventory, Dim_Store', 'Warehouse và Management xem được'),
('city_id',           'Khoá ngoại dùng chung giữa MySQL Source 1 và PostgreSQL Source 2',        'Data Engineering','Data Team', 'internal',      'Customer, RepresentativeOffice', 'Internal only');
