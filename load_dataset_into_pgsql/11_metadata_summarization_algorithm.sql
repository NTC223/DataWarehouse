SET search_path TO metadata;

INSERT INTO summarization_algorithm (algorithm_name, target_table, description, formula) VALUES
('Total Sales Amount',  'Fact_Sales',     'Tổng tiền mỗi dòng đơn hàng',      'SUM(ordered_quantity * ordered_price)'),
('Quantity Ordered',    'Fact_Sales',     'Tổng số lượng sản phẩm đã bán',     'SUM(ordered_quantity)'),
('Quantity On Hand',    'Fact_Inventory', 'Tổng tồn kho theo cửa hàng/sản phẩm', 'SUM(stock_quantity)');
