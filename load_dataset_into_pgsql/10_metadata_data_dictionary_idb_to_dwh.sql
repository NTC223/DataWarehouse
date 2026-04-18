SET search_path TO metadata;

INSERT INTO data_dictionary
(pipeline_stage, source_schema, source_table, source_column, source_data_type,
 target_schema, target_table, target_column, target_data_type, transformation_rule) VALUES

-- ── IDB → Dim_Location ──────────────────────────────────────
('idb_to_dwh', 'idb', 'RepresentativeOffice','city_id',        'int',    'dwh', 'Dim_Location','location_key',  'int',    'USE AS PK: city_id → location_key'),
('idb_to_dwh', 'idb', 'RepresentativeOffice','city_name',      'varchar','dwh', 'Dim_Location','city',           'varchar', 'RENAME: city_name → city'),
('idb_to_dwh', 'idb', 'RepresentativeOffice','state',          'varchar','dwh', 'Dim_Location','state',          'varchar', NULL),
('idb_to_dwh', 'idb', 'RepresentativeOffice','office_address', 'varchar','dwh', 'Dim_Location','office_address', 'varchar', NULL),

-- ── IDB → Dim_Store ─────────────────────────────────────────
('idb_to_dwh', 'idb', 'Store','store_id',     'int',    'dwh', 'Dim_Store','store_key',    'int',    'USE AS PK: store_id → store_key'),
('idb_to_dwh', 'idb', 'Store','phone_number', 'varchar','dwh', 'Dim_Store','phone_number', 'varchar', NULL),
('idb_to_dwh', 'idb', 'Store','city_id',      'int',    'dwh', 'Dim_Store','location_key', 'int',    'DIRECT MAP: city_id → location_key (= Dim_Location.location_key)'),

-- ── IDB → Dim_Customer ──────────────────────────────────────
('idb_to_dwh', 'idb', 'Customer','customer_id',      'int',    'dwh', 'Dim_Customer','customer_key',    'int',    'USE AS PK: customer_id → customer_key'),
('idb_to_dwh', 'idb', 'Customer','customer_name',    'varchar','dwh', 'Dim_Customer','customer_name',   'varchar', NULL),
('idb_to_dwh', 'idb', 'Customer','first_order_date', 'date',   'dwh', 'Dim_Customer','first_order_date','date',   NULL),
('idb_to_dwh', 'idb', 'Customer','city_id',          'int',    'dwh', 'Dim_Customer','location_key',    'int',    'DIRECT MAP: city_id → location_key (= Dim_Location.location_key)'),
('idb_to_dwh', 'idb', 'TouristCustomer+MailOrderCustomer','customer_id','int','dwh', 'Dim_Customer','customer_type','varchar',
 'CASE WHEN tourist IS NOT NULL AND mail IS NOT NULL THEN ''Both'' WHEN tourist IS NOT NULL THEN ''Tourist'' WHEN mail IS NOT NULL THEN ''MailOrder'' ELSE ''Unknown'' END'),

-- ── IDB → Dim_Product ───────────────────────────────────────
('idb_to_dwh', 'idb', 'Product','product_id',  'int',    'dwh', 'Dim_Product','product_key', 'int',    'USE AS PK: product_id → product_key'),
('idb_to_dwh', 'idb', 'Product','description', 'varchar','dwh', 'Dim_Product','description', 'varchar', NULL),
('idb_to_dwh', 'idb', 'Product','size',        'varchar','dwh', 'Dim_Product','size',        'varchar', NULL),
('idb_to_dwh', 'idb', 'Product','weight',      'decimal','dwh', 'Dim_Product','weight',      'decimal', NULL),

-- ── IDB → Dim_Time (derive từ order_date) ───────────────────
('idb_to_dwh', 'idb', 'Order','order_date','date','dwh', 'Dim_Time','time_key', 'int',    'TO_CHAR(order_date, ''YYYYMM'')::int'),
('idb_to_dwh', 'idb', 'Order','order_date','date','dwh', 'Dim_Time','month',    'int',    'EXTRACT(MONTH FROM order_date)::int'),
('idb_to_dwh', 'idb', 'Order','order_date','date','dwh', 'Dim_Time','quarter',  'int',    'EXTRACT(QUARTER FROM order_date)::int'),
('idb_to_dwh', 'idb', 'Order','order_date','date','dwh', 'Dim_Time','year',     'int',    'EXTRACT(YEAR FROM order_date)::int'),

-- ── IDB → Fact_Sales ────────────────────────────────────────
('idb_to_dwh', 'idb', 'Order',       'order_date',       'date',    'dwh', 'Fact_Sales','time_key',        'int',    'TO_CHAR(order_date, ''YYYYMM'')::int'),
('idb_to_dwh', 'idb', 'OrderProduct','product_id',       'int',     'dwh', 'Fact_Sales','product_key',     'int',    'DIRECT MAP'),
('idb_to_dwh', 'idb', 'Order',       'customer_id',      'int',     'dwh', 'Fact_Sales','customer_key',    'int',    'DIRECT MAP'),
('idb_to_dwh', 'idb', 'OrderProduct','ordered_quantity', 'int',     'dwh', 'Fact_Sales','quantity_ordered','int',    'SUM(ordered_quantity) GROUP BY time_key, product_key, customer_key'),
('idb_to_dwh', 'idb', 'OrderProduct','ordered_price',    'decimal', 'dwh', 'Fact_Sales','total_amount',    'decimal','SUM(ordered_quantity * ordered_price) GROUP BY time_key, product_key, customer_key'),

-- ── IDB → Fact_Inventory ────────────────────────────────────
('idb_to_dwh', 'idb', 'StockedProduct','last_updated_time','date','dwh', 'Fact_Inventory','time_key',       'int',  'TO_CHAR(MAX(last_updated_time), ''YYYYMM'')::int — Periodic Snapshot'),
('idb_to_dwh', 'idb', 'StockedProduct','store_id',         'int', 'dwh', 'Fact_Inventory','store_key',      'int',  'DIRECT MAP'),
('idb_to_dwh', 'idb', 'StockedProduct','product_id',       'int', 'dwh', 'Fact_Inventory','product_key',    'int',  'DIRECT MAP'),
('idb_to_dwh', 'idb', 'StockedProduct','stock_quantity',   'int', 'dwh', 'Fact_Inventory','quantity_on_hand','int', 'LATEST value per (time_key, store_key, product_key) — ON CONFLICT DO UPDATE');
