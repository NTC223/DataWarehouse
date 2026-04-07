SET search_path TO metadata;

INSERT INTO data_dictionary
(pipeline_stage, source_schema, source_table, source_column, source_data_type,
 target_schema, target_table, target_column, target_data_type, transformation_rule) VALUES

-- ── MySQL Source 1 → IDB ────────────────────────────────────
('source_to_idb', NULL, 'Customer',         'customer_id',      'int',     'idb', 'Customer',         'customer_id',       'int',     NULL),
('source_to_idb', NULL, 'Customer',         'customer_name',    'varchar', 'idb', 'Customer',         'customer_name',     'varchar', NULL),
('source_to_idb', NULL, 'Customer',         'city_id',          'int',     'idb', 'Customer',         'city_id',           'int',     NULL),
('source_to_idb', NULL, 'Customer',         'first_order_date', 'date',    'idb', 'Customer',         'first_order_date',  'date',    NULL),
('source_to_idb', NULL, 'TouristCustomer',  'customer_id',      'int',     'idb', 'TouristCustomer',  'customer_id',       'int',     NULL),
('source_to_idb', NULL, 'TouristCustomer',  'tour_guide',       'varchar', 'idb', 'TouristCustomer',  'tour_guide',        'varchar', 'TRIM(tour_guide)'),
('source_to_idb', NULL, 'TouristCustomer',  'time',             'date',    'idb', 'TouristCustomer',  'last_updated_time', 'date',    'RENAME: time → last_updated_time'),
('source_to_idb', NULL, 'MailOrderCustomer','customer_id',      'int',     'idb', 'MailOrderCustomer','customer_id',       'int',     NULL),
('source_to_idb', NULL, 'MailOrderCustomer','postal_address',   'varchar', 'idb', 'MailOrderCustomer','postal_address',    'varchar', 'TRIM(postal_address)'),
('source_to_idb', NULL, 'MailOrderCustomer','time',             'date',    'idb', 'MailOrderCustomer','last_updated_time', 'date',    'RENAME: time → last_updated_time'),

-- ── PostgreSQL Source 2 → IDB ───────────────────────────────
('source_to_idb', 'sales_source', 'RepresentativeOffice','city_id',        'int',     'idb', 'RepresentativeOffice','city_id',           'int',     NULL),
('source_to_idb', 'sales_source', 'RepresentativeOffice','city_name',      'varchar', 'idb', 'RepresentativeOffice','city_name',         'varchar', 'TRIM(city_name)'),
('source_to_idb', 'sales_source', 'RepresentativeOffice','office_address', 'varchar', 'idb', 'RepresentativeOffice','office_address',    'varchar', NULL),
('source_to_idb', 'sales_source', 'RepresentativeOffice','state',          'varchar', 'idb', 'RepresentativeOffice','state',             'varchar', NULL),
('source_to_idb', 'sales_source', 'RepresentativeOffice','time',           'date',    'idb', 'RepresentativeOffice','last_updated_time', 'date',    'RENAME: time → last_updated_time'),
('source_to_idb', 'sales_source', 'Store',  'store_id',     'int',     'idb', 'Store',  'store_id',          'int',     NULL),
('source_to_idb', 'sales_source', 'Store',  'phone_number', 'varchar', 'idb', 'Store',  'phone_number',      'varchar', NULL),
('source_to_idb', 'sales_source', 'Store',  'city_id',      'int',     'idb', 'Store',  'city_id',           'int',     NULL),
('source_to_idb', 'sales_source', 'Store',  'time',         'date',    'idb', 'Store',  'last_updated_time', 'date',    'RENAME: time → last_updated_time'),
('source_to_idb', 'sales_source', 'Product','product_id',   'int',     'idb', 'Product','product_id',        'int',     NULL),
('source_to_idb', 'sales_source', 'Product','description',  'varchar', 'idb', 'Product','description',       'varchar', 'TRIM(description)'),
('source_to_idb', 'sales_source', 'Product','size',         'varchar', 'idb', 'Product','size',              'varchar', 'UPPER(size)'),
('source_to_idb', 'sales_source', 'Product','weight',       'decimal', 'idb', 'Product','weight',            'decimal', 'ROUND(weight, 2)'),
('source_to_idb', 'sales_source', 'Product','price',        'decimal', 'idb', 'Product','price',             'decimal', 'ROUND(price, 2)'),
('source_to_idb', 'sales_source', 'Product','time',         'date',    'idb', 'Product','last_updated_time', 'date',    'RENAME: time → last_updated_time'),
('source_to_idb', 'sales_source', 'Order',  'order_id',     'int',     'idb', 'Order',  'order_id',          'int',     NULL),
('source_to_idb', 'sales_source', 'Order',  'order_date',   'date',    'idb', 'Order',  'order_date',        'date',    NULL),
('source_to_idb', 'sales_source', 'Order',  'customer_id',  'int',     'idb', 'Order',  'customer_id',       'int',     NULL),
('source_to_idb', 'sales_source', 'OrderProduct',  'order_id',          'int',     'idb', 'OrderProduct',  'order_id',          'int',     NULL),
('source_to_idb', 'sales_source', 'OrderProduct',  'product_id',        'int',     'idb', 'OrderProduct',  'product_id',        'int',     NULL),
('source_to_idb', 'sales_source', 'OrderProduct',  'ordered_quantity',  'int',     'idb', 'OrderProduct',  'ordered_quantity',  'int',     NULL),
('source_to_idb', 'sales_source', 'OrderProduct',  'ordered_price',     'decimal', 'idb', 'OrderProduct',  'ordered_price',     'decimal', NULL),
('source_to_idb', 'sales_source', 'OrderProduct',  'time',              'date',    'idb', 'OrderProduct',  'last_updated_time', 'date',    'RENAME: time → last_updated_time'),
('source_to_idb', 'sales_source', 'StockedProduct','store_id',          'int',     'idb', 'StockedProduct','store_id',          'int',     NULL),
('source_to_idb', 'sales_source', 'StockedProduct','product_id',        'int',     'idb', 'StockedProduct','product_id',        'int',     NULL),
('source_to_idb', 'sales_source', 'StockedProduct','stock_quantity',    'int',     'idb', 'StockedProduct','stock_quantity',    'int',     NULL),
('source_to_idb', 'sales_source', 'StockedProduct','time',              'date',    'idb', 'StockedProduct','last_updated_time', 'date',    'RENAME: time → last_updated_time');
