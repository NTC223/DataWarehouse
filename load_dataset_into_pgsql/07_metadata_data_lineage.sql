SET search_path TO metadata;

INSERT INTO data_lineage
(data_object, source_system, source_object, transformation_path, data_currency, refresh_frequency) VALUES
-- ── IDB layer ───────────────────────────────────────────────
('idb.RepresentativeOffice', 'PostgreSQL', 'sales_source.RepresentativeOffice',
 'PostgreSQL.RepresentativeOffice → idb.RepresentativeOffice → dwh.Dim_Location',              'active', 'monthly'),
('idb.Customer',             'MySQL',      'Customer',
 'MySQL.Customer → idb.Customer → dwh.Dim_Customer',                                           'active', 'monthly'),
('idb.TouristCustomer',      'MySQL',      'TouristCustomer',
 'MySQL.TouristCustomer → idb.TouristCustomer → dwh.Dim_Customer (customer_type)',             'active', 'monthly'),
('idb.MailOrderCustomer',    'MySQL',      'MailOrderCustomer',
 'MySQL.MailOrderCustomer → idb.MailOrderCustomer → dwh.Dim_Customer (customer_type)',         'active', 'monthly'),
('idb.Store',                'PostgreSQL', 'sales_source.Store',
 'PostgreSQL.Store → idb.Store → dwh.Dim_Store',                                               'active', 'monthly'),
('idb.Product',              'PostgreSQL', 'sales_source.Product',
 'PostgreSQL.Product → idb.Product → dwh.Dim_Product',                                         'active', 'monthly'),
('idb.Order',                'PostgreSQL', 'sales_source.Order',
 'PostgreSQL.Order → idb.Order → dwh.Fact_Sales (time_key)',                                   'active', 'monthly'),
('idb.OrderProduct',         'PostgreSQL', 'sales_source.OrderProduct',
 'PostgreSQL.OrderProduct → idb.OrderProduct → dwh.Fact_Sales',                                'active', 'monthly'),
('idb.StockedProduct',       'PostgreSQL', 'sales_source.StockedProduct',
 'PostgreSQL.StockedProduct → idb.StockedProduct → dwh.Fact_Inventory',                        'active', 'monthly'),

-- ── DWH layer ───────────────────────────────────────────────
('dwh.Dim_Location',  'IDB', 'idb.RepresentativeOffice',
 'idb.RepresentativeOffice → dwh.Dim_Location',                                                'active', 'monthly'),
('dwh.Dim_Customer',  'IDB', 'idb.Customer + idb.TouristCustomer + idb.MailOrderCustomer',
 'JOIN 3 bảng IDB → Dim_Customer',                                                             'active', 'monthly'),
('dwh.Dim_Store',     'IDB', 'idb.Store',
 'idb.Store → dwh.Dim_Store',                                                                  'active', 'monthly'),
('dwh.Dim_Product',   'IDB', 'idb.Product',
 'idb.Product → dwh.Dim_Product',                                                              'active', 'monthly'),
('dwh.Dim_Time',      'IDB', 'idb.Order',
 'DERIVE từ idb.Order.order_date → dwh.Dim_Time',                                              'active', 'monthly'),
('dwh.Fact_Sales',    'IDB', 'idb.OrderProduct + idb.Order',
 'JOIN OrderProduct + Order → GROUP BY → dwh.Fact_Sales',                                       'active', 'monthly'),
('dwh.Fact_Inventory','IDB', 'idb.StockedProduct',
 'idb.StockedProduct → dwh.Fact_Inventory',                                                    'active', 'monthly');
