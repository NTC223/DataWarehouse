SET search_path TO metadata;

INSERT INTO data_lineage
(data_object, source_system, source_object, transformation_path, data_currency, refresh_frequency) VALUES
-- ── IDB layer ───────────────────────────────────────────────
('idb.RepresentativeOffice', 'PostgreSQL', 'sales_source.RepresentativeOffice',
 'PostgreSQL.RepresentativeOffice → idb.RepresentativeOffice → dwh.Dim_Location',              'active', 'daily'),
('idb.Customer',             'MySQL',      'Customer',
 'MySQL.Customer → idb.Customer → dwh.Dim_Customer',                                           'active', 'daily'),
('idb.TouristCustomer',      'MySQL',      'TouristCustomer',
 'MySQL.TouristCustomer → idb.TouristCustomer → dwh.Dim_Customer (customer_type)',             'active', 'daily'),
('idb.MailOrderCustomer',    'MySQL',      'MailOrderCustomer',
 'MySQL.MailOrderCustomer → idb.MailOrderCustomer → dwh.Dim_Customer (customer_type)',         'active', 'daily'),
('idb.Store',                'PostgreSQL', 'sales_source.Store',
 'PostgreSQL.Store → idb.Store → dwh.Dim_Store',                                               'active', 'daily'),
('idb.Product',              'PostgreSQL', 'sales_source.Product',
 'PostgreSQL.Product → idb.Product → dwh.Dim_Product',                                         'active', 'daily'),
('idb.Order',                'PostgreSQL', 'sales_source.Order',
 'PostgreSQL.Order → idb.Order → dwh.Fact_Sales (time_key)',                                   'active', 'daily'),
('idb.OrderProduct',         'PostgreSQL', 'sales_source.OrderProduct',
 'PostgreSQL.OrderProduct → idb.OrderProduct → dwh.Fact_Sales',                                'active', 'daily'),
('idb.StockedProduct',       'PostgreSQL', 'sales_source.StockedProduct',
 'PostgreSQL.StockedProduct → idb.StockedProduct → dwh.Fact_Inventory',                        'active', 'daily'),

-- ── DWH layer ───────────────────────────────────────────────
('dwh.Dim_Location',  'IDB', 'idb.RepresentativeOffice',
 'idb.RepresentativeOffice → dwh.Dim_Location',                                                'active', 'daily'),
('dwh.Dim_Customer',  'IDB', 'idb.Customer + idb.TouristCustomer + idb.MailOrderCustomer',
 'JOIN 3 bảng IDB → Dim_Customer',                                                             'active', 'daily'),
('dwh.Dim_Store',     'IDB', 'idb.Store',
 'idb.Store → dwh.Dim_Store',                                                                  'active', 'daily'),
('dwh.Dim_Product',   'IDB', 'idb.Product',
 'idb.Product → dwh.Dim_Product',                                                              'active', 'daily'),
('dwh.Dim_Time',      'IDB', 'idb.Order',
 'DERIVE từ idb.Order.order_date → dwh.Dim_Time',                                              'active', 'daily'),
('dwh.Fact_Sales',    'IDB', 'idb.OrderProduct + idb.Order',
 'JOIN OrderProduct + Order → GROUP BY → dwh.Fact_Sales',                                       'active', 'daily'),
('dwh.Fact_Inventory','IDB', 'idb.StockedProduct',
 'idb.StockedProduct → dwh.Fact_Inventory',                                                    'active', 'daily');
