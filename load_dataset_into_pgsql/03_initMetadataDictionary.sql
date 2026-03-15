-- ===================================================================================
-- SCRIPT KHỞI TẠO TỪ ĐIỂN DỮ LIỆU (DATA DICTIONARY MAPPING)
-- Kịch bản này được nạp vào lúc khởi tạo hạ tầng DWH để Airflow/Python đọc và sinh mã.
-- ===================================================================================

-- 1. NẠP METADATA: GIAI ĐOẠN 2 NGUỒN (MySQL + Postgres) -> IDB
SET search_path TO metadata_source_to_idb;

-- Xóa dữ liệu cũ nếu chạy lại kịch bản
TRUNCATE TABLE Data_Dictionary_Mapping RESTART IDENTITY;

-- Hệ thống Nguồn 1 (MySQL RepresentativeOffice - db Khách hàng)
INSERT INTO Data_Dictionary_Mapping 
(source_system, source_table, source_column, source_data_type, target_schema, target_table, target_column, target_data_type, transformation_rule)
VALUES 
-- Bảng Customer
('MySQL RepresentativeOffice', 'Customer', 'customer_id', 'int', 'idb', 'Customer', 'customer_id', 'int', NULL),
('MySQL RepresentativeOffice', 'Customer', 'customer_name', 'varchar', 'idb', 'Customer', 'customer_name', 'varchar', 'TRIM(UPPER(source_column))'),
('MySQL RepresentativeOffice', 'Customer', 'city_id', 'int', 'idb', 'Customer', 'city_id', 'int', NULL),
('MySQL RepresentativeOffice', 'Customer', 'first_order_date', 'date', 'idb', 'Customer', 'first_order_date', 'date', 'COALESCE(source_column, CURRENT_DATE)'),

-- Bảng TouristCustomer
('MySQL RepresentativeOffice', 'TouristCustomer', 'customer_id', 'int', 'idb', 'TouristCustomer', 'customer_id', 'int', NULL),
('MySQL RepresentativeOffice', 'TouristCustomer', 'tour_guide', 'varchar', 'idb', 'TouristCustomer', 'tour_guide', 'varchar', NULL),
('MySQL RepresentativeOffice', 'TouristCustomer', 'time', 'date', 'idb', 'TouristCustomer', 'tour_time', 'date', NULL),

-- Bảng MailOrderCustomer
('MySQL RepresentativeOffice', 'MailOrderCustomer', 'customer_id', 'int', 'idb', 'MailOrderCustomer', 'customer_id', 'int', NULL),
('MySQL RepresentativeOffice', 'MailOrderCustomer', 'postal_address', 'varchar', 'idb', 'MailOrderCustomer', 'postal_address', 'varchar', NULL),
('MySQL RepresentativeOffice', 'MailOrderCustomer', 'time', 'date', 'idb', 'MailOrderCustomer', 'join_time', 'date', NULL);

-- Hệ thống Nguồn 2 (PostgreSQL Sales - db Bán hàng)
INSERT INTO Data_Dictionary_Mapping 
(source_system, source_table, source_column, source_data_type, target_schema, target_table, target_column, target_data_type, transformation_rule)
VALUES 
-- Bảng Store (Ví dụ có Rule ép dữ liệu SĐT Null thành Không xác định)
('PostgreSQL Sales', 'Store', 'store_id', 'int', 'idb', 'Store', 'store_id', 'int', NULL),
('PostgreSQL Sales', 'Store', 'phone_number', 'varchar', 'idb', 'Store', 'phone_number', 'varchar', 'COALESCE(source_column, ''Unknown'')'),
('PostgreSQL Sales', 'Store', 'time', 'date', 'idb', 'Store', 'Opening_time', 'date', NULL),
('PostgreSQL Sales', 'Store', 'city_id', 'int', 'idb', 'Store', 'city_id', 'int', NULL),

-- Bảng RepresentativeOffice
('PostgreSQL Sales', 'RepresentativeOffice', 'city_id', 'int', 'idb', 'RepresentativeOffice', 'city_id', 'int', NULL),
('PostgreSQL Sales', 'RepresentativeOffice', 'city_name', 'varchar', 'idb', 'RepresentativeOffice', 'city_name', 'varchar', NULL),
('PostgreSQL Sales', 'RepresentativeOffice', 'office_address', 'varchar', 'idb', 'RepresentativeOffice', 'office_address', 'varchar', NULL),
('PostgreSQL Sales', 'RepresentativeOffice', 'state', 'varchar', 'idb', 'RepresentativeOffice', 'state', 'varchar', NULL),
('PostgreSQL Sales', 'RepresentativeOffice', 'time', 'date', 'idb', 'RepresentativeOffice', 'Established_time', 'date', NULL),

-- Bảng Product
('PostgreSQL Sales', 'Product', 'product_id', 'int', 'idb', 'Product', 'product_id', 'int', NULL),
('PostgreSQL Sales', 'Product', 'description', 'varchar', 'idb', 'Product', 'description', 'varchar', NULL),
('PostgreSQL Sales', 'Product', 'size', 'varchar', 'idb', 'Product', 'size', 'varchar', 'UPPER(source_column)'),
('PostgreSQL Sales', 'Product', 'weight', 'decimal', 'idb', 'Product', 'weight', 'decimal', NULL),
('PostgreSQL Sales', 'Product', 'price', 'decimal', 'idb', 'Product', 'price', 'decimal', 'ROUND(source_column, 2)'),
('PostgreSQL Sales', 'Product', 'time', 'date', 'idb', 'Product', 'created_time', 'date', NULL),

-- Bảng Order
('PostgreSQL Sales', 'Order', 'order_id', 'int', 'idb', '"Order"', 'order_id', 'int', NULL),
('PostgreSQL Sales', 'Order', 'order_date', 'date', 'idb', '"Order"', 'order_date', 'date', NULL),
('PostgreSQL Sales', 'Order', 'customer_id', 'int', 'idb', '"Order"', 'customer_id', 'int', NULL),

-- Bảng StockedProduct
('PostgreSQL Sales', 'StockedProduct', 'store_id', 'int', 'idb', 'StockedProduct', 'store_id', 'int', NULL),
('PostgreSQL Sales', 'StockedProduct', 'product_id', 'int', 'idb', 'StockedProduct', 'product_id', 'int', NULL),
('PostgreSQL Sales', 'StockedProduct', 'stock_quantity', 'int', 'idb', 'StockedProduct', 'stock_quantity', 'int', NULL),
('PostgreSQL Sales', 'StockedProduct', 'time', 'date', 'idb', 'StockedProduct', 'Restock_time', 'date', NULL),

-- Bảng OrderProduct
('PostgreSQL Sales', 'OrderProduct', 'order_id', 'int', 'idb', 'OrderProduct', 'order_id', 'int', NULL),
('PostgreSQL Sales', 'OrderProduct', 'product_id', 'int', 'idb', 'OrderProduct', 'product_id', 'int', NULL),
('PostgreSQL Sales', 'OrderProduct', 'ordered_quantity', 'int', 'idb', 'OrderProduct', 'ordered_quantity', 'int', NULL),
('PostgreSQL Sales', 'OrderProduct', 'ordered_price', 'decimal', 'idb', 'OrderProduct', 'ordered_price', 'decimal', NULL),
('PostgreSQL Sales', 'OrderProduct', 'time', 'date', 'idb', 'OrderProduct', 'added_time', 'date', NULL);



-- ===================================================================================
-- 2. NẠP METADATA: GIAI ĐOẠN CHUYỂN TỪ IDB (Chuẩn hóa 3NF) VÀO DWH (Mô hình Star Schema)
-- ===================================================================================
SET search_path TO metadata_idb_to_dwh;

TRUNCATE TABLE Data_Dictionary_Mapping RESTART IDENTITY;

INSERT INTO Data_Dictionary_Mapping 
(source_schema, source_table, source_column, source_data_type, target_schema, target_table, target_column, target_data_type, transformation_rule)
VALUES 
-- Map cho Dim_Location (Gộp từ RepresentativeOffice của IDB)
('idb', 'RepresentativeOffice', 'city_id', 'int', 'dwh', 'Dim_Location', 'city_id', 'int', 'GENERATE_SURROGATE_KEY(city_key)'),
('idb', 'RepresentativeOffice', 'city_name', 'varchar', 'dwh', 'Dim_Location', 'city_name', 'varchar', NULL),
('idb', 'RepresentativeOffice', 'state', 'varchar', 'dwh', 'Dim_Location', 'state', 'varchar', NULL),
('idb', 'RepresentativeOffice', 'office_address', 'varchar', 'dwh', 'Dim_Location', 'office_address', 'varchar', NULL),


-- Map cho Dim_Customer (Chứa logic Lookup phức tạp phân loại Customer_Type theo yêu cầu đề bài)
('idb', 'Customer', 'customer_id', 'int', 'dwh', 'Dim_Customer', 'customer_id', 'int', 'GENERATE_SURROGATE_KEY(customer_key)'),
('idb', 'Customer', 'customer_name', 'varchar', 'dwh', 'Dim_Customer', 'customer_name', 'varchar', NULL),
('idb', 'Customer', 'first_order_date', 'date', 'dwh', 'Dim_Customer', 'first_order_date', 'date', NULL),
('idb', 'Customer', 'city_id', 'int', 'dwh', 'Dim_Customer', 'city_key', 'int', 'LOOKUP(Dim_Location, city_key)'),

-- Logic biến đổi cho Customer_Type (Gộp từ TouristCustomer và MailOrderCustomer đưa vào Dim_Customer)
('idb', 'TouristCustomer', 'tour_guide', 'varchar', 'dwh', 'Dim_Customer', 'tour_guide', 'varchar', 'LEFT_JOIN_WITH(Customer)'),
('idb', 'MailOrderCustomer', 'postal_address', 'varchar', 'dwh', 'Dim_Customer', 'postal_address', 'varchar', 'LEFT_JOIN_WITH(Customer)'),
('idb', 'TouristCustomer, MailOrderCustomer', 'N/A', 'N/A', 'dwh', 'Dim_Customer', 'customer_type', 'varchar', 'CASE WHEN Tourist THEN Tourist WHEN Mail THEN Mail ELSE Both END'),


-- Map cho Fact_Sales (Dữ liệu Đơn Đặt Hàng và Mặt Hàng)
('idb', 'Order', 'order_id', 'int', 'dwh', 'Fact_Sales', 'order_id', 'int', 'DEGENERATE_DIMENSION'),
('idb', 'Order', 'order_date', 'date', 'dwh', 'Fact_Sales', 'date_key', 'int', 'PARSE_TO_DATE_KEY(YYYYMMDD)'),
('idb', 'Order', 'customer_id', 'int', 'dwh', 'Fact_Sales', 'customer_key', 'int', 'LOOKUP(Dim_Customer, customer_key)'),
('idb', 'OrderProduct', 'product_id', 'int', 'dwh', 'Fact_Sales', 'product_key', 'int', 'LOOKUP(Dim_Product, product_key)'),
('idb', 'OrderProduct', 'ordered_quantity', 'int', 'dwh', 'Fact_Sales', 'ordered_quantity', 'int', NULL),
('idb', 'OrderProduct', 'ordered_price', 'decimal', 'dwh', 'Fact_Sales', 'ordered_price', 'decimal', NULL),
('idb', 'OrderProduct', 'N/A', 'N/A', 'dwh', 'Fact_Sales', 'total_amount', 'decimal', 'CALCULATED(ordered_quantity * ordered_price)');

-- Nạp Waterkmark khởi tạo mặc định cho tất cả các table:
INSERT INTO etl_watermark (table_name, last_load_time) VALUES
('Customer', '2000-01-01 00:00:00'),
('Store', '2000-01-01 00:00:00'),
('Product', '2000-01-01 00:00:00'),
('Order', '2000-01-01 00:00:00');
