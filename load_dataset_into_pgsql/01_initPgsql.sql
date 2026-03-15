-- 1. KHỞI TẠO CÁC SCHEMA
CREATE SCHEMA IF NOT EXISTS sales_source;        -- Nguồn 2
CREATE SCHEMA IF NOT EXISTS idb;                 -- Integrated Database
CREATE SCHEMA IF NOT EXISTS dwh;                 -- Data Warehouse
CREATE SCHEMA IF NOT EXISTS metadata_source_to_idb;  -- Metadata ánh xạ Nguồn -> IDB
CREATE SCHEMA IF NOT EXISTS metadata_idb_to_dwh;     -- Metadata ánh xạ IDB -> DWH

-- 2. KHỞI TẠO BẢNG DỮ LIỆU NGUỒN (SCHEMA sales_source)
SET search_path TO sales_source;

CREATE TABLE RepresentativeOffice (
    city_id int PRIMARY KEY,
    city_name varchar(100),
    office_address varchar(255),
    state varchar(100),
    time date
);

CREATE TABLE Store (
    store_id int PRIMARY KEY,
    phone_number varchar(20),
    time date,
    city_id int,
    FOREIGN KEY (city_id) REFERENCES RepresentativeOffice(city_id)
);

CREATE TABLE Product (
    product_id int PRIMARY KEY,
    description varchar(255),
    size varchar(50),
    weight decimal(10, 2),
    price decimal(10, 2),
    time date
);

-- Vì Order là keyword trong PostgreSQL nên cần ""
CREATE TABLE "Order" (
    order_id int PRIMARY KEY,
    order_date date,
    customer_id int
);

CREATE TABLE StockedProduct (
    store_id int,
    product_id int,
    stock_quantity int,
    time date,
    PRIMARY KEY (store_id, product_id),
    FOREIGN KEY (store_id) REFERENCES Store(store_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
);

CREATE TABLE OrderProduct (
    order_id int,
    product_id int,
    ordered_quantity int,
    ordered_price decimal(10, 2),
    time date,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES "Order"(order_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
);

-- 3. KHỞI TẠO BẢNG DỮ LIỆU IDB (SCHEMA idb)
SET search_path TO idb;

CREATE TABLE RepresentativeOffice (
    city_id int PRIMARY KEY,
    city_name varchar(100),
    office_address varchar(255),
    state varchar(100),
    Established_time date
);

CREATE TABLE Customer (
    customer_id int PRIMARY KEY,
    customer_name varchar(100),
    city_id int,
    first_order_date date,
    FOREIGN KEY (city_id) REFERENCES RepresentativeOffice(city_id)
);

CREATE TABLE TouristCustomer (
    customer_id int PRIMARY KEY,
    tour_guide varchar(255),
    tour_time date,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE MailOrderCustomer (
    customer_id int PRIMARY KEY,
    postal_address varchar(255),
    join_time date,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE Store (
    store_id int PRIMARY KEY,
    phone_number varchar(20),
    Opening_time date,
    city_id int,
    FOREIGN KEY (city_id) REFERENCES RepresentativeOffice(city_id)
);

CREATE TABLE Product (
    product_id int PRIMARY KEY,
    description varchar(255),
    size varchar(50),
    weight decimal(10, 2),
    price decimal(10, 2),
    created_time date
);

-- Vì Order là keyword trong PostgreSQL nên cần ""
CREATE TABLE "Order" (
    order_id int PRIMARY KEY,
    order_date date,
    customer_id int,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE StockedProduct (
    store_id int,
    product_id int,
    stock_quantity int,
    Restock_time date,
    PRIMARY KEY (store_id, product_id),
    FOREIGN KEY (store_id) REFERENCES Store(store_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
);

CREATE TABLE OrderProduct (
    order_id int,
    product_id int,
    ordered_quantity int,
    ordered_price decimal(10, 2),
    added_time date,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES "Order"(order_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
);
-- 4. KHỞI TẠO BẢNG DỮ LIỆU DWH (SCHMA dwh)

SET search_path TO dwh;

CREATE TABLE Dim_Date (
    date_key int PRIMARY KEY, -- Định dạng YYYYMMDD
    full_date date,
    day int,
    month int,
    quarter int,
    year int
);

CREATE TABLE Dim_Location (
    city_key serial PRIMARY KEY, -- Khóa thay thế (Surrogate Key) tự tăng
    city_id int,                 -- Khóa tự nhiên (Natural Key) từ IDB
    city_name varchar(100),
    state varchar(100),
    office_address varchar(255)
);

CREATE TABLE Dim_Store (
    store_key serial PRIMARY KEY,
    store_id int,
    phone_number varchar(20),
    city_key int,
    FOREIGN KEY (city_key) REFERENCES Dim_Location(city_key)
);

CREATE TABLE Dim_Customer (
    customer_key serial PRIMARY KEY,
    customer_id int,
    customer_name varchar(100),
    customer_type varchar(20), -- 'Tourist', 'Mail-order' hoặc 'Both'
    tour_guide varchar(255),
    postal_address varchar(255),
    first_order_date date,
    city_key int,
    FOREIGN KEY (city_key) REFERENCES Dim_Location(city_key)
);

CREATE TABLE Dim_Product (
    product_key serial PRIMARY KEY,
    product_id int,
    description varchar(255),
    size varchar(50),
    weight decimal(10, 2),
    price decimal(10, 2)
);

CREATE TABLE Fact_Sales (
    date_key int,
    product_key int,
    customer_key int,
    store_key int,
    order_id int, -- Degenerate Dimension
    ordered_quantity int,
    ordered_price decimal(10, 2),
    total_amount decimal(15, 2),
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (product_key) REFERENCES Dim_Product(product_key),
    FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key),
    FOREIGN KEY (store_key) REFERENCES Dim_Store(store_key)
);

CREATE TABLE Fact_Inventory (
    date_key int,
    store_key int,
    product_key int,
    stock_quantity int,
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (store_key) REFERENCES Dim_Store(store_key),
    FOREIGN KEY (product_key) REFERENCES Dim_Product(product_key)
);


-- 5. KHỞI TẠO BẢNG METADATA ÁNH XẠ NGUỒN -> IDB (SCHEMA metadata_source_to_idb)
SET search_path TO metadata_source_to_idb;

-- Bảng 5.1: Lưu trữ Cấu hình Tracking Ánh xạ Cột (Column Mapping)
CREATE TABLE Data_Dictionary_Mapping (
    mapping_id serial PRIMARY KEY,
    source_system varchar(50),      -- Vd: 'MySQL RepresentativeOffice', 'PostgreSQL Sales'
    source_table varchar(100),      
    source_column varchar(100),     
    source_data_type varchar(50),   -- Vd: 'integer(10)', 'varchar(255)'
    
    target_schema varchar(50) DEFAULT 'idb',
    target_table varchar(100),      
    target_column varchar(100),
    target_data_type varchar(50),   -- Vd: 'int', 'varchar(255)'
    
    transformation_rule text,       -- Các quy tắc biến đổi nếu có (Ví dụ: CAST, UPPER, COALESCE)
    is_active boolean DEFAULT TRUE  -- Cờ đánh dấu rule ánh xạ còn hiệu lực không
);

-- Bảng 5.2: Watermark (Đánh dấu load dữ liệu gia tăng)
CREATE TABLE etl_watermark (
    table_name varchar(50) PRIMARY KEY,
    last_load_time timestamp
);

-- Bảng 5.3: Nhật ký ETL
CREATE TABLE etl_log (
    log_id serial PRIMARY KEY,
    job_name varchar(100),
    status varchar(20), -- 'SUCCESS', 'FAILED', 'RUNNING'
    rows_inserted int,
    rows_updated int,
    error_message text,
    run_time timestamp DEFAULT CURRENT_TIMESTAMP
);


-- 6. KHỞI TẠO BẢNG METADATA ÁNH XẠ IDB -> DWH (SCHEMA metadata_idb_to_dwh)
SET search_path TO metadata_idb_to_dwh;

-- Bảng 6.1: Lưu trữ Cấu hình Tracking Ánh xạ Cột (từ IDB lên thiết kế Sao / Dimensions, Facts)
CREATE TABLE Data_Dictionary_Mapping (
    mapping_id serial PRIMARY KEY,
    source_schema varchar(50) DEFAULT 'idb',
    source_table varchar(100),      
    source_column varchar(100),     
    source_data_type varchar(50),
    
    target_schema varchar(50) DEFAULT 'dwh',
    target_table varchar(100),      -- Vd: 'Dim_Customer', 'Fact_Sales'
    target_column varchar(100),
    target_data_type varchar(50),
    
    transformation_rule text,       -- Phục vụ ghi chú các logic ví dụ phân loại Customer_Type, Generate Surrogate Keys
    is_active boolean DEFAULT TRUE
);

CREATE TABLE etl_watermark (
    table_name varchar(50) PRIMARY KEY,
    last_load_time timestamp
);

CREATE TABLE etl_log (
    log_id serial PRIMARY KEY,
    job_name varchar(100),
    status varchar(20),
    rows_inserted int,
    rows_updated int,
    error_message text,
    run_time timestamp DEFAULT CURRENT_TIMESTAMP
);