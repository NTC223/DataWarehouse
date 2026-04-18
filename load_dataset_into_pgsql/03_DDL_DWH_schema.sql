CREATE SCHEMA IF NOT EXISTS dwh;

SET search_path TO dwh;

CREATE TABLE Dim_Time (
    time_key int PRIMARY KEY,
    month int,
    quarter int,
    year int
);

CREATE TABLE Dim_Location (
    location_key int PRIMARY KEY,
    city varchar(100),
    state varchar(100),
    office_address varchar(255)
);

CREATE TABLE Dim_Store (
    store_key int PRIMARY KEY,
    phone_number varchar(20),
    location_key int,
    FOREIGN KEY (location_key) REFERENCES Dim_Location(location_key)
);

CREATE TABLE Dim_Customer (
    customer_key int PRIMARY KEY,
    customer_name varchar(100),
    customer_type varchar(50),
    first_order_date date,
    location_key int,
    FOREIGN KEY (location_key) REFERENCES Dim_Location(location_key)
);

CREATE TABLE Dim_Product (
    product_key int PRIMARY KEY,
    description varchar(255),
    size varchar(50),
    weight decimal(10, 2)
);

CREATE TABLE Fact_Sales (
    time_key int,
    product_key int,
    customer_key int,
    quantity_ordered int,
    total_amount decimal(15, 2),
    PRIMARY KEY (time_key, product_key, customer_key),
    FOREIGN KEY (time_key) REFERENCES Dim_Time(time_key),
    FOREIGN KEY (product_key) REFERENCES Dim_Product(product_key),
    FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key)
);

CREATE TABLE Fact_Inventory (
    time_key int,
    store_key int,
    product_key int,    
    quantity_on_hand int,
    PRIMARY KEY (time_key, store_key, product_key),
    FOREIGN KEY (time_key) REFERENCES Dim_Time(time_key),
    FOREIGN KEY (store_key) REFERENCES Dim_Store(store_key),
    FOREIGN KEY (product_key) REFERENCES Dim_Product(product_key)
);