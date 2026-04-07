CREATE SCHEMA IF NOT EXISTS idb;

SET search_path TO idb;

CREATE TABLE RepresentativeOffice (
    city_id int PRIMARY KEY,
    city_name varchar(100),
    office_address varchar(255),
    state varchar(50),
    last_updated_time date
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
    last_updated_time date,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE MailOrderCustomer (
    customer_id int PRIMARY KEY,
    postal_address varchar(255),
    last_updated_time date,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE Store (
    store_id int PRIMARY KEY,
    phone_number varchar(20),
    last_updated_time date,
    city_id int,
    FOREIGN KEY (city_id) REFERENCES RepresentativeOffice(city_id)
);

CREATE TABLE Product (
    product_id int PRIMARY KEY,
    description varchar(255),
    size varchar(50),
    weight decimal(10, 2),
    price decimal(15, 2),
    last_updated_time date
);

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
    last_updated_time date,
    PRIMARY KEY (store_id, product_id),
    FOREIGN KEY (store_id) REFERENCES Store(store_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
);

CREATE TABLE OrderProduct (
    order_id int,
    product_id int,
    ordered_quantity int,
    ordered_price decimal(15, 2),
    last_updated_time date,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES "Order"(order_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
);