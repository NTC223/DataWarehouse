CREATE SCHEMA IF NOT EXISTS sales_source;

SET search_path TO sales_source;

CREATE TABLE RepresentativeOffice (
    city_id int PRIMARY KEY,
    city_name varchar(100),
    office_address varchar(255),
    state varchar(50),
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
    ordered_price decimal(15, 2),
    time date,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES "Order"(order_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
);