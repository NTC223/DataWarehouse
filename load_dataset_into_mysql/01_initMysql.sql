-- Cơ sở dữ liệu Khách hàng (Văn phòng đại diện)

CREATE DATABASE IF NOT EXISTS RepresentativeOffice;

USE RepresentativeOffice;

CREATE TABLE Customer (
    customer_id int PRIMARY KEY,
    customer_name varchar(100),
    city_id int,
    first_order_date date
);

CREATE TABLE TouristCustomer (
    customer_id int PRIMARY KEY,
    tour_guide varchar(255),
    time date,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE MailOrderCustomer (
    customer_id int PRIMARY KEY,
    postal_address varchar(255),
    time date,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);