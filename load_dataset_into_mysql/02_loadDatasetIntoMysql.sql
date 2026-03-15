USE RepresentativeOffice;

-- Để nạp dữ liệu tốc độ cao vào MySQL từ file CSV
-- Do mysql đang chạy trong Docker nên đường dẫn trỏ tới là trong Volume nội bộ /tmp/dataset
LOAD DATA INFILE '/tmp/dataset/customer.csv' 
INTO TABLE Customer
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, customer_name, city_id, first_order_date);


LOAD DATA INFILE '/tmp/dataset/tourist_customer.csv' 
INTO TABLE TouristCustomer
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, tour_guide, time);


LOAD DATA INFILE '/tmp/dataset/mail_order_customer.csv' 
INTO TABLE MailOrderCustomer
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, postal_address, time);
