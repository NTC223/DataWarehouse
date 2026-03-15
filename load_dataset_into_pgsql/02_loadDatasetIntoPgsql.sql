-- Nạp dữ liệu vào Schema Nguồn (Sales Source) 
SET search_path TO sales_source;

-- Sử dụng lệnh COPY trong Postgres để chép dữ liệu siêu tốc từ CSV vào DB 
-- Do Postgres chạy trong container, volume đã map thư mục dataset ra `/tmp/dataset`
COPY RepresentativeOffice(city_id, city_name, office_address, state, time)
FROM '/tmp/dataset/representative_office.csv'
DELIMITER ','
CSV HEADER;

COPY Store(store_id, phone_number, time, city_id)
FROM '/tmp/dataset/store.csv'
DELIMITER ','
CSV HEADER;

COPY Product(product_id, description, size, weight, price, time)
FROM '/tmp/dataset/product.csv'
DELIMITER ','
CSV HEADER;

COPY "Order"(order_id, order_date, customer_id)
FROM '/tmp/dataset/order.csv'
DELIMITER ','
CSV HEADER;

COPY OrderProduct(order_id, product_id, ordered_quantity, ordered_price, time)
FROM '/tmp/dataset/order_product.csv'
DELIMITER ','
CSV HEADER;

COPY StockedProduct(store_id, product_id, stock_quantity, time)
FROM '/tmp/dataset/stocked_product.csv'
DELIMITER ','
CSV HEADER;
