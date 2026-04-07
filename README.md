# DataWarehouse

## 1. Sinh dữ liệu

python dataset/generate_data.py

## 2. Khởi động hệ thống

> Nếu cài postgresql và mysql trên máy thì cần đổi port trong dockercompose để tránh conflict

docker compose build

docker compose up -d

docker compose up --build -d (để vừa build vừa up)

docker compose down (-v để xóa volume nếu cần reset)

## 3. Trigger ETL: http://localhost:8080

→ DAG: source_to_idb  (chạy trước)

→ DAG: idb_to_dwh     (chạy sau)