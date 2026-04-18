# DataWarehouse

## 1. Sinh dữ liệu

Sinh dữ liệu lần đầu

```bash
python ./dataset/generate_data.py
```

Sinh thêm dữ liệu cho tháng tiếp theo

```bash
python ./dataset/simulate_next_month.py
```

## 2. Khởi động hệ thống

> Nếu cài postgresql và mysql trên máy thì cần đổi port trong dockercompose để tránh conflict

```bash
docker compose build

docker compose up -d

docker compose up --build -d (để vừa build vừa up)

docker compose down (-v để xóa volume nếu cần reset)
```

## 3. Trigger ETL: http://localhost:8080

→ DAG: source_to_idb  (1)

→ DAG: idb_to_dwh     (2)

→ DAG: dwh_to_olap    (3)