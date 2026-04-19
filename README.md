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

## 4. Giao diện Web phân tích OLAP Explorer

Ứng dụng Web App hạng doanh nghiệp (chạy tại cổng 3000) dành riêng cho Business Users. Hệ thống cho phép người dùng cuối giao tiếp trực tiếp với Data Warehouse thông qua các khối pre-calculated cuboids mà không cần viết SQL.

### 🔥 Tính Năng Phân Tích
- **Roll-Up & Drill-Down**: Điều hướng tự do lên/xuống theo các chiều phân cấp (Ví dụ: `Year` -> `Quarter` -> `Month`, hoặc Quản lý vùng `State` -> `City` -> `Store`).
- **Drill-Across**: Kết nối các độ đo (Measures) giữa đa luồng dữ liệu thông qua Full Outer Join. Giúp người dùng phân tích tương quan song song Doanh số (Sales) và Tồn kho (Inventory) trên cùng một trục kích thước.
- **Drill-Through**: Tính năng "Đào mỏ" gốc - cho phép click vào bất kỳ con số tổng hợp nào để gọi truy vấn xuyên thấu về `dwh`, trả ra tận gốc các bản ghi chi tiết (Raw fact records).
- **Pivot (Cross-Tabulation)**: Xoay và thiết lập tự do ma trận dữ liệu phân tích, giúp hiển thị góc nhìn trực quan như một bảng Pivot của Excel.
- **Tối ưu Hóa Hiệu Suất Cao**: Hệ thống phân trang (Pagination) kết hợp tính toán `COUNT()` bất đồng bộ cùng Limit tối ưu, vận hành mượt mà dù phải xử lý hơn triệu dòng dữ liệu.

### 🏗️ Cấu Trúc Kỹ Thuật (OLAP Architecture)
- **Backend (FastAPI)**: Trái tim định tuyến bằng Python. Cung cấp API `query_cube`, tự phân tích filter và dimensions để định tuyến siêu tốc về đúng bảng Cuboid trong schema `olap`. Sử dụng ThreadedConnectionPool cho hiệu suất cao.
- **Frontend (React)**: Quản lý History Stack toàn diện bằng custom Redux/Reducer. Chịu trách nhiệm render dữ liệu ra Pivot Table cực kỳ linh hoạt và thẩm mỹ (Light Mode presentation-ready).

### 🚀 Cách Triển Khai OLAP Web App
Lưu ý: Phải đảm bảo đã `Sinh dữ liệu` (bước 1) và chạy xong các `ETL DAG` (bước 3) để DB PostgreSQL có đủ schema `dwh`, `olap` và các khối cuboid.

```bash
cd olap-ui
docker compose up --build -d
```

### 💻 Trải Nghiệm Hệ Thống
Mở trình duyệt và truy cập hệ thống theo địa chỉ:
- **Giao diện người dùng Web**: `http://localhost:3000`
- **Tài liệu Backend API (Swagger API)**: `http://localhost:8000/docs`