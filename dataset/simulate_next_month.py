import pandas as pd
import numpy as np
from faker import Faker
import random
import os
from datetime import timedelta

# ─────────────────────────────────────────────
# Khởi tạo
# ─────────────────────────────────────────────
fake = Faker('en_US')
Faker.seed(99)
random.seed(99)
np.random.seed(99)

DIR_SOURCE1 = "dataset/representative_office_data"
DIR_SOURCE2 = "dataset/sales_data"

print("=" * 60)
print("  SIMULATE NEXT MONTH DATA (MÔ PHỎNG THÁNG TIẾP THEO)")
print("=" * 60)

# Khai báo số lượng đơn hàng mới trong tháng tới
NUM_NEW_ORDERS = 10000

# 1. Đọc dữ liệu hiện tại
print("Đọc dữ liệu hiện tại...")
df_customer = pd.read_csv(f"{DIR_SOURCE1}/customer.csv")
df_product = pd.read_csv(f"{DIR_SOURCE2}/product.csv")
df_order = pd.read_csv(f"{DIR_SOURCE2}/order.csv")
df_op = pd.read_csv(f"{DIR_SOURCE2}/order_product.csv")
df_stock = pd.read_csv(f"{DIR_SOURCE2}/stocked_product.csv")

customer_ids = df_customer['customer_id'].tolist()
product_ids = df_product['product_id'].tolist()
price_map = dict(zip(df_product['product_id'], df_product['price']))

# Định nghĩa tháng tiếp theo
df_order['order_date'] = pd.to_datetime(df_order['order_date'])
max_date = df_order['order_date'].max()
start_date = max_date + timedelta(days=1)
end_date = start_date + timedelta(days=30)
max_order_id = df_order['order_id'].max()

print(f"Sinh thêm {NUM_NEW_ORDERS:,} đơn hàng từ {start_date.date()} đến {end_date.date()}...")

# 2. Tạo đơn hàng và chi tiết đơn hàng
pre_orders = []
for i in range(1, NUM_NEW_ORDERS + 1):
    cust_id = random.choice(customer_ids)
    order_date = fake.date_between(start_date=start_date.date(), end_date=end_date.date())
    pre_orders.append({
        "customer_id": cust_id,
        "order_date": order_date
    })

pre_orders.sort(key=lambda x: x["order_date"])

new_order_rows = []
new_op_rows = []

for i, o_info in enumerate(pre_orders):
    order_id = max_order_id + i + 1
    order_date = o_info["order_date"]
    
    new_order_rows.append({
        "order_id": order_id,
        "customer_id": o_info["customer_id"],
        "order_date": order_date
    })
    
    num_items = random.randint(1, 8)
    selected_products = sorted(random.sample(product_ids, num_items))
    
    for prod_id in selected_products:
        ordered_qty = random.randint(1, 5)
        new_op_rows.append({
            "order_id": order_id,
            "product_id": prod_id,
            "ordered_quantity": ordered_qty,
            "ordered_price": price_map[prod_id],
            "time": order_date            
        })

df_new_order = pd.DataFrame(new_order_rows)
df_new_op = pd.DataFrame(new_op_rows)

# 3. Cập nhật tồn kho (Stocked Product)
print("Cập nhật lại bảng stocked_product...")
# Tính tổng số lượng hàng đã bán của mỗi mặt hàng trong đợt này
sold_qty = df_new_op.groupby('product_id')['ordered_quantity'].sum().reset_index()
sold_dict = dict(zip(sold_qty['product_id'], sold_qty['ordered_quantity']))

# Nếu để list of dict thì thao tác dễ hơn
stock_list = df_stock.to_dict('records')

for prod_id, qty_to_deduct in sold_dict.items():
    # Tìm các cửa hàng đang tồn mặt hàng này
    stores_with_prod = [s for s in stock_list if s['product_id'] == prod_id]
    if stores_with_prod:
        # Phân bổ trừ dần vào tồn kho của các cửa hàng
        for s in stores_with_prod:
            if qty_to_deduct <= 0:
                break
            
            # Khách có thể mua nhiều hơn tồn kho nên giả lập có thể âm (hoặc giới hạn về 0 tùy logic kinh doanh)
            # Ở đây ta ưu tiên xài sạch số tồn hiện tại trước, nếu không đủ thì cho âm = nợ order.
            current_stock = s['stock_quantity']
            deduct = min(current_stock, qty_to_deduct) if current_stock > 0 else 0
            
            # Trừ số lượng tồn kho (nếu store hết tồn kho nhưng bị trừ tiếp thì cho thành số âm - hết hàng/đặt trước)
            if deduct == 0:  
                deduct = max(qty_to_deduct, 0)
                
            s['stock_quantity'] -= deduct
            qty_to_deduct -= deduct
            
            # Cập nhật time để ghi nhận thời điểm biến động mới nhất cho logic ETL
            s['time'] = str(end_date.date())

df_new_stock = pd.DataFrame(stock_list).sort_values(by='time')

# 4. Ghi file
print("Lưu và cập nhật dữ liệu CSV...")
df_new_order.to_csv(f"{DIR_SOURCE2}/order.csv", mode='a', header=False, index=False, lineterminator='\n')
df_new_op.to_csv(f"{DIR_SOURCE2}/order_product.csv", mode='a', header=False, index=False, lineterminator='\n')
# Stocked_product lưu dạng ghi đè (overwrite toàn bộ file vì coi như lấy full load mới)
df_new_stock.to_csv(f"{DIR_SOURCE2}/stocked_product.csv", index=False, lineterminator='\n')

print(f"    → Đã thêm {len(df_new_order):,} đơn hàng | {len(df_new_op):,} dòng order_product")
target_date_str = str(end_date.date())
updated_stock_records = sum(1 for s in stock_list if str(s['time']) == target_date_str)
print(f"    → Đã cập nhật biến động tồn kho: {len(sold_dict)} sản phẩm, {updated_stock_records:,} bản ghi (store × product)")

# 5. Insert directly to Database
print("\nĐẩy dữ liệu trực tiếp vào Database OLTP (sales_source)...")
try:
    import psycopg2
    from psycopg2.extras import execute_values, execute_batch
    from psycopg2.extensions import register_adapter, AsIs
    register_adapter(np.int64, AsIs)
    register_adapter(np.float64, AsIs)
    
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_USER = "admin"
    DB_PASS = "admin"
    DB_NAME = "postgres"

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )
    cursor = conn.cursor()
    cursor.execute("SET search_path TO sales_source;")

    # Insert Order
    order_tuples = [(r["order_id"], r["customer_id"], r["order_date"]) for r in new_order_rows]
    execute_values(
        cursor,
        'INSERT INTO "Order" (order_id, customer_id, order_date) VALUES %s',
        order_tuples
    )
    
    # Insert OrderProduct
    op_tuples = [(r["order_id"], r["product_id"], r["ordered_quantity"], r["ordered_price"], r["time"]) for r in new_op_rows]
    execute_values(
        cursor,
        'INSERT INTO OrderProduct (order_id, product_id, ordered_quantity, ordered_price, time) VALUES %s',
        op_tuples
    )
    
    # Update StockedProduct
    # Lấy ra những records đã cập nhật tồn kho (trường time mang nhãn của tháng mới)
    target_date_str = str(end_date.date())
    stock_update_tuples = [(s['stock_quantity'], s['time'], s['store_id'], s['product_id']) 
                           for s in stock_list if str(s['time']) == target_date_str]
            
    if stock_update_tuples:
        execute_batch(
            cursor,
            'UPDATE StockedProduct SET stock_quantity = %s, time = %s WHERE store_id = %s AND product_id = %s',
            stock_update_tuples
        )
    
    conn.commit()
    print(f"    → (DB) Đã insert thành công {len(order_tuples):,} Orders & {len(op_tuples):,} OrderProducts")
    print(f"    → (DB) Đã update {len(stock_update_tuples):,} bản ghi tồn kho (StockedProduct)")

except ImportError:
    print("    → (Warning) Thư viện psycopg2 chưa được cài đặt. Bỏ qua ghi vào Database.")
    print("    → Bạn có thể chạy: pip install psycopg2-binary")
except Exception as e:
    print(f"    → (Error) Quá trình đẩy vào DB thất bại: {e}")
finally:
    if 'conn' in locals() and conn:
        cursor.close()
        conn.close()

print("\n" + "=" * 60)
print("  HOÀN THÀNH - SẴN SÀNG CHẠY PIPELINE (ETL DWH)")
print("=" * 60)
