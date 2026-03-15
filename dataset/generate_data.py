import pandas as pd
import numpy as np
from faker import Faker
import random
import os

fake = Faker('vi_VN') # Dùng Locale tiếng Việt cho tên và địa chỉ chân thật
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# ========== CONFIGURATION ==========
NUM_CITIES = 50
NUM_STORES = 200
NUM_PRODUCTS = 1000
NUM_CUSTOMERS = 50000 
NUM_ORDERS = 200000  # Generate 200k orders

OUTPUT_DIR = "dataset"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Starting Data Generation...")

# 1. RepresentativeOffice (Thành phố/Văn phòng)
print("Generating RepresentativeOffice...")
city_data = []
for i in range(1, NUM_CITIES + 1):
    city_data.append({
        "city_id": i,
        "city_name": fake.city(),
        "office_address": fake.address().replace('\n', ', '),
        "state": fake.city(), # Ở VN state/provice hay trùng city
        "time": fake.date_between(start_date='-10y', end_date='-5y')
    })
df_city = pd.DataFrame(city_data)
df_city.to_csv(f"{OUTPUT_DIR}/representative_office.csv", index=False)

# 2. Store (Cửa hàng)
print("Generating Store...")
store_data = []
for i in range(1, NUM_STORES + 1):
    store_data.append({
        "store_id": i,
        "phone_number": fake.phone_number()[:20],
        "time": fake.date_between(start_date='-5y', end_date='today'),
        "city_id": random.choice(df_city['city_id'])
    })
df_store = pd.DataFrame(store_data)
df_store.to_csv(f"{OUTPUT_DIR}/store.csv", index=False)

# 3. Product (Sản phẩm)
print("Generating Product...")
product_data = []
for i in range(1, NUM_PRODUCTS + 1):
    product_data.append({
        "product_id": i,
        "description": fake.catch_phrase(),
        "size": random.choice(['S', 'M', 'L', 'XL', 'XXL', 'Freesize']),
        "weight": round(random.uniform(0.1, 50.0), 2),
        "price": round(random.uniform(10.0, 5000.0), 2),
        "time": fake.date_between(start_date='-5y', end_date='today')
    })
df_product = pd.DataFrame(product_data)
df_product.to_csv(f"{OUTPUT_DIR}/product.csv", index=False)

# 4. Customer & SubTypes
print("Generating Customers...")
customer_data = []
tourist_data = []
mail_data = []

for i in range(1, NUM_CUSTOMERS + 1):
    # Base Customer
    cust_type = random.choices(['Tourist', 'Mail', 'Both'], weights=[40, 40, 20])[0]
    first_order = fake.date_between(start_date='-4y', end_date='today')
    
    customer_data.append({
        "customer_id": i,
        "customer_name": fake.name(),
        "city_id": random.choice(df_city['city_id']),
        "first_order_date": first_order
    })
    
    if cust_type in ['Tourist', 'Both']:
        tourist_data.append({
            "customer_id": i,
            "tour_guide": fake.name(),
            "time": fake.date_between(start_date=first_order, end_date='today')
        })
        
    if cust_type in ['Mail', 'Both']:
        mail_data.append({
            "customer_id": i,
            "postal_address": fake.address().replace('\n', ', '),
            "time": fake.date_between(start_date=first_order, end_date='today')
        })

df_customer = pd.DataFrame(customer_data)
df_tourist = pd.DataFrame(tourist_data)
df_mail = pd.DataFrame(mail_data)

df_customer.to_csv(f"{OUTPUT_DIR}/customer.csv", index=False)
df_tourist.to_csv(f"{OUTPUT_DIR}/tourist_customer.csv", index=False)
df_mail.to_csv(f"{OUTPUT_DIR}/mail_order_customer.csv", index=False)

# 5. Order & OrderProduct
print("Generating Orders (This will take a few seconds)...")
order_data = []
order_product_data = []

# Chọn random cửa hàng dựa theo thành phố của khách hàng 
# (Như yêu cầu: Ưu tiên lấy kho từ thành phố của khách, nếu không thì lấy kho khác)
cust_city_map = dict(zip(df_customer.customer_id, df_customer.city_id))
city_store_map = df_store.groupby('city_id')['store_id'].apply(list).to_dict()
all_stores = df_store['store_id'].tolist()

product_price_map = dict(zip(df_product.product_id, df_product.price))

for i in range(1, NUM_ORDERS + 1):
    cust_id = random.randint(1, NUM_CUSTOMERS)
    order_date = fake.date_between(start_date='-2y', end_date='today')
    
    order_data.append({
        "order_id": i,
        "order_date": order_date,
        "customer_id": cust_id
    })
    
    # Generate 1 to 5 products per order
    num_items = random.randint(1, 5)
    selected_products = random.sample(range(1, NUM_PRODUCTS + 1), num_items)
    
    for prod_id in selected_products:
        order_product_data.append({
            "order_id": i,
            "product_id": prod_id,
            "ordered_quantity": random.randint(1, 10),
            "ordered_price": product_price_map[prod_id], # Giá lúc bán = giá sản phẩm
            "time": order_date
        })

df_order = pd.DataFrame(order_data)
df_op = pd.DataFrame(order_product_data)

df_order.to_csv(f"{OUTPUT_DIR}/order.csv", index=False)
df_op.to_csv(f"{OUTPUT_DIR}/order_product.csv", index=False)

# 6. StockedProduct (Tồn kho)
print("Generating Stock Inventory...")
stock_data = []
# Giả sử mỗi cửa hàng bán khoảng 100 sản phẩm ngẫu nhiên
for store in range(1, NUM_STORES + 1):
    stocked_products = random.sample(range(1, NUM_PRODUCTS + 1), 100)
    for prod in stocked_products:
        stock_data.append({
            "store_id": store,
            "product_id": prod,
            "stock_quantity": random.randint(0, 500),
            "time": fake.date_between(start_date='-1y', end_date='today')
        })
        
df_stock = pd.DataFrame(stock_data)
df_stock.to_csv(f"{OUTPUT_DIR}/stocked_product.csv", index=False)

print(f"Data Generation Completed successfully in '{OUTPUT_DIR}' directory!")
print(f"Total rows generated: {len(df_city) + len(df_store) + len(df_product) + len(df_customer) + len(df_tourist) + len(df_mail) + len(df_order) + len(df_op) + len(df_stock):,}")
