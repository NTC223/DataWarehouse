import pandas as pd
import numpy as np
from faker import Faker
import random
import os

# ─────────────────────────────────────────────
# Khởi tạo
# ─────────────────────────────────────────────

fake = Faker('vi_VN')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# ─────────────────────────────────────────────
# Tham số cấu hình
# ─────────────────────────────────────────────

NUM_CITIES          = 50
NUM_STORES          = 200
NUM_PRODUCTIONS     = 1000
NUM_CUSTOMERS       = 50000
NUM_ORDERS          = 200000

DIR_SOURCE1 = "dataset/representative_office_data"
DIR_SOURCE2 = "dataset/sales_data"
os.makedirs(DIR_SOURCE1, exist_ok=True)
os.makedirs(DIR_SOURCE2, exist_ok=True)

print("=" * 60)
print("  BẮT ĐẦU SINH DỮ LIỆU")
print("=" * 60)

# ─────────────────────────────────────────────
# Source 2 - sales data
# ─────────────────────────────────────────────

print ("\n [SOURCE 2] Sinh RepresentativeOffice data ...")

city_rows = []
for i in range (1, NUM_CITIES + 1):
    city_rows.append({
        "city_id"       : i,
        "city_name"     : fake.city(),
        "office_address": fake.address().replace('\n', ', '),
        "state"         : fake.city(),
        "time"          : fake.date_between(start_date='-10y', end_date='-5y')
    })

df_city = pd.DataFrame(city_rows)
df_city.to_csv(f"{DIR_SOURCE2}/representative_office.csv", index=False, lineterminator='\n')
print(f"    → {len(df_city):,} văn phòng đại diện")

# ─────────────────────────────────────────────
# SOURCE 2: Store
# ─────────────────────────────────────────────

print("[Source 2] Sinh Store data...")

store_rows = []
for i in range(1, NUM_STORES + 1):
    store_rows.append({
        "store_id"      : i,
        "phone_number"  : fake.phone_number()[:20],
        "time"          : fake.date_between(start_date='-5y', end_date='today'),
        "city_id"       : random.choice(df_city['city_id'].tolist())
    })

df_store = pd.DataFrame(store_rows)
df_store.to_csv(f"{DIR_SOURCE2}/store.csv", index=False, lineterminator='\n')
print(f"    → {len(df_store):,} cửa hàng")

# ─────────────────────────────────────────────
# SOURCE 2: Product
# ─────────────────────────────────────────────

print ("[SOURCE 2] Sinh Product data...")

product_rows = []
for i in range(1, NUM_PRODUCTIONS + 1):
    product_rows.append({
        "product_id"    : i,
        "description"   : fake.catch_phrase(),
        "size"          : random.choice(['S', 'M', 'L', 'XL', 'XXL', 'Freesize']),
        "weight"        : round(random.uniform(0.1, 50.0), 2),
        "price"         : round(random.uniform(10.0, 5000.0), 2),
        "time"          : fake.date_between(start_date='-5y', end_date='today')
    })

df_product = pd.DataFrame(product_rows)
df_product.to_csv(f"{DIR_SOURCE2}/product.csv", index=False, lineterminator='\n')
print(f"    → {len(df_product):,} sản phẩm")

# ─────────────────────────────────────────────
# SOURCE 1: Customer + Subtypes
# ─────────────────────────────────────────────

print("[SOURCE 1] Sinh Customer, TourisrCustomer, MailOrderCustomer data...")

customer_rows = []
tourist_rows = []
mail_rows = []

city_ids = df_city['city_id'].tolist() # Dùng city_id từ Source 2

for i in range(1, NUM_CUSTOMERS + 1):
    cust_type   = random.choices(['Tourist', 'Mail', 'Both'], weights=[40, 40, 20])[0]
    first_order = fake.date_between(start_date='-5y', end_date='today')

    # ── Customer
    customer_rows.append({
        "customer_id"       : i,
        "customer_name"     : fake.name(),
        "city_id"           : random.choice(city_ids),
        "first_order_date"  : first_order
    })

    # ── TouristCustomer
    if cust_type in ['Tourist', 'Both']:
        tourist_rows.append({
            "customer_id"   : i,
            "tour_guide"    : fake.name(),
            "time"          : fake.date_between(start_date=first_order, end_date='today')
        })

    # ── MailOrderCustomer
    if cust_type in ['Mail', 'Both']:
        mail_rows.append({
            "customer_id"   : i,
            "postal_address": fake.address().replace('\n', ', '),
            "time"          : fake.date_between(start_date=first_order, end_date='today')
        })

df_customer = pd.DataFrame(customer_rows)
df_tourist  = pd.DataFrame(tourist_rows)
df_mail     = pd.DataFrame(mail_rows)


df_customer.to_csv(f"{DIR_SOURCE1}/customer.csv", index=False, lineterminator='\n')
df_tourist.to_csv(f"{DIR_SOURCE1}/tourist_customer.csv", index=False, lineterminator='\n')
df_mail.to_csv(f"{DIR_SOURCE1}/mail_order_customer.csv", index=False, lineterminator='\n')

tourist_count = len(df_tourist)
mail_count = len(df_mail)
both_count = NUM_CUSTOMERS - tourist_count - mail_count + len(
    set(df_tourist['customer_id']) & set(df_mail['customer_id'])
)
print(f"    → {len(df_customer):,} khách hàng")
print(f"       Tourist: {tourist_count:,} | MailOrder: {mail_count:,} | Both: {both_count:,}")

# ─────────────────────────────────────────────
# SOURCE 2: Order + OrderProduct
# ─────────────────────────────────────────────

print("[SOURCE 2] Sinh Order + OrderProduct data...")

order_rows = []
op_rows    = []

customer_ids    = df_customer['customer_id'].tolist()
product_ids     = df_product['product_id'].tolist()
price_map       = dict(zip(df_product['product_id'], df_product['price']))
first_order_map = dict(zip(df_customer['customer_id'], df_customer['first_order_date']))

for i in range(1, NUM_ORDERS + 1):
    cust_id = random.choice(customer_ids)

    first_order_date = first_order_map[cust_id]
    order_date = fake.date_between(start_date=first_order_date, end_date='today')

    # ── Order
    order_rows.append({
        "order_id"      : i,
        "order_date"    : order_date,
        "customer_id"   : cust_id
    })

    # ── OrderProduct
    num_items       = random.randint(1, 10)
    selected_products  = random.sample(product_ids, num_items)

    for prod_id in selected_products:
        op_rows.append({
            "order_id"          : i,
            "product_id"        : prod_id,
            "ordered_quantity"  : random.randint(1, 10),
            "ordered_price"       : price_map[prod_id],
            "time"              : order_date            
        })

df_order = pd.DataFrame(order_rows)
df_op    = pd.DataFrame(op_rows)
 
df_order.to_csv(f"{DIR_SOURCE2}/order.csv",         index=False, lineterminator='\n')
df_op.to_csv(   f"{DIR_SOURCE2}/order_product.csv", index=False, lineterminator='\n')
print(f"    → {len(df_order):,} đơn hàng | {len(df_op):,} dòng order_product")


# ─────────────────────────────────────────────
# SOURCE 2: StockedProduct
# ─────────────────────────────────────────────

print("[SOURCE 2] Sinh StockedProduct data...")

stock_rows = []
for store_id in range(1, NUM_STORES + 1):
    stocked = random.sample(product_ids, 100)
    for prod_id in stocked:
        stock_rows.append({
            "store_id"      : store_id,
            "product_id"    : prod_id,
            "stock_quantity": random.randint(0, 500),
            "time"          :fake.date_between(start_date='-1y', end_date='today')
        })

df_stock = pd.DataFrame(stock_rows)
df_stock.to_csv(f"{DIR_SOURCE2}/stocked_product.csv", index=False, lineterminator='\n')
print(f"    → {len(df_stock):,} bản ghi tồn kho")

print("\n" + "=" * 60)
print("  HOÀN THÀNH")
print("=" * 60)