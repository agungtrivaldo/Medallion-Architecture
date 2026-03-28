import psycopg2
from psycopg2 import extras
import random
from faker import Faker
from datetime import datetime, timedelta

# Menggunakan locale Indonesia
fake = Faker('id_ID')

# ==========================================
# KONFIGURASI DATABASE (Sesuai Docker Compose)
# ==========================================
DB_CONFIG = {
    'dbname': 'source_db',
    'user': 'admin',
    'password': 'password123',
    'host': 'localhost', # Berjalan di VPS yang sama dengan Docker
    'port': '5432'
}

# ==========================================
# PENGATURAN SKALA DATA (Bisa kamu ubah nanti)
# ==========================================
NUM_USERS = 10_000
NUM_ORDERS = 50_000  # Menghasilkan puluhan ribu transaksi (Aman untuk RAM 8GB)
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 3, 27)

def create_tables(cursor):
    print("Membangun 9 Tabel Skema E-Commerce...")
    # CASCADE digunakan agar jika kita re-run script ini, tabel lama terhapus bersih
    cursor.execute("""
        DROP TABLE IF EXISTS shipping, payments, order_items, orders, products, categories, brands, user_addresses, users CASCADE;

        -- 1. DATA MASTER
        CREATE TABLE users (
            user_id SERIAL PRIMARY KEY, 
            email VARCHAR(255), 
            password_hash VARCHAR(255), 
            phone_number VARCHAR(50), 
            created_at TIMESTAMP, 
            updated_at TIMESTAMP
        );
        CREATE TABLE user_addresses (
            address_id SERIAL PRIMARY KEY, 
            user_id INT REFERENCES users(user_id), 
            province VARCHAR(100), 
            city VARCHAR(100), 
            postal_code VARCHAR(20), 
            full_address TEXT
        );
        CREATE TABLE brands (
            brand_id SERIAL PRIMARY KEY, 
            brand_name VARCHAR(100), 
            country_of_origin VARCHAR(100)
        );
        CREATE TABLE categories (
            category_id SERIAL PRIMARY KEY, 
            category_name VARCHAR(100)
        );
        CREATE TABLE products (
            product_id SERIAL PRIMARY KEY, 
            category_id INT REFERENCES categories(category_id), 
            brand_id INT REFERENCES brands(brand_id), 
            product_name VARCHAR(255), 
            base_price DECIMAL, 
            weight_grams INT, 
            updated_at TIMESTAMP
        );

        -- 2. DATA TRANSAKSI
        CREATE TABLE orders (
            order_id VARCHAR(50) PRIMARY KEY, 
            user_id INT REFERENCES users(user_id), 
            address_id INT REFERENCES user_addresses(address_id), 
            order_date TIMESTAMP, 
            total_amount DECIMAL, 
            order_status VARCHAR(50)
        );
        CREATE TABLE order_items (
            item_id SERIAL PRIMARY KEY, 
            order_id VARCHAR(50) REFERENCES orders(order_id), 
            product_id INT REFERENCES products(product_id), 
            quantity INT, 
            unit_price_at_purchase DECIMAL
        );

        -- 3. DATA OPERASIONAL
        CREATE TABLE payments (
            payment_id SERIAL PRIMARY KEY, 
            order_id VARCHAR(50) REFERENCES orders(order_id), 
            payment_method VARCHAR(50), 
            payment_status VARCHAR(50), 
            payment_date TIMESTAMP
        );
        CREATE TABLE shipping (
            shipping_id SERIAL PRIMARY KEY, 
            order_id VARCHAR(50) REFERENCES orders(order_id), 
            courier_name VARCHAR(50), 
            tracking_number VARCHAR(100), 
            shipping_cost DECIMAL, 
            shipping_status VARCHAR(50)
        );
    """)

def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def generate_master_data(cursor):
    print("Insert Data Master (Brands, Categories, Products)...")
    
    brands = [
        ('Asus', 'Taiwan'), ('MSI', 'Taiwan'), ('Gigabyte', 'Taiwan'), 
        ('Samsung', 'South Korea'), ('Apple', 'USA'), ('Poco', 'China'), 
        ('Infinix', 'China'), ('Logitech', 'Switzerland')
    ]
    extras.execute_values(cursor, "INSERT INTO brands (brand_name, country_of_origin) VALUES %s", brands)

    categories = [
        ('Motherboard',), ('VGA / Graphic Card',), ('Smartphone',), 
        ('Gaming Monitor',), ('Peripherals',)
    ]
    extras.execute_values(cursor, "INSERT INTO categories (category_name) VALUES %s", categories)

    products = [
        (2, 1, 'Asus ROG RTX 4060 Ti', 8500000, 1500, datetime.now()),
        (2, 2, 'MSI Ventus RTX 3060', 5200000, 1200, datetime.now()),
        (1, 1, 'Asus TUF Gaming B550M', 2500000, 1000, datetime.now()),
        (3, 6, 'Poco X6 Pro 5G', 4999000, 200, datetime.now()),
        (3, 7, 'Infinix Note 40 Pro', 3500000, 210, datetime.now()),
        (3, 4, 'Samsung Galaxy S24 Ultra', 21999000, 230, datetime.now()),
        (4, 3, 'Gigabyte G24F 2 165Hz', 2800000, 5000, datetime.now()),
        (5, 8, 'Logitech G Pro X Superlight', 1800000, 150, datetime.now())
    ]
    extras.execute_values(cursor, "INSERT INTO products (category_id, brand_id, product_name, base_price, weight_grams, updated_at) VALUES %s", products)

def generate_users(cursor):
    print(f"Insert {NUM_USERS} Users & Addresses...")
    users = []
    addresses = []
    
    for user_id in range(1, NUM_USERS + 1):
        dt = random_date(START_DATE, END_DATE)
        users.append((fake.email(), fake.password(), fake.phone_number(), dt, dt))
        
        # Tiap user punya 1-2 alamat
        for _ in range(random.randint(1, 2)):
            addresses.append((user_id, fake.state(), fake.city(), fake.postcode(), fake.street_address()))

    # Bulk insert
    extras.execute_values(cursor, "INSERT INTO users (email, password_hash, phone_number, created_at, updated_at) VALUES %s", users, page_size=5000)
    extras.execute_values(cursor, "INSERT INTO user_addresses (user_id, province, city, postal_code, full_address) VALUES %s", addresses, page_size=5000)

def generate_transactions(cursor):
    print(f"Insert {NUM_ORDERS} Orders, Items, Payments, & Shipping (Bisa memakan waktu beberapa detik/menit)...")
    
    # Ambil referensi ID untuk relasi
    cursor.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT address_id, user_id FROM user_addresses")
    address_map = {}
    for row in cursor.fetchall():
        address_map.setdefault(row[1], []).append(row[0])

    cursor.execute("SELECT product_id, base_price FROM products")
    products = cursor.fetchall()

    orders, order_items, payments, shipping = [], [], [], []
    statuses = ['Completed', 'Completed', 'Completed', 'Completed', 'Pending', 'Cancelled']
    couriers = ['JNE', 'SiCepat', 'GoSend', 'J&T']
    
    for i in range(NUM_ORDERS):
        order_id = f"ORD-{fake.uuid4()[:8].upper()}"
        uid = random.choice(user_ids)
        aid = random.choice(address_map[uid])
        odt = random_date(START_DATE, END_DATE)
        status = random.choice(statuses)

        total_amount = 0
        for _ in range(random.randint(1, 3)):
            prod = random.choice(products)
            qty = random.randint(1, 2)
            unit_price = prod[1]
            total_amount += (unit_price * qty)
            order_items.append((order_id, prod[0], qty, unit_price))
        
        shipping_cost = random.randint(15, 100) * 1000
        total_amount += shipping_cost

        orders.append((order_id, uid, aid, odt, total_amount, status))
        
        if status != 'Cancelled':
            payments.append((
                order_id, 
                random.choice(['Credit Card', 'Bank Transfer', 'E-Wallet']), 
                'Success', 
                odt + timedelta(minutes=random.randint(1, 60))
            ))
            shipping.append((
                order_id, 
                random.choice(couriers), 
                fake.ean(length=13), 
                shipping_cost, 
                'Delivered' if status == 'Completed' else 'Packed'
            ))

    # Bulk Insert menggunakan page_size agar RAM tidak membeludak
    extras.execute_values(cursor, "INSERT INTO orders (order_id, user_id, address_id, order_date, total_amount, order_status) VALUES %s", orders, page_size=5000)
    extras.execute_values(cursor, "INSERT INTO order_items (order_id, product_id, quantity, unit_price_at_purchase) VALUES %s", order_items, page_size=5000)
    extras.execute_values(cursor, "INSERT INTO payments (order_id, payment_method, payment_status, payment_date) VALUES %s", payments, page_size=5000)
    extras.execute_values(cursor, "INSERT INTO shipping (order_id, courier_name, tracking_number, shipping_cost, shipping_status) VALUES %s", shipping, page_size=5000)

if __name__ == "__main__":
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        create_tables(cursor)
        generate_master_data(cursor)
        generate_users(cursor)
        generate_transactions(cursor)
        
        cursor.close()
        conn.close()
        print("🎉 SUCCESS! Semua data awal berhasil dimuat ke PostgreSQL.")
    except Exception as e:
        print(f"❌ Error: {e}")