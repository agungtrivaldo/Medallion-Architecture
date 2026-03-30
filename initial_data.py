import psycopg2
from psycopg2 import extras
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('id_ID')

DB_CONFIG = {
    'dbname': 'oltp',
    'user': 'oltp',
    'password': 'oltppass',
    'host': '10.8.0.1',
    'port': '5433'
}

# --- [UPDATE 1: SAKLAR MODE] ---
# Ubah ke "INIT" untuk pertama kali jalan. 
# Ubah ke "INCREMENTAL" untuk simulasi jalan harian.
RUN_MODE = "INIT" 

# Jika mode INCREMENTAL, kita cuma bikin sedikit data agar server tidak berat
NUM_USERS = 10_000 if RUN_MODE == "INIT" else 50 
NUM_ORDERS = 50_000 if RUN_MODE == "INIT" else 200

# Waktu disesuaikan dengan mode
if RUN_MODE == "INIT":
    START_DATE = datetime(2025, 1, 1)
    END_DATE = datetime(2026, 3, 27)
else:
    # Mode Incremental mengambil waktu HARI INI
    START_DATE = datetime.now() - timedelta(days=1)
    END_DATE = datetime.now()

def create_tables(cursor):
    # [UPDATE 2: Tambah updated_at di tabel orders]
    cursor.execute("""
        DROP TABLE IF EXISTS shipping, payments, order_items, orders, products, categories, brands, user_addresses, users CASCADE;

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
        CREATE TABLE orders (
            order_id VARCHAR(50) PRIMARY KEY, 
            user_id INT REFERENCES users(user_id), 
            address_id INT REFERENCES user_addresses(address_id), 
            order_date TIMESTAMP, 
            total_amount DECIMAL, 
            order_status VARCHAR(50),
            updated_at TIMESTAMP  -- <--- INI WAJIB UNTUK INCREMENTAL AIRFLOW
        );
        CREATE TABLE order_items (
            item_id SERIAL PRIMARY KEY, 
            order_id VARCHAR(50) REFERENCES orders(order_id), 
            product_id INT REFERENCES products(product_id), 
            quantity INT, 
            unit_price_at_purchase DECIMAL
        );
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
    # Logika sama seperti sebelumnya... (Hanya berjalan saat INIT)
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
    users = []
    addresses = []
    
    # [UPDATE 3: Ambil ID terakhir agar tidak bentrok saat incremental]
    cursor.execute("SELECT COALESCE(MAX(user_id), 0) FROM users")
    last_user_id = cursor.fetchone()[0]
    
    for _ in range(NUM_USERS):
        last_user_id += 1
        dt = random_date(START_DATE, END_DATE)
        
        phone = fake.phone_number() if random.random() > 0.15 else None
        users.append((fake.email(), fake.password(), phone, dt, dt))
        
        for _ in range(random.randint(1, 2)):
            postal = fake.postcode() if random.random() > 0.10 else None
            addresses.append((last_user_id, fake.state(), fake.city(), postal, fake.street_address()))

    extras.execute_values(cursor, "INSERT INTO users (email, password_hash, phone_number, created_at, updated_at) VALUES %s", users, page_size=5000)
    extras.execute_values(cursor, "INSERT INTO user_addresses (user_id, province, city, postal_code, full_address) VALUES %s", addresses, page_size=5000)

def generate_transactions(cursor):
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
    
    # Bikin prefix order_id beda buat incremental biar gampang dilacak
    order_prefix = "INIT" if RUN_MODE == "INIT" else "INCR"
    
    for i in range(NUM_ORDERS):
        order_id = f"ORD-{order_prefix}-{i}-{fake.uuid4()[:6].upper()}"
        uid = random.choice(user_ids)
        
        # Cegah error jika user belum punya alamat
        if uid not in address_map: continue 
        
        aid = random.choice(address_map[uid]) if random.random() > 0.02 else None
        
        odt = random_date(START_DATE, END_DATE)
        status = random.choice(statuses)

        total_amount = 0
        has_null_price = False
        
        for _ in range(random.randint(1, 3)):
            prod = random.choice(products)
            qty = random.randint(1, 2)
            unit_price = prod[1] if random.random() > 0.01 else None
            
            if unit_price is not None:
                total_amount += (unit_price * qty)
            else:
                has_null_price = True
                
            order_items.append((order_id, prod[0], qty, unit_price))
        
        shipping_cost = random.randint(15, 100) * 1000
        
        if has_null_price or random.random() < 0.01:
            final_total_amount = None
        else:
            final_total_amount = total_amount + shipping_cost

        # [UPDATE 4: Masukkan `updated_at` ke tabel orders]
        # Untuk simulasi, anggap order terakhir diupdate beberapa jam setelah dibuat
        updated_at = odt + timedelta(hours=random.randint(1, 24)) 
        orders.append((order_id, uid, aid, odt, final_total_amount, status, updated_at))
        
        if status != 'Cancelled':
            payments.append((
                order_id, 
                random.choice(['Credit Card', 'Bank Transfer', 'E-Wallet']), 
                'Success', 
                odt + timedelta(minutes=random.randint(1, 60))
            ))
            
            ship_status = 'Delivered' if status == 'Completed' else 'Packed'
            
            if ship_status == 'Packed':
                tracking = None
                courier = random.choice(couriers) if random.random() > 0.2 else None
            else:
                tracking = fake.ean(length=13)
                courier = random.choice(couriers)

            shipping.append((
                order_id, 
                courier, 
                tracking, 
                shipping_cost, 
                ship_status
            ))

    # [UPDATE 5: Sesuaikan struktur INSERT dengan kolom baru]
    extras.execute_values(cursor, "INSERT INTO orders (order_id, user_id, address_id, order_date, total_amount, order_status, updated_at) VALUES %s", orders, page_size=5000)
    extras.execute_values(cursor, "INSERT INTO order_items (order_id, product_id, quantity, unit_price_at_purchase) VALUES %s", order_items, page_size=5000)
    extras.execute_values(cursor, "INSERT INTO payments (order_id, payment_method, payment_status, payment_date) VALUES %s", payments, page_size=5000)
    extras.execute_values(cursor, "INSERT INTO shipping (order_id, courier_name, tracking_number, shipping_cost, shipping_status) VALUES %s", shipping, page_size=5000)

def update_existing_orders(cursor):
    """
    [FITUR BARU]
    Mensimulasikan perubahan data lama (contoh: status Pending berubah jadi Completed).
    Ini gunanya agar Airflow MERGE kita melakukan aksi UPDATE, bukan cuma INSERT.
    """
    print("Mensimulasikan perubahan status pada order lama...")
    cursor.execute("""
        UPDATE orders 
        SET order_status = 'Completed', updated_at = NOW() 
        WHERE order_status = 'Pending' 
        AND order_id IN (
            SELECT order_id FROM orders WHERE order_status = 'Pending' LIMIT 100
        );
    """)
    print(f"Berhasil mengupdate {cursor.rowcount} order yang sebelumnya Pending.")

if __name__ == "__main__":
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print(f"=== MENJALANKAN PABRIK DATA DALAM MODE: {RUN_MODE} ===")
        
        if RUN_MODE == "INIT":
            print("Membangun Tabel Baru...")
            create_tables(cursor)
            print("Insert Data Master...")
            generate_master_data(cursor)
            print("Insert Data Users Awal...")
            generate_users(cursor)
            print("Insert Data Transaksi Awal (Harap tunggu)...")
            generate_transactions(cursor)
        
        elif RUN_MODE == "INCREMENTAL":
            print("Memasukkan Data Users Baru...")
            generate_users(cursor)
            print("Memasukkan Transaksi Hari Ini...")
            generate_transactions(cursor)
            update_existing_orders(cursor) # <-- Mengubah order kemarin
            
        cursor.close()
        conn.close()
        print("SUCCESS! Proses selesai.")
    except Exception as e:
        print(f"ERROR: {e}")