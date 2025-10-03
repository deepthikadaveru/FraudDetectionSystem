import psycopg2
from faker import Faker
import random
from tqdm import tqdm
from datetime import datetime, timedelta

fake = Faker()

# ---------------------------
# 1️⃣ Connect to PostgreSQL
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="postgres",  # or your DB name
    user="postgres",
    password="Paused.0"   # change this
)
cur = conn.cursor()

# ---------------------------
# 2️⃣ Generate Users
# ---------------------------
NUM_USERS = 50
user_ids = []

for _ in range(NUM_USERS):
    full_name = fake.name()
    email = fake.unique.email()
    phone = fake.phone_number()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
    kyc_level = random.randint(0, 5)
    cur.execute("""
        INSERT INTO users(full_name, email, phone, dob, kyc_level)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING user_id
    """, (full_name, email, phone, dob, kyc_level))
    user_ids.append(cur.fetchone()[0])

conn.commit()
print(f"Inserted {len(user_ids)} users.")

# ---------------------------
# 3️⃣ Generate Accounts
# ---------------------------
accounts = []
account_ids = []

account_types_list = ['savings', 'current', 'credit']

for user_id in user_ids:
    num_accounts = random.randint(1, 3)
    for _ in range(num_accounts):
        account_type = random.choice(account_types_list)
        balance = round(random.uniform(1000, 500000), 2)
        account_no = str(random.randint(1000000000, 9999999999))  # 10-digit unique number
        cur.execute("""
            INSERT INTO accounts(user_id, account_no, account_type, balance)
            VALUES (%s, %s, %s, %s)
            RETURNING account_id
        """, (user_id, account_no, account_type, balance))
        account_ids.append(cur.fetchone()[0])

conn.commit()
print(f"Inserted {len(account_ids)} accounts.")

# ---------------------------
# 4️⃣ Generate Merchants
# ---------------------------
NUM_MERCHANTS = 20
merchant_ids = []
categories = ['Retail', 'Travel', 'Electronics', 'Food', 'Health']

for _ in range(NUM_MERCHANTS):
    name = fake.company()
    merchant_code = fake.unique.bothify(text='MC####')
    category = random.choice(categories)
    city = fake.city()
    country = fake.country()
    cur.execute("""
        INSERT INTO merchants(name, merchant_code, category, city, country)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING merchant_id
    """, (name, merchant_code, category, city, country))
    merchant_ids.append(cur.fetchone()[0])

conn.commit()
print(f"Inserted {len(merchant_ids)} merchants.")

# ---------------------------
# 5️⃣ Generate Devices
# ---------------------------
NUM_DEVICES = 50
device_ids = []

import json

# Inside your devices loop:
for _ in range(NUM_DEVICES):
    device_fingerprint = fake.unique.sha1()
    device_info = {
        'os': random.choice(['Windows', 'Linux', 'MacOS', 'Android', 'iOS']),
        'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
    }
    cur.execute("""
        INSERT INTO devices(device_fingerprint, device_info)
        VALUES (%s, %s)
        RETURNING device_id
    """, (device_fingerprint, json.dumps(device_info)))  # ✅ use json.dumps
    device_ids.append(cur.fetchone()[0])


conn.commit()
print(f"Inserted {len(device_ids)} devices.")

# ---------------------------
# 6️⃣ Generate Transactions
# ---------------------------
NUM_TRANSACTIONS = 1000
channels = ['atm','pos','web','mobile','bank_transfer','cheque']

transactions = []
for _ in tqdm(range(NUM_TRANSACTIONS)):
    account_id = random.choice(account_ids)
    merchant_id = random.choice(merchant_ids)
    device_id = random.choice(device_ids)
    amount = round(random.uniform(10, 2000000), 2)  # include high-value for fraud testing
    txn_time = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0,23))
    channel = random.choice(channels)
    location = f"{fake.city()}, {fake.country()}"
    ip_address = fake.ipv4_public()
    geo_lat = float(fake.latitude())
    geo_lng = float(fake.longitude())
    merchant_category = random.choice(categories)
    metadata = {
        'note': fake.sentence(),
        'device_id': device_id
    }
    # Inside transactions loop:
cur.execute("""
    INSERT INTO transactions(account_id, merchant_id, device_id, amount, txn_timestamp,
                              channel, location, ip_address, geo_lat, geo_lng, merchant_category, metadata)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (
    account_id, merchant_id, device_id, amount, txn_time, channel, location,
    ip_address, geo_lat, geo_lng, merchant_category, json.dumps(metadata)  # ✅ fix here
))
conn.commit()
print(f"Inserted {NUM_TRANSACTIONS} transactions.")

# ---------------------------
# 7️⃣ Close connection
# ---------------------------
cur.close()
conn.close()
print("All data generated successfully!")
