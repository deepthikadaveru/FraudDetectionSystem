import psycopg2
import psycopg2.extras  # ✅ add this
from faker import Faker
import random
from datetime import datetime, timedelta


fake = Faker()

# ---------------------------
# 1️⃣ Connect to PostgreSQL
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Paused.0"  # your password
)
cur = conn.cursor()

# ---------------------------
# 2️⃣ Parameters
# ---------------------------
NUM_NEW_TXNS = 100  # number of transactions to simulate per run
channels = ['atm', 'pos', 'web', 'mobile', 'bank_transfer']

# ---------------------------
# 3️⃣ Fetch existing IDs
# ---------------------------
cur.execute("SELECT account_id FROM accounts;")
account_ids = [row[0] for row in cur.fetchall()]

cur.execute("SELECT merchant_id FROM merchants;")
merchant_ids = [row[0] for row in cur.fetchall()]

cur.execute("SELECT device_id FROM devices;")
device_ids = [row[0] for row in cur.fetchall()]

# ---------------------------
# 4️⃣ Generate transactions
# ---------------------------
transactions = []
for _ in range(NUM_NEW_TXNS):
    account_id = random.choice(account_ids)
    merchant_id = random.choice(merchant_ids)
    device_id = random.choice(device_ids)
    amount = round(random.uniform(10, 200000), 2)
    txn_time = datetime.now() - timedelta(minutes=random.randint(0, 1440))
    channel = random.choice(channels)
    location = f"{fake.city()}, {fake.country()}"
    ip_address = fake.ipv4()
    geo_lat = float(fake.latitude())
    geo_lng = float(fake.longitude())
    merchant_category = random.choice(['Retail','Travel','Electronics','Food','Health'])
    metadata = {}  # can add custom info if needed

    transactions.append((
        account_id, merchant_id, device_id, amount, txn_time, channel,
        location, ip_address, geo_lat, geo_lng, merchant_category, psycopg2.extras.Json(metadata)
    ))

# ---------------------------
# 5️⃣ Insert transactions
# ---------------------------
insert_query = """
INSERT INTO transactions(
    account_id, merchant_id, device_id, amount, txn_timestamp,
    channel, location, ip_address, geo_lat, geo_lng, merchant_category, metadata
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

cur.executemany(insert_query, transactions)
conn.commit()
print(f"Inserted {len(transactions)} new transactions.")

# ---------------------------
# 6️⃣ Close connection
# ---------------------------
cur.close()
conn.close()
print("Daily transaction simulation complete!")
 
