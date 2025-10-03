# insert_bulk_test_txns.py
import psycopg2
import psycopg2.extras
import random
import json
from datetime import datetime, timedelta
from faker import Faker
fake = Faker()

# ---------- CONFIG ----------
DB = {
    "host": "localhost",
    "database": "postgres",   # change if you use a different DB name
    "user": "postgres",
    "password": "Paused.0"    # change to your password
}
NUM_RANDOM = 60   # total transactions to insert (adjust as needed)
# ----------------------------

conn = psycopg2.connect(**DB)
cur = conn.cursor()

# fetch valid ids
cur.execute("SELECT account_id FROM accounts")
account_ids = [r[0] for r in cur.fetchall()]
if not account_ids:
    raise SystemExit("No accounts found — create accounts first.")

cur.execute("SELECT merchant_id, category FROM merchants")
merchants = cur.fetchall()
if not merchants:
    raise SystemExit("No merchants found — create merchants first.")
merchant_ids = [r[0] for r in merchants]
merchant_cat_map = {r[0]: r[1] for r in merchants}

cur.execute("SELECT device_id FROM devices")
device_ids = [r[0] for r in cur.fetchall()]
if not device_ids:
    raise SystemExit("No devices found — create devices first.")

print(f"Found {len(account_ids)} accounts, {len(merchant_ids)} merchants, {len(device_ids)} devices.")

# helper to pick random existing IDs
def pick_account():
    return random.choice(account_ids)

def pick_merchant():
    return random.choice(merchant_ids)

def pick_device():
    return random.choice(device_ids)

insert_query = """
INSERT INTO transactions(
    account_id, merchant_id, device_id, amount, txn_timestamp,
    channel, location, ip_address, geo_lat, geo_lng, merchant_category, metadata
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

txns = []

# 1) Add some high-value txns (to trigger HIGH_VALUE)
for _ in range(8):
    acct = pick_account()
    mid = pick_merchant()
    dev = pick_device()
    amount = random.uniform(120000, 500000)  # >100k
    ts = datetime.now() - timedelta(minutes=random.randint(0, 120))
    city = fake.city()
    country = fake.country()
    lat = float(fake.latitude())
    lng = float(fake.longitude())
    cat = merchant_cat_map.get(mid) or random.choice(['Retail','Travel','Electronics','Food','Health'])
    meta = {"note":"test_high_value"}
    txns.append((acct, mid, dev, round(amount,2), ts, random.choice(['atm','pos','web','mobile']), f"{city}, {country}",
                 fake.ipv4_public(), lat, lng, cat, json.dumps(meta)))

# 2) Create velocity bursts (multiple txns for same account within short window)
for _ in range(6):
    acct = pick_account()
    mid = pick_merchant()
    dev = pick_device()
    # create 4 quick txns for this account
    base_time = datetime.now() - timedelta(minutes=random.randint(0,60))
    for k in range(4):
        amount = random.uniform(100, 8000)
        ts = base_time + timedelta(minutes=k*2)  # 2 minutes apart
        city = fake.city(); country = fake.country()
        lat = float(fake.latitude()); lng = float(fake.longitude())
        cat = merchant_cat_map.get(mid) or random.choice(['Retail','Travel'])
        meta = {"note":"test_velocity"}
        txns.append((acct, mid, dev, round(amount,2), ts, 'pos', f"{city}, {country}",
                     fake.ipv4_public(), lat, lng, cat, json.dumps(meta)))

# 3) Geo-mismatch: insert two txns for same account far apart
for _ in range(6):
    acct = pick_account()
    mid = pick_merchant()
    dev = pick_device()
    # first txn at one location
    ts1 = datetime.now() - timedelta(hours=5)
    lat1, lng1 = 12.9716, 77.5946   # e.g., Bangalore
    txns.append((acct, mid, dev, round(random.uniform(100,5000),2), ts1, 'web', "Bangalore, IN",
                 fake.ipv4_public(), lat1, lng1, 'Retail', json.dumps({"note":"geo1"})))
    # second txn quickly from a far location (simulate > 500km apart)
    ts2 = ts1 + timedelta(minutes=30)
    lat2, lng2 = 28.7041, 77.1025   # e.g., Delhi (far)
    txns.append((acct, mid, dev, round(random.uniform(100,5000),2), ts2, 'web', "Delhi, IN",
                 fake.ipv4_public(), lat2, lng2, 'Retail', json.dumps({"note":"geo2"})))

# 4) New device: choose an account but a device id not used earlier for that account
# We can't easily check device history here; instead create txns where device_id is a likely new one:
for _ in range(6):
    acct = pick_account()
    mid = pick_merchant()
    # create a new device id (temporary) by generating a large number not in device_ids
    # but to avoid FK error, we need existing device ids — instead pick a device and mark it suspicious via metadata
    dev = pick_device()
    ts = datetime.now() - timedelta(minutes=random.randint(0,200))
    lat = float(fake.latitude()); lng = float(fake.longitude())
    txns.append((acct, mid, dev, round(random.uniform(50,8000),2), ts, 'mobile', fake.city()+", "+fake.country(),
                 fake.ipv4_public(), lat, lng, 'Retail', json.dumps({"note":"possible_new_device"})))

# 5) Suspicious merchant categories (Gambling/Crypto)
# If you don't have merchants with such categories, we set merchant_category explicitly
for _ in range(8):
    acct = pick_account()
    mid = pick_merchant()
    dev = pick_device()
    amount = random.uniform(500,20000)
    ts = datetime.now() - timedelta(minutes=random.randint(0,1440))
    lat = float(fake.latitude()); lng = float(fake.longitude())
    cat = random.choice(['Gambling','Crypto'])
    txns.append((acct, mid, dev, round(amount,2), ts, random.choice(['web','pos']), fake.city()+", "+fake.country(),
                 fake.ipv4_public(), lat, lng, cat, json.dumps({"note":"suspicious_merchant"})))

# 6) Fill remaining transactions randomly until NUM_RANDOM
while len(txns) < NUM_RANDOM:
    acct = pick_account()
    mid = pick_merchant()
    dev = pick_device()
    amount = random.uniform(10,50000)
    ts = datetime.now() - timedelta(minutes=random.randint(0,10000))
    lat = float(fake.latitude()); lng = float(fake.longitude())
    cat = merchant_cat_map.get(mid) or random.choice(['Retail','Food','Electronics'])
    txns.append((acct, mid, dev, round(amount,2), ts, random.choice(['atm','pos','web','mobile']),
                 fake.city()+", "+fake.country(), fake.ipv4_public(), lat, lng, cat, json.dumps({"note":"random"})))

# Shuffle for randomness
random.shuffle(txns)

# Bulk insert
print(f"Inserting {len(txns)} transactions...")
psycopg2.extras.execute_batch(cur, insert_query, txns, page_size=200)
conn.commit()
print("Insert complete.")

# optionally display number of alerts after insert
cur.execute("SELECT count(*) FROM fraud_alerts;")
print("Total fraud alerts now:", cur.fetchone()[0])

cur.close()
conn.close()
