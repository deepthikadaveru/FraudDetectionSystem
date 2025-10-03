# fraud_generate_advanced.py

import psycopg2
from datetime import datetime, timedelta
from geopy.distance import geodesic
import random

# ---------------------------
# 1️⃣ Connect to PostgreSQL
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Paused.0"
)
cur = conn.cursor()

# ---------------------------
# 2️⃣ Fetch transactions
# ---------------------------
cur.execute("""
    SELECT txn_id, account_id, amount, txn_timestamp, location, geo_lat, geo_lng, device_id
    FROM transactions
    WHERE txn_id NOT IN (SELECT txn_id FROM fraud_alerts)
""")
transactions = cur.fetchall()
print(f"Transactions to check: {len(transactions)}")

# ---------------------------
# 3️⃣ Define fraud rules
# ---------------------------

def high_value_rule(txn):
    return txn[2] > 100000

def velocity_rule(txn_list, account_id, current_txn):
    one_hour_ago = current_txn[3] - timedelta(hours=1)
    recent_txns = [t for t in txn_list if t[1] == account_id and one_hour_ago <= t[3] < current_txn[3]]
    return len(recent_txns) >= 5

def geo_mismatch_rule(txn_list, account_id, current_txn, max_distance_km=500):
    # Compare current txn location with last txn location for same account
    last_txns = [t for t in txn_list if t[1] == account_id and t[3] < current_txn[3] and t[5] and t[6]]
    if not last_txns:
        return False
    last_txn = last_txns[-1]  # most recent
    distance = geodesic((current_txn[5], current_txn[6]), (last_txn[5], last_txn[6])).km
    return distance > max_distance_km

def device_anomaly_rule(txn_list, account_id, current_txn):
    # Randomly flag a device anomaly (simulate stolen device/IP)
    return random.random() < 0.02  # 2% chance

# ---------------------------
# 4️⃣ Generate alerts
# ---------------------------
alerts_inserted = 0

for txn in transactions:
    txn_id, account_id, amount, txn_time, location, geo_lat, geo_lng, device_id = txn
    rules_triggered = []
    
    if high_value_rule(txn):
        rules_triggered.append(("HIGH_VALUE", "Amount exceeds 100,000", "high", 90.0))
    
    if velocity_rule(transactions, account_id, txn):
        rules_triggered.append(("VELOCITY", "Too many transactions in 1 hour", "medium", 70.0))
    
    if geo_mismatch_rule(transactions, account_id, txn):
        rules_triggered.append(("GEO_MISMATCH", "Transaction location far from last location", "high", 85.0))
    
    if device_anomaly_rule(transactions, account_id, txn):
        rules_triggered.append(("DEVICE_ANOMALY", "Unrecognized device/IP used", "medium", 75.0))
    
    # Insert alerts
    for rule_id, reason, severity, score in rules_triggered:
        cur.execute("""
            INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'new')
        """, (txn_id, account_id, rule_id, reason, severity, score))
        alerts_inserted += 1

conn.commit()
print(f"Total fraud alerts inserted: {alerts_inserted}")

# ---------------------------
# 5️⃣ Close connection
# ---------------------------
cur.close()
conn.close()
print("Advanced fraud alert generation completed!")
