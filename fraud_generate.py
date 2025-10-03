# fraud_generate.py

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
    SELECT txn_id, account_id, amount, txn_timestamp, location
    FROM transactions
    WHERE txn_id NOT IN (SELECT txn_id FROM fraud_alerts)
""")
transactions = cur.fetchall()
print(f"Transactions to check: {len(transactions)}")

# ---------------------------
# 3️⃣ Define fraud rules
# ---------------------------

def high_value_rule(txn):
    # Amount > 100,000
    return txn[2] > 100000

def velocity_rule(txn_list, account_id, current_txn):
    # >5 transactions in last 1 hour
    one_hour_ago = current_txn[3] - timedelta(hours=1)
    recent_txns = [t for t in txn_list if t[1] == account_id and one_hour_ago <= t[3] < current_txn[3]]
    return len(recent_txns) >= 5

# For demo, skip geo-mismatch for simplicity

# ---------------------------
# 4️⃣ Generate alerts
# ---------------------------
alerts_inserted = 0

for txn in transactions:
    txn_id, account_id, amount, txn_time, location = txn
    rules_triggered = []
    
    if high_value_rule(txn):
        rules_triggered.append(("HIGH_VALUE", "Amount exceeds 100,000", "high", 90.0))
    
    if velocity_rule(transactions, account_id, txn):
        rules_triggered.append(("VELOCITY", "Too many transactions in 1 hour", "medium", 70.0))
    
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
print("Fraud alert generation completed!")
