import psycopg2
from datetime import datetime, timedelta
from geopy.distance import geodesic

# ---------------------------
# 1️⃣ Connect to PostgreSQL
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="postgres",  # your DB name
    user="postgres",
    password="Paused.0"
)
cur = conn.cursor()

# ---------------------------
# 2️⃣ Parameters / Thresholds
# ---------------------------
HIGH_VALUE_THRESHOLD = 1000000     # transactions above this flagged
VELOCITY_COUNT = 3                 # >3 txns in short period
VELOCITY_WINDOW_MIN = 10           # minutes
GEO_DISTANCE_KM = 500              # km for geo-mismatch

# ---------------------------
# 3️⃣ Fetch recent transactions
# ---------------------------
cur.execute("""
    SELECT txn_id, account_id, amount, txn_timestamp, geo_lat, geo_lng, device_id
    FROM transactions
    ORDER BY txn_timestamp ASC
""")
transactions = cur.fetchall()

# Store last transaction per account for velocity / geo check
account_last_txns = {}   # account_id -> list of (timestamp, lat, lng, device_id)

# ---------------------------
# 4️⃣ Process transactions
# ---------------------------
for txn in transactions:
    txn_id, account_id, amount, txn_time, geo_lat, geo_lng, device_id = txn
    alerts = []

    # --- High Value ---
    if amount > HIGH_VALUE_THRESHOLD:
        alerts.append({
            'rule_id': 'HIGH_VALUE',
            'reason': f'Transaction amount {amount} exceeds threshold',
            'severity': 'high',
            'score': 90
        })

    # --- Velocity / Rapid Transactions ---
    last_txns = account_last_txns.get(account_id, [])
    recent_txns = [t for t in last_txns if (txn_time - t[0]).total_seconds()/60 <= VELOCITY_WINDOW_MIN]

    if len(recent_txns) >= VELOCITY_COUNT - 1:  # current txn makes it VELOCITY_COUNT
        alerts.append({
            'rule_id': 'VELOCITY',
            'reason': f'{len(recent_txns)+1} transactions within {VELOCITY_WINDOW_MIN} minutes',
            'severity': 'medium',
            'score': 70
        })

    # --- Geo Mismatch ---
    for t in last_txns:
        prev_lat, prev_lng = t[1], t[2]
        if prev_lat is not None and geo_lat is not None:
            distance = geodesic((prev_lat, prev_lng), (geo_lat, geo_lng)).km
            if distance > GEO_DISTANCE_KM:
                alerts.append({
                    'rule_id': 'GEO_MISMATCH',
                    'reason': f'Transaction {distance:.1f} km from last txn',
                    'severity': 'medium',
                    'score': 75
                })
                break  # one alert is enough per txn

    # --- Device Anomaly ---
    known_devices = [t[3] for t in last_txns]
    if device_id not in known_devices and known_devices:
        alerts.append({
            'rule_id': 'DEVICE_ANOMALY',
            'reason': f'New device used for this account',
            'severity': 'medium',
            'score': 65
        })

    # --- Insert alerts ---
    for alert in alerts:
        cur.execute("""
            INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (txn_id, account_id, alert['rule_id'], alert['reason'], alert['severity'], alert['score']))

    # Update last_txns
    last_txns.append((txn_time, geo_lat, geo_lng, device_id))
    # Keep only last 10 transactions per account to save memory
    account_last_txns[account_id] = last_txns[-10:]

conn.commit()
cur.close()
conn.close()
print("Fraud alerts generated successfully!")
