import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Paused.0"
)
cur = conn.cursor()

sql = """
-- Drop old trigger and function first
DROP TRIGGER IF EXISTS trg_check_txn ON transactions;
DROP FUNCTION IF EXISTS trg_check_txn_for_fraud();

-- Create enhanced fraud-check function
CREATE OR REPLACE FUNCTION trg_check_txn_for_fraud()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    last_txn TIMESTAMP;
    txn_count INT; 
    distance_km DOUBLE PRECISION;
    prev_lat DOUBLE PRECISION;
    prev_lng DOUBLE PRECISION;
BEGIN
    -- High-value transaction
    IF NEW.amount > 100000 THEN
        INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
        VALUES (NEW.txn_id, NEW.account_id, 'HIGH_VALUE', 'Amount exceeds threshold', 'high', 90.0);
    END IF;

    -- Rapid multiple transactions (velocity)
    SELECT MAX(txn_timestamp) INTO last_txn
    FROM transactions
    WHERE account_id = NEW.account_id
      AND txn_timestamp > now() - INTERVAL '1 hour';

    SELECT COUNT(*) INTO txn_count
    FROM transactions
    WHERE account_id = NEW.account_id
      AND txn_timestamp > now() - INTERVAL '1 hour';

    IF txn_count > 3 THEN
        INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
        VALUES (NEW.txn_id, NEW.account_id, 'VELOCITY', 'More than 3 transactions in 1 hour', 'medium', 70.0);
    END IF;

    -- Geo-mismatch
    SELECT geo_lat, geo_lng INTO prev_lat, prev_lng
    FROM transactions
    WHERE account_id = NEW.account_id
    ORDER BY txn_timestamp DESC
    LIMIT 1;

    IF prev_lat IS NOT NULL AND prev_lng IS NOT NULL THEN
        distance_km := 6371 * acos(
            cos(radians(prev_lat)) * cos(radians(NEW.geo_lat)) *
            cos(radians(NEW.geo_lng) - radians(prev_lng)) +
            sin(radians(prev_lat)) * sin(radians(NEW.geo_lat))
        );
        IF distance_km > 500 THEN
            INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
            VALUES (NEW.txn_id, NEW.account_id, 'GEO_MISMATCH', 'Transaction location far from last txn', 'high', 85.0);
        END IF;
    END IF;

    -- New/unknown device
    IF NOT EXISTS (
        SELECT 1 FROM transactions t
        WHERE t.account_id = NEW.account_id
          AND t.device_id = NEW.device_id
    ) THEN
        INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
        VALUES (NEW.txn_id, NEW.account_id, 'NEW_DEVICE', 'Transaction from new device', 'medium', 60.0);
    END IF;

    -- Suspicious merchant category
    IF NEW.merchant_category IN ('Gambling', 'Crypto') THEN
        INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
        VALUES (NEW.txn_id, NEW.account_id, 'SUSPICIOUS_MERCHANT', 'High-risk merchant category', 'high', 80.0);
    END IF;

    RETURN NEW;
END;
$$;

-- Attach trigger to transactions table
CREATE TRIGGER trg_check_txn
AFTER INSERT ON transactions
FOR EACH ROW
EXECUTE FUNCTION trg_check_txn_for_fraud();
"""

cur.execute(sql)
conn.commit()
cur.close()
conn.close()

print("Trigger and function created successfully.")
