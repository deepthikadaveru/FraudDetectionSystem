SELECT version();

CREATE EXTENSION postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE account_status AS ENUM ('active','suspended','closed');
CREATE TYPE account_type AS ENUM ('savings','current','credit');
CREATE TYPE txn_channel AS ENUM ('atm','pos','web','mobile','bank_transfer','cheque');
CREATE TYPE alert_severity AS ENUM ('low','medium','high','critical');
CREATE TYPE alert_status AS ENUM ('new','investigating','confirmed','dismissed');


CREATE TABLE users (
  user_id              BIGSERIAL PRIMARY KEY,
  full_name            TEXT NOT NULL,
  email                TEXT UNIQUE NOT NULL,
  phone                TEXT,
  dob                  DATE,
  kyc_level            SMALLINT DEFAULT 0,  -- 0..n
  created_at           TIMESTAMPTZ DEFAULT now(),
  updated_at           TIMESTAMPTZ DEFAULT now(),
  risk_score           NUMERIC(5,2) DEFAULT 0.00 -- 0..100 scale
);

CREATE TABLE roles (
  role_id   SMALLSERIAL PRIMARY KEY,
  role_name TEXT UNIQUE NOT NULL  -- e.g., 'analyst','admin','auditor'
);

CREATE TABLE users_roles (
  user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
  role_id SMALLINT REFERENCES roles(role_id) ON DELETE CASCADE,
  PRIMARY KEY (user_id, role_id)
);

select * from users_roles;

CREATE TABLE accounts (
  account_id    BIGSERIAL PRIMARY KEY,
  user_id       BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  account_no    TEXT UNIQUE NOT NULL,
  account_type  account_type NOT NULL,
  balance       NUMERIC(18,2) DEFAULT 0.00,
  currency      TEXT DEFAULT 'INR',
  status        account_status DEFAULT 'active',
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_accounts_user ON accounts(user_id);

CREATE TABLE merchants (
  merchant_id   BIGSERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  merchant_code TEXT UNIQUE, -- MC code / aggregator id
  category      TEXT, -- merchant category code or description
  city          TEXT,
  country       TEXT,
  -- optional: geometry for geo-enabled merchant location (PostGIS)
  -- geom       geography(POINT,4326),
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_merchants_category ON merchants(category);


CREATE TABLE devices (
  device_id     BIGSERIAL PRIMARY KEY,
  device_fingerprint TEXT UNIQUE,
  device_info   JSONB,
  first_seen    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE transactions (
  txn_id        BIGSERIAL NOT NULL,
  account_id    BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  merchant_id   BIGINT REFERENCES merchants(merchant_id),
  device_id     BIGINT REFERENCES devices(device_id),
  amount        NUMERIC(18,2) NOT NULL,
  currency      TEXT DEFAULT 'INR',
  txn_timestamp TIMESTAMPTZ NOT NULL,
  channel       txn_channel NOT NULL,
  status        TEXT DEFAULT 'posted', -- pending/posted/failed
  location      TEXT,                  -- e.g., "Hyderabad,IN" (or use geometry)
  ip_address    INET,
  geo_lat       DOUBLE PRECISION,      -- optional latitude
  geo_lng       DOUBLE PRECISION,      -- optional longitude
  merchant_category TEXT,
  metadata      JSONB,
  PRIMARY KEY (txn_id, txn_timestamp)  -- composite key helps partition routing
) PARTITION BY RANGE (txn_timestamp);


-- Example partition (monthly). Create partitions for months you'll insert.
CREATE TABLE transactions_2025_10 PARTITION OF transactions
  FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

-- Create indexes on access patterns (global indexes on parent supported in PG >= 11? create on partitions)
CREATE INDEX idx_txn_account_ts ON transactions (account_id, txn_timestamp DESC);
CREATE INDEX idx_txn_txnid ON transactions (txn_id);
CREATE INDEX idx_txn_merchant ON transactions (merchant_id);
CREATE INDEX idx_txn_amount ON transactions (amount);


-- 1️⃣ Drop existing tables if any
DROP TABLE IF EXISTS fraud_alerts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;

-- 2️⃣ Transactions table (txn_id is PRIMARY KEY)
CREATE TABLE transactions (
    txn_id        BIGSERIAL PRIMARY KEY,
    account_id    BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    merchant_id   BIGINT REFERENCES merchants(merchant_id),
    device_id     BIGINT REFERENCES devices(device_id),
    amount        NUMERIC(18,2) NOT NULL,
    currency      TEXT DEFAULT 'INR',
    txn_timestamp TIMESTAMPTZ NOT NULL,
    channel       VARCHAR(50) NOT NULL, -- ATM, POS, Online, Mobile, etc.
    status        TEXT DEFAULT 'posted',
    location      TEXT,
    ip_address    INET,
    geo_lat       DOUBLE PRECISION,
    geo_lng       DOUBLE PRECISION,
    merchant_category TEXT,
    metadata      JSONB
);

-- Optional indexes for performance
CREATE INDEX idx_txn_account_ts ON transactions (account_id, txn_timestamp DESC);
CREATE INDEX idx_txn_merchant ON transactions (merchant_id);
CREATE INDEX idx_txn_amount ON transactions (amount);

-- 3️⃣ Fraud Alerts table
CREATE TABLE fraud_alerts (
    alert_id      BIGSERIAL PRIMARY KEY,
    txn_id        BIGINT NOT NULL REFERENCES transactions(txn_id) ON DELETE CASCADE,
    account_id    BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    rule_id       TEXT,         -- e.g., "HIGH_VALUE", "VELOCITY", "GEO_MISMATCH"
    reason        TEXT,
    severity      TEXT DEFAULT 'medium',  -- you can later create ENUM
    score         NUMERIC(5,2) DEFAULT 0.0,
    status        TEXT DEFAULT 'new',
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now(),
    analyst_id    BIGINT        -- FK to users (analyst)
);

-- Indexes for quick access
CREATE INDEX idx_alert_account ON fraud_alerts(account_id);
CREATE INDEX idx_alert_status ON fraud_alerts(status);


CREATE TABLE alert_actions (
  action_id   BIGSERIAL PRIMARY KEY,
  alert_id    BIGINT NOT NULL REFERENCES fraud_alerts(alert_id) ON DELETE CASCADE,
  actor_id    BIGINT REFERENCES users(user_id),
  action      TEXT NOT NULL, -- 'investigate','confirm','dismiss','comment'
  comment     TEXT,
  action_time TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE audit_logs (
  audit_id    BIGSERIAL PRIMARY KEY,
  entity      TEXT NOT NULL, -- e.g., 'transaction','fraud_alert','account'
  entity_id   TEXT,          -- id as text to be generic
  action      TEXT NOT NULL, -- 'create','update','flag','delete'
  payload     JSONB,         -- snapshot or change details
  actor       BIGINT,        -- user who caused it (nullable for system)
  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE MATERIALIZED VIEW mv_fraud_by_day AS
SELECT date_trunc('day', fa.created_at) AS day,
       COUNT(*) AS total_alerts,
       SUM(CASE WHEN fa.severity IN ('high','critical') THEN 1 ELSE 0 END) AS high_severity
FROM fraud_alerts fa
GROUP BY 1
WITH NO DATA;


CREATE OR REPLACE FUNCTION trg_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER users_updated_at BEFORE UPDATE ON users
  FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

CREATE TRIGGER accounts_updated_at BEFORE UPDATE ON accounts
  FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

-- Placeholder for fraud-checking trigger function (we'll implement complex logic later)
CREATE OR REPLACE FUNCTION trg_check_txn_for_fraud()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
  -- local variables for checks
  rapid_count INT;
  last_txn_ts TIMESTAMPTZ;
  distance_km DOUBLE PRECISION;
BEGIN
  -- Example simple rule: high value transaction
  IF NEW.amount > 1000000 THEN
    INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
    VALUES (NEW.txn_id, NEW.account_id, 'HIGH_VALUE', 'Amount exceeds threshold', 'high', 90.0);
  END IF;

  -- More rules (velocity, geo mismatch) will be added in Stage 3/4

  RETURN NEW;
END;
$$;

-- Attach trigger to each partition (or parent if you choose FOR EACH ROW ON transactions)
CREATE TRIGGER trg_check_txn AFTER INSERT ON transactions
  FOR EACH ROW EXECUTE FUNCTION trg_check_txn_for_fraud();

SELECT account_id FROM accounts LIMIT 10;


-- Example: 5 high-value transactions for testing
INSERT INTO transactions(account_id, merchant_id, device_id, amount, txn_timestamp, channel, location)
VALUES
  (6, 1, 1, 150000, now(), 'ATM', 'Hyderabad, IN'),
  (2, 2, 2, 120000, now(), 'POS', 'Bangalore, IN'),
  (3, 3, 3, 130000, now(), 'Online', 'Delhi, IN'),
  (4, 4, 4, 160000, now(), 'Mobile', 'Mumbai, IN'),
  (5, 5, 5, 200000, now(), 'Bank_Transfer', 'Chennai, IN');


 SELECT * FROM fraud_alerts ORDER BY created_at DESC LIMIT 10;
 
CREATE OR REPLACE FUNCTION trg_check_txn_for_fraud()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  -- Lower threshold for testing
  IF NEW.amount > 100000 THEN
    INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
    VALUES (NEW.txn_id, NEW.account_id, 'HIGH_VALUE', 'Amount exceeds threshold', 'high', 90.0);
  END IF;

  RETURN NEW;
END;


SELECT * FROM fraud_alerts ORDER BY created_at DESC LIMIT 20;


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
    SELECT COUNT(*) INTO txn_count
    FROM transactions
    WHERE account_id = NEW.account_id
      AND txn_timestamp > now() - INTERVAL '1 hour';

    IF txn_count > 3 THEN
        INSERT INTO fraud_alerts(txn_id, account_id, rule_id, reason, severity, score)
        VALUES (NEW.txn_id, NEW.account_id, 'VELOCITY', 'More than 3 transactions in 1 hour', 'medium', 70.0);
    END IF;

    -- Geo-mismatch (>500 km)
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

-- Example: Insert transactions to trigger different fraud rules

-- 1️⃣ High-value transaction
INSERT INTO transactions(account_id, merchant_id, device_id, amount, txn_timestamp, channel, location, geo_lat, geo_lng, merchant_category)
VALUES
  (100, 1, 1, 150000, now(), 'ATM', 'Hyderabad, IN', 17.3850, 78.4867, 'Food');

-- 2️⃣ Multiple transactions in 1 hour (velocity)
INSERT INTO transactions(account_id, merchant_id, device_id, amount, txn_timestamp, channel, location, geo_lat, geo_lng, merchant_category)
VALUES
  (2, 2, 2, 5000, now(), 'POS', 'Bangalore, IN', 12.9716, 77.5946, 'Retail'),
  (2, 3, 2, 3000, now() - INTERVAL '10 minutes', 'POS', 'Bangalore, IN', 12.9716, 77.5946, 'Retail'),
  (2, 4, 2, 4000, now() - INTERVAL '20 minutes', 'POS', 'Bangalore, IN', 12.9716, 77.5946, 'Retail'),
  (2, 5, 2, 6000, now() - INTERVAL '30 minutes', 'POS', 'Bangalore, IN', 12.9716, 77.5946, 'Retail');

-- 3️⃣ Geo-mismatch (distance >500 km from last transaction)
INSERT INTO transactions(account_id, merchant_id, device_id, amount, txn_timestamp, channel, location, geo_lat, geo_lng, merchant_category)
VALUES
  (3, 6, 3, 8000, now(), 'Online', 'Delhi, IN', 28.7041, 77.1025, 'Electronics');

-- 4️⃣ New device
INSERT INTO transactions(account_id, merchant_id, device_id, amount, txn_timestamp, channel, location, geo_lat, geo_lng, merchant_category)
VALUES
  (4, 7, 7, 7000, now(), 'Mobile', 'Mumbai, IN', 19.0760, 72.8777, 'Food');

-- 5️⃣ Suspicious merchant category
INSERT INTO transactions(account_id, merchant_id, device_id, amount, txn_timestamp, channel, location, geo_lat, geo_lng, merchant_category)
VALUES
  (5, 8, 5, 12000, now(), 'Bank_Transfer', 'Chennai, IN', 13.0827, 80.2707, 'Gambling');

-- Check the triggered fraud alerts
SELECT *
FROM fraud_alerts
ORDER BY created_at DESC;

SELECT account_id FROM accounts ORDER BY account_id;

CREATE OR REPLACE FUNCTION trg_check_txn_for_fraud()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    prev_lat DOUBLE PRECISION;
    prev_lng DOUBLE PRECISION;
    distance_km DOUBLE PRECISION;
BEGIN
    -- Your logic using NEW
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_check_txn
AFTER INSERT ON transactions
FOR EACH ROW
EXECUTE FUNCTION trg_check_txn_for_fraud();
