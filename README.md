# 🛡️ Fraud Detection System

[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/)  
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue)](https://www.postgresql.org/)  
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-orange)](https://streamlit.io/)

A **banking fraud detection simulator** using **PostgreSQL** and **Python**, detecting suspicious transactions automatically and visualizing alerts in a **Streamlit dashboard**.

---

## 🗂 Project Structure

project-root/
│
├─ generate_data.py # Create users, accounts, merchants, devices
├─ daily_trans.py # Insert daily transactions
├─ all_fraud_data.py # Insert bulk random transactions
├─ fraud_detect.sql # PostgreSQL trigger/function for fraud detection
├─ dashboard.py # Streamlit dashboard
├─ requirements.txt # Python dependencies
└─ README.md # This file

yaml
Copy code

---

## ⚡ Requirements

- Python 3.10+  
- PostgreSQL 12+  

**Install dependencies:**

pip install -r requirements.txt
Libraries used: psycopg2-binary, pandas, streamlit, geopy, faker (optional for random data)

## 🛠 Setup

### Create database:

sql
Copy code
CREATE DATABASE fraud_detection;
Create tables:
users, accounts, merchants, devices, transactions, fraud_alerts.

### Update connection in Python scripts:

python
Copy code
conn = psycopg2.connect(
    host="localhost",
    database="fraud_detection",
    user="your_username",
    password="your_password"
)

## 🚀 How to Run
1️⃣ Generate base data
bash
Copy code
python generate_data.py
Creates users, accounts, merchants, and devices.

2️⃣ Insert daily transactions
bash
Copy code
python daily_trans.py
Adds transactions for testing.

3️⃣ Insert bulk random transactions (optional)
bash
Copy code
python all_fraud_data.py
Generates multiple realistic fraud alerts.

4️⃣ Set up fraud detection trigger
Run fraud_detect.sql in PostgreSQL.

Fraud rules include:

High-value transactions

Rapid multiple transactions (velocity)

Geo-location mismatches

New/unknown devices

Suspicious merchant categories

5️⃣ View the dashboard
bash
Copy code
streamlit run dashboard.py
Dashboard shows:

Total transactions

Total accounts

Total merchants

Fraud alerts detected

## 🎯 Features
✅ Automated fraud detection with triggers

✅ Realistic test data generation

✅ Interactive Streamlit dashboard

✅ Modular, easy-to-use Python scripts

## 💡 Tips
Always run generate_data.py first.

Use all_fraud_data.py for testing multiple alerts.

Commit all scripts to GitHub for reproducibility.

## 📸 Screenshots
<img width="800" src="https://github.com/user-attachments/assets/ad5bfee7-d9f5-40f4-b494-7a45efa083ba" /> <img width="800" src="https://github.com/user-attachments/assets/fd3cb8c1-c2c0-4ef8-9537-a6c419a56764" /> <img width="800" src="https://github.com/user-attachments/assets/e1f3e23a-efd1-4b38-babe-07c4e455d92d" /> ```
