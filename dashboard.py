import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# ---------------------------
# 1️⃣ Connect to PostgreSQL
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="postgres",  # your DB name
    user="postgres",
    password="Paused.0"
)

# ---------------------------
# 2️⃣ Fetch fraud alerts
# ---------------------------
@st.cache_data
def load_data():
    query = """
    SELECT fa.alert_id, fa.txn_id, fa.account_id, fa.rule_id, fa.reason,
           fa.severity, fa.score, fa.status, fa.created_at,
           a.balance, a.currency,
           t.amount, t.txn_timestamp, t.channel, t.location, t.merchant_id,
           m.name as merchant_name, m.category as merchant_category
    FROM fraud_alerts fa
    JOIN accounts a ON fa.account_id = a.account_id
    JOIN transactions t ON fa.txn_id = t.txn_id
    LEFT JOIN merchants m ON t.merchant_id = m.merchant_id
    ORDER BY fa.created_at DESC
"""

    df = pd.read_sql(query, conn)
    return df

df = load_data()

# ---------------------------
# 3️⃣ Dashboard Layout
# ---------------------------
st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("💳 Fraud Detection Dashboard")

# --- Summary Metrics ---
st.subheader("Summary Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Alerts", len(df))
col2.metric("High Severity", len(df[df['severity'].isin(['high','critical'])]))
col3.metric("Accounts Affected", df['account_id'].nunique())
col4.metric("Merchants Involved", df['merchant_id'].nunique())

# --- Severity Distribution Chart ---
st.subheader("Alerts by Severity")
fig_severity = px.histogram(df, x='severity', color='severity', title="Alerts by Severity")
st.plotly_chart(fig_severity, use_container_width=True)

# --- Alerts Over Time ---
st.subheader("Alerts Over Time")
df['day'] = pd.to_datetime(df['created_at']).dt.date
alerts_by_day = df.groupby('day').size().reset_index(name='count')
fig_time = px.line(alerts_by_day, x='day', y='count', title="Alerts Over Time")
st.plotly_chart(fig_time, use_container_width=True)

# --- Top Risky Accounts ---
st.subheader("Top Risky Accounts")
top_accounts = df.groupby('account_id')['score'].sum().reset_index().sort_values(by='score', ascending=False).head(10)
st.dataframe(top_accounts)

# --- Recent Alerts Table ---
st.subheader("Recent Alerts")
st.dataframe(df[['alert_id','txn_id','account_id','rule_id','reason','severity','score','status','created_at']])

# --- Optional: Filter by Severity ---
st.subheader("Filter Alerts by Severity")
selected_severity = st.multiselect("Select Severity", options=df['severity'].unique(), default=df['severity'].unique())
filtered_df = df[df['severity'].isin(selected_severity)]
st.dataframe(filtered_df[['alert_id','txn_id','account_id','rule_id','reason','severity','score','status','created_at']])
