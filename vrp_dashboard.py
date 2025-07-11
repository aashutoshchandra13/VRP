import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
from datetime import datetime

# --- Page Config ---
st.set_page_config(page_title="VRP Dashboard", layout="wide")

# --- Title ---
st.title("📊 Volatility Risk Premium (VRP) Dashboard")

# --- DB Connection ---
@st.cache_data
def load_data():
    conn = sqlite3.connect("vrp_repository.db")
    df = pd.read_sql("SELECT * FROM vrp_data", conn, parse_dates=["run_date", "inserted_at"])
    conn.close()
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    return df

df = load_data()

if df.empty:
    st.warning("No data found in database.")
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("🔎 Filter Data")

symbols = df["symbol"].dropna().unique().tolist()
selected_symbol = st.sidebar.selectbox("Select Symbol", symbols)

min_date = df["run_date"].min().date()
max_date = df["run_date"].max().date()

# Fix: Use date_input if only one date exists
if min_date == max_date:
    selected_date = st.sidebar.date_input("Select Run Date", min_date)
else:
    selected_date = st.sidebar.slider("Select Run Date", min_date, max_date, max_date)

vrp_threshold = st.sidebar.slider("Min VRP (10D) %", 0.0, 10.0, 5.0)

# --- Optional: Display available dates and their record counts
with st.sidebar.expander("📆 Available Data Dates"):
    st.dataframe(df['run_date'].value_counts().sort_index())

# --- Filtered Data ---
filtered = df[(df["symbol"] == selected_symbol) & (df["run_date"] == pd.to_datetime(selected_date))]

st.subheader(f"📅 VRP Snapshot on {selected_date} for {selected_symbol}")
st.dataframe(filtered[[
    "expiry", "ltp", "atm_strike", "atm_iv",
    "rv_5d", "vrp_5d", "rv_10d", "vrp_10d", "rv_20d", "vrp_20d"
]].sort_values("expiry"))

# --- High VRP Filter ---
st.subheader(f"🔥 High VRP Opportunities (> {vrp_threshold:.1f}% 10D)")
high_vrp = filtered[filtered["vrp_10d"] > vrp_threshold]
st.dataframe(high_vrp[["expiry", "atm_iv", "rv_10d", "vrp_10d"]].sort_values("vrp_10d", ascending=False))

# --- Historical Trend for a Selected Expiry ---
if not filtered.empty:
    latest_expiry = filtered["expiry"].max()
    trend_df = df[(df["symbol"] == selected_symbol) & (df["expiry"] == latest_expiry)].sort_values("run_date")

    st.subheader(f"📈 VRP 10D Trend for Expiry: {latest_expiry.date()}")

    fig = px.line(trend_df, x="run_date", y=["atm_iv", "rv_10d"],
                  labels={"value": "Volatility (%)", "run_date": "Run Date"},
                  title=f"ATM IV vs RV 10D for {selected_symbol} - {latest_expiry.date()}")

    fig.add_scatter(x=trend_df["run_date"], y=trend_df["vrp_10d"], name="VRP 10D",
                    mode="lines+markers", line=dict(dash="dot"))

    st.plotly_chart(fig, use_container_width=True)

# --- Aggregated VRP Stats ---
st.subheader("📉 Average VRP (10D) by Expiry")
agg = df[df["symbol"] == selected_symbol].groupby("expiry").agg(
    avg_vrp_10d=("vrp_10d", "mean"),
    max_vrp_10d=("vrp_10d", "max"),
    min_vrp_10d=("vrp_10d", "min"),
    count=("vrp_10d", "count")
).reset_index().sort_values("avg_vrp_10d", ascending=False)

st.dataframe(agg)

# --- Footer ---
st.caption("Proprietary Analysis of Aashutosh Chandra")
