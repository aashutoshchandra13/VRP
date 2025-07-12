#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sqlite3
from nsepython import oi_chain_builder, index_history, expiry_list

# --- Step 1: Set Parameters Dynamically ---
symbol_index = "NIFTY 50"       # For historical data
symbol_option = "NIFTY"         # For option chain data
run_date = datetime.today().date()

# --- Step 2: Fetch historical index data ---
start_date = (datetime.today() - timedelta(days=30)).strftime('%d-%b-%Y')
end_date = datetime.today().strftime('%d-%b-%Y')

hist_df = index_history(symbol_index, start_date, end_date)
hist_df.rename(columns={"HistoricalDate": "Date"}, inplace=True)
hist_df["CLOSE"] = pd.to_numeric(hist_df["CLOSE"], errors="coerce")
hist_df["Date"] = pd.to_datetime(hist_df["Date"])
hist_df.sort_values("Date", inplace=True)
hist_df["log_return"] = np.log(hist_df["CLOSE"] / hist_df["CLOSE"].shift(1))

# Calculate rolling realized volatilities
hist_df["rv_5d"] = hist_df["log_return"].rolling(5).std() * np.sqrt(252) * 100
hist_df["rv_10d"] = hist_df["log_return"].rolling(10).std() * np.sqrt(252) * 100
hist_df["rv_20d"] = hist_df["log_return"].rolling(20).std() * np.sqrt(252) * 100

rv_5d = hist_df["rv_5d"].iloc[-1]
rv_10d = hist_df["rv_10d"].iloc[-1]
rv_20d = hist_df["rv_20d"].iloc[-1]

# --- Step 3: Get upcoming expiries dynamically ---
expiries = expiry_list(symbol_option)[:3]  # Next 3 expiries

# --- Step 4: Loop through expiries and calculate VRPs ---
results = []

for expiry in expiries:
    try:
        oi_data, ltp, _ = oi_chain_builder(symbol_option, expiry, "full")
        df = pd.DataFrame(oi_data)
        df.rename(columns={"Strike Price": "strikePrice"}, inplace=True)
        df['strikePrice'] = pd.to_numeric(df['strikePrice'], errors='coerce')
        df['distance'] = abs(df['strikePrice'] - ltp)
        atm_row = df.loc[df['distance'].idxmin()]

        atm_iv = np.mean([atm_row["CALLS_IV"], atm_row["PUTS_IV"]])

        results.append({
            "symbol": symbol_option,
            "run_date": run_date,
            "expiry": expiry,
            "ltp": round(ltp, 2),
            "atm_strike": int(atm_row["strikePrice"]),
            "atm_iv": round(atm_iv, 2),
            "rv_5d": round(rv_5d, 2),
            "vrp_5d": round(atm_iv - rv_5d, 2),
            "rv_10d": round(rv_10d, 2),
            "vrp_10d": round(atm_iv - rv_10d, 2),
            "rv_20d": round(rv_20d, 2),
            "vrp_20d": round(atm_iv - rv_20d, 2),
            "inserted_at": datetime.now()
        })

    except Exception as e:
        results.append({
            "symbol": symbol_option,
            "run_date": run_date,
            "expiry": expiry,
            "ltp": None,
            "atm_strike": None,
            "atm_iv": None,
            "rv_5d": None,
            "vrp_5d": None,
            "rv_10d": None,
            "vrp_10d": None,
            "rv_20d": None,
            "vrp_20d": None,
            "inserted_at": datetime.now()
        })
        print(f"Error fetching data for expiry {expiry}: {e}")

# --- Step 5: Store in SQLite ---
conn = sqlite3.connect("vrp_repository.db")
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS vrp_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    run_date DATE,
    expiry TEXT,
    ltp REAL,
    atm_strike INTEGER,
    atm_iv REAL,
    rv_5d REAL,
    vrp_5d REAL,
    rv_10d REAL,
    vrp_10d REAL,
    rv_20d REAL,
    vrp_20d REAL,
    inserted_at TIMESTAMP
);
""")

# Insert data
df = pd.DataFrame(results)
df.to_sql("vrp_data", conn, if_exists="append", index=False)

# Close connection
conn.commit()
conn.close()

print("\n✅ Data successfully stored in SQLite!")
print(df)


# In[ ]:




