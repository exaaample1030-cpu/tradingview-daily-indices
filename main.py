import requests
import pandas as pd
from datetime import datetime
import uuid

# --- Metadata ---
RUN_ID = f"run_{uuid.uuid4().hex[:8]}"
TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# TradingView internal JSON endpoint (public)
url = "https://scanner.tradingview.com/indices/scan"

# Payload: request key index fields
payload = {
    "symbols": {"query": {"types": []}, "tickers": []},
    "columns": [
        "name",
        "close",
        "change",
        "change_abs",
        "high",
        "low",
        "description"
    ]
}

headers = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json"
}

# Make request
response = requests.post(url, headers=headers, json=payload)
if response.status_code != 200:
    raise Exception(f"TradingView API request failed: {response.status_code}")

data = response.json().get("data", [])
if not data:
    raise Exception("No data returned from TradingView JSON endpoint.")

records = []
for item in data:
    d = item.get("d", [])
    if len(d) < 6:
        continue
    records.append({
        "Symbol": item.get("s", ""),
        "FullName": d[6] if len(d) > 6 else "",
        "Price": d[1],
        "ChangePct": d[2],
        "ChangeAmount": d[3],
        "DayHigh": d[4],
        "DayLow": d[5]
    })

df = pd.DataFrame(records)
if df.empty:
    raise Exception("Parsed DataFrame is empty.")

# Sort and take top 3 by ChangePct
top3 = df.sort_values("ChangePct", ascending=False).head(3).copy()

# Add metadata
top3.insert(0, "RunID", RUN_ID)
top3.insert(1, "DateTimeUTC", TIMESTAMP)
top3.insert(2, "Rank", range(1, len(top3) + 1))

print(f"\n✅ Top 3 Indices (TradingView JSON API) — {TIMESTAMP}\n")
print(top3.to_string(index=False))

# Save output (so you can view it in workflow logs)
top3.to_csv("top3_indices.csv", index=False)
