import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import uuid

# --- Metadata ---
RUN_ID = f"run_{uuid.uuid4().hex[:8]}"
TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

url = "https://www.tradingview.com/markets/indices/quotes-all/"
response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
if response.status_code != 200:
    raise Exception(f"Failed to load TradingView page: {response.status_code}")

soup = BeautifulSoup(response.text, "lxml")

# Find the table
table = soup.select_one("table.tv-data-table__table")
if not table:
    raise Exception("Could not find TradingView indices table.")

# Extract header names (normalized)
headers = [th.get_text(strip=True).lower() for th in table.select("thead tr th")]
rows = table.select("tbody tr")

data = []
for row in rows:
    cols = [td.get_text(strip=True) for td in row.select("td")]
    if len(cols) != len(headers):
        continue
    record = dict(zip(headers, cols))
    data.append(record)

df = pd.DataFrame(data)
if df.empty:
    raise Exception("No rows parsed from TradingView table — check page layout.")

# Identify closest column names (flexible)
def find_col(possible_names):
    for name in df.columns:
        for p in possible_names:
            if p in name.lower():
                return name
    return None

symbol_col = find_col(["symbol", "ticker", "name"])
price_col = find_col(["last", "price", "close"])
chg_col = find_col(["change", "chg"])
pct_col = find_col(["%", "percent"])
high_col = find_col(["high"])
low_col = find_col(["low"])

# Safely extract
def get_value(row, col):
    try:
        val = row.get(col, "")
        val = val.replace("−", "-").replace("%", "").replace("+", "").replace(",", "").strip()
        return float(val) if val else 0.0
    except Exception:
        return 0.0

records = []
for _, row in df.iterrows():
    records.append({
        "Symbol": row.get(symbol_col, ""),
        "FullName": row.get(symbol_col, ""),
        "Price": get_value(row, price_col),
        "ChangePct": get_value(row, pct_col),
        "ChangeAmount": get_value(row, chg_col),
        "DayHigh": row.get(high_col, ""),
        "DayLow": row.get(low_col, "")
    })

parsed = pd.DataFrame(records)

if parsed.empty:
    raise Exception("Parsed dataframe empty — no valid trading data found.")

# Sort by % change and take top 3
top3 = parsed.sort_values("ChangePct", ascending=False).head(3).copy()

# Add metadata columns
top3.insert(0, "RunID", RUN_ID)
top3.insert(1, "DateTimeUTC", TIMESTAMP)
top3.insert(2, "Rank", range(1, len(top3) + 1))

print(f"\n✅ Top 3 Indices (TradingView) — {TIMESTAMP}\n")
print(top3.to_string(index=False))

# Optionally save to CSV (for GitHub log archiving)
top3.to_csv("top3_indices.csv", index=False)
