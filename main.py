import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import uuid

# --- Step 1: Setup metadata ---
RUN_ID = f"run_{uuid.uuid4().hex[:8]}"
TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# --- Step 2: Get TradingView Indices page ---
url = "https://www.tradingview.com/markets/indices/quotes-all/"
response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
if response.status_code != 200:
    raise Exception(f"Failed to load TradingView page: {response.status_code}")

# --- Step 3: Parse HTML ---
soup = BeautifulSoup(response.text, "html.parser")

# The table rows contain the data
rows = soup.select("table.tv-data-table__table tbody tr")

data = []
for row in rows:
    cols = row.find_all("td")
    if len(cols) < 6:
        continue
    try:
        symbol = cols[0].get_text(strip=True)
        fullname = cols[0].find("sup")
        if fullname:
            fullname.extract()
        fullname = cols[0].get_text(strip=True)

        price = cols[1].get_text(strip=True)
        change_amount = cols[3].get_text(strip=True)
        change_pct = cols[4].get_text(strip=True).replace("%", "")
        day_high = cols[5].get_text(strip=True)
        day_low = cols[6].get_text(strip=True)

        # Clean numeric values
        change_pct_val = float(change_pct.replace("−", "-").replace("+", "").replace(",", "") or 0)
        price_val = float(price.replace(",", "") or 0)
        change_amount_val = float(change_amount.replace("−", "-").replace("+", "").replace(",", "") or 0)

        data.append({
            "Symbol": symbol,
            "FullName": fullname,
            "Price": price_val,
            "ChangePct": change_pct_val,
            "ChangeAmount": change_amount_val,
            "DayHigh": day_high,
            "DayLow": day_low
        })
    except Exception:
        continue

# --- Step 4: Sort and take Top 3 by % change ---
df = pd.DataFrame(data)
df = df.sort_values("ChangePct", ascending=False).reset_index(drop=True)
top3 = df.head(3).copy()

# --- Step 5: Add Run metadata ---
top3.insert(0, "RunID", RUN_ID)
top3.insert(1, "DateTimeUTC", TIMESTAMP)
top3.insert(2, "Rank", range(1, len(top3) + 1))

# --- Step 6: Print or export ---
print(f"\nTop 3 Indices (TradingView) — {TIMESTAMP}\n")
print(top3.to_string(index=False))

# Optional: Save to CSV or Sheets
# top3.to_csv("top3_indices.csv", index=False)

