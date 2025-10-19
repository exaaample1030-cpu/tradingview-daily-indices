from playwright.sync_api import sync_playwright
import pandas as pd
from datetime import datetime
import uuid

RUN_ID = f"run_{uuid.uuid4().hex[:8]}"
TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

url = "https://www.tradingview.com/markets/indices/quotes-all/"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto(url)
    page.wait_for_selector("table.tv-data-table__table tbody tr", timeout=30000)

    rows = page.query_selector_all("table.tv-data-table__table tbody tr")
    data = []

    for row in rows:
        cols = row.query_selector_all("td")
        if len(cols) < 7:
            continue

        def clean_number(text):
            text = text.replace("−", "-").replace("%", "").replace(",", "").replace("+", "").strip()
            try:
                return float(text)
            except:
                return 0.0

        symbol = cols[0].inner_text().strip()
        fullname = cols[0].inner_text().strip()
        price = clean_number(cols[1].inner_text())
        change_amount = clean_number(cols[3].inner_text())
        change_pct = clean_number(cols[4].inner_text())
        day_high = cols[5].inner_text().strip()
        day_low = cols[6].inner_text().strip()

        data.append({
            "Symbol": symbol,
            "FullName": fullname,
            "Price": price,
            "ChangePct": change_pct,
            "ChangeAmount": change_amount,
            "DayHigh": day_high,
            "DayLow": day_low
        })

    browser.close()

df = pd.DataFrame(data)
top3 = df.sort_values("ChangePct", ascending=False).head(3).copy()
top3.insert(0, "RunID", RUN_ID)
top3.insert(1, "DateTimeUTC", TIMESTAMP)
top3.insert(2, "Rank", range(1, len(top3) + 1))

print(top3)
top3.to_csv("top3_indices.csv", index=False)
