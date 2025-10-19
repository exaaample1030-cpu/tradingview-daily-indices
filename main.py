from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime
import uuid
import time

RUN_ID = f"run_{uuid.uuid4().hex[:8]}"
TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.get("https://www.tradingview.com/markets/indices/quotes-all/")
time.sleep(10)

rows = driver.find_elements(By.CSS_SELECTOR, "table.tv-data-table__table tbody tr")
data = []

for row in rows:
    cols = row.find_elements(By.TAG_NAME, "td")
    if len(cols) < 5:
        continue
    try:
        symbol = cols[0].text.strip()
        # Attempt to get full name if present
        fullname_span = cols[0].find_elements(By.TAG_NAME, "span")
        fullname = fullname_span[0].text.strip() if fullname_span else symbol

        def clean_number(text):
            text = text.replace("−", "-").replace("%", "").replace(",", "").replace("+", "").strip()
            try:
                return float(text)
            except:
                return 0.0

        price = clean_number(cols[1].text)
        change_amount = clean_number(cols[3].text)
        change_pct = clean_number(cols[4].text)
        day_high = cols[5].text.strip()
        day_low = cols[6].text.strip()

        data.append({
            "Symbol": symbol,
            "FullName": fullname,
            "Price": price,
            "ChangePct": change_pct,
            "ChangeAmount": change_amount,
            "DayHigh": day_high,
            "DayLow": day_low
        })
    except Exception as e:
        print("Row parse error:", e)
        continue

driver.quit()

df = pd.DataFrame(data)

# Debug: verify columns exist
print("Columns detected:", df.columns.tolist())
if "ChangePct" not in df.columns:
    raise Exception("Column 'ChangePct' not found — scraping failed!")

top3 = df.sort_values("ChangePct", ascending=False).head(3).copy()
top3.insert(0, "RunID", RUN_ID)
top3.insert(1, "DateTimeUTC", TIMESTAMP)
top3.insert(2, "Rank", range(1, len(top3)+1))

print(top3)
top3.to_csv("top3_indices.csv", index=False)
