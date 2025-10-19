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

time.sleep(10)  # wait for JS table

rows = driver.find_elements(By.CSS_SELECTOR, "table.tv-data-table__table tbody tr")

data = []
for row in rows:
    cols = row.find_elements(By.TAG_NAME, "td")
    if len(cols) < 5:
        continue
    try:
        symbol = cols[0].text.strip()
        fullname = cols[0].find_element(By.TAG_NAME, "span").text if cols[0].find_elements(By.TAG_NAME, "span") else symbol
        price = float(cols[1].text.replace(",", ""))
        change_amount = float(cols[3].text.replace(",", "").replace("−", "-").replace("+",""))
        change_pct = float(cols[4].text.replace("%","").replace("−", "-").replace("+",""))
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
    except:
        continue

driver.quit()

df = pd.DataFrame(data)
top3 = df.sort_values("ChangePct", ascending=False).head(3).copy()
top3.insert(0, "RunID", RUN_ID)
top3.insert(1, "DateTimeUTC", TIMESTAMP)
top3.insert(2, "Rank", range(1, len(top3)+1))

print(top3)
top3.to_csv("top3_indices.csv", index=False)
