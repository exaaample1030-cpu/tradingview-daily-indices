#!/usr/bin/env python3
"""
Fetch global indices from Yahoo Finance JSON, compute metrics reliably,
pick Top 3 by % change, and save output CSV with columns:

RunID, DateTimeUTC, Rank, Symbol, FullName, Price, ChangePct, ChangeAmount, DayHigh, DayLow
"""

from datetime import datetime
import requests
import pandas as pd
import time
import uuid
import sys
from urllib.parse import quote_plus

# --- CONFIG ---
SYMBOLS = [
    "^GSPC",  # S&P 500
    "^DJI",   # Dow Jones Industrial Average
    "^IXIC",  # Nasdaq Composite
    "^FTSE",  # FTSE 100
    "^N225",  # Nikkei 225
    "^HSI",   # Hang Seng
    "^GDAXI", # DAX
    "^FCHI",  # CAC 40
    "^SSEC",  # Shanghai Composite
    "^BVSP"   # Bovespa
]
# Yahoo endpoint
BASE_URL = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}"
# Output file
OUT_CSV = "top3_indices.csv"
# HTTP settings
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.5  # seconds, exponential backoff

# --- Helpers ---
def safe_float(x, default=0.0):
    """Convert x to float safely, return default if fails."""
    try:
        if x is None:
            return default
        # If it's a string with commas or special minus sign, normalize
        if isinstance(x, str):
            x = x.strip().replace(",", "").replace("−", "-")
        return float(x)
    except Exception:
        return default

def fetch_with_retries(url):
    """GET with simple retries and backoff. Returns response JSON or raise."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent":"Mozilla/5.0"})
            # Accept 200 only; if 4xx/5xx, raise to retry (but 4xx usually won't succeed)
            if r.status_code == 200:
                return r.json()
            else:
                # raise to trigger retry/backoff path
                r.raise_for_status()
        except Exception as e:
            attempt += 1
            if attempt >= MAX_RETRIES:
                raise
            sleep = BACKOFF_FACTOR * (2 ** (attempt - 1))
            time.sleep(sleep)
    raise RuntimeError("Unreachable")

def build_record(item):
    """
    Given a single 'result' item from Yahoo JSON, extract fields with robust fallbacks.
    Calculation logic:
      - Price = regularMarketPrice (float)
      - ChangeAmount = regularMarketChange (float) OR price - previousClose
      - ChangePct = regularMarketChangePercent (float) OR (ChangeAmount / previousClose) * 100
      - DayHigh/DayLow = regularMarketDayHigh / regularMarketDayLow
    """
    symbol = item.get("symbol", "")
    fullname = item.get("shortName") or item.get("longName") or ""
    price = safe_float(item.get("regularMarketPrice"), 0.0)
    change_amount = None
    change_pct = None
    prev_close = safe_float(item.get("regularMarketPreviousClose"), None)

    # primary sources
    if item.get("regularMarketChange") is not None:
        change_amount = safe_float(item.get("regularMarketChange"), 0.0)
    if item.get("regularMarketChangePercent") is not None:
        # Yahoo provides percent as e.g., 0.52 (which means 0.52%); keep as float percent
        change_pct = safe_float(item.get("regularMarketChangePercent"), None)

    # fallbacks
    if change_amount is None and prev_close is not None:
        # compute change amount = price - prev_close
        change_amount = price - prev_close

    if change_pct is None:
        # compute percent if possible
        if prev_close not in (None, 0.0):
            # (change_amount / prev_close) * 100
            change_pct = (change_amount / prev_close) * 100 if change_amount is not None else 0.0
        else:
            change_pct = 0.0

    day_high = safe_float(item.get("regularMarketDayHigh"), None)
    day_low = safe_float(item.get("regularMarketDayLow"), None)

    # Round numeric values to 2 decimal places for readability
    price = round(price, 2)
    change_amount = round(change_amount, 2) if change_amount is not None else 0.0
    change_pct = round(change_pct, 2) if change_pct is not None else 0.0
    day_high_
