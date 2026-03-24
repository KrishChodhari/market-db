import os
import json
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- CONFIGURATION ---
load_dotenv()
AV_KEY  = os.getenv('ALPHA_VANTAGE_KEY')
TD_KEY  = os.getenv('TWELVE_DATA_KEY')
TICKERS_FILE = 'tickers.json'


# ---------------------------------------------------------------------------
# Trading day helpers
# ---------------------------------------------------------------------------

def get_last_trading_day(reference: datetime = None):
    """
    Returns the most recent completed trading day relative to `reference`.
      Mon → Friday  (−3)
      Sat → Friday  (−1)
      Sun → Friday  (−2)
      Tue–Fri → yesterday (always a weekday)
    """
    if reference is None:
        reference = datetime.now()
    d = reference.date()
    weekday = d.weekday()  # Mon=0 … Sun=6

    if weekday == 0:
        return d - timedelta(days=3)
    elif weekday == 6:
        return d - timedelta(days=2)
    elif weekday == 5:
        return d - timedelta(days=1)
    else:
        return d - timedelta(days=1)


def is_up_to_date(last_date) -> bool:
    if not last_date:
        return False
    return datetime.strptime(last_date, '%Y-%m-%d').date() >= get_last_trading_day()


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def file_id(symbol: str) -> str:
    """Strips '/' so FX symbols like XAU/USD become XAUUSD for file paths."""
    return symbol.replace('/', '')


def get_last_recorded_date(category, symbol):
    """Returns the most recent date string (YYYY-MM-DD) from the local JSON file."""
    file_path = f"data/{category}/{file_id(symbol)}.json"
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return sorted(data.keys())[-1] if data else None
    except (json.JSONDecodeError, IndexError):
        return None


def save_to_cache(category, symbol, new_data):
    """Merges new data into individual JSON files without overwriting history."""
    folder = f"data/{category}"
    os.makedirs(folder, exist_ok=True)
    file_path = f"{folder}/{file_id(symbol)}.json"

    cache = {}
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            try:
                cache = json.load(f)
            except Exception:
                cache = {}

    today = datetime.now().date()
    added = 0
    for date, values in new_data.items():
        # Never save today's or future data — intraday values are incomplete
        if datetime.strptime(date, '%Y-%m-%d').date() >= today:
            continue
        if date not in cache:
            cache[date] = values
            added += 1

    with open(file_path, 'w') as f:
        json.dump(dict(sorted(cache.items())), f, indent=4)
    return added


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch_twelve_data(symbol, category):
    """Fetch daily OHLC from Twelve Data. Returns normalised {date: {o,h,l,c,v?}} or None."""
    url = (
        f"https://api.twelvedata.com/time_series"
        f"?symbol={symbol}&interval=1day&outputsize=5000&apikey={TD_KEY}"
    )
    try:
        res = requests.get(url, timeout=15).json()
        if res.get("status") == "ok":
            formatted = {}
            for v in res["values"]:
                entry = {
                    "o": v["open"],
                    "h": v["high"],
                    "l": v["low"],
                    "c": v["close"],
                }
                if "volume" in v and v["volume"] not in (None, "N/A", ""):
                    entry["v"] = v["volume"]
                formatted[v["datetime"]] = entry
            return formatted
        else:
            msg = res.get("message", "unknown error")
            print(f"⚠️  API issue for {symbol}: {msg[:120]}")
            return None
    except Exception as e:
        print(f"❌ Connection error for {symbol}: {e}")
        return None


def fetch_alpha_vantage(symbol):
    """Fetch daily OHLCV from Alpha Vantage TIME_SERIES_DAILY. Returns normalised dict or None."""
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={AV_KEY}"
    )
    try:
        res = requests.get(url, timeout=15).json()
        key = "Time Series (Daily)"
        if key in res:
            return {
                d: {
                    "o": v["1. open"],
                    "h": v["2. high"],
                    "l": v["3. low"],
                    "c": v["4. close"],
                    "v": v["5. volume"],
                }
                for d, v in res[key].items()
            }
        error_msg = res.get("Note") or res.get("Information") or res.get("Error Message")
        print(f"⚠️  API issue for {symbol}. Keys: {list(res.keys())}")
        if error_msg:
            print(f"   Details: {error_msg[:120]}")
        return None
    except Exception as e:
        print(f"❌ Connection error for {symbol}: {e}")
        return None


# ---------------------------------------------------------------------------
# Pre-flight: build the work queue up front
# ---------------------------------------------------------------------------

def build_work_queue(config):
    """
    Inspects every data file and returns only the symbols that need updating.
    Prints a summary before any network request is made.
    """
    queue   = []
    skipped = []
    target  = get_last_trading_day()

    def check(category, symbol, source):
        last_date = get_last_recorded_date(category, symbol)
        if last_date and datetime.strptime(last_date, '%Y-%m-%d').date() >= target:
            skipped.append(f"{symbol} ({last_date})")
        else:
            queue.append({
                'category':  category,
                'symbol':    symbol,
                'last_date': last_date,
                'source':    source,   # 'twelve_data' | 'alpha_vantage'
            })

    # Stocks — Twelve Data
    for symbol in config.get('stocks', []):
        check('stocks', symbol, 'twelve_data')

    # Indices — Twelve Data except NIFTYBEES.BSE (Alpha Vantage)
    for symbol in config.get('indices', []):
        source = 'alpha_vantage' if symbol == 'NIFTYBEES.BSE' else 'twelve_data'
        check('indices', symbol, source)

    # FX — Twelve Data (includes XAU/USD)
    for pair in config.get('fx', []):
        pair_id = f"{pair['from']}/{pair['to']}"   # e.g. XAU/USD, USD/INR
        check('fx', pair_id, 'twelve_data')

    print(f"📋 Pre-flight check — target date: {target}")
    print(f"   ✅ Already up to date ({len(skipped)}): {', '.join(skipped) or 'none'}")
    print(f"   🔄 Needs update ({len(queue)}):      {', '.join(q['symbol'] for q in queue) or 'none'}")
    print()

    return queue


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not AV_KEY:
        print("❌ Error: ALPHA_VANTAGE_KEY not set in .env")
        return
    if not TD_KEY:
        print("❌ Error: TWELVE_DATA_KEY not set in .env")
        return

    with open(TICKERS_FILE, 'r') as f:
        config = json.load(f)

    queue = build_work_queue(config)

    if not queue:
        print("🚀 Everything is up to date. Nothing to fetch.")
        return

    for i, job in enumerate(queue):
        symbol   = job['symbol']
        category = job['category']
        source   = job['source']

        print(f"Syncing {symbol} (via {source.replace('_', ' ')})...")

        if source == 'twelve_data':
            formatted = fetch_twelve_data(symbol, category)
        else:
            formatted = fetch_alpha_vantage(symbol)

        if formatted:
            added = save_to_cache(category, symbol, formatted)
            print(f"✅ {symbol} updated (+{added} new rows).")
        else:
            print(f"⚠️  {symbol} — no data returned, skipping.")

        # Rate-limit pause between requests (skip after the last one)
        if i < len(queue) - 1:
            time.sleep(10)

    print("\n🚀 Database Sync Complete.")


if __name__ == "__main__":
    main()
