import os
import json
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- CONFIGURATION ---
load_dotenv()
API_KEY = os.getenv('ALPHA_VANTAGE_KEY')
TICKERS_FILE = 'tickers.json'

def get_last_recorded_date(category, symbol):
    """Returns the most recent date string (YYYY-MM-DD) from the local JSON file."""
    file_path = f"data/{category}/{symbol}.json"
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            if not data:
                return None
            return sorted(data.keys())[-1]
    except (json.JSONDecodeError, IndexError):
        return None

def is_data_up_to_date(last_date):
    """
    Checks if the local data matches the most recent possible trading day.
    Skips weekends and accounts for the fact that today's data usually 
    isn't final until after market close.
    """
    if not last_date:
        return False
    
    now = datetime.now()
    last_dt = datetime.strptime(last_date, '%Y-%m-%d').date()
    
    # If today is Saturday (5) or Sunday (6), check if we have Friday's data
    if now.weekday() >= 5:
        days_to_friday = (now.weekday() - 4) % 7
        target_date = (now - timedelta(days=days_to_friday)).date()
        return last_dt >= target_date
    
    # If it's a weekday, we check if we have data from at least yesterday.
    # (Fetching 'today' usually only works after market close).
    yesterday = (now - timedelta(days=1)).date()
    return last_dt >= yesterday

def save_to_cache(category, symbol, new_data):
    """Merges new data into individual JSON files without overwriting history."""
    folder = f"data/{category}"
    os.makedirs(folder, exist_ok=True)
    file_path = f"{folder}/{symbol}.json"
    
    cache = {}
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            try:
                cache = json.load(f)
            except:
                cache = {}
    
    added = 0
    for date, values in new_data.items():
        if date not in cache:
            cache[date] = values
            added += 1
            
    with open(file_path, 'w') as f:
        json.dump(dict(sorted(cache.items())), f, indent=4)
    return added

def fetch_data(url, key_name, symbol):
    """Helper to fetch from API with timeout and basic error logging."""
    try:
        response = requests.get(url, timeout=15)
        res_json = response.json()
        
        if key_name in res_json:
            return res_json[key_name]
        
        # Log specific Alpha Vantage errors (Rate limits, etc)
        error_msg = res_json.get('Note') or res_json.get('Information') or res_json.get('Error Message')
        print(f"⚠️ API issue for {symbol}: {error_msg}")
        return None
    except Exception as e:
        print(f"❌ Connection error for {symbol}: {e}")
        return None

def main():
    if not API_KEY:
        print("❌ Error: ALPHA_VANTAGE_KEY environment variable not set.")
        return

    with open(TICKERS_FILE, 'r') as f:
        config = json.load(f)

    # --- 1. STOCKS & INDICES (Using TIME_SERIES_DAILY) ---
    all_equities = [('stocks', s) for s in config.get('stocks', [])] + \
                   [('indices', i) for i in config.get('indices', [])]
    
    for category, symbol in all_equities:
        last_date = get_last_recorded_date(category, symbol)
        if is_data_up_to_date(last_date):
            print(f"⏩ Skipping {symbol}: Already up to date ({last_date}).")
            continue

        print(f"Syncing {symbol}...")
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
        raw = fetch_data(url, "Time Series (Daily)", symbol)
        
        if raw:
            formatted = {
                d: {"o": v["1. open"], "h": v["2. high"], "l": v["3. low"], "c": v["4. close"], "v": v["5. volume"]} 
                for d, v in raw.items()
            }
            save_to_cache(category, symbol, formatted)
            print(f"✅ {symbol} updated.")
            time.sleep(15) # Wait for rate limit

    # --- 2. COMMODITIES (Using global functions) ---
    for comm in config.get('commodities', []):
        last_date = get_last_recorded_date('commodities', comm)
        if is_data_up_to_date(last_date):
            print(f"⏩ Skipping {comm}: Already up to date.")
            continue

        print(f"Syncing {comm}...")
        url = f"https://www.alphavantage.co/query?function={comm}&interval=daily&apikey={API_KEY}"
        raw = fetch_data(url, "data", comm)
        
        if raw:
            formatted = { i["date"]: {"c": i["value"]} for i in raw if i["value"] != "." }
            save_to_cache("commodities", comm, formatted)
            print(f"✅ {comm} updated.")
            time.sleep(15)

    # --- 3. FX (Using FX_DAILY) ---
    for pair in config.get('fx', []):
        pair_id = f"{pair['from']}{pair['to']}"
        last_date = get_last_recorded_date('fx', pair_id)
        if is_data_up_to_date(last_date):
            print(f"⏩ Skipping {pair_id}: Already up to date.")
            continue

        print(f"Syncing {pair_id}...")
        url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={pair['from']}&to_symbol={pair['to']}&apikey={API_KEY}"
        raw = fetch_data(url, "Time Series FX (Daily)", pair_id)
        
        if raw:
            formatted = {
                d: {"o": v["1. open"], "h": v["2. high"], "l": v["3. low"], "c": v["4. close"]} 
                for d, v in raw.items()
            }
            save_to_cache("fx", pair_id, formatted)
            print(f"✅ {pair_id} updated.")
            time.sleep(15)

    print("\n🚀 Database Sync Complete.")

if __name__ == "__main__":
    main()