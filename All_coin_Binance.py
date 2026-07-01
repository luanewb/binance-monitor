#!/usr/bin/env python3
"""
Binance USDT Watchlist Generator for TradingView
Retrieves all active USDT trading pairs from Binance exchangeInfo
and exports them in a format importable by TradingView.

Version: 1.0.2
"""

import json
import sys
import urllib.request
import urllib.error

__version__ = "1.0.2"

EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
OUTPUT_FILE = "All_coin_binance.txt"

# Configuration to exclude leveraged/UP/DOWN/BULL/BEAR tokens
EXCLUDE_LEVERAGED = True
LEVERAGED_KEYWORDS = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]

def fetch_exchange_info():
    """Fetches exchange information from Binance API."""
    print("Fetching active trading pairs from Binance exchangeInfo...")
    req = urllib.request.Request(
        EXCHANGE_INFO_URL,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode())
    except urllib.error.URLError as e:
        print(f"Error fetching exchange info: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}", file=sys.stderr)
        sys.exit(1)

def filter_and_format_pairs(exchange_data):
    """Filters active USDT pairs and formats them for TradingView."""
    symbols_data = exchange_data.get("symbols", [])
    watchlist = []
    
    stats = {
        "total_symbols": len(symbols_data),
        "active_usdt_pairs": 0,
        "leveraged_excluded": 0,
        "inactive_usdt": 0,
        "other_pairs": 0
    }
    
    for s in symbols_data:
        symbol = s.get("symbol", "")
        status = s.get("status", "")
        
        # Check if it's a USDT pair
        if not symbol.endswith("USDT"):
            stats["other_pairs"] += 1
            continue
            
        # Check status
        if status != "TRADING":
            stats["inactive_usdt"] += 1
            continue
            
        # Check leveraged tokens
        if EXCLUDE_LEVERAGED and any(keyword in symbol for keyword in LEVERAGED_KEYWORDS):
            stats["leveraged_excluded"] += 1
            continue
            
        # Format for TradingView (BINANCE:SYMBOL)
        watchlist.append(f"BINANCE:{symbol}")
        stats["active_usdt_pairs"] += 1
        
    watchlist.sort()
    return watchlist, stats

def main():
    print(f"Binance USDT Watchlist Generator v{__version__}")
    print(f"Excluding leveraged tokens: {EXCLUDE_LEVERAGED}")
    
    exchange_data = fetch_exchange_info()
    watchlist, stats = filter_and_format_pairs(exchange_data)
    
    # Save watchlist to file
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(watchlist))
        print(f"\nSuccessfully wrote {len(watchlist)} pairs to '{OUTPUT_FILE}'.")
    except OSError as e:
        print(f"Error writing to file: {e}", file=sys.stderr)
        sys.exit(1)
        
    print("\n--- Statistics ---")
    print(f"Total symbols returned by API: {stats['total_symbols']}")
    print(f"Active USDT pairs exported:   {stats['active_usdt_pairs']}")
    if EXCLUDE_LEVERAGED:
        print(f"Leveraged pairs excluded:     {stats['leveraged_excluded']}")
    print(f"Inactive USDT pairs ignored:  {stats['inactive_usdt']}")
    print(f"Non-USDT pairs ignored:       {stats['other_pairs']}")
    print(f"\nYou can now import '{OUTPUT_FILE}' directly into TradingView Watchlist.")

if __name__ == "__main__":
    main()
