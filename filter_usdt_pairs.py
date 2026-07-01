#!/usr/bin/env python3
"""
USDT Pair Watchlist Generator for TradingView
Filters USDT coin pairs on Binance with 24h trading volume >= $1,000,000 USD
and exports them to a TradingView importable watchlist file.

Version: 1.0.0
"""

import json
import sys
import urllib.request
import urllib.error

__version__ = "1.0.0"

API_URL = "https://api.binance.com/api/v3/ticker/24hr"
DEFAULT_VOLUME_THRESHOLD = 1_000_000.0  # $1,000,000 USD
OUTPUT_FILE = "tradingview_watchlist.txt"

def fetch_ticker_data():
    """Fetches 24-hour ticker data from Binance API."""
    print("Fetching ticker data from Binance...")
    req = urllib.request.Request(
        API_URL,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode())
    except urllib.error.URLError as e:
        print(f"Error fetching ticker data: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}", file=sys.stderr)
        sys.exit(1)

def filter_and_format_pairs(tickers, threshold):
    """Filters USDT pairs exceeding the volume threshold, excluding leveraged tokens."""
    watchlist = []
    skipped_leveraged = 0
    low_volume = 0
    other_pairs = 0
    
    # Leveraged tokens suffix patterns to exclude
    leveraged_keywords = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]

    for ticker in tickers:
        symbol = ticker.get("symbol", "")
        
        # Check if it's a USDT pair
        if not symbol.endswith("USDT"):
            other_pairs += 1
            continue
            
        # Filter out leveraged/up/down tokens
        if any(keyword in symbol for keyword in leveraged_keywords):
            skipped_leveraged += 1
            continue
            
        try:
            # quoteVolume is the volume of the quote asset (USDT for *USDT pairs)
            quote_volume = float(ticker.get("quoteVolume", 0))
        except (ValueError, TypeError):
            continue
            
        if quote_volume >= threshold:
            # TradingView import format uses prefix, standard is BINANCE:
            watchlist.append(f"BINANCE:{symbol}")
        else:
            low_volume += 1
            
    watchlist.sort()
    return watchlist, {
        "skipped_leveraged": skipped_leveraged,
        "low_volume": low_volume,
        "other_pairs": other_pairs
    }

def main():
    print(f"Watchlist Generator v{__version__}")
    print(f"Threshold: ${DEFAULT_VOLUME_THRESHOLD:,.2f} USD")
    
    tickers = fetch_ticker_data()
    watchlist, stats = filter_and_format_pairs(tickers, DEFAULT_VOLUME_THRESHOLD)
    
    # Save watchlist to file
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(watchlist))
        print(f"\nSuccessfully wrote {len(watchlist)} pairs to '{OUTPUT_FILE}'.")
    except OSError as e:
        print(f"Error writing to file: {e}", file=sys.stderr)
        sys.exit(1)
        
    print("\n--- Statistics ---")
    print(f"Total matching USDT pairs: {len(watchlist)}")
    print(f"Filtered out (low volume): {stats['low_volume']}")
    print(f"Filtered out (leveraged):  {stats['skipped_leveraged']}")
    print(f"Non-USDT pairs ignored:    {stats['other_pairs']}")

if __name__ == "__main__":
    main()
