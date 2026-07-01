#!/usr/bin/env python3
"""
Binance USDT Futures Watchlist Generator for TradingView
Retrieves active USDT-margined futures contracts from Binance USD(S)-M Futures API
and exports them in a format importable by TradingView.

Version: 1.0.0
"""

import json
import sys
import urllib.request
import urllib.error

__version__ = "1.0.0"

EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
OUTPUT_FILE = "Future_Binance.txt"

# Configuration
EXCLUDE_DELIVERY = True       # Exclude quarterly delivery contracts (export only Perpetual contracts)
ADD_PERP_SUFFIX = True        # Add ".P" suffix for Perpetual contracts on TradingView (e.g. BINANCE:BTCUSDT.P)

def fetch_exchange_info():
    """Fetches exchange information from Binance Futures API."""
    print("Fetching active trading pairs from Binance Futures exchangeInfo...")
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
    """Filters active USDT futures pairs and formats them for TradingView."""
    symbols_data = exchange_data.get("symbols", [])
    watchlist = []
    
    stats = {
        "total_symbols": len(symbols_data),
        "active_usdt_futures": 0,
        "perpetuals": 0,
        "delivery_excluded": 0,
        "delivery_included": 0,
        "inactive_usdt": 0,
        "other_pairs": 0
    }
    
    for s in symbols_data:
        symbol = s.get("symbol", "")
        status = s.get("status", "")
        quote_asset = s.get("quoteAsset", "")
        contract_type = s.get("contractType", "")
        
        # Check if it's a USDT pair
        if quote_asset != "USDT":
            stats["other_pairs"] += 1
            continue
            
        # Check status
        if status != "TRADING":
            stats["inactive_usdt"] += 1
            continue
            
        # Distinguish Perpetual and Delivery contracts
        is_perpetual = contract_type == "PERPETUAL"
        
        if not is_perpetual and EXCLUDE_DELIVERY:
            stats["delivery_excluded"] += 1
            continue
        
        if is_perpetual:
            stats["perpetuals"] += 1
            # Perpetual suffix format for TradingView: BINANCE:SYMBOL.P
            suffix = ".P" if ADD_PERP_SUFFIX else ""
            watchlist.append(f"BINANCE:{symbol}{suffix}")
        else:
            stats["delivery_included"] += 1
            # Delivery contracts typically import directly as BINANCE:SYMBOL
            watchlist.append(f"BINANCE:{symbol}")
            
        stats["active_usdt_futures"] += 1
        
    watchlist.sort()
    return watchlist, stats

def main():
    print(f"Binance USDT Futures Watchlist Generator v{__version__}")
    print(f"Exclude quarterly delivery: {EXCLUDE_DELIVERY}")
    print(f"Add '.P' perpetual suffix: {ADD_PERP_SUFFIX}")
    
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
    print(f"Total symbols returned by API:    {stats['total_symbols']}")
    print(f"Active USDT Futures exported:     {stats['active_usdt_futures']}")
    print(f" - Perpetual Contracts:           {stats['perpetuals']}")
    if EXCLUDE_DELIVERY:
        print(f" - Delivery Contracts (Excluded): {stats['delivery_excluded']}")
    else:
        print(f" - Delivery Contracts (Included): {stats['delivery_included']}")
    print(f"Inactive USDT Futures ignored:    {stats['inactive_usdt']}")
    print(f"Non-USDT Futures ignored:         {stats['other_pairs']}")
    print(f"\nYou can now import '{OUTPUT_FILE}' directly into TradingView Watchlist.")

if __name__ == "__main__":
    main()
