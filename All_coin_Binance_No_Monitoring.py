#!/usr/bin/env python3
"""
Binance USDT Watchlist Generator for TradingView, excluding Monitoring-tagged coins.
Retrieves active Spot USDT trading pairs from Binance and exports them in a format
importable by TradingView.

Version: 1.0.0
"""

import json
import sys
import urllib.error
import urllib.request

__version__ = "1.0.0"

EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
PRODUCTS_URL = "https://www.binance.com/bapi/asset/v2/public/asset-service/product/get-products?includeEtf=true"
OUTPUT_FILE = "All_coin_binance_no_monitoring.txt"

EXCLUDE_LEVERAGED = True
LEVERAGED_KEYWORDS = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]
MONITORING_TAG = "monitoring"
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def fetch_json(url):
    req = urllib.request.Request(url, headers=REQUEST_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return json.loads(response.read().decode())
    except urllib.error.URLError as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {url}: {e}", file=sys.stderr)
        sys.exit(1)


def fetch_exchange_info():
    print("Fetching active Spot pairs from Binance exchangeInfo...")
    data = fetch_json(EXCHANGE_INFO_URL)
    if not isinstance(data.get("symbols"), list):
        print("Unexpected exchangeInfo response format.", file=sys.stderr)
        sys.exit(1)
    return data


def fetch_product_tags():
    print("Fetching Binance product tags for Monitoring filter...")
    data = fetch_json(PRODUCTS_URL)
    products = data.get("data")
    if data.get("success") is not True or not isinstance(products, list):
        print("Unexpected Binance products response format.", file=sys.stderr)
        sys.exit(1)

    tag_map = {}
    for product in products:
        symbol = product.get("s", "")
        tags = product.get("tags") or []
        if symbol:
            tag_map[symbol] = {str(tag).strip().lower() for tag in tags}

    if not tag_map:
        print("Binance products response did not include any symbol tags.", file=sys.stderr)
        sys.exit(1)
    return tag_map


def filter_and_format_pairs(exchange_data, product_tags):
    symbols_data = exchange_data.get("symbols", [])
    watchlist = []
    monitoring_symbols = []
    missing_product_tags = []

    stats = {
        "total_symbols": len(symbols_data),
        "active_usdt_pairs": 0,
        "monitoring_excluded": 0,
        "leveraged_excluded": 0,
        "inactive_usdt": 0,
        "other_pairs": 0,
    }

    for item in symbols_data:
        symbol = item.get("symbol", "")
        status = item.get("status", "")
        quote_asset = item.get("quoteAsset", "")

        if quote_asset != "USDT" and not symbol.endswith("USDT"):
            stats["other_pairs"] += 1
            continue
        if status != "TRADING":
            stats["inactive_usdt"] += 1
            continue
        if EXCLUDE_LEVERAGED and any(keyword in symbol for keyword in LEVERAGED_KEYWORDS):
            stats["leveraged_excluded"] += 1
            continue

        tags = product_tags.get(symbol)
        if tags is None:
            missing_product_tags.append(symbol)
        elif MONITORING_TAG in tags:
            stats["monitoring_excluded"] += 1
            monitoring_symbols.append(symbol)
            continue

        watchlist.append(f"BINANCE:{symbol}")
        stats["active_usdt_pairs"] += 1

    watchlist.sort()
    monitoring_symbols.sort()
    missing_product_tags.sort()
    return watchlist, stats, monitoring_symbols, missing_product_tags


def main():
    print(f"Binance Spot USDT Watchlist Without Monitoring Tag v{__version__}")
    print(f"Excluding leveraged tokens: {EXCLUDE_LEVERAGED}")
    print("Excluding symbols whose Binance product tags include: Monitoring")

    exchange_data = fetch_exchange_info()
    product_tags = fetch_product_tags()
    watchlist, stats, monitoring_symbols, missing_product_tags = filter_and_format_pairs(exchange_data, product_tags)

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(watchlist))
        print(f"\nSuccessfully wrote {len(watchlist)} pairs to '{OUTPUT_FILE}'.")
    except OSError as e:
        print(f"Error writing to file: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n--- Statistics ---")
    print(f"Total symbols returned by exchangeInfo: {stats['total_symbols']}")
    print(f"Active Spot USDT pairs exported:        {stats['active_usdt_pairs']}")
    print(f"Monitoring-tagged pairs excluded:       {stats['monitoring_excluded']}")
    if EXCLUDE_LEVERAGED:
        print(f"Leveraged pairs excluded:               {stats['leveraged_excluded']}")
    print(f"Inactive USDT pairs ignored:            {stats['inactive_usdt']}")
    print(f"Non-USDT pairs ignored:                 {stats['other_pairs']}")
    print(f"Pairs missing product tag data:         {len(missing_product_tags)}")

    if monitoring_symbols:
        print("\n--- Excluded Monitoring Symbols ---")
        for symbol in monitoring_symbols:
            print(f"BINANCE:{symbol}")

    if missing_product_tags:
        print("\n--- Symbols Included Without Product Tag Data ---")
        for symbol in missing_product_tags[:50]:
            print(f"BINANCE:{symbol}")
        if len(missing_product_tags) > 50:
            print(f"... and {len(missing_product_tags) - 50} more")

    print(f"\nYou can now import '{OUTPUT_FILE}' directly into TradingView Watchlist.")


if __name__ == "__main__":
    main()
