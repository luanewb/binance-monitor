#!/usr/bin/env python3
"""
Binance Spot Bid/Ask Spread Watchlist Generator
Finds active Spot USDT pairs where ask and bid differ by at least 0.5%
and 24h volume is at least 1,000,000 USDT,
and exports them in a TradingView importable watchlist format.

Version: 1.0.2
"""

import json
import sys
import urllib.error
import urllib.request

__version__ = "1.0.2"

EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
BOOK_TICKER_URL = "https://api.binance.com/api/v3/ticker/bookTicker"
TICKER_24H_URL = "https://api.binance.com/api/v3/ticker/24hr"
OUTPUT_FILE = "Bid_Ask_Spread_Binance.txt"
MIN_SPREAD_PCT = 0.5
MIN_24H_VOLUME_USDT = 1_000_000.0

EXCLUDE_LEVERAGED = True
LEVERAGED_KEYWORDS = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def fetch_json(url):
    """Fetch JSON from Binance with a browser-like User-Agent."""
    req = urllib.request.Request(url, headers=REQUEST_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode())
    except urllib.error.URLError as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {url}: {e}", file=sys.stderr)
        sys.exit(1)


def fetch_active_usdt_symbols():
    print("Fetching active Spot USDT pairs from Binance exchangeInfo...")
    exchange_data = fetch_json(EXCHANGE_INFO_URL)
    active_symbols = set()
    stats = {
        "total_symbols": len(exchange_data.get("symbols", [])),
        "active_usdt_pairs": 0,
        "leveraged_excluded": 0,
        "inactive_usdt": 0,
        "other_pairs": 0,
    }

    for item in exchange_data.get("symbols", []):
        symbol = item.get("symbol", "")
        status = item.get("status", "")

        if not symbol.endswith("USDT"):
            stats["other_pairs"] += 1
            continue
        if status != "TRADING":
            stats["inactive_usdt"] += 1
            continue
        if EXCLUDE_LEVERAGED and any(keyword in symbol for keyword in LEVERAGED_KEYWORDS):
            stats["leveraged_excluded"] += 1
            continue

        active_symbols.add(symbol)
        stats["active_usdt_pairs"] += 1

    return active_symbols, stats


def fetch_book_tickers():
    print("Fetching live bid/ask prices from Binance bookTicker...")
    data = fetch_json(BOOK_TICKER_URL)
    if not isinstance(data, list):
        print("Unexpected bookTicker response format.", file=sys.stderr)
        sys.exit(1)
    return data


def fetch_24h_quote_volumes():
    print("Fetching 24h quote volume from Binance ticker/24hr...")
    data = fetch_json(TICKER_24H_URL)
    if not isinstance(data, list):
        print("Unexpected ticker/24hr response format.", file=sys.stderr)
        sys.exit(1)

    volumes = {}
    invalid_volumes = 0
    for ticker in data:
        symbol = ticker.get("symbol", "")
        try:
            volumes[symbol] = float(ticker.get("quoteVolume", 0))
        except (TypeError, ValueError):
            invalid_volumes += 1

    return volumes, {"tickers": len(data), "invalid_volumes": invalid_volumes}


def filter_spread_pairs(book_tickers, active_symbols, quote_volumes, min_spread_pct, min_24h_volume):
    results = []
    stats = {
        "book_tickers": len(book_tickers),
        "active_usdt_checked": 0,
        "low_24h_volume": 0,
        "missing_24h_volume": 0,
        "invalid_prices": 0,
        "below_threshold": 0,
    }

    for ticker in book_tickers:
        symbol = ticker.get("symbol", "")
        if symbol not in active_symbols:
            continue

        stats["active_usdt_checked"] += 1
        quote_volume = quote_volumes.get(symbol)
        if quote_volume is None:
            stats["missing_24h_volume"] += 1
            continue
        if quote_volume < min_24h_volume:
            stats["low_24h_volume"] += 1
            continue

        try:
            bid = float(ticker.get("bidPrice", 0))
            ask = float(ticker.get("askPrice", 0))
        except (TypeError, ValueError):
            stats["invalid_prices"] += 1
            continue

        if bid <= 0 or ask <= 0 or ask < bid:
            stats["invalid_prices"] += 1
            continue

        spread_pct = ((ask - bid) / bid) * 100
        if spread_pct >= min_spread_pct:
            results.append({
                "symbol": symbol,
                "bid": bid,
                "ask": ask,
                "spread_pct": spread_pct,
                "quote_volume": quote_volume,
            })
        else:
            stats["below_threshold"] += 1

    results.sort(key=lambda item: (-item["spread_pct"], item["symbol"]))
    return results, stats


def format_watchlist(results):
    return "\n".join(f"BINANCE:{item['symbol']}" for item in results)


def main():
    print(f"Binance Spot Bid/Ask Spread Watchlist Generator v{__version__}")
    print(f"Minimum spread: {MIN_SPREAD_PCT:.2f}%")
    print(f"Minimum 24h volume: ${MIN_24H_VOLUME_USDT:,.2f} USDT")
    print(f"Excluding leveraged tokens: {EXCLUDE_LEVERAGED}")

    active_symbols, symbol_stats = fetch_active_usdt_symbols()
    quote_volumes, volume_stats = fetch_24h_quote_volumes()
    book_tickers = fetch_book_tickers()
    results, spread_stats = filter_spread_pairs(
        book_tickers,
        active_symbols,
        quote_volumes,
        MIN_SPREAD_PCT,
        MIN_24H_VOLUME_USDT,
    )

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(format_watchlist(results))
        print(f"\nSuccessfully wrote {len(results)} TradingView symbols to '{OUTPUT_FILE}'.")
    except OSError as e:
        print(f"Error writing to file: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n--- Statistics ---")
    print(f"Total symbols returned by exchangeInfo: {symbol_stats['total_symbols']}")
    print(f"24h ticker rows returned:              {volume_stats['tickers']}")
    print(f"Active Spot USDT pairs checked:        {spread_stats['active_usdt_checked']}")
    print(f"Pairs with volume >= $1M and spread >= {MIN_SPREAD_PCT:.2f}%: {len(results)}")
    print(f"Pairs below 24h volume threshold:      {spread_stats['low_24h_volume']}")
    print(f"Pairs missing 24h volume data:         {spread_stats['missing_24h_volume']}")
    print(f"Invalid 24h volume rows ignored:       {volume_stats['invalid_volumes']}")
    print(f"Pairs below threshold:                 {spread_stats['below_threshold']}")
    print(f"Invalid bid/ask prices ignored:        {spread_stats['invalid_prices']}")
    if EXCLUDE_LEVERAGED:
        print(f"Leveraged pairs excluded:              {symbol_stats['leveraged_excluded']}")
    print(f"Inactive USDT pairs ignored:           {symbol_stats['inactive_usdt']}")
    print(f"Non-USDT pairs ignored:                {symbol_stats['other_pairs']}")

    if results:
        print("\n--- Top Spread Pairs ---")
        for item in results[:20]:
            print(
                f"BINANCE:{item['symbol']} | "
                f"bid={item['bid']:.12g} ask={item['ask']:.12g} "
                f"spread={item['spread_pct']:.4f}% "
                f"24h_volume=${item['quote_volume']:,.0f}"
            )


if __name__ == "__main__":
    main()
