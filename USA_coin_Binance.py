#!/usr/bin/env python3
"""
Binance USA-Origin Coin Watchlist Generator for TradingView
Retrieves and filters active Spot USDT trading pairs for crypto projects
originating from the United States (US-founded, US headquarters, or US core teams).

Version: 1.0.0
"""

import json
import os
import sys
import urllib.request
import urllib.error

__version__ = "1.0.0"

EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
OUTPUT_FILE = "USA_coin_binance.txt"

# Configuration to exclude leveraged tokens
EXCLUDE_LEVERAGED = True
LEVERAGED_KEYWORDS = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]

# Curated registry of US-origin crypto projects
# (Project base ticker -> Metadata details)
USA_COINS_REGISTRY = {
    "BTC": {"name": "Bitcoin", "sector": "Store of Value / Currency", "origin": "US mining dominance, US ETF institutional pioneer"},
    "SOL": {"name": "Solana", "sector": "Layer 1", "origin": "Solana Labs (San Francisco, CA) - Anatoly Yakovenko, Raj Gokal"},
    "XRP": {"name": "XRP / Ripple", "sector": "Payments", "origin": "Ripple Labs (San Francisco, CA) - Brad Garlinghouse, Chris Larsen"},
    "ADA": {"name": "Cardano", "sector": "Layer 1", "origin": "IOG / Charles Hoskinson (Wyoming & Colorado, USA)"},
    "AVAX": {"name": "Avalanche", "sector": "Layer 1", "origin": "Ava Labs (New York, NY) - Emin Gun Sirer (Cornell Univ)"},
    "LINK": {"name": "Chainlink", "sector": "Oracle / Infra", "origin": "Chainlink Labs (San Francisco & NYC) - Sergey Nazarov"},
    "SUI": {"name": "Sui Network", "sector": "Layer 1", "origin": "Mysten Labs (Palo Alto, CA) - ex-Meta/Diem team: Evan Cheng"},
    "APT": {"name": "Aptos", "sector": "Layer 1", "origin": "Aptos Labs (Palo Alto, CA) - ex-Meta/Diem team: Mo Shaikh, Avery Ching"},
    "ARB": {"name": "Arbitrum", "sector": "Layer 2", "origin": "Offchain Labs (Princeton, NJ) - Ed Felten (Princeton/White House)"},
    "OP": {"name": "Optimism", "sector": "Layer 2", "origin": "OP Labs (New York & San Francisco, CA)"},
    "NEAR": {"name": "NEAR Protocol", "sector": "Layer 1", "origin": "NEAR Inc (San Francisco, CA) - Illia Polosukhin (ex-Google AI)"},
    "SEI": {"name": "Sei Network", "sector": "Layer 1", "origin": "Sei Labs (San Francisco, CA) - Jayendra Jog (ex-Robinhood)"},
    "HBAR": {"name": "Hedera", "sector": "Enterprise L1", "origin": "Hedera Hashgraph (Dallas, TX) - Leemon Baird, Mance Harmon"},
    "LTC": {"name": "Litecoin", "sector": "Payments", "origin": "Litecoin Foundation (San Francisco / Florida) - Charlie Lee"},
    "XLM": {"name": "Stellar", "sector": "Payments", "origin": "Stellar Development Foundation (San Francisco, CA) - Jed McCaleb"},
    "ALGO": {"name": "Algorand", "sector": "Layer 1", "origin": "Algorand Foundation (Boston, MA) - Silvio Micali (MIT Turing Award)"},
    "STX": {"name": "Stacks", "sector": "Bitcoin L2", "origin": "Hiro Systems (New York & Princeton, NJ) - Muneeb Ali (SEC Reg A+ qualified)"},
    "EIGEN": {"name": "EigenLayer", "sector": "Restaking", "origin": "EigenLabs (Seattle, WA) - Sreeram Kannan (Univ of Washington)"},
    "FIL": {"name": "Filecoin", "sector": "DePIN / Storage", "origin": "Protocol Labs (San Francisco, CA) - Juan Benet"},
    "UNI": {"name": "Uniswap", "sector": "DeFi DEX", "origin": "Uniswap Labs (New York, NY) - Hayden Adams"},
    "ONDO": {"name": "Ondo Finance", "sector": "RWA", "origin": "Ondo Finance (New York, NY) - Nathan Allman (ex-Goldman Sachs)"},
    "RENDER": {"name": "Render", "sector": "AI / DePIN GPU", "origin": "OTOY Inc (Los Angeles, CA) - Jules Urbach"},
    "DOGE": {"name": "Dogecoin", "sector": "Meme", "origin": "Portland, Oregon - Billy Markus"},
    "AERO": {"name": "Aerodrome Finance", "sector": "DeFi DEX", "origin": "Base Ecosystem / US liquidity hub"},
    "TIA": {"name": "Celestia", "sector": "Modular DA", "origin": "Celestia Labs (USA/Global roots)"},
    "TAO": {"name": "Bittensor", "sector": "AI", "origin": "Opentensor Foundation (USA/Canada) - Jacob Steeves"},
    "WLD": {"name": "Worldcoin", "sector": "Identity / AI", "origin": "Tools for Humanity (San Francisco, CA) - Sam Altman"},
    "ARKM": {"name": "Arkham", "sector": "Analytics / AI", "origin": "Arkham Intelligence (Austin, TX) - Miguel Morel"},
    "GRT": {"name": "The Graph", "sector": "Data Indexing", "origin": "Edge & Node (San Francisco, CA) - Yaniv Tal"},
    "COMP": {"name": "Compound", "sector": "DeFi Lending", "origin": "Compound Labs (San Francisco, CA) - Robert Leshner"},
    "SKY": {"name": "Sky (MakerDAO)", "sector": "DeFi Stablecoin", "origin": "Maker Foundation / US core contributors"},
    "KAVA": {"name": "Kava", "sector": "Layer 1 / DeFi", "origin": "Kava Labs (San Francisco, CA) - Brian Kerr"},
    "BLUR": {"name": "Blur", "sector": "NFT Marketplace", "origin": "San Francisco, CA - Tieshun Roquerre (Pacman)"},
    "PYTH": {"name": "Pyth Network", "sector": "Oracle", "origin": "Douro Labs / Jump Crypto (Chicago & New York)"},
    "JTO": {"name": "Jito", "sector": "Solana MEV / Liquid Staking", "origin": "Jito Labs (Austin, TX / San Francisco)"},
    "ENS": {"name": "Ethereum Name Service", "sector": "Identity", "origin": "True Names LTD / US-aligned foundation"},
    "ICP": {"name": "Internet Computer", "sector": "Compute L1", "origin": "DFINITY Foundation (Palo Alto, CA / Zurich)"},
    "INJ": {"name": "Injective", "sector": "DeFi L1", "origin": "Injective Labs (New York, NY) - Eric Chen, Albert Chon"},
    "DYDX": {"name": "dYdX", "sector": "DeFi Perps", "origin": "dYdX Trading Inc (San Francisco, CA) - Antonio Juliano"},
    "FLOW": {"name": "Flow", "sector": "Gaming / L1", "origin": "Dapper Labs (USA/Canada) - Roham Gharegozlou"},
    "ZEC": {"name": "Zcash", "sector": "Privacy", "origin": "Electric Coin Company (Denver, CO) - Zooko Wilcox"},
    "BAT": {"name": "Basic Attention Token", "sector": "Web3 Browser", "origin": "Brave Software (San Francisco, CA) - Brendan Eich"},
    "AMP": {"name": "Amp / Flexa", "sector": "Payments", "origin": "Flexa Network (New York, NY) - Tyler Spalding"},
    "APE": {"name": "ApeCoin", "sector": "Gaming / NFT", "origin": "Yuga Labs (Miami, FL) - Greg Solano, Wylie Aronow"},
    "AXL": {"name": "Axelar", "sector": "Interoperability", "origin": "Axelar (NYC / Boston) - Sergey Gorbunov (MIT)"},
    "GALA": {"name": "Gala Games", "sector": "Gaming", "origin": "Gala Games (San Francisco, CA) - Eric Schiermeyer (Zynga co-founder)"},
    "STORJ": {"name": "Storj", "sector": "DePIN / Storage", "origin": "Storj Labs (Atlanta, GA) - Shawn Wilkinson, Ben Golub"},
    "AUDIO": {"name": "Audius", "sector": "Music / Streaming", "origin": "Audius (San Francisco, CA) - Roneil Rumburg"},
    "LPT": {"name": "Livepeer", "sector": "Video / DePIN", "origin": "Livepeer Inc (New York, NY) - Doug Petkanics"},
    "NMR": {"name": "Numerai", "sector": "AI / Hedge Fund", "origin": "Numerai (San Francisco, CA) - Richard Craib"},
    "UMA": {"name": "UMA", "sector": "DeFi Oracles", "origin": "Risk Labs (New York, NY) - Hart Lambur, Allison Lu"},
    "RSR": {"name": "Reserve Rights", "sector": "DeFi Stablecoin", "origin": "Reserve (Oakland, CA - backed by Peter Thiel & Sam Altman)"},
    "CELO": {"name": "Celo", "sector": "Layer 1 / Mobile", "origin": "cLabs (San Francisco, CA) - Rene Reinsberg"},
    "MINA": {"name": "Mina Protocol", "sector": "zk-SNARK L1", "origin": "O(1) Labs (San Francisco, CA) - Evan Shapiro"},
    "ROSE": {"name": "Oasis Network", "sector": "Privacy L1", "origin": "Oasis Labs (Berkeley, CA) - Dawn Song (UC Berkeley)"},
    "SKL": {"name": "SKALE", "sector": "Modular L1", "origin": "SKALE Labs (San Francisco, CA) - Jack O'Holleran"},
    "CYBER": {"name": "CyberConnect", "sector": "SocialFi", "origin": "CyberConnect (San Francisco, CA) - Ryan Li, Wilson Wei"},
    "TNSR": {"name": "Tensor", "sector": "NFT Marketplace", "origin": "Tensor (San Francisco, CA)"},
    "OGN": {"name": "Origin Protocol", "sector": "DeFi / Yield", "origin": "Origin Protocol (San Francisco, CA) - Matthew Liu"},
    "DCR": {"name": "Decred", "sector": "PoW/PoS Hybrid", "origin": "Company 0 (Chicago, IL) - Jake Yocom-Piatt"},
    "RVN": {"name": "Ravencoin", "sector": "Asset Issuance", "origin": "Bruce Fenton / Tron Black (Utah, USA)"},
    "CVC": {"name": "Civic", "sector": "Identity", "origin": "Civic Technologies (San Francisco, CA) - Vinny Lingham"},
    "WLFI": {"name": "World Liberty Financial", "sector": "DeFi / Political", "origin": "Trump family DeFi project (USA)"},
    "TRUMP": {"name": "Official Trump", "sector": "Meme / Political", "origin": "US Political movement & brand"},
    "USDC": {"name": "USD Coin", "sector": "Regulated Stablecoin", "origin": "Circle (Boston, MA) - Jeremy Allaire"},
    "USDP": {"name": "Pax Dollar", "sector": "Regulated Stablecoin", "origin": "Paxos Trust Company (New York, NY - NYDFS regulated)"},
    "RLUSD": {"name": "Ripple USD", "sector": "Regulated Stablecoin", "origin": "Ripple Labs (San Francisco, CA)"}
}

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

def filter_usa_coins(exchange_data):
    """Filters active Binance Spot USDT pairs matching US-origin projects."""
    symbols_data = exchange_data.get("symbols", [])
    active_spot_symbols = {}
    
    for s in symbols_data:
        symbol = s.get("symbol", "")
        status = s.get("status", "")
        base_asset = s.get("baseAsset", "")
        quote_asset = s.get("quoteAsset", "")
        
        # We focus on USDT trading pairs
        if quote_asset != "USDT" or status != "TRADING":
            continue
            
        # Exclude leveraged tokens
        if EXCLUDE_LEVERAGED and any(keyword in symbol for keyword in LEVERAGED_KEYWORDS):
            continue
            
        active_spot_symbols[base_asset] = symbol
        
    watchlist = []
    matched_coins = []
    missing_coins = []
    
    for base_ticker, meta in USA_COINS_REGISTRY.items():
        if base_ticker in active_spot_symbols:
            tradingview_symbol = f"BINANCE:{active_spot_symbols[base_ticker]}"
            watchlist.append(tradingview_symbol)
            matched_coins.append((active_spot_symbols[base_ticker], meta))
        else:
            missing_coins.append((base_ticker, meta))
            
    watchlist.sort()
    return watchlist, matched_coins, missing_coins

def main():
    print(f"=== Binance USA-Origin Coin Watchlist Generator v{__version__} ===")
    print(f"Total US-origin projects tracked in registry: {len(USA_COINS_REGISTRY)}")
    
    exchange_data = fetch_exchange_info()
    watchlist, matched_coins, missing_coins = filter_usa_coins(exchange_data)
    
    # Save watchlist to file
    try:
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(watchlist))
        print(f"\n[OK] Successfully wrote {len(watchlist)} pairs to '{OUTPUT_FILE}'.")
    except OSError as e:
        print(f"Error writing to file: {e}", file=sys.stderr)
        sys.exit(1)
        
    print("\n--- Statistics ---")
    print(f"Matched & Active USDT Spot Pairs: {len(watchlist)}")
    print(f"Inactive / Not on USDT Spot:     {len(missing_coins)}")
    
    print("\n--- Sample Matched US Coins on Binance Spot ---")
    for pair, meta in sorted(matched_coins, key=lambda x: x[0])[:15]:
        print(f"  * {pair:<12} | {meta['name']:<22} | {meta['sector']:<18} | {meta['origin']}")
    if len(matched_coins) > 15:
        print(f"  ... and {len(matched_coins) - 15} more pairs.")
        
    print(f"\nYou can now import '{OUTPUT_FILE}' directly into TradingView Watchlist.")

if __name__ == "__main__":
    main()
