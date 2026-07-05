#!/usr/bin/env python3
"""
Bin Spot Monitor & Watchlist Web Dashboard
Provides a web interface to control the Binance Spot H1 anomaly detector
and run/manage the 3 existing watchlist scripts.

Version: 2.5.15
"""

import asyncio
import json
import os
import sys
import logging
import time
import re
import aiohttp
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Add local path to import binance_monitor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from binance_monitor import BinanceSpotMonitor, CONFIG_FILE, ALERTS_FILE, VERSION, seconds_until_next_m5_close

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Logger Setup
logger = logging.getLogger("Dashboard")

app = FastAPI(title="Binance Spot Monitor Dashboard", version=VERSION)

# Bot Instance
monitor_instance = BinanceSpotMonitor()
monitor_task: Optional[asyncio.Task] = None
m5_monitor_task: Optional[asyncio.Task] = None

# Script status tracker
script_processes = {
    "1m_vol_watchlist": {"status": "idle", "file": "1m_vol_watchlist.py", "output_txt": "1m_vol_watchlist.txt", "last_run": None},
    "All_coin_Binance": {"status": "idle", "file": "All_coin_Binance.py", "output_txt": "All_coin_binance.txt", "last_run": None},
    "Future_Binance": {"status": "idle", "file": "Future_Binance.py", "output_txt": "Future_Binance.txt", "last_run": None}
}

# Pydantic models for request bodies
class ConfigModel(BaseModel):
    telegram_token: str
    telegram_chat_id: str
    price_threshold_pct: float
    volume_multiplier: float
    scan_interval_sec: int
    volume_avg_period: int
    min_24h_volume: float
    min_h1_pump_volume: float = 1000000.0
    m5_d1_pump_enabled: bool = True
    m5_price_threshold_pct: float = 10.0
    m5_d1_volume_multiplier: float = 1.0
    m5_d1_scan_interval_sec: int = 300

# Background loop for Spot Monitor Bot
async def monitor_loop():
    logger.info("Binance Monitor loop started in background.")
    while True:
        try:
            # Load config from disk in case it changed
            monitor_instance.load_config()
            is_running = monitor_instance.config.get("is_running", False)
            scan_interval = monitor_instance.config.get("scan_interval_sec", 300)
            
            if is_running:
                logger.info("Triggering Spot Scan...")
                await monitor_instance.scan_all_symbols()
            
            # Calculate sleep duration
            if is_running and scan_interval == 3600:
                # Align to next H1 candle close (hour boundary)
                now = datetime.utcnow()
                seconds_to_next_hour = 3600 - (now.minute * 60 + now.second)
                # Add 10s delay to ensure candle is finalized on Binance side
                sleep_duration = seconds_to_next_hour + 10
                logger.info(f"H1 Close alignment active: next scan scheduled in {sleep_duration}s (at next hour + 10s)")
            else:
                sleep_duration = scan_interval if is_running else 10  # Sleep 10s if inactive to avoid CPU-hogging
            
            # Sleep second-by-second and watch for state changes
            for _ in range(max(1, int(sleep_duration))):
                await asyncio.sleep(1)
                current_active = monitor_instance.config.get("is_running", False)
                current_interval = monitor_instance.config.get("scan_interval_sec", 300)
                # Break immediately if status or interval changes
                if current_active != is_running or current_interval != scan_interval:
                    break
        except asyncio.CancelledError:
            logger.info("Monitor loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in monitor loop: {e}")
            await asyncio.sleep(10)

# Background loop for M5 pump vs previous D1 volume bot
async def m5_monitor_loop():
    logger.info("M5/D1 pump monitor loop started in background.")
    while True:
        try:
            monitor_instance.load_config()
            is_running = monitor_instance.config.get("is_running", False)
            is_enabled = monitor_instance.config.get("m5_d1_pump_enabled", True)

            if is_running and is_enabled:
                logger.info("Triggering M5/D1 Pump Scan...")
                await monitor_instance.scan_m5_d1_pump_symbols()
                sleep_duration = seconds_until_next_m5_close()
                logger.info(f"M5 close alignment active: next scan scheduled in {sleep_duration}s (at next 5m close + 10s)")
            else:
                sleep_duration = 10

            for _ in range(max(1, int(sleep_duration))):
                await asyncio.sleep(1)
                current_active = monitor_instance.config.get("is_running", False)
                current_enabled = monitor_instance.config.get("m5_d1_pump_enabled", True)
                if current_active != is_running or current_enabled != is_enabled:
                    break
        except asyncio.CancelledError:
            logger.info("M5/D1 pump monitor loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in M5/D1 pump monitor loop: {e}")
            await asyncio.sleep(10)

@app.on_event("startup")
async def startup_event():
    global monitor_task, m5_monitor_task
    # Start monitor task in background
    monitor_task = asyncio.create_task(monitor_loop())
    m5_monitor_task = asyncio.create_task(m5_monitor_loop())
    logger.info("FastAPI dashboard started.")

@app.on_event("shutdown")
async def shutdown_event():
    global monitor_task, m5_monitor_task
    if monitor_task:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    if m5_monitor_task:
        m5_monitor_task.cancel()
        try:
            await m5_monitor_task
        except asyncio.CancelledError:
            pass
    logger.info("FastAPI dashboard stopped.")

# Serve Front-end HTML
@app.get("/", response_class=HTMLResponse)
async def get_index():
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard HTML template not found.</h1>", status_code=404)

# CoinCap/CoinGecko Market Cap Cache
COINCAP_CACHE = {
    "data": {},
    "last_fetched": 0
}

MARKET_CAP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

COINGECKO_ID_OVERRIDES = {
    "NOM": "nomina"
}
AMBIGUOUS_MARKET_CAP_SYMBOLS = set(COINGECKO_ID_OVERRIDES.keys())

def _store_market_cap(market_caps: dict, symbol: str, market_cap) -> bool:
    sym = (symbol or "").upper().strip()
    if not sym:
        return False
    try:
        mcap = float(market_cap or 0)
    except (ValueError, TypeError):
        return False
    if mcap <= 0:
        return False
    if market_caps.get(sym, 0.0) <= 0:
        market_caps[sym] = mcap
        return True
    return False

async def _merge_coingecko_marketcaps(session: aiohttp.ClientSession, market_caps: dict, pages: int = 4) -> int:
    added = 0
    for page in range(1, pages + 1):
        url = (
            "https://api.coingecko.com/api/v3/coins/markets"
            f"?vs_currency=usd&order=market_cap_desc&per_page=250&page={page}"
        )
        try:
            async with session.get(url, headers=MARKET_CAP_HEADERS, timeout=10) as resp:
                if resp.status != 200:
                    logger.warning(f"CoinGecko markets page {page} returned status {resp.status}.")
                    break
                for asset in await resp.json():
                    if _store_market_cap(market_caps, asset.get("symbol"), asset.get("market_cap")):
                        added += 1
        except Exception as e:
            logger.warning(f"Error fetching CoinGecko markets page {page}: {e}")
            break
    return added

async def _merge_coincap_marketcaps(session: aiohttp.ClientSession, market_caps: dict) -> int:
    added = 0
    try:
        url = "https://api.coincap.io/v2/assets?limit=2000"
        async with session.get(url, headers=MARKET_CAP_HEADERS, timeout=10) as resp:
            if resp.status != 200:
                logger.warning(f"CoinCap assets returned status {resp.status}.")
                return added
            for asset in (await resp.json()).get("data", []):
                if _store_market_cap(market_caps, asset.get("symbol"), asset.get("marketCapUsd")):
                    added += 1
    except Exception as e:
        logger.warning(f"Error fetching CoinCap market caps: {e}")
    return added

async def _merge_coinpaprika_marketcaps(session: aiohttp.ClientSession, market_caps: dict, symbols: Optional[set] = None) -> int:
    added = 0
    wanted = {s.upper() for s in symbols} if symbols else None
    try:
        url = "https://api.coinpaprika.com/v1/tickers?quotes=USD"
        async with session.get(url, headers=MARKET_CAP_HEADERS, timeout=20) as resp:
            if resp.status != 200:
                logger.warning(f"CoinPaprika tickers returned status {resp.status}.")
                return added
            for asset in await resp.json():
                sym = (asset.get("symbol") or "").upper()
                if wanted and sym not in wanted:
                    continue
                market_cap = asset.get("quotes", {}).get("USD", {}).get("market_cap")
                if _store_market_cap(market_caps, sym, market_cap):
                    added += 1
                if wanted and wanted.issubset({k for k, v in market_caps.items() if v > 0}):
                    break
    except Exception as e:
        logger.warning(f"Error fetching CoinPaprika market caps: {e}")
    return added

async def _merge_coinlore_marketcaps(session: aiohttp.ClientSession, market_caps: dict, symbols: set, max_start: int = 5000) -> int:
    added = 0
    wanted = {s.upper() for s in symbols if s}
    if not wanted:
        return added

    found = {s for s in wanted if market_caps.get(s, 0.0) > 0}
    for start in range(0, max_start + 1, 100):
        if wanted.issubset(found):
            break
        try:
            url = f"https://api.coinlore.net/api/tickers/?start={start}&limit=100"
            async with session.get(url, headers=MARKET_CAP_HEADERS, timeout=10) as resp:
                if resp.status != 200:
                    logger.warning(f"CoinLore tickers start {start} returned status {resp.status}.")
                    break
                data = (await resp.json()).get("data", [])
        except Exception as e:
            logger.warning(f"Error fetching CoinLore market caps start {start}: {e}")
            break

        if not data:
            break
        for asset in data:
            sym = (asset.get("symbol") or "").upper()
            if sym not in wanted:
                continue
            if _store_market_cap(market_caps, sym, asset.get("market_cap_usd")):
                added += 1
            if market_caps.get(sym, 0.0) > 0:
                found.add(sym)
    return added

async def _fetch_coingecko_symbol_marketcaps(session: aiohttp.ClientSession, symbols: set) -> dict:
    requested_symbols = {s.upper() for s in symbols if s}
    id_by_symbol = {
        symbol: COINGECKO_ID_OVERRIDES[symbol]
        for symbol in requested_symbols
        if symbol in COINGECKO_ID_OVERRIDES
    }
    semaphore = asyncio.Semaphore(4)

    async def search_symbol(symbol: str):
        async with semaphore:
            try:
                async with session.get(
                    "https://api.coingecko.com/api/v3/search",
                    params={"query": symbol},
                    headers=MARKET_CAP_HEADERS,
                    timeout=10
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"CoinGecko search for {symbol} returned status {resp.status}.")
                        return
                    coins = (await resp.json()).get("coins", [])
            except Exception as e:
                logger.warning(f"Error searching CoinGecko market cap for {symbol}: {e}")
                return

        exact_matches = [
            coin for coin in coins
            if (coin.get("symbol") or "").upper() == symbol
        ]
        ranked = sorted(
            exact_matches,
            key=lambda coin: coin.get("market_cap_rank") or 10**9
        )
        if ranked:
            id_by_symbol[symbol] = ranked[0].get("id")

    search_symbols = requested_symbols - set(id_by_symbol.keys())
    await asyncio.gather(*(search_symbol(symbol) for symbol in search_symbols))
    ids = [coin_id for coin_id in id_by_symbol.values() if coin_id]
    if not ids:
        return {}

    market_caps = {}
    for attempt in range(2):
        try:
            async with session.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={"vs_currency": "usd", "ids": ",".join(ids), "per_page": len(ids)},
                headers=MARKET_CAP_HEADERS,
                timeout=15
            ) as resp:
                if resp.status == 429 and attempt == 0:
                    await asyncio.sleep(2)
                    continue
                if resp.status != 200:
                    logger.warning(f"CoinGecko exact-symbol markets returned status {resp.status}.")
                    return {}
                for asset in await resp.json():
                    _store_market_cap(market_caps, asset.get("symbol"), asset.get("market_cap"))
                break
        except Exception as e:
            logger.warning(f"Error fetching CoinGecko exact-symbol market caps: {e}")
            break
    return market_caps

async def fetch_missing_marketcaps(symbols: set) -> dict:
    missing = {s.upper() for s in symbols if s}
    if not missing:
        return {}

    market_caps = {}
    async with aiohttp.ClientSession() as session:
        market_caps.update(await _fetch_coingecko_symbol_marketcaps(session, missing))
        missing = {s for s in missing if market_caps.get(s, 0.0) <= 0}
        coinlore_symbols = missing - AMBIGUOUS_MARKET_CAP_SYMBOLS
        await _merge_coinlore_marketcaps(session, market_caps, coinlore_symbols)
        still_missing = {s for s in missing if market_caps.get(s, 0.0) <= 0}
        await _merge_coinpaprika_marketcaps(session, market_caps, still_missing)
        still_missing = {s for s in missing if market_caps.get(s, 0.0) <= 0}
        if still_missing:
            logger.warning(f"Market cap still missing for symbols: {sorted(still_missing)}")
    return market_caps

async def fetch_coincap_marketcaps():
    now = time.time()
    # Cache for 5 minutes (300 seconds)
    if now - COINCAP_CACHE["last_fetched"] < 300 and COINCAP_CACHE["data"]:
        return COINCAP_CACHE["data"]
    
    new_data = {}
    async with aiohttp.ClientSession() as session:
        coingecko_added = await _merge_coingecko_marketcaps(session, new_data)
        coincap_added = await _merge_coincap_marketcaps(session, new_data)
        coinpaprika_added = await _merge_coinpaprika_marketcaps(session, new_data)

    if new_data:
        COINCAP_CACHE["data"] = new_data
        COINCAP_CACHE["last_fetched"] = now
        logger.info(
            "Fetched market cap data: "
            f"CoinGecko +{coingecko_added}, CoinCap +{coincap_added}, "
            f"CoinPaprika +{coinpaprika_added}, total {len(new_data)} symbols."
        )

    return COINCAP_CACHE["data"]

# Binance Delisting Announcements Cache
DELISTING_CACHE = {
    "data": None,
    "last_fetched": 0
}

async def fetch_binance_delistings():
    now = time.time()
    # Cache for 10 minutes (600 seconds)
    if now - DELISTING_CACHE["last_fetched"] < 600 and DELISTING_CACHE["data"] is not None:
        return DELISTING_CACHE["data"]
        
    url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    params = {
        "type": 1,
        "catalogId": 161,
        "pageNo": 1,
        "pageSize": 20
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    if res_json.get("code") == "000000":
                        catalogs = res_json.get("data", {}).get("catalogs", [])
                        articles = []
                        if catalogs:
                            articles = catalogs[0].get("articles", [])
                        DELISTING_CACHE["data"] = articles
                        DELISTING_CACHE["last_fetched"] = now
                        logger.info("Fetched Binance delisting announcements successfully.")
                        return articles
                    else:
                        logger.error(f"Binance BAPI returned error code: {res_json.get('code')}")
                else:
                    logger.error(f"Binance BAPI returned status {resp.status}")
    except Exception as e:
        logger.error(f"Error fetching Binance delistings: {e}")
        
    return DELISTING_CACHE["data"] or []

def extract_text_from_node(node):
    if not node:
        return ""
    text = ""
    if node.get("node") == "text":
        text += node.get("text", "")
    
    if "child" in node:
        for child in node["child"]:
            text += extract_text_from_node(child)
            
    if node.get("node") == "element" and node.get("tag") in ["p", "h1", "h2", "h3", "h4", "div", "li", "tr", "br"]:
        text += "\n"
    return text

def parse_article_body(body_str):
    if not body_str:
        return ""
    try:
        body_data = json.loads(body_str)
        text = extract_text_from_node(body_data)
        text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        return text
    except Exception as e:
        logger.error(f"Error parsing article body JSON: {e}")
        return body_str

# REST APIs

@app.get("/api/top_gainers")
async def get_top_gainers():
    try:
        # 1. Fetch market cap data from CoinCap/CoinGecko cache
        market_caps = await fetch_coincap_marketcaps()
        
        # 2. Fetch exchangeInfo and 24h ticker info from Binance in parallel
        exchange_info_url = "https://api.binance.com/api/v3/exchangeInfo"
        ticker_url = "https://api.binance.com/api/v3/ticker/24hr"
        
        async with aiohttp.ClientSession() as session:
            async def fetch_json(url):
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.error(f"Failed to fetch {url}: status {resp.status}")
                        return None
            
            exchange_data, tickers = await asyncio.gather(
                fetch_json(exchange_info_url),
                fetch_json(ticker_url)
            )
            
        if not exchange_data or not tickers:
            raise HTTPException(status_code=500, detail="Failed to fetch complete data from Binance API.")
            
        # Get active delisting coins
        delisting_coins = {}
        try:
            delist_articles = await fetch_binance_delistings()
            now_utc = datetime.now(timezone.utc)
            for art in delist_articles:
                title = art.get("title", "")
                match = re.search(r"Binance Will Delist (.*?) on (\d{4}-\d{2}-\d{2})", title, re.IGNORECASE)
                if match:
                    coins_str = match.group(1)
                    date_str = match.group(2)
                    try:
                        delist_time_utc = datetime.strptime(f"{date_str} 03:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    except Exception:
                        continue
                    if delist_time_utc > now_utc:
                        raw_coins = re.split(r',|\band\b', coins_str)
                        for c in raw_coins:
                            c_clean = c.strip().upper()
                            if c_clean:
                                vn_tz = timezone(timedelta(hours=7))
                                delist_time_vn = delist_time_utc.astimezone(vn_tz)
                                delisting_coins[c_clean] = delist_time_vn.strftime("%d/%m/%Y")
        except Exception as e:
            logger.error(f"Error parsing delisting coins for top gainers: {e}")
            
        # 3. Create a set of active symbols (trading status must be 'TRADING')
        active_symbols = set()
        for s in exchange_data.get("symbols", []):
            if s.get("status", "") == "TRADING":
                active_symbols.add(s.get("symbol", ""))
        
        # 4. Filter USDT pairs that are active and not leveraged
        leveraged_keywords = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]
        valid_tickers = []
        for t in tickers:
            symbol = t.get("symbol", "")
            # Only process if it is currently trading, ends with USDT and is not a leveraged pair
            if symbol in active_symbols and symbol.endswith("USDT") and not any(kw in symbol for kw in leveraged_keywords):
                # Clean symbol like LDOUSDT -> LDO to map with CoinCap/CoinGecko
                base_symbol = symbol[:-4]
                try:
                    price_change_pct = float(t.get("priceChangePercent", 0.0))
                    last_price = float(t.get("lastPrice", 0.0))
                    quote_volume = float(t.get("quoteVolume", 0.0))
                except (ValueError, TypeError):
                    continue
                
                mcap = market_caps.get(base_symbol, 0.0)
                is_delisting = base_symbol in delisting_coins
                delist_date = delisting_coins.get(base_symbol) if is_delisting else None
                
                valid_tickers.append({
                    "symbol": symbol,
                    "base_symbol": base_symbol,
                    "price_change_pct": price_change_pct,
                    "last_price": last_price,
                    "quote_volume": quote_volume,
                    "market_cap": mcap,
                    "is_delisting": is_delisting,
                    "delist_date": delist_date
                })
        
        # 5. Sort by price_change_pct descending and pick top 20
        valid_tickers.sort(key=lambda x: x["price_change_pct"], reverse=True)
        top_20 = valid_tickers[:20]

        missing_mcap_symbols = {
            item["base_symbol"] for item in top_20
            if item.get("market_cap", 0.0) <= 0
        }
        if missing_mcap_symbols:
            top_20_market_caps = await fetch_missing_marketcaps(missing_mcap_symbols)
            market_caps.update(top_20_market_caps)
            for item in top_20:
                item["market_cap"] = market_caps.get(item["base_symbol"], item["market_cap"])
        
        return top_20
    except Exception as e:
        logger.error(f"Error getting top gainers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/explain_gainer/{symbol}")
async def explain_gainer(symbol: str):
    # Read API key from local ignored file or environment variable
    gemini_key = ""
    key_file = os.path.join(BASE_DIR, "gemini_key.txt")
    if os.path.exists(key_file):
        try:
            with open(key_file, "r", encoding="utf-8") as f:
                gemini_key = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading gemini_key.txt: {e}")
            
    if not gemini_key:
        gemini_key = os.environ.get("GEMINI_API_KEY", "")
        
    if not gemini_key:
        raise HTTPException(status_code=500, detail="Gemini API Key is not configured. Please create gemini_key.txt.")
    
    # We clean the symbol if needed (e.g. SOLUSDT -> SOL)
    base_symbol = symbol.replace("USDT", "")
    
    # Constructing prompt
    prompt = (
        f"Hãy phân tích và giải thích chi tiết lý do tại sao đồng tiền mã hóa {base_symbol} "
        f"(giao dịch trên sàn Binance Spot với cặp {symbol}) đang tăng giá mạnh trong 24 giờ qua.\n"
        f"Sử dụng công cụ Google Search được tích hợp để tìm kiếm và tổng hợp các tin tức mới nhất, "
        f"sự kiện vĩ mô, nâng cấp kỹ thuật, các mối quan hệ đối tác, listing sàn giao dịch mới, "
        f"hoặc bất kỳ thông tin on-chain nào nổi bật liên quan đến {base_symbol}.\n"
        f"Yêu cầu:\n"
        f"1. Trình bày hoàn toàn bằng tiếng Việt.\n"
        f"2. Trả lời súc tích, khách quan, đi thẳng vào nguyên nhân chính xác của đợt tăng giá này.\n"
        f"3. Định dạng câu trả lời bằng Markdown đẹp mắt với tiêu đề rõ ràng, các gạch đầu dòng và in đậm những thông tin then chốt.\n"
        f"4. Trích dẫn nguồn thông tin hoặc thời gian diễn ra tin tức nếu có."
    )
    
    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent?key={gemini_key}"
    
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": prompt
                    }
                ]
            }
        ],
        "tools": [
            {
                "googleSearch": {}
            }
        ]
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(gemini_url, json=payload, timeout=120) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Gemini API returned status {resp.status}: {error_text}")
                    raise HTTPException(status_code=500, detail=f"Gemini API error: {resp.status}")
                
                res_json = await resp.json()
                
                # Extract text response from Gemini structure
                try:
                    candidates = res_json.get("candidates", [])
                    if candidates:
                        parts = candidates[0].get("content", {}).get("parts", [])
                        if parts:
                            analysis_text = parts[0].get("text", "")
                            return {"analysis": analysis_text}
                    
                    raise ValueError("Empty or invalid structure in Gemini response")
                except Exception as ex:
                    logger.error(f"Error parsing Gemini response: {ex}. Response JSON: {res_json}")
                    raise HTTPException(status_code=500, detail="Failed to parse analysis response from AI.")
                    
    except Exception as e:
        logger.error(f"Error explaining gainer {symbol}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/status")
async def get_status():
    monitor_instance.load_config()
    is_running = monitor_instance.config.get("is_running", False)
    
    # Calculate elapsed scan time if active
    return {
        "monitor_running": is_running,
        "scripts": {
            k: {
                "status": v["status"],
                "last_run": v["last_run"],
                "has_output": os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), v["output_txt"]))
            }
            for k, v in script_processes.items()
        },
        "alerts_count": len(monitor_instance.alerts_history),
        "version": VERSION
    }

@app.get("/api/config")
async def get_config():
    monitor_instance.load_config()
    return monitor_instance.config

@app.post("/api/config")
async def update_config(new_config: ConfigModel):
    try:
        current_config = {}
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                current_config = json.load(f)
        
        # Merge changes
        current_config.update(new_config.dict())
        
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(current_config, f, indent=2, ensure_ascii=False)
            
        monitor_instance.load_config()
        logger.info("Configuration updated via API.")
        return {"status": "success", "config": monitor_instance.config}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/bot/start")
async def start_bot():
    try:
        monitor_instance.load_config()
        monitor_instance.config["is_running"] = True
        
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(monitor_instance.config, f, indent=2, ensure_ascii=False)
            
        monitor_instance.load_config()
        logger.info("Spot monitor bot started by user.")
        return {"status": "started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/bot/stop")
async def stop_bot():
    try:
        monitor_instance.load_config()
        monitor_instance.config["is_running"] = False
        
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(monitor_instance.config, f, indent=2, ensure_ascii=False)
            
        monitor_instance.load_config()
        logger.info("Spot monitor bot stopped by user.")
        return {"status": "stopped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/bot/test")
async def test_telegram():
    monitor_instance.load_config()
    test_msg = (
        "🔔 <b>BINANCE MONITOR: Connection Test</b>\n\n"
        "Your Telegram alert connection is working perfectly!\n"
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    success = await monitor_instance.send_telegram(test_msg)
    if success:
        return {"status": "success", "message": "Test notification sent successfully."}
    else:
        raise HTTPException(status_code=400, detail="Failed to send test notification. Check credentials in settings and log files.")

@app.get("/api/alerts")
async def get_alerts():
    monitor_instance.load_alerts_history()
    return monitor_instance.alerts_history[::-1]  # Return newest first

@app.post("/api/alerts/clear")
async def clear_alerts():
    try:
        monitor_instance.alerts_history = []
        monitor_instance.sent_alerts.clear()
        monitor_instance.save_alerts_history()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Running watchlists scripts
def run_script_sync(name: str, filename: str):
    import subprocess
    script_processes[name]["status"] = "running"
    script_processes[name]["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    dir_path = os.path.dirname(os.path.abspath(__file__))
    abs_filename = os.path.join(dir_path, filename)
    log_file = os.path.join(dir_path, f"{name}_run.log")
    
    # Initialize log
    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"=== Starting {filename} at {script_processes[name]['last_run']} ===\n")
        
    try:
        # Launch python script using subprocess Popen
        process = subprocess.Popen(
            [sys.executable, abs_filename],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=dir_path,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="ignore"
        )
        
        while True:
            line = process.stdout.readline()
            if not line:
                break
            # Write to script log file
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(line)
                
        process.wait()
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n=== Process finished with exit code {process.returncode} ===\n")
    except Exception as e:
        logger.error(f"Error running script {filename}: {e}", exc_info=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\nError running script: {repr(e)}\n")
    finally:
        script_processes[name]["status"] = "idle"

@app.post("/api/run_script/{name}")
async def run_script(name: str, background_tasks: BackgroundTasks):
    if name not in script_processes:
        raise HTTPException(status_code=404, detail="Script not found.")
        
    if script_processes[name]["status"] == "running":
        raise HTTPException(status_code=400, detail="Script is already running.")
        
    filename = script_processes[name]["file"]
    background_tasks.add_task(run_script_sync, name, filename)
    return {"status": "started", "script": name}

@app.get("/api/logs/{name}")
async def get_logs(name: str):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    if name == "monitor":
        log_file = os.path.join(dir_path, "binance_monitor.log")
    elif name in script_processes:
        log_file = os.path.join(dir_path, f"{name}_run.log")
    else:
        raise HTTPException(status_code=404, detail="Log source not found.")
        
    if not os.path.exists(log_file):
        return {"logs": "No logs recorded yet."}
        
    try:
        # Read the last 200 lines
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            last_lines = lines[-200:]
            return {"logs": "".join(last_lines)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/watchlist/{name}")
async def get_watchlist(name: str):
    if name not in script_processes:
        raise HTTPException(status_code=404, detail="Watchlist not found.")
        
    dir_path = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(dir_path, script_processes[name]["output_txt"])
    if not os.path.exists(filename):
        raise HTTPException(status_code=404, detail="Watchlist text file not generated yet. Run the bot first.")
        
    try:
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
        return {"filename": script_processes[name]["output_txt"], "content": content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/watchlist/download/{name}")
async def download_watchlist(name: str):
    if name not in script_processes:
        raise HTTPException(status_code=404, detail="Watchlist not found.")
        
    dir_path = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(dir_path, script_processes[name]["output_txt"])
    if not os.path.exists(filename):
        raise HTTPException(status_code=404, detail="Watchlist text file not generated yet. Run the bot first.")
        
    return FileResponse(path=filename, filename=script_processes[name]["output_txt"], media_type="text/plain")

@app.get("/api/restricted_events")
async def get_restricted_events():
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    raise HTTPException(status_code=500, detail="Failed to fetch economic calendar.")
                data = await resp.json()
        
        grouped_events = {}
        vietnam_tz = timezone(timedelta(hours=7))
        now_vn = datetime.now(vietnam_tz)
        
        for item in data:
            if item.get("country") == "USD" and item.get("impact") == "High":
                date_str = item.get("date")
                try:
                    dt = datetime.fromisoformat(date_str)
                    dt_vn = dt.astimezone(vietnam_tz)
                except Exception:
                    continue
                
                timestamp = dt_vn.timestamp()
                title = item.get("title", "")
                forecast = item.get("forecast", "")
                previous = item.get("previous", "")
                
                if timestamp not in grouped_events:
                    grouped_events[timestamp] = []
                
                grouped_events[timestamp].append({
                    "title": title,
                    "forecast": forecast,
                    "previous": previous,
                    "date": dt_vn.strftime("%d/%m/%Y"),
                    "time": dt_vn.strftime("%H:%M"),
                    "is_upcoming": dt_vn > now_vn,
                    "timestamp": timestamp
                })
        
        restricted_events = []
        for timestamp, events in grouped_events.items():
            if len(events) == 1:
                ev = events[0]
                restricted_events.append({
                    "title": ev["title"],
                    "country": "USD",
                    "date": ev["date"],
                    "time": ev["time"],
                    "impact": "High",
                    "forecast": ev["forecast"],
                    "previous": ev["previous"],
                    "is_upcoming": ev["is_upcoming"],
                    "timestamp": ev["timestamp"]
                })
            else:
                # Find the main event to prioritize and list others in parentheses
                main_ev = None
                # 1. Non-Farm Employment Change
                main_ev = next((ev for ev in events if "Non-Farm" in ev["title"] or "Employment Change" in ev["title"]), None)
                # 2. CPI
                if not main_ev:
                    main_ev = next((ev for ev in events if "CPI" in ev["title"]), None)
                # 3. FOMC / Interest Rate
                if not main_ev:
                    main_ev = next((ev for ev in events if "FOMC" in ev["title"] or "Federal Funds Rate" in ev["title"]), None)
                # 4. GDP
                if not main_ev:
                    main_ev = next((ev for ev in events if "GDP" in ev["title"]), None)
                # Default to the first event if none matched
                if not main_ev:
                    main_ev = events[0]
                
                # Combine remaining titles
                other_titles = [ev["title"] for ev in events if ev != main_ev]
                combined_title = f"{main_ev['title']} ({', '.join(other_titles)})"
                
                restricted_events.append({
                    "title": combined_title,
                    "country": "USD",
                    "date": main_ev["date"],
                    "time": main_ev["time"],
                    "impact": "High",
                    "forecast": main_ev["forecast"],
                    "previous": main_ev["previous"],
                    "is_upcoming": main_ev["is_upcoming"],
                    "timestamp": main_ev["timestamp"]
                })
        
        restricted_events.sort(key=lambda x: x["timestamp"])
        return restricted_events
    except Exception as e:
        logger.error(f"Error fetching restricted events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/delisting")
async def get_delisting():
    articles = await fetch_binance_delistings()
    parsed_results = []
    now_utc = datetime.now(timezone.utc)
    
    for art in articles:
        title = art.get("title", "")
        # Match "Binance Will Delist ... on YYYY-MM-DD"
        match = re.search(r"Binance Will Delist (.*?) on (\d{4}-\d{2}-\d{2})", title, re.IGNORECASE)
        if match:
            coins_str = match.group(1)
            date_str = match.group(2)
            
            # Clean coins string to list
            raw_coins = re.split(r',|\band\b', coins_str)
            coins = [c.strip().upper() for c in raw_coins if c.strip()]
            
            # Parse delist time (03:00 UTC)
            try:
                delist_time_utc = datetime.strptime(f"{date_str} 03:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                continue
                
            # Only keep if not yet delisted (time is in the future)
            if delist_time_utc > now_utc:
                # Convert to VN time (GMT+7)
                vn_tz = timezone(timedelta(hours=7))
                delist_time_vn = delist_time_utc.astimezone(vn_tz)
                delist_time_vn_str = delist_time_vn.strftime("%H:%M %d/%m/%Y")
                
                parsed_results.append({
                    "coins": coins,
                    "coins_str": ", ".join(coins),
                    "delist_time": delist_time_vn_str,
                    "code": art.get("code"),
                    "title": title,
                    "releaseDate": art.get("releaseDate")
                })
    return parsed_results

@app.get("/api/explain_delist/{article_code}")
async def explain_delist(article_code: str):
    gemini_key = ""
    key_file = os.path.join(BASE_DIR, "gemini_key.txt")
    if os.path.exists(key_file):
        try:
            with open(key_file, "r", encoding="utf-8") as f:
                gemini_key = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading gemini_key.txt: {e}")
            
    if not gemini_key:
        gemini_key = os.environ.get("GEMINI_API_KEY", "")
        
    if not gemini_key:
        raise HTTPException(status_code=500, detail="Gemini API Key is not configured. Please create gemini_key.txt.")

    detail_url = "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query"
    params = {
        "articleCode": article_code
    }
    
    title = ""
    clean_text = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(detail_url, params=params, timeout=15) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    if res_json.get("code") == "000000" and res_json.get("data"):
                        article_data = res_json["data"]
                        title = article_data.get("title", "")
                        body_str = article_data.get("body", "")
                        clean_text = parse_article_body(body_str)
                    else:
                        raise HTTPException(status_code=400, detail=f"Failed to load article detail: {res_json.get('message')}")
                else:
                    raise HTTPException(status_code=resp.status, detail="Failed to fetch article detail from Binance.")
    except Exception as e:
        logger.error(f"Error loading article detail for {article_code}: {e}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

    if not clean_text:
        clean_text = "Không tải được nội dung bài viết. Chỉ có tiêu đề thông báo."

    prompt = (
        f"Hãy phân tích bài viết thông báo delist sau đây từ Binance:\n\n"
        f"Tiêu đề: {title}\n"
        f"Nội dung bài viết:\n{clean_text}\n\n"
        f"Yêu cầu:\n"
        f"1. Trình bày hoàn toàn bằng tiếng Việt.\n"
        f"2. Trích xuất danh sách các đồng coin/token bị delist (nêu rõ tên đầy đủ và ký hiệu, ví dụ: NFPrompt Token (NFP)).\n"
        f"3. Cho biết thời gian delist chính thức (UTC và đổi sang giờ Việt Nam GMT+7).\n"
        f"4. Liệt kê các cặp giao dịch Spot bị ảnh hưởng trực tiếp (ví dụ: ALCX/USDT, NFP/BTC, v.v.).\n"
        f"5. Tóm tắt các dịch vụ khác của Binance bị ảnh hưởng và thời gian tương ứng (ví dụ: Binance Futures, Margin, Simple Earn, Deposit/Withdrawal deadline, Auto-Invest, v.v.).\n"
        f"6. Đưa ra các lưu ý quan trọng và lời khuyên ngắn gọn cho người dùng đang nắm giữ các đồng coin này.\n"
        f"7. Định dạng câu trả lời bằng Markdown đẹp mắt với tiêu đề rõ ràng, các gạch đầu dòng, bảng biểu (nếu cần) và in đậm thông tin quan trọng."
    )

    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent?key={gemini_key}"
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": prompt
                    }
                ]
            }
        ]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(gemini_url, json=payload, timeout=120) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Gemini API returned status {resp.status}: {error_text}")
                    raise HTTPException(status_code=500, detail=f"Gemini API error: {resp.status}")
                
                res_json = await resp.json()
                try:
                    candidates = res_json.get("candidates", [])
                    if candidates:
                        parts = candidates[0].get("content", {}).get("parts", [])
                        if parts:
                            analysis_text = parts[0].get("text", "")
                            return {"analysis": analysis_text}
                    
                    raise ValueError("Empty response structure from Gemini")
                except Exception as ex:
                    logger.error(f"Error parsing Gemini response: {ex}. Response JSON: {res_json}")
                    raise HTTPException(status_code=500, detail="Failed to parse analysis response from Gemini AI.")
    except Exception as e:
        logger.error(f"Error explaining delisting {article_code}: {e}", exc_info=True)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard:app", host="0.0.0.0", port=8080, reload=False)
