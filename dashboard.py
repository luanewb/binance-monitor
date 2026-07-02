#!/usr/bin/env python3
"""
Bin Spot Monitor & Watchlist Web Dashboard
Provides a web interface to control the Binance Spot H1 anomaly detector
and run/manage the 3 existing watchlist scripts.

Version: 2.5.10
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
from binance_monitor import BinanceSpotMonitor, CONFIG_FILE, ALERTS_FILE

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Logger Setup
logger = logging.getLogger("Dashboard")

app = FastAPI(title="Binance Spot Monitor Dashboard", version="2.5.10")

# Bot Instance
monitor_instance = BinanceSpotMonitor()
monitor_task: Optional[asyncio.Task] = None

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

@app.on_event("startup")
async def startup_event():
    global monitor_task
    # Start monitor task in background
    monitor_task = asyncio.create_task(monitor_loop())
    logger.info("FastAPI dashboard started.")

@app.on_event("shutdown")
async def shutdown_event():
    global monitor_task
    if monitor_task:
        monitor_task.cancel()
        try:
            await monitor_task
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

async def fetch_coincap_marketcaps():
    now = time.time()
    # Cache for 5 minutes (300 seconds)
    if now - COINCAP_CACHE["last_fetched"] < 300 and COINCAP_CACHE["data"]:
        return COINCAP_CACHE["data"]
    
    # Try CoinGecko first (since it is reachable on user's network)
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    new_data = {}
                    for asset in res_json:
                        sym = asset.get("symbol", "").upper()
                        try:
                            mcap = float(asset.get("market_cap") or 0)
                        except:
                            mcap = 0.0
                        new_data[sym] = mcap
                    COINCAP_CACHE["data"] = new_data
                    COINCAP_CACHE["last_fetched"] = now
                    logger.info("Fetched CoinGecko market cap data successfully.")
                    return COINCAP_CACHE["data"]
                else:
                    logger.warning(f"CoinGecko API returned status {resp.status}. Trying CoinCap fallback...")
    except Exception as e:
        logger.warning(f"Error fetching CoinGecko market caps: {e}. Trying CoinCap fallback...")
        
    # Fallback to CoinCap
    try:
        url = "https://api.coincap.io/v2/assets?limit=2000"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    new_data = {}
                    for asset in res_json.get("data", []):
                        sym = asset.get("symbol", "").upper()
                        try:
                            mcap = float(asset.get("marketCapUsd") or 0)
                        except:
                            mcap = 0.0
                        new_data[sym] = mcap
                    COINCAP_CACHE["data"] = new_data
                    COINCAP_CACHE["last_fetched"] = now
                    logger.info("Fetched CoinCap market cap data successfully as fallback.")
                else:
                    logger.error(f"Failed to fetch CoinCap data fallback: status {resp.status}")
    except Exception as e:
        logger.error(f"Error fetching CoinCap market caps fallback: {e}")
        
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
                
                valid_tickers.append({
                    "symbol": symbol,
                    "base_symbol": base_symbol,
                    "price_change_pct": price_change_pct,
                    "last_price": last_price,
                    "quote_volume": quote_volume,
                    "market_cap": mcap
                })
        
        # 5. Sort by price_change_pct descending and pick top 20
        valid_tickers.sort(key=lambda x: x["price_change_pct"], reverse=True)
        top_20 = valid_tickers[:20]
        
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
    
    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}"
    
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
        "version": "2.5.10"
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

    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}"
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
