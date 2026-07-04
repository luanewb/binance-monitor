#!/usr/bin/env python3
"""
Binance Spot H1 Price and Volume Anomaly Monitor Bot
Scans active USDT spot pairs on H1 timeframe, checks for > 10% price increase
and > 3x average volume, then alerts via Telegram.

Version: 2.5.15
"""

import asyncio
import json
import os
import sys
import time
import logging
from datetime import datetime
import aiohttp

# Configure Logging
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "binance_monitor.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("BinanceMonitor")

# Files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
ALERTS_FILE = os.path.join(BASE_DIR, "alerts_history.json")
VERSION = "2.5.15"

# Leveraged tokens to exclude
LEVERAGED_KEYWORDS = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]

class BinanceSpotMonitor:
    def __init__(self):
        self.config = {}
        self.sent_alerts = set()  # set of (symbol, candle_open_time)
        self.alerts_history = []
        self.load_config()
        self.load_alerts_history()

    def load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    self.config = json.load(f)
            else:
                self.config = {}

            # Use user's requested defaults if empty or missing
            if not self.config.get("telegram_token"):
                self.config["telegram_token"] = "8986756914:AAG2dj8r9RuT234iBNM98mUODSsiqY7Ti2w"
            if not self.config.get("telegram_chat_id"):
                self.config["telegram_chat_id"] = "-5308046923"

            # Apply other defaults if missing
            defaults = {
                "price_threshold_pct": 10.0,
                "volume_multiplier": 3.0,
                "scan_interval_sec": 300,
                "volume_avg_period": 20,
                "min_24h_volume": 1000000.0,
                "min_h1_pump_volume": 1000000.0,
                "is_running": False
            }
            for key, val in defaults.items():
                if key not in self.config:
                    self.config[key] = val

            logger.debug("Configuration loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")

    def load_alerts_history(self):
        try:
            if os.path.exists(ALERTS_FILE):
                with open(ALERTS_FILE, "r", encoding="utf-8") as f:
                    self.alerts_history = json.load(f)
                # Populate sent_alerts to avoid sending duplicates on restart
                for alert in self.alerts_history:
                    self.sent_alerts.add((alert["symbol"], alert["candle_open_time"]))
            logger.debug(f"Loaded {len(self.alerts_history)} alert history records.")
        except Exception as e:
            logger.error(f"Error loading alert history: {e}")

    def save_alerts_history(self):
        try:
            # Limit history to last 500 entries
            if len(self.alerts_history) > 500:
                self.alerts_history = self.alerts_history[-500:]
            with open(ALERTS_FILE, "w", encoding="utf-8") as f:
                json.dump(self.alerts_history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving alert history: {e}")

    async def send_telegram(self, message):
        token = self.config.get("telegram_token")
        chat_id = self.config.get("telegram_chat_id")
        
        if not token or not chat_id:
            logger.warning("Telegram token or Chat ID not configured. Skipping alert.")
            return False

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        logger.info("Telegram alert sent successfully.")
                        return True
                    else:
                        response_text = await resp.text()
                        logger.error(f"Telegram API returned status {resp.status}: {response_text}")
                        return False
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")
            return False

    async def fetch_24h_ticker_volumes(self, session):
        """Fetches 24h trading volume (quoteVolume) for all symbols on Binance."""
        url = "https://api.binance.com/api/v3/ticker/24hr"
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to fetch 24h ticker data: {resp.status}")
                    return {}
                data = await resp.json()
                volumes = {}
                if isinstance(data, list):
                    for ticker in data:
                        symbol = ticker.get("symbol", "")
                        try:
                            quote_volume = float(ticker.get("quoteVolume", 0.0))
                        except (ValueError, TypeError):
                            quote_volume = 0.0
                        volumes[symbol] = quote_volume
                return volumes
        except Exception as e:
            logger.error(f"Error fetching 24h ticker volumes: {e}")
            return {}

    async def fetch_active_usdt_pairs(self, session):
        """Fetches active spot trading symbols on Binance ending in USDT (excluding leveraged)."""
        url = "https://api.binance.com/api/v3/exchangeInfo"
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to fetch exchangeInfo: {resp.status}")
                    return []
                data = await resp.json()
                symbols = []
                for s in data.get("symbols", []):
                    symbol = s.get("symbol", "")
                    status = s.get("status", "")
                    # Filter Spot, Trading, ending in USDT
                    if status == "TRADING" and symbol.endswith("USDT"):
                        # Exclude leveraged
                        if not any(kw in symbol for kw in LEVERAGED_KEYWORDS):
                            symbols.append(symbol)
                logger.info(f"Found {len(symbols)} active USDT pairs to monitor.")
                return symbols
        except Exception as e:
            logger.error(f"Error fetching active pairs: {e}")
            return []

    async def fetch_kline_data(self, session, symbol, semaphore):
        """Fetches the last N H1 candles for a specific symbol."""
        # Need one extra candle when scanning on H1 close: current new candle
        # plus the just-closed candle and its completed volume baseline.
        limit = self.config.get("volume_avg_period", 20) + 2
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "1h",
            "limit": limit
        }
        async with semaphore:
            try:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        klines = await resp.json()
                        return symbol, klines
                    elif resp.status == 429:
                        logger.warning(f"Binance rate limit hit (429) while fetching {symbol}. Sleeping.")
                        await asyncio.sleep(2)
                        return symbol, None
                    else:
                        # Silently skip errors for individual coins (e.g. deleted/maintenance)
                        return symbol, None
            except Exception:
                return symbol, None

    async def scan_all_symbols(self):
        logger.info("Starting Price/Volume scan...")
        start_time = time.time()
        
        # Load latest config changes
        self.load_config()
        
        price_threshold = self.config.get("price_threshold_pct", 10.0)
        volume_multiplier = self.config.get("volume_multiplier", 3.0)
        period = self.config.get("volume_avg_period", 20)
        min_24h_volume = self.config.get("min_24h_volume", 1000000.0)
        min_h1_pump_volume = self.config.get("min_h1_pump_volume", 1000000.0)
        
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:
            ticker_volumes = await self.fetch_24h_ticker_volumes(session)
            symbols = await self.fetch_active_usdt_pairs(session)
            if not symbols:
                logger.warning("No symbols fetched. Skipping scan.")
                return

            semaphore = asyncio.Semaphore(15)  # Limit concurrency to stay safe
            tasks = [self.fetch_kline_data(session, symbol, semaphore) for symbol in symbols]
            
            results = await asyncio.gather(*tasks)
            
            alerts_triggered = 0
            scanned_count = 0
            
            for symbol, klines in results:
                if not klines or len(klines) < period + 1:
                    continue
                
                # Check 24h volume threshold
                vol_24h = ticker_volumes.get(symbol, 0.0)
                if vol_24h < min_24h_volume:
                    continue
                
                scanned_count += 1
                
                # When the bot is aligned to H1 close (scan_interval_sec >= 3600),
                # Binance already returns a new current candle. Evaluate the
                # just-closed candle instead, otherwise H1 pumps are missed.
                use_closed_h1 = self.config.get("scan_interval_sec", 300) >= 3600
                if use_closed_h1:
                    if len(klines) < period + 2:
                        continue
                    completed_candles = klines[-(period + 2):-2]
                    current_candle = klines[-2]
                else:
                    completed_candles = klines[-(period + 1):-1]
                    current_candle = klines[-1]
                
                # Extract details
                candle_open_time_ms = current_candle[0]
                candle_open_time_str = datetime.utcfromtimestamp(candle_open_time_ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S UTC')
                
                # Current pricing
                open_price = float(current_candle[1])
                high_price = float(current_candle[2])
                low_price = float(current_candle[3])
                current_price = float(current_candle[4])
                current_volume = float(current_candle[7])  # Quote asset volume (USDT)

                if current_volume < min_h1_pump_volume:
                    continue
                
                # Calculate average volume of completed candles
                completed_volumes = [float(c[7]) for c in completed_candles[-period:]]
                avg_volume = sum(completed_volumes) / len(completed_volumes) if completed_volumes else 0
                
                if avg_volume <= 0:
                    continue
                
                # Calculate anomalies
                price_change_pct = ((current_price - open_price) / open_price) * 100.0
                volume_ratio = current_volume / avg_volume
                
                # Check condition: Price increase > threshold AND volume > x3 average
                if price_change_pct >= price_threshold and volume_ratio >= volume_multiplier:
                    # Check if already notified for this symbol in this candle
                    alert_key = (symbol, str(candle_open_time_ms))
                    if alert_key not in self.sent_alerts:
                        self.sent_alerts.add(alert_key)
                        alerts_triggered += 1
                        
                        # Generate Telegram message
                        message = (
                            f"🚨 <b>BINANCE SPOT ALERT: Price & Vol Spike</b>\n\n"
                            f"<b>Symbol:</b> #{symbol}\n"
                            f"<b>Timeframe:</b> H1\n"
                            f"<b>Price Change:</b> 🟢 +{price_change_pct:.2f}%\n"
                            f"<b>Current Price:</b> {current_price:.6g} USDT\n"
                            f"<b>Open Price:</b> {open_price:.6g} USDT\n"
                            f"<b>High / Low:</b> {high_price:.6g} / {low_price:.6g}\n"
                            f"<b>H1 Volume:</b> {current_volume:,.2f} USDT\n"
                            f"<b>Avg H1 Vol ({period}p):</b> {avg_volume:,.2f} USDT\n"
                            f"<b>Volume Multiplier:</b> 🔥 <b>{volume_ratio:.2f}x</b>\n"
                            f"<b>Candle Open:</b> {candle_open_time_str}"
                        )
                        
                        # Send Telegram alert
                        success = await self.send_telegram(message)
                        
                        # Save to history
                        self.alerts_history.append({
                            "symbol": symbol,
                            "price_change": price_change_pct,
                            "current_price": current_price,
                            "open_price": open_price,
                            "volume": current_volume,
                            "avg_volume": avg_volume,
                            "volume_ratio": volume_ratio,
                            "candle_open_time": str(candle_open_time_ms),
                            "candle_open_time_str": candle_open_time_str,
                            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'),
                            "telegram_sent": success
                        })
                        self.save_alerts_history()
                        
            elapsed = time.time() - start_time
            logger.info(f"Scan complete. Scanned {scanned_count}/{len(symbols)} coins. "
                        f"Alerts triggered: {alerts_triggered}. Elapsed: {elapsed:.2f}s")
            
            # Clean up old alerts from set to free memory (e.g. keep only those from last 24h)
            # Candle open times are strings of milliseconds
            now_ms = int(time.time() * 1000)
            cutoff_ms = now_ms - (24 * 3600 * 1000)
            self.sent_alerts = {
                k for k in self.sent_alerts
                if int(k[1]) > cutoff_ms
            }

async def main():
    monitor = BinanceSpotMonitor()
    logger.info("Starting standalone Binance Spot Monitor bot...")
    
    last_mtime = 0
    if os.path.exists(CONFIG_FILE):
        last_mtime = os.path.getmtime(CONFIG_FILE)
        
    while True:
        try:
            # Read config to see if we should scan
            monitor.load_config()
            is_running = monitor.config.get("is_running", False)
            scan_interval = monitor.config.get("scan_interval_sec", 300)
            
            if is_running:
                await monitor.scan_all_symbols()
            else:
                logger.info("Bot is set to INACTIVE in config. Skipping scan.")
            
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
                logger.info(f"Sleeping for {sleep_duration} seconds...")
            
            # Sleep second-by-second and check if config file changes on disk
            for _ in range(max(1, int(sleep_duration))):
                await asyncio.sleep(1)
                if os.path.exists(CONFIG_FILE):
                    mtime = os.path.getmtime(CONFIG_FILE)
                    if mtime != last_mtime:
                        last_mtime = mtime
                        monitor.load_config()
                        current_active = monitor.config.get("is_running", False)
                        current_interval = monitor.config.get("scan_interval_sec", 300)
                        if current_active != is_running or current_interval != scan_interval:
                            break
                            
        except KeyboardInterrupt:
            logger.info("Stopping bot...")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
