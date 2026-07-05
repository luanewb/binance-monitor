#!/usr/bin/env python3
"""
Binance Spot Price and Volume Anomaly Monitor Bot
Scans active USDT spot pairs on H1 and M5 timeframes, then alerts via Telegram.

Version: 2.5.16
"""

import asyncio
import json
import os
import sys
import time
import logging
from datetime import datetime
import urllib.parse
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
ALERTS_LOCK_FILE = f"{ALERTS_FILE}.lock"
VERSION = "2.5.16"

# Leveraged tokens to exclude
LEVERAGED_KEYWORDS = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]
BINANCE_API_BASE_URLS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
    "https://data-api.binance.vision",
]

def seconds_until_next_m5_close(delay_sec=10):
    now = datetime.utcnow()
    seconds_into_candle = (now.minute % 5) * 60 + now.second
    seconds_to_next_close = 300 - seconds_into_candle
    return seconds_to_next_close + delay_sec

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
                "m5_d1_pump_enabled": True,
                "m5_price_threshold_pct": 10.0,
                "m5_d1_volume_multiplier": 1.0,
                "m5_d1_scan_interval_sec": 300,
                "binance_api_base_urls": BINANCE_API_BASE_URLS,
                "use_curl_fallback": True,
                "curl_path": "curl.exe",
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
            self.alerts_history = self._read_alerts_history_from_disk()
            self._rebuild_sent_alerts()
            logger.debug(f"Loaded {len(self.alerts_history)} alert history records.")
        except Exception as e:
            logger.error(f"Error loading alert history: {e}")

    def _read_alerts_history_from_disk(self):
        if not os.path.exists(ALERTS_FILE):
            return []

        with open(ALERTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data if isinstance(data, list) else []

    def _get_alert_key_from_record(self, alert):
        try:
            alert_type = alert.get("alert_type", "h1_price_volume")
            symbol = alert.get("symbol")
            candle_open_time = alert.get("candle_open_time")
            if not symbol or candle_open_time is None:
                return None
            symbol_key = symbol if alert_type == "h1_price_volume" else f"{alert_type}:{symbol}"
            return (symbol_key, str(candle_open_time))
        except AttributeError:
            return None

    def _rebuild_sent_alerts(self):
        self.sent_alerts = {
            alert_key
            for alert_key in (self._get_alert_key_from_record(alert) for alert in self.alerts_history)
            if alert_key
        }

    def save_alerts_history(self):
        try:
            # Limit history to last 500 entries
            if len(self.alerts_history) > 500:
                self.alerts_history = self.alerts_history[-500:]
            with open(ALERTS_FILE, "w", encoding="utf-8") as f:
                json.dump(self.alerts_history, f, indent=2, ensure_ascii=False)
            self._rebuild_sent_alerts()
        except Exception as e:
            logger.error(f"Error saving alert history: {e}")

    def _acquire_alerts_lock(self, timeout_sec=30, stale_after_sec=120):
        deadline = time.time() + timeout_sec

        while True:
            try:
                fd = os.open(ALERTS_LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                lock_info = f"pid={os.getpid()} time={time.time()}\n"
                os.write(fd, lock_info.encode("utf-8"))
                return fd
            except FileExistsError:
                try:
                    lock_age = time.time() - os.path.getmtime(ALERTS_LOCK_FILE)
                    if lock_age > stale_after_sec:
                        logger.warning("Removing stale alerts history lock.")
                        os.remove(ALERTS_LOCK_FILE)
                        continue
                except FileNotFoundError:
                    continue
                except OSError as e:
                    logger.warning(f"Could not inspect alerts history lock: {e}")

                if time.time() >= deadline:
                    raise TimeoutError("Timed out waiting for alerts history lock.")
                time.sleep(0.2)

    def _release_alerts_lock(self, fd):
        try:
            os.close(fd)
        except OSError:
            pass

        try:
            os.remove(ALERTS_LOCK_FILE)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning(f"Could not remove alerts history lock: {e}")

    async def send_unique_alert(self, alert_key, message, alert_record):
        lock_fd = None
        try:
            lock_fd = self._acquire_alerts_lock()
            self.load_alerts_history()

            if alert_key in self.sent_alerts:
                logger.info(f"Skipping duplicate alert for {alert_key[0]} candle {alert_key[1]}.")
                return False

            success = await self.send_telegram(message)
            alert_record["telegram_sent"] = success
            self.alerts_history.append(alert_record)
            self.save_alerts_history()
            return success
        except TimeoutError as e:
            logger.error(f"{e} Skipping Telegram alert to avoid duplicate sends.")
            return False
        except Exception as e:
            logger.error(f"Error sending unique Telegram alert: {e}")
            return False
        finally:
            if lock_fd is not None:
                self._release_alerts_lock(lock_fd)

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
                        return await self.send_telegram_with_curl(url, payload)
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")
            return await self.send_telegram_with_curl(url, payload)

    async def send_telegram_with_curl(self, url, payload):
        if not self.config.get("use_curl_fallback", True):
            return False

        curl_path = self.config.get("curl_path", "curl.exe")
        try:
            process = await asyncio.create_subprocess_exec(
                curl_path,
                "-fsSL",
                "--connect-timeout",
                "10",
                "--max-time",
                "15",
                "-X",
                "POST",
                "-H",
                "Content-Type: application/json",
                "--data-binary",
                json.dumps(payload, ensure_ascii=False),
                url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=20)
            if process.returncode != 0:
                logger.error(f"Telegram curl fallback failed: {stderr.decode('utf-8', errors='ignore')[:300]}")
                return False

            response = json.loads(stdout.decode("utf-8"))
            if response.get("ok"):
                logger.info("Telegram alert sent successfully via curl fallback.")
                return True

            logger.error(f"Telegram curl fallback returned error: {response}")
            return False
        except Exception as e:
            logger.error(f"Telegram curl fallback failed: {e}")
            return False

    def get_binance_api_base_urls(self):
        urls = self.config.get("binance_api_base_urls", BINANCE_API_BASE_URLS)
        if not isinstance(urls, list):
            return BINANCE_API_BASE_URLS

        normalized_urls = []
        for url in urls:
            if isinstance(url, str) and url.startswith("https://"):
                normalized_urls.append(url.rstrip("/"))
        return normalized_urls or BINANCE_API_BASE_URLS

    async def fetch_binance_json(self, session, path, params=None, timeout=15, context="Binance request", log_failures=True):
        last_error = None
        for base_url in self.get_binance_api_base_urls():
            url = f"{base_url}{path}"
            try:
                async with session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    response_text = await resp.text()
                    last_error = f"{base_url} returned HTTP {resp.status}: {response_text[:160]}"
                    if log_failures:
                        logger.warning(f"{context}: {last_error}")

                    if resp.status == 429:
                        await asyncio.sleep(2)
            except Exception as e:
                last_error = f"{base_url}: {e}"
                if log_failures:
                    logger.warning(f"{context}: {last_error}")

        if self.config.get("use_curl_fallback", True):
            data = await self.fetch_binance_json_with_curl(path, params=params, timeout=timeout, context=context, log_failures=log_failures)
            if data is not None:
                return data

        if log_failures:
            logger.error(f"{context}: all Binance API endpoints failed. Last error: {last_error}")
        return None

    async def fetch_binance_json_with_curl(self, path, params=None, timeout=15, context="Binance request", log_failures=True):
        last_error = None
        curl_path = self.config.get("curl_path", "curl.exe")

        for base_url in self.get_binance_api_base_urls():
            query = urllib.parse.urlencode(params or {})
            url = f"{base_url}{path}"
            if query:
                url = f"{url}?{query}"

            try:
                process = await asyncio.create_subprocess_exec(
                    curl_path,
                    "-fsSL",
                    "--connect-timeout",
                    str(timeout),
                    "--max-time",
                    str(timeout),
                    url,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout + 5)
                if process.returncode == 0:
                    if log_failures:
                        logger.info(f"{context}: fetched via curl fallback from {base_url}")
                    return json.loads(stdout.decode("utf-8"))

                last_error = f"{base_url}: {stderr.decode('utf-8', errors='ignore')[:200]}"
                if log_failures:
                    logger.warning(f"{context} curl fallback: {last_error}")
            except Exception as e:
                last_error = f"{base_url}: {e}"
                if log_failures:
                    logger.warning(f"{context} curl fallback: {last_error}")

        if log_failures:
            logger.error(f"{context}: curl fallback failed for all Binance endpoints. Last error: {last_error}")
        return None

    async def fetch_24h_ticker_volumes(self, session):
        """Fetches 24h trading volume (quoteVolume) for all symbols on Binance."""
        try:
            data = await self.fetch_binance_json(
                session,
                "/api/v3/ticker/24hr",
                timeout=15,
                context="24h ticker volumes"
            )
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
        try:
            data = await self.fetch_binance_json(
                session,
                "/api/v3/exchangeInfo",
                timeout=15,
                context="active pairs"
            )
            if not isinstance(data, dict):
                return []

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

    async def fetch_kline_data(self, session, symbol, semaphore, interval="1h", limit=None):
        """Fetches recent candles for a specific symbol."""
        # Need one extra candle when scanning on H1 close: current new candle
        # plus the just-closed candle and its completed volume baseline.
        if limit is None:
            limit = self.config.get("volume_avg_period", 20) + 2
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        async with semaphore:
            try:
                klines = await self.fetch_binance_json(
                    session,
                    "/api/v3/klines",
                    params=params,
                    timeout=10,
                    context=f"{symbol} {interval} klines",
                    log_failures=False
                )
                return symbol, klines
            except Exception:
                return symbol, None

    async def fetch_m5_d1_kline_data(self, session, symbol, semaphore):
        """Fetches the latest M5 candles and D1 candles for the M5/D1 pump bot."""
        async with semaphore:
            try:
                m5_klines = await self.fetch_binance_json(
                    session,
                    "/api/v3/klines",
                    params={"symbol": symbol, "interval": "5m", "limit": 3},
                    timeout=10,
                    context=f"{symbol} M5 klines",
                    log_failures=False
                )
                if not m5_klines:
                    return symbol, None, None

                d1_klines = await self.fetch_binance_json(
                    session,
                    "/api/v3/klines",
                    params={"symbol": symbol, "interval": "1d", "limit": 2},
                    timeout=10,
                    context=f"{symbol} D1 klines",
                    log_failures=False
                )
                if not d1_klines:
                    return symbol, None, None

                return symbol, m5_klines, d1_klines
            except Exception:
                return symbol, None, None

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
                    if alert_key in self.sent_alerts:
                        continue

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

                    alert_record = {
                        "alert_type": "h1_price_volume",
                        "timeframe": "H1",
                        "symbol": symbol,
                        "price_change": price_change_pct,
                        "current_price": current_price,
                        "open_price": open_price,
                        "volume": current_volume,
                        "avg_volume": avg_volume,
                        "volume_ratio": volume_ratio,
                        "candle_open_time": str(candle_open_time_ms),
                        "candle_open_time_str": candle_open_time_str,
                        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                    }

                    if await self.send_unique_alert(alert_key, message, alert_record):
                        alerts_triggered += 1
                        
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

    async def scan_m5_d1_pump_symbols(self):
        logger.info("Starting M5/D1 pump scan...")
        start_time = time.time()

        self.load_config()

        if not self.config.get("m5_d1_pump_enabled", True):
            logger.info("M5/D1 pump bot is disabled. Skipping scan.")
            return

        price_threshold = self.config.get("m5_price_threshold_pct", 10.0)
        volume_multiplier = self.config.get("m5_d1_volume_multiplier", 1.0)

        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:
            symbols = await self.fetch_active_usdt_pairs(session)
            if not symbols:
                logger.warning("No symbols fetched. Skipping M5/D1 pump scan.")
                return

            semaphore = asyncio.Semaphore(12)
            tasks = [self.fetch_m5_d1_kline_data(session, symbol, semaphore) for symbol in symbols]
            results = await asyncio.gather(*tasks)

            alerts_triggered = 0
            scanned_count = 0

            for symbol, m5_klines, d1_klines in results:
                if not m5_klines or not d1_klines or len(m5_klines) < 2 or len(d1_klines) < 2:
                    continue

                current_candle = m5_klines[-2]  # last closed M5 candle
                reference_d1_candle = d1_klines[-2]  # last closed D1 candle

                candle_open_time_ms = current_candle[0]
                candle_open_time_str = datetime.utcfromtimestamp(candle_open_time_ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S UTC')

                try:
                    open_price = float(current_candle[1])
                    high_price = float(current_candle[2])
                    low_price = float(current_candle[3])
                    current_price = float(current_candle[4])
                    current_volume = float(current_candle[7])  # Quote asset volume (USDT)
                    d1_volume = float(reference_d1_candle[7])  # Quote asset volume (USDT)
                except (ValueError, TypeError, IndexError):
                    continue

                if open_price <= 0 or d1_volume <= 0:
                    continue

                scanned_count += 1
                price_change_pct = ((current_price - open_price) / open_price) * 100.0
                volume_ratio = current_volume / d1_volume

                if price_change_pct <= price_threshold:
                    continue
                if current_volume <= d1_volume * volume_multiplier:
                    continue

                alert_key = (f"m5_d1_pump:{symbol}", str(candle_open_time_ms))
                if alert_key in self.sent_alerts:
                    continue

                message = (
                    f"<b>BINANCE SPOT ALERT: M5 Pump vs D1 Volume</b>\n\n"
                    f"<b>Symbol:</b> #{symbol}\n"
                    f"<b>Timeframe:</b> M5\n"
                    f"<b>Price Change:</b> +{price_change_pct:.2f}%\n"
                    f"<b>Current Price:</b> {current_price:.6g} USDT\n"
                    f"<b>Open Price:</b> {open_price:.6g} USDT\n"
                    f"<b>High / Low:</b> {high_price:.6g} / {low_price:.6g}\n"
                    f"<b>M5 Volume:</b> {current_volume:,.2f} USDT\n"
                    f"<b>Previous D1 Volume:</b> {d1_volume:,.2f} USDT\n"
                    f"<b>M5 / D1 Volume:</b> <b>{volume_ratio:.2f}x</b>\n"
                    f"<b>Candle Open:</b> {candle_open_time_str}"
                )

                alert_record = {
                    "alert_type": "m5_d1_pump",
                    "timeframe": "M5",
                    "symbol": symbol,
                    "price_change": price_change_pct,
                    "current_price": current_price,
                    "open_price": open_price,
                    "volume": current_volume,
                    "avg_volume": d1_volume,
                    "volume_ratio": volume_ratio,
                    "reference_volume": d1_volume,
                    "reference_timeframe": "D1",
                    "candle_open_time": str(candle_open_time_ms),
                    "candle_open_time_str": candle_open_time_str,
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                }

                if await self.send_unique_alert(alert_key, message, alert_record):
                    alerts_triggered += 1

            elapsed = time.time() - start_time
            logger.info(f"M5/D1 pump scan complete. Scanned {scanned_count}/{len(symbols)} coins. "
                        f"Alerts triggered: {alerts_triggered}. Elapsed: {elapsed:.2f}s")

            now_ms = int(time.time() * 1000)
            cutoff_ms = now_ms - (24 * 3600 * 1000)
            self.sent_alerts = {
                k for k in self.sent_alerts
                if int(k[1]) > cutoff_ms
            }

async def m5_d1_monitor_loop(monitor):
    logger.info("Standalone M5/D1 pump monitor loop started.")
    while True:
        try:
            monitor.load_config()
            is_running = monitor.config.get("is_running", False)
            is_enabled = monitor.config.get("m5_d1_pump_enabled", True)

            if is_running and is_enabled:
                await monitor.scan_m5_d1_pump_symbols()
                sleep_duration = seconds_until_next_m5_close()
                logger.info(f"M5 close alignment active: next scan scheduled in {sleep_duration}s (at next 5m close + 10s)")
            else:
                logger.info("M5/D1 pump bot is inactive. Skipping scan.")
                sleep_duration = 10

            for _ in range(max(1, int(sleep_duration))):
                await asyncio.sleep(1)
                current_active = monitor.config.get("is_running", False)
                current_enabled = monitor.config.get("m5_d1_pump_enabled", True)
                if current_active != is_running or current_enabled != is_enabled:
                    break
        except asyncio.CancelledError:
            logger.info("M5/D1 pump monitor loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in M5/D1 pump monitor loop: {e}")
            await asyncio.sleep(10)

async def main():
    monitor = BinanceSpotMonitor()
    logger.info("Starting standalone Binance Spot Monitor bot...")
    m5_task = asyncio.create_task(m5_d1_monitor_loop(monitor))
    
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

    m5_task.cancel()
    try:
        await m5_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())
