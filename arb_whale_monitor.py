#!/usr/bin/env python3
"""
Arbitrum Whale Monitor Bot
Monitors Arbitrum One blockchain for large USDT and ARB token transfers (>= 1,000,000 USDT).
Identifies CEX deposits, CEX withdrawals, and whale-to-whale transfers,
then sends real-time formatted alerts to Telegram.

Version: 2.5.19
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import aiohttp

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [WhaleMonitor] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("WhaleMonitor")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
WHALE_HISTORY_FILE = os.path.join(BASE_DIR, "arb_whale_history.json")
WHALE_LABELS_FILE = os.path.join(BASE_DIR, "whale_labels.json")

# Public Arbitrum One RPC endpoints with fallback
ARBITRUM_RPCS = [
    "https://arb1.arbitrum.io/rpc",
    "https://arbitrum.llamarpc.com",
    "https://1rpc.io/arb",
    "https://arbitrum-one-rpc.publicnode.com",
    "https://rpc.ankr.com/arbitrum"
]

# Smart Contract Addresses on Arbitrum One
USDT_CONTRACT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9".lower()
USDT_DECIMALS = 6

ARB_CONTRACT = "0x912CE59144191C1204E64559FE8253a0e49E6548".lower()
ARB_DECIMALS = 18

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Known Exchange & Entity Labels on Arbitrum One
DEFAULT_LABELS = {
    # Binance
    "0xb38e8c17e38363af6ebdcb3dae12e0243582891d": {"name": "Binance: Hot Wallet 54", "type": "cex", "exchange": "Binance"},
    "0xf977814e90da44bfa03b6295a0616a897441acec": {"name": "Binance: Hot Wallet 20", "type": "cex", "exchange": "Binance"},
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": {"name": "Binance: Hot Wallet 14", "type": "cex", "exchange": "Binance"},
    "0xdfd5293d8e347dfe59e90efd55b2956a1343ee33": {"name": "Binance: Hot Wallet", "type": "cex", "exchange": "Binance"},
    "0x28c6c06298d514db089934071355e5743bf21d60": {"name": "Binance: Hot Wallet 14", "type": "cex", "exchange": "Binance"},
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": {"name": "Binance: Hot Wallet 15", "type": "cex", "exchange": "Binance"},
    
    # OKX
    "0xa7efae728d2936e78bda97dc267687568dd593f3": {"name": "OKX: Hot Wallet", "type": "cex", "exchange": "OKX"},
    "0x1f5f3e7edbb9ff4d03e5c704fdfecae930b135da": {"name": "OKX: Hot Wallet 2", "type": "cex", "exchange": "OKX"},
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": {"name": "OKX: Hot Wallet 3", "type": "cex", "exchange": "OKX"},
    "0x58b9f1d0411a76abfa926a8d810a97c9b0e5fa3d": {"name": "OKX: Hot Wallet 4", "type": "cex", "exchange": "OKX"},

    # Bybit
    "0xf89d7b9c372f2561083e747bbff0b0c23d71ff04": {"name": "Bybit: Hot Wallet", "type": "cex", "exchange": "Bybit"},
    "0x1db3439a222c519ab44bb1144fc28167b4fa6ee6": {"name": "Bybit: Hot Wallet 2", "type": "cex", "exchange": "Bybit"},
    "0xee5b5b923f707a8b897321045f8f85f3404c0556": {"name": "Bybit: Hot Wallet 3", "type": "cex", "exchange": "Bybit"},

    # Coinbase
    "0x503828976d22510aad0201ac7ec88293211d23da": {"name": "Coinbase: Hot Wallet", "type": "cex", "exchange": "Coinbase"},
    "0xb5d85cbf7cb3ee0e56b3bb207d5fc4b82f43f511": {"name": "Coinbase: Hot Wallet 2", "type": "cex", "exchange": "Coinbase"},
    
    # Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": {"name": "Gate.io: Hot Wallet", "type": "cex", "exchange": "Gate.io"},
    
    # MEXC
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": {"name": "MEXC: Hot Wallet", "type": "cex", "exchange": "MEXC"},
    
    # Bitget
    "0x0d3c01e695d3e028b030d5efd8fc8c6fb25c5a77": {"name": "Bitget: Hot Wallet", "type": "cex", "exchange": "Bitget"},

    # Kraken
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": {"name": "Kraken: Hot Wallet", "type": "cex", "exchange": "Kraken"},

    # Arbitrum System / Foundations / Whales
    "0x9f43ab02cacc8e709b05936a92dc85b76d1523c4": {"name": "Arbitrum: DAO Treasury", "type": "foundation", "exchange": None},
    "0xf3fc178157fb3c87548baa86f9d24ba38e649b58": {"name": "Arbitrum: Foundation", "type": "foundation", "exchange": None},
    "0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9": {"name": "Arbitrum: Token Distributor", "type": "foundation", "exchange": None},
    "0x13a290eb618a80496101eaadbb953d582b1d5c22": {"name": "Wintermute Trading", "type": "market_maker", "exchange": None},
    "0xdbf5e9c5206d0ba70a4c208051777da53387b570": {"name": "Amber Group", "type": "market_maker", "exchange": None},
    "0x4b16c5de96eb2117bbe5fd171e4d203624b014aa": {"name": "Flow Traders", "type": "market_maker", "exchange": None}
}


class ArbitrumWhaleMonitor:
    def __init__(self):
        self.config = {}
        self.labels = {}
        self.history: List[dict] = []
        self.known_txs = set()
        self.current_rpc_index = 0
        self.last_scanned_block = 0
        self.arb_price_cache = {"price": 0.22, "updated_at": 0}
        self.is_running = False
        self._load_config()
        self._load_labels()
        self._load_history()

    def _load_config(self):
        """Loads configuration from config.json."""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    self.config = json.load(f)
            except Exception as e:
                logger.error(f"Error reading config file: {e}")
                self.config = {}

        # Default values if missing
        self.config.setdefault("whale_monitor_enabled", True)
        self.config.setdefault("whale_min_usdt", 1000000.0)
        self.config.setdefault("whale_track_usdt", True)
        self.config.setdefault("whale_track_arb", True)
        self.config.setdefault("whale_scan_interval_sec", 15)

    def save_config(self):
        """Saves whale configuration back to config.json."""
        try:
            full_config = {}
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    full_config = json.load(f)
            full_config["whale_monitor_enabled"] = self.config.get("whale_monitor_enabled", True)
            full_config["whale_min_usdt"] = float(self.config.get("whale_min_usdt", 1000000.0))
            full_config["whale_track_usdt"] = bool(self.config.get("whale_track_usdt", True))
            full_config["whale_track_arb"] = bool(self.config.get("whale_track_arb", True))
            full_config["whale_scan_interval_sec"] = int(self.config.get("whale_scan_interval_sec", 15))
            
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(full_config, f, indent=2)
            logger.info("Whale config saved successfully.")
        except Exception as e:
            logger.error(f"Error saving config: {e}")

    def _load_labels(self):
        """Loads default and custom wallet labels."""
        self.labels = dict(DEFAULT_LABELS)
        if os.path.exists(WHALE_LABELS_FILE):
            try:
                with open(WHALE_LABELS_FILE, "r", encoding="utf-8") as f:
                    custom = json.load(f)
                    for k, v in custom.items():
                        self.labels[k.lower()] = v
            except Exception as e:
                logger.error(f"Error loading custom labels: {e}")

    def save_custom_label(self, address: str, name: str, wallet_type: str = "whale", exchange: Optional[str] = None):
        """Saves a custom wallet label."""
        addr = address.lower().strip()
        label_info = {"name": name.strip(), "type": wallet_type.strip(), "exchange": exchange}
        self.labels[addr] = label_info
        try:
            custom = {}
            if os.path.exists(WHALE_LABELS_FILE):
                with open(WHALE_LABELS_FILE, "r", encoding="utf-8") as f:
                    custom = json.load(f)
            custom[addr] = label_info
            with open(WHALE_LABELS_FILE, "w", encoding="utf-8") as f:
                json.dump(custom, f, indent=2)
            logger.info(f"Saved custom label for {addr}: {name}")
        except Exception as e:
            logger.error(f"Error writing custom label: {e}")

    def _load_history(self):
        """Loads detected whale alerts from disk."""
        if os.path.exists(WHALE_HISTORY_FILE):
            try:
                with open(WHALE_HISTORY_FILE, "r", encoding="utf-8") as f:
                    self.history = json.load(f)
                    for item in self.history:
                        tx_id = item.get("tx_hash", "") + "_" + str(item.get("log_index", 0))
                        self.known_txs.add(tx_id)
            except Exception as e:
                logger.error(f"Error loading whale history: {e}")
                self.history = []

    def save_history(self):
        """Saves detected whale alerts to disk (keeps last 300)."""
        try:
            self.history = self.history[-300:]
            with open(WHALE_HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump(self.history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving whale history: {e}")

    async def get_arb_price(self) -> float:
        """Fetches ARBUSDT price from Binance with 60s cache."""
        now = time.time()
        if now - self.arb_price_cache["updated_at"] < 60 and self.arb_price_cache["price"] > 0:
            return self.arb_price_cache["price"]
        
        url = "https://api.binance.com/api/v3/ticker/price?symbol=ARBUSDT"
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = float(data.get("price", 0.22))
                        self.arb_price_cache = {"price": price, "updated_at": now}
                        return price
        except Exception as e:
            logger.warning(f"Failed to fetch ARB price from Binance: {e}. Using cached price.")
        return self.arb_price_cache["price"]

    async def rpc_call(self, method: str, params: list) -> Optional[dict]:
        """Calls Arbitrum JSON-RPC with automatic failover across multiple nodes."""
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": int(time.time() * 1000) % 1000000
        }
        headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}

        # Try up to len(ARBITRUM_RPCS) times
        for _ in range(len(ARBITRUM_RPCS)):
            rpc_url = ARBITRUM_RPCS[self.current_rpc_index]
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(rpc_url, json=payload, headers=headers, timeout=8) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if "error" in data:
                                logger.warning(f"RPC error from {rpc_url}: {data['error']}")
                            else:
                                return data.get("result")
                        else:
                            logger.warning(f"RPC HTTP {resp.status} from {rpc_url}")
            except Exception as e:
                logger.warning(f"RPC connection failed to {rpc_url}: {e}")

            # Switch to next RPC endpoint
            self.current_rpc_index = (self.current_rpc_index + 1) % len(ARBITRUM_RPCS)
            logger.info(f"Switched to RPC endpoint: {ARBITRUM_RPCS[self.current_rpc_index]}")
            await asyncio.sleep(0.5)

        return None

    async def get_latest_block(self) -> int:
        """Returns the latest Arbitrum block number."""
        res = await self.rpc_call("eth_blockNumber", [])
        if res:
            return int(res, 16)
        return 0

    def classify_action(self, from_addr: str, to_addr: str) -> Tuple[str, str, Optional[str]]:
        """
        Classifies the transfer:
        - 🔴 NẠP LÊN SÀN (CEX Deposit)
        - 🟢 RÚT KHỎI SÀN (CEX Withdrawal)
        - 🔄 CHUYỂN VÍ CÁ MẬP (Whale Transfer)
        Returns (action_type, action_label, exchange_name)
        """
        from_label = self.labels.get(from_addr.lower())
        to_label = self.labels.get(to_addr.lower())

        # Check if destination is a CEX
        if to_label and to_label.get("type") == "cex":
            exchange = to_label.get("exchange") or to_label.get("name")
            return "DEPOSIT", "🔴 NẠP LÊN SÀN (CEX Deposit)", exchange

        # Check if source is a CEX
        if from_label and from_label.get("type") == "cex":
            exchange = from_label.get("exchange") or from_label.get("name")
            return "WITHDRAWAL", "🟢 RÚT KHỎI SÀN (CEX Withdrawal)", exchange

        # Whale to Whale or OTC
        return "TRANSFER", "🔄 CHUYỂN VÍ CÁ MẬP (Whale Transfer)", None

    def get_address_display(self, addr: str) -> str:
        """Returns friendly name if labeled, else shortened address."""
        low = addr.lower()
        if low in self.labels:
            return f"<b>{self.labels[low]['name']}</b> (<code>{addr[:6]}...{addr[-4:]}</code>)"
        return f"<code>{addr[:6]}...{addr[-4:]}</code>"

    async def send_telegram(self, message: str) -> bool:
        """Sends formatted Telegram message using credentials from config.json."""
        token = self.config.get("telegram_token")
        chat_id = self.config.get("telegram_chat_id")
        if not token or not chat_id:
            logger.warning("Telegram token or chat_id not configured in config.json")
            return False

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        logger.info("Whale Telegram alert sent successfully.")
                        return True
                    else:
                        resp_text = await resp.text()
                        logger.error(f"Telegram returned HTTP {resp.status}: {resp_text}")
                        return await self._send_telegram_curl(url, payload)
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")
            return await self._send_telegram_curl(url, payload)

    async def _send_telegram_curl(self, url: str, payload: dict) -> bool:
        """Fallback to curl.exe for sending Telegram messages."""
        try:
            curl_path = "curl.exe"
            data_json = json.dumps(payload)
            proc = await asyncio.create_subprocess_exec(
                curl_path,
                "-fsSL",
                "--connect-timeout", "10",
                "-H", "Content-Type: application/json",
                "-d", data_json,
                url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                logger.info("Telegram alert sent successfully via curl fallback.")
                return True
            else:
                logger.error(f"Curl fallback failed: {stderr.decode(errors='ignore')}")
                return False
        except Exception as e:
            logger.error(f"Error in curl fallback: {e}")
            return False

    async def send_test_alert(self) -> bool:
        """Sends a mock sample whale alert to verify Telegram connection."""
        arb_price = await self.get_arb_price()
        mock_msg = (
            "🚨 <b>[TEST] CẢNH BÁO BIẾN ĐỘNG VÍ CÁ MẬP ARBITRUM</b> 🚨\n\n"
            "💰 <b>Giá trị:</b> $2,500,000 USDT (Mẫu Thử Nghiệm)\n"
            "🏷️ <b>Hành động:</b> 🔴 <b>NẠP LÊN SÀN (CEX Deposit)</b>\n"
            "🏢 <b>Sàn liên quan:</b> Binance\n"
            "📤 <b>Ví gửi (From):</b> <code>0x3841...7a20</code> (Ví Cá Mập Lớn)\n"
            "📥 <b>Ví nhận (To):</b> <b>Binance: Hot Wallet 54</b> (<code>0xB38e...891D</code>)\n"
            "⛓️ <b>Mạng:</b> Arbitrum One\n"
            f"📊 <b>Giá ARB hiện tại:</b> ${arb_price:,.4f}\n"
            f"⏰ <b>Thời gian:</b> {(datetime.now(timezone.utc) + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')} (UTC+7)\n\n"
            "🔗 <a href=\"https://arbiscan.io/address/0xb38e8c17e38363af6ebdcb3dae12e0243582891d\">Kiểm tra ví sàn trên Arbiscan</a>\n"
            "<i>(Đây là thông báo mẫu kiểm tra kết nối bot từ Dashboard)</i>"
        )
        return await self.send_telegram(mock_msg)

    async def process_transfer(self, log: dict, token_symbol: str, decimals: int, arb_price: float):
        """Processes an on-chain Transfer log and sends an alert if value >= threshold."""
        min_usdt = float(self.config.get("whale_min_usdt", 1000000.0))
        tx_hash = log.get("transactionHash")
        log_index = int(log.get("logIndex", "0x0"), 16)
        tx_id = f"{tx_hash}_{log_index}"

        if tx_id in self.known_txs:
            return

        topics = log.get("topics", [])
        if len(topics) < 3:
            return

        # Extract addresses (padded 32 bytes to 20 bytes hex)
        from_addr = "0x" + topics[1][-40:]
        to_addr = "0x" + topics[2][-40:]
        
        # Raw value
        raw_val = int(log.get("data", "0x0"), 16)
        amount = raw_val / (10 ** decimals)

        if token_symbol == "USDT":
            usd_value = amount
            token_display = f"{amount:,.2f} USDT"
        elif token_symbol == "ARB":
            usd_value = amount * arb_price
            token_display = f"{amount:,.0f} ARB (~${usd_value:,.2f} USDT)"
        else:
            return

        # Check threshold
        if usd_value < min_usdt:
            return

        # Mark as seen
        self.known_txs.add(tx_id)

        # Classify
        action_type, action_label, exchange_name = self.classify_action(from_addr, to_addr)
        now_vn = datetime.now(timezone.utc) + timedelta(hours=7)
        time_str = now_vn.strftime('%Y-%m-%d %H:%M:%S')

        # Build History Record
        history_item = {
            "tx_hash": tx_hash,
            "log_index": log_index,
            "token": token_symbol,
            "amount": amount,
            "usd_value": usd_value,
            "action_type": action_type,
            "action_label": action_label,
            "exchange": exchange_name,
            "from_address": from_addr,
            "from_name": self.labels.get(from_addr.lower(), {}).get("name", "Ví Cá Mập"),
            "to_address": to_addr,
            "to_name": self.labels.get(to_addr.lower(), {}).get("name", "Ví Cá Mập"),
            "timestamp": time_str,
            "arbiscan_url": f"https://arbiscan.io/tx/{tx_hash}"
        }
        self.history.append(history_item)
        self.save_history()

        logger.info(f"🔥 WHALE DETECTED: {token_display} | {action_label} | Tx: {tx_hash}")

        # Format Telegram Message
        exchange_line = f"🏢 <b>Sàn liên quan:</b> {exchange_name}\n" if exchange_name else ""
        from_display = self.get_address_display(from_addr)
        to_display = self.get_address_display(to_addr)

        msg = (
            "🚨 <b>CẢNH BÁO BIẾN ĐỘNG VÍ CÁ MẬP ARBITRUM</b> 🚨\n\n"
            f"💰 <b>Giá trị:</b> ${usd_value:,.2f} USDT\n"
            f"🪙 <b>Tài sản:</b> {token_display}\n"
            f"🏷️ <b>Hành động:</b> <b>{action_label}</b>\n"
            f"{exchange_line}"
            f"📤 <b>Ví gửi (From):</b> {from_display}\n"
            f"📥 <b>Ví nhận (To):</b> {to_display}\n"
            "⛓️ <b>Mạng:</b> Arbitrum One\n"
            f"⏰ <b>Thời gian:</b> {time_str} (UTC+7)\n\n"
            f"🔗 <a href=\"https://arbiscan.io/tx/{tx_hash}\">Xem chi tiết giao dịch trên Arbiscan</a>"
        )

        await self.send_telegram(msg)

    async def scan_range(self, from_block: int, to_block: int):
        """Scans Arbitrum logs for USDT and ARB tokens in a block range."""
        arb_price = await self.get_arb_price()
        track_usdt = self.config.get("whale_track_usdt", True)
        track_arb = self.config.get("whale_track_arb", True)

        tasks = []
        if track_usdt:
            tasks.append(("USDT", USDT_CONTRACT, USDT_DECIMALS))
        if track_arb:
            tasks.append(("ARB", ARB_CONTRACT, ARB_DECIMALS))

        for symbol, contract, decimals in tasks:
            filter_param = {
                "address": contract,
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "topics": [TRANSFER_TOPIC]
            }
            logs = await self.rpc_call("eth_getLogs", [filter_param])
            if logs and isinstance(logs, list):
                for log in logs:
                    await self.process_transfer(log, symbol, decimals, arb_price)

    async def run_scan_cycle(self):
        """Performs one scan cycle comparing current block to last scanned block."""
        self._load_config()
        if not self.config.get("whale_monitor_enabled", True):
            return

        latest_block = await self.get_latest_block()
        if latest_block <= 0:
            logger.warning("Could not retrieve latest Arbitrum block.")
            return

        if self.last_scanned_block == 0:
            # First initialization: look back last 100 blocks (~30 seconds)
            self.last_scanned_block = latest_block - 100

        from_block = self.last_scanned_block + 1
        to_block = latest_block

        if from_block > to_block:
            return

        # Cap scan range to maximum 2,000 blocks to prevent RPC limits
        if to_block - from_block > 2000:
            from_block = to_block - 2000

        logger.info(f"Scanning Arbitrum blocks {from_block} -> {to_block} ({to_block - from_block + 1} blocks)...")
        await self.scan_range(from_block, to_block)
        self.last_scanned_block = to_block

    async def run_loop(self):
        """Main continuous background monitor loop."""
        self.is_running = True
        logger.info("Arbitrum Whale Monitor Bot started.")
        while self.is_running:
            try:
                await self.run_scan_cycle()
                interval = int(self.config.get("whale_scan_interval_sec", 15))
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.info("Arbitrum Whale Monitor loop stopped.")
                self.is_running = False
                break
            except Exception as e:
                logger.error(f"Unexpected error in whale loop: {e}", exc_info=True)
                await asyncio.sleep(10)


# Standalone runner for testing or running directly
if __name__ == "__main__":
    bot = ArbitrumWhaleMonitor()
    logger.info("Running Arbitrum Whale Monitor in standalone CLI mode...")
    try:
        asyncio.run(bot.run_loop())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
