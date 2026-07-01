#!/usr/bin/env python3
"""
Binance Spot Monitor & Watchlist Web Dashboard
Provides a web interface to control the Binance Spot H1 anomaly detector
and run/manage the 3 existing watchlist scripts.

Version: 2.5.2
"""

import asyncio
import json
import os
import sys
import logging
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Add local path to import binance_monitor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from binance_monitor import BinanceSpotMonitor, CONFIG_FILE, ALERTS_FILE

# Logger Setup
logger = logging.getLogger("Dashboard")

app = FastAPI(title="Binance Spot Monitor Dashboard", version="2.5.2")

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
            
            # Sleep second-by-second so we can interrupt quickly if stopped
            for _ in range(max(1, int(scan_interval))):
                await asyncio.sleep(1)
                # check if state changed in memory
                if not monitor_instance.config.get("is_running", False):
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

# REST APIs

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
        "version": "2.5.2"
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard:app", host="127.0.0.1", port=8080, reload=True)
