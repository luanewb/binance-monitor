@echo off
title Binance Spot Monitor & Watchlist Dashboard
echo ========================================================
echo   Binance Spot Monitor & Watchlist Dashboard v2.5.14
echo ========================================================
echo.
echo Starting FastAPI Dashboard using Uvicorn...
echo Access the dashboard in your web browser at:
echo.
echo   =====>  http://127.0.0.1:8080  <=====
echo.
echo Press Ctrl+C in this terminal to stop the server.
echo ========================================================
echo.

python -m uvicorn dashboard:app --host 0.0.0.0 --port 8080

pause
