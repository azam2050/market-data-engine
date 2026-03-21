import os
import json
import asyncio
from datetime import datetime, timezone

import aiohttp
import websockets

# ── Config ────────────────────────────────────────────────────
API_KEY     = os.getenv("POLYGON_API_KEY")
WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

SYMBOLS = [
    "QQQ", "SPY", "AAPL", "NVDA", "MSFT",
    "TSLA", "AMD", "META", "AMZN", "GOOGL", "AVGO", "NFLX"
]

WS_URL = "wss://socket.polygon.io/stocks"

# ── Storage: OHLCV كاملة لكل رمز ─────────────────────────────
latest_bars: dict = {}


# ── Send Snapshot ─────────────────────────────────────────────
async def send_snapshot(session: aiohttp.ClientSession):
    timestamp = datetime.now(timezone.utc).isoformat()

    for symbol, bar in list(latest_bars.items()):
        payload = {
            "timestamp":  timestamp,
            "symbol":     symbol,
            "open":       bar.get("open"),
            "high":       bar.get("high"),
            "low":        bar.get("low"),
            "close":      bar.get("close"),
            "volume":     bar.get("volume"),
            "vwap":       bar.get("vwap"),
            "trades":     bar.get("trades"),
            "bar_start":  bar.get("bar_start_ts"),
            "bar_end":    bar.get("bar_end_ts"),
        }
        try:
            async with session.post(WEBHOOK_URL, json=payload) as resp:
                print(f"✅ [{symbol}] O={bar.get('open')} H={bar.get('high')} "
                      f"L={bar.get('low')} C={bar.get('close')} "
                      f"V={bar.get('volume')} → {resp.status}")
        except Exception as e:
            print(f"Webhook error [{symbol}]:", str(e))
