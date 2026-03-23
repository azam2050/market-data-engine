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

# ── Storage ───────────────────────────────────────────────────
latest_bars: dict = {}


# ── Send Snapshot ─────────────────────────────────────────────
async def send_snapshot(session: aiohttp.ClientSession):
    if not latest_bars:
        return

    timestamp = datetime.now(timezone.utc).isoformat()

    payload = {
        "timestamp": timestamp,
        "bars": [
            {
                "symbol":    symbol,
                "open":      bar.get("open"),
                "high":      bar.get("high"),
                "low":       bar.get("low"),
                "close":     bar.get("close"),
                "volume":    bar.get("volume"),
                "vwap":      bar.get("vwap"),
                "trades":    bar.get("trades"),
                "bar_start": bar.get("bar_start_ts"),
                "bar_end":   bar.get("bar_end_ts"),
            }
            for symbol, bar in latest_bars.items()
        ]
    }

    try:
        async with session.post(WEBHOOK_URL, json=payload) as resp:
            print(f"✅ Snapshot sent: {len(latest_bars)} symbols → {resp.status}")
    except Exception as e:
        print("Webhook error:", str(e))


# ── Snapshot Loop ─────────────────────────────────────────────
async def snapshot_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(10)
            if latest_bars and WEBHOOK_URL:
                await send_snapshot(session)
            else:
                print("Waiting for data or webhook URL...")


# ── WebSocket Loop ────────────────────────────────────────────
async def websocket_loop():
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10
            ) as ws:
                # 1) Auth
                await ws.send(json.dumps({
                    "action": "auth",
                    "params": API_KEY
                }))
                auth_resp = await ws.recv()
                print("Auth:", auth_resp)

                # 2) Subscribe
                subs = ",".join([f"A.{s}" for s in SYMBOLS])
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": subs
                }))
                sub_resp = await ws.recv()
                print("Subscribed:", sub_resp)

                # 3) Read messages forever
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    for event in data:
                        if event.get("ev") == "A":
                            sym = event.get("sym")
                            latest_bars[sym] = {
                                "open":         event.get("o"),
                                "high":         event.get("h"),
                                "low":          event.get("l"),
                                "close":        event.get("c"),
                                "volume":       event.get("v"),
                                "vwap":         event.get("vw"),
                                "trades":       event.get("z"),
                                "bar_start_ts": event.get("s"),
                                "bar_end_ts":   event.get("e"),
                            }

        except Exception as e:
            print("WebSocket error:", str(e))
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


# ── Main ──────────────────────────────────────────────────────
async def main():
    if not API_KEY:
        raise ValueError("POLYGON_API_KEY is missing")
    if not WEBHOOK_URL:
        print("Warning: N8N_WEBHOOK_URL is missing")

    await asyncio.gather(
        websocket_loop(),
        snapshot_loop()
    )


if __name__ == "__main__":
    asyncio.run(main())
