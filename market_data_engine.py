import os
import json
import asyncio
from datetime import datetime

import aiohttp
import websockets

API_KEY = os.getenv("POLYGON_API_KEY")
WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

SYMBOLS = [
    "QQQ",
    "SPY",
    "AAPL",
    "NVDA",
    "MSFT",
    "TSLA",
    "AMD",
    "META",
    "AMZN",
    "GOOGL",
    "AVGO",
    "NFLX"
]

WS_URL = "wss://socket.polygon.io/stocks"

latest_prices = {}


async def send_snapshot(session):
    snapshot = {
        "timestamp": datetime.utcnow().isoformat(),
        "symbols": latest_prices
    }

    try:
        async with session.post(WEBHOOK_URL, json=snapshot) as resp:
            print("Snapshot sent:", resp.status)
    except Exception as e:
        print("Webhook error:", str(e))


async def snapshot_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(5)

            if latest_prices and WEBHOOK_URL:
                await send_snapshot(session)
            else:
                print("Waiting for prices or webhook URL...")


async def websocket_loop():
    while True:
        try:
            print("Connecting to Polygon WebSocket...")
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

                auth_response = await ws.recv()
                print("Auth response:", auth_response)

                # 2) Subscribe to aggregate bars for selected symbols
                subs = ",".join([f"A.{s}" for s in SYMBOLS])

                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": subs
                }))

                sub_response = await ws.recv()
                print("Subscribe response:", sub_response)

                # 3) Read messages forever
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)

                    if isinstance(data, list):
                        for ev in data:
                            sym = ev.get("sym")
                            close_price = ev.get("c")

                            if sym in SYMBOLS and close_price is not None:
                                latest_prices[sym] = close_price

                        if latest_prices:
                            print("Updated prices:", latest_prices)

        except Exception as e:
            print("WebSocket error:", str(e))
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


async def main():
    if not API_KEY:
        raise ValueError("POLYGON_API_KEY is missing")

    if not WEBHOOK_URL:
        print("Warning: N8N_WEBHOOK_URL is missing. Snapshot sending will wait.")

    await asyncio.gather(
        websocket_loop(),
        snapshot_loop()
    )


if __name__ == "__main__":
    asyncio.run(main())
