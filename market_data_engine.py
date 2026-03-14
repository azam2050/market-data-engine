import os
import json
import asyncio
from datetime import datetime
import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("MASSIVE_API_KEY")
WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

SYMBOLS = ["QQQ","AAPL","NVDA","MSFT","TSLA","AMD"]

WS_URL = "wss://socket.polygon.io/stocks"

latest_prices = {}

async def send_snapshot(session):
    snapshot = {
        "timestamp": datetime.utcnow().isoformat(),
        "symbols": latest_prices
    }

    try:
        async with session.post(WEBHOOK_URL, json=snapshot) as resp:
            print("Snapshot sent", resp.status)
    except Exception as e:
        print("Webhook error", e)

async def snapshot_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(5)
            if latest_prices:
                await send_snapshot(session)

async def websocket_loop():
    async with websockets.connect(WS_URL) as ws:

        await ws.send(json.dumps({
            "action":"auth",
            "params":API_KEY
        }))

        await ws.recv()

        subs = ",".join([f"A.{s}" for s in SYMBOLS])

        await ws.send(json.dumps({
            "action":"subscribe",
            "params":subs
        }))

        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            if isinstance(data,list):
                for ev in data:
                    if ev.get("sym") in SYMBOLS:
                        latest_prices[ev["sym"]] = ev.get("c")

async def main():
    await asyncio.gather(
        websocket_loop(),
        snapshot_loop()
    )

asyncio.run(main())
