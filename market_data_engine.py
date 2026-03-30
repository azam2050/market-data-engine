import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import websockets


# =========================
# Config
# =========================
API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
BOT_WEBHOOK_URL = os.getenv("BOT_WEBHOOK_URL", "").strip()

PRIMARY_SYMBOL = os.getenv("PRIMARY_SYMBOL", "QQQ").strip().upper()
LEADERS = [
    s.strip().upper()
    for s in os.getenv(
        "LEADER_SYMBOLS",
        "AAPL,MSFT,NVDA,AMZN,GOOGL,META,AVGO,TSLA",
    ).split(",")
    if s.strip()
]

SNAPSHOT_INTERVAL_SEC = int(os.getenv("SNAPSHOT_INTERVAL_SEC", "60"))

WS_STOCKS_URL = "wss://socket.polygon.io/stocks"
HTTP_TIMEOUT_SEC = int(os.getenv("HTTP_TIMEOUT_SEC", "20"))


# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("market-data-engine")


# =========================
# Runtime State
# =========================
latest_minute_bars: Dict[str, Dict[str, Any]] = {}
last_delivery_status: Optional[int] = None
last_error: Optional[str] = None


# =========================
# Helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def payload_summary(payload: Dict[str, Any]) -> str:
    underlying = payload.get("underlying") or {}
    symbol = underlying.get("symbol")
    close = underlying.get("close")
    leaders_count = len(payload.get("leaders") or [])
    return (
        f"underlying={symbol} close={close} "
        f"leaders={leaders_count} options=0 (disabled)"
    )


# =========================
# WebSocket bars (1m)
# =========================
async def websocket_loop() -> None:
    global last_error

    symbols = [PRIMARY_SYMBOL] + LEADERS
    subs = ",".join([f"AM.{s}" for s in symbols])

    while True:
        try:
            async with websockets.connect(
                WS_STOCKS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as ws:
                await ws.send(json.dumps({"action": "auth", "params": API_KEY}))
                auth_resp = await ws.recv()
                log.info("Auth response: %s", auth_resp)

                await ws.send(json.dumps({"action": "subscribe", "params": subs}))
                sub_resp = await ws.recv()
                log.info("Subscribed response: %s", sub_resp)

                while True:
                    raw_msg = await ws.recv()
                    events = json.loads(raw_msg)

                    for event in events:
                        if event.get("ev") != "AM":
                            continue

                        symbol = event.get("sym")
                        if symbol not in symbols:
                            continue

                        ts_ms = event.get("e") or event.get("s")
                        ts_iso = (
                            datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                            .replace(microsecond=0)
                            .isoformat()
                            .replace("+00:00", "Z")
                            if ts_ms
                            else utc_now_iso()
                        )

                        latest_minute_bars[symbol] = {
                            "symbol": symbol,
                            "timeframe": "1m",
                            "open": safe_float(event.get("o")),
                            "high": safe_float(event.get("h")),
                            "low": safe_float(event.get("l")),
                            "close": safe_float(event.get("c")),
                            "volume": int(event.get("v") or 0),
                            "vwap": safe_float(event.get("vw")),
                            "timestamp": ts_iso,
                        }

        except Exception as e:
            last_error = f"websocket_error: {e}"
            log.exception("WebSocket loop failed")
            await asyncio.sleep(5)


# =========================
# Payload build + send
# =========================
def build_payload() -> Optional[Dict[str, Any]]:
    qqq_bar = latest_minute_bars.get(PRIMARY_SYMBOL)
    if not qqq_bar:
        return None

    leaders_payload = [latest_minute_bars[s] for s in LEADERS if s in latest_minute_bars]

    payload = {
        "timestamp": utc_now_iso(),
        "timeframe": "1m",
        "underlying": qqq_bar,
        "leaders": leaders_payload,
        "options_candidates": [],  # Disabled: no options subscription
    }
    return payload


async def send_snapshot(session: aiohttp.ClientSession) -> None:
    global last_delivery_status, last_error

    payload = build_payload()
    if not payload:
        log.warning("Snapshot skipped: missing underlying")
        return

    if not BOT_WEBHOOK_URL:
        log.warning("BOT_WEBHOOK_URL missing; skipping send")
        return

    try:
        log.info("Sending webhook | %s", payload_summary(payload))

        async with session.post(BOT_WEBHOOK_URL, json=payload, timeout=HTTP_TIMEOUT_SEC) as resp:
            last_delivery_status = resp.status
            text = await resp.text()

            if 200 <= resp.status < 300:
                log.info(
                    "Webhook sent successfully | status=%s | %s",
                    resp.status,
                    payload_summary(payload),
                )
            else:
                last_error = f"webhook_failed:{resp.status}:{text[:200]}"
                log.error(
                    "Webhook failed | status=%s | response=%s | %s",
                    resp.status,
                    text[:200],
                    payload_summary(payload),
                )

    except Exception as e:
        last_error = f"webhook_exception:{e}"
        log.exception("Snapshot send failed")


async def snapshot_loop() -> None:
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)
                await send_snapshot(session)
            except Exception:
                log.exception("Snapshot loop failed")


# =========================
# Main
# =========================
async def main() -> None:
    if not API_KEY:
        raise RuntimeError("POLYGON_API_KEY is missing")

    log.info("==========================================")
    log.info("QQQ Market Data Engine starting")
    log.info("Primary: %s", PRIMARY_SYMBOL)
    log.info("Leaders: %s", ", ".join(LEADERS))
    log.info("Snapshot interval: %ss", SNAPSHOT_INTERVAL_SEC)
    log.info("Options: DISABLED (no subscription)")
    log.info("Webhook configured: %s", bool(BOT_WEBHOOK_URL))
    log.info("Webhook URL: %s", BOT_WEBHOOK_URL if BOT_WEBHOOK_URL else "NOT SET")
    log.info("==========================================")

    await asyncio.gather(
        websocket_loop(),
        snapshot_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
