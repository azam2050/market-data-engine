import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
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
TOP_CALLS = int(os.getenv("TOP_CALLS", "3"))
TOP_PUTS = int(os.getenv("TOP_PUTS", "3"))

OPTION_MIN_VOLUME = int(os.getenv("OPTION_MIN_VOLUME", "10"))
OPTION_MIN_OPEN_INTEREST = int(os.getenv("OPTION_MIN_OPEN_INTEREST", "50"))
MAX_STRIKE_DISTANCE_PCT = float(os.getenv("MAX_STRIKE_DISTANCE_PCT", "1.5"))

WS_STOCKS_URL = "wss://socket.polygon.io/stocks"
REST_BASE_URL = "https://api.polygon.io"
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
last_options_candidates: List[Dict[str, Any]] = []
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


def get_today_and_tomorrow_dates() -> List[str]:
    now = datetime.now(timezone.utc)
    today = now.date()
    tomorrow = today + timedelta(days=1)
    return [today.isoformat(), tomorrow.isoformat()]


def dte_from_expiry(expiry: Optional[str]) -> Optional[int]:
    if not expiry:
        return None
    try:
        exp = datetime.fromisoformat(expiry).date()
        now = datetime.now(timezone.utc).date()
        return (exp - now).days
    except Exception:
        return None


def strike_distance_pct(spot: float, strike: float) -> float:
    if spot <= 0:
        return 999.0
    return abs(strike - spot) / spot * 100.0


def is_option_liquid(volume: int, open_interest: int, bid: float, ask: float) -> bool:
    if bid <= 0 or ask <= 0:
        return False
    if volume < OPTION_MIN_VOLUME and open_interest < OPTION_MIN_OPEN_INTEREST:
        return False
    return True


def rank_option(contract: Dict[str, Any], spot_price: float) -> tuple:
    strike = safe_float(contract.get("strike")) or 0.0
    bid = safe_float(contract.get("bid")) or 0.0
    ask = safe_float(contract.get("ask")) or 0.0
    volume = int(contract.get("volume") or 0)
    oi = int(contract.get("open_interest") or 0)

    dist = strike_distance_pct(spot_price, strike)
    spread_pct = ((ask - bid) / max((ask + bid) / 2.0, 0.01)) * 100.0 if ask >= bid else 999.0

    # أقرب strike + سبريد أقل + سيولة أعلى
    return (dist, spread_pct, -volume, -oi)


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
# Polygon REST options
# =========================
async def fetch_json(session: aiohttp.ClientSession, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(params)
    params["apiKey"] = API_KEY

    url = f"{REST_BASE_URL}{path}"
    async with session.get(url, params=params, timeout=HTTP_TIMEOUT_SEC) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"GET {url} failed: {resp.status} {text[:300]}")
        return await resp.json()


async def fetch_options_for_expiry(
    session: aiohttp.ClientSession,
    expiry: str,
    side: str,
) -> List[Dict[str, Any]]:
    data = await fetch_json(
        session,
        f"/v3/snapshot/options/{PRIMARY_SYMBOL}",
        {
            "expiration_date": expiry,
            "contract_type": side,
            "limit": 250,
        },
    )

    results = data.get("results") or []
    cleaned: List[Dict[str, Any]] = []

    for item in results:
        details = item.get("details") or {}
        quote = item.get("last_quote") or {}
        greeks = item.get("greeks") or {}
        day = item.get("day") or {}

        strike = safe_float(details.get("strike_price"))
        bid = safe_float(quote.get("bid"))
        ask = safe_float(quote.get("ask"))
        volume = int(day.get("volume") or 0)
        open_interest = int(item.get("open_interest") or 0)

        if strike is None or bid is None or ask is None:
            continue

        if not is_option_liquid(volume, open_interest, bid, ask):
            continue

        cleaned.append(
            {
                "contract_symbol": details.get("ticker"),
                "side": "CALL" if side.lower() == "call" else "PUT",
                "strike": strike,
                "expiry": details.get("expiration_date"),
                "dte": dte_from_expiry(details.get("expiration_date")),
                "bid": bid,
                "ask": ask,
                "mid": round((bid + ask) / 2.0, 4),
                "volume": volume,
                "open_interest": open_interest,
                "delta": safe_float(greeks.get("delta")),
                "iv": safe_float(item.get("implied_volatility")),
            }
        )

    return cleaned


async def refresh_options_loop() -> None:
    global last_options_candidates, last_error

    while True:
        try:
            await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)

            qqq_bar = latest_minute_bars.get(PRIMARY_SYMBOL)
            if not qqq_bar:
                log.warning("Options refresh skipped: missing QQQ bar")
                continue

            spot_price = safe_float(qqq_bar.get("close")) or 0.0
            if spot_price <= 0:
                log.warning("Options refresh skipped: invalid QQQ close")
                continue

            expiries = get_today_and_tomorrow_dates()

            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                tasks = []
                for expiry in expiries:
                    tasks.append(fetch_options_for_expiry(session, expiry, "call"))
                    tasks.append(fetch_options_for_expiry(session, expiry, "put"))

                results = await asyncio.gather(*tasks, return_exceptions=True)

            calls: List[Dict[str, Any]] = []
            puts: List[Dict[str, Any]] = []

            for result in results:
                if isinstance(result, Exception):
                    log.warning("Options fetch error: %s", result)
                    continue

                for contract in result:
                    dte = contract.get("dte")
                    if dte not in (0, 1):
                        continue

                    strike = safe_float(contract.get("strike")) or 0.0
                    if strike_distance_pct(spot_price, strike) > MAX_STRIKE_DISTANCE_PCT:
                        continue

                    if contract["side"] == "CALL":
                        calls.append(contract)
                    else:
                        puts.append(contract)

            calls = sorted(calls, key=lambda x: rank_option(x, spot_price))[:TOP_CALLS]
            puts = sorted(puts, key=lambda x: rank_option(x, spot_price))[:TOP_PUTS]

            last_options_candidates = calls + puts
            log.info(
                "Options shortlist refreshed | calls=%s puts=%s total=%s",
                len(calls),
                len(puts),
                len(last_options_candidates),
            )

        except Exception as e:
            last_error = f"options_error: {e}"
            log.exception("Options refresh loop failed")


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
        "options_candidates": last_options_candidates,
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
        async with session.post(BOT_WEBHOOK_URL, json=payload, timeout=HTTP_TIMEOUT_SEC) as resp:
            last_delivery_status = resp.status
            text = await resp.text()

            if 200 <= resp.status < 300:
                log.info(
                    "Snapshot sent | leaders=%s | options=%s | status=%s",
                    len(payload["leaders"]),
                    len(payload["options_candidates"]),
                    resp.status,
                )
            else:
                last_error = f"webhook_failed:{resp.status}:{text[:200]}"
                log.error("Webhook failed: %s | %s", resp.status, text[:200])

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
    log.info("Top calls: %s | Top puts: %s", TOP_CALLS, TOP_PUTS)
    log.info("Webhook configured: %s", bool(BOT_WEBHOOK_URL))
    log.info("==========================================")

    await asyncio.gather(
        websocket_loop(),
        refresh_options_loop(),
        snapshot_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
