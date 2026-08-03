"""REST client for Massive (formerly Polygon.io).

Only the endpoints the engine actually needs. Every call is retried with
exponential backoff, and rate-limit responses are honoured rather than hammered.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime
from typing import Any

import httpx

from qqq_alpha.config import Settings, get_settings
from qqq_alpha.domain import Bar, FlowEvent, FlowKind, OptionContract, OptionType

log = logging.getLogger(__name__)

MAX_RETRIES = 4
BACKOFF_BASE_SEC = 1.5


class MassiveError(RuntimeError):
    pass


def _ms_to_dt(value: float | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def parse_occ_symbol(occ: str) -> tuple[str, date, OptionType, float]:
    """Decode an OCC symbol, e.g. ``O:QQQ260802C00485000``.

    Returns (underlying, expiry, option_type, strike).
    """
    body = occ.removeprefix("O:")
    # strike is the trailing 8 digits, type the char before, date the 6 before that
    strike = int(body[-8:]) / 1000.0
    option_type = OptionType.CALL if body[-9].upper() == "C" else OptionType.PUT
    expiry_raw = body[-15:-9]
    expiry = date(2000 + int(expiry_raw[:2]), int(expiry_raw[2:4]), int(expiry_raw[4:6]))
    underlying = body[:-15]
    return underlying, expiry, option_type, strike


class MassiveClient:
    def __init__(self, settings: Settings | None = None, client: httpx.AsyncClient | None = None):
        self.settings = settings or get_settings()
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> MassiveClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.settings.massive_rest_url, timeout=30.0
            )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._client is None:
            raise MassiveError("client not started; use `async with MassiveClient() as c:`")
        if not self.settings.massive_api_key:
            raise MassiveError("MASSIVE_API_KEY is not configured")

        query = dict(params or {})
        query["apiKey"] = self.settings.massive_api_key

        last_error: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                response = await self._client.get(path, params=query)
                if response.status_code == 429:
                    wait = BACKOFF_BASE_SEC * (2**attempt)
                    log.warning("rate limited on %s, sleeping %.1fs", path, wait)
                    await asyncio.sleep(wait)
                    continue
                if response.status_code >= 400:
                    raise MassiveError(f"GET {path} -> {response.status_code}: {response.text[:300]}")
                return response.json()
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_error = exc
                await asyncio.sleep(BACKOFF_BASE_SEC * (2**attempt))
        raise MassiveError(f"GET {path} failed after {MAX_RETRIES} attempts: {last_error}")

    # ------------------------------------------------------------------
    # Equities
    # ------------------------------------------------------------------
    async def minute_bars(self, symbol: str, day: date) -> list[Bar]:
        """All 1-minute bars for one symbol on one calendar day."""
        payload = await self._get(
            f"/v2/aggs/ticker/{symbol}/range/1/minute/{day.isoformat()}/{day.isoformat()}",
            {"adjusted": "true", "sort": "asc", "limit": 50_000},
        )
        return [
            Bar(
                symbol=symbol,
                ts=_ms_to_dt(row.get("t")),
                open=float(row["o"]),
                high=float(row["h"]),
                low=float(row["l"]),
                close=float(row["c"]),
                volume=int(row.get("v") or 0),
                vwap=float(row["vw"]) if row.get("vw") is not None else None,
            )
            for row in payload.get("results") or []
        ]

    async def daily_bars(self, symbol: str, start: date, end: date) -> list[Bar]:
        payload = await self._get(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{start.isoformat()}/{end.isoformat()}",
            {"adjusted": "true", "sort": "asc", "limit": 5000},
        )
        return [
            Bar(
                symbol=symbol,
                ts=_ms_to_dt(row.get("t")),
                open=float(row["o"]),
                high=float(row["h"]),
                low=float(row["l"]),
                close=float(row["c"]),
                volume=int(row.get("v") or 0),
                vwap=float(row["vw"]) if row.get("vw") is not None else None,
            )
            for row in payload.get("results") or []
        ]

    # ------------------------------------------------------------------
    # Options
    # ------------------------------------------------------------------
    async def option_chain(
        self, underlying: str, expiry: date, option_type: OptionType | None = None
    ) -> list[OptionContract]:
        params: dict[str, Any] = {"expiration_date": expiry.isoformat(), "limit": 250}
        if option_type is not None:
            params["contract_type"] = option_type.value.lower()

        payload = await self._get(f"/v3/snapshot/options/{underlying}", params)
        contracts: list[OptionContract] = []

        for item in payload.get("results") or []:
            details = item.get("details") or {}
            quote = item.get("last_quote") or {}
            trade = item.get("last_trade") or {}
            greeks = item.get("greeks") or {}
            day = item.get("day") or {}

            ticker = details.get("ticker")
            if not ticker:
                continue

            contracts.append(
                OptionContract(
                    occ_symbol=ticker,
                    underlying=underlying,
                    option_type=(
                        OptionType.CALL
                        if str(details.get("contract_type", "")).lower() == "call"
                        else OptionType.PUT
                    ),
                    strike=float(details.get("strike_price") or 0.0),
                    expiry=date.fromisoformat(details["expiration_date"]),
                    bid=_safe_float(quote.get("bid")),
                    ask=_safe_float(quote.get("ask")),
                    last=_safe_float(trade.get("price")),
                    volume=int(day.get("volume") or 0),
                    open_interest=int(item.get("open_interest") or 0),
                    implied_volatility=_safe_float(item.get("implied_volatility")),
                    delta=_safe_float(greeks.get("delta")),
                    gamma=_safe_float(greeks.get("gamma")),
                    theta=_safe_float(greeks.get("theta")),
                )
            )
        return contracts

    async def option_minute_bars(self, occ_symbol: str, day: date) -> list[Bar]:
        """Historical 1-minute bars for a single contract — the backtest's price source."""
        payload = await self._get(
            f"/v2/aggs/ticker/{occ_symbol}/range/1/minute/{day.isoformat()}/{day.isoformat()}",
            {"sort": "asc", "limit": 50_000},
        )
        return [
            Bar(
                symbol=occ_symbol,
                ts=_ms_to_dt(row.get("t")),
                open=float(row["o"]),
                high=float(row["h"]),
                low=float(row["l"]),
                close=float(row["c"]),
                volume=int(row.get("v") or 0),
                vwap=float(row["vw"]) if row.get("vw") is not None else None,
            )
            for row in payload.get("results") or []
        ]

    async def option_trades(
        self, occ_symbol: str, day: date, limit: int = 50_000
    ) -> list[FlowEvent]:
        """Raw prints for one contract — the raw material for whale detection.

        Requires the Developer tier or above.
        """
        payload = await self._get(
            f"/v3/trades/{occ_symbol}",
            {
                "timestamp.gte": f"{day.isoformat()}T00:00:00Z",
                "timestamp.lte": f"{day.isoformat()}T23:59:59Z",
                "limit": min(limit, 50_000),
                "sort": "timestamp",
                "order": "asc",
            },
        )
        underlying, expiry, option_type, strike = parse_occ_symbol(occ_symbol)
        events: list[FlowEvent] = []
        for row in payload.get("results") or []:
            price = _safe_float(row.get("price")) or 0.0
            size = int(row.get("size") or 0)
            if price <= 0 or size <= 0:
                continue
            events.append(
                FlowEvent(
                    ts=_ms_to_dt((row.get("sip_timestamp") or 0) / 1_000_000),
                    occ_symbol=occ_symbol,
                    underlying=underlying,
                    option_type=option_type,
                    strike=strike,
                    expiry=expiry,
                    price=price,
                    size=size,
                    premium=round(price * size * 100, 2),
                    kind=FlowKind.BLOCK if size >= 250 else FlowKind.NORMAL,
                )
            )
        return events


def _safe_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None
