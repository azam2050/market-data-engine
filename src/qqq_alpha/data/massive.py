"""REST client for Massive (formerly Polygon.io).

Only the endpoints the engine actually needs. Every call is retried with
exponential backoff, and rate-limit responses are honoured rather than hammered.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import httpx

from qqq_alpha.config import Settings, get_settings
from qqq_alpha.domain import Bar, FlowEvent, FlowKind, OptionContract, OptionType

if TYPE_CHECKING:
    from qqq_alpha.data.quality import DataQuality

log = logging.getLogger(__name__)


@dataclass
class TradingSession:
    """A cleaned trading day: regular hours isolated, quality assessed."""

    symbol: str
    day: date
    regular: list[Bar] = field(default_factory=list)
    premarket: list[Bar] = field(default_factory=list)
    afterhours: list[Bar] = field(default_factory=list)
    premarket_high: float | None = None
    premarket_low: float | None = None
    quality: DataQuality | None = None

    @property
    def is_usable(self) -> bool:
        return bool(self.regular) and (self.quality is None or self.quality.is_usable)

    @property
    def daily_bar(self) -> Bar | None:
        """The whole regular session rolled into one daily candle.

        Yesterday's high, low and close are the levels every desk on the street
        has drawn on its chart, and the classic pivot is computed from exactly
        these three numbers. Rolling them up here — from the same minute bars
        everything else uses — keeps them arithmetically consistent with the
        intraday view rather than coming from a second, differently-adjusted
        endpoint.
        """
        if not self.regular:
            return None
        return Bar(
            symbol=self.symbol,
            ts=self.regular[-1].ts,
            open=self.regular[0].open,
            high=max(b.high for b in self.regular),
            low=min(b.low for b in self.regular),
            close=self.regular[-1].close,
            volume=sum(b.volume for b in self.regular),
        )

MAX_RETRIES = 4
BACKOFF_BASE_SEC = 1.5

# The provider caps one chain page at 250 contracts and offers ``next_url``
# for the rest. A 0DTE QQQ or SPY chain has more strikes than that on a
# single side, and the page comes back in ascending strike order — so an
# unpaged request returns the *lowest* strikes and can miss the money
# entirely. Both guards below exist for that: a strike window around the
# spot when the caller knows it, and following the pages when it does not.
CHAIN_PAGE_LIMIT = 250
CHAIN_MAX_PAGES = 8
# ±12% of spot covers every strike a day trade can want, and on a 0DTE
# chain it is a few dozen contracts instead of several hundred
CHAIN_STRIKE_WINDOW = 0.12


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
        """Raw 1-minute bars for one calendar day.

        Note this spans 04:00-20:00 ET: the provider includes extended hours and
        offers no parameter to exclude them. Use `session()` unless you
        specifically want pre/post-market prints.
        """
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
                transactions=int(row["n"]) if row.get("n") is not None else None,
            )
            for row in payload.get("results") or []
        ]

    async def range_minute_bars(
        self, symbol: str, minutes: int, start: date, end: date
    ) -> list[Bar]:
        """N-minute bars across a date span in one request (up to 50k rows).

        Includes extended hours — the provider offers no RTH-only switch, so
        callers that need the regular session must filter by timestamp.
        """
        payload = await self._get(
            f"/v2/aggs/ticker/{symbol}/range/{minutes}/minute/"
            f"{start.isoformat()}/{end.isoformat()}",
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
                transactions=int(row["n"]) if row.get("n") is not None else None,
            )
            for row in payload.get("results") or []
        ]

    async def last_prices(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        """The last trade for several tickers in one request.

        This is the number a screen shows as "the price now": it moves with
        every print, not once a minute like an aggregate bar. One request
        covers the whole list, so polling it costs the same for two symbols
        as for one.
        """
        if not symbols:
            return {}
        payload = await self._get(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            {"tickers": ",".join(sorted({s.upper() for s in symbols}))},
        )
        out: dict[str, dict[str, Any]] = {}
        for row in payload.get("tickers") or []:
            ticker = str(row.get("ticker") or "").upper()
            if not ticker:
                continue
            trade = row.get("lastTrade") or {}
            price = _safe_float(trade.get("p"))
            if price is None or price <= 0:
                # no print yet today (pre-open): the day's close is honest
                price = _safe_float((row.get("day") or {}).get("c")) or _safe_float(
                    (row.get("prevDay") or {}).get("c")
                )
            if price is None or price <= 0:
                continue
            # the provider stamps trades in nanoseconds
            stamp = trade.get("t") or trade.get("sip_timestamp")
            out[ticker] = {
                "price": round(price, 4),
                "ts": (
                    datetime.fromtimestamp(float(stamp) / 1e9, tz=UTC).isoformat()
                    if stamp else None
                ),
                "size": int(trade.get("s") or 0),
                "change_pct": _safe_float(row.get("todaysChangePerc")),
            }
        return out

    async def session(self, symbol: str, day: date) -> TradingSession:
        """One trading day, cleaned and ready to use.

        This is the entry point the engine should call. It fetches the raw day,
        removes duplicate minutes, splits off extended hours, bridges only very
        short gaps, and attaches a quality verdict so nothing downstream has to
        guess whether the data is trustworthy.
        """
        from qqq_alpha.data.quality import dedupe, fill_gaps, inspect_session
        from qqq_alpha.features.timeframes import split_session

        raw = await self.minute_bars(symbol, day)
        split = split_session(dedupe(raw))
        regular = fill_gaps(split.regular)

        return TradingSession(
            symbol=symbol,
            day=day,
            regular=regular,
            premarket=split.premarket,
            afterhours=split.afterhours,
            premarket_high=split.premarket_high,
            premarket_low=split.premarket_low,
            quality=inspect_session(regular),
        )

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
                transactions=int(row["n"]) if row.get("n") is not None else None,
            )
            for row in payload.get("results") or []
        ]

    # ------------------------------------------------------------------
    # Options
    # ------------------------------------------------------------------
    async def option_chain(
        self,
        underlying: str,
        expiry: date,
        option_type: OptionType | None = None,
        around: float | None = None,
        window: float = CHAIN_STRIKE_WINDOW,
    ) -> list[OptionContract]:
        """Fetch the chain for one expiry, optionally scoped to one side.

        Each page is capped at 250 contracts by the API, and pages arrive in
        ascending strike order. A 0DTE QQQ or SPY chain has more strikes than
        that on a *single* side, so one unpaged request returns the cheapest
        deep out-of-the-money strikes and stops short of the money — the
        contract every caller actually wants. Splitting the two sides (an
        earlier fix, kept) stops calls from starving puts, but it does not
        stop that.

        Two guards do. ``around`` — the spot, when the caller knows it —
        asks the provider for a strike band around the money, which is both
        complete and small. And whether or not a window was used, the pages
        are followed to the end instead of being silently dropped.

        Neither guard changes what a complete chain contains: they only stop
        an incomplete one from looking complete.
        """
        if option_type is None:
            calls, puts = await asyncio.gather(
                self.option_chain(underlying, expiry, OptionType.CALL, around, window),
                self.option_chain(underlying, expiry, OptionType.PUT, around, window),
            )
            return calls + puts

        params: dict[str, Any] = {
            "expiration_date": expiry.isoformat(),
            "limit": CHAIN_PAGE_LIMIT,
            "contract_type": option_type.value.lower(),
        }
        windowed = bool(around and around > 0 and window > 0)
        if windowed:
            params["strike_price.gte"] = round(around * (1 - window), 2)
            params["strike_price.lte"] = round(around * (1 + window), 2)

        path = f"/v3/snapshot/options/{underlying}"
        try:
            payload = await self._get(path, params)
        except MassiveError:
            if not windowed:
                raise
            # a provider that does not honour the strike-range filter must not
            # cost us the chain: ask again for everything and page through it
            log.warning("strike window rejected for %s %s; falling back to full pages", underlying, expiry)
            params.pop("strike_price.gte", None)
            params.pop("strike_price.lte", None)
            windowed = False
            payload = await self._get(path, params)

        contracts: list[OptionContract] = []
        pages = 0
        while True:
            contracts.extend(_contracts_from(payload, underlying))
            pages += 1
            next_url = payload.get("next_url")
            if not next_url:
                break
            if pages >= CHAIN_MAX_PAGES:
                log.warning(
                    "chain for %s %s %s stopped at %d pages (%d contracts) with more to come",
                    underlying, expiry, option_type.value, pages, len(contracts),
                )
                break
            next_path, next_params = _split_url(next_url)
            payload = await self._get(next_path, next_params)
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
                transactions=int(row["n"]) if row.get("n") is not None else None,
            )
            for row in payload.get("results") or []
        ]

    async def option_trades_since(
        self, occ_symbol: str, since: datetime, limit: int = 5_000
    ) -> list[dict[str, Any]]:
        """Raw prints for one contract since a timestamp — the live flow feed's
        incremental pull. Returns raw rows: converting a print into a classified
        FlowEvent needs the contract's current quote, which the caller holds.

        Requires a plan with options trades (Developer tier or above).
        """
        payload = await self._get(
            f"/v3/trades/{occ_symbol}",
            {
                "timestamp.gte": int(since.timestamp() * 1_000_000_000),
                "limit": min(limit, 50_000),
                "sort": "timestamp",
                "order": "asc",
            },
        )
        return payload.get("results") or []

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


def _split_url(url: str) -> tuple[str, dict[str, Any]]:
    """A ``next_url`` as (path, params). The key is added back by ``_get``."""
    from urllib.parse import parse_qsl, urlsplit

    parts = urlsplit(url)
    params = {k: v for k, v in parse_qsl(parts.query) if k != "apiKey"}
    return parts.path, params


def _contracts_from(payload: dict[str, Any], underlying: str) -> list[OptionContract]:
    """The contracts in one snapshot page. Rows without a ticker or an
    expiry are skipped rather than guessed at."""
    out: list[OptionContract] = []
    for item in payload.get("results") or []:
        details = item.get("details") or {}
        quote = item.get("last_quote") or {}
        trade = item.get("last_trade") or {}
        greeks = item.get("greeks") or {}
        day = item.get("day") or {}

        ticker = details.get("ticker")
        expiration = details.get("expiration_date")
        if not ticker or not expiration:
            continue

        out.append(
            OptionContract(
                occ_symbol=ticker,
                underlying=underlying,
                option_type=(
                    OptionType.CALL
                    if str(details.get("contract_type", "")).lower() == "call"
                    else OptionType.PUT
                ),
                strike=float(details.get("strike_price") or 0.0),
                expiry=date.fromisoformat(expiration),
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
    return out
