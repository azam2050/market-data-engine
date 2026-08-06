"""Live institutional options flow.

The whale layer (`features/flow.py`) — aggressor classification, sweep
detection, the FlowSummary the brain and the attention engine read — has been
built and waiting since the start. This module is the missing feed: it polls
the options tape for the contracts that matter right now (near the money, both
sides) and turns raw prints into classified FlowEvents.

Polling REST once per bar is deliberate. A WebSocket options feed would be
lower-latency, but the decision cadence is one minute anyway, and a poll that
fails degrades to "no new prints this minute" instead of a connection to
babysit. On the current paid plan the request budget is not a constraint.

If the plan turns out not to include the trades endpoint, the feed disables
itself after the first authorization error and says so once — flow goes back
to UNAVAILABLE in the prompt, and nothing else about the engine changes.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from qqq_alpha.config import Settings
from qqq_alpha.data.chain import LiveChainPricer
from qqq_alpha.data.massive import MassiveClient, _ms_to_dt, parse_occ_symbol
from qqq_alpha.domain import FlowEvent, OptionContract, OptionType
from qqq_alpha.features.flow import classify_aggressor, detect_sweeps

log = logging.getLogger(__name__)

# a print below this premium is retail noise for an index option; letting it
# into the totals would drown the institutional signal the summary exists for
MIN_PRINT_PREMIUM_USD = 10_000.0
CONTRACTS_PER_SIDE = 5
WINDOW_MINUTES = 45
FIRST_POLL_LOOKBACK_MIN = 15


def rows_to_events(
    occ_symbol: str, rows: list[dict[str, Any]], bid: float | None, ask: float | None
) -> list[FlowEvent]:
    """Convert raw tape rows into classified FlowEvents.

    The aggressor read uses the contract's *current* quote, not the quote at
    print time (the plan's trades endpoint does not carry it). Over the
    one-minute polling window that approximation is usually right and always
    labelled advisory downstream.
    """
    underlying, expiry, option_type, strike = parse_occ_symbol(occ_symbol)
    events: list[FlowEvent] = []
    for row in rows:
        price = float(row.get("price") or 0.0)
        size = int(row.get("size") or 0)
        if price <= 0 or size <= 0:
            continue
        premium = round(price * size * 100, 2)
        if premium < MIN_PRINT_PREMIUM_USD:
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
                premium=premium,
                aggressor=classify_aggressor(price, bid, ask),
            )
        )
    return events


@dataclass
class LiveFlowFeed:
    """Rolling window of classified prints for the strikes in play."""

    settings: Settings
    pricer: LiveChainPricer
    disabled: bool = False
    last_error: str | None = None
    polls: int = 0
    _raw: list[FlowEvent] = field(default_factory=list)
    _last_poll: datetime | None = None

    @property
    def status(self) -> str:
        if self.disabled:
            return f"disabled: {self.last_error}"
        return f"{self.polls} polls, {len(self._raw)} prints in window"

    def _targets(self, spot: float) -> list[OptionContract]:
        """The contracts worth watching: near the money, both sides, traded."""
        contracts = [
            c
            for side in (OptionType.CALL, OptionType.PUT)
            for c in self.pricer.nearby(spot, side, count=CONTRACTS_PER_SIDE)
        ]
        return [c for c in contracts if c.volume > 0]

    async def _fetch(
        self, targets: list[OptionContract], since: datetime
    ) -> list[list[dict[str, Any]] | BaseException]:
        """One request per contract, concurrently. Split out so tests can fake it."""
        async with MassiveClient(self.settings) as client:
            return await asyncio.gather(
                *[client.option_trades_since(c.occ_symbol, since) for c in targets],
                return_exceptions=True,
            )

    async def poll(self, now: datetime, spot: float) -> list[FlowEvent]:
        """Pull new prints and return the classified window, sweeps marked."""
        if self.disabled or self.pricer.snapshot is None:
            return self.marked(now)

        targets = self._targets(spot)
        if not targets:
            return self.marked(now)

        since = self._last_poll or now - timedelta(minutes=FIRST_POLL_LOOKBACK_MIN)
        try:
            results = await self._fetch(targets, since)
        except Exception as exc:  # noqa: BLE001 - the tape must never stop a decision
            self.last_error = str(exc)
            log.warning("flow poll failed: %s", exc)
            return self.marked(now)

        for contract, rows in zip(targets, results, strict=True):
            if isinstance(rows, BaseException):
                message = str(rows)
                if "403" in message or "NOT_AUTHORIZED" in message.upper():
                    # the plan does not cover the tape: stop asking, say so once
                    self.disabled = True
                    self.last_error = message[:200]
                    log.warning("options tape not in plan, flow feed disabled: %s", message)
                    return self.marked(now)
                self.last_error = message[:200]
                continue
            self._raw.extend(
                rows_to_events(contract.occ_symbol, rows, contract.bid, contract.ask)
            )

        self.polls += 1
        self._last_poll = now
        cutoff = now - timedelta(minutes=WINDOW_MINUTES)
        self._raw = [e for e in self._raw if e.ts >= cutoff]
        return self.marked(now)

    def marked(self, now: datetime) -> list[FlowEvent]:
        """The current window with sweeps and blocks identified."""
        cutoff = now - timedelta(minutes=WINDOW_MINUTES)
        return detect_sweeps([e for e in self._raw if e.ts >= cutoff])
