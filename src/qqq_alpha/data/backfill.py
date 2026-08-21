"""Fetching the minutes the live stream dropped.

A websocket that reconnects loses whatever printed while it was away, and the
engine had no way to get those minutes back: it counted them, lowered its own
confidence, and carried on with a hole in its indicators. That is the wrong
trade — the bars exist at the provider, on a different and slower path, and
asking for them costs one request.

The hole is not merely missing information; it is misleading information. A
20-minute average computed over 18 bars returns a number that looks right, and
a high that printed inside the gap is a level the engine believes never traded
— which matters most for the declared-trigger lock, where the whole point is to
wait for a price the tape has to reach.

Two limits are deliberate.

**Only recent gaps.** Pulling twenty minutes of history into a live session can
move an indicator far enough to fire a stop *retroactively* — exiting a trade
on information that arrived late. Old gaps stay recorded as a declared
shortfall instead.

**Not every gap is a loss.** A minute with no trades produces no bar at all, so
a symbol that genuinely went quiet has nothing to fetch. Rare on QQQ, ordinary
on a single name at lunchtime — which is why a fetch that comes back short is
recorded and dropped rather than retried forever.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from qqq_alpha.config import Settings
from qqq_alpha.domain import Bar

log = logging.getLogger(__name__)

# how far back a gap may be and still be worth repairing. Beyond this the
# repair is more dangerous than the hole: see the module docstring.
MAX_REPAIR_AGE_MIN = 10
# How stale a minute must be before it counts as lost rather than late.
#
# It has to exceed one bar interval to mean anything: the moment the next bar
# arrives, the missing minute is already 60 seconds old, so any threshold below
# that fires on every gap and chases minutes the stream is still delivering.
# Two intervals is the first value that distinguishes a real loss from a slow
# one, at the cost of repairing a gap roughly two bars after it opens — the
# decision immediately after still sees the hole, every later one does not.
MIN_SETTLE_SEC = 120
# one runaway session must not turn into thousands of requests
MAX_REPAIRS_PER_SESSION = 60


@dataclass
class RepairLog:
    """What was missing, what came back, and what never existed."""

    requested: int = 0
    recovered: int = 0
    unavailable: int = 0
    skipped_too_old: int = 0
    failures: int = 0
    minutes: list[datetime] = field(default_factory=list)

    @property
    def attempted(self) -> bool:
        return bool(self.requested)

    def summary(self) -> str:
        if not self.requested:
            return "لا فجوات"
        parts = [f"طُلبت {self.requested} دقيقة", f"استُرجعت {self.recovered}"]
        if self.unavailable:
            parts.append(f"{self.unavailable} لم تُتداول أصلًا")
        if self.skipped_too_old:
            parts.append(f"{self.skipped_too_old} أقدم من أن تُصلَح")
        if self.failures:
            parts.append(f"{self.failures} فشل الطلب")
        return " · ".join(parts)


def missing_minutes(bars: list[Bar]) -> list[datetime]:
    """The minute timestamps absent from an ordered run of bars.

    Pure clock arithmetic on the timestamps the bars already carry — the same
    detection ``quality.inspect_session`` does, returned as the minutes
    themselves so they can be asked for rather than merely counted.
    """
    if len(bars) < 2:
        return []
    ordered = sorted(bars, key=lambda b: b.ts)
    gaps: list[datetime] = []
    for previous, current in zip(ordered, ordered[1:], strict=False):
        step = int((current.ts - previous.ts).total_seconds() // 60)
        for offset in range(1, step):
            gaps.append(previous.ts + timedelta(minutes=offset))
    return gaps


def merge_bars(existing: list[Bar], fetched: list[Bar]) -> list[Bar]:
    """Insert recovered bars in time order, never overwriting a live one.

    Order is not cosmetic here: every indicator reads this list as a sequence,
    so a bar appended to the end instead of slotted into place corrupts the
    calculation it was meant to repair. A live bar always wins a collision —
    it came from the tape, the fetched one is a reconstruction.
    """
    known = {bar.ts.replace(second=0, microsecond=0) for bar in existing}
    additions = [
        bar for bar in fetched if bar.ts.replace(second=0, microsecond=0) not in known
    ]
    if not additions:
        return existing
    return sorted([*existing, *additions], key=lambda b: b.ts)


class GapRepairer:
    """Asks the provider for minutes the stream failed to deliver."""

    def __init__(self, settings: Settings, client_factory=None):
        self.settings = settings
        self._client_factory = client_factory
        self.repairs_this_session = 0
        self.log = RepairLog()

    def reset(self) -> None:
        self.repairs_this_session = 0
        self.log = RepairLog()

    # ------------------------------------------------------------------
    def _repairable(self, minutes: list[datetime], now: datetime) -> list[datetime]:
        """Which of these minutes are both recent enough and settled enough."""
        fresh: list[datetime] = []
        for minute in minutes:
            age_sec = (now - minute).total_seconds()
            if age_sec > MAX_REPAIR_AGE_MIN * 60:
                self.log.skipped_too_old += 1
                continue
            if age_sec < MIN_SETTLE_SEC:
                continue  # not stale, just not ready — it may still arrive live
            fresh.append(minute)
        return fresh

    async def repair(
        self, symbol: str, bars: list[Bar], now: datetime | None = None
    ) -> list[Bar]:
        """Return ``bars`` with any recoverable missing minutes filled in.

        Failure is always a no-op returning the original list: a broken repair
        must never be worse than the hole it was trying to close.
        """
        now = now or datetime.now(UTC)
        wanted = self._repairable(missing_minutes(bars), now)
        if not wanted:
            return bars
        if self.repairs_this_session >= MAX_REPAIRS_PER_SESSION:
            return bars

        self.repairs_this_session += 1
        self.log.requested += len(wanted)

        try:
            fetched = await self._fetch(symbol, wanted[0].astimezone(UTC).date())
        except Exception as exc:  # noqa: BLE001 - a failed repair leaves the hole
            self.log.failures += 1
            log.warning("gap repair failed for %s: %s", symbol, exc)
            return bars

        target = {m.replace(second=0, microsecond=0) for m in wanted}
        usable = [
            bar for bar in fetched if bar.ts.replace(second=0, microsecond=0) in target
        ]
        merged = merge_bars(bars, usable)
        recovered = len(merged) - len(bars)
        self.log.recovered += recovered
        # a minute nobody traded produces no bar, so it is absent rather than
        # lost — counted once and never asked for again
        self.log.unavailable += len(wanted) - recovered
        self.log.minutes.extend(wanted)
        if recovered:
            log.info("recovered %d missing minute(s) for %s", recovered, symbol)
        return merged

    async def _fetch(self, symbol: str, day) -> list[Bar]:
        if self._client_factory is not None:
            client = self._client_factory()
            async with client as open_client:
                return await open_client.minute_bars(symbol, day)

        from qqq_alpha.data.massive import MassiveClient

        async with MassiveClient(self.settings) as client:
            return await client.minute_bars(symbol, day)
