"""Session splitting and multi-timeframe aggregation.

Two things live here, and both exist because of how real market data actually
arrives:

1. **Session splitting.** The provider's minute aggregates cover 04:00-20:00 ET
   and there is no API parameter to exclude extended hours — you must filter by
   timestamp yourself. Skipping this silently corrupts VWAP, the opening range,
   and session high/low, because pre-market prints get folded into the regular
   session. Pre-market is not discarded though: its high and low are among the
   most-watched levels of the day, so we keep them as reference levels.

2. **Timeframe aggregation.** We never aggregate raw ticks — that is fragile and
   is what makes hand-rolled data pipelines unreliable. We take the provider's
   clean 1-minute bars and roll them up. Rolling minutes into 5m/15m is exact
   arithmetic (max of highs, min of lows, sum of volume, volume-weighted vwap),
   so every timeframe is guaranteed consistent with every other one. Requesting
   5-minute bars separately from the API cannot make that guarantee at session
   boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta

from qqq_alpha.config import MARKET_TZ, REGULAR_CLOSE, REGULAR_OPEN
from qqq_alpha.domain import Bar

PREMARKET_OPEN = time(4, 0)
AFTERHOURS_CLOSE = time(20, 0)


@dataclass
class SessionSplit:
    """One trading day, separated into the three sessions that matter."""

    premarket: list[Bar]
    regular: list[Bar]
    afterhours: list[Bar]

    @property
    def premarket_high(self) -> float | None:
        return max((b.high for b in self.premarket), default=None)

    @property
    def premarket_low(self) -> float | None:
        return min((b.low for b in self.premarket), default=None)


def split_session(bars: list[Bar]) -> SessionSplit:
    """Separate a day's minute bars into pre-market, regular, and after-hours."""
    premarket: list[Bar] = []
    regular: list[Bar] = []
    afterhours: list[Bar] = []

    for bar in bars:
        local = bar.ts.astimezone(MARKET_TZ).time()
        if PREMARKET_OPEN <= local < REGULAR_OPEN:
            premarket.append(bar)
        elif REGULAR_OPEN <= local < REGULAR_CLOSE:
            regular.append(bar)
        elif REGULAR_CLOSE <= local <= AFTERHOURS_CLOSE:
            afterhours.append(bar)

    for group in (premarket, regular, afterhours):
        group.sort(key=lambda b: b.ts)
    return SessionSplit(premarket=premarket, regular=regular, afterhours=afterhours)


def regular_session(bars: list[Bar]) -> list[Bar]:
    """Just the 09:30-16:00 ET bars. The default view for everything intraday."""
    return split_session(bars).regular


def _bucket_start(bar: Bar, minutes: int) -> datetime:
    """Anchor buckets to the 09:30 open so 5m bars land on :30, :35, :40…"""
    local = bar.ts.astimezone(MARKET_TZ)
    open_dt = local.replace(
        hour=REGULAR_OPEN.hour, minute=REGULAR_OPEN.minute, second=0, microsecond=0
    )
    offset = int((local - open_dt).total_seconds() // 60)
    # floor division handles pre-market (negative offsets) correctly
    bucket = (offset // minutes) * minutes
    return open_dt + timedelta(minutes=bucket)


def resample(bars: list[Bar], minutes: int) -> list[Bar]:
    """Roll 1-minute bars up into `minutes`-minute bars. Exact, not approximate.

    The last bucket may be partial — that is intentional. A forming 5-minute bar
    is real information to a trader, and hiding it would delay every decision by
    up to four minutes.
    """
    if minutes <= 1 or not bars:
        return list(bars)

    buckets: dict[datetime, list[Bar]] = {}
    for bar in sorted(bars, key=lambda b: b.ts):
        buckets.setdefault(_bucket_start(bar, minutes), []).append(bar)

    out: list[Bar] = []
    for start in sorted(buckets):
        group = buckets[start]
        volume = sum(b.volume for b in group)

        # volume-weighted vwap across the bucket; falls back to typical price
        weighted = 0.0
        weight = 0
        for b in group:
            reference = b.vwap if b.vwap is not None else (b.high + b.low + b.close) / 3.0
            weighted += reference * b.volume
            weight += b.volume
        vwap = round(weighted / weight, 4) if weight > 0 else None

        # only sum when EVERY bar in the bucket carries a count. A partial sum
        # would sit next to a complete volume total, and the ratio the brain
        # reads as "average trade size" would be inflated by exactly the
        # fraction of bars that were missing their count.
        counts = [b.transactions for b in group if b.transactions is not None]
        transactions = counts if len(counts) == len(group) else []

        out.append(
            Bar(
                symbol=group[0].symbol,
                ts=start,
                open=group[0].open,
                high=max(b.high for b in group),
                low=min(b.low for b in group),
                close=group[-1].close,
                volume=volume,
                vwap=vwap,
                transactions=sum(transactions) if transactions else None,
            )
        )
    return out


@dataclass
class TimeframeSet:
    """The same session seen at three resolutions.

    A discretionary trader does exactly this: the 15m says which way the day is
    going, the 5m says whether the structure supports the trade, the 1m says
    when to press the button. Reading only one of them is how you end up buying
    a bounce inside a downtrend.
    """

    m1: list[Bar]
    m5: list[Bar]
    m15: list[Bar]

    @classmethod
    def build(cls, minute_bars: list[Bar]) -> TimeframeSet:
        return cls(
            m1=list(minute_bars),
            m5=resample(minute_bars, 5),
            m15=resample(minute_bars, 15),
        )

    def as_dict(self) -> dict[str, list[Bar]]:
        return {"1m": self.m1, "5m": self.m5, "15m": self.m15}


def hourly(minute_bars: list[Bar]) -> list[Bar]:
    """Sixty-minute bars — and this one deliberately wants several days of input.

    A regular session is 390 minutes, so a single day yields six and a half
    hourly candles: not enough for an EMA, not enough for a swing high, not
    enough to be a chart. The hourly a trader actually reads spans the week,
    which is why the engine loads the previous sessions rather than resampling
    today on its own.

    Buckets are anchored to each day's own 09:30 open, the same as every other
    timeframe here, so a session yields 09:30/10:30/…/15:30 and days never
    bleed into one another — five sessions give about 35 hourly candles.
    """
    return resample(minute_bars, 60)
