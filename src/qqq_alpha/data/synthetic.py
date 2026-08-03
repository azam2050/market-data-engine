"""Deterministic synthetic session generator.

Lets the whole pipeline — features, attention, brain, trade manager, report —
be exercised end to end before any data subscription exists. Seeded, so a given
day always replays identically and test failures are reproducible.
"""

from __future__ import annotations

import math
import random
from datetime import date, datetime, timedelta

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import Bar

SESSION_MINUTES = 390


def synthetic_session(
    symbol: str,
    day: date,
    seed: int | None = None,
    start_price: float = 480.0,
    trend: float = 0.0,
    volatility: float = 0.0009,
) -> list[Bar]:
    """One regular-session day of 1-minute bars.

    ``trend`` is total drift across the session, expressed as a fraction
    (0.004 = +0.4% on the day). Intraday shape adds an opening-range burst and a
    midday lull so the feature layer sees realistic structure.
    """
    rng = random.Random(seed if seed is not None else hash((symbol, day)) & 0xFFFF)
    price = start_price
    bars: list[Bar] = []
    open_dt = datetime(day.year, day.month, day.day, 9, 30, tzinfo=MARKET_TZ)

    for minute in range(SESSION_MINUTES):
        # volatility smile across the session: hot open, quiet lunch, hot close
        phase = minute / SESSION_MINUTES
        intensity = 1.6 - 1.1 * math.sin(math.pi * phase) + 0.5 * (phase > 0.9)
        step = rng.gauss(trend / SESSION_MINUTES, volatility * intensity)
        close = price * (1.0 + step)
        high = max(price, close) * (1.0 + abs(rng.gauss(0, volatility * 0.6)))
        low = min(price, close) * (1.0 - abs(rng.gauss(0, volatility * 0.6)))
        volume = int(abs(rng.gauss(40_000, 15_000)) * intensity) + 1_000

        bars.append(
            Bar(
                symbol=symbol,
                ts=open_dt + timedelta(minutes=minute),
                open=round(price, 2),
                high=round(high, 2),
                low=round(low, 2),
                close=round(close, 2),
                volume=volume,
                vwap=round((high + low + close) / 3, 2),
            )
        )
        price = close

    return bars


def synthetic_week(symbol: str, start: date, seed: int = 7) -> dict[date, list[Bar]]:
    """Five consecutive weekday sessions with continuity in price."""
    rng = random.Random(seed)
    sessions: dict[date, list[Bar]] = {}
    price = 480.0
    day = start
    while len(sessions) < 5:
        if day.weekday() < 5:
            trend = rng.choice([0.006, -0.005, 0.001, 0.009, -0.008])
            bars = synthetic_session(symbol, day, seed=rng.randint(0, 10_000), start_price=price, trend=trend)
            sessions[day] = bars
            price = bars[-1].close
        day += timedelta(days=1)
    return sessions
