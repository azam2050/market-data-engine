"""Price levels a discretionary trader would draw on a chart — computed exactly.

This is the direct replacement for "the AI looks at TradingView". Everything a
professional reads off a chart is a number; we hand the brain the numbers.
"""

from __future__ import annotations

from qqq_alpha.domain import Bar

OPENING_RANGE_MINUTES = 15


def _round_levels(price: float, step: float = 5.0, span: int = 2) -> dict[str, float]:
    """Psychological round numbers straddling the current price."""
    base = round(price / step) * step
    levels: dict[str, float] = {}
    for offset in range(-span, span + 1):
        level = round(base + offset * step, 2)
        if level > 0:
            levels[f"round_{level:g}"] = level
    return levels


def opening_range(bars: list[Bar], minutes: int = OPENING_RANGE_MINUTES) -> tuple[float, float] | None:
    if len(bars) < minutes:
        return None
    window = bars[:minutes]
    return (max(b.high for b in window), min(b.low for b in window))


def compute_levels(
    session_bars: list[Bar],
    prior_day: Bar | None = None,
    overnight_high: float | None = None,
    overnight_low: float | None = None,
) -> dict[str, float | None]:
    """All reference levels for the current session."""
    if not session_bars:
        return {}

    price = session_bars[-1].close
    levels: dict[str, float | None] = {
        "session_open": session_bars[0].open,
        "session_high": max(b.high for b in session_bars),
        "session_low": min(b.low for b in session_bars),
        "overnight_high": overnight_high,
        "overnight_low": overnight_low,
    }

    orange = opening_range(session_bars)
    if orange:
        levels["opening_range_high"], levels["opening_range_low"] = orange

    if prior_day is not None:
        levels["prior_high"] = prior_day.high
        levels["prior_low"] = prior_day.low
        levels["prior_close"] = prior_day.close
        pivot = (prior_day.high + prior_day.low + prior_day.close) / 3.0
        levels["pivot"] = round(pivot, 2)
        levels["r1"] = round(2 * pivot - prior_day.low, 2)
        levels["s1"] = round(2 * pivot - prior_day.high, 2)

    levels.update(_round_levels(price))
    return levels


def opening_gap(session_bars: list[Bar], prior_day: Bar | None) -> dict | None:
    """Today's open against yesterday's close — and whether it has been filled.

    The gap is the one piece of daily-timeframe information that matters to a
    trade expiring this afternoon, because it names a specific level and a
    specific behaviour: price either returns to yesterday's close or it does
    not, and which of the two happens usually decides the character of the
    session. Anything slower than that — weekly trends, multi-day ranges — is
    context a 0DTE position will never live long enough to collect on.

    ``filled`` means price has traded back through yesterday's close at some
    point today. ``pct_to_fill`` is how far the current price still is from it,
    signed in the direction price would have to travel.
    """
    if not session_bars or prior_day is None or prior_day.close <= 0:
        return None

    open_price = session_bars[0].open
    close_ref = prior_day.close
    gap_pct = (open_price - close_ref) / close_ref * 100.0
    if abs(gap_pct) < 0.05:  # anything smaller is not a gap, it is a tick
        return {"direction": "none", "pct": round(gap_pct, 3)}

    session_high = max(b.high for b in session_bars)
    session_low = min(b.low for b in session_bars)
    filled = session_low <= close_ref <= session_high
    price = session_bars[-1].close

    return {
        "direction": "up" if gap_pct > 0 else "down",
        "pct": round(gap_pct, 3),
        "fill_level": round(close_ref, 2),
        "filled": filled,
        "pct_to_fill": round((close_ref - price) / price * 100.0, 3),
    }


def nearest_levels(
    price: float, levels: dict[str, float | None], count: int = 3
) -> dict[str, list[tuple[str, float, float]]]:
    """Split levels into resistance above and support below, nearest first.

    Each entry is (name, level, distance_pct).
    """
    above: list[tuple[str, float, float]] = []
    below: list[tuple[str, float, float]] = []

    for name, level in levels.items():
        if level is None or level <= 0:
            continue
        distance = round((level - price) / price * 100.0, 3)
        if level > price:
            above.append((name, level, distance))
        elif level < price:
            below.append((name, level, distance))

    above.sort(key=lambda item: item[2])
    below.sort(key=lambda item: item[2], reverse=True)
    return {"resistance": above[:count], "support": below[:count]}


def distance_to_nearest_pct(price: float, levels: dict[str, float | None]) -> float | None:
    """How far the price is from the closest level of any kind, in percent.

    Small values mean the price is at a decision point — exactly where 0DTE
    setups live.
    """
    distances = [
        abs((level - price) / price * 100.0)
        for level in levels.values()
        if level is not None and level > 0
    ]
    return round(min(distances), 3) if distances else None
