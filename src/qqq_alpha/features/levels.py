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
