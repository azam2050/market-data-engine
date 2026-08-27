"""Price levels a discretionary trader would draw on a chart — computed exactly.

This is the direct replacement for "the AI looks at TradingView". Everything a
professional reads off a chart is a number; we hand the brain the numbers.
"""

from __future__ import annotations

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import Bar
from qqq_alpha.features import structure

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


# a swing is "the same level" as another when they sit this close together.
# 0.15% of QQQ is roughly a point — tight enough that two genuinely different
# levels stay apart, loose enough that the same level tested on three days
# does not fragment into three separate near-identical numbers.
TOUCH_TOLERANCE_PCT = 0.15
# how many independent touches make a level worth naming. Two is a level a
# trader would draw; one is just a high.
MIN_TOUCHES = 2


def multi_day_levels(
    bars: list[Bar],
    price: float,
    tolerance_pct: float = TOUCH_TOLERANCE_PCT,
    min_touches: int = MIN_TOUCHES,
    min_sessions: int = 2,
) -> dict:
    """The levels a trader sees after scrolling the chart LEFT.

    Everything else in this module describes today and yesterday. That is the
    horizon a 0DTE position lives inside, and for a long time it was assumed to
    be the only horizon that mattered. It is not: a range whose edges were set
    three sessions ago still decides whether today's breakout runs or fades,
    and a position opened in the middle of that range is a coin flip no matter
    how clean the one-minute tape looks. The engine already fetches five prior
    sessions to build the hourly candles — this reads the levels out of the
    same bars, so the week costs no extra data.

    ``bars`` should span the whole history window (hourly is the right
    granularity: fine enough to place a level, coarse enough that intraday
    noise does not mint one). Returns the window's extremes, where price
    currently sits inside them, and the levels price has tested repeatedly.
    """
    if not bars or price <= 0:
        return {}

    # one session is not a week. Without this the first day back from a data
    # outage would hand the model today's own high and low relabelled as the
    # range of the week — the same two numbers it already has as
    # session_high/session_low, wearing a name that claims far more authority
    sessions = len({b.ts.astimezone(MARKET_TZ).date() for b in bars})
    if sessions < min_sessions:
        return {}

    window_high = max(b.high for b in bars)
    window_low = min(b.low for b in bars)
    span = window_high - window_low
    result: dict = {
        "high": round(window_high, 2),
        "low": round(window_low, 2),
        "sessions": sessions,
    }
    if span > 0:
        # 0 = sitting on the floor of the week, 100 = on its ceiling, 50 = the
        # middle, where directional trades have the worst odds
        result["range_position_pct"] = round((price - window_low) / span * 100.0, 1)
        result["range_width_pct"] = round(span / price * 100.0, 2)

    # cluster the week's pivots: a price several swings agree on is a level,
    # a price one swing touched is a high
    swings = structure.swing_points(bars)
    clusters: list[list] = []
    for swing in sorted(swings, key=lambda s: s.price):
        if clusters and abs(swing.price - clusters[-1][-1].price) / price * 100.0 <= tolerance_pct:
            clusters[-1].append(swing)
        else:
            clusters.append([swing])

    repeated = []
    for cluster in clusters:
        if len(cluster) < min_touches:
            continue
        level = sum(s.price for s in cluster) / len(cluster)
        kinds = {s.kind for s in cluster}
        repeated.append(
            {
                "price": round(level, 2),
                "touches": len(cluster),
                # a level tested from both sides has flipped role at least once
                # (resistance that became support, or the reverse) — the kind a
                # trader trusts most
                "kind": "both" if len(kinds) > 1 else next(iter(kinds)),
                "distance_pct": round((level - price) / price * 100.0, 2),
            }
        )

    # most-tested first; ties broken by proximity, since a level far from price
    # cannot matter to a position that expires this afternoon
    repeated.sort(key=lambda item: (-item["touches"], abs(item["distance_pct"])))
    result["repeated"] = repeated
    return result


def merge_multi_day(levels: dict[str, float | None], multi: dict) -> dict[str, float | None]:
    """Fold the week's levels into the session levels dict.

    Done as a merge rather than a separate channel so that ``nearest_levels``
    and ``distance_to_nearest_pct`` — which already answer "what is price about
    to run into?" — start counting the week's levels without knowing they
    exist.
    """
    if not multi:
        return levels
    merged = dict(levels)
    if multi.get("high") is not None:
        merged["week_high"] = multi["high"]
    if multi.get("low") is not None:
        merged["week_low"] = multi["low"]
    for index, level in enumerate(multi.get("repeated", [])[:4], start=1):
        merged[f"tested_{index}x{level['touches']}"] = level["price"]
    return merged


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
