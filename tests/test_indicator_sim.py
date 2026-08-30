"""The indicator's Python twin: a full trade cycle on crafted bars.

The scenario builds two weeks of gentle uptrend (so the previous-day and
previous-week edges exist and the frames align bullish), then a volume
breakout through the prior day's high, a retrace to the entry zone, a rally
through the first target, and a collapse that breaks the trailed stop —
one complete trade the simulator must record with honest R accounting.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from qqq_alpha.backtest.indicator_sim import SimResult, format_report, run_simulation
from qqq_alpha.domain import Bar

NY = ZoneInfo("America/New_York")


def _bar(ts: datetime, o: float, h: float, low: float, c: float, v: int) -> Bar:
    return Bar(symbol="TEST", ts=ts, open=o, high=h, low=low, close=c, volume=v)


def _day_start(d: datetime) -> datetime:
    return d.replace(hour=9, minute=30, second=0, microsecond=0)


def _build_scenario() -> list[Bar]:
    bars: list[Bar] = []
    price = 100.0
    # week one, Monday..Friday 2025-06-02..06: 78 five-minute bars a day,
    # +0.02 a bar with a 0.5 range so ATR settles near half a dollar
    for day_off in range(5):
        t = _day_start(datetime(2025, 6, 2, tzinfo=NY) + timedelta(days=day_off))
        for _ in range(78):
            o = price
            c = price + 0.02
            bars.append(_bar(t, o, max(o, c) + 0.25, min(o, c) - 0.25, c, 1000))
            price = c
            t += timedelta(minutes=5)
    prev_day_high = max(b.high for b in bars if b.ts.date() == bars[-1].ts.date())

    # week two, Monday 2025-06-09: a flat morning just under Friday's high —
    # close enough that the breakout candle stays inside the ATR size filter
    t = _day_start(datetime(2025, 6, 9, tzinfo=NY))
    flat = prev_day_high - 0.35
    for _ in range(40):
        bars.append(_bar(t, flat, flat + 0.20, flat - 0.20, flat + 0.02, 1000))
        t += timedelta(minutes=5)

    # the breakout candle: closes through the prior-day high on 2.5x volume
    o = flat + 0.02
    c = prev_day_high + 0.30
    bars.append(_bar(t, o, c + 0.05, o - 0.05, c, 2500))
    t += timedelta(minutes=5)

    # retrace touches the candle midpoint (the pending entry) without
    # breaking the invalidation stop
    mid = ((c + 0.05) + (o - 0.05)) / 2.0
    bars.append(_bar(t, c, c + 0.05, mid - 0.02, c - 0.10, 1200))
    t += timedelta(minutes=5)

    # the rally: ten bars climbing well past the first target
    price = c
    for _ in range(10):
        o2 = price
        c2 = price + 0.35
        bars.append(_bar(t, o2, c2 + 0.05, o2 - 0.05, c2, 1100))
        price = c2
        t += timedelta(minutes=5)

    # the collapse: one close far below any trailed stop ends the trade
    bars.append(_bar(t, price, price + 0.05, price - 3.5, price - 3.4, 3000))
    return bars


def test_full_trade_cycle_records_honest_r() -> None:
    res = run_simulation(_build_scenario(), "TEST", 5)
    assert res.total == 1
    trade = res.trades[0]
    assert trade.direction == 1
    assert trade.t1_hit, "the rally runs 3.5 dollars past entry — T1 must light"
    assert res.wins == 1 and res.win_rate == 100.0
    # honest R: measured to the exit close, which gave back part of the run
    assert res.best_r == trade.r_mult
    assert res.worst_r == trade.r_mult
    assert trade.r_mult > 0


def test_premarket_bars_are_ignored() -> None:
    bars = _build_scenario()
    early = datetime(2025, 6, 9, 8, 0, tzinfo=NY)
    bars.append(_bar(early, 1.0, 1000.0, 0.5, 999.0, 10_000_000))
    res = run_simulation(bars, "TEST", 5)
    assert res.total == 1, "a pre-market bar must never create or kill trades"


def test_report_formats_arabic_summary() -> None:
    res = run_simulation(_build_scenario(), "TEST", 5)
    text = format_report([res, SimResult(symbol="EMPTY", frame_min=5)], months=12)
    assert "TEST · 5 د" in text
    assert "نجاح 100.0%" in text
    assert "أفضل صفقة" in text and "أسوأ صفقة" in text
    assert "EMPTY" in text and "لا صفقات" in text


def test_day_close_mode_flattens_at_session_end() -> None:
    # cut the scenario before the collapse: the trade would still be open,
    # so day-close mode must flatten it on the session's last bar
    bars = _build_scenario()[:-1]
    carried = run_simulation(bars, "TEST", 5, day_close=False)
    flat = run_simulation(bars, "TEST", 5, day_close=True)
    assert carried.total == 0, "overnight mode keeps the trade open past the data"
    assert flat.total == 1, "day-close mode must book the trade at the last bar"
    assert flat.trades[0].t1_hit and flat.trades[0].r_mult > 0
