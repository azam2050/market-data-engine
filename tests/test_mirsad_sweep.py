"""The Python twin must trade the same doctrine as the Pine, and say so honestly."""

from __future__ import annotations

import math
import random
from datetime import UTC, datetime, timedelta

from qqq_alpha.backtest.mirsad import Outcome, Trade, format_sweep, profile_for, run
from qqq_alpha.domain import Bar


def _bars(n: int, *, drift: float = 0.0, seed: int = 7) -> list[Bar]:
    rnd = random.Random(seed)
    ts = datetime(2025, 1, 2, 14, 30, tzinfo=UTC)
    px = 100.0
    out = []
    for i in range(n):
        px = max(1.0, px + drift + rnd.uniform(-0.35, 0.35))
        hi = px + abs(rnd.uniform(0, 0.3))
        lo = px - abs(rnd.uniform(0, 0.3))
        op = rnd.uniform(lo, hi)
        out.append(Bar(symbol="TEST", ts=ts + timedelta(minutes=5 * i), open=op,
                       high=hi, low=lo, close=px,
                       volume=1000 + rnd.randint(0, 900)))
    return out


def test_each_timeframe_gets_the_personality_its_trader_wants():
    """A one-minute contract is burned by the clock; a daily one is not."""
    burn = profile_for(1)
    day = profile_for(60)
    assert burn.tp1 < day.tp1
    assert burn.time_stop < day.time_stop
    # only the burn profile still closes on a target count
    assert burn.max_targets == 2
    assert day.max_targets > 4


def test_a_flat_random_market_does_not_manufacture_an_edge():
    out = run(_bars(1500), "TEST", 5)
    assert out.total >= 0
    if out.total:
        # nothing here should look like a discovery
        assert -1.5 < out.avg_r < 1.5


def test_every_result_is_booked_net_of_the_contract_spread():
    """A trade that exits exactly where it entered is a loss, not a scratch."""
    out = run(_bars(1200, drift=0.02), "TEST", 5)
    for t in out.trades:
        risk = abs(t.entry - t.stop0)
        assert risk > 0
        # the net R can never equal the gross R
        assert not math.isclose(t.r_net, 0.0, abs_tol=1e-12) or t.bars == 0


def test_the_report_names_the_win_rate_the_payoff_would_need():
    o = Outcome(symbol="X", minutes=5, profile="سريع")
    o.trades = [Trade(1, 100, 99, 0, 5, 2.0, 5), Trade(1, 100, 99, 0, 5, -1.0, 3),
                Trade(-1, 100, 101, 0, 5, -1.0, 3)]
    assert round(o.payoff, 2) == 2.0
    assert round(o.breakeven_win_rate) == 33
    text = format_sweep([o], 1, 12)
    assert "التعادل يحتاج" in text and "33%" in text


def test_a_sweep_with_no_paying_frame_says_so_instead_of_flattering():
    losing = Outcome(symbol="X", minutes=5, profile="سريع")
    losing.trades = [Trade(1, 100, 99, 0, 5, -0.5, 4) for _ in range(40)]
    text = format_sweep([losing], 1, 12)
    assert "لا فريم يعبر" in text
    assert "المشكلة في الدخول" in text


def test_the_autopsy_refuses_to_crown_a_four_trade_slice():
    """A slice that pays on a handful of trades is thin, not a discovery."""
    from qqq_alpha.backtest.mirsad import format_autopsy

    early = Outcome(symbol="X", minutes=5, profile="سريع")
    late = Outcome(symbol="X", minutes=5, profile="سريع")
    # forty losers through the early door, four winners through the late one
    early.trades = [Trade(1, 100, 99, 0, 5, -0.4, 4, kind="مبكر", hour=10) for _ in range(40)]
    early.trades += [Trade(1, 100, 99, 0, 5, 3.0, 9, kind="استئناف", hour=11) for _ in range(4)]
    late.trades = list(early.trades)
    text = format_autopsy(early, late)
    assert "عينة صغيرة" in text
    assert "لا شريحة تعبر" in text


def test_the_autopsy_reports_a_real_slice_when_one_survives():
    from qqq_alpha.backtest.mirsad import format_autopsy

    early = Outcome(symbol="X", minutes=5, profile="سريع")
    late = Outcome(symbol="X", minutes=5, profile="سريع")
    early.trades = [Trade(1, 100, 99, 0, 5, 0.5, 9, kind="مبكر", hour=10) for _ in range(30)]
    early.trades += [Trade(-1, 100, 101, 0, 5, -0.2, 4, kind="مبكر", hour=10) for _ in range(20)]
    late.trades = list(early.trades)
    text = format_autopsy(early, late)
    assert "شريحة صمدت" in text
