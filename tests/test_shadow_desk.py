"""Shadow stock desk: the single-name learner must trade only on paper.

What matters here: it simulates entries on WEEKLY contracts (single names have
no daily expiries), keeps its journal in a subdirectory the live dashboard
globs never touch, respects its own cost caps, and flattens at every session
boundary like the live desk does.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from qqq_alpha.brain.decider import next_expiry, occ_symbol
from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.config import Settings
from qqq_alpha.data.massive import parse_occ_symbol
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, OptionType, Target
from qqq_alpha.live.shadow import ShadowStockDesk

DAY = date(2026, 3, 2)  # a Monday — the weekly expiry test depends on it


@pytest.fixture
def settings(tmp_path):
    return Settings(
        massive_api_key="test-key",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        attention_threshold=0.0,  # every bar wakes: the test drives decisions
        attention_cooldown_sec=0,
        max_data_age_sec=10**9,
        shadow_symbols_csv="NVDA",
    )


class _EnterOnceDecider:
    """ENTERs a same-day CALL on the first call, then passes forever."""

    def __init__(self):
        self.calls = 0

    async def decide(self, snapshot, **kwargs):
        self.calls += 1
        if self.calls > 1:
            return Decision(ts=snapshot.ts, action=Action.PASS, confidence=3)
        price = snapshot.underlying.close
        return Decision(
            ts=snapshot.ts,
            action=Action.ENTER,
            direction=OptionType.CALL,
            # deliberately a same-day (Monday) expiry — QQQ rules. The desk
            # must snap it to the Friday weekly before simulating anything.
            occ_symbol=occ_symbol(
                snapshot.underlying.symbol,
                next_expiry(snapshot.ts.date(), 0),
                OptionType.CALL,
                round(price),
            ),
            targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
            stop_return_pct=-40,
            confidence=7,
            expected_hold_minutes=60,
            invalidation_level=price * 0.98,
        )


class _CountingPassDecider:
    def __init__(self):
        self.calls = 0

    async def decide(self, snapshot, **kwargs):
        self.calls += 1
        return Decision(ts=snapshot.ts, action=Action.PASS, confidence=3)


@pytest.mark.asyncio
async def test_shadow_entry_is_simulated_on_the_weekly_contract(settings):
    decider = _EnterOnceDecider()
    desk = ShadowStockDesk(settings, decider, Playbook())
    bars = synthetic_session("NVDA", DAY, seed=5, trend=0.02, volatility=0.002)

    for bar in bars[:60]:
        await desk.on_bar(bar)

    book = desk.books["NVDA"]
    all_trades = book.manager.open_trades + book.manager.closed_trades
    assert len(all_trades) == 1
    underlying, expiry, _, _ = parse_occ_symbol(all_trades[0].occ_symbol)
    assert underlying == "NVDA"
    assert expiry.weekday() == 4  # Friday: snapped from the Monday dte=0 ask
    assert expiry == date(2026, 3, 6)
    # sized by the same arithmetic as the live desk (conf 7, first hour → 0.375)
    assert all_trades[0].decision.size_factor > 0

    # the record lands in the shadow subdirectory...
    shadow_dir = settings.journal_dir / "shadow"
    assert list(shadow_dir.glob("trades-*.jsonl"))
    assert list(shadow_dir.glob("decisions-*.jsonl"))
    # ...and never in the top-level journal the live dashboard pages glob
    assert not list(settings.journal_dir.glob("trades-*.jsonl"))
    assert not list(settings.journal_dir.glob("decisions-*.jsonl"))


@pytest.mark.asyncio
async def test_shadow_brain_calls_are_capped_per_day(settings):
    capped = settings.model_copy(update={"shadow_max_brain_calls_per_day": 2})
    decider = _CountingPassDecider()
    desk = ShadowStockDesk(capped, decider, Playbook())
    bars = synthetic_session("NVDA", DAY, seed=6, trend=0.02, volatility=0.002)

    for bar in bars[:120]:
        await desk.on_bar(bar)

    # every bar past warmup wakes attention, but the desk pays for at most 2
    assert decider.calls == 2


@pytest.mark.asyncio
async def test_shadow_positions_flatten_at_the_day_boundary(settings):
    decider = _EnterOnceDecider()
    desk = ShadowStockDesk(settings, decider, Playbook())
    bars = synthetic_session("NVDA", DAY, seed=7, trend=0.025, volatility=0.001)

    for bar in bars[:60]:
        await desk.on_bar(bar)
    book = desk.books["NVDA"]

    next_day = synthetic_session("NVDA", DAY + timedelta(days=1), seed=8)[0]
    await desk.on_bar(next_day)

    assert book.manager.open_trades == []
    assert book.brain_calls_today == 0
    assert book.trades_today == 0
    for closed in book.manager.closed_trades:
        assert closed.exit_reason in ("session_close", "stop_hit", "trail_stop",
                                      "breakeven_stop", "time_stop", "thesis_invalidated")


@pytest.mark.asyncio
async def test_symbols_outside_the_leader_list_are_refused(settings, caplog):
    """A shadow symbol with no bar source would shadow silence for weeks —
    the desk must drop it loudly at boot instead."""
    odd = settings.model_copy(update={"shadow_symbols_csv": "NVDA,ZZZT"})
    desk = ShadowStockDesk(odd, _CountingPassDecider(), Playbook())
    assert desk.symbols == ["NVDA"]
