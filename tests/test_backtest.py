"""Tests for the backtester's missed-opportunity attribution.

The engine already priced forward setups the rails blocked. The path for the
AI's own PASS existed in ``_record_missed`` (the "brain declined" reason
string) but no call site ever reached it — a pure decline was scored as
nothing at all. This covers the fix: caution has to answer for itself whether
it came from a rail or from the brain's own judgement.
"""

from datetime import date

from qqq_alpha.backtest.engine import Backtester
from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.config import Settings
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, MarketSnapshot
from qqq_alpha.memory import Memory

DAY = date(2026, 3, 2)


class _AlwaysPassDecider:
    """Never enters. Isolates the missed-opportunity path from decision noise."""

    async def decide(self, snapshot: MarketSnapshot, **kwargs) -> Decision:
        return Decision(
            ts=snapshot.ts, action=Action.PASS, confidence=3, thesis="test: never enters"
        )


async def test_a_declined_setup_with_strong_follow_through_is_recorded_as_missed():
    settings = Settings(min_target_return_pct=50.0)
    bars = synthetic_session("QQQ", DAY, seed=12, trend=0.03, volatility=0.002)

    backtester = Backtester(
        settings=settings,
        decider=_AlwaysPassDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
    )
    result = await backtester.run_day(DAY, bars)

    assert result.brain_calls > 0  # the brain was actually asked, and passed every time
    assert result.trades == []  # by construction, it never enters
    assert result.missed
    # some declines are pre-check rail blocks that never reach the brain at
    # all — the fix under test is that a *pure* PASS is no longer silently
    # unscored, not that every miss funnels through one reason
    assert any(m.reason == "brain declined" for m in result.missed)
    assert all(m.peak_return_pct >= settings.min_target_return_pct for m in result.missed)
    assert all(m.regime for m in result.missed)


async def test_flat_bias_produces_no_missed_opportunity():
    """No obvious trade, nothing to grade the decline against."""
    settings = Settings(min_target_return_pct=50.0)
    bars = synthetic_session("QQQ", DAY, seed=1, trend=0.0, volatility=0.0004)

    backtester = Backtester(
        settings=settings,
        decider=_AlwaysPassDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
    )
    result = await backtester.run_day(DAY, bars)

    assert not any(m.peak_return_pct < settings.min_target_return_pct for m in result.missed)


async def test_missed_opportunities_are_persisted_to_memory(tmp_path):
    settings = Settings(min_target_return_pct=50.0)
    bars = synthetic_session("QQQ", DAY, seed=12, trend=0.03, volatility=0.002)
    memory = Memory(tmp_path / "memory.db")

    backtester = Backtester(
        settings=settings,
        decider=_AlwaysPassDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        memory=memory,
    )
    result = await backtester.run_day(DAY, bars)

    assert memory.missed_count() == len(result.missed)
