from datetime import date, datetime, timedelta

import pytest

from qqq_alpha.brain.attention import AttentionEngine
from qqq_alpha.brain.decider import next_expiry, occ_symbol
from qqq_alpha.brain.rails import DayState, SafetyRails
from qqq_alpha.config import MARKET_TZ, Settings
from qqq_alpha.data.massive import parse_occ_symbol
from qqq_alpha.data.pricing import black_scholes, implied_volatility
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, OptionContract, OptionType, Target
from qqq_alpha.features.snapshot import SnapshotBuilder


@pytest.fixture
def settings():
    return Settings(massive_api_key="test", anthropic_api_key="test", anthropic_model="test")


@pytest.fixture
def snapshot():
    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=11, trend=0.008)
    # 11:00 ET is bar index 90
    return SnapshotBuilder("QQQ").build(bars[:120])


def test_rails_allow_a_normal_moment(settings, snapshot):
    snapshot.data_age_sec = 5
    verdict = SafetyRails(settings).pre_check(snapshot, DayState())
    assert verdict.allowed, verdict.blocks


def test_rails_block_stale_data(settings, snapshot):
    snapshot.data_age_sec = 999
    verdict = SafetyRails(settings).pre_check(snapshot, DayState())
    assert not verdict.allowed
    assert any(b.startswith("stale_data") for b in verdict.blocks)


def test_rails_block_daily_trade_cap(settings, snapshot):
    snapshot.data_age_sec = 5
    verdict = SafetyRails(settings).pre_check(snapshot, DayState(trades_taken=2))
    assert not verdict.allowed
    assert any(b.startswith("daily_trade_cap") for b in verdict.blocks)


def test_rails_block_circuit_breaker(settings, snapshot):
    snapshot.data_age_sec = 5
    verdict = SafetyRails(settings).pre_check(snapshot, DayState(realized_return_pct=-40))
    assert not verdict.allowed
    assert any(b.startswith("circuit_breaker") for b in verdict.blocks)


def test_rails_hold_no_market_opinion(settings, snapshot):
    """A strongly bearish tape must not be blocked — that is the brain's call."""
    snapshot.data_age_sec = 5
    for obs in snapshot.observations:
        obs.score = -1.0
    verdict = SafetyRails(settings).pre_check(snapshot, DayState())
    assert verdict.allowed


def test_pre_check_no_longer_blocks_purely_on_clock_time(settings, snapshot):
    """The late-entry cutoff moved to post_check, where the chosen contract's
    actual expiry is known — pre_check cannot yet tell 0DTE from 1DTE."""
    snapshot.data_age_sec = 5
    snapshot.ts = snapshot.ts.replace(hour=15, minute=45)  # past the old cutoff
    verdict = SafetyRails(settings).pre_check(snapshot, DayState())
    assert verdict.allowed, verdict.blocks


def _decision(**overrides):
    base = dict(
        ts=datetime(2026, 3, 2, 15, 0, tzinfo=MARKET_TZ),
        action=Action.ENTER,
        direction=OptionType.CALL,
        occ_symbol="O:QQQ260302C00485000",
        targets=[Target(label="T1", price=1.5, return_pct=50, take_pct=50)],
        stop_price=0.6,
        stop_return_pct=-40,
        confidence=7,
        thesis="test",
    )
    base.update(overrides)
    return Decision(**base)


def _contract(**overrides):
    base = dict(
        occ_symbol="O:QQQ260302C00485000",
        underlying="QQQ",
        option_type=OptionType.CALL,
        strike=485.0,
        expiry=date(2026, 3, 2),
        bid=1.00,
        ask=1.02,
        volume=500,
        open_interest=1200,
    )
    base.update(overrides)
    return OptionContract(**base)


def test_post_check_passes_clean_trade(settings):
    verdict = SafetyRails(settings).post_check(_decision(), _contract())
    assert verdict.allowed, verdict.blocks


def test_post_check_blocks_wide_spread(settings):
    verdict = SafetyRails(settings).post_check(_decision(), _contract(bid=1.0, ask=1.5))
    assert not verdict.allowed
    assert any(b.startswith("spread_too_wide") for b in verdict.blocks)


def test_post_check_requires_stop_and_targets(settings):
    rails = SafetyRails(settings)
    assert not rails.post_check(_decision(stop_return_pct=None), _contract()).allowed
    assert not rails.post_check(_decision(stop_return_pct=10), _contract()).allowed
    assert not rails.post_check(_decision(targets=[]), _contract()).allowed


def test_post_check_warns_but_allows_low_target(settings):
    decision = _decision(targets=[Target(label="T1", price=1.2, return_pct=25, take_pct=100)])
    verdict = SafetyRails(settings).post_check(decision, _contract())
    assert verdict.allowed
    assert any("below_target_bar" in w for w in verdict.warnings)


def test_post_check_blocks_a_same_day_contract_entered_past_the_cutoff(settings):
    """A broker restricts trading a same-day contract as it nears expiry —
    this is the case the cutoff exists for."""
    late = _decision(ts=datetime(2026, 3, 2, 15, 45, tzinfo=MARKET_TZ))
    verdict = SafetyRails(settings).post_check(late, _contract(expiry=date(2026, 3, 2)))
    assert not verdict.allowed
    assert any(b.startswith("late_0dte_entry") for b in verdict.blocks)


def test_post_check_allows_a_next_day_contract_at_the_same_late_clock_time(settings):
    """The same clock time is not restricted for a contract expiring tomorrow
    — that used to be blocked too, which was the actual bug."""
    late = _decision(
        ts=datetime(2026, 3, 2, 15, 45, tzinfo=MARKET_TZ),
        occ_symbol="O:QQQ260303C00485000",
    )
    contract = _contract(occ_symbol="O:QQQ260303C00485000", expiry=date(2026, 3, 3))
    verdict = SafetyRails(settings).post_check(late, contract)
    assert verdict.allowed, verdict.blocks


def test_attention_wakes_on_activity_and_respects_cooldown():
    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=3, trend=0.02, volatility=0.002)
    builder = SnapshotBuilder("QQQ")
    engine = AttentionEngine(threshold=0.3, cooldown_sec=600)

    wakes = 0
    for i in range(40, 200):
        verdict = engine.evaluate(builder.build(bars[: i + 1]))
        if verdict.should_wake:
            wakes += 1
    assert wakes > 0
    # cooldown must keep it from firing every single minute
    assert wakes < 60


def test_occ_symbol_roundtrip():
    symbol = occ_symbol("QQQ", date(2026, 8, 3), OptionType.CALL, 485.0)
    assert symbol == "O:QQQ260803C00485000"
    underlying, expiry, option_type, strike = parse_occ_symbol(symbol)
    assert (underlying, expiry, option_type, strike) == ("QQQ", date(2026, 8, 3), OptionType.CALL, 485.0)


def test_next_expiry_skips_weekend():
    friday = date(2026, 8, 7)
    assert next_expiry(friday, 1) == date(2026, 8, 10)


def test_black_scholes_call_gains_with_spot():
    cheap = black_scholes(480, 485, 0.002, 0.2, OptionType.CALL)
    rich_ = black_scholes(490, 485, 0.002, 0.2, OptionType.CALL)
    assert rich_ > cheap


def test_black_scholes_expired_is_intrinsic():
    assert black_scholes(490, 485, 0.0, 0.2, OptionType.CALL) == 5.0
    assert black_scholes(480, 485, 0.0, 0.2, OptionType.CALL) == 0.0


def test_implied_volatility_recovers_input():
    price = black_scholes(480, 480, 0.01, 0.25, OptionType.CALL)
    recovered = implied_volatility(price, 480, 480, 0.01, OptionType.CALL)
    assert recovered is not None
    assert abs(recovered - 0.25) < 0.02


def test_trade_manager_banks_half_then_the_trade_cannot_go_red():
    """The new geometry: at +35% half is sold and the cost is secured, so even
    a full collapse afterwards closes the whole position green."""
    from qqq_alpha.trades import TradeManager

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=5)
    snap = SnapshotBuilder("QQQ").build(bars[:60])
    manager = TradeManager()
    trade = manager.open_trade(_decision(), fill_price=1.00, snapshot=snap)

    assert trade.decision.targets[0].price == 1.5
    assert trade.decision.stop_price == 0.6

    now = trade.opened_at + timedelta(minutes=5)
    update = manager.update(trade, 1.60, now)
    assert update is not None and update.note.startswith("scale_out")
    assert trade.open_fraction == 0.5
    assert trade.banked_return_pct == 30.0  # half the position, banked at +60%
    assert trade.is_open

    # the crash that used to produce -45% now exits at breakeven on the
    # remainder and keeps the banked half: whole position closes positive
    update = manager.update(trade, 0.55, now + timedelta(minutes=1))
    assert update is not None and "closed:breakeven_stop" in update.note
    assert not trade.is_open
    assert trade.return_pct == 7.5  # 30.0 banked + 0.5 x -45.0


def test_trade_manager_trails_the_runner_from_its_peak():
    from qqq_alpha.trades import TradeManager

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=5)
    snap = SnapshotBuilder("QQQ").build(bars[:60])
    manager = TradeManager()
    trade = manager.open_trade(_decision(), fill_price=1.00, snapshot=snap)

    now = trade.opened_at + timedelta(minutes=5)
    manager.update(trade, 1.60, now)  # scale out, peak +60%
    manager.update(trade, 2.20, now + timedelta(minutes=2))  # peak +120%
    assert trade.is_open

    # +120% peak minus 25% giveback → exits near +95% on the runner
    update = manager.update(trade, 1.90, now + timedelta(minutes=4))
    assert update is not None and "closed:trail_stop" in update.note
    assert trade.return_pct == 75.0  # 30 banked + 0.5 x 90


def test_trade_manager_time_stops_a_thesis_that_never_moved():
    from qqq_alpha.trades import TradeManager

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=5)
    snap = SnapshotBuilder("QQQ").build(bars[:60])
    manager = TradeManager()
    decision = _decision().model_copy(update={"expected_hold_minutes": 20})
    trade = manager.open_trade(decision, fill_price=1.00, snapshot=snap)

    # +5% after 1.5x the expected hold is theta bleed, not patience
    update = manager.update(trade, 1.05, trade.opened_at + timedelta(minutes=31))
    assert update is not None and "closed:time_stop" in update.note


def test_thesis_stop_fires_when_spot_crosses_the_invalidation_level():
    from qqq_alpha.domain import OptionType
    from qqq_alpha.trades import TradeManager

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=5)
    snap = SnapshotBuilder("QQQ").build(bars[:60])
    manager = TradeManager()
    decision = _decision().model_copy(
        update={"direction": OptionType.CALL, "invalidation_level": 480.0}
    )
    trade = manager.open_trade(decision, fill_price=1.00, snapshot=snap)

    assert not manager.check_thesis(trade, spot=485.0)  # thesis alive
    assert manager.check_thesis(trade, spot=479.5)  # CALL, spot below the level

    put = _decision().model_copy(
        update={"direction": OptionType.PUT, "invalidation_level": 490.0}
    )
    put_trade = manager.open_trade(put, fill_price=1.00, snapshot=snap)
    assert not manager.check_thesis(put_trade, spot=485.0)
    assert manager.check_thesis(put_trade, spot=490.5)  # PUT, spot above the level


def test_infeasible_separates_impossibility_from_caution():
    from qqq_alpha.brain.rails import infeasible

    assert infeasible(["outside_session: 09:29 ET"])
    assert infeasible(["stale_data: last bar is 999s old"])
    assert infeasible(["unusable_data: gaps"])
    # policy blocks are choices, and choices deserve to be graded
    assert not infeasible(["daily_trade_cap: 2/2"])
    assert not infeasible(["circuit_breaker: day at -40.0%"])
    assert not infeasible([])
