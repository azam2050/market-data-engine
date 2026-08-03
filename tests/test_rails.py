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


def test_trade_manager_stops_and_targets():
    from qqq_alpha.trades import TradeManager

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=5)
    snap = SnapshotBuilder("QQQ").build(bars[:60])
    manager = TradeManager()
    trade = manager.open_trade(_decision(), fill_price=1.00, snapshot=snap)

    assert trade.decision.targets[0].price == 1.5
    assert trade.decision.stop_price == 0.6

    now = trade.opened_at + timedelta(minutes=5)
    update = manager.update(trade, 1.60, now)
    assert update is not None and "target:T1" in update.note
    assert trade.is_open

    update = manager.update(trade, 0.55, now + timedelta(minutes=1))
    assert update is not None and "closed:stop_hit" in update.note
    assert not trade.is_open
    assert trade.return_pct == -45.0
