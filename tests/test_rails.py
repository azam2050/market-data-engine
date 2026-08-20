from datetime import date, datetime, timedelta

import pytest

from qqq_alpha.brain.attention import AttentionEngine
from qqq_alpha.brain.decider import next_expiry, occ_symbol
from qqq_alpha.brain.rails import DayState, SafetyRails
from qqq_alpha.config import MARKET_TZ, Settings
from qqq_alpha.data.massive import parse_occ_symbol
from qqq_alpha.data.pricing import black_scholes, implied_volatility
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, OptionContract, OptionType, Target, Trigger
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


def test_circuit_breaker_measures_risk_not_contract_drama(settings, snapshot):
    """2026-08-17: one HALF-size trade stopped out at -42.7% and the desk went
    quiet for the rest of the day. -42.7% on half a position is ~-21% of normal
    risk — nowhere near the -25% limit. The breaker must read the weighted
    number, and the day must stay open."""
    snapshot.data_age_sec = 5
    state = DayState(
        trades_taken=1,
        realized_return_pct=-42.7,  # what the contract did
        realized_risk_pct=-42.7 * 0.5,  # what it cost at the size taken
    )
    verdict = SafetyRails(settings).pre_check(snapshot, state)
    assert verdict.allowed, verdict.blocks


def test_circuit_breaker_still_closes_the_day_on_a_full_size_hit(settings, snapshot):
    snapshot.data_age_sec = 5
    state = DayState(realized_return_pct=-40, realized_risk_pct=-40.0)
    verdict = SafetyRails(settings).pre_check(snapshot, state)
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


# ---------------------------------------------------------------- the declared-trigger lock
def _at(snapshot, minutes: int) -> datetime:
    return snapshot.ts - timedelta(minutes=minutes)


def _wait(snapshot, minutes: int, triggers: list[Trigger]) -> Decision:
    return Decision(
        ts=_at(snapshot, minutes), action=Action.WAIT, thesis="waiting", triggers=triggers
    )


def _enter(snapshot, direction: OptionType) -> Decision:
    return Decision(
        ts=snapshot.ts,
        action=Action.ENTER,
        direction=direction,
        occ_symbol="O:QQQ260819P00713000",
        thesis="entering",
    )


def test_entry_before_its_declared_level_is_blocked(settings, snapshot):
    """2026-08-19, the trade that cost -45%. At 10:18 the brain wrote that the
    PUT needed "a break of 713.33"; at 10:21 it entered at 713.49, sixteen
    cents above its own level, which never traded again."""
    untouched = min(b.low for b in snapshot.recent_bars_1m) - 0.10
    prior = [_wait(snapshot, 3, [Trigger(direction=OptionType.PUT, level=untouched, side="below")])]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, prior
    )

    assert not verdict.allowed
    assert any(b.startswith("declared_trigger_unmet") for b in verdict.blocks)


def test_entry_after_its_declared_level_is_allowed(settings, snapshot):
    """The winning trade the same morning: the trigger (a break of pivot
    718.56) was declared one wake earlier and price actually got there."""
    spot = snapshot.underlying.close
    prior = [_wait(snapshot, 3, [Trigger(direction=OptionType.PUT, level=spot + 0.50, side="below")])]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, prior
    )

    assert verdict.allowed, verdict.blocks


def test_a_trigger_the_tape_touched_and_left_still_arms_the_entry(settings, snapshot):
    """"Break 713.33" means the tape printed it. A break that snaps back is
    still a break the brain is entitled to act on, so the lock reads the bars
    since the commitment, not only the price at this instant."""
    low = min(b.low for b in snapshot.recent_bars_1m[-5:])
    assert low < snapshot.underlying.close  # traded, then left behind — not at spot
    prior = [_wait(snapshot, 5, [Trigger(direction=OptionType.PUT, level=low, side="below")])]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, prior
    )

    assert verdict.allowed, verdict.blocks


def test_a_trigger_for_the_other_direction_never_blocks(settings, snapshot):
    spot = snapshot.underlying.close
    prior = [_wait(snapshot, 3, [Trigger(direction=OptionType.CALL, level=spot + 5, side="above")])]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, prior
    )

    assert verdict.allowed, verdict.blocks


def test_the_newest_declaration_replaces_the_older_one(settings, snapshot):
    """The brain revises openly by naming a new level; a stale commitment must
    not outlive the revision it was replaced by."""
    spot = snapshot.underlying.close
    prior = [
        _wait(snapshot, 9, [Trigger(direction=OptionType.PUT, level=spot - 5, side="below")]),
        _wait(snapshot, 2, [Trigger(direction=OptionType.PUT, level=spot + 1, side="below")]),
    ]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, prior
    )

    assert verdict.allowed, verdict.blocks


def test_an_expired_commitment_stops_binding(settings, snapshot):
    """A level named half an hour ago describes a market that no longer exists."""
    spot = snapshot.underlying.close
    ttl = settings.trigger_ttl_minutes
    prior = [_wait(snapshot, ttl + 5, [Trigger(direction=OptionType.PUT, level=spot - 1, side="below")])]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, prior
    )

    assert verdict.allowed, verdict.blocks


def test_an_absurd_level_is_discarded_rather_than_enforced(settings, snapshot):
    """A misplaced decimal must not freeze the desk for the rest of its life."""
    prior = [_wait(snapshot, 2, [Trigger(direction=OptionType.PUT, level=1.0, side="below")])]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, prior
    )

    assert verdict.allowed, verdict.blocks


def test_no_commitment_means_no_lock(settings, snapshot):
    """The lock holds the brain to what it said — it does not require it to
    pre-announce every trade, which would block the first entry of every day."""
    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, []
    )
    assert verdict.allowed, verdict.blocks


def test_an_entry_is_never_judged_against_its_own_bookkeeping(settings, snapshot):
    """An ENTER acts, it does not promise. If a stray trigger rides along on an
    entry it must not become the commitment that blocks the next one."""
    spot = snapshot.underlying.close
    stray = _enter(snapshot, OptionType.PUT)
    stray.ts = _at(snapshot, 4)
    stray.triggers = [Trigger(direction=OptionType.PUT, level=spot - 3, side="below")]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, [stray]
    )

    assert verdict.allowed, verdict.blocks


def test_measurement_mode_warns_instead_of_blocking(settings, snapshot):
    """So the backtest can price what the lock costs without the lock changing
    the run it is measuring."""
    settings.enforce_declared_trigger = False
    untouched = min(b.low for b in snapshot.recent_bars_1m) - 0.10
    prior = [_wait(snapshot, 3, [Trigger(direction=OptionType.PUT, level=untouched, side="below")])]

    verdict = SafetyRails(settings).commitment_check(
        _enter(snapshot, OptionType.PUT), snapshot, prior
    )

    assert verdict.allowed
    assert any(w.startswith("declared_trigger_unmet") for w in verdict.warnings)


def test_a_wait_is_never_blocked_by_a_commitment(settings, snapshot):
    spot = snapshot.underlying.close
    prior = [_wait(snapshot, 3, [Trigger(direction=OptionType.PUT, level=spot - 5, side="below")])]

    verdict = SafetyRails(settings).commitment_check(
        _wait(snapshot, 0, []), snapshot, prior
    )

    assert verdict.allowed, verdict.blocks


# ---------------------------------------------------------------- parsing what the model sent
def _payload(**extra):
    return {"action": "WAIT", "confidence": 4, "thesis": "t", **extra}


def test_declared_triggers_survive_the_round_trip(settings, snapshot):
    from qqq_alpha.brain.decider import AIDecider

    decision = AIDecider._to_decision(
        _payload(
            triggers=[
                {"direction": "PUT", "level": 713.33, "side": "below", "note": "failed bounce"},
                {"direction": "CALL", "level": 716.6, "side": "above"},
            ]
        ),
        snapshot,
    )

    assert [t.level for t in decision.triggers] == [713.33, 716.6]
    assert decision.triggers[0].direction is OptionType.PUT
    assert decision.triggers[0].note == "failed bounce"


@pytest.mark.parametrize(
    "bad",
    [
        {"direction": "SIDEWAYS", "level": 713.33, "side": "below"},
        {"direction": "PUT", "level": "not a number", "side": "below"},
        {"direction": "PUT", "level": 713.33, "side": "sideways"},
        {"direction": "PUT", "side": "below"},
        "not even an object",
    ],
)
def test_a_malformed_trigger_is_dropped_not_raised(settings, snapshot, bad):
    """An advisory field the model fumbled must never turn a sound decision
    into a technical PASS."""
    from qqq_alpha.brain.decider import AIDecider

    decision = AIDecider._to_decision(_payload(triggers=[bad]), snapshot)

    assert decision.action is Action.WAIT
    assert decision.triggers == []
