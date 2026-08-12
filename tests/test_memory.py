"""Tests for long-term memory and the learning loop.

The point of these is that memory has to survive things: process restarts,
unclean shutdowns, months of accumulation. And the learner has to refuse to
learn from too little — an eager learner is worse than none, because it produces
confident rules built on noise.
"""

from datetime import date, datetime, timedelta

import pytest

from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.config import MARKET_TZ, Settings
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, MarketRegime, MissedOpportunity, OptionType, Target
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.learning import (
    MIN_TOTAL_TRADES,
    analyse,
    apply_lesson,
    propose,
    with_applied_lessons,
)
from qqq_alpha.memory import Memory
from qqq_alpha.trades import TradeManager

DAY = date(2026, 3, 2)


def _snapshot(seed: int = 15, bars: int = 120):
    session = synthetic_session("QQQ", DAY, seed=seed)
    return SnapshotBuilder("QQQ").build(session[:bars])


def _make_trade(
    snapshot,
    trade_id: str,
    return_pct: float | None = None,
    confidence: int = 7,
    direction: OptionType = OptionType.CALL,
    day_offset: int = 0,
):
    decision = Decision(
        ts=snapshot.ts,
        action=Action.ENTER,
        direction=direction,
        occ_symbol="O:QQQ260302C00485000",
        targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40,
        confidence=confidence,
        thesis="opening range break",
    )
    trade = TradeManager().open_trade(decision, 1.00, snapshot)
    trade.trade_id = trade_id
    trade.opened_at = datetime(2026, 3, 2, 10, 0, tzinfo=MARKET_TZ) + timedelta(days=day_offset)
    if return_pct is not None:
        trade.closed_at = trade.opened_at + timedelta(minutes=30)
        trade.return_pct = return_pct
        trade.max_favorable_pct = max(return_pct, 0)
        trade.exit_reason = "target" if return_pct > 0 else "stop_hit"
    return trade


# ---------------------------------------------------------------- persistence
def test_memory_survives_reopening(tmp_path):
    """The whole point: a restart must not cost the engine its history."""
    path = tmp_path / "memory.db"
    snapshot = _snapshot()

    first = Memory(path)
    first.remember_trade(_make_trade(snapshot, "t1", 75.0), snapshot)
    del first

    reopened = Memory(path)
    assert reopened.counts()["closed"] == 1
    assert reopened.recent_trades()[0].return_pct == 75.0


def test_trade_updates_overwrite_rather_than_duplicate(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()

    trade = _make_trade(snapshot, "t1")
    memory.remember_trade(trade, snapshot)  # opened
    trade.return_pct = 60.0
    trade.closed_at = trade.opened_at + timedelta(minutes=20)
    memory.remember_trade(trade)  # closed

    counts = memory.counts()
    assert counts["trades"] == 1
    assert counts["closed"] == 1


def test_market_fingerprint_is_stored(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    memory.remember_trade(_make_trade(snapshot, "t1", 50.0), snapshot)

    row = memory.closed_trades()[0]
    assert row["regime"] == snapshot.regime.value
    assert row["session_minute"] == snapshot.session_minute
    assert row["net_bias"] == snapshot.net_bias
    assert row["features"]  # full evidence pack retained for later analysis


def test_decisions_not_to_trade_are_remembered(tmp_path):
    """Passes are half the record — a system that only logs entries cannot learn."""
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    decision = Decision(
        ts=snapshot.ts, action=Action.PASS, confidence=3, thesis="conflicting timeframes"
    )
    memory.remember_decision(decision, snapshot, attention_score=0.6, blocked_by=[])

    assert memory.counts()["decisions"] == 1


# ---------------------------------------------------------------- recall
def test_similar_recall_filters_by_regime(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()

    same = _make_trade(snapshot, "same", 90.0)
    memory.remember_trade(same, snapshot)

    other = _snapshot(seed=15)
    other.regime = MarketRegime.VOLATILE_CHOP
    memory.remember_trade(_make_trade(other, "other", -40.0), other)

    recalled = memory.similar_trades(snapshot)
    assert [t.trade_id for t in recalled] == ["same"]


def test_similar_recall_ranks_by_closeness(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()

    near = _make_trade(snapshot, "near", 40.0)
    memory.remember_trade(near, snapshot)

    far = _snapshot(seed=15)
    far.observations = []  # collapses net_bias
    far.indicators = dict(far.indicators, vwap_dev_pct=5.0, rel_volume=9.0, rsi14=95.0)
    memory.remember_trade(_make_trade(far, "far", -40.0), far)

    recalled = memory.similar_trades(snapshot)
    assert recalled[0].trade_id == "near"
    assert recalled[0].distance < recalled[-1].distance


def test_recall_skips_open_trades(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    memory.remember_trade(_make_trade(snapshot, "open"), snapshot)

    assert memory.similar_trades(snapshot) == []
    assert memory.recent_trades() == []


def test_recalled_trade_renders_for_the_prompt(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    memory.remember_trade(_make_trade(snapshot, "t1", 120.0), snapshot)

    row = memory.recent_trades()[0].as_prompt_row()
    assert row["result_pct"] == 120.0
    assert row["direction"] == "CALL"
    assert "thesis" in row


# ---------------------------------------------------------------- aggregation
def test_performance_grouping_respects_minimum_sample(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    for index in range(4):
        memory.remember_trade(_make_trade(snapshot, f"t{index}", 30.0, day_offset=index), snapshot)

    assert memory.performance_by("regime", min_sample=3)
    assert memory.performance_by("regime", min_sample=10) == []


def test_performance_by_hour_labels_session_hours(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    for index in range(3):
        memory.remember_trade(_make_trade(snapshot, f"t{index}", 20.0, day_offset=index), snapshot)

    hours = memory.performance_by_hour(min_sample=1)
    assert hours and "ET" in hours[0]["session_hour"]


def test_cannot_group_by_arbitrary_columns(tmp_path):
    with pytest.raises(ValueError):
        Memory(tmp_path / "memory.db").performance_by("thesis")


# ---------------------------------------------------------------- learning
def _fill(memory, snapshot, results: list[float], **kwargs):
    for index, result in enumerate(results):
        memory.remember_trade(
            _make_trade(snapshot, f"t{index}", result, day_offset=index, **kwargs), snapshot
        )


def test_learner_refuses_to_learn_from_too_little(tmp_path):
    """An eager learner is worse than none: it produces confident noise."""
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    _fill(memory, snapshot, [50.0, -40.0, 80.0])

    report = analyse(memory)
    assert not report.has_findings
    assert any("minimum before any pattern" in n for n in report.notes)


def test_learner_stays_silent_when_nothing_stands_out(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    _fill(memory, snapshot, [10.0] * MIN_TOTAL_TRADES)

    report = analyse(memory)
    assert report.total_trades == MIN_TOTAL_TRADES
    assert not report.has_findings
    assert any("strong enough" in n for n in report.notes)


def test_learner_flags_uninformative_confidence(tmp_path):
    """High-confidence calls that lose should be reported as such."""
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()

    for index in range(10):
        memory.remember_trade(
            _make_trade(snapshot, f"hi{index}", -40.0, confidence=9, day_offset=index), snapshot
        )
    for index in range(10):
        memory.remember_trade(
            _make_trade(snapshot, f"lo{index}", 60.0, confidence=4, day_offset=index + 20),
            snapshot,
        )

    report = analyse(memory)
    assert any(f.key == "calibration" for f in report.findings)


def test_findings_become_pending_lessons_then_reach_the_playbook(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()

    for index in range(10):
        memory.remember_trade(
            _make_trade(snapshot, f"hi{index}", -40.0, confidence=9, day_offset=index), snapshot
        )
    for index in range(10):
        memory.remember_trade(
            _make_trade(snapshot, f"lo{index}", 60.0, confidence=4, day_offset=index + 20),
            snapshot,
        )

    report = analyse(memory)
    ids = propose(memory, report)
    assert ids
    assert len(memory.pending_lessons()) == len(ids)

    # proposing twice must not duplicate
    assert propose(memory, report) == []

    settings = Settings(playbook_path=tmp_path / "playbook.yaml")
    updated = apply_lesson(memory, Playbook(version=1), ids[0], settings)

    assert updated.version == 2
    assert updated.lessons[0].sample_size > 0
    # only the applied lesson leaves pending — others found in the same pass
    # (this fixture happens to make exit_reason perfectly predict confidence
    # too) still await their own approval
    assert ids[0] not in {row["id"] for row in memory.pending_lessons()}

    # the approval must survive a redeploy: recomposing from the seed plus
    # durable memory — with no playbook file on disk at all — keeps the lesson
    rebooted = with_applied_lessons(Playbook(version=1), Memory(tmp_path / "memory.db"))
    assert rebooted.version == 2
    assert rebooted.lessons and rebooted.lessons[0].statement == updated.lessons[0].statement

    # and an applied lesson must never be re-proposed just because the
    # underlying numbers drifted — L002 came back the morning after approval
    assert propose(memory, analyse(memory)) == []


def test_applied_lesson_reaches_the_brains_prompt(tmp_path):
    """A lesson nobody reads is not learning."""
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    for index in range(10):
        memory.remember_trade(
            _make_trade(snapshot, f"hi{index}", -40.0, confidence=9, day_offset=index), snapshot
        )
    for index in range(10):
        memory.remember_trade(
            _make_trade(snapshot, f"lo{index}", 60.0, confidence=4, day_offset=index + 20),
            snapshot,
        )

    ids = propose(memory, analyse(memory))
    settings = Settings(playbook_path=tmp_path / "playbook.yaml")
    updated = apply_lesson(memory, Playbook(version=1), ids[0], settings)

    block = updated.as_prompt_block()
    assert "LESSONS LEARNED" in block
    assert "n=" in block


def test_rejecting_a_lesson_removes_it_from_pending(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    lesson_id = memory.save_lesson("test claim", "some evidence", 12, 0.6)

    memory.set_lesson_status(lesson_id, "rejected")
    assert not memory.pending_lessons()


# ---------------------------------------------------------------- missed opportunities
def _missed(
    regime: str = "TRENDING_UP",
    peak: float = 80.0,
    direction: OptionType = OptionType.CALL,
    blocked: list[str] | None = None,
) -> MissedOpportunity:
    return MissedOpportunity(
        ts=datetime(2026, 3, 2, 10, 0, tzinfo=MARKET_TZ),
        reason="blocked before the brain could act" if blocked else "brain declined",
        would_be_direction=direction,
        occ_symbol="O:QQQ260302C00485000",
        hypothetical_entry=1.0,
        best_price_after=round(1.0 * (1 + peak / 100), 2),
        peak_return_pct=peak,
        blocked_by=blocked or [],
        regime=regime,
        session_minute=30,
    )


def test_missed_opportunity_round_trips(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    memory.remember_missed(_missed())

    assert memory.missed_count() == 1
    assert memory.counts()["missed"] == 1


def test_missed_performance_groups_by_regime_and_respects_minimum(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    for _ in range(9):
        memory.remember_missed(_missed(peak=80.0))

    rows = memory.missed_performance_by("regime", min_sample=8)
    assert rows and rows[0]["bucket"] == "TRENDING_UP"
    assert rows[0]["count"] == 9
    assert rows[0]["avg_peak"] == 80.0
    assert memory.missed_performance_by("regime", min_sample=20) == []


def test_cannot_group_missed_by_arbitrary_columns(tmp_path):
    with pytest.raises(ValueError):
        Memory(tmp_path / "memory.db").missed_performance_by("occ_symbol")


def test_learner_flags_regimes_where_caution_is_expensive(tmp_path):
    """A pile of declined setups that would have cleared the target is a signal."""
    memory = Memory(tmp_path / "memory.db")
    for _ in range(9):
        memory.remember_missed(_missed(peak=90.0))  # well past the default 50% target

    report = analyse(memory)
    findings = [f for f in report.findings if f.key.startswith("missed:regime:")]
    assert findings
    assert findings[0].direction == "unfavourable"
    assert "TRENDING_UP" in findings[0].statement


def test_learner_ignores_missed_opportunities_that_barely_clear_the_target(tmp_path):
    memory = Memory(tmp_path / "memory.db")
    for _ in range(9):
        memory.remember_missed(_missed(peak=55.0))  # clears 50%, but not by much

    report = analyse(memory)
    assert not any(f.key.startswith("missed:regime:") for f in report.findings)


def test_missed_opportunity_findings_do_not_wait_for_closed_trades(tmp_path):
    """Unlike trade-outcome dimensions, this one has its own sample size to trust."""
    memory = Memory(tmp_path / "memory.db")
    for _ in range(9):
        memory.remember_missed(_missed(peak=90.0))

    report = analyse(memory)  # zero closed trades on record
    assert report.total_trades == 0
    assert any(f.key.startswith("missed:regime:") for f in report.findings)


def test_infeasible_missed_rows_are_purged_on_open(tmp_path):
    """A pre-open "miss" is fiction — nobody can buy an option before the open.

    Early builds stored them anyway, and the learning loop proposed loosening
    entry confidence over trades that never could have existed. The purge runs
    on every open, so an existing poisoned database heals itself on restart.
    """
    path = tmp_path / "memory.db"
    memory = Memory(path)
    memory.remember_missed(_missed(blocked=["outside_session: 09:29 ET"]))
    memory.remember_missed(_missed(blocked=["daily_trade_cap: 2/2"]))  # policy: keep
    memory.remember_missed(_missed())  # the AI's own PASS: keep
    assert memory.missed_count() == 3

    reopened = Memory(path)
    assert reopened.missed_count() == 2


def test_a_rejected_lesson_stays_rejected(tmp_path):
    """The operator said no. Re-proposing it every morning is nagging."""
    memory = Memory(tmp_path / "memory.db")
    snapshot = _snapshot()
    for index in range(10):
        memory.remember_trade(
            _make_trade(snapshot, f"hi{index}", -40.0, confidence=9, day_offset=index), snapshot
        )
    for index in range(10):
        memory.remember_trade(
            _make_trade(snapshot, f"lo{index}", 60.0, confidence=4, day_offset=index + 20),
            snapshot,
        )

    ids = propose(memory, analyse(memory))
    for lesson_id in ids:
        memory.set_lesson_status(lesson_id, "rejected")

    assert propose(memory, analyse(memory)) == []


def test_subscriber_trial_lifecycle(tmp_path):
    """Sign-up, broadcast list, expiry — and /start never resets the clock."""
    from datetime import UTC

    memory = Memory(tmp_path / "memory.db")
    now = datetime(2026, 8, 12, 10, 0, tzinfo=UTC)
    expires = now + timedelta(days=30)

    assert memory.add_subscriber("111", "abu_azam", "Azam", now, expires)
    # a second /start must not extend the trial
    assert not memory.add_subscriber("111", "abu_azam", "Azam", now, expires + timedelta(days=99))
    assert memory.subscriber("111")["expires_at"] == expires.isoformat()

    assert memory.active_subscriber_ids(now + timedelta(days=29)) == ["111"]
    assert memory.active_subscriber_ids(now + timedelta(days=31)) == []

    due = memory.expire_due_subscribers(now + timedelta(days=31))
    assert [row["chat_id"] for row in due] == ["111"]
    assert memory.subscriber("111")["status"] == "expired"
    # already expired: not returned twice, so the farewell is sent only once
    assert memory.expire_due_subscribers(now + timedelta(days=32)) == []
    assert memory.subscriber_counts() == {"trial": 0, "expired": 1}
