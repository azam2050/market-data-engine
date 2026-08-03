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
from qqq_alpha.domain import Action, Decision, MarketRegime, OptionType, Target
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.learning import MIN_TOTAL_TRADES, analyse, apply_lesson, propose
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
    assert settings.playbook_path.exists()
    assert not memory.pending_lessons()


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
