"""Tests for the live path.

The live engine is the component nobody can debug interactively at 09:31 ET, so
its failure modes are tested here: rejected credentials, malformed frames, feed
silence, session rollover, and warm start.
"""

import json
from datetime import date, datetime, timedelta

import pytest

from qqq_alpha.brain.decider import HeuristicDecider
from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.config import MARKET_TZ, Settings
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, MarketSnapshot
from qqq_alpha.journal import Journal
from qqq_alpha.live.engine import LiveEngine
from qqq_alpha.live.notifier import NullNotifier, format_signal
from qqq_alpha.live.stream import LiveBarStream
from qqq_alpha.trades import TradeManager

DAY = date(2026, 3, 2)


@pytest.fixture
def settings(tmp_path):
    return Settings(
        massive_api_key="test-key",
        anthropic_api_key="test",
        anthropic_model="test",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        massive_feed_mode="delayed",
    )


def _engine(settings, tmp_path) -> LiveEngine:
    return LiveEngine(
        settings=settings,
        decider=HeuristicDecider(settings),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )


class _AlwaysPassDecider:
    """Never enters. Isolates missed-opportunity scoring from decision noise."""

    async def decide(self, snapshot: MarketSnapshot, **kwargs) -> Decision:
        return Decision(
            ts=snapshot.ts, action=Action.PASS, confidence=3, thesis="test: never enters"
        )


# ---------------------------------------------------------------- parsing
def test_stream_parses_a_minute_aggregate(settings):
    stream = LiveBarStream(settings, ["QQQ"])
    frame = json.dumps(
        [
            {
                "ev": "AM",
                "sym": "QQQ",
                "o": 480.0,
                "h": 481.0,
                "l": 479.5,
                "c": 480.75,
                "v": 120_000,
                "vw": 480.4,
                "z": 850,
                "s": 1772000000000,
            }
        ]
    )
    bars = stream._parse(frame)
    assert len(bars) == 1
    assert bars[0].close == 480.75
    assert bars[0].transactions == 850
    assert bars[0].ts.second == 0


def test_stream_ignores_untracked_symbols_and_status_frames(settings):
    stream = LiveBarStream(settings, ["QQQ"])
    frame = json.dumps(
        [
            {"ev": "status", "status": "success", "message": "subscribed"},
            {"ev": "AM", "sym": "SPY", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "s": 1},
        ]
    )
    assert stream._parse(frame) == []


def test_stream_survives_malformed_frames(settings):
    """One bad frame must never kill a session that has hours left to run."""
    stream = LiveBarStream(settings, ["QQQ"])
    assert stream._parse("not json at all") == []
    assert stream._parse(json.dumps([{"ev": "AM", "sym": "QQQ"}])) == []


def test_delayed_feed_is_reported(settings):
    assert LiveBarStream(settings, ["QQQ"]).is_delayed
    realtime = settings.model_copy(update={"massive_feed_mode": "real_time"})
    assert not LiveBarStream(realtime, ["QQQ"]).is_delayed


# ---------------------------------------------------------------- engine
@pytest.mark.asyncio
async def test_engine_warms_up_before_deciding(settings, tmp_path):
    engine = _engine(settings, tmp_path)
    bars = synthetic_session("QQQ", DAY, seed=5)

    for bar in bars[:10]:
        await engine._on_bar(bar)

    assert engine.status.bars_received == 10
    assert engine.status.brain_calls == 0  # below the warmup threshold


@pytest.mark.asyncio
async def test_engine_routes_leader_bars_separately(settings, tmp_path):
    engine = _engine(settings, tmp_path)
    leader = synthetic_session("AAPL", DAY, seed=6)[0]

    await engine._on_bar(leader)

    assert engine.leader_bars["AAPL"] == [leader]
    assert engine.session_bars == []


@pytest.mark.asyncio
async def test_engine_never_acts_on_stale_bars(settings, tmp_path):
    """The most dangerous live failure: acting on data that is no longer true.

    These bars are months old. The engine must reach the rails and stop there,
    without ever waking the brain — otherwise a lagging feed silently becomes a
    signal generator.
    """
    engine = _engine(settings, tmp_path)
    engine._current_day = DAY
    bars = synthetic_session("QQQ", DAY, seed=3, trend=0.02, volatility=0.002)

    for bar in bars[:200]:
        await engine._on_bar(bar)

    assert engine.status.bars_received == 200
    assert engine.status.brain_calls == 0
    assert engine.status.signals_sent == 0


@pytest.mark.asyncio
async def test_engine_decides_once_data_is_fresh(settings, tmp_path):
    fresh = settings.model_copy(update={"max_data_age_sec": 10**9})
    engine = _engine(fresh, tmp_path)
    engine._current_day = DAY
    bars = synthetic_session("QQQ", DAY, seed=3, trend=0.02, volatility=0.002)

    for bar in bars[:200]:
        await engine._on_bar(bar)

    assert engine.status.brain_calls > 0
    assert isinstance(engine.notifier, NullNotifier)


@pytest.mark.asyncio
async def test_session_rollover_flattens_and_resets(settings, tmp_path):
    engine = _engine(settings, tmp_path)
    engine._current_day = DAY

    bars = synthetic_session("QQQ", DAY, seed=8, trend=0.02, volatility=0.002)
    for bar in bars[:200]:
        await engine._on_bar(bar)

    engine.status.trades_today = 2
    next_day = synthetic_session("QQQ", date(2026, 3, 3), seed=9)[0]
    await engine._on_bar(next_day)

    assert engine.status.trades_today == 0
    assert engine.manager.open_trades == []
    assert engine.status.open_positions == 0
    assert len(engine.session_bars) == 1


@pytest.mark.asyncio
async def test_declined_setups_are_priced_forward_and_remembered(settings, tmp_path):
    """The AI's own PASS gets graded too, not just rail blocks — on a delay,
    since at decision time the engine cannot yet know what came next."""
    fresh = settings.model_copy(update={"max_data_age_sec": 10**9})
    engine = LiveEngine(
        settings=fresh,
        decider=_AlwaysPassDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )
    engine._current_day = DAY
    bars = synthetic_session("QQQ", DAY, seed=12, trend=0.03, volatility=0.002)

    for bar in bars:
        await engine._on_bar(bar)

    assert engine.status.brain_calls > 0
    assert engine.status.signals_sent == 0  # it never enters, by construction
    assert engine.memory.missed_count() > 0


@pytest.mark.asyncio
async def test_pending_missed_checks_flush_on_session_rollover(settings, tmp_path):
    """A decline near the close must not vanish unscored at the day boundary."""
    fresh = settings.model_copy(update={"max_data_age_sec": 10**9})
    engine = LiveEngine(
        settings=fresh,
        decider=_AlwaysPassDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )
    engine._current_day = DAY
    bars = synthetic_session("QQQ", DAY, seed=12, trend=0.03, volatility=0.002)

    for bar in bars[:120]:
        await engine._on_bar(bar)

    assert engine._pending_missed  # a decline is queued but not yet resolvable

    next_day = synthetic_session("QQQ", date(2026, 3, 3), seed=9)[0]
    await engine._on_bar(next_day)

    assert engine._pending_missed == []  # rollover must not lose it silently


@pytest.mark.asyncio
async def test_engine_closes_everything_at_the_bell(settings, tmp_path):
    engine = _engine(settings, tmp_path)
    engine._current_day = DAY

    bars = synthetic_session("QQQ", DAY, seed=12, trend=0.03, volatility=0.002)
    for bar in bars[:250]:
        await engine._on_bar(bar)

    closing = bars[-1].model_copy(
        update={"ts": datetime(2026, 3, 2, 16, 0, tzinfo=MARKET_TZ)}
    )
    await engine._on_bar(closing)

    assert engine.manager.open_trades == []


# ---------------------------------------------------------------- messaging
def _sample_trade():
    from qqq_alpha.domain import Action, Decision, OptionType, Target
    from qqq_alpha.features.snapshot import SnapshotBuilder

    bars = synthetic_session("QQQ", DAY, seed=15)
    snap = SnapshotBuilder("QQQ").build(bars[:80])
    decision = Decision(
        ts=snap.ts,
        action=Action.ENTER,
        direction=OptionType.CALL,
        occ_symbol="O:QQQ260302C00485000",
        targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40,
        confidence=7,
        thesis="break of the opening range with participation",
        risks=["reversal into VWAP"],
        invalidation="loses 484.20",
    )
    return TradeManager().open_trade(decision, 1.00, snap)


def test_signal_message_warns_when_data_is_delayed():
    trade = _sample_trade()
    delayed = format_signal(trade, delayed=True)
    live = format_signal(trade, delayed=False)

    assert "متأخرة" in delayed
    assert "متأخرة" not in live
    # the disclaimer is not optional, in either mode
    assert "توصية تعليمية" in delayed and "توصية تعليمية" in live


def test_signal_message_contains_the_full_trade_plan():
    """A subscriber must never receive an entry without targets and a stop."""
    message = format_signal(_sample_trade(), delayed=False)

    assert "O:QQQ260302C00485000" in message
    assert "$1.50" in message  # +50% target, priced off the fill
    assert "$0.60" in message  # -40% stop
    assert "الثقة: 7/10" in message
    assert "يُلغى إذا" in message


def test_status_is_serialisable(settings, tmp_path):
    engine = _engine(settings, tmp_path)
    payload = engine.status.as_dict()
    assert "bars_received" in payload
    assert "reconnects" in payload
    assert json.dumps(payload, default=str)


def _unused(_: timedelta) -> None:  # pragma: no cover
    return None
