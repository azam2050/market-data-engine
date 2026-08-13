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
from qqq_alpha.live.notifier import NullNotifier, format_signal, format_update, human_contract
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
async def test_daily_review_lists_pending_lessons_with_reply_instructions(settings, tmp_path):
    engine = _engine(settings, tmp_path)
    engine._current_day = DAY
    engine.memory.save_lesson("a real finding worth reviewing", "evidence", 12, 0.6)

    await engine._run_daily_review()

    combined = "\n".join(engine.notifier.notes)
    assert "a real finding worth reviewing" in combined
    assert "موافق" in combined and "رفض" in combined


@pytest.mark.asyncio
async def test_daily_review_says_so_when_nothing_is_pending(settings, tmp_path):
    engine = _engine(settings, tmp_path)
    engine._current_day = DAY

    await engine._run_daily_review()

    combined = "\n".join(engine.notifier.notes)
    assert "موافق" not in combined  # nothing to approve, so no instructions either


# ---------------------------------------------------------------- lesson approval by reply
def _engine_with_writable_playbook(settings, tmp_path) -> LiveEngine:
    scoped = settings.model_copy(update={"playbook_path": tmp_path / "playbook.yaml"})
    return LiveEngine(
        settings=scoped,
        decider=HeuristicDecider(scoped),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )


@pytest.mark.asyncio
async def test_approve_reply_applies_the_lesson_and_updates_the_running_playbook(
    settings, tmp_path
):
    engine = _engine_with_writable_playbook(settings, tmp_path)
    lesson_id = engine.memory.save_lesson("test claim", "some evidence", 12, 0.6)

    await engine._handle_command(f"موافق {lesson_id}")

    assert engine.playbook.version == 2  # the engine's own running copy
    assert not engine.memory.pending_lessons()
    # durability lives in memory, not the (ephemeral) playbook file: a fresh
    # engine over the same data dir must boot with the lesson already in place
    rebooted = _engine_with_writable_playbook(settings, tmp_path)
    assert rebooted.playbook.version == 2
    assert rebooted.playbook.lessons and rebooted.playbook.lessons[0].statement == "test claim"


@pytest.mark.asyncio
async def test_reject_reply_clears_the_lesson_without_touching_the_playbook(settings, tmp_path):
    engine = _engine_with_writable_playbook(settings, tmp_path)
    lesson_id = engine.memory.save_lesson("test claim", "some evidence", 12, 0.6)

    await engine._handle_command(f"reject {lesson_id}")

    assert engine.playbook.version == 1
    assert not engine.memory.pending_lessons()


@pytest.mark.asyncio
async def test_malformed_replies_are_ignored_without_raising(settings, tmp_path):
    engine = _engine_with_writable_playbook(settings, tmp_path)
    await engine._handle_command("hello there")
    await engine._handle_command("موافق not-a-number")
    await engine._handle_command("موافق 9999")  # well-formed, but no such lesson

    assert engine.playbook.version == 1


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

    # a trader-readable label, not the raw OCC symbol nobody reads at a glance
    assert "QQQ 485 CALL 0DTE" in message
    assert "$1.50" in message  # +50% target, priced off the fill
    assert "$0.60" in message  # -40% stop
    assert "الثقة: 7/10" in message
    assert "يُلغى إذا" in message


def test_signal_message_carries_size_thesis_stop_and_exit_plan():
    """The subscriber acts on the size line and the thesis-stop level directly."""
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
        confidence=6,
        thesis="x",
        invalidation_level=484.2,
        size_factor=0.5,
    )
    message = format_signal(TradeManager().open_trade(decision, 1.00, snap), delayed=False)

    assert "نصف الحجم المعتاد" in message
    assert "وقف الفكرة" in message and "484.20" in message
    assert "نبيع النصف ونؤمّن التكلفة" in message


def test_size_factor_pays_for_conviction_and_fears_the_open():
    from qqq_alpha.domain import Action, Decision
    from qqq_alpha.live.engine import LiveEngine

    def _decision(confidence: int) -> Decision:
        return Decision(
            ts=datetime(2026, 3, 2, 11, 0, tzinfo=MARKET_TZ),
            action=Action.ENTER,
            confidence=confidence,
        )

    midday = datetime(2026, 3, 2, 11, 0, tzinfo=MARKET_TZ)
    open_min = datetime(2026, 3, 2, 9, 36, tzinfo=MARKET_TZ)

    assert LiveEngine._size_factor(_decision(8), midday) == 1.0
    assert LiveEngine._size_factor(_decision(7), midday) == 0.75
    assert LiveEngine._size_factor(_decision(6), midday) == 0.5
    # the first hour halves everything: the record's worst losses lived there
    assert LiveEngine._size_factor(_decision(8), open_min) == 0.5
    assert LiveEngine._size_factor(_decision(5), open_min) == 0.25


# ---------------------------------------------------------------- human_contract
def test_human_contract_labels_a_same_day_call():
    as_of = datetime(2026, 3, 2, 10, 0, tzinfo=MARKET_TZ)
    assert human_contract("O:QQQ260302C00485000", as_of) == "QQQ 485 CALL 0DTE"


def test_human_contract_labels_a_next_day_put():
    as_of = datetime(2026, 3, 2, 15, 45, tzinfo=MARKET_TZ)
    assert human_contract("O:QQQ260303P00720000", as_of) == "QQQ 720 PUT 1DTE"


def test_human_contract_keeps_a_fractional_strike():
    as_of = datetime(2026, 3, 2, 10, 0, tzinfo=MARKET_TZ)
    assert human_contract("O:QQQ260302C00484500", as_of) == "QQQ 484.5 CALL 0DTE"


def test_human_contract_falls_back_to_the_raw_symbol_if_unparseable():
    as_of = datetime(2026, 3, 2, 10, 0, tzinfo=MARKET_TZ)
    assert human_contract("not-a-real-symbol", as_of) == "not-a-real-symbol"


def test_update_message_uses_the_human_label_too():
    from qqq_alpha.domain import TradeUpdate

    trade = _sample_trade()
    update = TradeUpdate(ts=trade.opened_at, price=1.5, return_pct=50.0, note="target:T1 reached")

    message = format_update(trade, update, delayed=False)
    assert "QQQ 485 CALL 0DTE" in message


def test_status_is_serialisable(settings, tmp_path):
    engine = _engine(settings, tmp_path)
    payload = engine.status.as_dict()
    assert "bars_received" in payload
    assert "reconnects" in payload
    assert json.dumps(payload, default=str)


def _unused(_: timedelta) -> None:  # pragma: no cover
    return None


def test_an_infeasible_decline_is_never_queued_as_missed(settings, tmp_path):
    """Nobody can buy an option before the open — a pre-open rail block is not
    a missed opportunity, and pricing it forward poisons the learning loop."""
    from qqq_alpha.features.snapshot import SnapshotBuilder

    engine = LiveEngine(
        settings=settings,
        decider=_AlwaysPassDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )
    bars = synthetic_session("QQQ", DAY, seed=12, trend=0.03, volatility=0.002)
    snapshot = SnapshotBuilder("QQQ").build(bars[:120])
    for obs in snapshot.observations:
        obs.score = 1.0  # a strong bias, so only the block reason decides

    engine._queue_missed_check(snapshot, ["outside_session: 09:29 ET"])
    assert engine._pending_missed == []

    engine._queue_missed_check(snapshot, ["daily_trade_cap: 2/2"])
    assert len(engine._pending_missed) == 1


def test_prompt_renders_recalled_trades_from_durable_memory():
    """Regression: at boot and at each session roll the engine reloads recent
    trades as RecalledTrade summaries, not full Trade objects. Assuming the
    full shape crashed the engine mid-session the first day the memory
    actually had a trade to reload."""
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.features.snapshot import SnapshotBuilder
    from qqq_alpha.memory import RecalledTrade

    bars = synthetic_session("QQQ", DAY, seed=12)
    snapshot = SnapshotBuilder("QQQ").build(bars[:120])
    recalled = RecalledTrade(
        trade_id="t1",
        opened_at="2026-08-06T19:08:00+00:00",
        direction="CALL",
        return_pct=-47.7,
        max_favorable_pct=7.0,
        confidence=6,
        regime="VOLATILE_CHOP",
        thesis="دخول جريء",
        exit_reason="stop_hit",
    )
    prompt = build_user_prompt(snapshot, Playbook(), recent_trades=[recalled])
    assert "RECENT TRADES" in prompt
    assert "-47.7" in prompt


@pytest.mark.asyncio
async def test_a_broken_prompt_becomes_a_safe_pass_not_a_crash(settings):
    """Anything that breaks while assembling the decision context must degrade
    to a PASS the operator can read about, never kill the engine."""
    from qqq_alpha.brain.decider import AIDecider
    from qqq_alpha.features.snapshot import SnapshotBuilder

    bars = synthetic_session("QQQ", DAY, seed=12)
    snapshot = SnapshotBuilder("QQQ").build(bars[:120])

    decision = await AIDecider(settings).decide(
        snapshot=snapshot,
        playbook=Playbook(),
        open_trades=[],
        recent_trades=[object()],  # wrong shape, guaranteed to break rendering
        rail_warnings=[],
        attention_note="",
    )
    assert decision.action is Action.PASS
    assert "فشل تقني" in decision.thesis


def test_prompt_carries_todays_earlier_decisions():
    """Plan continuity: 2026-08-11 produced zero trades on a clean trend day
    because every wake named a trigger and the next wake quietly re-derived a
    new reason to wait. The brain now sees what it said earlier today."""
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.features.snapshot import SnapshotBuilder

    bars = synthetic_session("QQQ", DAY, seed=12)
    snapshot = SnapshotBuilder("QQQ").build(bars[:120])
    earlier = Decision(
        ts=snapshot.ts,
        action=Action.WAIT,
        confidence=4,
        thesis="أنتظر كسر 718.40 مع rel-volume أعلى من 1.2 ثم أدخل PUT",
    )
    prompt = build_user_prompt(snapshot, Playbook(), recent_decisions=[earlier])
    assert "YOUR EARLIER DECISIONS THIS SESSION" in prompt
    assert "718.40" in prompt


@pytest.mark.asyncio
async def test_todays_decisions_accumulate_and_reset_at_rollover(settings, tmp_path):
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
    for bar in bars[:200]:
        await engine._on_bar(bar)

    assert engine._today_decisions  # plans are being carried forward

    next_day = synthetic_session("QQQ", date(2026, 3, 3), seed=9)[0]
    await engine._on_bar(next_day)
    assert engine._today_decisions == []  # yesterday's plans are not today's


class _FakeCommands:
    """Captures direct replies instead of hitting Telegram."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def send(self, chat_id: str, text: str) -> bool:
        self.sent.append((chat_id, text))
        return True


def _subscriber_engine(settings, tmp_path) -> LiveEngine:
    scoped = settings.model_copy(
        update={"trial_days": 30, "post_trial_channel_url": "https://t.me/qqq_free"}
    )
    engine = LiveEngine(
        settings=scoped,
        decider=HeuristicDecider(scoped),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )
    engine.commands = _FakeCommands()
    return engine


@pytest.mark.asyncio
async def test_start_registers_a_trial_and_welcomes(settings, tmp_path):
    from qqq_alpha.live.telegram import InboundMessage

    engine = _subscriber_engine(settings, tmp_path)
    await engine._handle_subscriber(InboundMessage("555", "/start", username="trader1"))

    row = engine.memory.subscriber("555")
    assert row is not None and row["status"] == "trial"
    assert engine.commands.sent and engine.commands.sent[0][0] == "555"
    assert "30" in engine.commands.sent[0][1]  # the trial length is stated
    # the operator is told about the new sign-up
    assert any("مشترك" in note for note in engine.notifier.notes)


@pytest.mark.asyncio
async def test_expired_subscriber_is_pointed_at_the_next_channel(settings, tmp_path):
    from datetime import UTC

    from qqq_alpha.live.telegram import InboundMessage

    engine = _subscriber_engine(settings, tmp_path)
    long_ago = datetime(2026, 1, 1, tzinfo=UTC)
    engine.memory.add_subscriber("777", "old", "Old", long_ago, long_ago + timedelta(days=30))

    await engine._expire_subscribers()

    assert engine.memory.subscriber("777")["status"] == "expired"
    assert any("t.me/qqq_free" in text for _, text in engine.commands.sent)

    # and if they message again later, they get the channel link, not signals
    engine.commands.sent.clear()
    await engine._handle_subscriber(InboundMessage("777", "/start"))
    assert any("t.me/qqq_free" in text for _, text in engine.commands.sent)


@pytest.mark.asyncio
async def test_strangers_without_start_are_ignored(settings, tmp_path):
    from qqq_alpha.live.telegram import InboundMessage

    engine = _subscriber_engine(settings, tmp_path)
    await engine._handle_subscriber(InboundMessage("888", "hello?"))

    assert engine.memory.subscriber("888") is None
    assert engine.commands.sent == []


@pytest.mark.asyncio
async def test_any_operator_message_gets_a_reception_receipt(settings, tmp_path):
    """The operator's one-tap health check: text the bot anything, get an
    answer. Silence here once hid a dead inbound path for weeks."""
    engine = _subscriber_engine(settings, tmp_path)
    await engine._handle_command("هل انت شغال؟")

    assert any("استقبال الرسائل شغال" in note for note in engine.notifier.notes)


@pytest.mark.asyncio
async def test_operator_can_ask_for_subscriber_counts(settings, tmp_path):
    from datetime import UTC

    engine = _subscriber_engine(settings, tmp_path)
    now = datetime(2026, 8, 12, tzinfo=UTC)
    engine.memory.add_subscriber("111", "a", "A", now, now + timedelta(days=30))

    await engine._handle_command("مشتركين")

    assert any("تجريبي نشط: 1" in note for note in engine.notifier.notes)


@pytest.mark.asyncio
async def test_a_refused_welcome_is_escalated_to_the_operator(settings, tmp_path):
    """Registration succeeding while the welcome bounces is a silent funnel
    break — the operator note must carry the warning."""
    from qqq_alpha.live.telegram import InboundMessage

    engine = _subscriber_engine(settings, tmp_path)

    async def refuse(chat_id, text):
        return False

    engine.commands.send = refuse
    await engine._handle_subscriber(InboundMessage("444", "/start", username="friend"))

    assert engine.memory.subscriber("444") is not None  # sign-up still counted
    assert any("تعذر إرسال رسالة الترحيب" in note for note in engine.notifier.notes)
