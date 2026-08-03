"""Tests for the shadow-period machinery: persistence, delivery, review.

The theme is survivability. During a two-month shadow run the process will be
restarted, Telegram will rate-limit, and the journal will be read weeks after
it was written. None of those can lose a signal.
"""

import json
from datetime import date, datetime, timedelta

import httpx
import pytest

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, OptionType, Target
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal
from qqq_alpha.live.review import load_period, review
from qqq_alpha.live.state import SessionState, StateStore
from qqq_alpha.live.telegram import FanoutNotifier, TelegramCommandListener, TelegramNotifier
from qqq_alpha.trades import TradeManager

DAY = date(2026, 3, 2)


def _trade(entry: float = 1.00):
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
        thesis="test setup",
    )
    return TradeManager().open_trade(decision, entry, snap)


# ---------------------------------------------------------------- state
def test_state_round_trips(tmp_path):
    store = StateStore(tmp_path / "state.json")
    trade = _trade()
    store.save(SessionState(session_day=DAY, trades_today=1, open_trades=[trade]))

    restored = store.load(expected_day=DAY)
    assert restored is not None
    assert restored.trades_today == 1
    assert restored.open_trades[0].occ_symbol == trade.occ_symbol
    assert restored.open_trades[0].entry_price == trade.entry_price
    # the whole plan must survive, not just the symbol
    assert restored.open_trades[0].decision.targets[0].price == 1.5


def test_state_from_a_previous_day_is_discarded(tmp_path):
    store = StateStore(tmp_path / "state.json")
    store.save(SessionState(session_day=DAY, trades_today=2))

    assert store.load(expected_day=date(2026, 3, 3)) is None
    assert store.load(expected_day=DAY) is not None


def test_corrupt_state_never_blocks_startup(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{ this is not json", encoding="utf-8")
    assert StateStore(path).load() is None


def test_state_write_is_atomic(tmp_path):
    """A crash mid-write must not leave a truncated file that loses the session."""
    store = StateStore(tmp_path / "state.json")
    store.save(SessionState(session_day=DAY, trades_today=1))
    store.save(SessionState(session_day=DAY, trades_today=2))

    assert not (tmp_path / "state.tmp").exists()
    assert store.load(expected_day=DAY).trades_today == 2


# ---------------------------------------------------------------- telegram
def test_long_messages_split_on_line_boundaries():
    text = "\n".join(f"line {i} with some content" for i in range(400))
    chunks = TelegramNotifier._chunks(text)

    assert len(chunks) > 1
    assert all(len(chunk) <= 4000 for chunk in chunks)
    # nothing is lost or duplicated in the split
    assert "\n".join(chunks) == text


def test_notifier_requires_both_credentials():
    with pytest.raises(ValueError):
        TelegramNotifier("", "123")
    with pytest.raises(ValueError):
        TelegramNotifier("token", "")


async def test_telegram_retries_then_gives_up_without_raising():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, json={"ok": False})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        notifier = TelegramNotifier("token", "chat", client=client)
        # a failed send must never propagate into the trading loop
        await notifier.note("hello")

    assert calls["n"] == 4
    assert notifier.failures == 1


async def test_telegram_sends_successfully():
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        notifier = TelegramNotifier("token", "chat", client=client)
        await notifier.signal(_trade(), delayed=True)

    assert len(seen) == 1
    assert seen[0]["chat_id"] == "chat"
    assert "متأخرة" in seen[0]["text"]


async def test_command_listener_only_accepts_the_configured_chat():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "ok": True,
                "result": [
                    {"update_id": 100, "message": {"chat": {"id": 999}, "text": "موافق 1"}},
                    {"update_id": 101, "message": {"chat": {"id": 1}, "text": "موافق 2"}},
                ],
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        listener = TelegramCommandListener("token", "999", client=client)
        commands = await listener.poll()

    assert commands == ["موافق 1"]  # the other chat's message is not authorization


async def test_command_listener_advances_the_offset_so_updates_are_not_replayed():
    offsets_seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        offsets_seen.append(request.url.params.get("offset"))
        return httpx.Response(
            200,
            json={
                "ok": True,
                "result": [{"update_id": 5, "message": {"chat": {"id": 1}, "text": "رفض 1"}}],
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        listener = TelegramCommandListener("token", "1", client=client)
        await listener.poll()
        await listener.poll()

    assert offsets_seen == ["0", "6"]


async def test_command_listener_survives_a_network_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("boom")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        listener = TelegramCommandListener("token", "1", client=client)
        assert await listener.poll() == []


async def test_fanout_survives_a_broken_channel():
    class Broken:
        async def note(self, text): raise RuntimeError("channel down")

    class Working:
        def __init__(self): self.notes = []
        async def note(self, text): self.notes.append(text)

    working = Working()
    await FanoutNotifier(Broken(), working).note("still delivered")
    assert working.notes == ["still delivered"]


# ---------------------------------------------------------------- review
def _journal_with_trades(tmp_path, returns: list[float], confidences: list[int] | None = None):
    journal = Journal(tmp_path, session_tag="test")
    confidences = confidences or [7] * len(returns)

    for index, (result, confidence) in enumerate(zip(returns, confidences, strict=False)):
        trade = _trade()
        trade.trade_id = f"t{index}"
        trade.decision.confidence = confidence
        opened = datetime(2026, 3, 2, 10, 0, tzinfo=MARKET_TZ) + timedelta(days=index)
        trade.opened_at = opened
        trade.closed_at = opened + timedelta(minutes=30)
        trade.return_pct = result
        trade.max_favorable_pct = max(result, 0)
        trade.exit_reason = "target" if result > 0 else "stop_hit"
        journal.log_trade(trade)
    return journal


def test_review_computes_the_headline_numbers(tmp_path):
    _journal_with_trades(tmp_path, [80.0, -40.0, 120.0, -40.0])
    stats = review(load_period(tmp_path))

    assert stats.closed == 4
    assert stats.wins == 2 and stats.losses == 2
    assert stats.win_rate == 50.0
    assert stats.expectancy_pct == 30.0
    assert stats.profit_factor == 2.5
    assert stats.ran_100 == 1


def test_review_flags_a_result_carried_by_one_trade(tmp_path):
    _journal_with_trades(tmp_path, [500.0, -40.0, -40.0, 10.0])
    stats = review(load_period(tmp_path))

    assert any("single trade" in w for w in stats.warnings)


def test_review_flags_a_meaningless_confidence_score(tmp_path):
    _journal_with_trades(
        tmp_path, [100.0, -40.0, -40.0, 90.0], confidences=[4, 9, 9, 4]
    )
    stats = review(load_period(tmp_path))

    assert any("confidence score" in w for w in stats.warnings)


def test_review_warns_while_the_sample_is_small(tmp_path):
    _journal_with_trades(tmp_path, [50.0, 60.0])
    stats = review(load_period(tmp_path))

    assert any("too few to judge" in w for w in stats.warnings)


def test_review_says_nothing_can_be_concluded_from_nothing(tmp_path):
    stats = review(load_period(tmp_path))
    assert stats.closed == 0
    assert any("nothing can be concluded" in w for w in stats.warnings)


def test_review_keeps_only_the_final_state_of_each_trade(tmp_path):
    """The journal is append-only: a trade is written on every update."""
    journal = Journal(tmp_path, session_tag="test")
    trade = _trade()
    trade.trade_id = "same-id"
    trade.opened_at = datetime(2026, 3, 2, 10, 0, tzinfo=MARKET_TZ)

    journal.log_trade(trade)  # open
    trade.return_pct = 75.0
    trade.closed_at = trade.opened_at + timedelta(minutes=20)
    journal.log_trade(trade)  # closed

    stats = review(load_period(tmp_path))
    assert stats.closed == 1
    assert stats.expectancy_pct == 75.0
