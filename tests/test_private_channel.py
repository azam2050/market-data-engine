"""The private subscribers channel: one tap on the invite link is the whole
sign-up. The bot approves, the trial clock starts, delivery becomes a single
post per signal, and expiry means removal — all without a human touching it."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from qqq_alpha.brain.decider import HeuristicDecider
from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.config import Settings
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.journal import Journal
from qqq_alpha.live.engine import LiveEngine
from qqq_alpha.live.notifier import NullNotifier
from qqq_alpha.live.telegram import (
    BroadcastNotifier,
    JoinRequest,
    TelegramCommandListener,
)
from qqq_alpha.memory import Memory

PRIVATE = "-1001234567890"


def _recording_transport(calls: list[tuple[str, dict]]):
    def handler(request: httpx.Request) -> httpx.Response:
        method = request.url.path.rsplit("/", 1)[-1]
        try:
            payload = json.loads(request.content) if request.content else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {}  # multipart photo bodies are not JSON
        calls.append((method, payload))
        if method == "getUpdates":
            return httpx.Response(200, json={"ok": True, "result": []})
        if method == "createChatInviteLink":
            return httpx.Response(
                200, json={"ok": True, "result": {"invite_link": "https://t.me/+abc"}}
            )
        return httpx.Response(200, json={"ok": True, "result": {}})

    return httpx.MockTransport(handler)


def _engine(tmp_path, calls: list[tuple[str, dict]]) -> LiveEngine:
    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        telegram_bot_token="token",
        telegram_chat_id="admin",
        telegram_private_channel_id=PRIVATE,
        shadow_symbols_csv="",
    )
    engine = LiveEngine(
        settings=settings,
        decider=HeuristicDecider(settings),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )
    assert engine.commands is not None
    engine.commands._client = httpx.AsyncClient(transport=_recording_transport(calls))
    return engine


def _methods(calls: list[tuple[str, dict]]) -> list[str]:
    return [method for method, _ in calls]


# ---------------------------------------------------------------- listener
@pytest.mark.asyncio
async def test_listener_surfaces_join_requests_and_promotions():
    def handler(request: httpx.Request) -> httpx.Response:
        assert "chat_join_request" in dict(request.url.params)["allowed_updates"]
        return httpx.Response(
            200,
            json={
                "ok": True,
                "result": [
                    {
                        "update_id": 1,
                        "my_chat_member": {
                            "chat": {"id": -100999, "type": "channel", "title": "خاصة"},
                            "new_chat_member": {"status": "administrator"},
                        },
                    },
                    {
                        "update_id": 2,
                        "chat_join_request": {
                            "chat": {"id": -100999, "type": "channel"},
                            "from": {"id": 777, "username": "u", "first_name": "U"},
                        },
                    },
                ],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        listener = TelegramCommandListener("token", "admin", client=client)
        listener._webhook_cleared = True
        messages = await listener.poll(timeout=0)

    assert messages == []
    assert listener.channel_promotions == [("-100999", "خاصة")]
    assert listener.join_requests == [
        JoinRequest(channel_id="-100999", user_id="777", username="u", first_name="U")
    ]


# ---------------------------------------------------------------- join flow
@pytest.mark.asyncio
async def test_new_join_request_gets_the_consent_gate_not_auto_approval(tmp_path):
    """Nobody enters before pressing أوافق: the request stays pending, the
    terms arrive with buttons, and nothing is registered yet."""
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)

    await engine._handle_join_request(
        JoinRequest(channel_id=PRIVATE, user_id="777", username="new", first_name="N")
    )

    methods = _methods(calls)
    assert "approveChatJoinRequest" not in methods  # gate first
    assert "sendMessage" in methods  # the consent message with buttons
    consent_payload = next(p for m, p in calls if m == "sendMessage")
    assert "reply_markup" in consent_payload
    assert "إقرار وإخلاء مسؤولية" in consent_payload["text"]
    assert engine.memory.subscriber("777") is None  # nothing recorded yet


@pytest.mark.asyncio
async def test_pressing_agree_admits_registers_and_records_consent(tmp_path):
    from qqq_alpha.live.telegram import CONSENT_YES, ButtonPress

    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)

    await engine._handle_button_press(
        ButtonPress(
            callback_id="cb1", user_id="777", chat_id="777", message_id=5,
            data=CONSENT_YES, username="new", first_name="N",
        )
    )

    methods = _methods(calls)
    assert "approveChatJoinRequest" in methods
    assert "answerCallbackQuery" in methods
    assert "editMessageText" in methods  # the buttons are retired in place
    row = engine.memory.subscriber("777")
    assert row is not None and row["status"] == "trial"
    assert row["consented_at"]  # the legal timestamp
    # welcome note + the cards guide both went out
    texts = [p.get("text", "") for m, p in calls if m == "sendMessage"]
    assert any("دليل ألوان البطاقات" in t for t in texts)


@pytest.mark.asyncio
async def test_pressing_decline_rejects_and_records_nothing(tmp_path):
    from qqq_alpha.live.telegram import CONSENT_NO, ButtonPress

    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)

    await engine._handle_button_press(
        ButtonPress(
            callback_id="cb2", user_id="888", chat_id="888", message_id=6,
            data=CONSENT_NO, username="no", first_name="No",
        )
    )

    methods = _methods(calls)
    assert "declineChatJoinRequest" in methods
    assert "approveChatJoinRequest" not in methods
    assert engine.memory.subscriber("888") is None  # a decline burns nothing
    # ...so coming back a minute later and agreeing works as a first-timer
    from qqq_alpha.live.telegram import CONSENT_YES

    await engine._handle_button_press(
        ButtonPress(
            callback_id="cb3", user_id="888", chat_id="888", message_id=7,
            data=CONSENT_YES, username="no", first_name="No",
        )
    )
    row = engine.memory.subscriber("888")
    assert row is not None and row["status"] == "trial"


@pytest.mark.asyncio
async def test_expired_subscriber_join_request_is_declined(tmp_path):
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)
    past = datetime.now(UTC) - timedelta(days=40)
    engine.memory.add_subscriber("888", "old", "O", joined_at=past, expires_at=past + timedelta(days=30))
    engine.memory.expire_due_subscribers(datetime.now(UTC))

    await engine._handle_join_request(
        JoinRequest(channel_id=PRIVATE, user_id="888", username="old", first_name="O")
    )

    methods = _methods(calls)
    assert "declineChatJoinRequest" in methods
    assert "approveChatJoinRequest" not in methods


@pytest.mark.asyncio
async def test_foreign_channel_requests_are_ignored(tmp_path):
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)

    await engine._handle_join_request(
        JoinRequest(channel_id="-100555", user_id="999", username="x", first_name="X")
    )

    assert calls == []  # not our channel, not our verdict
    assert engine.memory.subscriber("999") is None


# ---------------------------------------------------------------- expiry
@pytest.mark.asyncio
async def test_expiry_kicks_from_the_private_channel(tmp_path):
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)
    past = datetime.now(UTC) - timedelta(days=40)
    engine.memory.add_subscriber("777", "u", "U", joined_at=past, expires_at=past + timedelta(days=30))

    await engine._expire_subscribers()

    methods = _methods(calls)
    assert "banChatMember" in methods  # the removal
    assert "unbanChatMember" in methods  # so a future paid re-join works
    assert "sendMessage" in methods  # the farewell DM


# ---------------------------------------------------------------- delivery
@pytest.mark.asyncio
async def test_private_delivery_is_one_post_plus_operator_copy(tmp_path):
    """1000 subscribers or 10: the signal is ONE channel post. Subscriber
    DMs disappear entirely; the operator keeps a text audit copy."""
    from datetime import date

    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.domain import Action, Decision, OptionType, Target
    from qqq_alpha.features.snapshot import SnapshotBuilder
    from qqq_alpha.trades import TradeManager

    memory = Memory(tmp_path / "memory.db")
    now = datetime.now(UTC)
    for uid in ("201", "202", "203"):
        memory.add_subscriber(uid, uid, uid, joined_at=now, expires_at=now + timedelta(days=30))

    photo_chats: list[str] = []
    text_chats: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("sendPhoto"):
            # multipart body: chat_id arrives as a form field
            photo_chats.append(request.content.split(b'name="chat_id"')[1][:40].decode(errors="ignore"))
            return httpx.Response(200, json={"ok": True})
        text_chats.append(json.loads(request.content)["chat_id"])
        return httpx.Response(200, json={"ok": True})

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=21)
    snap = SnapshotBuilder("QQQ").build(bars[:80])
    decision = Decision(
        ts=snap.ts, action=Action.ENTER, direction=OptionType.CALL,
        occ_symbol="O:QQQ260302C00485000",
        targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40, confidence=7, thesis="x",
    )
    trade = TradeManager().open_trade(decision, 1.00, snap)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        notifier = BroadcastNotifier(
            "token", "admin", memory, client=client, private_channel_id=PRIVATE
        )
        await notifier.signal(trade, delayed=False)

    assert len(photo_chats) == 1 and PRIVATE in photo_chats[0]  # one channel post
    assert text_chats == ["admin"]  # the audit copy only — no subscriber DMs


# ---------------------------------------------------------------- living card
@pytest.mark.asyncio
async def test_heartbeat_edits_the_posted_card_instead_of_new_messages(tmp_path):
    """Every 15 minutes the entry card's badge refreshes in place — 'still
    in the trade, now +X%' — one message that lives, not a feed of pings."""
    from datetime import date
    from datetime import timedelta as td

    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.domain import Action, Decision, OptionType, Target
    from qqq_alpha.features.snapshot import SnapshotBuilder
    from qqq_alpha.trades import TradeManager

    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        method = request.url.path.rsplit("/", 1)[-1]
        methods.append(method)
        if method == "sendPhoto":
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})
        return httpx.Response(200, json={"ok": True, "result": {}})

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=21)
    snap = SnapshotBuilder("QQQ").build(bars[:80])
    decision = Decision(
        ts=snap.ts, action=Action.ENTER, direction=OptionType.CALL,
        occ_symbol="O:QQQ260302C00485000",
        targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40, confidence=7, thesis="x",
    )
    manager = TradeManager()
    trade = manager.open_trade(decision, 1.00, snap)

    memory = Memory(tmp_path / "memory.db")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        notifier = BroadcastNotifier(
            "token", "admin", memory, client=client, private_channel_id=PRIVATE
        )
        await notifier.signal(trade, delayed=False)
        assert notifier._live_cards[trade.trade_id] == 42

        heartbeat = manager.update(trade, 1.12, trade.opened_at + td(minutes=16))
        assert heartbeat is not None and heartbeat.note.startswith("status:")
        await notifier.update(trade, heartbeat, delayed=False)

    assert methods.count("sendPhoto") == 1        # the entry card, once
    assert methods.count("editMessageMedia") == 1  # the living refresh


@pytest.mark.asyncio
async def test_preview_command_sends_terms_guide_and_sample_cards(tmp_path):
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)

    await engine._handle_command("معاينه")  # the common taa/haa misspelling counts

    methods = _methods(calls)
    assert methods.count("sendPhoto") >= 5  # watch, entry, live, scale-out, closes
    consent = next(p for m, p in calls if m == "sendMessage" and "reply_markup" in p)
    assert "إقرار وإخلاء مسؤولية" in consent["text"]
