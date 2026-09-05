"""The updates channel and the consent gate: join requests are approved on
sight, the trial starts on the أوافق press, and the channel check tells the
operator where the report cards land — all without a human touching it."""

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
    FanoutNotifier,
    JoinRequest,
    TelegramCommandListener,
    TelegramNotifier,
)

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
async def test_join_requests_are_approved_on_sight_and_register_nothing(tmp_path):
    """The updates channel is free: whoever knocks is let in, and reading it
    is not a subscription, so no trial row appears."""
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)

    await engine._handle_join_request(
        JoinRequest(channel_id=PRIVATE, user_id="777", username="new", first_name="N")
    )

    methods = _methods(calls)
    assert "approveChatJoinRequest" in methods
    assert "sendMessage" not in methods  # no pitch, no terms — just the door
    assert engine.memory.subscriber("777") is None


@pytest.mark.asyncio
async def test_pressing_agree_registers_and_records_consent(tmp_path):
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
    assert "approveChatJoinRequest" not in methods  # consent opens no channel door
    assert "answerCallbackQuery" in methods
    assert "editMessageText" in methods  # the buttons are retired in place
    row = engine.memory.subscriber("777")
    assert row is not None and row["status"] == "trial"
    assert row["consented_at"]  # the legal timestamp
    # welcome note + the TradingView-name prompt both went out
    texts = [p.get("text", "") for m, p in calls if m == "sendMessage"]
    assert any("اسم المستخدم" in t and "TradingView" in t for t in texts)


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
    assert "declineChatJoinRequest" not in methods  # the channel is not part of the deal
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
async def test_expired_subscriber_join_request_is_still_approved(tmp_path):
    """An ended trial ends the indicator, never the free channel."""
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)
    past = datetime.now(UTC) - timedelta(days=40)
    engine.memory.add_subscriber("888", "old", "O", joined_at=past, expires_at=past + timedelta(days=30))
    engine.memory.expire_due_subscribers(datetime.now(UTC))

    await engine._handle_join_request(
        JoinRequest(channel_id=PRIVATE, user_id="888", username="old", first_name="O")
    )

    methods = _methods(calls)
    assert "approveChatJoinRequest" in methods
    assert "declineChatJoinRequest" not in methods


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
async def test_expiry_sends_the_farewell_and_evicts_nobody(tmp_path):
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)
    past = datetime.now(UTC) - timedelta(days=40)
    engine.memory.add_subscriber("777", "u", "U", joined_at=past, expires_at=past + timedelta(days=30))

    await engine._expire_subscribers()

    methods = _methods(calls)
    assert "banChatMember" not in methods  # the channel stays theirs for good
    assert "sendMessage" in methods  # the farewell DM


@pytest.mark.asyncio
async def test_preview_command_replays_the_whole_customer_journey(tmp_path):
    """The operator asked to see every message a customer gets, in the same
    form: the preview sends the journey verbatim, in order, buttons included,
    and registers nothing."""
    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)

    await engine._handle_command("معاينه")  # the common taa/haa misspelling counts

    texts = [p.get("text", "") for m, p in calls if m == "sendMessage"]
    joined = "\n".join(texts)
    for stage in ("/start", "أوافق وأقر", "TradingView", "تم المنح", "بيومين", "اشتراك", "بعد الدفع", "انتهاء الفترة"):
        assert stage in joined
    # the order the customer lives it
    assert (
        joined.index("مِرصاد") < joined.index("إقرار وإخلاء مسؤولية")
        < joined.index("سُجّل اسمك") < joined.index("تم تفعيل مِرصاد ٩ على حسابك")
        < joined.index("انتهت فترتك المجانية")
    )
    consent = next(p for m, p in calls if m == "sendMessage" and "reply_markup" in p)
    assert "إقرار وإخلاء مسؤولية" in consent["text"]
    assert "sendPhoto" not in _methods(calls)  # no sample cards of the old product
    assert engine.memory.all_subscribers() == []  # a preview registers nobody


# ---------------------------------------------------------------- delivery proof
@pytest.mark.asyncio
async def test_channel_check_reports_admin_rights_and_missing_membership():
    """The operator could not tell a healthy channel from one the bot cannot
    post in — in both cases their phone just shows the text audit copy."""
    state = {
        "status": "administrator",
        "can_post_messages": True,
        "can_edit_messages": True,
        "can_invite_users": True,
        "can_restrict_members": True,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        method = request.url.path.rsplit("/", 1)[-1]
        if method == "getChat":
            return httpx.Response(200, json={"ok": True, "result": {"title": "عقود الخيارات"}})
        if method == "getMe":
            return httpx.Response(200, json={"ok": True, "result": {"id": 555}})
        if method == "getChatMember":
            return httpx.Response(200, json={"ok": True, "result": state})
        return httpx.Response(200, json={"ok": True, "result": {}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        notifier = TelegramNotifier("token", "admin", client=client)

        assert "✅" in await notifier.check_channel(PRIVATE)

        # posting alone is not enough: the living card needs edit rights, the
        # invite links need invite rights, and expiry needs ban rights. A bot
        # granted only "post" used to pass this check and then fail silently
        # on every one of those, weeks later.
        state.pop("can_edit_messages")
        state.pop("can_restrict_members")
        partial = await notifier.check_channel(PRIVATE)
        assert partial.startswith("❌")
        assert "البطاقة الحية" in partial
        assert "إخراج المنتهية" in partial
        # the public channel only ever posts, so the same bot passes there
        assert (await notifier.check_channel(PRIVATE, full_rights=False)).startswith("✅")

        state.clear()
        state.update({"status": "member"})
        as_member = await notifier.check_channel(PRIVATE)
        assert "عضو وليس مشرفًا" in as_member
        assert "إضافة مشرف" in as_member  # the fix, in the message itself

        state.clear()
        state.update({"status": "left"})
        left = await notifier.check_channel(PRIVATE)
        assert left.startswith("❌") and "ليس داخل القناة" in left


@pytest.mark.asyncio
async def test_check_command_posts_a_real_card_to_the_private_channel(tmp_path):
    """"فحص" answers the question end to end: not "are the settings right"
    but "did a card actually land in the channel"."""
    posted: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        method = request.url.path.rsplit("/", 1)[-1]
        if method == "sendPhoto":
            posted.append(request.content.decode("latin-1"))
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 9}})
        if method == "getChat":
            return httpx.Response(200, json={"ok": True, "result": {"title": "المشتركون"}})
        if method == "getMe":
            return httpx.Response(200, json={"ok": True, "result": {"id": 555}})
        if method == "getChatMember":
            return httpx.Response(
                200,
                json={"ok": True, "result": {"status": "administrator", "can_post_messages": True}},
            )
        return httpx.Response(200, json={"ok": True, "result": {}})

    calls: list[tuple[str, dict]] = []
    engine = _engine(tmp_path, calls)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        # exactly how production wires it: the Telegram notifier is WRAPPED in
        # a fanout beside the console one. A diagnostic that isinstance-checks
        # engine.notifier directly is a no-op on the only deployment that
        # matters — which is what this test exists to prevent.
        engine.notifier = FanoutNotifier(
            NullNotifier(), TelegramNotifier("token", "admin", client=client)
        )
        await engine._handle_command("فحص")

    assert len(posted) == 1
    assert PRIVATE in posted[0]  # the card went to the channel, not to the DM


@pytest.mark.asyncio
async def test_an_unset_private_channel_is_named_as_the_cause_at_startup(tmp_path):
    """"Why do the cards come to me instead of the channel?" has one overwhelmingly
    likely answer, and the engine must say it out loud rather than leave the
    operator to guess between four indistinguishable causes."""
    from qqq_alpha.brain.decider import HeuristicDecider
    from qqq_alpha.config import Settings as S

    settings = S(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        telegram_bot_token="token",
        telegram_chat_id="admin",
        telegram_private_channel_id="",  # the misconfiguration under test
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
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"ok": True, "result": {}}))
    ) as client:
        notes = NullNotifier()
        engine.notifier = FanoutNotifier(notes, TelegramNotifier("token", "admin", client=client))
        await engine._report_channel_health()

    report = "\n".join(notes.notes)
    assert "TELEGRAM_PRIVATE_CHANNEL_ID فارغ" in report
    assert "تقارير مِرصاد ٩" in report
    # the public channel being unset is a DIFFERENT fact, said differently
    assert "النشر العام معطّل" in report


@pytest.mark.asyncio
async def test_a_missing_minus_sign_is_named_as_the_typo_it_is():
    """Telegram answers a positive id with a bare "chat not found", which says
    nothing about why. The most common cause is a dropped minus sign."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("getChat"):
            return httpx.Response(
                400, json={"ok": False, "description": "Bad Request: chat not found"}
            )
        return httpx.Response(200, json={"ok": True, "result": {}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        notifier = TelegramNotifier("token", "admin", client=client)

        no_minus = await notifier.check_channel("1001234567890")
        assert "الشرطة ناقصة" in no_minus

        wrong_prefix = await notifier.check_channel("-1234567890")
        assert "يجب أن يبدأ بـ -100" in wrong_prefix

        # a well-formed id that simply is not reachable keeps the plain wording
        well_formed = await notifier.check_channel("-1001234567890")
        assert "الشرطة ناقصة" not in well_formed
        assert "القناة غير متاحة للبوت" in well_formed
