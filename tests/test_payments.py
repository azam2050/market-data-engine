"""The money door: forged links buy nothing, replayed webhooks pay once.

The subscriber's browser sets the form's amount and metadata, so the
webhook's own story is never enough — every activation rides on a
server-side re-fetch of the payment. These tests hold that line.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from qqq_alpha import payments as pay_gateway
from qqq_alpha.config import Settings
from qqq_alpha.dashboard.app import create_app
from qqq_alpha.memory import Memory


def _settings(tmp_path, **overrides) -> Settings:
    base = dict(
        data_dir=tmp_path,
        telegram_bot_token="123:secret-token",
        moyasar_publishable_key="pk_test_x",
        moyasar_secret_key="sk_test_x",
        public_base_url="https://example.up.railway.app",
        price_indicator_sar=149,
        price_channel_sar=249,
        price_vip_sar=299,
        subscription_days=30,
    )
    base.update(overrides)
    return Settings(**base)


def _paid_payment(
    settings: Settings, chat_id: str = "555", plan: str = "vip", **overrides
) -> dict:
    payment = {
        "id": "pay_1",
        "status": "paid",
        "amount": pay_gateway.expected_amount_halalas(settings, plan),
        "currency": "SAR",
        "metadata": {
            "product": pay_gateway.PRODUCT_TAG,
            "plan": plan,
            "telegram_id": chat_id,
            "sig": pay_gateway.sign_chat(settings, chat_id),
        },
    }
    payment.update(overrides)
    return payment


# ---------------------------------------------------------------- signatures
def test_pay_link_is_signed_and_verifiable(tmp_path):
    settings = _settings(tmp_path)
    link = pay_gateway.pay_link(settings, "555", "channel")
    assert link and link.startswith("https://example.up.railway.app/pay?u=555&t=")
    assert link.endswith("&p=channel")
    token = link.rsplit("t=", 1)[1].split("&", 1)[0]
    assert pay_gateway.verify_chat_signature(settings, "555", token)
    assert not pay_gateway.verify_chat_signature(settings, "666", token)  # not transferable


def test_no_links_while_payments_are_dark(tmp_path):
    settings = _settings(tmp_path, moyasar_secret_key="")
    assert pay_gateway.pay_link(settings, "555") is None


# ------------------------------------------------------------- verification
def test_payment_problems_names_every_defect(tmp_path):
    settings = _settings(tmp_path)
    assert pay_gateway.payment_problems(settings, _paid_payment(settings)) == []

    tampered = _paid_payment(settings, amount=100)  # paid 1 SAR for a plan
    assert any("المبلغ" in p for p in pay_gateway.payment_problems(settings, tampered))

    unpaid = _paid_payment(settings, status="failed")
    assert any("الحالة" in p for p in pay_gateway.payment_problems(settings, unpaid))

    forged = _paid_payment(settings)
    forged["metadata"]["sig"] = "0" * 20
    assert any("توقيع" in p for p in pay_gateway.payment_problems(settings, forged))


def test_cheap_plan_price_cannot_buy_the_vip_bundle(tmp_path):
    """The browser sets the form's amount, so a fiddled page could pay the
    indicator's 149 while claiming plan=vip — the server must price the
    CLAIMED plan and refuse the mismatch."""
    settings = _settings(tmp_path)
    sneaky = _paid_payment(settings, amount=14900)  # indicator money, vip claim
    assert any("المبلغ" in p for p in pay_gateway.payment_problems(settings, sneaky))

    unknown = _paid_payment(settings)
    unknown["metadata"]["plan"] = "lifetime"
    assert any("باقة" in p for p in pay_gateway.payment_problems(settings, unknown))

    # each real plan at its own price is clean
    for plan in ("indicator", "channel", "vip"):
        assert pay_gateway.payment_problems(settings, _paid_payment(settings, plan=plan)) == []


# ------------------------------------------------------------------ webhook
def _client_and_log(tmp_path, settings, payment):
    """A dashboard app whose Moyasar re-fetch returns ``payment``."""
    events: list[tuple[str, str, dict]] = []

    async def fake_fetch(_settings, _payment_id, client=None):
        return payment

    async def on_payment(action, chat_id, info):
        events.append((action, chat_id, info))

    pay_gateway_fetch = pay_gateway.fetch_payment
    pay_gateway.fetch_payment = fake_fetch
    app = create_app(settings, on_payment=on_payment)
    return TestClient(app), events, pay_gateway_fetch


def _webhook_body(payment):
    return {"type": "payment_paid", "data": payment}


def test_verified_payment_activates_exactly_once(tmp_path):
    settings = _settings(tmp_path)
    payment = _paid_payment(settings)
    client, events, restore = _client_and_log(tmp_path, settings, payment)
    try:
        memory = Memory(settings.data_dir / "memory.db")
        now = datetime.now(UTC)
        memory.add_subscriber("555", "u", "U", now, now + timedelta(days=2))

        first = client.post("/moyasar/webhook", json=_webhook_body(payment))
        assert first.json().get("activated") is True
        replay = client.post("/moyasar/webhook", json=_webhook_body(payment))
        assert replay.json().get("duplicate") is True

        row = memory.subscriber("555")
        expires = datetime.fromisoformat(row["expires_at"])
        # one payment: ~32 days out (2 remaining + 30 bought), never 62
        assert timedelta(days=31) < expires - now < timedelta(days=33)
        assert [e[0] for e in events] == ["activated"]
        booked = memory.payments_for("555")[0]
        assert booked["amount"] == 29900 and booked["plan"] == "vip"
        # the purchased plan lands on the subscriber and rides the event
        assert row["plan"] == "vip"
        assert events[0][2]["plan"] == "vip"
    finally:
        pay_gateway.fetch_payment = restore


def test_tampered_amount_never_activates(tmp_path):
    settings = _settings(tmp_path)
    payment = _paid_payment(settings, amount=100)
    client, events, restore = _client_and_log(tmp_path, settings, payment)
    try:
        response = client.post("/moyasar/webhook", json=_webhook_body(payment))
        assert response.json().get("activated") is False
        assert [e[0] for e in events] == ["rejected"]
        assert Memory(settings.data_dir / "memory.db").subscriber("555") is None
    finally:
        pay_gateway.fetch_payment = restore


def test_other_apps_payments_are_ignored_silently(tmp_path):
    settings = _settings(tmp_path)
    foreign = _paid_payment(settings)
    foreign["metadata"] = {"invoice": "salla-123"}  # the shared account's other app
    client, events, restore = _client_and_log(tmp_path, settings, foreign)
    try:
        response = client.post("/moyasar/webhook", json=_webhook_body(foreign))
        assert response.json().get("ignored") == "other product"
        assert events == []
    finally:
        pay_gateway.fetch_payment = restore


def test_webhook_secret_gate_when_configured(tmp_path):
    settings = _settings(tmp_path, moyasar_webhook_secret="whsec")
    payment = _paid_payment(settings)
    client, events, restore = _client_and_log(tmp_path, settings, payment)
    try:
        body = _webhook_body(payment)
        assert client.post("/moyasar/webhook", json=body).status_code == 403
        body["secret_token"] = "whsec"
        assert client.post("/moyasar/webhook", json=body).json().get("activated") is True
    finally:
        pay_gateway.fetch_payment = restore


def test_payment_without_prior_start_registers_on_the_spot(tmp_path):
    settings = _settings(tmp_path)
    payment = _paid_payment(settings)
    client, events, restore = _client_and_log(tmp_path, settings, payment)
    try:
        client.post("/moyasar/webhook", json=_webhook_body(payment))
        row = Memory(settings.data_dir / "memory.db").subscriber("555")
        assert row is not None and row["status"] == "trial"
        expires = datetime.fromisoformat(row["expires_at"])
        assert expires - datetime.now(UTC) > timedelta(days=29)
    finally:
        pay_gateway.fetch_payment = restore


# ----------------------------------------------------------------- the page
def test_pay_page_renders_the_channel_brand_for_a_signed_link(tmp_path):
    settings = _settings(tmp_path)
    app = create_app(settings)
    client = TestClient(app)
    token = pay_gateway.sign_chat(settings, "555")

    page = client.get(f"/pay?u=555&t={token}")
    assert page.status_code == 200
    assert settings.brand_name in page.text
    # no plan in the link falls back to the VIP bundle
    assert "29900" in page.text  # halalas handed to the form
    assert '"vip"' in page.text  # the plan rides the form metadata
    assert settings.statement_name in page.text  # the bank-statement warning

    indicator = client.get(f"/pay?u=555&t={token}&p=indicator")
    assert "14900" in indicator.text and '"indicator"' in indicator.text
    channel = client.get(f"/pay?u=555&t={token}&p=channel")
    assert "24900" in channel.text and '"channel"' in channel.text

    forged = client.get("/pay?u=555&t=deadbeef")
    assert "غير صالح" in forged.text


def test_pay_page_while_dark_says_so(tmp_path):
    settings = _settings(tmp_path, moyasar_publishable_key="")
    client = TestClient(create_app(settings))
    page = client.get("/pay?u=555&t=x")
    assert "غير مفعّل" in page.text


# ------------------------------------------------------------------ Apple Pay
def test_apple_pay_domain_association_is_served_at_the_well_known_path(tmp_path):
    """Apple fetches this exact path unauthenticated before it will show the
    button on /pay — a 404 here is why Apple Pay opens then closes itself."""
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))
    response = client.get("/.well-known/apple-developer-merchantid-domain-association")
    assert response.status_code == 200
    # the issued file is hex-encoded JSON and must be served VERBATIM in that
    # hex form — that's how every merchant hosts it, and Moyasar's validator
    # compares byte-for-byte. Decoding it to JSON (as we once did) fails
    # verification with "must show the verification text file".
    import json

    decoded = bytes.fromhex(response.text.strip())
    payload = json.loads(decoded)  # must be well-formed, not truncated
    assert "pspId" in payload
    assert "signature" in payload

    # domain validators commonly probe with HEAD before the real GET —
    # a bare @app.get 405s HEAD in FastAPI, which reads as "not verified"
    head = client.head("/.well-known/apple-developer-merchantid-domain-association")
    assert head.status_code == 200

    # Apple's original spec named the file with .txt, and Moyasar's validator
    # fails a domain with "must show the verification text file" when only
    # the bare path answers — so both spellings must serve the same content
    txt = client.get("/.well-known/apple-developer-merchantid-domain-association.txt")
    assert txt.status_code == 200
    assert txt.text == response.text
    assert (
        client.head("/.well-known/apple-developer-merchantid-domain-association.txt").status_code
        == 200
    )


# ---------------------------------------------------------------- reminders
def test_reminder_sweep_targets_only_the_two_day_window_once(tmp_path):
    settings = _settings(tmp_path)
    memory = Memory(settings.data_dir / "memory.db")
    now = datetime.now(UTC)
    memory.add_subscriber("soon", "a", "A", now, now + timedelta(days=1))
    memory.add_subscriber("later", "b", "B", now, now + timedelta(days=10))
    memory.add_subscriber("gone", "c", "C", now - timedelta(days=40), now - timedelta(days=1))

    due = memory.trials_needing_reminder(now)
    assert [row["chat_id"] for row in due] == ["soon"]

    memory.mark_reminded("soon", now)
    assert memory.trials_needing_reminder(now) == []

    # a paid extension re-arms the reminder for the next window
    memory.clear_reminder("soon")
    assert [row["chat_id"] for row in memory.trials_needing_reminder(now)] == ["soon"]


@pytest.mark.asyncio
async def test_paid_activation_message_carries_expiry_and_door_key(tmp_path):
    """The engine's _on_payment: subscriber DM with expiry + single-use link,
    operator note with the amount."""
    from test_live import _FakeCommands, _subscriber_engine

    engine_settings = Settings(
        massive_api_key="test-key",
        anthropic_api_key="test",
        anthropic_model="test",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        massive_feed_mode="delayed",
    )
    engine = _subscriber_engine(engine_settings, tmp_path)
    assert isinstance(engine.commands, _FakeCommands)
    await engine._on_payment(
        "activated",
        "555",
        {"payment_id": "pay_9", "row": {"expires_at": "2026-09-22T20:00:00+00:00"}, "amount": 19900},
    )
    sent = "\n".join(text for _, text in engine.commands.sent)
    assert "2026-09-22" in sent and "t.me/+personal" in sent
    assert any("199" in note and "💳" in note for note in engine.notifier.notes)


@pytest.mark.asyncio
async def test_subscriber_can_ask_for_their_pay_link(tmp_path):
    """«اشتراك» from a registered chat returns the signed personal link —
    or an honest "still dark" line when the keys are not in place yet."""
    from test_live import _subscriber_engine

    engine_settings = Settings(
        massive_api_key="test-key",
        anthropic_api_key="test",
        anthropic_model="test",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        massive_feed_mode="delayed",
        moyasar_publishable_key="pk",
        moyasar_secret_key="sk",
        public_base_url="https://example.up.railway.app",
    )
    engine = _subscriber_engine(engine_settings, tmp_path)
    now = datetime.now(UTC)
    engine.memory.add_subscriber("555", "u", "U", now, now + timedelta(days=5))

    from qqq_alpha.live.telegram import InboundMessage

    await engine._handle_subscriber(InboundMessage("555", "اشتراك"))
    assert engine.commands.buttoned, "the offer must carry the plan buttons"
    _, text, buttons = engine.commands.buttoned[-1]
    # the three prices come from settings, so a repricing does not need a
    # test edit — what matters is that each plan quotes its own number
    for price in (
        engine_settings.price_indicator_sar,
        engine_settings.price_channel_sar,
        engine_settings.price_vip_sar,
    ):
        assert str(price) in text
    assert "ريال" in text
    # three plans, three signed personal links, each naming its plan
    plan_urls = [value for _label, value in buttons if "/pay?u=555&t=" in value]
    assert len(plan_urls) == 3
    assert {u.rsplit("&p=", 1)[1] for u in plan_urls} == {"indicator", "channel", "vip"}

    # a stranger asking gets silence — no probing the bot for links
    engine.commands.buttoned.clear()
    await engine._handle_subscriber(InboundMessage("999", "اشتراك"))
    assert engine.commands.buttoned == []

# --------------------------------------------------- the TradingView roster
def test_indicator_roster_splits_grant_and_revoke(tmp_path):
    """Trials and indicator/vip payers belong on the grant list; expired
    windows and channel-only payers must show up as revokes — TradingView
    access is a manual click, so this list IS the enforcement."""
    settings = _settings(tmp_path)
    memory = Memory(settings.data_dir / "memory.db")
    now = datetime.now(UTC)
    live = now + timedelta(days=5)

    memory.add_subscriber("1", "trial_guy", "T", now, live)
    memory.set_tv_username("1", "@Trial_Guy")  # the @ people paste gets stripped
    memory.add_subscriber("2", "vip_guy", "V", now, live)
    memory.extend_subscriber("2", 30, now, plan="vip")
    memory.set_tv_username("2", "Vip_Guy")
    memory.add_subscriber("3", "channel_only", "C", now, live)
    memory.extend_subscriber("3", 30, now, plan="channel")
    memory.set_tv_username("3", "Channel_Only")
    memory.add_subscriber("4", "expired_guy", "E", now - timedelta(days=9), now - timedelta(days=1))
    memory.set_tv_username("4", "Expired_Guy")
    memory.expire_due_subscribers(now)
    memory.add_subscriber("5", "no_tv", "N", now, live)  # never registered a name

    roster = memory.indicator_roster(now)
    assert {r["tv_username"] for r in roster["grant"]} == {"Trial_Guy", "Vip_Guy"}
    assert {r["tv_username"] for r in roster["revoke"]} == {"Channel_Only", "Expired_Guy"}


def test_paid_plan_replaces_the_stored_plan(tmp_path):
    settings = _settings(tmp_path)
    memory = Memory(settings.data_dir / "memory.db")
    now = datetime.now(UTC)
    memory.add_subscriber("9", "u", "U", now, now + timedelta(days=3))
    assert memory.subscriber("9")["plan"] is None  # a trial covers everything

    memory.extend_subscriber("9", 30, now, plan="indicator")
    assert memory.subscriber("9")["plan"] == "indicator"
    # a later upgrade wins; an extension without a plan changes nothing
    memory.extend_subscriber("9", 30, now, plan="vip")
    memory.extend_subscriber("9", 5, now)
    assert memory.subscriber("9")["plan"] == "vip"

@pytest.mark.asyncio
async def test_indicator_only_activation_drops_channel_and_asks_for_tv_name(tmp_path):
    """Paying for the indicator alone ends the trial's channel access and
    asks for the TradingView username the operator will grant."""
    from test_live import _subscriber_engine

    engine_settings = Settings(
        massive_api_key="test-key",
        anthropic_api_key="test",
        anthropic_model="test",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        massive_feed_mode="delayed",
    )
    engine = _subscriber_engine(engine_settings, tmp_path)
    kicked: list[tuple[str, str]] = []

    async def record_kick(channel_id, user_id):
        kicked.append((channel_id, user_id))
        return True

    engine.commands.kick = record_kick
    await engine._on_payment(
        "activated",
        "555",
        {
            "payment_id": "pay_9",
            "row": {"expires_at": "2026-09-22T20:00:00+00:00"},
            "amount": 14900,
            "plan": "indicator",
        },
    )
    sent = "\n".join(text for _, text in engine.commands.sent)
    assert "t.me/+personal" not in sent, "no channel door for an indicator-only plan"
    assert kicked == [("-100999", "555")]
    assert "مؤشر" in sent  # the TradingView-username prompt went out


@pytest.mark.asyncio
async def test_tv_username_capture_confirms_and_orders_the_grant(tmp_path):
    from test_live import _subscriber_engine

    engine_settings = Settings(
        massive_api_key="test-key",
        anthropic_api_key="test",
        anthropic_model="test",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        massive_feed_mode="delayed",
    )
    engine = _subscriber_engine(engine_settings, tmp_path)
    now = datetime.now(UTC)
    engine.memory.add_subscriber("555", "u", "U", now, now + timedelta(days=5))

    from qqq_alpha.live.telegram import InboundMessage

    await engine._handle_subscriber(InboundMessage("555", "مؤشر @Ahmed_Trader"))
    assert engine.memory.subscriber("555")["tv_username"] == "Ahmed_Trader"
    sent = "\n".join(text for _, text in engine.commands.sent)
    assert "Ahmed_Trader" in sent  # the subscriber's confirmation
    assert any("امنح" in note and "Ahmed_Trader" in note for note in engine.notifier.notes)

    # a malformed name gets the format explained, not a silent status reply
    await engine._handle_subscriber(InboundMessage("555", "مؤشر"))
    assert "Ahmed_Trader" in engine.memory.subscriber("555")["tv_username"]
    assert "مثال" in engine.commands.sent[-1][1]


# ------------------------------------------------------------- walkthrough
def test_mirsad_walkthrough_is_public_and_served_from_our_domain(tmp_path):
    """The bot sends every new customer this link, so it must open without a
    login and live on the product's own domain rather than a third-party host."""
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))
    page = client.get("/mirsad")
    assert page.status_code == 200
    assert page.headers["content-type"].startswith("text/html")
    assert page.text.startswith("<!doctype html>")
    assert 'lang="ar" dir="rtl"' in page.text
    assert "مِرصاد ٩" in page.text
    assert client.head("/mirsad").status_code == 200
