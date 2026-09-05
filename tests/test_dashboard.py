"""Tests for the admin dashboard.

The dashboard is read-only over the same journal and memory the engine
already writes, with one write path — approving or rejecting a lesson — so
the tests cover: authentication is enforced, every page renders against both
empty and populated data, and the one write path actually mutates the
playbook and memory it claims to.
"""

from datetime import UTC, date, datetime, timedelta

from fastapi.testclient import TestClient

from qqq_alpha.config import Settings
from qqq_alpha.dashboard.app import create_app
from qqq_alpha.dashboard.behavior import classify_entry, classify_exit
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, MissedOpportunity, OptionType, Target
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal
from qqq_alpha.memory import Memory
from qqq_alpha.trades import TradeManager

DAY = date(2026, 3, 2)


def _settings(tmp_path) -> Settings:
    settings = Settings(
        admin_username="admin",
        admin_password="secret",
        data_dir=tmp_path / "data",
        journal_dir=tmp_path / "journal",
        playbook_path=tmp_path / "playbook.yaml",
    )
    settings.ensure_dirs()
    return settings


def _snapshot():
    bars = synthetic_session("QQQ", DAY, seed=15)
    return SnapshotBuilder("QQQ").build(bars[:80])


AUTH = ("admin", "secret")


# ---------------------------------------------------------------- auth
def test_dashboard_requires_authentication(tmp_path):
    client = TestClient(create_app(_settings(tmp_path)))
    assert client.get("/").status_code == 401


def test_dashboard_rejects_wrong_credentials(tmp_path):
    client = TestClient(create_app(_settings(tmp_path)))
    assert client.get("/", auth=("admin", "wrong")).status_code == 401


def test_dashboard_accepts_correct_credentials(tmp_path):
    client = TestClient(create_app(_settings(tmp_path)))
    assert client.get("/", auth=AUTH).status_code == 200


# ---------------------------------------------------------------- pages, empty state
def test_every_page_renders_with_no_data_on_record(tmp_path):
    client = TestClient(create_app(_settings(tmp_path)))
    for path in (
        "/", "/trades", "/orders", "/decisions", "/missed",
        "/lessons", "/reports", "/report-card",
    ):
        response = client.get(path, auth=AUTH)
        assert response.status_code == 200, (path, response.text[:500])


# ---------------------------------------------------------------- pages, populated
def _seed(settings: Settings) -> int:
    """Write one closed trade, one PASS decision, one missed opportunity, one
    pending lesson — enough for every page to have something real to render."""
    journal = Journal(settings.journal_dir, session_tag="test")
    memory = Memory(settings.data_dir / "memory.db")
    snap = _snapshot()

    decision = Decision(
        ts=snap.ts,
        action=Action.ENTER,
        direction=OptionType.CALL,
        occ_symbol="O:QQQ260302C00485000",
        targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40,
        confidence=7,
        thesis="كسر النطاق مع حجم قوي",
        risks=["RSI ممتد"],
        invalidation="فقدان 484",
        overrides=["دخل قبل نافذة OR_BREAK لأن الكسر كان حاسمًا"],
    )
    trade = TradeManager().open_trade(decision, 1.00, snap)
    trade.closed_at = trade.opened_at + timedelta(minutes=19)
    trade.exit_price, trade.return_pct, trade.max_favorable_pct = 1.25, 25.0, 69.0
    trade.exit_reason = "trail_stop"
    journal.log_trade(trade)
    memory.remember_trade(trade, snap)
    journal.log_decision(decision, snap, [], [], 0.8)

    pass_decision = Decision(ts=snap.ts, action=Action.PASS, confidence=3, thesis="لا يوجد دليل كافٍ")
    journal.log_decision(pass_decision, snap, [], [], 0.3)

    missed = MissedOpportunity(
        ts=snap.ts,
        reason="brain declined",
        would_be_direction=OptionType.CALL,
        occ_symbol="O:QQQ260302C00487000",
        hypothetical_entry=1.0,
        best_price_after=1.9,
        peak_return_pct=90.0,
        regime="TRENDING_UP",
        session_minute=30,
    )
    journal.log_missed(missed)
    memory.remember_missed(missed)

    return memory.save_lesson("النظام الصاعد يعطي فرص أكثر", "9 صفقات، متوسط +40 نقطة", 9, 0.6)


def test_pages_render_populated_data(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    client = TestClient(create_app(settings))

    assert "trail_stop" in client.get("/trades", auth=AUTH).text
    decisions_page = client.get("/decisions", auth=AUTH).text
    assert "كسر النطاق مع حجم قوي" in decisions_page
    assert "لا يوجد دليل كافٍ" in decisions_page  # a plain PASS is shown too
    assert "TRENDING_UP" in client.get("/missed", auth=AUTH).text
    assert "النظام الصاعد يعطي فرص أكثر" in client.get("/lessons", auth=AUTH).text

    # the report card buckets the closed trade by regime, hour, confidence, exit
    card = client.get("/report-card", auth=AUTH).text
    assert "حسب نظام السوق" in card
    assert "7/10" in card
    assert "trail_stop" in card


def test_overview_shows_pending_lesson_count(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    client = TestClient(create_app(settings))

    response = client.get("/", auth=AUTH)
    assert "1" in response.text  # one pending lesson


# ---------------------------------------------------------------- lesson approval
def test_approving_a_lesson_keeps_it_durably_and_clears_it_from_pending(tmp_path):
    settings = _settings(tmp_path)
    lesson_id = _seed(settings)
    applied: list = []
    client = TestClient(
        create_app(settings, on_lesson_applied=lambda book: applied.append(book))
    )

    response = client.post(f"/lessons/{lesson_id}/approve", auth=AUTH, follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/lessons"
    assert applied and applied[0].version == 2
    memory = Memory(settings.data_dir / "memory.db")
    assert not memory.pending_lessons()
    # durability is the database, not the (ephemeral) playbook file: the
    # lessons page must still show the approval with no file on disk
    assert not settings.playbook_path.exists()
    assert "دروس" in client.get("/lessons", auth=AUTH).text
    assert memory.applied_lessons()


def test_rejecting_a_lesson_clears_it_without_writing_the_playbook(tmp_path):
    settings = _settings(tmp_path)
    lesson_id = _seed(settings)
    client = TestClient(create_app(settings))

    client.post(f"/lessons/{lesson_id}/reject", auth=AUTH, follow_redirects=False)

    assert not settings.playbook_path.exists()
    assert not Memory(settings.data_dir / "memory.db").pending_lessons()


def test_approving_a_nonexistent_lesson_does_not_crash(tmp_path):
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))

    response = client.post("/lessons/9999/approve", auth=AUTH, follow_redirects=False)
    assert response.status_code == 303


# ---------------------------------------------------------------- reports
def test_reports_page_navigates_by_day(tmp_path):
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))

    response = client.get(f"/reports?day={DAY.isoformat()}", auth=AUTH)
    assert response.status_code == 200
    assert (DAY - timedelta(days=1)).isoformat() in response.text
    assert (DAY + timedelta(days=1)).isoformat() in response.text


# ---------------------------------------------------------------- behavior classifier
def test_classify_exit_flags_a_fast_stop_out():
    trade = {"exit_reason": "stop_hit", "hold_minutes": 3, "max_favorable_pct": 5, "return_pct": -40}
    assert "سريع" in classify_exit(trade)


def test_classify_exit_flags_a_big_giveback_from_the_peak():
    trade = {"exit_reason": "trail_stop", "return_pct": 25, "max_favorable_pct": 69, "hold_minutes": 19}
    assert "تراجع" in classify_exit(trade)


def test_classify_exit_praises_a_runner_that_was_allowed_to_run():
    trade = {"exit_reason": "target", "return_pct": 150, "max_favorable_pct": 160, "hold_minutes": 40}
    assert "قوية" in classify_exit(trade)


def test_classify_exit_default_case_is_not_alarming():
    trade = {"exit_reason": "target", "return_pct": 50, "max_favorable_pct": 55, "hold_minutes": 25}
    result = classify_exit(trade)
    assert "طبيعية" in result


def test_classify_entry_flags_a_playbook_override():
    trade = {"decision": {"overrides": ["دخل قبل النافذة المحددة"]}}
    note = classify_entry(trade)
    assert note is not None and "جريء" in note


def test_classify_entry_is_silent_without_an_override():
    assert classify_entry({"decision": {"overrides": []}}) is None


def test_health_is_open_and_leaks_nothing(tmp_path):
    """The one unauthenticated route: an external uptime monitor must be able
    to poll it, because a process that has been killed cannot report that it
    was killed. It carries liveness only — no trades, no keys, no subscribers."""
    from datetime import UTC, datetime, timedelta

    from qqq_alpha.live.engine import LiveStatus

    status = LiveStatus()
    status.started_at = datetime.now(UTC) - timedelta(hours=2)
    status.last_bar_at = datetime.now(UTC) - timedelta(seconds=90)
    client = TestClient(create_app(_settings(tmp_path), status=status))

    response = client.get("/health")  # deliberately no auth
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert 80 <= body["last_bar_age_sec"] <= 100
    assert set(body) == {
        "ok", "started_at", "last_bar_at", "last_bar_age_sec", "trades_today", "reconnects"
    }


# ---------------------------------------------------------------- subscribers
def _with_subscribers(tmp_path):
    """Two sign-ups: one live trial, one that lapsed a week ago."""
    settings = _settings(tmp_path)
    memory = Memory(settings.data_dir / "memory.db")
    now = datetime.now(UTC)
    memory.add_subscriber("111", "abu_layth", "Layth", now - timedelta(days=3), now + timedelta(days=27))
    memory.add_subscriber("222", "saud", "Saud", now - timedelta(days=40), now - timedelta(days=7))
    return settings, memory


def test_subscribers_page_lists_lapsed_sign_ups_too(tmp_path):
    """The operator counted two friends and the overview said one. The roster
    has to show the second and say why it is not counted, or the number just
    looks broken."""
    settings, _ = _with_subscribers(tmp_path)
    body = TestClient(create_app(settings)).get("/subscribers", auth=AUTH).text

    assert "Layth" in body
    assert "Saud" in body  # lapsed, but still on the roster
    assert "منتهي" in body


def test_subscribers_page_renders_when_nobody_has_signed_up(tmp_path):
    body = TestClient(create_app(_settings(tmp_path))).get("/subscribers", auth=AUTH).text
    assert "لا يوجد مشتركون" in body


def test_extending_a_live_trial_adds_to_what_is_left(tmp_path):
    settings, memory = _with_subscribers(tmp_path)
    before = datetime.fromisoformat(memory.subscriber("111")["expires_at"])

    client = TestClient(create_app(settings))
    response = client.post("/subscribers/111/extend", data={"days": 10}, auth=AUTH)

    assert response.status_code == 200  # followed the redirect
    after = datetime.fromisoformat(memory.subscriber("111")["expires_at"])
    assert (after - before).days == 10


def test_extending_a_lapsed_trial_starts_a_fresh_window(tmp_path):
    """Adding 10 days to an expiry a week in the past would grant three days."""
    settings, memory = _with_subscribers(tmp_path)

    TestClient(create_app(settings)).post(
        "/subscribers/222/extend", data={"days": 10}, auth=AUTH
    )

    row = memory.subscriber("222")
    left = (datetime.fromisoformat(row["expires_at"]) - datetime.now(UTC)).days
    assert 9 <= left <= 10
    assert row["status"] == "trial"  # revived, not left marked expired


def test_a_slipped_keystroke_cannot_grant_a_decade(tmp_path):
    """The cap bounds a single grant, not the running total — topping up a
    live trial is meant to add to what is left."""
    settings, memory = _with_subscribers(tmp_path)
    before = datetime.fromisoformat(memory.subscriber("111")["expires_at"])

    TestClient(create_app(settings)).post(
        "/subscribers/111/extend", data={"days": 99999}, auth=AUTH
    )

    after = datetime.fromisoformat(memory.subscriber("111")["expires_at"])
    assert (after - before).days == 365


def test_a_nonsense_days_value_falls_back_instead_of_erroring(tmp_path):
    settings, memory = _with_subscribers(tmp_path)
    before = datetime.fromisoformat(memory.subscriber("111")["expires_at"])

    for bad in ({"days": "abc"}, {"days": ""}, {"days": -5}, {}):
        response = TestClient(create_app(settings)).post(
            "/subscribers/111/extend", data=bad, auth=AUTH
        )
        assert response.status_code == 200  # no 422, no 500

    # unreadable input falls back to 30 days; a negative one clamps to 1. The
    # invariant that matters is that nothing here can ever SHORTEN a trial.
    after = datetime.fromisoformat(memory.subscriber("111")["expires_at"])
    assert (after - before).days == 30 + 30 + 1 + 30


def test_removing_a_subscriber_deletes_the_row(tmp_path):
    settings, memory = _with_subscribers(tmp_path)

    TestClient(create_app(settings)).post("/subscribers/111/remove", auth=AUTH)

    assert memory.subscriber("111") is None
    assert memory.subscriber("222") is not None  # only the one named


def test_removal_also_closes_the_channel_door(tmp_path):
    """Deleting the row while leaving them in the private channel would be
    worse than not deleting: they keep every signal, and nothing records it."""
    settings, _ = _with_subscribers(tmp_path)
    calls = []

    async def _hook(action, row, days):
        calls.append((action, row["chat_id"], days))

    client = TestClient(create_app(settings, on_subscriber_change=_hook))
    client.post("/subscribers/111/remove", auth=AUTH)
    client.post("/subscribers/222/extend", data={"days": 5}, auth=AUTH)

    assert calls == [("removed", "111", None), ("extended", "222", 5)]


def test_subscriber_edits_require_authentication(tmp_path):
    settings, memory = _with_subscribers(tmp_path)
    client = TestClient(create_app(settings))

    assert client.get("/subscribers").status_code == 401
    assert client.post("/subscribers/111/remove").status_code == 401
    assert memory.subscriber("111") is not None


def test_editing_an_unknown_subscriber_is_a_no_op_not_a_crash(tmp_path):
    settings, _ = _with_subscribers(tmp_path)
    calls = []

    async def _hook(action, row, days):
        calls.append(action)

    client = TestClient(create_app(settings, on_subscriber_change=_hook))
    assert client.post("/subscribers/999/remove", auth=AUTH).status_code == 200
    assert client.post("/subscribers/999/extend", data={"days": 5}, auth=AUTH).status_code == 200
    assert calls == []  # nothing to announce about somebody who does not exist


def test_the_page_no_longer_grades_channel_membership(tmp_path):
    """The channel is free for everyone now, so the roster stops comparing
    itself to the member count: the probe may still run, but nothing on
    the page says who is inside."""
    settings, _ = _with_subscribers(tmp_path)

    async def _roster(chat_ids):
        return {"channel_total": 4, "inside": {"111": True, "222": False}}

    body = TestClient(create_app(settings, channel_roster=_roster)).get(
        "/subscribers", auth=AUTH
    ).text

    assert "داخل القناة الخاصة فعليًا" not in body
    assert "لا يتيح للبوت سرد أعضاء القناة" not in body


def test_no_gap_warning_when_the_roster_matches_the_channel(tmp_path):
    settings, _ = _with_subscribers(tmp_path)

    async def _roster(chat_ids):
        # two subscribers plus the bot itself
        return {"channel_total": 3, "inside": {"111": True, "222": True}}

    body = TestClient(create_app(settings, channel_roster=_roster)).get(
        "/subscribers", auth=AUTH
    ).text

    assert "لا يتيح للبوت سرد أعضاء القناة" not in body


def test_a_telegram_hiccup_does_not_blank_the_roster(tmp_path):
    """The page's job is the roster; the channel probe is a bonus on top of it."""
    settings, _ = _with_subscribers(tmp_path)

    async def _roster(chat_ids):
        raise RuntimeError("telegram is down")

    response = TestClient(create_app(settings, channel_roster=_roster)).get(
        "/subscribers", auth=AUTH
    )

    assert response.status_code == 200
    assert "Layth" in response.text


# ---------------------------------------------------------------- missed: name the rail
def _seed_missed(settings: Settings, blocked_by: list[str], peak: float = 120.0) -> None:
    journal = Journal(settings.journal_dir, session_tag="test")
    journal.log_missed(
        MissedOpportunity(
            ts=_snapshot().ts,
            reason="blocked before the brain could act",
            would_be_direction=OptionType.PUT,
            occ_symbol="O:QQQ260302P00711000",
            hypothetical_entry=1.0,
            best_price_after=2.2,
            peak_return_pct=peak,
            blocked_by=blocked_by,
            regime="VOLATILE_CHOP",
            session_minute=200,
        )
    )


def test_missed_page_names_the_rail_that_refused(tmp_path):
    """"رفضته الحواجز" told the operator only what he already knew.

    Which rail refused is the whole measurement: it separates "the lock is
    protecting me" from "the lock is strangling me".
    """
    settings = _settings(tmp_path)
    _seed_missed(settings, ["position_cap: 1/1 open"])

    body = TestClient(create_app(settings)).get("/missed", auth=AUTH).text

    assert "صفقة أخرى كانت مفتوحة" in body
    assert "رفضته الحواجز" not in body
    # the raw code stays reachable as a tooltip, numbers included
    assert "position_cap: 1/1 open" in body


def test_missed_page_separates_the_declared_trigger_lock_from_the_caps(tmp_path):
    settings = _settings(tmp_path)
    _seed_missed(settings, ["declared_trigger_unmet: you said PUT arms at below 713.01"])
    _seed_missed(settings, ["daily_trade_cap: 3/3"], peak=90.0)

    body = TestClient(create_app(settings)).get("/missed", auth=AUTH).text

    assert "المستوى الذي أعلنه لم يتحقق بعد" in body
    assert "بلغ سقف الصفقات اليومي" in body


def test_missed_page_still_marks_the_brains_own_declines(tmp_path):
    settings = _settings(tmp_path)
    _seed_missed(settings, [])

    body = TestClient(create_app(settings)).get("/missed", auth=AUTH).text

    assert "رفضه الذكاء بنفسه" in body


def test_an_unknown_rail_code_is_shown_rather_than_swallowed(tmp_path):
    """A rail added later must not vanish from the page until someone
    remembers to translate it."""
    settings = _settings(tmp_path)
    _seed_missed(settings, ["some_future_rail: 3"])

    body = TestClient(create_app(settings)).get("/missed", auth=AUTH).text

    # more than once: the label itself, not merely the raw-code tooltip
    assert body.count("some_future_rail") >= 2


# ---------------------------------------------------------------- risks, letter by letter
def test_a_risk_note_stored_letter_by_letter_reads_as_a_sentence(tmp_path):
    """History written by the old parser is repaired on the way out.

    The model answered a list field with prose, ``list()`` split it into
    characters, and the page rendered ``ا · ل · س · ...``.
    """
    settings = _settings(tmp_path)
    snap = _snapshot()
    exploded = list("السيولة ضعيفة")
    Journal(settings.journal_dir, session_tag="test").log_decision(
        Decision(ts=snap.ts, action=Action.PASS, confidence=3, thesis="انتظار", risks=exploded),
        snap,
        [],
        [],
        0.3,
    )

    body = TestClient(create_app(settings)).get("/decisions", auth=AUTH).text

    assert "السيولة ضعيفة" in body
    assert "ا · ل · س" not in body


def test_a_genuine_list_of_risks_is_left_alone(tmp_path):
    settings = _settings(tmp_path)
    snap = _snapshot()
    Journal(settings.journal_dir, session_tag="test").log_decision(
        Decision(
            ts=snap.ts,
            action=Action.PASS,
            confidence=3,
            thesis="انتظار",
            risks=["السيولة ضعيفة", "خبر بعد ساعة"],
        ),
        snap,
        [],
        [],
        0.3,
    )

    body = TestClient(create_app(settings)).get("/decisions", auth=AUTH).text

    assert "السيولة ضعيفة · خبر بعد ساعة" in body


# ---------------------------------------------------------------- execution page
def _seed_orders(settings: Settings, rows: list[dict]) -> None:
    journal = Journal(settings.journal_dir, session_tag="test")
    for row in rows:
        journal.log_order(row)


def _order(**extra) -> dict:
    row = {
        "ts": "2026-03-02T14:31:00+00:00",
        "outcome": "execution_disabled",
        "armed": False,
        "broker": "paper",
        "client_order_id": "t1-entry",
        "occ_symbol": "O:QQQ260302C00485000",
        "side": "BUY",
        "quantity": 8,
        "limit_price": 1.25,
        "trade_id": "t1",
        "reason": "entry",
    }
    row.update(extra)
    return row


def test_the_execution_page_says_plainly_when_nothing_was_sent(tmp_path):
    """The one fact an operator must never have to infer."""
    settings = _settings(tmp_path)
    _seed_orders(settings, [_order()])

    body = TestClient(create_app(settings)).get("/orders", auth=AUTH).text

    assert "التنفيذ مُطفأ" in body
    assert "🔒 محجوز" in body
    assert "$1,000" in body  # 8 contracts × $1.25 × 100


def test_the_execution_page_measures_slippage_against_the_asked_price(tmp_path):
    """A buy filling above the limit is a cost; the sign has to say so."""
    settings = _settings(tmp_path)
    _seed_orders(
        settings,
        [
            _order(
                outcome="submitted",
                armed=True,
                order={"average_fill_price": 1.29, "state": "FILLED", "filled_quantity": 8},
            )
        ],
    )

    body = TestClient(create_app(settings)).get("/orders", auth=AUTH).text

    assert "التنفيذ مفعّل" in body
    assert "+3.20%" in body  # 1.29 vs 1.25


def test_a_sell_filling_below_the_ask_also_reads_as_a_cost(tmp_path):
    settings = _settings(tmp_path)
    _seed_orders(
        settings,
        [
            _order(
                side="SELL",
                reason="trail_stop",
                limit_price=2.00,
                outcome="submitted",
                armed=True,
                order={"average_fill_price": 1.94, "state": "FILLED", "filled_quantity": 8},
            )
        ],
    )

    body = TestClient(create_app(settings)).get("/orders", auth=AUTH).text

    assert "+3.00%" in body, "selling below the asked price is a cost, not a gain"


def test_an_order_of_unknown_fate_is_shown_as_unknown(tmp_path):
    """The process died between asking and being answered."""
    settings = _settings(tmp_path)
    _seed_orders(settings, [_order(outcome="submitting", armed=True)])

    body = TestClient(create_app(settings)).get("/orders", auth=AUTH).text

    assert "مصيره مجهول" in body


def test_the_execution_page_is_behind_the_login(tmp_path):
    """Slippage on the operator's own wallet is nobody else's business."""
    assert TestClient(create_app(_settings(tmp_path))).get("/orders").status_code == 401


# ---------------------------------------------------------------- bias study
def _write_jsonl(settings: Settings, name: str, rows: list[dict]) -> None:
    import json

    path = settings.journal_dir / name
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _decision_row(action: str, bias: float, direction: str | None = None) -> dict:
    return {
        "ts": "2026-08-21T14:00:00+00:00",
        "action": action,
        "direction": direction,
        "snapshot": {"net_bias": bias},
    }


def _missed_row(direction: str, peak: float, blocked_by: list | None = None) -> dict:
    return {
        "ts": "2026-08-21T15:00:00+00:00",
        "would_be_direction": direction,
        "peak_return_pct": peak,
        "blocked_by": blocked_by or [],
    }


def test_bias_study_counts_each_side_with_the_same_yardstick(tmp_path):
    """The only fair comparison: entry rate when the tape leaned up versus
    entry rate when it leaned down."""
    from qqq_alpha.dashboard.data import bias_study

    settings = _settings(tmp_path)
    _write_jsonl(
        settings,
        "decisions-a.jsonl",
        [
            _decision_row("WAIT", 0.5),
            _decision_row("WAIT", 0.4),
            _decision_row("ENTER", 0.6, "CALL"),
            _decision_row("ENTER", -0.5, "PUT"),
            _decision_row("ENTER", -0.4, "PUT"),
            _decision_row("WAIT", -0.3),
            _decision_row("PASS", 0.05),  # neutral — must not count anywhere
        ],
    )

    study = bias_study(settings)

    assert study["behaviour"]["bullish"]["moments"] == 3
    assert study["behaviour"]["bullish"]["entered"] == 1
    assert study["behaviour"]["bearish"]["moments"] == 3
    assert study["behaviour"]["bearish"]["entered"] == 2


def test_bias_study_prices_the_refused_calls(tmp_path):
    from qqq_alpha.dashboard.data import bias_study

    settings = _settings(tmp_path)
    _write_jsonl(
        settings,
        "missed-a.jsonl",
        [
            _missed_row("CALL", 80.0),
            _missed_row("CALL", 120.0),
            _missed_row("CALL", 60.0, blocked_by=["position_cap: 1/1"]),
            _missed_row("PUT", 90.0),
        ],
    )

    study = bias_study(settings)

    call = study["missed"]["CALL"]
    assert call["count"] == 3
    assert call["declined_by_brain"] == 2, "the rails-blocked one is not the brain's fault"
    assert call["sum_peak"] == 260.0
    assert call["max_peak"] == 120.0


def test_a_small_bullish_sample_returns_unproven_not_innocent(tmp_path):
    """Absence of evidence, stated as such — the market never sat the exam."""
    from qqq_alpha.dashboard.data import bias_study

    settings = _settings(tmp_path)
    _write_jsonl(
        settings,
        "decisions-a.jsonl",
        [_decision_row("ENTER", -0.5, "PUT")] * 12 + [_decision_row("WAIT", 0.4)] * 3,
    )

    study = bias_study(settings)

    assert study["status"] == "unproven"
    assert "غير كافية" in study["verdict"]


def test_a_lopsided_record_with_refused_calls_is_called_biased(tmp_path):
    from qqq_alpha.dashboard.data import bias_study

    settings = _settings(tmp_path)
    _write_jsonl(
        settings,
        "decisions-a.jsonl",
        [_decision_row("WAIT", 0.5)] * 11
        + [_decision_row("ENTER", 0.5, "CALL")]
        + [_decision_row("ENTER", -0.5, "PUT")] * 5
        + [_decision_row("WAIT", -0.5)] * 7,
    )
    _write_jsonl(
        settings,
        "missed-a.jsonl",
        [_missed_row("CALL", 70.0), _missed_row("CALL", 55.0), _missed_row("CALL", 90.0)],
    )

    study = bias_study(settings)

    assert study["status"] == "biased"
    assert "مثبت" in study["verdict"]


def test_balanced_behaviour_is_cleared(tmp_path):
    from qqq_alpha.dashboard.data import bias_study

    settings = _settings(tmp_path)
    _write_jsonl(
        settings,
        "decisions-a.jsonl",
        [_decision_row("ENTER", 0.5, "CALL")] * 3
        + [_decision_row("WAIT", 0.5)] * 9
        + [_decision_row("ENTER", -0.5, "PUT")] * 3
        + [_decision_row("WAIT", -0.5)] * 9,
    )

    study = bias_study(settings)

    assert study["status"] == "clear"


def test_the_bias_page_renders_with_data_and_without(tmp_path):
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))
    assert client.get("/bias", auth=AUTH).status_code == 200

    _seed(settings)
    body = client.get("/bias", auth=AUTH).text
    assert "دراسة الانحياز" in body
    assert "قمم" in body  # the ceiling disclaimer is part of the page's honesty
    assert client.get("/bias").status_code == 401


# ---------------------------------------------------------------- conversations
def test_subscriber_conversation_page_shows_both_directions(tmp_path):
    """The operator reads what a subscriber sent and what the bot answered,
    in order, with button taps shown as inbound lines."""
    settings, memory = _with_subscribers(tmp_path)
    memory.set_tv_username("111", "layth_tv")
    memory.log_message("111", "in", "/start")
    memory.log_message("111", "out", "أهلاً بك في مِرصاد ٩")
    memory.log_message("111", "in", "[زر] ✅ أوافق وأقر")
    memory.log_message("111", "out", "الخطوة الأخيرة: أرسل اسم المستخدم")

    client = TestClient(create_app(settings))
    roster = client.get("/subscribers", auth=AUTH).text
    assert "layth_tv" in roster  # the TradingView name on the roster
    assert "/subscribers/111/messages" in roster and "💬 4" in roster

    page = client.get("/subscribers/111/messages", auth=AUTH).text
    assert "layth_tv" in page
    assert page.index("/start") < page.index("أهلاً بك في مِرصاد ٩") < page.index("[زر] ✅ أوافق")

    # an unknown chat renders an empty conversation, not an error
    assert "لا رسائل مسجلة" in client.get("/subscribers/999/messages", auth=AUTH).text


# ---------------------------------------------------------------- tradingview board
def test_tradingview_board_renders_with_nobody_signed_up(tmp_path):
    body = TestClient(create_app(_settings(tmp_path))).get("/tradingview", auth=AUTH).text
    assert "إدارة الوصول" in body
    assert "لا أحد بانتظار المنح" in body


def test_tradingview_board_sorts_names_into_grant_and_revoke_lists(tmp_path):
    """A live subscriber with a name is owed a grant until the operator
    confirms it; a lapsed one is only on the removal list if access was
    actually granted; a live one without a name is simply waiting."""
    settings, memory = _with_subscribers(tmp_path)
    now = datetime.now(UTC)
    memory.add_subscriber("333", "noor", "Noor", now - timedelta(days=1), now + timedelta(days=29))
    memory.set_tv_username("111", "layth_tv")
    memory.set_tv_username("222", "saud_tv")

    heard: list[tuple] = []

    async def carry(action, row, days):
        heard.append((action, row["chat_id"]))

    client = TestClient(create_app(settings, on_subscriber_change=carry))
    page = client.get("/tradingview", auth=AUTH).text
    assert page.index("⏳ بانتظار المنح") < page.index("layth_tv") < page.index("⛔ يجب إزالته")
    assert "saud_tv" not in page  # lapsed, but never granted: nothing to remove
    assert "Noor" in page and "لم يرسل اسمه" in page

    # the operator clicks "granted": the row moves, the subscriber is told
    response = client.post("/tradingview/111/granted", auth=AUTH)
    assert response.status_code == 200
    assert heard == [("tv_granted", "111")]
    assert memory.subscriber("111")["tv_granted_at"]
    page = client.get("/tradingview", auth=AUTH).text
    assert page.index("✅ مفعّل الآن") < page.index("layth_tv")

    # a granted name that lapses is on the removal list until confirmed
    memory.set_tv_granted("222", now - timedelta(days=30))
    page = client.get("/tradingview", auth=AUTH).text
    assert page.index("⛔ يجب إزالته") < page.index("saud_tv") < page.index("✅ مفعّل الآن")
    client.post("/tradingview/222/revoked", auth=AUTH)
    assert heard[-1] == ("tv_revoked", "222")
    assert memory.subscriber("222")["tv_granted_at"] is None
    assert "saud_tv" not in client.get("/tradingview", auth=AUTH).text

    # re-sending the same name keeps the grant; a different name is a new grant to make
    memory.set_tv_username("111", "@layth_tv")
    assert memory.subscriber("111")["tv_granted_at"]
    memory.set_tv_username("111", "layth_new")
    assert memory.subscriber("111")["tv_granted_at"] is None
    page = client.get("/tradingview", auth=AUTH).text
    assert page.index("⏳ بانتظار المنح") < page.index("layth_new") < page.index("⛔ يجب إزالته")
