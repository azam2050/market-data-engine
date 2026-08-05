"""Tests for the admin dashboard.

The dashboard is read-only over the same journal and memory the engine
already writes, with one write path — approving or rejecting a lesson — so
the tests cover: authentication is enforced, every page renders against both
empty and populated data, and the one write path actually mutates the
playbook and memory it claims to.
"""

from datetime import date, timedelta

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
    for path in ("/", "/trades", "/decisions", "/missed", "/lessons", "/reports"):
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


def test_overview_shows_pending_lesson_count(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    client = TestClient(create_app(settings))

    response = client.get("/", auth=AUTH)
    assert "1" in response.text  # one pending lesson


# ---------------------------------------------------------------- lesson approval
def test_approving_a_lesson_writes_the_playbook_and_clears_it_from_pending(tmp_path):
    settings = _settings(tmp_path)
    lesson_id = _seed(settings)
    applied: list = []
    client = TestClient(
        create_app(settings, on_lesson_applied=lambda book: applied.append(book))
    )

    response = client.post(f"/lessons/{lesson_id}/approve", auth=AUTH, follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/lessons"
    assert settings.playbook_path.exists()
    assert applied and applied[0].version == 2
    assert not Memory(settings.data_dir / "memory.db").pending_lessons()


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
