"""The economic calendar: a CPI morning and a random Tuesday are different
markets, and the brain must be told which one it is standing in."""

from __future__ import annotations

from datetime import datetime

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.data.calendar import todays_events


def _write_calendar(tmp_path, body: str):
    path = tmp_path / "cal.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def test_todays_events_returns_only_today_with_distance(tmp_path):
    path = _write_calendar(
        tmp_path,
        "events:\n"
        '  - {date: 2026-08-12, time_et: "08:30", label: "CPI", impact: high}\n'
        '  - {date: 2026-08-13, time_et: "14:00", label: "FOMC", impact: high}\n',
    )
    now = datetime(2026, 8, 12, 9, 36, tzinfo=MARKET_TZ)

    events = todays_events(now, path=path)
    assert len(events) == 1
    assert events[0]["label"] == "CPI"
    assert events[0]["minutes_from_now"] == -66  # released 66 minutes ago


def test_nfp_first_friday_needs_no_calendar_entry(tmp_path):
    path = _write_calendar(tmp_path, "events: []\n")
    # 2026-08-07 is the first Friday of August
    first_friday = datetime(2026, 8, 7, 9, 0, tzinfo=MARKET_TZ)
    ordinary_friday = datetime(2026, 8, 14, 9, 0, tzinfo=MARKET_TZ)

    assert any("NFP" in e["label"] for e in todays_events(first_friday, path=path))
    assert todays_events(ordinary_friday, path=path) == []


def test_a_broken_calendar_file_degrades_to_no_awareness(tmp_path):
    path = _write_calendar(tmp_path, "{{{{ not yaml")
    now = datetime(2026, 8, 12, 9, 36, tzinfo=MARKET_TZ)
    assert todays_events(now, path=path) == []


def test_calendar_section_reaches_the_brains_prompt():
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    bars = synthetic_session("QQQ", datetime(2026, 8, 12).date(), seed=3)
    snap = SnapshotBuilder("QQQ").build(bars[:60])

    prompt = build_user_prompt(
        snap,
        Playbook(),
        calendar_events=[
            {"time_et": "08:30", "label": "CPI", "impact": "high", "minutes_from_now": -66}
        ],
    )
    assert "ECONOMIC CALENDAR TODAY" in prompt
    assert "CPI" in prompt and "66 min ago" in prompt

    quiet = build_user_prompt(snap, Playbook(), calendar_events=[])
    assert "ECONOMIC CALENDAR" not in quiet
