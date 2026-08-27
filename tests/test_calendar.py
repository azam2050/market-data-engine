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


# ---------------------------------------------------------------- earnings
def test_an_after_close_report_is_flagged_on_the_day_it_lands(tmp_path):
    path = _write_calendar(
        tmp_path,
        "events:\n"
        '  - {date: 2026-08-26, time_et: "16:20", label: "أرباح NVDA", '
        "impact: high, after_close: true}\n",
    )
    now = datetime(2026, 8, 26, 14, 20, tzinfo=MARKET_TZ)

    events = todays_events(now, path=path)
    assert len(events) == 1
    assert events[0]["minutes_from_now"] == 120  # two hours out, still to come
    # the flag must travel with today's event, not only with yesterday's: a
    # bare "16:20" reads as merely distant, when the real constraint is that
    # today's 0DTE contracts expire before the report is even published
    assert events[0]["after_close"] is True


def test_an_intraday_release_is_not_marked_after_close(tmp_path):
    path = _write_calendar(
        tmp_path,
        "events:\n" '  - {date: 2026-08-12, time_et: "08:30", label: "CPI", impact: high}\n',
    )
    now = datetime(2026, 8, 12, 10, 0, tzinfo=MARKET_TZ)

    events = todays_events(now, path=path)
    assert events[0]["after_close"] is False


def test_the_gap_morning_after_an_after_close_report_is_flagged_too(tmp_path):
    """The violent session is the *next* open, not the afternoon before it."""
    path = _write_calendar(
        tmp_path,
        "events:\n"
        '  - {date: 2026-08-26, time_et: "16:20", label: "أرباح NVDA", '
        "impact: high, after_close: true}\n",
    )
    now = datetime(2026, 8, 27, 9, 40, tzinfo=MARKET_TZ)

    events = todays_events(now, path=path)
    assert len(events) == 1
    assert "صدرت بعد إغلاق أمس" in events[0]["label"]
    assert events[0]["impact"] == "high"


def test_monday_looks_back_to_fridays_close(tmp_path):
    path = _write_calendar(
        tmp_path,
        "events:\n"
        '  - {date: 2026-08-28, time_et: "16:05", label: "أرباح X", '
        "impact: high, after_close: true}\n",
    )
    # 2026-08-28 is a Friday; 2026-08-31 the Monday that opens on its gap
    monday = datetime(2026, 8, 31, 9, 40, tzinfo=MARKET_TZ)

    assert any("صدرت بعد إغلاق أمس" in e["label"] for e in todays_events(monday, path=path))


def test_a_daytime_release_does_not_leak_into_the_next_session(tmp_path):
    """CPI lands at 08:30 and is finished. Only after_close events carry over."""
    path = _write_calendar(
        tmp_path,
        'events:\n  - {date: 2026-08-12, time_et: "08:30", label: "CPI", impact: high}\n',
    )
    next_day = datetime(2026, 8, 13, 9, 40, tzinfo=MARKET_TZ)

    assert todays_events(next_day, path=path) == []


def test_last_nights_report_is_listed_before_todays_schedule(tmp_path):
    path = _write_calendar(
        tmp_path,
        "events:\n"
        '  - {date: 2026-08-26, time_et: "16:20", label: "أرباح NVDA", '
        "impact: high, after_close: true}\n"
        '  - {date: 2026-08-27, time_et: "08:30", label: "CPI", impact: high}\n',
    )
    now = datetime(2026, 8, 27, 9, 40, tzinfo=MARKET_TZ)

    events = todays_events(now, path=path)
    assert [e["impact"] for e in events] == ["high", "high"]
    assert "NVDA" in events[0]["label"] and events[1]["label"] == "CPI"


def test_the_shipped_calendar_carries_the_index_heavyweights(tmp_path):
    """QQQ is a handful of companies wearing an index costume."""
    import yaml

    from qqq_alpha.data.calendar import CALENDAR_PATH

    labels = " ".join(
        str(row.get("label", ""))
        for row in yaml.safe_load(CALENDAR_PATH.read_text(encoding="utf-8"))["events"]
    )
    for ticker in ("NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA"):
        assert ticker in labels, ticker


def test_an_earnings_day_reaches_the_brains_prompt(tmp_path):
    """Deliberately reads a fixture, not the shipped file.

    The operator is told to correct these dates against each company's investor
    relations page; a test pinned to a shipped date would turn doing that into
    a broken build.
    """
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    path = _write_calendar(
        tmp_path,
        "events:\n"
        '  - {date: 2026-08-26, time_et: "16:20", label: "أرباح NVDA", '
        "impact: high, after_close: true}\n",
    )
    bars = synthetic_session("QQQ", datetime(2026, 8, 27).date(), seed=3)
    snap = SnapshotBuilder("QQQ").build(bars[:60])

    prompt = build_user_prompt(
        snap,
        Playbook(),
        calendar_events=todays_events(
            datetime(2026, 8, 27, 9, 40, tzinfo=MARKET_TZ), path=path
        ),
    )
    assert "NVDA" in prompt and "صدرت بعد إغلاق أمس" in prompt


def test_an_after_close_session_tells_the_brain_its_contracts_expire_first(tmp_path):
    """The failure this closes: on NVDA earnings day the brain saw a bare
    "16:20 (in 380 min)" and read it as far away and therefore irrelevant.

    The truth is structural, not a countdown — the report lands after the bell,
    so the whole session is positioning into it and a 0DTE contract expires
    before the catalyst prints. Two trades were taken that day inside a 0.6%
    range and both lost.
    """
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    path = _write_calendar(
        tmp_path,
        "events:\n"
        '  - {date: 2026-08-26, time_et: "16:20", label: "أرباح NVDA", '
        "impact: high, after_close: true}\n",
    )
    bars = synthetic_session("QQQ", datetime(2026, 8, 26).date(), seed=3)
    snap = SnapshotBuilder("QQQ").build(bars[:60])

    prompt = build_user_prompt(
        snap,
        Playbook(),
        calendar_events=todays_events(
            datetime(2026, 8, 26, 10, 0, tzinfo=MARKET_TZ), path=path
        ),
    )
    assert "RELEASED AFTER TODAY'S CLOSE" in prompt
    assert "WAITING IS THE DEFAULT TODAY" in prompt
    # the countdown must be explicitly disarmed: the constraint applies from
    # the opening bell, not from an hour before the release
    assert "irrelevant" in prompt


def test_an_intraday_release_keeps_the_ordinary_event_guidance(tmp_path):
    """CPI at 08:30 is a normal event day — the after-close block must not
    fire and turn every release into a stand-down."""
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    path = _write_calendar(
        tmp_path,
        "events:\n" '  - {date: 2026-08-12, time_et: "08:30", label: "CPI", impact: high}\n',
    )
    bars = synthetic_session("QQQ", datetime(2026, 8, 12).date(), seed=3)
    snap = SnapshotBuilder("QQQ").build(bars[:60])

    prompt = build_user_prompt(
        snap,
        Playbook(),
        calendar_events=todays_events(
            datetime(2026, 8, 12, 10, 0, tzinfo=MARKET_TZ), path=path
        ),
    )
    assert "ECONOMIC CALENDAR TODAY" in prompt
    assert "RELEASED AFTER TODAY'S CLOSE" not in prompt
    assert "WAITING IS THE DEFAULT TODAY" not in prompt
