"""Economic calendar awareness.

A CPI morning and a random Tuesday are different markets, and an engine that
cannot tell them apart trades the violent post-release whipsaw as if it were a
clean trend. The schedule itself is deliberately dumb data: a YAML file the
operator maintains (FOMC and CPI dates are published far in advance), plus the
one release that never needs a file — nonfarm payrolls, first Friday of the
month. The brain gets today's events with their distance from now and draws its
own conclusions.

Earnings are the same kind of fact and were the gap in it. QQQ is a handful of
companies wearing an index costume: when NVDA or AAPL reports, the whole index
moves, and the engine was walking into those sessions unaware. They are listed
here as dates, exactly like CPI — no headline, no estimate, no opinion about
what the number will be.

Earnings differ from a macro release in one way that matters, though. CPI lands
at 08:30 on the day it is dated; earnings land *after the close*, so the violent
session is the **next** morning's gap. An after-close event is therefore
surfaced twice: on its own day, as something approaching, and again on the
following session, as something that already happened.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from pathlib import Path

import yaml

from qqq_alpha.config import MARKET_TZ

log = logging.getLogger(__name__)

CALENDAR_PATH = Path(__file__).with_name("economic_calendar.yaml")

# the slot an after-close release occupies on the following session: no clock
# time fits it, because it landed while the market was shut
BEFORE_OPEN = "قبل الافتتاح"

# NYSE full-day closures. Kept as plain dates so a glance shows what is
# covered; extend the table each autumn when the exchange publishes the next
# year. A date missing here degrades to "treated as a trading day", which is
# the pre-existing behaviour, not a crash.
US_MARKET_HOLIDAYS: frozenset[date] = frozenset({
    # 2026
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
    date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
    date(2026, 11, 26), date(2026, 12, 25),
    # 2027
    date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15), date(2027, 3, 26),
    date(2027, 5, 31), date(2027, 6, 18), date(2027, 7, 5), date(2027, 9, 6),
    date(2027, 11, 25), date(2027, 12, 24),
})


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in US_MARKET_HOLIDAYS


def next_trading_day(day: date) -> date:
    """The first trading day strictly after ``day``."""
    probe = day + timedelta(days=1)
    while not is_trading_day(probe):
        probe += timedelta(days=1)
    return probe


def _first_friday(year: int, month: int) -> date:
    day = date(year, month, 1)
    return day + timedelta(days=(4 - day.weekday()) % 7)


def _previous_session(day: date) -> date:
    """The weekday before ``day`` — Monday looks back to Friday.

    Calendar-only, like ``_is_last_session_of_month``: a market holiday would
    make this a day early, which costs a stale line in the prompt. Getting it
    wrong the other way would hide the gap that actually moved the open.
    """
    probe = day - timedelta(days=1)
    while probe.weekday() >= 5:
        probe -= timedelta(days=1)
    return probe


def _row_date(row: dict) -> date | None:
    value = row.get("date")
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return value if isinstance(value, date) else None


def _load_events(path: Path) -> list[dict]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return list(payload.get("events") or [])
    except (OSError, yaml.YAMLError) as exc:
        # a broken calendar file must never block trading — it degrades to
        # "no event awareness today", which is exactly where we started
        log.warning("could not read economic calendar (%s)", exc)
        return []


def todays_events(now: datetime, path: Path | None = None) -> list[dict]:
    """Today's scheduled releases, each stamped with distance from ``now``,
    plus anything that landed after the previous session's close.

    Returns an empty list on a no-event day — the prompt section simply
    disappears rather than announcing an absence.
    """
    local = now.astimezone(MARKET_TZ)
    today = local.date()
    yesterday = _previous_session(today)

    events: list[dict] = []
    for row in _load_events(path or CALENDAR_PATH):
        row_date = _row_date(row)
        after_close = bool(row.get("after_close"))
        if row_date == today:
            # `after_close` travels with today's event too, not only with
            # yesterday's. Without it the brain sees a bare "16:20" and reads
            # the event as merely distant, when the truth is structural: the
            # report lands after the bell, so the WHOLE session is positioning
            # ahead of it and a 0DTE contract expires before the catalyst it
            # would need. That is the shape of an NVDA-earnings session — the
            # index goes nowhere all day, then gaps tomorrow.
            events.append(
                {
                    "time_et": str(row.get("time_et", "?")),
                    "label": str(row.get("label", "?")),
                    "impact": str(row.get("impact", "medium")),
                    "after_close": after_close,
                }
            )
        elif row_date == yesterday and after_close:
            # the report itself was last night; this session opens on its gap,
            # which is the half of an earnings event that actually trades
            events.append(
                {
                    "time_et": BEFORE_OPEN,
                    "label": f"{row.get('label', '?')} — صدرت بعد إغلاق أمس",
                    "impact": str(row.get("impact", "medium")),
                    "minutes_from_now": None,
                }
            )

    if today == _first_friday(today.year, today.month):
        events.append(
            {"time_et": "08:30", "label": "تقرير الوظائف NFP", "impact": "high"}
        )

    for event in events:
        try:
            hour, minute = str(event["time_et"]).split(":")
            event_dt = datetime.combine(
                today, time(int(hour), int(minute)), tzinfo=MARKET_TZ
            )
            event["minutes_from_now"] = round((event_dt - local).total_seconds() / 60.0)
        except (ValueError, KeyError):
            event["minutes_from_now"] = None

    # last night's release belongs at the top: it is the only one that already
    # moved the price the brain is looking at
    events.sort(key=lambda e: (0 if e["time_et"] == BEFORE_OPEN else 1, e["time_et"]))
    return events
