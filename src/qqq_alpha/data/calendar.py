"""Economic calendar awareness.

A CPI morning and a random Tuesday are different markets, and an engine that
cannot tell them apart trades the violent post-release whipsaw as if it were a
clean trend. The schedule itself is deliberately dumb data: a YAML file the
operator maintains (FOMC and CPI dates are published far in advance), plus the
one release that never needs a file — nonfarm payrolls, first Friday of the
month. The brain gets today's events with their distance from now and draws its
own conclusions.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from pathlib import Path

import yaml

from qqq_alpha.config import MARKET_TZ

log = logging.getLogger(__name__)

CALENDAR_PATH = Path(__file__).with_name("economic_calendar.yaml")


def _first_friday(year: int, month: int) -> date:
    day = date(year, month, 1)
    return day + timedelta(days=(4 - day.weekday()) % 7)


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
    """Today's scheduled releases, each stamped with distance from ``now``.

    Returns an empty list on a no-event day — the prompt section simply
    disappears rather than announcing an absence.
    """
    local = now.astimezone(MARKET_TZ)
    today = local.date()

    events: list[dict] = []
    for row in _load_events(path or CALENDAR_PATH):
        row_date = row.get("date")
        if isinstance(row_date, str):
            try:
                row_date = date.fromisoformat(row_date)
            except ValueError:
                continue
        if row_date == today:
            events.append(
                {
                    "time_et": str(row.get("time_et", "?")),
                    "label": str(row.get("label", "?")),
                    "impact": str(row.get("impact", "medium")),
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

    events.sort(key=lambda e: e.get("time_et") or "")
    return events
