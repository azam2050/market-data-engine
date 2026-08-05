"""Data access for the dashboard.

Every function here reads the journal (JSONL, full detail — including the
model's actual thesis/risks/invalidation text) or the long-term memory
(SQLite, structured aggregates and pending lessons). Nothing is computed or
cached separately from what the engine itself already persists.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from qqq_alpha.config import MARKET_TZ, Settings
from qqq_alpha.dashboard.behavior import classify_entry, classify_exit
from qqq_alpha.live.review import ReviewStats, load_period, read_jsonl, review
from qqq_alpha.memory import Memory


def _latest_trades(settings: Settings) -> list[dict[str, Any]]:
    """Every trade, final state only — the journal is append-only per update."""
    rows = read_jsonl(sorted(settings.journal_dir.glob("trades-*.jsonl")))
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        trade_id = row.get("trade_id")
        if trade_id:
            latest[trade_id] = row
    return sorted(
        latest.values(), key=lambda r: r.get("opened_at") or "", reverse=True
    )


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def recent_trades(settings: Settings, limit: int = 100) -> list[dict[str, Any]]:
    trades = _latest_trades(settings)[:limit]
    for trade in trades:
        # hold_minutes is not a Trade field — it only exists once a trade has
        # both ends, so it is derived here rather than stored redundantly
        opened, closed = _parse_ts(trade.get("opened_at")), _parse_ts(trade.get("closed_at"))
        trade["hold_minutes"] = (
            round((closed - opened).total_seconds() / 60, 1) if opened and closed else None
        )
        trade["behavior"] = classify_exit(trade)
        trade["entry_note"] = classify_entry(trade)
    return trades


def open_trades(settings: Settings) -> list[dict[str, Any]]:
    return [t for t in _latest_trades(settings) if not t.get("closed_at")]


def recent_decisions(settings: Settings, limit: int = 150) -> list[dict[str, Any]]:
    """Every decision the brain made, including a plain PASS — the full reasoning."""
    rows = read_jsonl(sorted(settings.journal_dir.glob("decisions-*.jsonl")))
    rows.sort(key=lambda r: r.get("ts") or "", reverse=True)
    return rows[:limit]


def recent_missed(settings: Settings, limit: int = 100) -> list[dict[str, Any]]:
    rows = read_jsonl(sorted(settings.journal_dir.glob("missed-*.jsonl")))
    rows.sort(key=lambda r: r.get("ts") or "", reverse=True)
    return rows[:limit]


def pending_lessons(settings: Settings) -> list[dict[str, Any]]:
    return Memory(settings.data_dir / "memory.db").pending_lessons()


def memory_counts(settings: Settings) -> dict[str, int]:
    return Memory(settings.data_dir / "memory.db").counts()


def daily_report(settings: Settings, day: date) -> ReviewStats:
    return review(load_period(settings.journal_dir, since=day, until=day))


def weekly_report(settings: Settings, end_day: date) -> ReviewStats:
    return review(load_period(settings.journal_dir, since=end_day - timedelta(days=6), until=end_day))


def today_et() -> date:
    return datetime.now(MARKET_TZ).date()
