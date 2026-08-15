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
    memory = Memory(settings.data_dir / "memory.db")
    counts = memory.counts()
    counts["subscribers"] = memory.subscriber_counts()["trial"]
    return counts


def report_card(settings: Settings) -> dict[str, Any]:
    """Where the engine wins and loses: closed trades bucketed by regime,
    entry hour, stated confidence, and exit reason.

    After enough trades this page answers the questions that matter — "does
    confidence 6 ever pay?", "should the first hour be off-limits?" — with
    arithmetic instead of memory. Below ~10 trades per bucket it is a sketch,
    and the template says so.
    """
    closed = [
        t
        for t in _latest_trades(settings)
        if t.get("closed_at") and t.get("return_pct") is not None
    ]

    def _rows(label_of) -> list[dict[str, Any]]:
        buckets: dict[str, list[float]] = {}
        for trade in closed:
            label = label_of(trade)
            if label is None:
                continue
            buckets.setdefault(str(label), []).append(trade["return_pct"])
        rows = [
            {
                "bucket": bucket,
                "trades": len(returns),
                "win_rate": round(100.0 * sum(r > 0 for r in returns) / len(returns)),
                "avg_return": round(sum(returns) / len(returns), 1),
                "best": round(max(returns), 1),
                "worst": round(min(returns), 1),
            }
            for bucket, returns in buckets.items()
        ]
        return sorted(rows, key=lambda r: r["avg_return"], reverse=True)

    def _hour(trade: dict[str, Any]) -> str | None:
        opened = _parse_ts(trade.get("opened_at"))
        if opened is None:
            return None
        local = opened.astimezone(MARKET_TZ)
        return f"{local.hour:02d}:00-{local.hour + 1:02d}:00 ET"

    returns = [t["return_pct"] for t in closed]
    return {
        "total": len(closed),
        "avg_return": round(sum(returns) / len(returns), 1) if returns else 0.0,
        "win_rate": (
            round(100.0 * sum(r > 0 for r in returns) / len(returns)) if returns else 0
        ),
        "dimensions": [
            {"title": "حسب نظام السوق", "rows": _rows(
                lambda t: (t.get("snapshot_at_entry") or {}).get("regime")
            )},
            {"title": "حسب ساعة الدخول", "rows": _rows(_hour)},
            {"title": "حسب الثقة المعلنة", "rows": _rows(
                lambda t: f"{(t.get('decision') or {}).get('confidence', '?')}/10"
            )},
            {"title": "حسب سبب الخروج", "rows": _rows(
                lambda t: t.get("exit_reason") or None
            )},
        ],
    }


def shadow_overview(settings: Settings) -> dict[str, Any]:
    """The shadow stock desk's record: per-symbol scoreboard plus the raw
    decisions and simulated trades behind it.

    Reads the shadow subdirectory only — the live QQQ pages glob the top-level
    journal dir, and the two records must never mix. Every number here is a
    simulation on model-priced weekly contracts, and the page says so.
    """
    from qqq_alpha.data.massive import parse_occ_symbol

    shadow_dir = settings.journal_dir / "shadow"

    def _symbol_of(row: dict[str, Any]) -> str | None:
        occ = row.get("occ_symbol")
        if occ:
            try:
                return parse_occ_symbol(occ)[0]
            except (ValueError, IndexError):
                pass
        return (row.get("snapshot") or row.get("snapshot_at_entry") or {}).get("symbol")

    trades_latest: dict[str, dict[str, Any]] = {}
    for row in read_jsonl(sorted(shadow_dir.glob("trades-*.jsonl"))):
        if row.get("trade_id"):
            trades_latest[row["trade_id"]] = row
    trades = sorted(
        trades_latest.values(), key=lambda r: r.get("opened_at") or "", reverse=True
    )
    for trade in trades:
        trade["symbol"] = _symbol_of(trade)

    decisions = read_jsonl(sorted(shadow_dir.glob("decisions-*.jsonl")))
    decisions.sort(key=lambda r: r.get("ts") or "", reverse=True)
    for row in decisions:
        row["symbol"] = _symbol_of(row)

    symbols = []
    for symbol in settings.shadow_symbols:
        mine = [t for t in trades if t.get("symbol") == symbol]
        closed = [
            t for t in mine if t.get("closed_at") and t.get("return_pct") is not None
        ]
        returns = [t["return_pct"] for t in closed]
        my_decisions = [d for d in decisions if d.get("symbol") == symbol]
        symbols.append(
            {
                "symbol": symbol,
                "decisions": len(my_decisions),
                "entries": sum(1 for d in my_decisions if d.get("action") == "ENTER"),
                "open": sum(1 for t in mine if not t.get("closed_at")),
                "closed": len(closed),
                "win_rate": (
                    round(100.0 * sum(r > 0 for r in returns) / len(returns))
                    if returns
                    else None
                ),
                "avg_return": (
                    round(sum(returns) / len(returns), 1) if returns else None
                ),
                "total_return": round(sum(returns), 1) if returns else None,
            }
        )

    return {"symbols": symbols, "decisions": decisions[:60], "trades": trades[:60]}


def daily_report(settings: Settings, day: date) -> ReviewStats:
    return review(load_period(settings.journal_dir, since=day, until=day))


def weekly_report(settings: Settings, end_day: date) -> ReviewStats:
    return review(load_period(settings.journal_dir, since=end_day - timedelta(days=6), until=end_day))


def today_et() -> date:
    return datetime.now(MARKET_TZ).date()
