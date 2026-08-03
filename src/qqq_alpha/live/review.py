"""Shadow-period performance review.

Reads the journal and answers the only question that matters during the shadow
period: is this engine actually good, or does it just look busy?

Deliberately reports the uncomfortable numbers alongside the flattering ones —
confidence calibration, how often the engine sat out, what its caution cost, and
whether a handful of outliers are carrying the entire result. A track record you
can trust is one that was allowed to look bad.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median

from qqq_alpha.config import MARKET_TZ


@dataclass
class ReviewPeriod:
    since: date | None = None
    until: date | None = None
    sessions: set[date] = field(default_factory=set)
    trades: list[dict] = field(default_factory=list)
    decisions: list[dict] = field(default_factory=list)
    missed: list[dict] = field(default_factory=list)

    @property
    def closed(self) -> list[dict]:
        return [t for t in self.trades if t.get("return_pct") is not None]


def _read_jsonl(paths: list[Path]) -> list[dict]:
    rows: list[dict] = []
    for path in paths:
        if not path.exists():
            continue
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    return rows


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def load_period(
    journal_dir: Path, since: date | None = None, until: date | None = None
) -> ReviewPeriod:
    """Gather every journalled trade and decision inside a date range."""
    period = ReviewPeriod(since=since, until=until)

    trades = _read_jsonl(sorted(journal_dir.glob("trades-*.jsonl")))
    decisions = _read_jsonl(sorted(journal_dir.glob("decisions-*.jsonl")))
    missed = _read_jsonl(sorted(journal_dir.glob("missed-*.jsonl")))

    # the journal is append-only, so a trade appears once per update; keep the
    # last record for each id, which is its final state
    latest: dict[str, dict] = {}
    for row in trades:
        trade_id = row.get("trade_id")
        if trade_id:
            latest[trade_id] = row

    def _in_range(row: dict, key: str) -> bool:
        ts = _parse_ts(row.get(key))
        if ts is None:
            return False
        day = ts.astimezone(MARKET_TZ).date()
        if since and day < since:
            return False
        if until and day > until:
            return False
        period.sessions.add(day)
        return True

    period.trades = [t for t in latest.values() if _in_range(t, "opened_at")]
    period.decisions = [d for d in decisions if _in_range(d, "ts")]
    period.missed = [m for m in missed if _in_range(m, "ts")]
    return period


@dataclass
class ReviewStats:
    sessions: int = 0
    sessions_with_signal: int = 0
    signals: int = 0
    closed: int = 0
    still_open: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    expectancy_pct: float = 0.0
    median_pct: float = 0.0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    profit_factor: float | None = None
    best_pct: float = 0.0
    worst_pct: float = 0.0
    hit_50: int = 0
    ran_100: int = 0
    top_trade_share: float = 0.0
    avg_hold_min: float = 0.0
    passes: int = 0
    waits: int = 0
    overrides: int = 0
    missed: int = 0
    by_confidence: dict[int, tuple[int, float]] = field(default_factory=dict)
    by_hour: dict[int, tuple[int, float]] = field(default_factory=dict)
    by_exit: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def review(period: ReviewPeriod) -> ReviewStats:
    stats = ReviewStats(
        sessions=len(period.sessions),
        signals=len(period.trades),
        missed=len(period.missed),
    )

    stats.passes = sum(1 for d in period.decisions if d.get("action") == "PASS")
    stats.waits = sum(1 for d in period.decisions if d.get("action") == "WAIT")
    stats.overrides = sum(1 for d in period.decisions if d.get("overrides"))

    closed = period.closed
    stats.closed = len(closed)
    stats.still_open = len(period.trades) - len(closed)
    stats.sessions_with_signal = len(
        {
            _parse_ts(t.get("opened_at")).astimezone(MARKET_TZ).date()
            for t in period.trades
            if _parse_ts(t.get("opened_at"))
        }
    )

    if not closed:
        stats.warnings.append("no closed trades yet — nothing can be concluded")
        return stats

    returns = [float(t["return_pct"]) for t in closed]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]

    stats.wins, stats.losses = len(wins), len(losses)
    stats.win_rate = round(len(wins) / len(returns) * 100, 1)
    stats.expectancy_pct = round(mean(returns), 1)
    stats.median_pct = round(median(returns), 1)
    stats.avg_win_pct = round(mean(wins), 1) if wins else 0.0
    stats.avg_loss_pct = round(mean(losses), 1) if losses else 0.0
    stats.best_pct = round(max(returns), 1)
    stats.worst_pct = round(min(returns), 1)

    gross_win, gross_loss = sum(wins), abs(sum(losses))
    stats.profit_factor = round(gross_win / gross_loss, 2) if gross_loss else None

    stats.hit_50 = sum(1 for t in closed if float(t.get("max_favorable_pct") or 0) >= 50)
    stats.ran_100 = sum(1 for r in returns if r >= 100)

    # is the whole result one lucky trade?
    total = sum(returns)
    if total > 0:
        stats.top_trade_share = round(max(returns) / total * 100, 1)
        if stats.top_trade_share > 60:
            stats.warnings.append(
                f"a single trade is {stats.top_trade_share:.0f}% of all profit — "
                "the sample is not yet meaningful"
            )

    holds: list[float] = []
    by_conf: dict[int, list[float]] = defaultdict(list)
    by_hour: dict[int, list[float]] = defaultdict(list)

    for trade in closed:
        opened, closed_at = _parse_ts(trade.get("opened_at")), _parse_ts(trade.get("closed_at"))
        if opened and closed_at:
            holds.append((closed_at - opened).total_seconds() / 60)
        if opened:
            by_hour[opened.astimezone(MARKET_TZ).hour].append(float(trade["return_pct"]))

        confidence = (trade.get("decision") or {}).get("confidence")
        if isinstance(confidence, int):
            by_conf[confidence].append(float(trade["return_pct"]))

        reason = trade.get("exit_reason") or "—"
        stats.by_exit[reason] = stats.by_exit.get(reason, 0) + 1

    stats.avg_hold_min = round(mean(holds), 1) if holds else 0.0
    stats.by_confidence = {c: (len(v), round(mean(v), 1)) for c, v in sorted(by_conf.items())}
    stats.by_hour = {h: (len(v), round(mean(v), 1)) for h, v in sorted(by_hour.items())}

    # is stated confidence worth anything?
    if len(stats.by_confidence) >= 2:
        levels = sorted(stats.by_confidence)
        low, high = stats.by_confidence[levels[0]][1], stats.by_confidence[levels[-1]][1]
        if high <= low:
            stats.warnings.append(
                "high-confidence calls are not outperforming low-confidence ones — "
                "the confidence score is currently noise"
            )

    if stats.closed < 30:
        stats.warnings.append(
            f"only {stats.closed} closed trades — too few to judge; aim for 60+"
        )

    if stats.missed > stats.signals * 2 and stats.missed > 5:
        stats.warnings.append(
            f"{stats.missed} qualifying setups were declined vs {stats.signals} taken — "
            "the engine may be too cautious"
        )

    return stats
