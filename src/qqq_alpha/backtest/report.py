"""Backtest reporting.

Reports the numbers that decide whether this strategy is real:
expectancy, profit factor, the distribution of outcomes (because the whole
thesis rests on runners), and the cost of the engine's own caution.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from statistics import mean

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from qqq_alpha.backtest.engine import BacktestResult
from qqq_alpha.config import MARKET_TZ


@dataclass
class Summary:
    days: int = 0
    trading_days: int = 0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    expectancy_pct: float = 0.0
    profit_factor: float | None = None
    best_pct: float = 0.0
    worst_pct: float = 0.0
    total_pct: float = 0.0
    runners_over_100: int = 0
    hit_50_pct: int = 0
    avg_hold_minutes: float = 0.0
    brain_calls: int = 0
    missed_count: int = 0
    missed_avg_peak_pct: float = 0.0
    by_hour: dict[int, tuple[int, float]] = field(default_factory=dict)
    by_confidence: dict[int, tuple[int, float]] = field(default_factory=dict)
    rail_blocks: dict[str, int] = field(default_factory=dict)
    exit_reasons: dict[str, int] = field(default_factory=dict)


def summarize(result: BacktestResult) -> Summary:
    trades = result.trades
    summary = Summary(
        days=len(result.days),
        trading_days=sum(1 for d in result.days if d.trades),
        trades=len(trades),
        brain_calls=result.brain_calls,
        missed_count=len(result.missed),
    )

    if result.missed:
        summary.missed_avg_peak_pct = round(
            mean(m.peak_return_pct for m in result.missed), 1
        )

    for day in result.days:
        for key, count in day.rail_blocks.items():
            summary.rail_blocks[key] = summary.rail_blocks.get(key, 0) + count

    if not trades:
        return summary

    returns = [t.return_pct or 0.0 for t in trades]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]

    summary.wins = len(wins)
    summary.losses = len(losses)
    summary.win_rate = round(len(wins) / len(returns) * 100.0, 1)
    summary.avg_win_pct = round(mean(wins), 1) if wins else 0.0
    summary.avg_loss_pct = round(mean(losses), 1) if losses else 0.0
    summary.expectancy_pct = round(mean(returns), 1)
    summary.best_pct = round(max(returns), 1)
    summary.worst_pct = round(min(returns), 1)
    summary.total_pct = round(sum(returns), 1)
    summary.runners_over_100 = sum(1 for r in returns if r >= 100)
    summary.hit_50_pct = sum(1 for t in trades if t.max_favorable_pct >= 50)

    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    summary.profit_factor = round(gross_win / gross_loss, 2) if gross_loss > 0 else None

    holds = [
        (t.closed_at - t.opened_at).total_seconds() / 60.0
        for t in trades
        if t.closed_at is not None
    ]
    summary.avg_hold_minutes = round(mean(holds), 1) if holds else 0.0

    by_hour: dict[int, list[float]] = defaultdict(list)
    by_conf: dict[int, list[float]] = defaultdict(list)
    for trade in trades:
        hour = trade.opened_at.astimezone(MARKET_TZ).hour
        by_hour[hour].append(trade.return_pct or 0.0)
        by_conf[trade.decision.confidence].append(trade.return_pct or 0.0)
        summary.exit_reasons[trade.exit_reason] = (
            summary.exit_reasons.get(trade.exit_reason, 0) + 1
        )

    summary.by_hour = {h: (len(v), round(mean(v), 1)) for h, v in sorted(by_hour.items())}
    summary.by_confidence = {
        c: (len(v), round(mean(v), 1)) for c, v in sorted(by_conf.items())
    }
    return summary


def render_report(result: BacktestResult, console: Console | None = None) -> None:
    console = console or Console()
    summary = summarize(result)

    if result.price_source_is_approximate:
        console.print(
            Panel(
                "[bold yellow]Option prices are MODELLED (Black-Scholes), not real fills.[/]\n"
                "Treat these numbers as a plumbing check, not evidence of edge.\n"
                "Re-run against real contract history before drawing any conclusion.",
                title="⚠ approximation",
                border_style="yellow",
            )
        )

    headline = Table(title="Backtest Summary", show_header=False, box=None)
    headline.add_column(style="cyan", width=28)
    headline.add_column(style="bold")
    headline.add_row("Sessions replayed", str(summary.days))
    headline.add_row("Sessions with a trade", f"{summary.trading_days} ({_pct(summary.trading_days, summary.days)})")
    headline.add_row("Trades", str(summary.trades))
    headline.add_row("Win rate", f"{summary.win_rate}%  ({summary.wins}W / {summary.losses}L)")
    headline.add_row("Expectancy per trade", _colored(summary.expectancy_pct))
    headline.add_row("Average win", _colored(summary.avg_win_pct))
    headline.add_row("Average loss", _colored(summary.avg_loss_pct))
    headline.add_row("Profit factor", str(summary.profit_factor if summary.profit_factor is not None else "—"))
    headline.add_row("Best / worst", f"{summary.best_pct:+.0f}% / {summary.worst_pct:+.0f}%")
    headline.add_row("Trades that ran +100%", str(summary.runners_over_100))
    headline.add_row("Trades that touched +50%", f"{summary.hit_50_pct} of {summary.trades}")
    headline.add_row("Average hold", f"{summary.avg_hold_minutes:.0f} min")
    headline.add_row("Brain invocations", str(summary.brain_calls))
    console.print(headline)

    if summary.by_hour:
        hours = Table(title="By hour of day (ET)")
        hours.add_column("Hour")
        hours.add_column("Trades", justify="right")
        hours.add_column("Avg return", justify="right")
        for hour, (count, avg) in summary.by_hour.items():
            hours.add_row(f"{hour:02d}:00", str(count), _colored(avg))
        console.print(hours)

    if summary.by_confidence:
        conf = Table(title="Confidence calibration — does a 9 actually beat a 6?")
        conf.add_column("Stated confidence")
        conf.add_column("Trades", justify="right")
        conf.add_column("Avg return", justify="right")
        for level, (count, avg) in summary.by_confidence.items():
            conf.add_row(str(level), str(count), _colored(avg))
        console.print(conf)

    if summary.exit_reasons:
        exits = Table(title="Exit reasons")
        exits.add_column("Reason")
        exits.add_column("Count", justify="right")
        for reason, count in sorted(summary.exit_reasons.items(), key=lambda kv: -kv[1]):
            exits.add_row(reason or "—", str(count))
        console.print(exits)

    cost = Table(title="Cost of caution — setups we declined that would have paid")
    cost.add_column("Metric")
    cost.add_column("Value", justify="right")
    cost.add_row("Missed opportunities (≥ target)", str(summary.missed_count))
    cost.add_row("Their average peak", f"{summary.missed_avg_peak_pct:+.0f}%")
    for key, count in sorted(summary.rail_blocks.items(), key=lambda kv: -kv[1]):
        cost.add_row(f"blocked by {key}", str(count))
    console.print(cost)

    if summary.missed_count > max(summary.trades * 2, 5):
        console.print(
            Panel(
                "[bold red]The engine declined far more qualifying setups than it took.[/]\n"
                "Loosen ATTENTION_THRESHOLD or the rails before blaming the brain.",
                border_style="red",
            )
        )


def _pct(part: int, whole: int) -> str:
    return f"{(part / whole * 100):.0f}%" if whole else "—"


def _colored(value: float) -> str:
    color = "green" if value > 0 else "red" if value < 0 else "white"
    return f"[{color}]{value:+.1f}%[/]"
