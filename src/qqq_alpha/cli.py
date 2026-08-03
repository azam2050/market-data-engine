"""Command line interface.

    qqq demo                      offline end-to-end run, no API keys needed
    qqq backtest --start --end    replay real history
    qqq snapshot --day            print what the brain would see at a moment
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta

import typer
from rich.console import Console

from qqq_alpha.backtest.engine import Backtester, prior_day_map, sessions_from_bars
from qqq_alpha.backtest.report import render_report
from qqq_alpha.brain.decider import build_decider
from qqq_alpha.brain.playbook import load_playbook
from qqq_alpha.config import get_settings
from qqq_alpha.data.massive import MassiveClient
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.data.synthetic import synthetic_week
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal

app = typer.Typer(add_completion=False, help="QQQ Alpha — AI options research engine")
console = Console()


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )


@app.command()
def demo(
    days: int = typer.Option(5, help="How many synthetic sessions to replay"),
    mode: str = typer.Option("heuristic", help="heuristic (free) or ai (uses your API key)"),
    seed: int = typer.Option(7, help="Deterministic seed"),
) -> None:
    """Run the whole pipeline offline on generated data. Proves the plumbing works."""
    settings = get_settings()
    _setup_logging(settings.log_level)

    start = date.today() - timedelta(days=days * 2)
    sessions = synthetic_week(settings.primary_symbol, start, seed=seed)

    journal = Journal(settings.journal_dir, session_tag="demo")
    backtester = Backtester(
        settings=settings,
        decider=build_decider(settings, mode),
        pricer=BlackScholesPricer(),
        playbook=load_playbook(settings.playbook_path),
        journal=journal,
    )

    result = asyncio.run(backtester.run(sessions))
    render_report(result, console)
    console.print(f"\n[dim]journal written to {settings.journal_dir}[/]")


@app.command()
def backtest(
    start: str = typer.Option(..., help="Start date, YYYY-MM-DD"),
    end: str = typer.Option(..., help="End date, YYYY-MM-DD"),
    mode: str = typer.Option("ai", help="ai or heuristic"),
    volatility: float = typer.Option(
        0.22, help="Volatility used to model contract prices until real chains are wired in"
    ),
) -> None:
    """Replay real market history through the full engine."""
    settings = get_settings()
    _setup_logging(settings.log_level)

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    async def _run() -> None:
        async with MassiveClient(settings) as client:
            console.print(f"[cyan]fetching {settings.primary_symbol} history…[/]")
            daily = await client.daily_bars(
                settings.primary_symbol, start_date - timedelta(days=10), end_date
            )
            prior_days = prior_day_map(daily)

            sessions: dict[date, list] = {}
            day = start_date
            while day <= end_date:
                if day.weekday() < 5:
                    bars = await client.minute_bars(settings.primary_symbol, day)
                    if bars:
                        sessions.update(sessions_from_bars(bars))
                day += timedelta(days=1)

            if not sessions:
                console.print("[red]no sessions returned — check the date range and plan tier[/]")
                raise typer.Exit(code=1)

            console.print(f"[green]{len(sessions)} sessions loaded[/]")

            journal = Journal(settings.journal_dir, session_tag=f"bt-{start}-{end}")
            backtester = Backtester(
                settings=settings,
                decider=build_decider(settings, mode),
                pricer=BlackScholesPricer(volatility),
                playbook=load_playbook(settings.playbook_path),
                journal=journal,
            )
            result = await backtester.run(sessions, prior_days=prior_days)
            render_report(result, console)

    asyncio.run(_run())


@app.command()
def snapshot(
    day: str = typer.Option(..., help="Session date, YYYY-MM-DD"),
    at: str = typer.Option("10:30", help="Time of day, HH:MM ET"),
) -> None:
    """Print the exact evidence pack the brain would receive at a moment."""
    settings = get_settings()
    _setup_logging(settings.log_level)
    target_day = date.fromisoformat(day)
    hour, minute = (int(part) for part in at.split(":"))

    async def _run() -> None:
        async with MassiveClient(settings) as client:
            bars = await client.minute_bars(settings.primary_symbol, target_day)

        if not bars:
            console.print("[red]no bars for that session[/]")
            raise typer.Exit(code=1)

        from qqq_alpha.config import MARKET_TZ

        cutoff = datetime(
            target_day.year, target_day.month, target_day.day, hour, minute, tzinfo=MARKET_TZ
        )
        window = [b for b in bars if b.ts.astimezone(MARKET_TZ) <= cutoff]
        if len(window) < 30:
            console.print("[red]not enough bars before that time[/]")
            raise typer.Exit(code=1)

        snap = SnapshotBuilder(settings.primary_symbol).build(window)
        console.print(f"[bold]{settings.primary_symbol} @ {snap.underlying.close}[/]  "
                      f"regime={snap.regime.value}  net_bias={snap.net_bias:+.3f}")
        console.print(snap.indicators)
        console.print(snap.levels)
        for obs in snap.observations:
            console.print(f"  [cyan]{obs.name:<22}[/] score={obs.score:+.2f} "
                          f"conf={obs.confidence:.2f}  {obs.note}")

    asyncio.run(_run())


@app.command()
def config() -> None:
    """Show the resolved configuration and flag anything missing."""
    settings = get_settings()
    console.print(f"primary symbol      : {settings.primary_symbol}")
    console.print(f"leaders             : {', '.join(settings.leader_symbols)}")
    console.print(f"max trades/day      : {settings.max_trades_per_day}")
    console.print(f"attention threshold : {settings.attention_threshold}")
    console.print(f"min target return   : {settings.min_target_return_pct}%")
    console.print(f"data key configured : {bool(settings.massive_api_key)}")
    console.print(f"brain key configured: {bool(settings.anthropic_api_key)}")
    console.print(f"brain model set     : {bool(settings.anthropic_model)}")
    console.print(f"playbook            : {settings.playbook_path}")


if __name__ == "__main__":
    app()
