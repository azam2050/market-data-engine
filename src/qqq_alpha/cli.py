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
from rich.panel import Panel
from rich.table import Table

from qqq_alpha.backtest.engine import Backtester, prior_day_map
from qqq_alpha.backtest.report import render_report
from qqq_alpha.brain.decider import build_decider, next_expiry
from qqq_alpha.brain.playbook import load_playbook
from qqq_alpha.config import MARKET_TZ, get_settings
from qqq_alpha.data.chain import LiveChainPricer
from qqq_alpha.data.massive import MassiveClient
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.data.synthetic import synthetic_week
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal
from qqq_alpha.learning import analyse, apply_lesson, propose
from qqq_alpha.live.engine import LiveEngine
from qqq_alpha.live.notifier import ConsoleNotifier
from qqq_alpha.live.preflight import run_preflight
from qqq_alpha.live.review import load_period, review
from qqq_alpha.live.stream import LiveBarStream, StreamAuthError, drain
from qqq_alpha.live.telegram import FanoutNotifier, TelegramNotifier, verify_telegram
from qqq_alpha.memory import Memory

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
        memory=Memory(settings.data_dir / "memory.db"),
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
            skipped: list[str] = []
            day = start_date
            while day <= end_date:
                if day.weekday() < 5:
                    session_data = await client.session(settings.primary_symbol, day)
                    if session_data.regular and session_data.is_usable:
                        sessions[day] = session_data.regular
                    elif session_data.regular:
                        skipped.append(
                            f"{day}: {session_data.quality.summary() if session_data.quality else 'unusable'}"
                        )
                day += timedelta(days=1)

            if not sessions:
                console.print("[red]no sessions returned — check the date range and plan tier[/]")
                raise typer.Exit(code=1)

            console.print(f"[green]{len(sessions)} sessions loaded[/]")
            if skipped:
                console.print(f"[yellow]{len(skipped)} sessions skipped for data quality:[/]")
                for line in skipped[:10]:
                    console.print(f"  [dim]{line}[/]")

            journal = Journal(settings.journal_dir, session_tag=f"bt-{start}-{end}")
            backtester = Backtester(
                settings=settings,
                decider=build_decider(settings, mode),
                pricer=BlackScholesPricer(volatility),
                playbook=load_playbook(settings.playbook_path),
                journal=journal,
                memory=Memory(settings.data_dir / "memory.db"),
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
            session_data = await client.session(settings.primary_symbol, target_day)

        if not session_data.regular:
            console.print("[red]no regular-session bars for that day[/]")
            raise typer.Exit(code=1)

        from qqq_alpha.config import MARKET_TZ

        cutoff = datetime(
            target_day.year, target_day.month, target_day.day, hour, minute, tzinfo=MARKET_TZ
        )
        window = [b for b in session_data.regular if b.ts.astimezone(MARKET_TZ) <= cutoff]
        if len(window) < 30:
            console.print("[red]not enough bars before that time[/]")
            raise typer.Exit(code=1)

        snap = SnapshotBuilder(settings.primary_symbol).build(
            window,
            overnight_high=session_data.premarket_high,
            overnight_low=session_data.premarket_low,
            quality=session_data.quality,
        )
        console.print(f"[bold]{settings.primary_symbol} @ {snap.underlying.close}[/]  "
                      f"regime={snap.regime.value}  net_bias={snap.net_bias:+.3f}")
        console.print(f"[dim]data: {snap.data_quality}[/]")
        for label, pack in snap.timeframes.items():
            console.print(f"[magenta]{label}[/] {pack}")
        console.print(snap.levels)
        for obs in snap.observations:
            console.print(f"  [cyan]{obs.name:<22}[/] score={obs.score:+.2f} "
                          f"conf={obs.confidence:.2f}  {obs.note}")

    asyncio.run(_run())


@app.command()
def live(
    mode: str = typer.Option("ai", help="ai or heuristic"),
    shadow: bool = typer.Option(
        True,
        help="Shadow mode publishes signals without claiming a track record. Keep it on until the engine has proven itself.",
    ),
    volatility: float = typer.Option(0.22, help="Volatility for modelled contract pricing"),
) -> None:
    """Run the engine against the live market feed."""
    settings = get_settings()
    _setup_logging(settings.log_level)

    if not settings.massive_api_key:
        console.print("[red]MASSIVE_API_KEY is not set — see .env.example[/]")
        raise typer.Exit(code=1)

    if settings.massive_feed_mode != "real_time":
        console.print(
            Panel(
                "[bold yellow]Feed is DELAYED (15 minutes).[/]\n"
                "0DTE setups do not survive a 15-minute delay, so these signals are for "
                "pipeline validation only — never for execution.\n"
                "Real-time requires the Advanced tier on both stocks and options.",
                border_style="yellow",
            )
        )

    notifier = ConsoleNotifier(console)
    if settings.telegram_bot_token and settings.telegram_chat_id:
        notifier = FanoutNotifier(
            notifier,
            TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id),
        )
        console.print("[green]Telegram delivery enabled[/]")
    else:
        console.print(
            "[yellow]Telegram not configured — signals print here only. "
            "You will miss them when away from this screen.[/]"
        )

    # real quotes, with the model kept only as a labelled fallback for contracts
    # the chain does not cover
    pricer = LiveChainPricer(settings, fallback=BlackScholesPricer(volatility))

    engine = LiveEngine(
        settings=settings,
        decider=build_decider(settings, mode),
        pricer=pricer,
        playbook=load_playbook(settings.playbook_path),
        journal=Journal(settings.journal_dir, session_tag="live"),
        notifier=notifier,
        dry_run=shadow,
    )

    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        console.print("\n[dim]stopped by user[/]")
        console.print(engine.status.as_dict())


@app.command()
def report(
    since: str = typer.Option(None, help="Start date YYYY-MM-DD (default: everything)"),
    until: str = typer.Option(None, help="End date YYYY-MM-DD"),
) -> None:
    """Review the shadow period: what the engine did, and whether it was any good."""
    settings = get_settings()
    period = load_period(
        settings.journal_dir,
        since=date.fromisoformat(since) if since else None,
        until=date.fromisoformat(until) if until else None,
    )
    stats = review(period)

    head = Table(title="Shadow Period Review", show_header=False, box=None)
    head.add_column(style="cyan", width=30)
    head.add_column(style="bold")
    head.add_row("Sessions observed", str(stats.sessions))
    head.add_row("Sessions with a signal", str(stats.sessions_with_signal))
    head.add_row("Signals published", str(stats.signals))
    head.add_row("Closed / still open", f"{stats.closed} / {stats.still_open}")
    head.add_row("Passed / waited", f"{stats.passes} / {stats.waits}")
    head.add_row("Playbook overrides", str(stats.overrides))
    console.print(head)

    if stats.closed:
        perf = Table(title="Performance", show_header=False, box=None)
        perf.add_column(style="cyan", width=30)
        perf.add_column(style="bold")
        perf.add_row("Win rate", f"{stats.win_rate}% ({stats.wins}W / {stats.losses}L)")
        perf.add_row("Expectancy per trade", f"{stats.expectancy_pct:+.1f}%")
        perf.add_row("Median trade", f"{stats.median_pct:+.1f}%")
        perf.add_row("Average win / loss", f"{stats.avg_win_pct:+.1f}% / {stats.avg_loss_pct:+.1f}%")
        perf.add_row("Profit factor", str(stats.profit_factor or "—"))
        perf.add_row("Best / worst", f"{stats.best_pct:+.0f}% / {stats.worst_pct:+.0f}%")
        perf.add_row("Touched +50%", f"{stats.hit_50} of {stats.closed}")
        perf.add_row("Ran past +100%", str(stats.ran_100))
        perf.add_row("Average hold", f"{stats.avg_hold_min:.0f} min")
        console.print(perf)

        if stats.by_confidence:
            conf = Table(title="Is the confidence score real?")
            conf.add_column("Confidence")
            conf.add_column("Trades", justify="right")
            conf.add_column("Avg return", justify="right")
            for level, (count, avg) in stats.by_confidence.items():
                conf.add_row(str(level), str(count), f"{avg:+.1f}%")
            console.print(conf)

        if stats.by_hour:
            hours = Table(title="Best hours to trade (ET)")
            hours.add_column("Hour")
            hours.add_column("Trades", justify="right")
            hours.add_column("Avg return", justify="right")
            for hour, (count, avg) in stats.by_hour.items():
                hours.add_row(f"{hour:02d}:00", str(count), f"{avg:+.1f}%")
            console.print(hours)

        if stats.by_exit:
            exits = Table(title="How trades ended")
            exits.add_column("Reason")
            exits.add_column("Count", justify="right")
            for reason, count in sorted(stats.by_exit.items(), key=lambda kv: -kv[1]):
                exits.add_row(reason, str(count))
            console.print(exits)

    if stats.warnings:
        console.print(
            Panel(
                "\n".join(f"• {w}" for w in stats.warnings),
                title="⚠ read this before drawing conclusions",
                border_style="yellow",
            )
        )


@app.command()
def memory() -> None:
    """Show what the engine remembers, and what that memory says about it."""
    settings = get_settings()
    store = Memory(settings.data_dir / "memory.db")
    counts = store.counts()

    head = Table(title="Engine Memory", show_header=False, box=None)
    head.add_column(style="cyan", width=26)
    head.add_column(style="bold")
    head.add_row("Database", str(settings.data_dir / "memory.db"))
    head.add_row("Session days recorded", str(counts["session_days"]))
    head.add_row("Trades remembered", f"{counts['closed']} closed of {counts['trades']}")
    head.add_row("Decisions recorded", str(counts["decisions"]))
    console.print(head)

    for column, title in (
        ("regime", "By market regime"),
        ("direction", "By direction"),
        ("exit_reason", "By exit reason"),
    ):
        rows = store.performance_by(column, min_sample=1)
        if not rows:
            continue
        table = Table(title=title)
        table.add_column(column)
        table.add_column("Trades", justify="right")
        table.add_column("Win rate", justify="right")
        table.add_column("Avg return", justify="right")
        for row in rows:
            table.add_row(
                str(row["bucket"]),
                str(row["trades"]),
                f"{row['win_rate']:.0f}%",
                f"{row['avg_return']:+.1f}%",
            )
        console.print(table)

    hours = store.performance_by_hour(min_sample=1)
    if hours:
        table = Table(title="Best hours to trade")
        table.add_column("Session hour")
        table.add_column("Trades", justify="right")
        table.add_column("Win rate", justify="right")
        table.add_column("Avg return", justify="right")
        for row in hours:
            table.add_row(
                row["session_hour"],
                str(row["trades"]),
                f"{row['win_rate']:.0f}%",
                f"{row['avg_return']:+.1f}%",
            )
        console.print(table)

    if counts["closed"] == 0:
        console.print("[dim]nothing learned yet — the engine has no closed trades[/]")


@app.command()
def learn(
    apply: int = typer.Option(None, help="Approve a pending lesson by id and write it into the playbook"),
    reject: int = typer.Option(None, help="Reject a pending lesson by id"),
) -> None:
    """Analyse the trade record and propose playbook amendments for your approval."""
    settings = get_settings()
    store = Memory(settings.data_dir / "memory.db")

    if apply is not None:
        book = apply_lesson(store, load_playbook(settings.playbook_path), apply, settings)
        console.print(f"[green]lesson {apply} applied — playbook is now v{book.version}[/]")
        return

    if reject is not None:
        store.set_lesson_status(reject, "rejected")
        console.print(f"[yellow]lesson {reject} rejected[/]")
        return

    report = analyse(store, settings=settings)
    missed = store.missed_count()
    console.print(
        f"[cyan]analysed {report.total_trades} closed trades "
        f"(baseline {report.baseline_return:+.1f}% per trade) "
        f"| {missed} declined setups priced forward[/]"
    )

    for note in report.notes:
        console.print(f"[dim]• {note}[/]")

    if report.has_findings:
        propose(store, report)

    pending = store.pending_lessons()
    if not pending:
        console.print("[dim]no lessons pending approval[/]")
        return

    for row in pending:
        console.print(
            Panel(
                f"{row['statement']}\n\n[dim]evidence: {row['evidence']}[/]\n"
                f"[dim]sample: {row['sample_size']} trades | confidence: {row['confidence']}[/]",
                title=f"lesson #{row['id']} — pending your approval",
                border_style="cyan",
            )
        )
    console.print(
        "[dim]approve with  qqq learn --apply <id>   |   reject with  qqq learn --reject <id>[/]"
    )


@app.command()
def dashboard(
    port: int = typer.Option(None, help="Overrides DASHBOARD_PORT / PORT for a local run"),
) -> None:
    """Run the admin dashboard standalone — reads the same journal and memory
    the live engine writes, without needing the engine itself running."""
    import uvicorn

    from qqq_alpha.dashboard.app import create_app

    settings = get_settings()
    if not settings.admin_username or not settings.admin_password:
        console.print(
            "[red]ADMIN_USERNAME and ADMIN_PASSWORD must both be set — see .env.example[/]"
        )
        raise typer.Exit(code=1)

    app_ = create_app(settings)
    console.print(f"[green]dashboard on http://0.0.0.0:{port or settings.dashboard_port}[/]")
    uvicorn.run(app_, host="0.0.0.0", port=port or settings.dashboard_port, log_level="warning")  # noqa: S104


@app.command()
def telegram() -> None:
    """Verify the Telegram bot can reach your chat before a session depends on it."""
    settings = get_settings()
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        console.print("[red]TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must both be set[/]")
        raise typer.Exit(code=1)

    ok, message = asyncio.run(
        verify_telegram(settings.telegram_bot_token, settings.telegram_chat_id)
    )
    if ok:
        console.print(f"[green]{message}[/] — check your Telegram for the test message")
    else:
        console.print(f"[red]{message}[/]")
        raise typer.Exit(code=1)


@app.command()
def preflight() -> None:
    """Test every external dependency and report. Run this before anything else."""
    settings = get_settings()
    _setup_logging(settings.log_level)

    report = asyncio.run(run_preflight(settings))
    for check in report.checks:
        colour = "green" if check.ok else ("red" if check.fatal else "yellow")
        console.print(f"[{colour}]{check.icon} {check.name}[/] — {check.detail}")

    console.print()
    if report.all_green:
        console.print("[bold green]كل شيء سليم — جاهز للتشغيل[/]")
    elif report.passed:
        console.print("[yellow]يعمل، لكن راجع التحذيرات[/]")
    else:
        console.print("[bold red]لا يمكن التشغيل — أصلح الأخطاء الحمراء[/]")
        raise typer.Exit(code=1)


@app.command()
def chain(
    dte: int = typer.Option(0, help="0 for today's expiry, 1 for the next session"),
) -> None:
    """Print the live option chain around the money. Verifies the options plan works."""
    settings = get_settings()
    _setup_logging(settings.log_level)

    async def _run() -> None:
        async with MassiveClient(settings) as client:
            bars = await client.minute_bars(settings.primary_symbol, date.today())
        if not bars:
            console.print("[yellow]no bars today (market closed?) — using last known price[/]")

        spot = bars[-1].close if bars else 0.0
        if spot <= 0:
            console.print("[red]cannot determine the underlying price[/]")
            raise typer.Exit(code=1)

        pricer = LiveChainPricer(settings)
        expiry = next_expiry(date.today(), dte)
        if not await pricer.refresh(expiry, force=True):
            console.print(f"[red]chain fetch failed: {pricer.last_error}[/]")
            console.print(
                "[yellow]if this says 'NOT_AUTHORIZED', the options plan does not cover "
                "snapshots — check the subscription tier[/]"
            )
            raise typer.Exit(code=1)

        console.print(
            f"[green]{settings.primary_symbol} @ {spot} — expiry {expiry} — {pricer.status}[/]"
        )

        table = Table(title="Tradeable contracts around the money")
        for column in ("Contract", "Type", "Strike", "Bid", "Ask", "Spread", "Delta", "IV", "Vol", "OI"):
            table.add_column(column, justify="right" if column != "Contract" else "left")

        for row in pricer.chain_context(spot, count=5):
            table.add_row(
                str(row["symbol"]),
                str(row["type"]),
                f"{row['strike']:.0f}",
                f"{row['bid']:.2f}" if row["bid"] else "—",
                f"{row['ask']:.2f}" if row["ask"] else "—",
                f"{row['spread_pct']:.1f}%" if row["spread_pct"] is not None else "—",
                f"{row['delta']:.2f}" if row["delta"] is not None else "—",
                f"{row['iv']:.2f}" if row["iv"] is not None else "—",
                str(row["volume"]),
                str(row["open_interest"]),
            )
        console.print(table)
        console.print(
            "[dim]entries fill at the ask, exits at the bid — the spread is paid twice[/]"
        )

    asyncio.run(_run())


@app.command()
def check() -> None:
    """Connect to the live feed for 60 seconds and report what actually arrives.

    Run this before trusting a subscription: it proves the key works, the plan
    covers the channels we need, and bars are shaped the way we expect.
    """
    settings = get_settings()
    _setup_logging(settings.log_level)

    async def _run() -> None:
        stream = LiveBarStream(settings, [settings.primary_symbol])
        console.print(f"[cyan]listening for {settings.primary_symbol} bars (60s)…[/]")
        try:
            bars = await drain(stream, seconds=60)
        except StreamAuthError as exc:
            console.print(f"[red]authentication failed: {exc}[/]")
            raise typer.Exit(code=1) from exc

        if not bars:
            console.print(
                "[yellow]connected, but no bars arrived.[/]\n"
                "If the market is open this means the plan does not include the "
                "minute-aggregate channel. If it is closed, that is expected."
            )
            return

        console.print(f"[green]{len(bars)} bars received[/]")
        for bar in bars[-5:]:
            console.print(
                f"  {bar.ts.astimezone(MARKET_TZ).strftime('%H:%M')} "
                f"O={bar.open} H={bar.high} L={bar.low} C={bar.close} "
                f"V={bar.volume} VWAP={bar.vwap} trades={bar.transactions}"
            )

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
