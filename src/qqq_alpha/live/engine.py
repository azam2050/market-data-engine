"""The live engine.

Drives the identical objects the backtester drives — snapshot builder, attention
engine, safety rails, decider, trade manager. Nothing about the decision logic
knows whether it is replaying March or watching today, which is the only way a
backtest result means anything about production.

What is different here is everything around the decision: warm-starting from
REST so indicators are usable from the first live bar rather than an hour later,
detecting a dead feed, resetting cleanly at each session boundary, and refusing
to act on data it knows is stale.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from qqq_alpha.brain.attention import AttentionEngine
from qqq_alpha.brain.decider import Decider, next_expiry, occ_symbol
from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.brain.rails import DayState, SafetyRails, infeasible
from qqq_alpha.brain.tutor import ConceptTutor
from qqq_alpha.config import MARKET_TZ, REGULAR_CLOSE, REGULAR_OPEN, Settings
from qqq_alpha.data.backfill import GapRepairer
from qqq_alpha.data.calendar import todays_events
from qqq_alpha.data.chain import LiveChainPricer
from qqq_alpha.data.health import assess
from qqq_alpha.data.massive import MassiveClient
from qqq_alpha.data.pricing import BlackScholesPricer, OptionPricer
from qqq_alpha.data.pulse import PulseTracker, chain_pulse
from qqq_alpha.data.quality import inspect_session
from qqq_alpha.domain import (
    Action,
    Bar,
    Decision,
    MarketSnapshot,
    MissedOpportunity,
    OptionType,
    Trade,
    TradeStatus,
)
from qqq_alpha.execution import ExecutionRouter, OrderRequest, Side, build_broker
from qqq_alpha.execution.sizing import size_order, split_for_scale_out
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal
from qqq_alpha.learning import analyse, propose, with_applied_lessons
from qqq_alpha.learning import apply_lesson as apply_pending_lesson
from qqq_alpha.live.channel import ChannelPublisher
from qqq_alpha.live.flowfeed import LiveFlowFeed
from qqq_alpha.live.notifier import ConsoleNotifier, Notifier, human_contract
from qqq_alpha.live.preflight import run_preflight
from qqq_alpha.live.review import load_period, review
from qqq_alpha.live.shadow import ShadowStockDesk
from qqq_alpha.live.state import SessionState, StateStore
from qqq_alpha.live.stream import LiveBarStream
from qqq_alpha.live.telegram import TelegramCommandListener
from qqq_alpha.memory import Memory
from qqq_alpha.trades import TradeManager, recommended_size_factor

log = logging.getLogger(__name__)

WARMUP_BARS = 30
# how long to wait after a decline before pricing forward what was missed —
# long enough to catch the move, short enough to resolve within the session
MISSED_LOOKAHEAD_MINUTES = 60
# a lone bad bar is skipped and logged; this many in a row is not a glitch,
# it is a broken engine that should stop loudly instead of trading blind
MAX_CONSECUTIVE_BAR_FAILURES = 10
# how many prior sessions to keep for the hourly chart, and how far back to
# search for them. Five sessions give roughly 35 hourly candles — a real
# chart — and ten calendar days covers a long weekend plus a holiday.
HISTORY_SESSIONS = 5
HISTORY_SEARCH_DAYS = 10
# minute bars arrive every 60s during the session. Five quiet minutes is a
# real outage, not a slow tick — long enough not to fire on a reconnect,
# short enough that the operator hears about it while it still matters.
TAPE_SILENCE_ALERT_SEC = 300


@dataclass
class LiveStatus:
    """Everything needed to answer 'is the engine healthy?' without reading logs."""

    started_at: datetime | None = None
    session_day: date | None = None
    bars_received: int = 0
    last_bar_at: datetime | None = None
    brain_calls: int = 0
    signals_sent: int = 0
    open_positions: int = 0
    trades_today: int = 0
    realized_pct: float = 0.0
    last_error: str | None = None
    reconnects: int = 0
    warm_started: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "started_at": self.started_at,
            "session_day": self.session_day,
            "bars_received": self.bars_received,
            "last_bar_at": self.last_bar_at,
            "brain_calls": self.brain_calls,
            "signals_sent": self.signals_sent,
            "open_positions": self.open_positions,
            "trades_today": self.trades_today,
            "realized_pct": self.realized_pct,
            "reconnects": self.reconnects,
            "warm_started": self.warm_started,
            "last_error": self.last_error,
        }


@dataclass
class LiveEngine:
    settings: Settings
    decider: Decider
    pricer: OptionPricer
    playbook: Playbook
    journal: Journal
    notifier: Notifier = field(default_factory=ConsoleNotifier)
    dry_run: bool = True
    """When true the engine publishes signals but never treats them as taken
    positions worth reporting as a track record. Shadow mode is the default
    because an untested engine should not be able to claim a record."""

    def __post_init__(self) -> None:
        self.builder = SnapshotBuilder(self.settings.primary_symbol)
        self.rails = SafetyRails(self.settings)
        self.attention = AttentionEngine(
            self.settings.attention_threshold, self.settings.attention_cooldown_sec
        )
        self.manager = TradeManager()
        self.status = LiveStatus()
        self.session_bars: list[Bar] = []
        # the reference levels every desk has drawn before the bell: yesterday's
        # candle (which also yields the classic pivot, R1 and S1) and the
        # overnight range. The backtester has always passed these; the live
        # engine never did, so live ran blind to the most-watched levels of the
        # day and quietly disagreed with its own backtest.
        self.prior_day: Bar | None = None
        self.overnight_high: float | None = None
        self.overnight_low: float | None = None
        # each heavyweight's yesterday close, so its day change is the number
        # every screen quotes rather than "since the engine started watching"
        self.leader_prior_close: dict[str, float] = {}
        # minute bars from the previous sessions, kept only to build the
        # hourly chart: one day yields six and a half hourly candles, which
        # is not enough for an EMA, a swing high, or a trend
        self.history_bars: list[Bar] = []
        self.leader_bars: dict[str, list[Bar]] = {}
        self.recent_trades: list[Trade] = []
        self._current_day: date | None = None
        self.store = StateStore(self.settings.journal_dir / "session-state.json")
        # long-term memory lives on disk, so a restart never costs the engine
        # what it has learned — unlike the in-process list it replaced
        self.memory = Memory(self.settings.data_dir / "memory.db")
        # operator-approved lessons live in durable memory, not in the seed
        # playbook file (which is wiped on every redeploy) — compose them in
        self.playbook = with_applied_lessons(self.playbook, self.memory)
        # declined setups awaiting a look-back price check. self.pricer is often
        # a LiveChainPricer, which only knows the *current* quote — it cannot
        # answer "what was this contract worth 20 minutes ago", so the
        # retrospective check below always uses a time-aware model instead
        self._pending_missed: list[dict] = []
        self._attribution_pricer = BlackScholesPricer()
        # today's decisions, shown back to the brain for plan continuity —
        # in-process only; a mid-session restart starts the thread afresh
        self._today_decisions: list[Decision] = []
        # lets the operator approve or reject a proposed lesson by replying
        # to Telegram — there is no assumption anywhere else in this project
        # that the operator has a terminal, and lesson approval should not be
        # the one place that breaks that
        self.commands = (
            TelegramCommandListener(self.settings.telegram_bot_token, self.settings.telegram_chat_id)
            if self.settings.telegram_bot_token and self.settings.telegram_chat_id
            else None
        )
        self._command_task: asyncio.Task | None = None
        self._dashboard_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None
        # the door to a real broker. Disarmed unless EXECUTION_ENABLED is on
        # *and* a broker is configured, and it journals every intent either
        # way — so the order file fills with what would have been sent long
        # before anything is, and the first live day is a file you recognise.
        self.execution = ExecutionRouter(
            self.settings,
            broker=build_broker(self.settings),
            journal=self.journal,
            on_alert=self.notifier.note,
        )
        # contracts actually held per trade, decremented as the exit engine
        # banks part of a position. Persisted with the session, because a
        # recovered position whose size was forgotten cannot be sold: the
        # engine would either dump more than it holds or leave a leg behind.
        self._executed: dict[str, int] = {}
        # asks the provider for minutes the websocket dropped. The stream is
        # fast and lossy; this is the slow, reliable path back to the same bars
        self.repairer = GapRepairer(self.settings)
        # "price of the day": where options money is concentrating, for QQQ and
        # the leaders — context even when the tape itself is quiet
        self.pulse = PulseTracker(self.settings)
        # the real tape: near-the-money prints classified into sweeps/blocks.
        # Only meaningful with a live chain; disables itself if the plan
        # turns out not to cover the trades endpoint
        self.flow_feed = (
            LiveFlowFeed(self.settings, self.pricer)
            if isinstance(self.pricer, LiveChainPricer)
            else None
        )
        # the expansion candidates, learning in the background on the leader
        # bars this engine already receives — simulated, journal-only, and
        # entirely absent from subscriber-facing output
        self.shadow = (
            ShadowStockDesk(self.settings, self.decider, self.playbook)
            if self.settings.shadow_symbols
            else None
        )
        # the public channel: two live shares a week, daily and weekly
        # reports, and the education series — all best-effort, never blocking
        self.channel = (
            ChannelPublisher(self.settings.telegram_bot_token, self.settings.telegram_channel_id)
            if self.settings.telegram_bot_token and self.settings.telegram_channel_id
            else None
        )
        # the same after-the-bell reports, aimed at the private channel. The
        # subscribers were getting the live cards but not the daily or weekly
        # scoreboard — the two posts that show the record honestly, losses
        # included — so the paying room saw less accounting than the free one.
        # Only the report methods are ever called on this publisher: the live
        # shares and the education series stay a public-channel affair.
        self.private_channel = (
            ChannelPublisher(
                self.settings.telegram_bot_token,
                self.settings.telegram_private_channel_id,
            )
            if self.settings.telegram_bot_token
            and self.settings.telegram_private_channel_id
            else None
        )
        self._channel_daily_posted: date | None = None
        # the daily concept lesson: Claude teaching one option-market idea a
        # day, gated so it can never leak advice, prices, or this desk's own
        # management rules. None when disabled or when no API key is set.
        self.tutor = (
            ConceptTutor(self.settings)
            if self.settings.daily_lesson_enabled and self.settings.anthropic_api_key
            else None
        )
        # which session's data-health verdict has already been sent
        self._health_reported: date | None = None
        # blue under-watch cards published today — capped so a choppy session
        # cannot turn the watch card into noise
        self._watch_shared_today = 0
        # the circuit breaker stops the desk for the rest of the day. That is a
        # legitimate rail, but it used to happen in total silence — the
        # operator's only symptom was a dashboard that stopped updating. It
        # gets announced once per day now.
        self._breaker_announced: date | None = None
        # whether a market-data outage is currently being reported, so the
        # watchdog speaks on the edges instead of once a minute
        self._tape_alerted = False

    # ------------------------------------------------------------------
    def _persist(self) -> None:
        """Snapshot the session to disk. Called after anything that changes it."""
        self.store.save(
            SessionState(
                session_day=self._current_day,
                trades_today=self.status.trades_today,
                realized_pct=self.manager.realized_return_pct,
                signals_sent=self.status.signals_sent,
                brain_calls=self.status.brain_calls,
                open_trades=list(self.manager.open_trades),
                closed_trades=list(self.manager.closed_trades),
                executed=dict(self._executed),
                channel_daily_posted=(self._channel_daily_posted == self._current_day),
                health_reported=(self._health_reported == self._current_day),
                breaker_announced=(self._breaker_announced == self._current_day),
            )
        )

    async def _restore(self) -> None:
        """Resume an interrupted session so open positions are never orphaned."""
        today = datetime.now(MARKET_TZ).date()
        state = self.store.load(expected_day=today)
        if state is None:
            return

        self.manager.open_trades = list(state.open_trades)
        self.manager.closed_trades = list(state.closed_trades)
        self.status.trades_today = state.trades_today
        self.status.signals_sent = state.signals_sent
        self.status.brain_calls = state.brain_calls
        self.status.open_positions = len(state.open_trades)
        self._current_day = state.session_day
        # the sizes come back with the positions: a recovered trade whose
        # contract count was lost cannot be sold correctly, and reconstructing
        # it from a budget and a price that has since moved would be a guess
        # wearing the shape of a fact
        self._executed = dict(state.executed)
        # a restart after the bell must not repeat the daily report — the
        # attempt already happened in a previous process lifetime
        if state.channel_daily_posted:
            self._channel_daily_posted = state.session_day
        if state.health_reported:
            self._health_reported = state.session_day
        if state.breaker_announced:
            self._breaker_announced = state.session_day

        if state.open_trades:
            symbols = ", ".join(t.occ_symbol for t in state.open_trades)
            await self.notifier.note(
                f"♻️ resumed session {state.session_day}: "
                f"{len(state.open_trades)} open position(s) restored — {symbols}"
            )
        else:
            await self.notifier.note(f"♻️ resumed session {state.session_day} (flat)")

    # ------------------------------------------------------------------
    async def run(self) -> None:
        self.status.started_at = datetime.now(UTC)
        stream = LiveBarStream(self.settings, self.settings.tracked_symbols)

        await self.notifier.note(
            f"engine starting | feed={self.settings.massive_feed_mode} "
            f"| mode={'shadow' if self.dry_run else 'live'}"
        )
        if stream.is_delayed:
            await self.notifier.note(
                "⚠️ delayed feed: signals are for validation only, not execution"
            )

        # test every external dependency and report to the operator's phone.
        # a container has no terminal; this is how they learn it is healthy.
        report = await run_preflight(self.settings)
        await self.notifier.note(report.as_message())
        if not report.passed:
            self.status.last_error = "preflight failed"
            log.error("preflight failed; refusing to start")
            return

        if self.commands is not None:
            self._command_task = asyncio.create_task(self._command_loop())
            note = 'lesson approval is live — reply "موافق <رقم>" or "رفض <رقم>"'
            if self.settings.trial_days > 0:
                # doubles as visible proof of which build is running: this
                # line only exists in versions that accept subscribers
                note += (
                    f"\n🎁 التسجيل التجريبي مفعّل: أي شخص يضغط Start "
                    f"يحصل على {self.settings.trial_days} يوماً مجاناً"
                )
            await self.notifier.note(note)

        if self.settings.admin_username and self.settings.admin_password:
            self._dashboard_task = asyncio.create_task(self._run_dashboard())
            await self.notifier.note(
                f"📊 لوحة التحكم شغّالة على المنفذ {self.settings.dashboard_port}"
            )

        if self.channel is not None:
            await self.notifier.note(
                f"📢 النشر في القناة مفعّل: {self.settings.telegram_channel_id} — "
                "دراستا حالة أسبوعيًا + تقرير يومي وأسبوعي ومنهج تعليمي"
            )
        if self.settings.telegram_private_channel_id:
            await self.notifier.note(
                "🔒 قناة المشتركين الخاصة مفعّلة — دراسات الحالة تُنشر فيها منشورًا "
                "واحدًا، والتقرير اليومي والأسبوعي والشهري يصل إليها كما يصل "
                "للقناة العامة، والانضمام بطلب يوافق عليه البوت آليًا، "
                "والمنتهون يُخرَجون تلقائيًا"
            )
        await self._report_channel_health()
        # from here on, a market-data outage is announced instead of silent
        self._watchdog_task = asyncio.create_task(self._watch_the_tape())

        await self._restore()
        # after _restore, because reconciliation compares the broker's holdings
        # against the book we just recovered from disk
        await self._announce_execution_mode()
        await self._reconcile_positions()
        await self._expire_subscribers()
        self._refresh_recent()
        await self._warm_start()

        # one bad bar must not kill a desk with open positions: the failure is
        # logged and the next bar gets a clean attempt. Only a persistent
        # streak — a systemic problem, not a glitch — stops the engine, and a
        # restart-per-crash loop (as produced by the RecalledTrade bug) is
        # strictly worse than skipping a minute.
        consecutive_failures = 0
        try:
            async for bar in stream.bars():
                self.status.reconnects = stream.reconnects
                try:
                    await self._on_bar(bar)
                    consecutive_failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    consecutive_failures += 1
                    self.status.last_error = f"bar handling: {exc}"
                    log.exception(
                        "bar handling failed (%d in a row)", consecutive_failures
                    )
                    if consecutive_failures == 1:
                        # first of a streak only — a broken minute is worth one
                        # message, not one per minute
                        await self.notifier.note(
                            f"⚠️ خطأ في معالجة شمعة — المحرك مستمر ({exc})"
                        )
                    if consecutive_failures >= MAX_CONSECUTIVE_BAR_FAILURES:
                        raise
        except asyncio.CancelledError:
            await self._shutdown()
            raise
        except Exception as exc:
            self.status.last_error = str(exc)
            log.exception("live engine stopped")
            await self.notifier.note(f"engine stopped: {exc}")
            raise

    # ------------------------------------------------------------------
    async def _warm_start(self) -> None:
        """Backfill today's session so the first live bar is already actionable.

        Without this the engine spends the first 30+ minutes of the session
        unable to compute an EMA, which is exactly the window where the best
        setups appear.
        """
        if not self.settings.massive_api_key:
            await self.notifier.note("no data key — skipping warm start")
            return

        today = datetime.now(MARKET_TZ).date()
        try:
            async with MassiveClient(self.settings) as client:
                session = await client.session(self.settings.primary_symbol, today)
                if session.regular:
                    self.session_bars = list(session.regular)
                    self._current_day = today
                    self.status.warm_started = True
                    await self.notifier.note(
                        f"warm start: {len(self.session_bars)} bars restored "
                        f"({session.quality.summary() if session.quality else 'no verdict'})"
                    )
                # pre-market is fetched anyway as part of the same day; its
                # high and low are among the most-watched levels of the session
                self.overnight_high = session.premarket_high
                self.overnight_low = session.premarket_low
                await self._load_prior_day(client, today)

                for symbol in self.settings.leader_symbols:
                    leader = await client.session(symbol, today)
                    if leader.regular:
                        self.leader_bars[symbol] = list(leader.regular)
                        if self.shadow is not None:
                            self.shadow.seed(symbol, list(leader.regular))
        except Exception as exc:
            # a failed warm start degrades the engine, it does not stop it
            self.status.last_error = f"warm_start_failed: {exc}"
            log.warning("warm start failed: %s", exc)
            await self.notifier.note(f"warm start failed ({exc}); starting cold")

    async def _load_prior_day(self, client, today: date) -> None:
        """Yesterday's candle — walking back past weekends and holidays.

        A market holiday returns an empty session rather than an error, so the
        only honest way to find the previous *trading* day is to walk backwards
        until a day has bars. Five attempts covers a long weekend plus a
        holiday; failing that, the levels stay absent rather than wrong.

        Once the day is identified, the heavyweights' closes for that same day
        are fetched too, so every leader's day change is measured against the
        same session the index is.
        """
        probe = today
        collected: list[list[Bar]] = []
        for _ in range(HISTORY_SEARCH_DAYS):
            if len(collected) >= HISTORY_SESSIONS:
                break
            probe -= timedelta(days=1)
            if probe.weekday() >= 5:
                continue
            try:
                previous = await client.session(self.settings.primary_symbol, probe)
            except Exception as exc:  # noqa: BLE001 - levels are context, not a blocker
                log.warning("history fetch failed for %s: %s", probe, exc)
                break
            daily = previous.daily_bar
            if daily is None:
                continue  # a holiday returns an empty session, not an error
            collected.append(list(previous.regular))
            if len(collected) == 1:
                # the first session found walking back IS yesterday — and it is
                # reassigned every call, so a session roll refreshes it rather
                # than keeping the day before last
                self.prior_day = daily
                log.info(
                    "prior day %s: H %.2f L %.2f C %.2f",
                    probe, daily.high, daily.low, daily.close,
                )
                await self._load_leader_closes(client, probe)

        # oldest first, so the hourly resample reads chronologically
        self.history_bars = [bar for session in reversed(collected) for bar in session]
        if not collected:
            log.warning("no prior trading day found in the days before %s", today)

    async def _load_leader_closes(self, client, day: date) -> None:
        """Yesterday's close for each heavyweight. Best-effort, one at a time:
        a leader that fails simply has no day change rather than a wrong one."""
        for symbol in self.settings.leader_symbols:
            try:
                session = await client.session(symbol, day)
            except Exception as exc:  # noqa: BLE001
                log.warning("prior close fetch failed for %s: %s", symbol, exc)
                continue
            daily = session.daily_bar
            if daily is not None:
                self.leader_prior_close[symbol] = daily.close

    # ------------------------------------------------------------------
    async def _on_bar(self, bar: Bar) -> None:
        self.status.bars_received += 1
        self.status.last_bar_at = datetime.now(UTC)

        local_day = bar.ts.astimezone(MARKET_TZ).date()
        if self._current_day is not None and local_day != self._current_day:
            await self._roll_session(local_day)
        self._current_day = local_day
        self.status.session_day = local_day

        if bar.symbol != self.settings.primary_symbol:
            self.leader_bars.setdefault(bar.symbol, []).append(bar)
            if self.shadow is not None:
                # shadow failures must never cost a QQQ bar — the live desk
                # outranks the learner in every conflict
                try:
                    await self.shadow.on_bar(bar)
                except Exception:  # noqa: BLE001
                    log.exception("shadow desk failed on %s bar", bar.symbol)
            return

        self.session_bars.append(bar)
        if len(self.session_bars) < WARMUP_BARS:
            return

        # close any hole the stream left before anything reads the series:
        # marking a position or building a snapshot on a gapped history is
        # exactly the mistake this repairs
        self.session_bars = await self.repairer.repair(
            self.settings.primary_symbol, self.session_bars, now=bar.ts
        )

        await self._refresh_chain(bar)
        await self._mark_open_positions(bar)
        await self._maybe_decide(bar)
        self._resolve_pending_missed(bar)
        await self._close_if_session_over(bar)

    async def _mark_open_positions(self, bar: Bar) -> None:
        for trade in list(self.manager.open_trades):
            # mark at the bid: that is what closing the position would actually fetch
            price = self.pricer.price_at(trade.occ_symbol, bar.ts, bar.close, side="exit")
            if price is None:
                continue
            # the thesis stop outranks price P&L: the brain named the spot level
            # where its idea is wrong, and the underlying just crossed it
            if self.manager.check_thesis(trade, bar.close):
                update = self.manager.force_close(trade, price, bar.ts, "thesis_invalidated")
            else:
                update = self.manager.update(trade, price, bar.ts)
            if update is not None:
                await self.notifier.update(trade, update, self._delayed)
                if trade.shared_to_channel and self.channel is not None:
                    await self.channel.post_trade_update(trade, update, self._delayed)
                self.journal.log_trade(trade)
                self.memory.remember_trade(trade)
                self._recall_after_close(trade)
                self._persist()
                # a scale-out banks half WITHOUT closing the trade, so it needs
                # its own branch: the exit path below only ever fires on a
                # position the engine has finished with
                if update.note.startswith("scale_out"):
                    await self._execute_scale_out(trade)
                else:
                    await self._execute_exit(trade)

        self.status.open_positions = len(self.manager.open_trades)
        self.status.realized_pct = self.manager.realized_return_pct

    async def _maybe_decide(self, bar: Bar) -> None:
        # the tape is polled before the snapshot is built because attention
        # wakes on flow urgency — a sweep barrage must be able to trigger a
        # look even when price itself is quiet
        flow_events: list | None = None
        if self.flow_feed is not None and not self.flow_feed.disabled:
            flow_events = await self.flow_feed.poll(bar.ts, bar.close)

        quality = inspect_session(self.session_bars)
        snapshot = self.builder.build(
            session_bars=self.session_bars,
            leader_bars=self.leader_bars or None,
            flow_events=flow_events,
            prior_day=self.prior_day,
            overnight_high=self.overnight_high,
            overnight_low=self.overnight_low,
            leader_prior_close=self.leader_prior_close,
            history_bars=self.history_bars,
            leader_priority=self.settings.leader_symbols,
            now=bar.ts,
            quality=quality,
        )

        verdict = self.attention.evaluate(snapshot)
        self.journal.log_attention(
            bar.ts, verdict.score, verdict.should_wake, verdict.summary, verdict.suppressed_by
        )
        if not verdict.should_wake:
            return

        state = DayState(
            trades_taken=self.status.trades_today,
            open_positions=len(self.manager.open_trades),
            realized_return_pct=self.manager.realized_return_pct,
            realized_risk_pct=self.manager.realized_risk_pct,
        )
        pre = self.rails.pre_check(snapshot, state)
        if not pre.allowed:
            log.debug("rails blocked: %s", pre.blocks)
            await self._announce_circuit_breaker(pre.blocks, state)
            self._queue_missed_check(snapshot, pre.blocks)
            return

        decision = await self.decider.decide(
            snapshot=snapshot,
            playbook=self.playbook,
            open_trades=self.manager.open_trades,
            recent_trades=self.recent_trades,
            rail_warnings=pre.warnings,
            attention_note=verdict.summary,
            similar_trades=self.memory.similar_trades(snapshot, limit=8),
            chain=(
                self.pricer.chain_context(bar.close)
                if isinstance(self.pricer, LiveChainPricer)
                else None
            ),
            options_pulse=await self._options_pulse(bar),
            recent_decisions=self._today_decisions[-4:],
            calendar_events=todays_events(bar.ts),
        )
        self.status.brain_calls += 1
        # the lock reads the decisions made BEFORE this one: an entry is judged
        # against what the brain promised earlier, never against itself
        lock = self.rails.commitment_check(decision, snapshot, self._today_decisions)
        # shown back to the brain on later wakes so an announced plan is
        # followed through (or explicitly revised), not silently re-derived
        self._today_decisions.append(decision)

        # with a live chain we can validate the real contract: spread, liquidity,
        # whether it exists at all
        contract = (
            self.pricer.contract(decision.occ_symbol or "")
            if isinstance(self.pricer, LiveChainPricer)
            else None
        )
        post = self.rails.post_check(decision, contract)
        blocks = post.blocks + lock.blocks
        warnings = pre.warnings + post.warnings + lock.warnings
        self.journal.log_decision(decision, snapshot, blocks, warnings, verdict.score)
        self.memory.remember_decision(decision, snapshot, verdict.score, blocks)
        await self._maybe_publish_watch(decision, snapshot)

        if decision.action is not Action.ENTER or blocks:
            if decision.action is Action.ENTER:
                await self.notifier.note(f"entry blocked by rails: {blocks}")
                # the operator asked for this one by name, so it says in his
                # language what was promised and what the tape actually did
                if lock.blocks:
                    await self.notifier.note(
                        "🔒 مُنِع الدخول: البوت أعلن مستوى دخول ولم يتحقق بعد.\n"
                        f"{lock.blocks[0]}"
                    )
            self._queue_missed_check(
                snapshot, blocks if decision.action is Action.ENTER else []
            )
            return

        # you pay the offer; pricing an entry at the mid invents profit that
        # will not exist when the trade is actually taken
        fill = self.pricer.price_at(decision.occ_symbol or "", bar.ts, bar.close, side="entry")
        if fill is None or fill <= 0:
            await self.notifier.note(f"could not price {decision.occ_symbol}; signal dropped")
            return

        decision.size_factor = self._size_factor(decision, bar.ts)
        trade = self.manager.open_trade(decision, fill, snapshot)
        self.status.trades_today += 1
        self.status.signals_sent += 1
        # is this the week's live public share? First trade of a randomly
        # chosen share day — flagged before persisting so a restart mid-trade
        # keeps following it in the channel
        if self.channel is not None:
            local_day = bar.ts.astimezone(MARKET_TZ).date()
            already_shared = any(
                t.shared_to_channel
                for t in (*self.manager.open_trades, *self.manager.closed_trades)
            )
            if self.channel.is_share_day(local_day) and not already_shared:
                trade.shared_to_channel = True
        self.journal.log_trade(trade)
        self.memory.remember_trade(trade, snapshot)
        # persist before publishing: a crash between the two must leave the
        # position recoverable, never announced-but-forgotten
        self._persist()
        # the order goes out after the position is durably recorded: a crash
        # between the two must leave a trade we can find, never a fill we
        # cannot explain
        await self._execute_entry(trade)
        await self.notifier.signal(trade, self._delayed)
        if trade.shared_to_channel and self.channel is not None:
            await self.channel.post_trade_entry(trade, self._delayed)

    # ------------------------------------------------------------------
    async def _maybe_publish_watch(self, decision: Decision, snapshot: MarketSnapshot) -> None:
        """The blue "under watch" card, for a qualified WAIT.

        Fires only when the brain named a specific condition it is waiting
        for at confidence 6+, at most twice a day: the watch card is a
        promise of discipline, and promises lose value when spammed. Private
        channel always; the public channel only on live-share days.
        """
        if decision.action is not Action.WAIT or decision.confidence < 6:
            return
        if self._watch_shared_today >= 2:
            return
        condition = (decision.invalidation or "").strip() or (decision.thesis or "").strip()
        if not condition or abs(snapshot.net_bias) < 0.2:
            return
        direction_hint = "صعود CALL" if snapshot.net_bias > 0 else "هبوط PUT"

        png: bytes | None = None
        try:
            from qqq_alpha.live import cards

            png = cards.render_watch_card(
                snapshot.underlying.symbol,
                direction_hint,
                condition[:180],
                decision.confidence,
                snapshot.ts,
                level=decision.invalidation_level,
            )
        except Exception:  # noqa: BLE001 - a drawing bug must never cost a wake
            log.exception("watch card rendering failed")

        from qqq_alpha.live.notifier import DISCLAIMER

        text = (
            "🔵 حالة قيد التكوّن — لم تصدر دراستها بعد\n"
            f"الاتجاه المحتمل: {direction_hint}\n"
            f"الشرط المنتظر: {condition[:300]}\n"
            f"قوة الإشارة حتى الآن: {decision.confidence}/10\n"
            "إذا اكتمل الشرط تصدر دراسة الحالة كاملة — وإذا لم يكتمل فلن يصدر شيء.\n"
            f"⚠️ {DISCLAIMER}"
        )
        await self.notifier.watch(png, text)
        if self.channel is not None and self.channel.is_share_day(
            snapshot.ts.astimezone(MARKET_TZ).date()
        ):
            await self.channel.post_watch(png, text)
        self._watch_shared_today += 1

    # ------------------------------------------------------------------
    async def _watch_the_tape(self) -> None:
        """Notice when the bars stop arriving, and say so.

        Every outage the engine can suffer while still running looks identical
        from outside: no cards, no notes, nothing. The stream already
        reconnects on its own with backoff, so a provider hiccup heals without
        help — but a long one used to pass in complete silence, and silence is
        the one thing an operator with paying subscribers cannot accept. This
        loop watches the clock against the last bar during market hours and
        reports both the outage and the recovery.
        """
        while True:
            await asyncio.sleep(60)
            with contextlib.suppress(Exception):
                await self._tape_tick(datetime.now(MARKET_TZ))

    async def _tape_tick(self, now: datetime) -> None:
        """One watchdog evaluation. Alerts on the edges only — an outage is
        announced once when it starts and once when it clears, never every
        minute in between."""
        in_session = now.weekday() < 5 and REGULAR_OPEN <= now.time() <= REGULAR_CLOSE
        last_bar = self.status.last_bar_at
        age = (
            (now.astimezone(UTC) - last_bar).total_seconds()
            if last_bar is not None
            else None
        )
        if not in_session or age is None:
            self._tape_alerted = False
            return

        if age >= TAPE_SILENCE_ALERT_SEC and not self._tape_alerted:
            self._tape_alerted = True
            await self.notifier.note(
                f"⚠️ انقطاع في بيانات السوق — لم تصل أي شمعة منذ {age / 60:.0f} دقيقة\n"
                f"محاولات إعادة الاتصال: {self.status.reconnects}\n"
                "المحرك يعيد الاتصال تلقائيًا. لن تصدر أي دراسة حالة جديدة حتى تعود "
                "البيانات، والصفقات المفتوحة لا تُدار بدون أسعار."
            )
        elif age < TAPE_SILENCE_ALERT_SEC and self._tape_alerted:
            self._tape_alerted = False
            await self.notifier.note("✅ عادت بيانات السوق — المحرك يعمل بشكل طبيعي")

    # ------------------------------------------------------------------
    def _telegram(self):
        """The Telegram notifier actually in use, wherever it is wrapped.

        In production ``self.notifier`` is a FanoutNotifier holding a console
        notifier and a BroadcastNotifier — so an ``isinstance`` check against
        TelegramNotifier is False, and any diagnostic guarded by one becomes a
        silent no-op on the exact deployment it was written for. Unwrap once,
        here, instead of getting this wrong at every call site.
        """
        from qqq_alpha.live.telegram import TelegramNotifier

        candidates = [self.notifier, *getattr(self.notifier, "notifiers", [])]
        for candidate in candidates:
            if isinstance(candidate, TelegramNotifier):
                return candidate
        return None

    # ------------------------------------------------------------------
    async def _report_channel_health(self) -> str:
        """Tell the operator, at boot, where the cards will actually land.

        The operator's report that "the bot posts to me instead of the
        channel" could not be answered from the outside: in private-channel
        mode they receive the text audit copy either way, so a channel that
        silently rejects every photo looks exactly like a healthy one. This
        asks Telegram which is true and puts the answer on their phone before
        the session starts. Returns the private channel's verdict so فحص can
        state one conclusion instead of two lines that may disagree.
        """
        telegram = self._telegram()
        if telegram is None:
            return ""
        targets = [
            (
                "قناة المشتركين الخاصة",
                self.settings.telegram_private_channel_id,
                # the single most likely cause of "the cards come to me instead
                # of the channel", and previously indistinguishable from every
                # other cause: name it outright
                "غير مُعدّة — المتغير TELEGRAM_PRIVATE_CHANNEL_ID فارغ، ولهذا "
                "تصل البطاقات إلى محادثة البوت بدل القناة",
            ),
            (
                "القناة العامة",
                self.settings.telegram_channel_id,
                # a different fact entirely: nothing is misrouted, the public
                # package is simply switched off
                "غير مُعدّة — النشر العام معطّل (لا تقارير ولا دراسات حالة للجمهور)",
            ),
        ]
        lines: list[str] = []
        private_verdict = ""
        for index, (label, chat_id, unset_note) in enumerate(targets):
            if not chat_id:
                verdict = f"❌ {unset_note}"
                lines.append(f"{label}: {verdict}")
            else:
                try:
                    # the public channel only ever posts; the private one also
                    # edits living cards, issues invite links and removes
                    # expired subscribers, so it needs the full set
                    verdict = await telegram.check_channel(
                        str(chat_id), full_rights=(index == 0)
                    )
                except Exception as exc:  # noqa: BLE001 - diagnostics never block a start
                    log.exception("channel health check failed for %s", chat_id)
                    verdict = f"⚠️ تعذّر الفحص ({exc})"
                lines.append(f"{label} ({chat_id}): {verdict}")
            if index == 0:
                private_verdict = verdict
        await self.notifier.note("🔎 فحص قنوات النشر\n" + "\n".join(lines))
        return private_verdict

    # ------------------------------------------------------------------
    async def _announce_circuit_breaker(self, blocks: list[str], state: DayState) -> None:
        """Say it out loud, once, the day the desk closes itself.

        A tripped breaker means no further wake produces a decision for the
        rest of the session. Until now that was invisible: no message, no
        dashboard row, nothing — the operator watched a bot that had quietly
        stopped looking at the market and had no way to know why.
        """
        if not any(block.startswith("circuit_breaker") for block in blocks):
            return
        today = datetime.now(MARKET_TZ).date()
        if self._breaker_announced == today:
            return
        self._breaker_announced = today
        self._persist()
        await self.notifier.note(
            "🛑 قاطع الخسارة اليومي فُعِّل — أُغلق المكتب لبقية الجلسة\n"
            f"خسارة اليوم بعد وزن الحجم: {state.loss_measure_pct:+.1f}% "
            f"(الحد {-abs(self.settings.daily_loss_circuit_breaker_pct):.0f}%)\n"
            f"الرقم الخام على العقود: {state.realized_return_pct:+.1f}%\n"
            "لن تُفتح أي حالة جديدة حتى جلسة الغد. هذا قرار حماية رأس مال، "
            "وليس عطلًا في النظام."
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _size_factor(decision: Decision, ts: datetime) -> float:
        """Delegates to the shared sizing arithmetic in trades.py — the shadow
        stock desk uses the identical function, so both records compare."""
        return recommended_size_factor(decision, ts)

    # ------------------------------------------------------------------
    async def _options_pulse(self, bar: Bar) -> list[dict] | None:
        """The options-money picture shown to the brain alongside each decision.

        QQQ's pulse comes free from the chain the pricer already holds; the
        leaders are fetched on a slow cache. Runs only when the brain is about
        to be asked, and never blocks a decision on a failed fetch.
        """
        if not isinstance(self.pricer, LiveChainPricer):
            # without a live chain there is no live options data to summarise —
            # and this keeps model-priced runs (tests, offline replays) off the network
            return None
        primary = (
            chain_pulse(
                self.settings.primary_symbol,
                list(self.pricer.snapshot.contracts.values()),
                spot=bar.close,
            )
            if self.pricer.snapshot is not None
            else None
        )
        # each leader's last price, so its unusual strikes can be labelled with
        # how far from the money they sit — a 6% OTM call bought in size reads
        # very differently from one at the money
        spots = {
            symbol: bars[-1].close
            for symbol, bars in self.leader_bars.items()
            if bars
        }
        await self.pulse.refresh_leaders(bar.ts.astimezone(MARKET_TZ).date(), spots)
        rows = self.pulse.rows(primary)
        return rows or None

    # ------------------------------------------------------------------
    def _queue_missed_check(self, snapshot: MarketSnapshot, blocked_by: list[str]) -> None:
        """Remember a declined setup so it can be priced forward later.

        Scoring happens on a delay, not here: at the moment of the decision we
        do not yet know what the market did next. A flat or ambiguous bias is
        skipped — there is no "obvious trade" to grade it against. So is an
        infeasible decline (market closed, broken data): a trade that could not
        exist was not missed, and counting it poisons the learning loop.

        A capacity block while already riding the same direction is not a miss
        either: when the caps say "no second PUT" and the desk is holding a PUT,
        the move was captured, not missed. Recording those rows taught the
        ledger that big gains were "lost" during our best trades and pressured
        the learning loop toward loosening caps that were doing their job.
        """
        if abs(snapshot.net_bias) < 0.2 or infeasible(blocked_by):
            return
        if any(b.startswith(("daily_trade_cap", "position_cap")) for b in blocked_by):
            wanted = OptionType.CALL if snapshot.net_bias > 0 else OptionType.PUT
            if any(t.decision.direction is wanted for t in self.manager.open_trades):
                return
        self._pending_missed.append(
            {
                "ts": snapshot.ts,
                "index": len(self.session_bars) - 1,
                "bias": snapshot.net_bias,
                "regime": snapshot.regime.value,
                "session_minute": snapshot.session_minute,
                "blocked_by": blocked_by,
            }
        )

    def _resolve_pending_missed(self, bar: Bar) -> None:
        """Grade declined setups once enough time has passed to judge them."""
        if not self._pending_missed:
            return

        session_over = bar.ts.astimezone(MARKET_TZ).time() >= REGULAR_CLOSE
        still_pending = []
        for pending in self._pending_missed:
            elapsed_min = (bar.ts - pending["ts"]).total_seconds() / 60
            if elapsed_min < MISSED_LOOKAHEAD_MINUTES and not session_over:
                still_pending.append(pending)
                continue
            self._score_missed(pending)
        self._pending_missed = still_pending

    def _score_missed(self, pending: dict) -> None:
        """Price forward what the declined setup would have made.

        Uses a labelled Black-Scholes approximation, not ``self.pricer``: the
        live chain only ever knows the *current* quote, so it cannot answer
        what a contract was worth at each minute since the decision — only a
        time-aware model can replay that window.
        """
        index = pending["index"]
        window = [b for b in self.session_bars[index:] if b.ts >= pending["ts"]]
        if len(window) < 2:
            return

        direction = OptionType.CALL if pending["bias"] > 0 else OptionType.PUT
        strike = round(window[0].close)
        symbol = occ_symbol(
            self.settings.primary_symbol,
            next_expiry(pending["ts"].date(), 0),
            direction,
            strike,
        )
        entry = self._attribution_pricer.price_at(symbol, window[0].ts, window[0].close)
        if entry is None or entry <= 0:
            return

        best = entry
        for future in window[1:]:
            price = self._attribution_pricer.price_at(symbol, future.ts, future.close)
            if price is not None:
                best = max(best, price)

        peak_pct = round((best - entry) / entry * 100.0, 1)
        if peak_pct < self.settings.min_target_return_pct:
            return

        missed = MissedOpportunity(
            ts=pending["ts"],
            reason="blocked before the brain could act"
            if pending["blocked_by"]
            else "brain declined",
            would_be_direction=direction,
            occ_symbol=symbol,
            hypothetical_entry=round(entry, 2),
            best_price_after=round(best, 2),
            peak_return_pct=peak_pct,
            blocked_by=pending["blocked_by"],
            regime=pending["regime"],
            session_minute=pending["session_minute"],
        )
        self.journal.log_missed(missed)
        self.memory.remember_missed(missed)

    async def _close_if_session_over(self, bar: Bar) -> None:
        local = bar.ts.astimezone(MARKET_TZ).time()
        if local < REGULAR_CLOSE:
            return
        for trade in list(self.manager.open_trades):
            price = (
                self.pricer.price_at(trade.occ_symbol, bar.ts, bar.close, side="exit") or 0.01
            )
            update = self.manager.force_close(trade, price, bar.ts, "session_close")
            await self.notifier.update(trade, update, self._delayed)
            if trade.shared_to_channel and self.channel is not None:
                await self.channel.post_trade_update(trade, update, self._delayed)
            self.journal.log_trade(trade)
            self.memory.remember_trade(trade)
            self._recall_after_close(trade)
            self._persist()
            await self._execute_exit(trade)
        await self._publish_channel_daily(bar.ts.astimezone(MARKET_TZ).date())
        # the operator reads the day's verdict at the bell, not tomorrow: the
        # first placement of this call was at the session roll, which fires on
        # the NEXT day's first bar — so Friday's report would have arrived
        # Monday. It belongs here, with the rest of the after-the-bell package.
        await self._report_data_health(bar.ts.astimezone(MARKET_TZ).date())

    @property
    def _report_channels(self) -> list[ChannelPublisher]:
        """Everywhere the daily/weekly/monthly scoreboard belongs.

        Both rooms, public and private. Each publisher swallows its own
        failures, so a private channel the bot was removed from cannot stop the
        public post, and vice versa.
        """
        return [c for c in (self.channel, self.private_channel) if c is not None]

    async def _publish_channel_daily(self, day: date) -> None:
        """The after-the-bell package: the daily report, the weekly report on
        Fridays, and the education series on its two slots. Guarded so it runs
        once per session no matter how many post-close bars arrive.

        The reports go to both channels; the education series stays public,
        because it is written to attract readers rather than to inform paying
        ones.
        """
        targets = self._report_channels
        if not targets or self._channel_daily_posted == day:
            return
        self._channel_daily_posted = day
        # written to disk before the attempt, not after: a crash mid-publish
        # must still be remembered, or the next boot repeats it verbatim
        self._persist()
        try:
            closed = list(self.manager.closed_trades)
            for channel in targets:
                await channel.post_daily_report(day, closed)
            # the concept lesson rides the bell's attention: minutes after the
            # numbers, while the audience is still reading them
            await self._post_daily_concept_lesson(day, targets)
            if self.channel is not None and day.weekday() in (1, 3):  # Tue, Thu
                await self.channel.post_education(day)
            if day.weekday() == 4:  # Friday: the weekly scoreboard
                period = load_period(
                    self.settings.journal_dir, since=day - timedelta(days=6), until=day
                )
                stats = review(period)
                channel_rows = []
                for row in period.closed:
                    if not row.get("shared_to_channel"):
                        continue
                    opened = row.get("opened_at")
                    try:
                        when = datetime.fromisoformat(opened) if opened else datetime.now(UTC)
                    except ValueError:
                        when = datetime.now(UTC)
                    channel_rows.append(
                        {
                            "label": human_contract(row.get("occ_symbol", ""), when),
                            "return_pct": row.get("return_pct"),
                        }
                    )
                for channel in targets:
                    await channel.post_weekly_report(stats, channel_rows)
            if self._is_last_session_of_month(day):
                await self._publish_channel_monthly(day)
        except Exception:  # noqa: BLE001 - the shop window must never stop the desk
            log.exception("channel daily publishing failed")

    async def _post_daily_concept_lesson(self, day: date, targets: list[ChannelPublisher]) -> None:
        """One gated concept lesson to both rooms. A failure here is a log
        line and an operator note — never a blocked report."""
        if self.tutor is None:
            return
        try:
            lesson = await self.tutor.compose(day)
        except Exception:  # noqa: BLE001 - the lesson is garnish, the reports are dinner
            log.exception("daily lesson generation failed")
            return
        if not lesson:
            # composed but gated out (or empty) — the operator should know the
            # gate fired, because a silently missing lesson looks like a bug
            await self.notifier.note(
                "🚫 درس اليوم حُجب آلياً (خالف بوابة المحتوى) — راجع السجلات"
            )
            return
        for channel in targets:
            await channel.post_lesson(lesson)

    @staticmethod
    def _is_last_session_of_month(day: date) -> bool:
        """True when no weekday remains in this month after ``day``.

        Deliberately calendar-only: market holidays would make the true last
        session earlier, and posting the statement a day late is a far smaller
        problem than never posting it because the engine was waiting for a
        session that a holiday deleted.
        """
        if day.weekday() >= 5:
            return False
        probe = day + timedelta(days=1)
        while probe.month == day.month:
            if probe.weekday() < 5:
                return False
            probe += timedelta(days=1)
        return True

    async def _publish_channel_monthly(self, day: date) -> None:
        """The month's statement. Its daily series and its trade statistics are
        read from the same journal rows, so the curve's final value and the net
        in the totals panel can never disagree.

        Goes to both rooms, like the daily and weekly reports it belongs with.
        """
        targets = self._report_channels
        if not targets:
            return
        month_start = day.replace(day=1)
        period = load_period(self.settings.journal_dir, since=month_start, until=day)
        stats = review(period)

        by_day: dict[date, float] = {}
        channel_rows: list[dict] = []
        for row in period.closed:
            opened = row.get("opened_at")
            try:
                when = datetime.fromisoformat(opened) if opened else datetime.now(UTC)
            except ValueError:
                when = datetime.now(UTC)
            session = when.astimezone(MARKET_TZ).date()
            result = float(row.get("return_pct") or 0.0)
            by_day[session] = by_day.get(session, 0.0) + result
            if row.get("shared_to_channel"):
                channel_rows.append(
                    {"label": human_contract(row.get("occ_symbol", ""), when),
                     "return_pct": result}
                )

        daily_returns = sorted(by_day.items())
        for channel in targets:
            await channel.post_monthly_report(
                month_start, stats, daily_returns, channel_rows
            )

    async def _roll_session(self, new_day: date) -> None:
        """New session: flatten, archive, reset counters."""
        for trade in list(self.manager.open_trades):
            last = self.session_bars[-1] if self.session_bars else None
            price = (
                self.pricer.price_at(trade.occ_symbol, last.ts, last.close) if last else None
            ) or 0.01
            update = self.manager.force_close(
                trade, price, last.ts if last else datetime.now(UTC), "session_rollover"
            )
            await self.notifier.update(trade, update, self._delayed)
            if trade.shared_to_channel and self.channel is not None:
                await self.channel.post_trade_update(trade, update, self._delayed)

        # if the close-time bar never arrived (dead feed at the bell), the
        # day's channel report still goes out at the boundary instead of never
        if self._current_day is not None:
            await self._publish_channel_daily(self._current_day)

        for closed in self.manager.closed_trades:
            self.memory.remember_trade(closed)

        await self.notifier.note(
            f"session {self._current_day} closed | trades={self.status.trades_today} "
            f"| realized={self.manager.realized_return_pct:+.1f}%"
        )
        # normally already sent at the bell; this is the fallback for a session
        # that ended without a post-close bar (crash, feed loss at 15:59)
        if self._current_day is not None:
            await self._report_data_health(self._current_day)

        # score whatever declined setups are still awaiting judgement on
        # whatever window they got, rather than losing them at the boundary
        for pending in self._pending_missed:
            self._score_missed(pending)
        self._pending_missed = []

        # a review failure must never block the actual rollover — trading
        # state (flattening positions, resetting counters) comes first
        try:
            await self._run_daily_review()
        except Exception:  # noqa: BLE001
            log.exception("daily review failed")

        self.manager = TradeManager()
        self.attention.reset()
        self._refresh_recent()
        self.session_bars = []
        self._today_decisions = []
        self.leader_bars = {}
        self.status.trades_today = 0
        self.repairer.reset()
        self.status.realized_pct = 0.0
        self.status.open_positions = 0
        self._watch_shared_today = 0
        # the day flattened everything, so nothing is held into the new one
        self._executed = {}
        self._current_day = new_day
        self._tape_alerted = False
        # yesterday changed at midnight: today's session must be measured
        # against the day that just closed, not the one before it
        if self.settings.massive_api_key:
            try:
                async with MassiveClient(self.settings) as client:
                    await self._load_prior_day(client, new_day)
                    session = await client.session(self.settings.primary_symbol, new_day)
                    self.overnight_high = session.premarket_high
                    self.overnight_low = session.premarket_low
            except Exception:  # noqa: BLE001 - a level refresh never blocks a session
                log.exception("reference level refresh failed at session roll")
        self._persist()
        await self._expire_subscribers()
        # a positive daily sign of life. Absence of signals is ambiguous —
        # a quiet market and a dead engine look the same — so the engine says
        # good morning on its own every session, and a morning with no such
        # message is itself the alarm.
        await self.notifier.note(
            f"🌅 جلسة {new_day.isoformat()} بدأت — المحرك متصل ويستقبل البيانات\n"
            f"الشموع المستلمة حتى الآن: {self.status.bars_received} | "
            f"إعادة الاتصال: {self.status.reconnects}"
        )
        log.info("rolled into session %s", new_day)

    # ------------------------------------------------------------------
    async def _run_daily_review(self) -> None:
        """Turn yesterday's record into a plain-language digest on Telegram.

        This is what makes "learns every day" true in practice rather than in
        theory: the operator does not need to remember to run a command from
        a terminal they do not have — the engine brings the review to them.
        """
        counts = self.memory.counts()
        report = analyse(self.memory, settings=self.settings)
        new_ids = propose(self.memory, report) if report.has_findings else []
        pending = self.memory.pending_lessons()

        lines = [f"📊 المراجعة اليومية — {self._current_day} | دفتر التشغيل v{self.playbook.version}"]
        if report.total_trades:
            lines.append(
                f"صفقات مغلقة على السجل: {counts['closed']} "
                f"| متوسط العائد: {report.baseline_return:+.1f}%"
            )
        else:
            lines.append("لا صفقات مغلقة على السجل بعد")
        if counts["missed"]:
            lines.append(f"فرص فائتة مُسعّرة رجعيًا: {counts['missed']}")

        if new_ids:
            lines.append(f"\n🆕 {len(new_ids)} درس جديد يقترحه التحليل:")
        if pending:
            for row in pending:
                lines.append(
                    f"#{row['id']}: {row['statement']}\n"
                    f"   (عيّنة {row['sample_size']} | ثقة {row['confidence']})"
                )
            lines.append('\nرد بـ "موافق <رقم>" لاعتماده في دفتر التشغيل، أو "رفض <رقم>" لتجاهله.')
        elif not report.has_findings and report.notes:
            lines.append(report.notes[0])

        await self.notifier.note("\n".join(lines))

    async def _command_loop(self) -> None:
        """Long-poll the bot's inbox forever, routing by sender.

        The operator's chat gets the lesson approve/reject verbs; everyone
        else is a would-be subscriber. A bad message from a stranger must
        never take down the loop that also carries operator commands.
        """
        if self.commands is None:
            return
        while True:
            for message in await self.commands.poll():
                try:
                    if message.chat_id == str(self.settings.telegram_chat_id):
                        await self._handle_command(message.text)
                    else:
                        await self._handle_subscriber(message)
                except Exception:  # noqa: BLE001
                    log.exception("inbound message handling failed")

            for request in self.commands.join_requests:
                try:
                    await self._handle_join_request(request)
                except Exception:  # noqa: BLE001
                    log.exception("join request handling failed")
            self.commands.join_requests = []

            for change in self.commands.membership_changes:
                try:
                    await self._handle_membership_change(change)
                except Exception:  # noqa: BLE001
                    log.exception("membership change handling failed")
            self.commands.membership_changes = []

            for press in self.commands.button_presses:
                try:
                    await self._handle_button_press(press)
                except Exception:  # noqa: BLE001
                    log.exception("button press handling failed")
            self.commands.button_presses = []

            for channel_id, title in self.commands.channel_promotions:
                # this is how the operator learns a private channel's numeric
                # id — no technical digging, the bot reports it the moment it
                # is promoted
                await self.notifier.note(
                    f'🔑 تمت إضافتي مشرفًا في قناة "{title}"\n'
                    f"المعرّف الرقمي: {channel_id}\n"
                    "إن كانت هذه قناة المشتركين الخاصة، ضع هذا المعرّف في متغير "
                    "TELEGRAM_PRIVATE_CHANNEL_ID في Railway ثم أعد التشغيل."
                )
            self.commands.channel_promotions = []

    async def _handle_membership_change(self, change) -> None:
        """Put anyone who walks into the private channel onto the books.

        The consent gate only ever sees people whose join needs approving. Our
        own funnel hands out single-use invite links, and a link admits its
        holder with no join request at all — so the one route the engine
        invites people through was the one route it could not see. The result
        on 2026-08-20: two friends inside the channel, receiving every signal,
        and a roster showing one name that came from the old DM funnel.

        Registering here is deliberately quiet. It records who is inside so
        the roster is true; it does not send the welcome or the terms, because
        this person did not just agree to anything.
        """
        private = self.settings.telegram_private_channel_id
        if not private or str(change.chat_id) != str(private):
            return
        if self.settings.trial_days <= 0:
            return

        name = change.first_name or change.username or change.user_id
        if not change.joined:
            await self.notifier.note(f"👋 غادر القناة الخاصة: {name}")
            return

        now = datetime.now(UTC)
        added = self.memory.add_subscriber(
            chat_id=change.user_id,
            username=change.username,
            first_name=change.first_name,
            joined_at=now,
            expires_at=now + timedelta(days=self.settings.trial_days),
        )
        if added:
            active = len(self.memory.active_subscriber_ids(now))
            await self.notifier.note(
                f"👤 انضم للقناة الخاصة: {name} — سُجّل تلقائيًا، "
                f"النشطون الآن: {active}"
            )

    async def _handle_join_request(self, request) -> None:
        """The private channel's front door — now a consent gate.

        Nobody enters and no trial starts until they press "أوافق وأقر" on
        the legal terms. Declining (or never answering) leaves them outside
        with nothing recorded. Telegram permits messaging anyone with a
        pending join request, which is exactly what makes this gate work.
        """
        from qqq_alpha.live.telegram import (
            CONSENT_BUTTONS,
            consent_terms_message,
            trial_status_message,
            welcome_pitch_message,
        )

        private = self.settings.telegram_private_channel_id
        if self.commands is None or not private or request.channel_id != str(private):
            return  # a request for some other chat is not ours to judge

        now = datetime.now(UTC)
        row = self.memory.subscriber(request.user_id)
        name = request.username or request.first_name or request.user_id

        if row is not None:
            expires = datetime.fromisoformat(row["expires_at"])
            if row["status"] == "trial" and expires > now:
                # a known active subscriber re-joining (new phone, left by
                # accident): let them back in on their existing clock —
                # their consent is already on record
                await self.commands.approve_join_request(private, request.user_id)
                await self.commands.send(
                    request.user_id, trial_status_message((expires - now).days)
                )
                return
            await self.commands.decline_join_request(private, request.user_id)
            await self._send_farewell(request.user_id)
            await self.notifier.note(f"⛔ طلب انضمام من مشترك منتهي: {name} — رُفض تلقائيًا")
            return

        if self.settings.trial_days <= 0:
            await self.commands.decline_join_request(private, request.user_id)
            return

        # first-timer: the request stays pending; the verdict is theirs to press
        await self.commands.send(
            request.user_id,
            welcome_pitch_message(
                self.settings.trial_days, self.settings.subscription_price_sar
            ),
        )
        delivered = await self.commands.send_with_buttons(
            request.user_id, consent_terms_message(), CONSENT_BUTTONS
        )
        if not delivered:
            await self.notifier.note(
                f"⚠️ تعذر إرسال رسالة الإقرار لطالب الانضمام {name} — طلبه معلق"
            )

    async def _handle_button_press(self, press) -> None:
        """Route inline-button taps: the consent verdicts and operator previews."""
        from qqq_alpha.live.telegram import (
            CONSENT_NO,
            CONSENT_YES,
            PREVIEW_NO,
            PREVIEW_YES,
            cards_guide_message,
            consent_accepted_note,
            consent_declined_note,
            consent_terms_message,
        )

        if self.commands is None:
            return

        if press.data in (PREVIEW_YES, PREVIEW_NO):
            await self.commands.answer_button(
                press.callback_id, "هذه معاينة فقط — لا تسجيل ولا تأثير ✅"
            )
            return

        private = self.settings.telegram_private_channel_id
        if press.data not in (CONSENT_YES, CONSENT_NO) or not private:
            await self.commands.answer_button(press.callback_id)
            return

        now = datetime.now(UTC)
        name = press.username or press.first_name or press.user_id

        if press.data == CONSENT_NO:
            await self.commands.decline_join_request(private, press.user_id)
            await self.commands.answer_button(press.callback_id, "لم يُسجَّل شيء")
            await self.commands.replace_message(
                press.chat_id, press.message_id,
                consent_terms_message() + "\n\n❌ لم تتم الموافقة — لم يُسجَّل شيء.",
            )
            await self.commands.send(press.user_id, consent_declined_note())
            return

        # consent:yes. Two doors lead here and both are now behind this one
        # button: a pending join request (they tapped a channel link) gets
        # approved, and a /start conversation (no request exists to approve)
        # gets a personal single-use invite link instead. Consent first,
        # admission second — in both cases the press is the admission ticket.
        existing = self.memory.subscriber(press.user_id)
        if existing is not None and existing["status"] != "trial":
            # an expired subscriber pressing an old consent button must not
            # re-enter for free — that would be a permanent unkickable leak

            await self.commands.answer_button(press.callback_id)
            await self._send_farewell(press.user_id)
            return

        link = ""
        if not await self.commands.approve_join_request(private, press.user_id):
            link = (
                await self.commands.create_invite_link(
                    private, name=f"consent-{press.user_id}"
                )
                or ""
            )
            if not link:
                await self.commands.answer_button(press.callback_id, "تعذر إصدار الرابط")
                await self.commands.send(
                    press.user_id,
                    "تعذر إصدار رابط الدخول مؤقتاً — أرسل /start من جديد بعد "
                    "قليل وسيصلك رابطك.",
                )
                await self.notifier.note(
                    f"⚠️ تعذر إصدار رابط دخول لمن أقرّ بالشروط: {name}"
                )
                return

        expires = now + timedelta(days=self.settings.trial_days)
        self.memory.add_subscriber(
            press.user_id,
            press.username,
            press.first_name,
            joined_at=now,
            expires_at=expires,
        )
        self.memory.record_consent(press.user_id, now)
        await self.commands.answer_button(press.callback_id, "تم الإقرار — أهلاً بك 🎉")
        await self.commands.replace_message(
            press.chat_id, press.message_id,
            consent_terms_message() + "\n\n✅ تم الإقرار.",
        )
        await self.commands.send(
            press.user_id,
            consent_accepted_note(
                self.settings.trial_days, expires.date().isoformat(), link
            ),
        )
        await self.commands.send(press.user_id, cards_guide_message())
        active = len(self.memory.active_subscriber_ids(now))
        await self.notifier.note(
            f"👤 مشترك جديد أقرّ بالشروط وانضم: {name} — النشطون الآن: {active}"
        )

    async def _handle_subscriber(self, message) -> None:
        """The trial funnel: /start shows the pitch and the terms — nothing
        is registered and no link is issued until they press أوافق. The one
        consent gate now guards every door."""
        from qqq_alpha.live.telegram import (
            CONSENT_BUTTONS,
            consent_terms_message,
            trial_status_message,
            welcome_pitch_message,
        )

        if self.settings.trial_days <= 0 or self.commands is None:
            return  # operator-only bot; strangers are ignored entirely

        now = datetime.now(UTC)
        row = self.memory.subscriber(message.chat_id)
        log.info(
            "inbound from non-operator chat %s (%s): %r",
            message.chat_id, message.username or message.first_name, message.text
        )

        private = self.settings.telegram_private_channel_id

        if row is None:
            if not message.text.lower().startswith("/start"):
                return  # unknown chat, no sign-up intent: stay silent
            # top-of-funnel demand signal, recorded before any consent:
            # which app language do the people our campaigns reach speak?
            self.memory.record_start_language(
                message.chat_id, message.language or "", now
            )
            await self.commands.send(
                message.chat_id,
                welcome_pitch_message(
                    self.settings.trial_days, self.settings.subscription_price_sar
                ),
            )
            delivered = await self.commands.send_with_buttons(
                message.chat_id, consent_terms_message(), CONSENT_BUTTONS
            )
            if not delivered:
                name = message.username or message.first_name or message.chat_id
                await self.notifier.note(
                    f"⚠️ تعذر إرسال رسائل التسجيل لطالب /start‏ {name} — "
                    "تحقق من سجلات Railway"
                )
            return

        if self._wants_pay_link_text(message.text):
            # a registered person, active or lapsed, asking to pay: a clean
            # offer message with the URL riding a button, not a raw link
            await self._send_pay_offer(message.chat_id)
            return

        expires = datetime.fromisoformat(row["expires_at"])
        if row["status"] == "trial" and expires > now:
            days_left = (expires - now).days
            status = trial_status_message(days_left)
            if private:
                link = await self.commands.create_invite_link(
                    private, name=f"status-{message.chat_id}"
                )
                if link:
                    status += f"\n\n🔗 إن لم تكن داخل القناة الخاصة بعد، هذا رابطك:\n{link}"
            await self.commands.send(message.chat_id, status)
        else:
            await self._send_farewell(message.chat_id)

    _PAY_WORDS = ("اشتراك", "اشترك", "دفع", "/pay", "/subscribe")

    @classmethod
    def _wants_pay_link_text(cls, text: str) -> bool:
        first = text.strip().split()[0].lower() if text.strip() else ""
        return first in cls._PAY_WORDS

    def _pay_link(self, chat_id: str) -> str:
        """The subscriber's personal payment URL, or "" while payments are
        dark — messages simply omit the line instead of pointing at a 404."""
        from qqq_alpha.payments import pay_link

        return pay_link(self.settings, str(chat_id)) or ""

    async def _send_farewell(self, chat_id: str) -> None:
        """The end-of-trial goodbye: pay and public-channel doors as buttons."""
        from qqq_alpha.live.telegram import PAY_BUTTON, farewell_message

        if self.commands is None:
            return
        buttons: list[tuple[str, str]] = []
        pay = self._pay_link(chat_id)
        if pay:
            buttons.append((PAY_BUTTON, pay))
        if self.settings.post_trial_channel_url:
            buttons.append(("📢 القناة العامة المجانية", self.settings.post_trial_channel_url))
        if buttons:
            await self.commands.send_with_buttons(
                chat_id, farewell_message(True), buttons
            )
        else:
            await self.commands.send(chat_id, farewell_message(False))

    async def _send_pay_offer(self, chat_id: str) -> bool:
        """The pay offer as a card-like message with a real ادفع الآن button."""
        from qqq_alpha.live.telegram import PAY_BUTTON, pay_offer_message

        if self.commands is None:
            return False
        link = self._pay_link(chat_id)
        if not link:
            await self.commands.send(
                chat_id, "الدفع غير مفعّل بعد — سيصلك إشعار عند إتاحته."
            )
            return False
        return await self.commands.send_with_buttons(
            chat_id,
            pay_offer_message(
                self.settings.subscription_price_sar, self.settings.subscription_days
            ),
            [(PAY_BUTTON, link)],
        )

    async def _expire_subscribers(self) -> None:
        """Flip finished trials and send each one the follow-up-channel note.

        Called at boot and at every session roll — daily granularity is plenty
        for a 30-day trial, and both hooks together survive restarts. The same
        sweep sends the once-per-window renewal nudge to trials with two days
        left, so the funnel's last touch happens before the door closes, not
        after.
        """
        if self.commands is None:
            return
        from qqq_alpha.live.telegram import renewal_reminder_message

        now = datetime.now(UTC)
        due = self.memory.expire_due_subscribers(now)
        private = self.settings.telegram_private_channel_id
        for row in due:
            if private:
                # removal from the private channel IS the cutoff; the DM only
                # explains it and points at the follow-up channel
                await self.commands.kick(private, row["chat_id"])
            await self._send_farewell(row["chat_id"])
        if due:
            await self.notifier.note(f"⏳ انتهت الفترة التجريبية لـ {len(due)} مشترك")

        for row in self.memory.trials_needing_reminder(now):
            expires_on = str(row.get("expires_at") or "")[:10]
            link = self._pay_link(row["chat_id"])
            text = renewal_reminder_message(
                expires_on, self.settings.subscription_price_sar, bool(link)
            )
            if link:
                from qqq_alpha.live.telegram import PAY_BUTTON

                await self.commands.send_with_buttons(
                    row["chat_id"], text, [(PAY_BUTTON, link)]
                )
            else:
                await self.commands.send(row["chat_id"], text)
            # marked regardless of delivery: a blocked bot must not retry the
            # nudge every day for the rest of the window
            self.memory.mark_reminded(row["chat_id"], now)

    async def _run_indicator_backtest(
        self, symbols: list[str], frame: int, months: int
    ) -> None:
        """Fetch a year of bars per symbol and replay the indicator's twin."""
        from qqq_alpha.backtest.indicator_sim import format_report, run_simulation

        try:
            end = datetime.now(UTC).date()
            start = end - timedelta(days=round(months * 30.4))
            # keep each aggregates request under the provider's 50k-row cap
            chunk_days = 20 if frame < 5 else 90
            results = []
            async with MassiveClient(self.settings) as client:
                for sym in symbols:
                    bars: list = []
                    s = start
                    while s <= end:
                        e = min(s + timedelta(days=chunk_days - 1), end)
                        bars.extend(await client.range_minute_bars(sym, frame, s, e))
                        s = e + timedelta(days=1)
                    # the replay is pure CPU over ~20k bars — off the event loop
                    results.append(await asyncio.to_thread(run_simulation, bars, sym, frame))
            await self.notifier.note(format_report(results, months))
        except Exception as exc:  # noqa: BLE001 - report to the operator, never crash the loop
            await self.notifier.note(f"⚠️ تعطل فحص المؤشر: {exc}")

    async def _handle_command(self, text: str) -> None:
        parts = text.strip().split()
        if parts and parts[0].strip().lower() in {"دفع", "رابط", "paylink"}:
            # the operator's own signed pay link — the fastest way to test
            # the money door end-to-end with a real card. Same styled offer
            # a subscriber sees, so the test previews the real experience.
            link = self._pay_link(str(self.settings.telegram_chat_id))
            if link:
                await self._send_pay_offer(str(self.settings.telegram_chat_id))
            else:
                missing = [
                    name
                    for name, value in (
                        ("MOYASAR_PUBLISHABLE_KEY", self.settings.moyasar_publishable_key),
                        ("MOYASAR_SECRET_KEY", self.settings.moyasar_secret_key),
                        ("PUBLIC_BASE_URL", self.settings.public_base_url),
                    )
                    if not value
                ]
                await self.notifier.note(
                    "➖ الدفع غير مفعّل — المتغيرات الناقصة في Railway:\n"
                    + "\n".join(f"  • {name}" for name in missing)
                )
            return
        if parts and parts[0].strip().lower() in {"مشتركين", "subscribers"}:
            counts = self.memory.subscriber_counts()
            await self.notifier.note(
                f"👥 المشتركون — تجريبي نشط: {counts.get('trial', 0)}"
                f" | منتهي: {counts.get('expired', 0)}"
            )
            return
        if parts and parts[0].strip().lower() in {"معاينة", "معاينه", "preview"}:
            # the operator sees the consent gate exactly as a subscriber
            # would — real buttons, zero side effects
            from qqq_alpha.live.telegram import (
                PREVIEW_NO,
                PREVIEW_YES,
                cards_guide_message,
                consent_terms_message,
                welcome_pitch_message,
            )

            if self.commands is not None:
                admin = str(self.settings.telegram_chat_id)
                await self.commands.send(
                    admin,
                    welcome_pitch_message(
                        self.settings.trial_days, self.settings.subscription_price_sar
                    ),
                )
                await self.commands.send_with_buttons(
                    admin,
                    consent_terms_message(),
                    [("✅ أوافق وأقر", PREVIEW_YES), ("❌ لا أوافق", PREVIEW_NO)],
                )
                await self.commands.send(admin, cards_guide_message())
                for caption, png in self._preview_cards():
                    await self.commands.send_photo(admin, png, caption)
            return
        if parts and parts[0].strip().lower() in {"باكتست", "backtest", "فحص المؤشر"}:
            # the indicator's Python twin over our own year of bars — the
            # operator asks from Telegram instead of hunting TradingView tabs.
            # Syntax: "باكتست NVDA" | "باكتست NVDA 15" | "باكتست NVDA 5 6"
            # | "باكتست الكل" for the whole launch list on 5m.
            symbols = ["TSLA", "AAPL", "MSFT", "NVDA", "QQQ", "AMZN", "GOOG", "MU"]
            frame = 5
            months = 12
            if len(parts) >= 2 and parts[1].strip() not in {"الكل", "all"}:
                symbols = [parts[1].strip().upper()]
            if len(parts) >= 3 and parts[2].isdigit():
                frame = max(1, min(60, int(parts[2])))
            if len(parts) >= 4 and parts[3].isdigit():
                months = max(1, min(24, int(parts[3])))
            await self.notifier.note(
                f"🧪 بدأ الفحص: {'، '.join(symbols)} · فريم {frame} د · {months} شهراً — "
                "أرسل النتيجة أول ما تخلص (دقيقة تقريباً لكل سهم)"
            )
            asyncio.create_task(self._run_indicator_backtest(symbols, frame, months))
            return
        if parts and parts[0].strip().lower() in {"فحص", "فحص القنوات", "check"}:
            # the definitive answer to "is the bot posting to the channel or
            # only to me?" — permissions first, then a real card actually sent
            # to the private channel, and the delivery result reported back
            from qqq_alpha.live import cards as _cards

            # rendering seven cards is a few seconds of CPU: off the event
            # loop, so an operator running فحص mid-session never delays a bar
            _, card_report = await asyncio.to_thread(_cards.self_test)
            await self.notifier.note(card_report)
            private_verdict = await self._report_channel_health()
            target = str(self.settings.telegram_private_channel_id or "")
            if not target:
                await self.notifier.note(
                    "➖ لا توجد قناة خاصة مُعدّة (TELEGRAM_PRIVATE_CHANNEL_ID فارغ) — "
                    "لذلك تصل البطاقات إلى المحادثة الخاصة بالبوت."
                )
                return
            telegram = self._telegram()
            if telegram is None:
                await self.notifier.note("⚠️ لا يوجد اتصال تلجرام لتشغيل الفحص")
                return
            samples = self._preview_cards()
            if not samples:
                await self.notifier.note("⚠️ تعذّر توليد بطاقة اختبار")
                return
            caption, png = samples[0]
            delivered = await telegram._post_photo(
                png, caption=f"🧪 بطاقة فحص — {caption}", silent=True, chat_id=target
            )
            # one conclusion, not two lines that can contradict each other: the
            # dry run produced a report saying the channel was unreachable and,
            # directly under it, "✅ the test card arrived"
            if delivered:
                # if the permission check disagreed with the delivery, say both
                # rather than picking the happier one — a green conclusion
                # printed under a red diagnostic is worse than no conclusion
                caveat = (
                    ""
                    if private_verdict.startswith("✅")
                    else f"\n⚠️ لكن فحص الصلاحيات قال: {private_verdict}"
                )
                await self.notifier.note(
                    f"🧪 الخلاصة: ✅ البطاقات تصل إلى القناة الخاصة ({target}) — "
                    "افتح القناة وستجد بطاقة الفحص فيها الآن." + caveat
                )
            else:
                await self.notifier.note(
                    f"🧪 الخلاصة: ❌ البطاقات لا تصل إلى القناة الخاصة ({target})\n"
                    f"حالة القناة: {private_verdict}\n"
                    "ولهذا تصل النسخة النصية إليك وحدك. صحّح ما ورد أعلاه ثم "
                    'أرسل "فحص" مرة أخرى.'
                )
            return
        if len(parts) != 2 or not parts[1].isdigit():
            # any other operator text gets an answer on purpose: it is the
            # operator's one-tap proof that the inbound path is alive at all —
            # for weeks a stale webhook made replies vanish with zero symptom
            await self.notifier.note(
                "✅ وصلتني رسالتك — استقبال الرسائل شغال.\n"
                'الأوامر: "موافق <رقم>" / "رفض <رقم>" لقرارات الدروس، '
                '"مشتركين" لعدد المشتركين، "معاينة" لتجربة رسالة الإقرار بأزرارها، '
                '"فحص" للتأكد أن البطاقات تصل إلى القناة الخاصة.'
            )
            return

        verb, lesson_id = parts[0].strip().lower(), int(parts[1])
        if verb in {"موافق", "apply", "approve"}:
            try:
                self.playbook = apply_pending_lesson(
                    self.memory, self.playbook, lesson_id, self.settings
                )
                await self.notifier.note(
                    f"✅ اعتُمد الدرس #{lesson_id} — دفتر التشغيل الآن v{self.playbook.version}"
                )
            except ValueError as exc:
                await self.notifier.note(f"⚠️ تعذر اعتماد الدرس #{lesson_id}: {exc}")
        elif verb in {"رفض", "reject"}:
            self.memory.set_lesson_status(lesson_id, "rejected")
            await self.notifier.note(f"🚫 رُفض الدرس #{lesson_id}")

    @staticmethod
    def _preview_cards() -> list[tuple[str, bytes]]:
        """Sample renders of every card type, for the operator's معاينة.

        Built on synthetic data and clearly captioned as models — how the
        operator inspects design changes on their phone without waiting for
        a live trade. Best-effort: a broken renderer skips its card.
        """
        from qqq_alpha.data.synthetic import synthetic_session
        from qqq_alpha.domain import Target, TradeUpdate
        from qqq_alpha.live import cards

        samples: list[tuple[str, bytes]] = []
        try:
            bars = synthetic_session("QQQ", date(2026, 8, 14), seed=15)
            snap = SnapshotBuilder("QQQ").build(bars[:80])

            def demo_trade(direction: OptionType):
                decision = Decision(
                    ts=snap.ts, action=Action.ENTER, direction=direction,
                    occ_symbol="O:QQQ260814P00731000"
                    if direction is OptionType.PUT
                    else "O:QQQ260814C00731000",
                    targets=[
                        Target(label="T1", price=0.0, return_pct=50, take_pct=50),
                        Target(label="T2", price=0.0, return_pct=100, take_pct=30),
                    ],
                    stop_return_pct=-40, confidence=7, thesis="نموذج للمعاينة",
                    invalidation_level=732.70, size_factor=0.5,
                )
                manager = TradeManager()
                return manager, manager.open_trade(decision, 1.79, snap)

            samples.append((
                "🔵 نموذج: بطاقة قيد التكوّن",
                cards.render_watch_card(
                    "QQQ", "هبوط PUT", "ارتداد فاشل نحو VWAP", 6, snap.ts, level=732.50
                ),
            ))

            manager, trade = demo_trade(OptionType.PUT)
            samples.append(("نموذج: بطاقة دراسة الحالة", cards.render_entry_card(trade, False)))

            live = TradeUpdate(
                ts=trade.opened_at + timedelta(minutes=31), price=2.01,
                return_pct=12.3, note="status: still open",
            )
            samples.append((
                "🟢 نموذج: بطاقة المجريات (تتجدد كل ربع ساعة)",
                cards.render_entry_card(trade, False, live=live),
            ))

            scale = manager.update(trade, 2.42, trade.opened_at + timedelta(minutes=9))
            if scale is not None:
                samples.append((
                    "🟢 نموذج: لحظة تأمين التكلفة", cards.render_scale_out_card(trade, scale)
                ))
            manager.update(trade, 3.60, trade.opened_at + timedelta(minutes=20))
            close = manager.update(trade, 3.00, trade.opened_at + timedelta(minutes=26))
            if close is not None:
                samples.append((
                    "🟢 نموذج: خلاصة رابحة", cards.render_close_card(trade, close)
                ))

            manager2, trade2 = demo_trade(OptionType.CALL)
            close2 = manager2.update(trade2, 1.05, trade2.opened_at + timedelta(minutes=7))
            if close2 is not None:
                samples.append((
                    "🔴 نموذج: خلاصة خاسرة", cards.render_close_card(trade2, close2)
                ))

            follow_up = TradeUpdate(
                ts=trade.opened_at + timedelta(minutes=14), price=2.68,
                return_pct=49.7, note="target:T1 reached (+50%)",
            )
            samples.append((
                "🟢 نموذج: بطاقة المحطات (محطة تحققت)",
                cards.render_update_card(trade, follow_up),
            ))
            samples.append((
                "📚 نموذج: البطاقة التعليمية",
                cards.render_education_card(cards._SAMPLE_LESSON),
            ))
            samples.append((
                "📄 نموذج: التقرير اليومي",
                cards.render_daily_report_card(
                    date(2026, 8, 14),
                    [{"label": "QQQ 731 PUT 0DTE", "return_pct": 67.6, "shared": True},
                     {"label": "QQQ 733 CALL 0DTE", "return_pct": -41.3, "shared": False}],
                ),
            ))
            samples.append((
                "🗓️ نموذج: البيان الشهري",
                cards.render_monthly_report_card(
                    date(2026, 8, 1), cards._sample_stats(),
                    [(date(2026, 8, 3) + timedelta(days=index), value)
                     for index, value in enumerate(
                         [12.5, -8.0, 31.2, -15.4, 22.0, 5.5, -21.0, 44.0])],
                    [{"label": "QQQ 731 PUT 0DTE", "return_pct": 44.0}],
                ),
            ))
        except Exception:  # noqa: BLE001 - a preview must never crash the engine
            log.exception("preview card rendering failed")
        return samples

    async def _channel_roster(self, chat_ids: list[str]) -> dict:
        """What Telegram says about the private channel, for the roster page.

        Telegram offers no way to LIST a channel's members — that endpoint has
        never existed — so the honest best is a total and a per-person check.
        The total is what exposes the gap: "3 in the channel, 1 on the books"
        is a fact the operator can act on, where a bare "1" looked like a bug.
        """
        private = self.settings.telegram_private_channel_id
        if self.commands is None or not private:
            return {}
        checks = await asyncio.gather(
            self.commands.member_count(private),
            *(self.commands.is_member(private, chat_id) for chat_id in chat_ids),
            return_exceptions=True,
        )
        total, per_person = checks[0], checks[1:]
        return {
            "channel_total": total if isinstance(total, int) else None,
            "inside": {
                chat_id: (result if isinstance(result, bool) else None)
                for chat_id, result in zip(chat_ids, per_person, strict=False)
            },
        }

    async def _on_subscriber_change(
        self, action: str, row: dict, days: int | None
    ) -> None:
        """Carry a dashboard edit through to Telegram.

        The database row is only half of a subscription — the other half is
        membership of the private channel. Extending without telling anyone is
        a gift nobody notices; deleting without removing them from the channel
        leaves someone reading every signal with no record that they are there.
        """
        if self.commands is None:
            return
        chat_id = str(row.get("chat_id") or "")
        name = row.get("first_name") or row.get("username") or chat_id
        private = self.settings.telegram_private_channel_id

        if action == "extended":
            expires = row.get("expires_at", "")[:10]
            await self.commands.send(
                chat_id,
                f"🎁 تم تمديد اشتراكك المجاني {days} يومًا إضافية.\n"
                f"ينتهي الآن في: {expires}\n"
                "استمتع بالمحتوى التعليمي 🤍",
            )
            await self.notifier.note(f"🎁 مُدّد اشتراك {name} بـ {days} يومًا")
            return

        if action == "removed":
            if private:
                # the channel removal IS the cutoff; the DM only explains it
                await self.commands.kick(private, chat_id)
            await self.commands.send(
                chat_id, "انتهى وصولك إلى القناة الخاصة. شكرًا لك 🤍"
            )
            await self.notifier.note(f"🗑️ حُذف المشترك {name} وأُخرج من القناة")

    async def _on_payment(self, action: str, chat_id: str, info: dict) -> None:
        """A verified Moyasar payment reaches Telegram: the subscriber gets
        their confirmation (and a door key if they need one), the operator
        gets the receipt. Rejections are operator-only — a tampered payment
        does not deserve a polite reply."""
        payment_id = info.get("payment_id", "؟")
        if action == "rejected":
            problems = "\n".join(f"  • {p}" for p in info.get("problems", []))
            await self.notifier.note(
                f"🚨 دفعة مرفوضة — لم يُفعَّل شيء (المعرف {payment_id}):\n{problems}\n"
                "راجع لوحة ميسر بنفسك."
            )
            return
        if action != "activated":
            return

        row = info.get("row") or {}
        expires = str(row.get("expires_at") or "")[:10]
        amount_sar = int(info.get("amount") or 0) // 100
        if self.commands is not None:
            message = (
                "✅ تم تفعيل اشتراكك — شكرًا لك 🤍\n"
                f"اشتراكك فعال حتى {expires}."
            )
            private = self.settings.telegram_private_channel_id
            if private:
                link = await self.commands.create_invite_link(
                    private, name=f"paid-{chat_id}"
                )
                if link:
                    message += (
                        "\n\n🔗 إن لم تكن داخل القناة الخاصة بعد، هذا رابط "
                        f"دخولك (صالح لشخص واحد):\n{link}"
                    )
            await self.commands.send(chat_id, message)
        name = row.get("first_name") or row.get("username") or chat_id
        await self.notifier.note(
            f"💳 دفعة مؤكدة: {name} — {amount_sar} ريال، اشتراكه فعال حتى {expires}"
        )

    async def _run_dashboard(self) -> None:
        """Serve the admin dashboard for the life of the session.

        Runs embedded in this same process so it shares the event loop and
        reads ``self.status`` live, without a second deployment or a second
        copy of the data to keep in sync.
        """
        import uvicorn

        from qqq_alpha.dashboard.app import create_app

        def _apply_playbook(book: Playbook) -> None:
            self.playbook = book
            if self.shadow is not None:
                # the learner reads the same playbook the live desk does
                self.shadow.playbook = book

        app = create_app(
            self.settings,
            status=self.status,
            on_lesson_applied=_apply_playbook,
            on_subscriber_change=self._on_subscriber_change,
            channel_roster=self._channel_roster,
            on_payment=self._on_payment,
        )
        config = uvicorn.Config(
            app,
            host="0.0.0.0",  # noqa: S104 - intentional: this is the container's only network interface
            port=self.settings.dashboard_port,
            log_level="warning",
            access_log=False,
        )
        server = uvicorn.Server(config)
        try:
            await server.serve()
        except asyncio.CancelledError:
            await server.shutdown()
            raise

    async def _shutdown(self) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watchdog_task
        if self._dashboard_task is not None:
            self._dashboard_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dashboard_task
        if self._command_task is not None:
            self._command_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._command_task
        if self.commands is not None:
            await self.commands.aclose()
        if self.channel is not None:
            await self.channel.aclose()

        for trade in list(self.manager.open_trades):
            self.journal.log_trade(trade)
        if self.shadow is not None:
            self.shadow.flatten(datetime.now(UTC))
        # pending missed-opportunity checks are in-memory only; score what we
        # can rather than silently lose them to a restart
        for pending in self._pending_missed:
            self._score_missed(pending)
        self._pending_missed = []
        self._persist()
        await self.notifier.note(
            f"engine stopped | signals={self.status.signals_sent} "
            f"| open positions left unmanaged: {len(self.manager.open_trades)}"
        )

    async def _refresh_chain(self, bar: Bar) -> None:
        """Keep live quotes current. A stale chain prices today's trades at yesterday."""
        if not isinstance(self.pricer, LiveChainPricer):
            return

        expiry = next_expiry(bar.ts.astimezone(MARKET_TZ).date(), 0)
        if not await self.pricer.refresh(expiry) and self.pricer.last_error:
            self.status.last_error = f"chain: {self.pricer.last_error}"

    def _refresh_recent(self) -> None:
        """Reload recent history from disk rather than trusting process memory."""
        self.recent_trades = self.memory.recent_trades(limit=15)

    async def _report_data_health(self, day: date) -> None:
        """The day's verdict on its own data, with the cause named.

        The engine always knew the data was incomplete — it said so inside
        almost every decision and lowered its confidence for it. What it never
        said was *why*, which left the operator choosing between guesses whose
        fixes differ and one of which is expensive. The three witnesses exist
        already; this is the first place they are put side by side.

        Guarded per day: after-hours bars keep arriving until 20:00 ET, and
        each of them passes through the same post-close path that sends this.
        """
        if self._health_reported == day or not self.session_bars:
            return
        self._health_reported = day
        self._persist()
        health = assess(
            inspect_session(self.session_bars),
            self.status.reconnects,
            self.repairer.log,
        )
        await self.notifier.note(health.message())

    async def _announce_execution_mode(self) -> None:
        """Say, at every boot, whether real money is on the line.

        The one fact an operator must never have to guess or infer from a
        dashboard, and the one that silence answers wrongly in both
        directions.
        """
        if self.execution.armed:
            await self.notifier.note(
                "💰 **التنفيذ الحقيقي مُفعَّل** — البوت يرسل أوامر فعلية عبر "
                f"«{getattr(self.execution.broker, 'name', '?')}»، "
                f"بحدّ أقصى {self.settings.execution_max_contracts} عقدًا للأمر.\n"
                "للإيقاف فورًا: اجعل EXECUTION_ENABLED=false في Railway."
            )
            return
        await self.notifier.note(
            "🧾 التنفيذ مُطفأ — البوت يقرّر ويسجّل ولا يرسل أي أمر لأي وسيط. "
            "كل أمر كان سيُرسَل يُكتب في سجل الأوامر حتى تقرأه قبل التفعيل."
        )

    # ------------------------------------------------------------------
    async def _execute_entry(self, trade: Trade) -> None:
        """Buy what the engine just opened on paper.

        Priced at the entry the engine recorded, which is already the ask: the
        live order asks for the same price the paper record assumed, so any
        divergence between the two is real slippage rather than an artefact of
        pricing them differently.

        The size is a dollar budget turned into whole contracts. Conviction
        sizing is deliberately not applied: every decision on record has come
        in at confidence 6, so it would shrink every trade by the same factor
        rather than tell any two apart — and only a record where each trade
        risks the same amount can later prove whether confidence means
        anything.
        """
        sizing = size_order(
            trade.entry_price,
            self.settings.execution_dollars_per_trade,
            self.settings.execution_size_tolerance_pct,
            self.settings.execution_max_contracts,
        )
        if not sizing.ok:
            self._executed[trade.trade_id] = 0
            self._persist()
            await self.notifier.note(
                f"⚠️ لم يُرسَل أمر حقيقي لـ {human_contract(trade.occ_symbol, trade.opened_at)}\n"
                f"{sizing.reason}\n"
                "الصفقة مسجّلة ورقيًا كالمعتاد — محفظتك فقط لم تدخلها."
            )
            return

        order = await self.execution.submit(
            OrderRequest(
                client_order_id=f"{trade.trade_id}-entry",
                occ_symbol=trade.occ_symbol,
                side=Side.BUY,
                quantity=sizing.contracts,
                limit_price=trade.entry_price,
                trade_id=trade.trade_id,
                reason="entry",
            )
        )
        # what we actually hold, which is what every later sell is measured
        # against. A partial fill means the position is smaller than intended,
        # and selling the intended size would leave a short leg behind.
        held = sizing.contracts if order is None else (order.filled_quantity or 0)
        self._executed[trade.trade_id] = held if self.execution.armed else sizing.contracts
        # written straight after, not on the next event: a restart between the
        # fill and the next persist would leave a position on the books with
        # no size beside it
        self._persist()

    async def _execute_scale_out(self, trade: Trade) -> None:
        """Bank part of a position the exit engine just took half off.

        The paper record banks exactly half; contracts are whole, so an odd
        position is split unevenly and the split rounds toward banking more —
        the purpose of the scale-out is to take the cost off the table, and
        under-banking fails at that while over-banking merely leaves a smaller
        runner.
        """
        held = self._executed.get(trade.trade_id, 0)
        # read from the exit policy rather than restated here, so the paper
        # split and the live one cannot drift apart
        sell = split_for_scale_out(held, self.manager.policy.scale_out_fraction)
        if sell <= 0:
            return
        last = trade.updates[-1] if trade.updates else None
        price = last.price if last is not None else None
        if price is None or price <= 0:
            return
        await self.execution.submit(
            OrderRequest(
                client_order_id=f"{trade.trade_id}-scale",
                occ_symbol=trade.occ_symbol,
                side=Side.SELL,
                quantity=sell,
                limit_price=price,
                trade_id=trade.trade_id,
                reason="scale_out",
            )
        )
        self._executed[trade.trade_id] = held - sell
        self._persist()

    async def _execute_exit(self, trade: Trade) -> None:
        """Sell whatever is left of a position the engine has finished with.

        The remainder, not the original size: a trade that banked half earlier
        holds fewer contracts now, and asking to sell the opening quantity
        would leave a short leg behind.
        """
        if trade.is_open or trade.exit_price is None or trade.exit_price <= 0:
            return
        remaining = self._executed.pop(trade.trade_id, 0)
        self._persist()
        if remaining <= 0:
            return
        await self.execution.submit(
            OrderRequest(
                client_order_id=f"{trade.trade_id}-exit",
                occ_symbol=trade.occ_symbol,
                side=Side.SELL,
                quantity=remaining,
                limit_price=trade.exit_price,
                trade_id=trade.trade_id,
                reason=trade.exit_reason or "exit",
            )
        )

    async def _reconcile_positions(self) -> None:
        """Ask the broker what we actually own, and say so if it disagrees.

        Run at boot, which is the moment after every disconnect: whatever
        happened while the process was dead happened without us, and the
        broker is the only witness.

        Sizes are restored with the positions, so a normal restart knows what
        it holds. A trade that still has no remembered size — state discarded
        as stale, a fill that landed while the process was dying — is left out
        of the expected book rather than guessed back from a dollar budget and
        a price that has since moved, so the broker's copy shows up as
        unknown-to-engine and says so out loud.
        """
        if not self.execution.armed:
            return
        expected: dict[str, int] = {}
        for trade in self.manager.open_trades:
            held = self._executed.get(trade.trade_id, 0)
            if held <= 0:
                continue
            expected[trade.occ_symbol] = expected.get(trade.occ_symbol, 0) + held
        await self.execution.reconcile(expected)

    def _recall_after_close(self, trade: Trade) -> None:
        """Let the brain see a trade it already closed today.

        This list was read at boot and at the session roll and nowhere else, so
        a trade opened and closed inside one session did not join it until the
        next morning — and an open trade drops out of CURRENTLY OPEN the moment
        it closes. Between those two facts the brain had no way to know what it
        had already done today.

        It showed on 2026-08-20: the day's second entry reasoned "I have not
        entered a trade today" while the first was banked at +31.6%. That day
        it cost nothing because the first trade won. The day the first one
        loses, the brain would size and argue the second as if the loss never
        happened — and it now has three chances a day, not one.

        The rails always knew the count (``DayState.trades_taken``); only the
        judgment forming the decision did not. Nothing here changes a rule: the
        same list, in the same shape, simply arrives on time.
        """
        if not self.settings.recall_todays_trades or trade.status is TradeStatus.OPEN:
            return
        self._refresh_recent()

    @property
    def _delayed(self) -> bool:
        return self.settings.massive_feed_mode != "real_time"
