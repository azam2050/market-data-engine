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
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from qqq_alpha.brain.attention import AttentionEngine
from qqq_alpha.brain.decider import Decider, next_expiry
from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.brain.rails import DayState, SafetyRails
from qqq_alpha.config import MARKET_TZ, REGULAR_CLOSE, Settings
from qqq_alpha.data.chain import LiveChainPricer
from qqq_alpha.data.massive import MassiveClient
from qqq_alpha.data.pricing import OptionPricer
from qqq_alpha.data.quality import inspect_session
from qqq_alpha.domain import Action, Bar, Trade
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal
from qqq_alpha.live.notifier import ConsoleNotifier, Notifier
from qqq_alpha.live.state import SessionState, StateStore
from qqq_alpha.live.stream import LiveBarStream
from qqq_alpha.memory import Memory
from qqq_alpha.trades import TradeManager

log = logging.getLogger(__name__)

WARMUP_BARS = 30


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
        self.leader_bars: dict[str, list[Bar]] = {}
        self.recent_trades: list[Trade] = []
        self._current_day: date | None = None
        self.store = StateStore(self.settings.journal_dir / "session-state.json")
        # long-term memory lives on disk, so a restart never costs the engine
        # what it has learned — unlike the in-process list it replaced
        self.memory = Memory(self.settings.data_dir / "memory.db")

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

        await self._restore()
        self._refresh_recent()
        await self._warm_start()

        try:
            async for bar in stream.bars():
                self.status.reconnects = stream.reconnects
                await self._on_bar(bar)
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

                for symbol in self.settings.leader_symbols:
                    leader = await client.session(symbol, today)
                    if leader.regular:
                        self.leader_bars[symbol] = list(leader.regular)
        except Exception as exc:
            # a failed warm start degrades the engine, it does not stop it
            self.status.last_error = f"warm_start_failed: {exc}"
            log.warning("warm start failed: %s", exc)
            await self.notifier.note(f"warm start failed ({exc}); starting cold")

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
            return

        self.session_bars.append(bar)
        if len(self.session_bars) < WARMUP_BARS:
            return

        await self._refresh_chain(bar)
        await self._mark_open_positions(bar)
        await self._maybe_decide(bar)
        await self._close_if_session_over(bar)

    async def _mark_open_positions(self, bar: Bar) -> None:
        for trade in list(self.manager.open_trades):
            # mark at the bid: that is what closing the position would actually fetch
            price = self.pricer.price_at(trade.occ_symbol, bar.ts, bar.close, side="exit")
            if price is None:
                continue
            update = self.manager.update(trade, price, bar.ts)
            if update is not None:
                await self.notifier.update(trade, update, self._delayed)
                self.journal.log_trade(trade)
                self.memory.remember_trade(trade)
                self._persist()

        self.status.open_positions = len(self.manager.open_trades)
        self.status.realized_pct = self.manager.realized_return_pct

    async def _maybe_decide(self, bar: Bar) -> None:
        quality = inspect_session(self.session_bars)
        snapshot = self.builder.build(
            session_bars=self.session_bars,
            leader_bars=self.leader_bars or None,
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
        )
        pre = self.rails.pre_check(snapshot, state)
        if not pre.allowed:
            log.debug("rails blocked: %s", pre.blocks)
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
        )
        self.status.brain_calls += 1

        # with a live chain we can validate the real contract: spread, liquidity,
        # whether it exists at all
        contract = (
            self.pricer.contract(decision.occ_symbol or "")
            if isinstance(self.pricer, LiveChainPricer)
            else None
        )
        post = self.rails.post_check(decision, contract)
        self.journal.log_decision(
            decision, snapshot, post.blocks, pre.warnings + post.warnings, verdict.score
        )
        self.memory.remember_decision(decision, snapshot, verdict.score, post.blocks)

        if decision.action is not Action.ENTER or not post.allowed:
            if decision.action is Action.ENTER:
                await self.notifier.note(f"entry blocked by rails: {post.blocks}")
            return

        # you pay the offer; pricing an entry at the mid invents profit that
        # will not exist when the trade is actually taken
        fill = self.pricer.price_at(decision.occ_symbol or "", bar.ts, bar.close, side="entry")
        if fill is None or fill <= 0:
            await self.notifier.note(f"could not price {decision.occ_symbol}; signal dropped")
            return

        trade = self.manager.open_trade(decision, fill, snapshot)
        self.status.trades_today += 1
        self.status.signals_sent += 1
        self.journal.log_trade(trade)
        self.memory.remember_trade(trade, snapshot)
        # persist before publishing: a crash between the two must leave the
        # position recoverable, never announced-but-forgotten
        self._persist()
        await self.notifier.signal(trade, self._delayed)

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
            self.journal.log_trade(trade)
            self.memory.remember_trade(trade)
            self._persist()

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

        for closed in self.manager.closed_trades:
            self.memory.remember_trade(closed)

        await self.notifier.note(
            f"session {self._current_day} closed | trades={self.status.trades_today} "
            f"| realized={self.manager.realized_return_pct:+.1f}%"
        )

        self.manager = TradeManager()
        self.attention.reset()
        self._refresh_recent()
        self.session_bars = []
        self.leader_bars = {}
        self.status.trades_today = 0
        self.status.realized_pct = 0.0
        self.status.open_positions = 0
        self._current_day = new_day
        self._persist()
        log.info("rolled into session %s", new_day)

    async def _shutdown(self) -> None:
        for trade in list(self.manager.open_trades):
            self.journal.log_trade(trade)
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

    @property
    def _delayed(self) -> bool:
        return self.settings.massive_feed_mode != "real_time"
