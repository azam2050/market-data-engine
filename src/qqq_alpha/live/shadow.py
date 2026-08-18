"""Shadow stock desk — the expansion candidates learning in the background.

The operator wants the bot trading single names (NVDA, TSLA, AAPL, …) with
the same system it runs on QQQ. Jumping straight to live signals would throw
away the one thing that makes this project trustworthy: an evidence trail.
So the expansion starts here — the same brain, the same playbook, the same
exit engine and the same sizing arithmetic, run per symbol on the leader bars
the engine already streams. Every trade is simulated, every option price is a
labelled Black-Scholes approximation of the WEEKLY contract, and nothing is
ever sent to a subscriber. The record accumulates on the admin dashboard;
a symbol graduates to the live desk only when its shadow record earns it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime

from qqq_alpha.brain.attention import AttentionEngine
from qqq_alpha.brain.decider import Decider, occ_symbol
from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.brain.rails import DayState, SafetyRails
from qqq_alpha.config import MARKET_TZ, REGULAR_CLOSE, Settings
from qqq_alpha.data.calendar import todays_events
from qqq_alpha.data.massive import parse_occ_symbol
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.data.pulse import nearest_weekly_expiry
from qqq_alpha.domain import Action, Bar, Decision
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal
from qqq_alpha.trades import TradeManager, recommended_size_factor

log = logging.getLogger(__name__)

SHADOW_WARMUP_BARS = 30
# per-symbol simulated caps, mirroring the live desk's discipline
SHADOW_MAX_TRADES_PER_DAY = 2
SHADOW_MAX_OPEN = 1

# single names are not the index: pricing NVDA weekly premium off QQQ's 22%
# implied vol would flatter every simulated fill. These are coarse resting
# levels per name — the record they produce is labelled an approximation
# everywhere it is shown, and calibration against real chains happens before
# any symbol goes live.
SHADOW_VOLATILITY = {
    "AAPL": 0.28,
    "MSFT": 0.25,
    "GOOGL": 0.32,
    "AMZN": 0.35,
    "META": 0.40,
    "NVDA": 0.45,
    "AMD": 0.50,
    "TSLA": 0.60,
    "MSTR": 0.90,
    "LULU": 0.45,
}
DEFAULT_SHADOW_VOLATILITY = 0.40


@dataclass
class ShadowBook:
    """Everything one shadowed symbol owns: bars, attention, positions, caps."""

    symbol: str
    builder: SnapshotBuilder
    attention: AttentionEngine
    manager: TradeManager
    pricer: BlackScholesPricer
    bars: list[Bar] = field(default_factory=list)
    brain_calls_today: int = 0
    trades_today: int = 0
    decisions_today: list[Decision] = field(default_factory=list)

    def roll_day(self) -> None:
        self.bars = []
        self.brain_calls_today = 0
        self.trades_today = 0
        self.decisions_today = []
        self.attention.reset()


class ShadowStockDesk:
    """Runs the QQQ decision loop, per symbol, with simulated execution.

    Deliberately journal-only: it never touches the notifier, never writes to
    the QQQ learning memory (a weekly NVDA record must not tilt QQQ 0DTE
    lessons), and its journal lives in a subdirectory so the main dashboard
    pages — which glob the top-level journal dir — never mix the two records.
    """

    def __init__(
        self,
        settings: Settings,
        decider: Decider,
        playbook: Playbook,
        journal: Journal | None = None,
    ):
        self.settings = settings
        self.decider = decider
        self.playbook = playbook
        self.journal = journal or Journal(settings.journal_dir / "shadow")
        self.rails = SafetyRails(settings)
        self.brain_calls = 0
        self._current_day: date | None = None

        tracked = set(settings.leader_symbols)
        self.books: dict[str, ShadowBook] = {}
        for symbol in settings.shadow_symbols:
            if symbol == settings.primary_symbol:
                continue
            if symbol not in tracked:
                # no bars will ever arrive for it — say so once, loudly, at
                # boot rather than shadowing silence for weeks
                log.warning(
                    "shadow symbol %s is not in LEADER_SYMBOLS; it gets no bars "
                    "and is skipped",
                    symbol,
                )
                continue
            self.books[symbol] = ShadowBook(
                symbol=symbol,
                builder=SnapshotBuilder(symbol),
                attention=AttentionEngine(
                    settings.attention_threshold, settings.attention_cooldown_sec
                ),
                manager=TradeManager(),
                pricer=BlackScholesPricer(
                    volatility=SHADOW_VOLATILITY.get(symbol, DEFAULT_SHADOW_VOLATILITY)
                ),
            )

    @property
    def symbols(self) -> list[str]:
        return list(self.books)

    # ------------------------------------------------------------------
    def seed(self, symbol: str, bars: list[Bar]) -> None:
        """Warm-start one book from a REST backfill, same as the live desk."""
        book = self.books.get(symbol)
        if book is not None and bars:
            book.bars = list(bars)
            self._current_day = bars[-1].ts.astimezone(MARKET_TZ).date()

    # ------------------------------------------------------------------
    async def on_bar(self, bar: Bar) -> None:
        book = self.books.get(bar.symbol)
        if book is None:
            return

        local_day = bar.ts.astimezone(MARKET_TZ).date()
        if self._current_day is not None and local_day != self._current_day:
            self._roll_session(bar)
        self._current_day = local_day

        book.bars.append(bar)
        if len(book.bars) < SHADOW_WARMUP_BARS:
            return

        self._mark_positions(book, bar)
        if bar.ts.astimezone(MARKET_TZ).time() >= REGULAR_CLOSE:
            # mirror the live desk: intraday record, nothing held past the bell
            for trade in list(book.manager.open_trades):
                price = book.pricer.price_at(
                    trade.occ_symbol, bar.ts, bar.close, side="exit"
                ) or 0.01
                book.manager.force_close(trade, price, bar.ts, "session_close")
                self.journal.log_trade(trade)
            return
        await self._maybe_decide(book, bar)

    # ------------------------------------------------------------------
    def _mark_positions(self, book: ShadowBook, bar: Bar) -> None:
        for trade in list(book.manager.open_trades):
            price = book.pricer.price_at(trade.occ_symbol, bar.ts, bar.close, side="exit")
            if price is None:
                continue
            if book.manager.check_thesis(trade, bar.close):
                update = book.manager.force_close(
                    trade, price, bar.ts, "thesis_invalidated"
                )
            else:
                update = book.manager.update(trade, price, bar.ts)
            if update is not None:
                self.journal.log_trade(trade)

    # ------------------------------------------------------------------
    async def _maybe_decide(self, book: ShadowBook, bar: Bar) -> None:
        snapshot = book.builder.build(session_bars=book.bars, now=bar.ts)

        verdict = book.attention.evaluate(snapshot)
        self.journal.log_attention(
            bar.ts, verdict.score, verdict.should_wake, verdict.summary, verdict.suppressed_by
        )
        if not verdict.should_wake:
            return

        # the live rails plus the desk's own cost gate: each wake here is a
        # real brain call, and three symbols must not triple the API bill
        state = DayState(
            trades_taken=book.trades_today,
            open_positions=len(book.manager.open_trades),
            realized_return_pct=book.manager.realized_return_pct,
            realized_risk_pct=book.manager.realized_risk_pct,
        )
        pre = self.rails.pre_check(snapshot, state)
        if not pre.allowed:
            return
        if book.brain_calls_today >= self.settings.shadow_max_brain_calls_per_day:
            return
        if book.trades_today >= SHADOW_MAX_TRADES_PER_DAY:
            return
        if len(book.manager.open_trades) >= SHADOW_MAX_OPEN:
            return

        decision = await self.decider.decide(
            snapshot=snapshot,
            playbook=self.playbook,
            open_trades=book.manager.open_trades,
            recent_trades=book.manager.closed_trades[-10:],
            rail_warnings=pre.warnings,
            attention_note=f"SHADOW {book.symbol} (weekly options): {verdict.summary}",
            recent_decisions=book.decisions_today[-4:],
            calendar_events=todays_events(bar.ts),
        )
        self.brain_calls += 1
        book.brain_calls_today += 1
        book.decisions_today.append(decision)

        if decision.action is Action.ENTER and decision.occ_symbol:
            self._force_weekly_expiry(decision)
        self.journal.log_decision(decision, snapshot, [], pre.warnings, verdict.score)

        if decision.action is not Action.ENTER or not decision.occ_symbol:
            return

        fill = book.pricer.price_at(decision.occ_symbol, bar.ts, bar.close, side="entry")
        if fill is None or fill <= 0.05:
            return
        decision.size_factor = recommended_size_factor(decision, bar.ts)
        trade = book.manager.open_trade(decision, fill, snapshot)
        book.trades_today += 1
        self.journal.log_trade(trade)
        log.info(
            "shadow %s: simulated entry %s @ %.2f (conf %d)",
            book.symbol,
            trade.occ_symbol,
            fill,
            decision.confidence,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _force_weekly_expiry(decision: Decision) -> None:
        """Single names have Friday weeklies, not the daily expiries QQQ has.

        The decider resolves expiry_dte on QQQ rules (any weekday), so a
        Tuesday dte=0 would name a contract that does not exist on NVDA.
        Snap whatever it chose to the Friday of that week.
        """
        underlying, expiry, option_type, strike = parse_occ_symbol(decision.occ_symbol)
        weekly = nearest_weekly_expiry(expiry)
        if weekly != expiry:
            decision.occ_symbol = occ_symbol(underlying, weekly, option_type, strike)

    # ------------------------------------------------------------------
    def _roll_session(self, bar: Bar) -> None:
        """New day: flatten every simulated position and reset the books.

        Weekly contracts could in principle be held overnight, but the shadow
        record must stay comparable with the live desk's intraday discipline —
        and an unattended overnight simulation is a fiction generator.
        """
        for book in self.books.values():
            last = book.bars[-1] if book.bars else None
            for trade in list(book.manager.open_trades):
                price = (
                    book.pricer.price_at(trade.occ_symbol, last.ts, last.close, side="exit")
                    if last
                    else None
                ) or 0.01
                book.manager.force_close(
                    trade, price, last.ts if last else bar.ts, "session_close"
                )
                self.journal.log_trade(trade)
            book.roll_day()

    # ------------------------------------------------------------------
    def flatten(self, now: datetime) -> None:
        """Shutdown path: close simulated positions so the journal never
        carries a phantom open trade across a redeploy."""
        for book in self.books.values():
            last = book.bars[-1] if book.bars else None
            for trade in list(book.manager.open_trades):
                price = (
                    book.pricer.price_at(trade.occ_symbol, last.ts, last.close, side="exit")
                    if last
                    else None
                ) or 0.01
                book.manager.force_close(trade, price, last.ts if last else now, "session_close")
                self.journal.log_trade(trade)
