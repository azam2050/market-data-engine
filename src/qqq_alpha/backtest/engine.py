"""Historical replay.

The backtester drives exactly the same objects the live engine will drive —
snapshot builder, attention engine, rails, decider, trade manager. There is no
separate "backtest logic" that could quietly diverge from production behaviour.

It also measures the thing most backtests ignore: the opportunities the system
declined. Every high-attention moment that did not become a trade is priced
forward, so the report can say "our rails cost us N setups that would have paid
+X%". Without that number you cannot tell a disciplined system from a paralysed
one.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime

from qqq_alpha.brain.attention import AttentionEngine
from qqq_alpha.brain.decider import Decider, next_expiry, occ_symbol
from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.brain.rails import DayState, SafetyRails, infeasible
from qqq_alpha.config import Settings
from qqq_alpha.data.pricing import OptionPricer
from qqq_alpha.data.quality import inspect_session
from qqq_alpha.domain import (
    Action,
    Bar,
    Decision,
    FlowEvent,
    MarketSnapshot,
    MissedOpportunity,
    OptionContract,
    OptionType,
    Trade,
)
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal
from qqq_alpha.memory import Memory
from qqq_alpha.trades import TradeManager

log = logging.getLogger(__name__)

WARMUP_BARS = 30
MISSED_LOOKAHEAD_MINUTES = 60
# how many earlier sessions feed the hourly chart — the same number the
# live engine loads, so both build the identical timeframe
HISTORY_SESSIONS = 5


@dataclass
class DayResult:
    day: date
    trades: list[Trade] = field(default_factory=list)
    decisions: list[Decision] = field(default_factory=list)
    brain_calls: int = 0
    attention_wakes: int = 0
    rail_blocks: dict[str, int] = field(default_factory=dict)
    missed: list[MissedOpportunity] = field(default_factory=list)

    @property
    def realized_pct(self) -> float:
        return round(sum(t.return_pct or 0.0 for t in self.trades), 2)


@dataclass
class BacktestResult:
    days: list[DayResult] = field(default_factory=list)
    price_source_is_approximate: bool = False

    @property
    def trades(self) -> list[Trade]:
        return [t for day in self.days for t in day.trades]

    @property
    def missed(self) -> list[MissedOpportunity]:
        return [m for day in self.days for m in day.missed]

    @property
    def brain_calls(self) -> int:
        return sum(day.brain_calls for day in self.days)


class Backtester:
    def __init__(
        self,
        settings: Settings,
        decider: Decider,
        pricer: OptionPricer,
        playbook: Playbook,
        journal: Journal | None = None,
        contracts_by_symbol: dict[str, OptionContract] | None = None,
        memory: Memory | None = None,
    ):
        self.settings = settings
        self.decider = decider
        self.pricer = pricer
        self.playbook = playbook
        self.journal = journal
        self.contracts = contracts_by_symbol or {}
        self.memory = memory
        self.builder = SnapshotBuilder(settings.primary_symbol)

    # ------------------------------------------------------------------
    async def run(
        self,
        sessions: dict[date, list[Bar]],
        leader_sessions: dict[date, dict[str, list[Bar]]] | None = None,
        flow_sessions: dict[date, list[FlowEvent]] | None = None,
        prior_days: dict[date, Bar] | None = None,
    ) -> BacktestResult:
        result = BacktestResult(price_source_is_approximate=self.pricer.is_approximation)
        recent_trades: list[Trade] = []

        ordered = sorted(sessions)
        for position, day in enumerate(ordered):
            # the replay already holds every earlier session, so the hourly
            # chart and the leaders' prior closes cost nothing here. Passing
            # them is what keeps the backtest measuring the SAME engine that
            # runs live — a divergence in either direction makes the record a
            # statement about a system nobody is running.
            previous = ordered[max(0, position - HISTORY_SESSIONS) : position]
            history = [bar for earlier in previous for bar in sessions[earlier]]
            leader_prior_close: dict[str, float] = {}
            if previous and leader_sessions:
                for symbol, bars in (leader_sessions.get(previous[-1]) or {}).items():
                    if bars:
                        leader_prior_close[symbol] = bars[-1].close

            day_result = await self.run_day(
                day=day,
                session_bars=sessions[day],
                leader_bars=(leader_sessions or {}).get(day),
                flow_events=(flow_sessions or {}).get(day),
                prior_day=(prior_days or {}).get(day),
                recent_trades=recent_trades,
                history_bars=history,
                leader_prior_close=leader_prior_close,
            )
            result.days.append(day_result)
            recent_trades.extend(day_result.trades)
            log.info(
                "%s | trades=%d realized=%+.1f%% brain_calls=%d missed=%d",
                day,
                len(day_result.trades),
                day_result.realized_pct,
                day_result.brain_calls,
                len(day_result.missed),
            )

        return result

    # ------------------------------------------------------------------
    async def run_day(
        self,
        day: date,
        session_bars: list[Bar],
        leader_bars: dict[str, list[Bar]] | None = None,
        flow_events: list[FlowEvent] | None = None,
        prior_day: Bar | None = None,
        recent_trades: list[Trade] | None = None,
        history_bars: list[Bar] | None = None,
        leader_prior_close: dict[str, float] | None = None,
    ) -> DayResult:
        result = DayResult(day=day)
        # inspect the day once; the verdict rides along with every snapshot so
        # the rails and the brain both know how much to trust the picture
        quality = inspect_session(session_bars)
        if not quality.is_usable:
            log.warning("%s | skipping session: %s", day, quality.summary())
            result.rail_blocks["unusable_data"] = 1
            return result

        rails = SafetyRails(self.settings)
        attention = AttentionEngine(
            self.settings.attention_threshold, self.settings.attention_cooldown_sec
        )
        manager = TradeManager()
        state = DayState()

        for index in range(WARMUP_BARS, len(session_bars)):
            window = session_bars[: index + 1]
            now = window[-1].ts
            spot = window[-1].close

            # 1. mark open positions to market first
            for trade in list(manager.open_trades):
                price = self._price(trade.occ_symbol, now, spot)
                if price is not None:
                    manager.update(trade, price, now)

            state.open_positions = len(manager.open_trades)
            state.trades_taken = len(result.trades)
            state.realized_return_pct = manager.realized_return_pct
            state.realized_risk_pct = manager.realized_risk_pct

            # 2. build the view of the world
            snapshot = self.builder.build(
                session_bars=window,
                leader_bars=self._slice_leaders(leader_bars, now),
                flow_events=[e for e in (flow_events or []) if e.ts <= now],
                prior_day=prior_day,
                # the replay reads regular-hours bars only, so the overnight
                # range genuinely is not knowable here. Left absent rather
                # than approximated from the session's own open.
                leader_prior_close=leader_prior_close,
                history_bars=history_bars,
                now=now,
                quality=quality,
            )
            snapshot.data_age_sec = 0.0  # replay: bars are by definition current

            # 3. is this moment worth thinking about?
            verdict = attention.evaluate(snapshot)
            if self.journal:
                self.journal.log_attention(
                    now, verdict.score, verdict.should_wake, verdict.summary, verdict.suppressed_by
                )
            if verdict.should_wake:
                result.attention_wakes += 1

            # 4. hard safety layer
            pre = rails.pre_check(snapshot, state)
            for block in pre.blocks:
                key = block.split(":")[0]
                result.rail_blocks[key] = result.rail_blocks.get(key, 0) + 1

            if not verdict.should_wake:
                continue

            if not pre.allowed:
                self._record_missed(result, session_bars, index, snapshot, pre.blocks)
                continue

            # 5. the brain decides
            decision = await self.decider.decide(
                snapshot=snapshot,
                playbook=self.playbook,
                open_trades=manager.open_trades,
                recent_trades=recent_trades or [],
                rail_warnings=pre.warnings,
                attention_note=verdict.summary,
                similar_trades=(
                    self.memory.similar_trades(snapshot, limit=8) if self.memory else None
                ),
            )
            result.brain_calls += 1
            result.decisions.append(decision)

            contract = self.contracts.get(decision.occ_symbol or "")
            post = rails.post_check(decision, contract or self._synthetic_contract(decision, now, spot))

            if self.journal:
                self.journal.log_decision(
                    decision, snapshot, post.blocks, pre.warnings + post.warnings, verdict.score
                )

            if decision.action is not Action.ENTER:
                # the AI looked and passed on its own judgement — price it
                # forward too, not just the setups the rails blocked
                self._record_missed(result, session_bars, index, snapshot, [])
                continue

            if not post.allowed:
                self._record_missed(result, session_bars, index, snapshot, post.blocks)
                continue

            fill = self._price(decision.occ_symbol or "", now, spot)
            if fill is None or fill <= 0:
                result.rail_blocks["unpriceable"] = result.rail_blocks.get("unpriceable", 0) + 1
                continue

            trade = manager.open_trade(decision, fill, snapshot)
            result.trades.append(trade)
            if self.journal:
                self.journal.log_trade(trade)
            if self.memory:
                self.memory.remember_trade(trade, snapshot)

        # 6. nothing survives the close on 0DTE
        if session_bars:
            final = session_bars[-1]
            for trade in list(manager.open_trades):
                price = self._price(trade.occ_symbol, final.ts, final.close) or 0.01
                manager.force_close(trade, price, final.ts, "session_close")

        if self.memory:
            for trade in result.trades:
                self.memory.remember_trade(trade)
            for missed in result.missed:
                self.memory.remember_missed(missed)
        if self.journal:
            for trade in result.trades:
                self.journal.log_trade(trade)
            for missed in result.missed:
                self.journal.log_missed(missed)

        return result

    # ------------------------------------------------------------------
    def _slice_leaders(
        self, leader_bars: dict[str, list[Bar]] | None, now: datetime
    ) -> dict[str, list[Bar]] | None:
        if not leader_bars:
            return None
        return {
            symbol: [b for b in bars if b.ts <= now] for symbol, bars in leader_bars.items()
        }

    def _price(self, occ: str, ts: datetime, spot: float) -> float | None:
        if not occ:
            return None
        return self.pricer.price_at(occ, ts, spot)

    def _synthetic_contract(
        self, decision: Decision, now: datetime, spot: float
    ) -> OptionContract | None:
        """Stand-in contract so the rails can validate shape when we lack a chain."""
        if decision.action is not Action.ENTER or not decision.occ_symbol:
            return None
        from qqq_alpha.data.massive import parse_occ_symbol

        underlying, expiry, option_type, strike = parse_occ_symbol(decision.occ_symbol)
        price = self._price(decision.occ_symbol, now, spot)
        if price is None:
            return None
        # assume a realistic 2% spread when no quote data exists
        return OptionContract(
            occ_symbol=decision.occ_symbol,
            underlying=underlying,
            option_type=option_type,
            strike=strike,
            expiry=expiry,
            bid=round(price * 0.99, 2),
            ask=round(price * 1.01, 2),
            last=price,
            volume=500,
            open_interest=1000,
        )

    def _record_missed(
        self,
        result: DayResult,
        session_bars: list[Bar],
        index: int,
        snapshot: MarketSnapshot,
        blocked_by: list[str],
    ) -> None:
        """Price forward what we would have made had we taken the obvious trade.

        Uses an at-the-money contract in the direction the evidence leaned. It is
        an estimate, not a promise — but a consistently large number here means
        the engine is too cautious, and that is worth knowing. Covers both a rail
        block and the AI's own PASS — a caller passes an empty ``blocked_by`` for
        the latter. An infeasible decline (market closed, broken data) is not
        recorded at all: a trade that could not exist was not missed.
        """
        bias = snapshot.net_bias
        if abs(bias) < 0.2 or infeasible(blocked_by):
            return

        now, spot = snapshot.ts, snapshot.underlying.close
        direction = OptionType.CALL if bias > 0 else OptionType.PUT
        strike = round(spot)
        symbol = occ_symbol(
            session_bars[-1].symbol if session_bars else "QQQ",
            next_expiry(now.date(), 0),
            direction,
            strike,
        )
        entry = self._price(symbol, now, spot)
        if entry is None or entry <= 0:
            return

        best = entry
        horizon = min(index + MISSED_LOOKAHEAD_MINUTES, len(session_bars) - 1)
        for future in session_bars[index + 1 : horizon + 1]:
            price = self._price(symbol, future.ts, future.close)
            if price is not None:
                best = max(best, price)

        peak_pct = round((best - entry) / entry * 100.0, 1)
        if peak_pct < self.settings.min_target_return_pct:
            return

        result.missed.append(
            MissedOpportunity(
                ts=now,
                reason="blocked before the brain could act"
                if blocked_by
                else "brain declined",
                would_be_direction=direction,
                occ_symbol=symbol,
                hypothetical_entry=round(entry, 2),
                best_price_after=round(best, 2),
                peak_return_pct=peak_pct,
                blocked_by=blocked_by,
                regime=snapshot.regime.value,
                session_minute=snapshot.session_minute,
            )
        )


def sessions_from_bars(bars: list[Bar]) -> dict[date, list[Bar]]:
    """Group a flat list of minute bars into calendar sessions."""
    grouped: dict[date, list[Bar]] = {}
    for bar in bars:
        grouped.setdefault(bar.ts.date(), []).append(bar)
    for day_bars in grouped.values():
        day_bars.sort(key=lambda b: b.ts)
    return grouped


def prior_day_map(daily: list[Bar]) -> dict[date, Bar]:
    """Map each session to the previous session's daily bar."""
    ordered = sorted(daily, key=lambda b: b.ts)
    mapping: dict[date, Bar] = {}
    for i in range(1, len(ordered)):
        mapping[ordered[i].ts.date()] = ordered[i - 1]
    return mapping
