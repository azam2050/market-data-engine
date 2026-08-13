"""Trade lifecycle management.

One object owns an open position from entry to exit: it applies targets, moves
the stop, enforces the time stop, and emits the update messages a subscriber
would receive. The backtester and the live engine both drive it the same way, so
what you validate historically is literally what runs in production.

The exit geometry is deliberately asymmetric, because the engine's own
missed-opportunity record says 0DTE winners have a fat right tail (avg peak
+156%, best +614%) while the losers cluster at the stop. So instead of a fixed
+50% target that amputates the right tail while the -40% stop eats the whole
left one, the manager banks half the position early to secure the cost, floors
the remainder at breakeven, and trails the rest so a runner is allowed to run.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from qqq_alpha.domain import (
    Decision,
    MarketSnapshot,
    Target,
    Trade,
    TradeStatus,
    TradeUpdate,
)

SCALE_OUT_TRIGGER_PCT = 35.0
TRAIL_GIVEBACK_PCT = 25.0
DEFAULT_TIME_STOP_MINUTES = 15


@dataclass
class ExitPolicy:
    """How a position is managed once open. All values are percentages."""

    # catastrophic backstop only — the working exits (scale-out, breakeven
    # floor, trail, time stop, thesis stop) should all fire long before this
    stop_return_pct: float = -40.0
    # at this contract gain, bank half the position: the trade can no longer
    # lose money, and the remainder is free to chase the fat right tail
    scale_out_trigger_pct: float = SCALE_OUT_TRIGGER_PCT
    scale_out_fraction: float = 0.5
    # once the scale-out has armed the trail, give back at most this much
    # from the peak before the remainder is closed
    trail_giveback_pct: float = TRAIL_GIVEBACK_PCT
    # a position that goes nowhere is theta bleed, not patience — if the brain
    # gave no expected hold, this fallback kicks in
    time_stop_minutes: int | None = DEFAULT_TIME_STOP_MINUTES
    hard_close_minutes_before_expiry: int = 20


class TradeManager:
    """Owns open trades and produces follow-up updates."""

    def __init__(self, policy: ExitPolicy | None = None):
        self.policy = policy or ExitPolicy()
        self.open_trades: list[Trade] = []
        self.closed_trades: list[Trade] = []

    # ------------------------------------------------------------------
    def open_trade(
        self, decision: Decision, fill_price: float, snapshot: MarketSnapshot
    ) -> Trade:
        targets = [
            Target(
                label=t.label,
                price=round(fill_price * (1 + t.return_pct / 100.0), 2),
                return_pct=t.return_pct,
                take_pct=t.take_pct,
            )
            for t in decision.targets
        ]
        stop_pct = decision.stop_return_pct if decision.stop_return_pct is not None else self.policy.stop_return_pct
        priced_decision = decision.model_copy(
            update={
                "entry_price": fill_price,
                "targets": targets,
                "stop_price": round(fill_price * (1 + stop_pct / 100.0), 2),
                "stop_return_pct": stop_pct,
            }
        )

        trade = Trade(
            trade_id=uuid.uuid4().hex[:12],
            opened_at=snapshot.ts,
            decision=priced_decision,
            occ_symbol=decision.occ_symbol or "",
            entry_price=fill_price,
            snapshot_at_entry=snapshot,
        )
        self.open_trades.append(trade)
        return trade

    # ------------------------------------------------------------------
    @staticmethod
    def blended_return_pct(trade: Trade, leg_return_pct: float) -> float:
        """Whole-position P&L: the banked half plus the still-open fraction."""
        return round(
            trade.banked_return_pct + trade.open_fraction * leg_return_pct, 2
        )

    def check_thesis(self, trade: Trade, spot: float) -> bool:
        """Has the underlying crossed the level where the thesis is wrong?

        The brain names a spot level with every entry; price P&L is noise on a
        0DTE contract, but the underlying crossing the invalidation level is a
        fact about the idea itself. The caller closes with reason
        ``thesis_invalidated`` when this returns True.
        """
        level = trade.decision.invalidation_level
        direction = trade.decision.direction
        if level is None or direction is None or not trade.is_open:
            return False
        if direction.value == "CALL":
            return spot <= level
        return spot >= level

    def update(self, trade: Trade, price: float, now: datetime) -> TradeUpdate | None:
        """Mark a trade to market and act on it. Returns an update worth sending."""
        if not trade.is_open or trade.entry_price <= 0:
            return None

        leg_return = round((price - trade.entry_price) / trade.entry_price * 100.0, 2)
        trade.max_favorable_pct = max(trade.max_favorable_pct, leg_return)
        trade.max_adverse_pct = min(trade.max_adverse_pct, leg_return)

        # --- breakeven floor once half is banked: the trade cannot go red.
        # checked before the catastrophic stop because it triggers far earlier
        # and names what actually happened ---
        if trade.open_fraction < 1.0 and leg_return <= 0.0:
            return self._close(trade, price, now, "breakeven_stop")

        # --- catastrophic stop (backstop, not the plan) ---
        stop_pct = trade.decision.stop_return_pct or self.policy.stop_return_pct
        if leg_return <= stop_pct:
            return self._close(trade, price, now, "stop_hit")

        # --- trail the runner: armed by the scale-out trigger ---
        if trade.max_favorable_pct >= self.policy.scale_out_trigger_pct:
            giveback = trade.max_favorable_pct - leg_return
            if giveback >= self.policy.trail_giveback_pct:
                return self._close(trade, price, now, "trail_stop")

        # --- bank half at the trigger: secures the cost, frees the runner ---
        if trade.open_fraction >= 1.0 and leg_return >= self.policy.scale_out_trigger_pct:
            taken = self.policy.scale_out_fraction
            trade.banked_return_pct = round(taken * leg_return, 2)
            trade.open_fraction = round(1.0 - taken, 2)
            update = TradeUpdate(
                ts=now,
                price=price,
                return_pct=leg_return,
                note=(
                    f"scale_out: +{leg_return:.0f}% — بيع نصف الكمية الآن لتأمين التكلفة؛ "
                    "النصف الباقي يركض بوقف عند التعادل ووقف متحرك من القمة"
                ),
            )
            trade.updates.append(update)
            return update

        # --- time stop: a thesis that never moved is being eaten by theta ---
        expected = trade.decision.expected_hold_minutes or self.policy.time_stop_minutes
        if expected:
            elapsed = (now - trade.opened_at).total_seconds() / 60.0
            if elapsed >= expected * 1.5 and leg_return < 15.0:
                return self._close(trade, price, now, "time_stop")

        # --- target notifications (the brain's own map, still worth relaying) ---
        for target in trade.decision.targets:
            already = any(t.note.startswith(f"target:{target.label}") for t in trade.updates)
            if not already and leg_return >= target.return_pct:
                update = TradeUpdate(
                    ts=now,
                    price=price,
                    return_pct=leg_return,
                    note=f"target:{target.label} reached (+{target.return_pct:.0f}%)",
                )
                trade.updates.append(update)
                return update

        # --- periodic heartbeat so a subscriber is never left guessing ---
        last_update = trade.updates[-1].ts if trade.updates else trade.opened_at
        if now - last_update >= timedelta(minutes=15):
            update = TradeUpdate(
                ts=now, price=price, return_pct=leg_return, note="status: still open"
            )
            trade.updates.append(update)
            return update

        return None

    # ------------------------------------------------------------------
    def force_close(self, trade: Trade, price: float, now: datetime, reason: str) -> TradeUpdate:
        return self._close(trade, price, now, reason)

    def _close(self, trade: Trade, price: float, now: datetime, reason: str) -> TradeUpdate:
        leg_return = round((price - trade.entry_price) / trade.entry_price * 100.0, 2)
        return_pct = self.blended_return_pct(trade, leg_return)
        trade.exit_price = price
        trade.closed_at = now
        trade.return_pct = return_pct
        trade.exit_reason = reason
        if return_pct > 1.0:
            trade.status = TradeStatus.CLOSED_WIN
        elif return_pct < -1.0:
            trade.status = TradeStatus.CLOSED_LOSS
        else:
            trade.status = TradeStatus.CLOSED_FLAT

        update = TradeUpdate(
            ts=now,
            price=price,
            return_pct=return_pct,
            note=f"closed:{reason} ({return_pct:+.1f}%)",
        )
        trade.updates.append(update)

        if trade in self.open_trades:
            self.open_trades.remove(trade)
        self.closed_trades.append(trade)
        return update

    # ------------------------------------------------------------------
    @property
    def realized_return_pct(self) -> float:
        return round(sum(t.return_pct or 0.0 for t in self.closed_trades), 2)
