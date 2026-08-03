"""Trade lifecycle management.

One object owns an open position from entry to exit: it applies targets, moves
the stop, enforces the time stop, and emits the update messages a subscriber
would receive. The backtester and the live engine both drive it the same way, so
what you validate historically is literally what runs in production.
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

BREAKEVEN_ARM_PCT = 40.0
TRAIL_GIVEBACK_PCT = 35.0


@dataclass
class ExitPolicy:
    """How a position is managed once open. All values are percentages."""

    stop_return_pct: float = -40.0
    breakeven_arm_pct: float = BREAKEVEN_ARM_PCT
    trail_giveback_pct: float = TRAIL_GIVEBACK_PCT
    time_stop_minutes: int | None = None
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
    def update(self, trade: Trade, price: float, now: datetime) -> TradeUpdate | None:
        """Mark a trade to market and act on it. Returns an update worth sending."""
        if not trade.is_open or trade.entry_price <= 0:
            return None

        return_pct = round((price - trade.entry_price) / trade.entry_price * 100.0, 2)
        trade.max_favorable_pct = max(trade.max_favorable_pct, return_pct)
        trade.max_adverse_pct = min(trade.max_adverse_pct, return_pct)

        # --- hard stop ---
        stop_pct = trade.decision.stop_return_pct or self.policy.stop_return_pct
        if return_pct <= stop_pct:
            return self._close(trade, price, now, "stop_hit")

        # --- trailing exit after the position has run ---
        if trade.max_favorable_pct >= self.policy.breakeven_arm_pct:
            giveback = trade.max_favorable_pct - return_pct
            if giveback >= self.policy.trail_giveback_pct:
                return self._close(trade, price, now, "trail_stop")

        # --- time stop ---
        expected = trade.decision.expected_hold_minutes or self.policy.time_stop_minutes
        if expected:
            elapsed = (now - trade.opened_at).total_seconds() / 60.0
            if elapsed >= expected * 1.5 and return_pct < 15.0:
                return self._close(trade, price, now, "time_stop")

        # --- target notifications ---
        for target in trade.decision.targets:
            already = any(t.note.startswith(f"target:{target.label}") for t in trade.updates)
            if not already and return_pct >= target.return_pct:
                update = TradeUpdate(
                    ts=now,
                    price=price,
                    return_pct=return_pct,
                    note=f"target:{target.label} reached (+{target.return_pct:.0f}%) — "
                    f"take {target.take_pct}%, move stop to breakeven",
                )
                trade.updates.append(update)
                return update

        # --- periodic heartbeat so a subscriber is never left guessing ---
        last_update = trade.updates[-1].ts if trade.updates else trade.opened_at
        if now - last_update >= timedelta(minutes=15):
            update = TradeUpdate(
                ts=now, price=price, return_pct=return_pct, note="status: still open"
            )
            trade.updates.append(update)
            return update

        return None

    # ------------------------------------------------------------------
    def force_close(self, trade: Trade, price: float, now: datetime, reason: str) -> TradeUpdate:
        return self._close(trade, price, now, reason)

    def _close(self, trade: Trade, price: float, now: datetime, reason: str) -> TradeUpdate:
        return_pct = round((price - trade.entry_price) / trade.entry_price * 100.0, 2)
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
