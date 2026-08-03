"""Safety rails.

Only two kinds of rule are allowed to live here:

1. **Execution feasibility** — the trade physically cannot be taken or filled
   sensibly (no data, market closed, dead contract).
2. **Capital protection** — hard stops on how much damage one day can do.

Nothing about *market opinion* belongs in this file. Whether a setup is good is
the brain's judgement. Confusing the two is what turns a trading system into a
box that never trades.

Every rail is individually configurable, and each one records exactly what it
blocked so the backtest can price the cost of the rail itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time

from qqq_alpha.config import MARKET_TZ, REGULAR_CLOSE, REGULAR_OPEN, Settings
from qqq_alpha.domain import (
    Action,
    Decision,
    MarketSnapshot,
    OptionContract,
    RailVerdict,
)

MAX_ACCEPTABLE_SPREAD_PCT = 15.0


@dataclass
class DayState:
    """Everything the rails need to know about today so far."""

    trades_taken: int = 0
    open_positions: int = 0
    realized_return_pct: float = 0.0


class SafetyRails:
    def __init__(self, settings: Settings):
        self.settings = settings

    # ------------------------------------------------------------------
    def pre_check(self, snapshot: MarketSnapshot, state: DayState) -> RailVerdict:
        """Run before the brain is woken. Cheap, and blocks only hard stops."""
        blocks: list[str] = []
        warnings: list[str] = []

        if snapshot.data_age_sec > self.settings.max_data_age_sec:
            blocks.append(
                f"stale_data: last bar is {snapshot.data_age_sec:.0f}s old "
                f"(limit {self.settings.max_data_age_sec}s)"
            )

        local_time = snapshot.ts.astimezone(MARKET_TZ).time()
        if not _within(local_time, REGULAR_OPEN, REGULAR_CLOSE):
            blocks.append(f"outside_session: {local_time.strftime('%H:%M')} ET")
        elif local_time > self.settings.last_entry_time:
            blocks.append(
                f"past_entry_cutoff: {local_time.strftime('%H:%M')} ET "
                f"> {self.settings.last_entry_time_et}"
            )

        if state.trades_taken >= self.settings.max_trades_per_day:
            blocks.append(
                f"daily_trade_cap: {state.trades_taken}/{self.settings.max_trades_per_day}"
            )

        if state.open_positions >= self.settings.max_open_positions:
            blocks.append(
                f"position_cap: {state.open_positions}/{self.settings.max_open_positions} open"
            )

        if state.realized_return_pct <= -abs(self.settings.daily_loss_circuit_breaker_pct):
            blocks.append(
                f"circuit_breaker: day at {state.realized_return_pct:.1f}% "
                f"(limit -{self.settings.daily_loss_circuit_breaker_pct}%)"
            )

        if snapshot.events:
            warnings.append(f"scheduled_event_nearby: {', '.join(snapshot.events)}")

        return RailVerdict(allowed=not blocks, blocks=blocks, warnings=warnings)

    # ------------------------------------------------------------------
    def post_check(
        self, decision: Decision, contract: OptionContract | None
    ) -> RailVerdict:
        """Run after the brain has chosen. Validates the trade is executable."""
        blocks: list[str] = []
        warnings: list[str] = []

        if decision.action is not Action.ENTER:
            return RailVerdict(allowed=True)

        if contract is None:
            blocks.append("contract_not_found: brain named a contract we cannot price")
            return RailVerdict(allowed=False, blocks=blocks)

        if contract.mid is None or contract.mid <= 0:
            blocks.append(f"contract_untradeable: no usable price for {contract.occ_symbol}")

        spread = contract.spread_pct
        if spread is not None and spread > MAX_ACCEPTABLE_SPREAD_PCT:
            blocks.append(
                f"spread_too_wide: {spread:.1f}% > {MAX_ACCEPTABLE_SPREAD_PCT}% "
                f"on {contract.occ_symbol}"
            )
        elif spread is not None and spread > 8.0:
            warnings.append(f"wide_spread: {spread:.1f}% — round trip costs more than it looks")

        if not decision.targets:
            blocks.append("no_targets: an entry without a target is not a plan")
        else:
            best = max(t.return_pct for t in decision.targets)
            if best < self.settings.min_target_return_pct:
                warnings.append(
                    f"below_target_bar: best target {best:.0f}% "
                    f"< configured {self.settings.min_target_return_pct:.0f}%"
                )

        # the stop is expressed as a % on the contract; the absolute price only
        # exists once we know the fill, so validate the intent, not the price
        if decision.stop_return_pct is None:
            blocks.append("no_stop: entries require a defined invalidation level")
        elif decision.stop_return_pct >= 0:
            blocks.append(
                f"invalid_stop: {decision.stop_return_pct}% is not a loss threshold"
            )

        if contract.open_interest < 50 and contract.volume < 20:
            warnings.append(
                f"thin_contract: OI={contract.open_interest}, vol={contract.volume}"
            )

        return RailVerdict(allowed=not blocks, blocks=blocks, warnings=warnings)


def _within(value: time, start: time, end: time) -> bool:
    return start <= value <= end
