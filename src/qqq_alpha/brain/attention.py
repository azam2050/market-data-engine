"""The attention engine — decides WHEN to wake the brain, never WHAT to trade.

This distinction matters. Calling a frontier model on every one of the 390
minutes in a session, across a year of backtesting, is financially absurd. So
something cheap has to choose the moments worth thinking about.

That something is deliberately *loose*. It fires on activity, not on opinion:
volatility expansion, level proximity, volume surges, flow bursts, regime
changes. It has no view on direction and cannot reject a trade. And every
moment it declines to wake the brain is still journalled with its score, so the
backtest report can answer the only question that matters here — "what did our
attention filter cost us?"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from qqq_alpha.domain import MarketSnapshot


@dataclass
class AttentionSignal:
    name: str
    weight: float
    note: str


@dataclass
class AttentionVerdict:
    score: float
    should_wake: bool
    signals: list[AttentionSignal] = field(default_factory=list)
    suppressed_by: str | None = None

    @property
    def summary(self) -> str:
        if not self.signals:
            return "quiet tape"
        return ", ".join(f"{s.name}({s.weight:.2f})" for s in self.signals)


class AttentionEngine:
    def __init__(self, threshold: float = 0.45, cooldown_sec: int = 180):
        self.threshold = threshold
        self.cooldown = timedelta(seconds=cooldown_sec)
        self._last_wake: datetime | None = None

    def reset(self) -> None:
        self._last_wake = None

    def evaluate(self, snapshot: MarketSnapshot) -> AttentionVerdict:
        signals: list[AttentionSignal] = []
        ind = snapshot.indicators

        # 1. Unusual participation
        rel_vol = ind.get("rel_volume")
        if rel_vol and rel_vol >= 1.5:
            signals.append(
                AttentionSignal("volume_surge", min((rel_vol - 1.0) / 2.0, 1.0), f"relvol {rel_vol}")
            )

        # 2. Momentum expansion in either direction
        for key, scale in (("mom_5m", 0.25), ("mom_15m", 0.45)):
            value = ind.get(key)
            if value is not None and abs(value) >= scale:
                signals.append(
                    AttentionSignal(f"{key}_expansion", min(abs(value) / (scale * 3), 1.0), f"{key}={value}")
                )

        # 3. Price pressed against a level — where breaks and rejections happen
        for obs in snapshot.observations:
            if not isinstance(obs.value, float):
                continue
            if obs.name == "level_proximity" and obs.value <= 0.12:
                signals.append(AttentionSignal("at_level", 1.0 - obs.value / 0.12, obs.note))
            elif obs.name == "range_position" and (obs.value >= 0.95 or obs.value <= 0.05):
                signals.append(
                    AttentionSignal("range_edge", 0.6, f"range position {obs.value}")
                )

        # 4. VWAP interaction — reclaims and rejections both live here
        vwap_dev = ind.get("vwap_dev_pct")
        if vwap_dev is not None and abs(vwap_dev) <= 0.05:
            signals.append(AttentionSignal("vwap_touch", 0.5, f"vwap dev {vwap_dev}%"))
        elif vwap_dev is not None and abs(vwap_dev) >= 0.45:
            signals.append(AttentionSignal("vwap_extended", 0.55, f"vwap dev {vwap_dev}%"))

        # 5. Institutional flow burst
        if snapshot.flow and snapshot.flow.urgency >= 0.35:
            signals.append(
                AttentionSignal(
                    "flow_burst",
                    min(snapshot.flow.urgency * 1.3, 1.0),
                    f"{snapshot.flow.sweep_count} sweeps / {snapshot.flow.block_count} blocks",
                )
            )

        # 6. Strong overall directional agreement across all evidence
        bias = abs(snapshot.net_bias)
        if bias >= 0.35:
            signals.append(AttentionSignal("evidence_alignment", min(bias * 1.4, 1.0), f"net bias {snapshot.net_bias}"))

        # 7. Opening drive and closing hour always deserve a look
        if snapshot.session_minute <= 20:
            signals.append(AttentionSignal("opening_window", 0.45, "first 20 minutes"))
        elif snapshot.session_minute >= 330:
            signals.append(AttentionSignal("power_hour", 0.4, "final hour"))

        # Combine: strongest signal dominates, others add diminishing weight.
        # A single decisive event should be enough to wake the brain.
        if signals:
            ordered = sorted(signals, key=lambda s: s.weight, reverse=True)
            score = ordered[0].weight
            for extra in ordered[1:]:
                score += extra.weight * 0.25 * (1.0 - score)
            score = round(min(score, 1.0), 3)
        else:
            score = 0.0

        suppressed = None
        should_wake = score >= self.threshold
        if (
            should_wake
            and self._last_wake is not None
            and snapshot.ts - self._last_wake < self.cooldown
        ):
            should_wake = False
            suppressed = "cooldown"

        if should_wake:
            self._last_wake = snapshot.ts

        return AttentionVerdict(
            score=score, should_wake=should_wake, signals=signals, suppressed_by=suppressed
        )
