"""Domain models shared by every layer of the engine.

Design note: nothing in this module makes decisions. It only describes what the
engine can observe (bars, flow, snapshots) and what it can conclude (decisions,
trades). Keeping these separate is what lets the backtester, the live engine and
the learning loop all speak the same language.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field


# --------------------------------------------------------------------------
# Market data
# --------------------------------------------------------------------------
class Bar(BaseModel):
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float | None = None
    transactions: int | None = None
    """Number of individual trades in the bar. Volume tells you how much changed
    hands; this tells you across how many decisions — a large volume spread over
    few trades is one institution, over many trades it is the crowd."""


class OptionType(StrEnum):
    CALL = "CALL"
    PUT = "PUT"


class OptionContract(BaseModel):
    """A single tradeable option contract with its live pricing."""

    occ_symbol: str
    underlying: str
    option_type: OptionType
    strike: float
    expiry: date
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    volume: int = 0
    open_interest: int = 0
    implied_volatility: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None

    @property
    def mid(self) -> float | None:
        if self.bid is None or self.ask is None or self.bid <= 0 or self.ask <= 0:
            return self.last
        return round((self.bid + self.ask) / 2.0, 4)

    @property
    def spread_pct(self) -> float | None:
        mid = self.mid
        if mid is None or mid <= 0 or self.bid is None or self.ask is None:
            return None
        return round((self.ask - self.bid) / mid * 100.0, 2)

    def dte(self, as_of: date) -> int:
        return (self.expiry - as_of).days


class FlowKind(StrEnum):
    SWEEP = "SWEEP"
    BLOCK = "BLOCK"
    SPLIT = "SPLIT"
    NORMAL = "NORMAL"


class FlowEvent(BaseModel):
    """One institutional-sized options print — the 'whale' primitive."""

    ts: datetime
    occ_symbol: str
    underlying: str
    option_type: OptionType
    strike: float
    expiry: date
    price: float
    size: int
    premium: float
    kind: FlowKind = FlowKind.NORMAL
    aggressor: Literal["BUY", "SELL", "MID"] = "MID"
    exchanges: int = 1

    @property
    def is_bullish(self) -> bool:
        buy_call = self.option_type is OptionType.CALL and self.aggressor == "BUY"
        sell_put = self.option_type is OptionType.PUT and self.aggressor == "SELL"
        return buy_call or sell_put


class FlowSummary(BaseModel):
    window_minutes: int
    call_premium: float = 0.0
    put_premium: float = 0.0
    sweep_count: int = 0
    block_count: int = 0
    net_premium: float = 0.0
    call_put_ratio: float | None = None
    urgency: float = 0.0  # 0..1, how aggressive the tape is right now
    notable: list[FlowEvent] = Field(default_factory=list)


# --------------------------------------------------------------------------
# Observations — soft signals, never vetoes
# --------------------------------------------------------------------------
class Observation(BaseModel):
    """A single piece of evidence handed to the brain.

    `score` is directional: +1 strongly bullish, -1 strongly bearish, 0 neutral.
    `confidence` is how much we trust the reading itself (data quality, sample).
    Nothing here can reject a trade — the brain weighs it.
    """

    name: str
    category: Literal["trend", "momentum", "volatility", "level", "flow", "context"]
    value: float | str | None = None
    score: float = 0.0
    confidence: float = 1.0
    note: str = ""


class MarketRegime(StrEnum):
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    RANGING = "RANGING"
    VOLATILE_CHOP = "VOLATILE_CHOP"
    UNKNOWN = "UNKNOWN"


class MarketSnapshot(BaseModel):
    """Everything the brain sees at one moment in time."""

    ts: datetime
    session_minute: int  # minutes since the 09:30 ET open
    underlying: Bar
    # the raw tape, not a summary of it. Indicators are somebody else's
    # opinion about the candles; a trader reads the candles. Without these
    # the brain cannot see an engulfing bar, a rejection wick, or a
    # narrowing range — it could only see what an EMA had already averaged away.
    recent_bars_1m: list[Bar] = Field(default_factory=list)
    recent_bars_5m: list[Bar] = Field(default_factory=list)
    leaders: list[Bar] = Field(default_factory=list)
    indicators: dict[str, float | None] = Field(default_factory=dict)
    timeframes: dict[str, dict[str, float | None]] = Field(
        default_factory=dict,
        description="Indicator pack per timeframe: 1m for timing, 5m for structure, 15m for trend",
    )
    levels: dict[str, float | None] = Field(default_factory=dict)
    flow: FlowSummary | None = None
    regime: MarketRegime = MarketRegime.UNKNOWN
    observations: list[Observation] = Field(default_factory=list)
    events: list[str] = Field(default_factory=list)  # scheduled macro events nearby
    data_age_sec: float = 0.0
    data_quality: str = ""
    data_usable: bool = True

    @property
    def net_bias(self) -> float:
        """Weighted directional lean of all observations. Advisory only."""
        if not self.observations:
            return 0.0
        total = sum(o.score * o.confidence for o in self.observations)
        weight = sum(abs(o.confidence) for o in self.observations) or 1.0
        return round(total / weight, 3)


# --------------------------------------------------------------------------
# Decisions
# --------------------------------------------------------------------------
class Action(StrEnum):
    ENTER = "ENTER"
    WAIT = "WAIT"
    PASS = "PASS"


class Target(BaseModel):
    label: str
    price: float
    return_pct: float
    take_pct: int = Field(default=50, description="portion of position to close, %")


class Decision(BaseModel):
    """The brain's verdict. Produced by the AI, never by a rule table."""

    ts: datetime
    action: Action
    direction: OptionType | None = None
    occ_symbol: str | None = None
    entry_price: float | None = None
    entry_zone: tuple[float, float] | None = None
    targets: list[Target] = Field(default_factory=list)
    stop_price: float | None = None
    stop_return_pct: float | None = None
    confidence: int = Field(default=0, ge=0, le=10)
    thesis: str = ""
    risks: list[str] = Field(default_factory=list)
    playbook_refs: list[str] = Field(default_factory=list)
    overrides: list[str] = Field(
        default_factory=list,
        description="playbook guidance the brain deliberately went against, with reasons",
    )
    invalidation: str = ""
    invalidation_level: float | None = Field(
        default=None,
        description="the UNDERLYING price that proves the thesis wrong; the "
        "engine exits the moment spot crosses it",
    )
    expected_hold_minutes: int | None = None
    size_factor: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="recommended position size as a fraction of normal, set by "
        "the engine from confidence and time of day",
    )
    raw: dict[str, Any] = Field(default_factory=dict)


class RailVerdict(BaseModel):
    """Result of the safety layer. Only execution-feasibility blocks live here."""

    allowed: bool
    blocks: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# --------------------------------------------------------------------------
# Trades
# --------------------------------------------------------------------------
class TradeStatus(StrEnum):
    OPEN = "OPEN"
    CLOSED_WIN = "CLOSED_WIN"
    CLOSED_LOSS = "CLOSED_LOSS"
    CLOSED_FLAT = "CLOSED_FLAT"
    EXPIRED = "EXPIRED"


class TradeUpdate(BaseModel):
    ts: datetime
    price: float
    return_pct: float
    note: str = ""


class Trade(BaseModel):
    trade_id: str
    opened_at: datetime
    decision: Decision
    occ_symbol: str
    entry_price: float
    status: TradeStatus = TradeStatus.OPEN
    updates: list[TradeUpdate] = Field(default_factory=list)
    closed_at: datetime | None = None
    exit_price: float | None = None
    return_pct: float | None = None
    max_favorable_pct: float = 0.0
    max_adverse_pct: float = 0.0
    exit_reason: str = ""
    # scale-out bookkeeping: the banked half's contribution to whole-position
    # P&L, and how much of the position is still open (1.0 = never scaled)
    banked_return_pct: float = 0.0
    open_fraction: float = 1.0
    # true when this trade is the week's live public share: its entry card
    # went to the channel before the outcome existed, and every update
    # follows it there. The weekly report tags these rows.
    shared_to_channel: bool = False
    snapshot_at_entry: MarketSnapshot | None = None

    @property
    def is_open(self) -> bool:
        return self.status is TradeStatus.OPEN


class MissedOpportunity(BaseModel):
    """A setup the engine did NOT take, scored after the fact.

    This is the antidote to over-filtering: we measure what our caution cost us.
    """

    ts: datetime
    reason: str
    would_be_direction: OptionType
    occ_symbol: str
    hypothetical_entry: float
    best_price_after: float
    peak_return_pct: float
    blocked_by: list[str] = Field(default_factory=list)
    # market fingerprint at the moment declined, so the learning loop can ask
    # "which regime is caution costing us the most in?"
    regime: str | None = None
    session_minute: int | None = None
