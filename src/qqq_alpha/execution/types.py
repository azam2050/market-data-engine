"""The vocabulary every broker adapter speaks.

Deliberately smaller than any real broker API. A broker will offer bracket
orders, trailing stops, OCO groups and a dozen time-in-force variants; this
engine needs none of them, because the exit logic already lives in
``TradeManager`` and duplicating it broker-side would create two authorities
over one position that can disagree during a disconnect.

So the contract is: we send a plain order, we ask what happened to it, and we
ask what we own. Everything else stays here, where it is tested.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderState(StrEnum):
    """Where an order is in its life.

    ``PARTIAL`` is the state that punishes engines written without it: you
    asked for four contracts, two filled, and an engine that only understands
    "filled" or "not filled" will either think it owns nothing or think it
    owns four.
    """

    PENDING = "PENDING"  # accepted by us, not yet acknowledged by the broker
    WORKING = "WORKING"  # live at the broker, unfilled
    PARTIAL = "PARTIAL"  # some quantity filled, the rest still working
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

    @property
    def is_final(self) -> bool:
        return self in (OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED)


class OrderRequest(BaseModel):
    """What the engine wants done.

    ``limit_price`` is required rather than optional, and there is no market
    order in this vocabulary at all. A market order on a 0DTE option is how a
    thin book takes a bite out of you that no backtest ever showed: the engine
    already prices entries at the ask and exits at the bid, so it always knows
    a price it is willing to accept, and sending anything else would make the
    live record diverge from the paper one for no gain.
    """

    client_order_id: str = Field(description="ours, unique, and the key we reconcile on")
    occ_symbol: str
    side: Side
    quantity: int = Field(gt=0, description="contracts, never shares")
    limit_price: float = Field(gt=0)
    trade_id: str = Field(default="", description="the engine trade this belongs to")
    reason: str = Field(default="", description="entry, scale_out, stop, session_close…")

    def describe(self) -> str:
        return (
            f"{self.side.value} {self.quantity}x {self.occ_symbol} "
            f"@ {self.limit_price:.2f} ({self.reason or 'unspecified'})"
        )


class BrokerOrder(BaseModel):
    """What the broker says about an order we sent."""

    client_order_id: str
    broker_order_id: str = ""
    state: OrderState = OrderState.PENDING
    filled_quantity: int = 0
    average_fill_price: float | None = None
    submitted_at: datetime | None = None
    updated_at: datetime | None = None
    message: str = Field(default="", description="the broker's own words on a rejection")

    @property
    def is_final(self) -> bool:
        return self.state.is_final


class BrokerPosition(BaseModel):
    """What the broker says we own — the only authority on that question."""

    occ_symbol: str
    quantity: int
    average_price: float | None = None


class BrokerAccount(BaseModel):
    """Enough to answer "can this order even be paid for?"."""

    account_id: str = ""
    cash: float | None = None
    buying_power: float | None = None
    currency: str = "USD"


class BrokerError(RuntimeError):
    """A broker refused, timed out, or answered something we cannot parse.

    Raised by adapters so the router can decide — never swallowed inside an
    adapter, because an order whose fate is unknown must not look like an
    order that was never sent.
    """
