"""The port every broker adapter plugs into.

Five methods. That is the whole surface a broker has to satisfy to be usable
here, which is what makes swapping one for another an afternoon rather than a
rewrite — and what makes it safe to build this before knowing which broker
answers yes.

Adapters raise ``BrokerError`` and never swallow failures: the router is the
only place allowed to decide what a failure means, because only the router
knows whether a position is riding on the answer.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from qqq_alpha.execution.types import (
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
    OrderRequest,
)


@runtime_checkable
class Broker(Protocol):
    """What the engine needs from a broker, and nothing more."""

    name: str

    async def place(self, request: OrderRequest) -> BrokerOrder:
        """Submit an order. Raises ``BrokerError`` if the fate is unknown."""
        ...

    async def cancel(self, client_order_id: str) -> BrokerOrder:
        """Ask to cancel. A cancel that races a fill is not an error — the
        returned state says which one won."""
        ...

    async def order(self, client_order_id: str) -> BrokerOrder | None:
        """The broker's current word on one order, or None if it never saw it."""
        ...

    async def positions(self) -> list[BrokerPosition]:
        """Everything the account currently holds, contracts included."""
        ...

    async def account(self) -> BrokerAccount:
        """Cash and buying power."""
        ...
