"""A broker that fills everything, instantly, at the price asked — on purpose.

This is the default adapter and the one that runs when no real broker is
configured. It exists so the whole execution path can be exercised, logged and
reconciled without a network, an account, or a way to lose money.

It is **not** a simulation of trading. It never slips, never partially fills,
never rejects and never goes down, so a good result here proves the plumbing
and says nothing whatsoever about profitability. That distinction is the
easiest one to lose and the most expensive: a broker's own demo account
flatters the same way, which is why the router logs every paper fill as
``paper`` rather than as a fill.
"""

from __future__ import annotations

from datetime import UTC, datetime

from qqq_alpha.execution.types import (
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
    OrderRequest,
    OrderState,
    Side,
)


class PaperBroker:
    """Fills at the limit price, tracks positions, forgets nothing."""

    name = "paper"

    def __init__(self, cash: float = 0.0) -> None:
        self._orders: dict[str, BrokerOrder] = {}
        self._requests: dict[str, OrderRequest] = {}
        self._holdings: dict[str, list[tuple[int, float]]] = {}
        self._cash = cash

    # ------------------------------------------------------------------
    async def place(self, request: OrderRequest) -> BrokerOrder:
        now = datetime.now(UTC)
        if request.client_order_id in self._orders:
            # a resend of an id we already have is the duplicate-submission
            # case, and answering "filled again" would double the position
            return self._orders[request.client_order_id]

        signed = request.quantity if request.side is Side.BUY else -request.quantity
        lots = self._holdings.setdefault(request.occ_symbol, [])
        lots.append((signed, request.limit_price))
        self._cash -= signed * request.limit_price * 100

        order = BrokerOrder(
            client_order_id=request.client_order_id,
            broker_order_id=f"paper-{len(self._orders) + 1}",
            state=OrderState.FILLED,
            filled_quantity=request.quantity,
            average_fill_price=request.limit_price,
            submitted_at=now,
            updated_at=now,
        )
        self._orders[request.client_order_id] = order
        self._requests[request.client_order_id] = request
        return order

    async def cancel(self, client_order_id: str) -> BrokerOrder:
        order = self._orders.get(client_order_id)
        if order is None:
            return BrokerOrder(
                client_order_id=client_order_id,
                state=OrderState.REJECTED,
                message="unknown order",
            )
        # everything here fills on arrival, so a cancel always loses the race.
        # Saying so plainly is the point: an engine that assumes cancels
        # succeed will be wrong against a real broker too.
        return order

    async def order(self, client_order_id: str) -> BrokerOrder | None:
        return self._orders.get(client_order_id)

    async def positions(self) -> list[BrokerPosition]:
        out: list[BrokerPosition] = []
        for symbol, lots in self._holdings.items():
            quantity = sum(size for size, _ in lots)
            if quantity == 0:
                continue
            spent = sum(size * price for size, price in lots)
            out.append(
                BrokerPosition(
                    occ_symbol=symbol,
                    quantity=quantity,
                    average_price=round(spent / quantity, 4) if quantity else None,
                )
            )
        return out

    async def account(self) -> BrokerAccount:
        return BrokerAccount(account_id="paper", cash=self._cash, buying_power=self._cash)

    # ------------------------------------------------------------------
    @property
    def submitted(self) -> list[OrderRequest]:
        """Every request this broker was handed, in order — the record a test
        (or an operator reading the journal) checks against."""
        return list(self._requests.values())
