"""Sending real orders to a real broker.

Everything in this package is inert until ``EXECUTION_ENABLED`` is switched on.
With it off — the default, and the only state this engine has ever run in —
the router records what it *would* have sent and returns, so the live path
behaves exactly as it did before this package existed.

The split is deliberate. ``broker.py`` is a port: a small protocol describing
what any broker must be able to do. ``paper.py`` implements it without a
network. A real broker (Derayah, IBKR, whoever answers first) becomes one more
implementation of the same protocol, so the choice of broker never leaks into
the engine, and being told "no" by one of them costs an adapter rather than
the whole build.
"""

from qqq_alpha.execution.broker import Broker
from qqq_alpha.execution.paper import PaperBroker
from qqq_alpha.execution.registry import build_broker
from qqq_alpha.execution.router import ExecutionRouter, Reconciliation
from qqq_alpha.execution.types import (
    BrokerAccount,
    BrokerOrder,
    BrokerPosition,
    OrderRequest,
    OrderState,
    Side,
)

__all__ = [
    "Broker",
    "BrokerAccount",
    "BrokerOrder",
    "BrokerPosition",
    "ExecutionRouter",
    "OrderRequest",
    "OrderState",
    "PaperBroker",
    "Reconciliation",
    "Side",
    "build_broker",
]
