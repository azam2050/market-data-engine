"""Which broker adapter a deployment gets, chosen by one setting.

The list is short today — paper, and nothing else — which is the honest state:
no real adapter is written, because no broker has confirmed that their API
serves individuals and covers options. When one does, it lands here as a single
extra entry and nothing above it changes.

An unknown name is a hard failure rather than a silent fall back to paper. The
fallback would be the worse bug by far: the operator believes he is live, the
engine reports fills, and none of them exist.
"""

from __future__ import annotations

from qqq_alpha.config import Settings
from qqq_alpha.execution.broker import Broker
from qqq_alpha.execution.paper import PaperBroker

BROKERS = ("none", "paper")


def build_broker(settings: Settings) -> Broker | None:
    """The configured adapter, or None when execution has no broker at all."""
    name = (settings.execution_broker or "none").strip().lower()
    if name in ("", "none"):
        return None
    if name == "paper":
        return PaperBroker()
    raise ValueError(
        f"unknown EXECUTION_BROKER {name!r}; known adapters: {', '.join(BROKERS)}"
    )
