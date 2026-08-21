"""The one door between a decision and a real order.

Nothing in the engine talks to a broker. Everything goes through here, because
the questions that decide whether automated trading is survivable are all the
same question — *did what we think happened actually happen?* — and that
question needs one place to live.

Three jobs:

**The switch.** ``EXECUTION_ENABLED`` defaults to off and the router refuses to
send while it is off, no matter what it is handed. Off is not a quiet no-op: the
intent is journalled either way, so the order file fills up with exactly what
would have been sent long before anything is sent, and the first live day is
reading a file you already recognise rather than meeting the code for the first
time.

**The size ceiling.** An order larger than the configured cap is refused, not
trimmed. Trimming would let a sizing bug reach the market wearing a safe
number; refusing turns it into a missed trade and a loud message, and a missed
trade costs nothing that a wrong-sized one does not cost more.

**Reconciliation.** The broker is the only authority on what is owned. Any
disagreement between its answer and the engine's own book is reported rather
than resolved, because both automatic repairs are worse than the disagreement:
closing a position the engine forgot could dump something deliberately held,
and adopting one it never opened would hand the exit logic a trade it has no
thesis for.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from qqq_alpha.config import Settings
from qqq_alpha.execution.broker import Broker
from qqq_alpha.execution.types import (
    BrokerError,
    BrokerOrder,
    BrokerPosition,
    OrderRequest,
    OrderState,
)
from qqq_alpha.journal import Journal

log = logging.getLogger(__name__)


@dataclass
class Reconciliation:
    """What the engine believes, next to what the broker reports."""

    matched: dict[str, int] = field(default_factory=dict)
    missing_at_broker: dict[str, int] = field(default_factory=dict)
    unknown_to_engine: dict[str, int] = field(default_factory=dict)
    quantity_mismatch: dict[str, tuple[int, int]] = field(default_factory=dict)
    checked_at: datetime | None = None
    error: str = ""

    @property
    def ok(self) -> bool:
        return not (
            self.missing_at_broker
            or self.unknown_to_engine
            or self.quantity_mismatch
            or self.error
        )

    def describe(self) -> str:
        """The operator's version, in his language, naming what to look at."""
        if self.error:
            return f"⚠️ تعذّرت مطابقة المراكز مع الوسيط: {self.error}"
        if self.ok:
            return f"✅ المراكز مطابقة للوسيط ({len(self.matched)} عقد)"
        lines = ["🚨 اختلاف بين ما يظنّه البوت وما لدى الوسيط:"]
        for symbol, quantity in self.missing_at_broker.items():
            lines.append(f"   • {symbol}: البوت يظنّه مفتوحًا ({quantity}) والوسيط لا يملكه")
        for symbol, quantity in self.unknown_to_engine.items():
            lines.append(f"   • {symbol}: الوسيط يملكه ({quantity}) والبوت لا يعرفه")
        for symbol, (mine, theirs) in self.quantity_mismatch.items():
            lines.append(f"   • {symbol}: البوت {mine} والوسيط {theirs}")
        lines.append("لم يُصحَّح شيء تلقائيًا — راجع حسابك عند الوسيط بنفسك.")
        return "\n".join(lines)


class ExecutionRouter:
    """Guards, journals and reconciles. Owns no strategy of its own."""

    def __init__(
        self,
        settings: Settings,
        broker: Broker | None = None,
        journal: Journal | None = None,
        on_alert: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self.settings = settings
        self.broker = broker
        self.journal = journal
        self._on_alert = on_alert
        self._sent: dict[str, BrokerOrder] = {}
        self._withheld = 0

    # ------------------------------------------------------------------
    @property
    def armed(self) -> bool:
        """True only when a broker exists *and* the operator switched it on.

        Both halves are required on purpose: configuring a broker must not be
        enough to start trading, so that adding credentials and going live stay
        two separate decisions made on two separate days.
        """
        return bool(self.settings.execution_enabled and self.broker is not None)

    @property
    def withheld_count(self) -> int:
        """Orders the router declined to send while disarmed — the number that
        says how busy live trading would have been."""
        return self._withheld

    # ------------------------------------------------------------------
    async def submit(self, request: OrderRequest) -> BrokerOrder | None:
        """Send an order, or record why it was not sent.

        Returns None whenever nothing reached a broker — disarmed, refused by a
        guard, or failed — so a caller cannot mistake silence for a fill.
        """
        if request.client_order_id in self._sent:
            # the same id twice means a retry loop or a restart replaying work;
            # either way the position must not be doubled
            await self._record(request, None, "duplicate_client_order_id")
            return None

        cap = self.settings.execution_max_contracts
        if request.quantity > cap:
            await self._record(request, None, f"over_size_cap: {request.quantity} > {cap}")
            await self._alert(
                f"🚫 أمر مرفوض قبل الإرسال: طلب {request.quantity} عقدًا والسقف {cap}.\n"
                f"{request.describe()}\n"
                "لم يُرسَل شيء، ولم يُقلَّص الحجم — تقليصه بصمت كان سيخفي الخطأ."
            )
            return None

        if not self.armed:
            self._withheld += 1
            await self._record(request, None, "execution_disabled")
            return None

        assert self.broker is not None  # narrowed by `armed`
        await self._record(request, None, "submitting")
        try:
            order = await self.broker.place(request)
        except BrokerError as exc:
            await self._record(request, None, f"broker_error: {exc}")
            await self._alert(
                f"🚨 فشل إرسال أمر — ومصيره غير معروف:\n{request.describe()}\n"
                f"السبب: {exc}\n"
                "راجع حسابك عند الوسيط قبل أي إجراء آخر."
            )
            return None

        self._sent[request.client_order_id] = order
        await self._record(request, order, "submitted")
        if order.state is OrderState.REJECTED:
            await self._alert(f"🚫 الوسيط رفض الأمر: {request.describe()}\n{order.message}")
        elif order.state is OrderState.PARTIAL:
            await self._alert(
                f"⚠️ تنفيذ جزئي: {order.filled_quantity} من {request.quantity} — "
                f"{request.occ_symbol}"
            )
        return order

    # ------------------------------------------------------------------
    async def reconcile(self, expected: dict[str, int]) -> Reconciliation:
        """Compare the engine's open book against the broker's holdings.

        ``expected`` maps OCC symbol to contract count. Called at boot and
        after any reconnect: the gap between "we were disconnected" and "we
        know what we own" is the window every automated desk gets hurt in.
        """
        result = Reconciliation(checked_at=datetime.now(UTC))
        if not self.armed:
            return result

        assert self.broker is not None
        try:
            held: list[BrokerPosition] = await self.broker.positions()
        except BrokerError as exc:
            result.error = str(exc)
            await self._alert(result.describe())
            return result

        theirs = {p.occ_symbol: p.quantity for p in held if p.quantity}
        for symbol, mine in expected.items():
            if symbol not in theirs:
                result.missing_at_broker[symbol] = mine
            elif theirs[symbol] != mine:
                result.quantity_mismatch[symbol] = (mine, theirs[symbol])
            else:
                result.matched[symbol] = mine
        for symbol, quantity in theirs.items():
            if symbol not in expected:
                result.unknown_to_engine[symbol] = quantity

        if not result.ok:
            await self._alert(result.describe())
        return result

    # ------------------------------------------------------------------
    async def _record(
        self, request: OrderRequest, order: BrokerOrder | None, outcome: str
    ) -> None:
        record = {
            "ts": datetime.now(UTC),
            "outcome": outcome,
            "armed": self.armed,
            "broker": getattr(self.broker, "name", None),
            **request.model_dump(mode="json"),
        }
        if order is not None:
            record["order"] = order.model_dump(mode="json")
        if self.journal is not None:
            self.journal.log_order(record)
        log.info("order %s: %s", outcome, request.describe())

    async def _alert(self, message: str) -> None:
        if self._on_alert is None:
            log.warning("execution alert (no notifier): %s", message)
            return
        try:
            await self._on_alert(message)
        except Exception:  # noqa: BLE001 - a failed alert must not abort execution
            log.exception("could not deliver execution alert")
