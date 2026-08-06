"""Signal delivery.

One interface, so the Telegram bots can be added later without touching the
engine. Today it prints to the console; the message shape is already the one a
subscriber will receive, which means the wording gets reviewed against real
signals long before anyone is paying for them.

Every message carries the data-mode banner. A recommendation built on 15-minute
delayed data must never be mistaken for a live one.
"""

from __future__ import annotations

import logging
from typing import Protocol

from rich.console import Console
from rich.panel import Panel

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import Decision, Trade, TradeUpdate

log = logging.getLogger(__name__)

DISCLAIMER = "توصية تعليمية — القرار والمسؤولية عليك"


class Notifier(Protocol):
    async def signal(self, trade: Trade, delayed: bool) -> None: ...
    async def update(self, trade: Trade, update: TradeUpdate, delayed: bool) -> None: ...
    async def note(self, text: str) -> None: ...


def human_contract(occ_symbol: str, as_of) -> str:
    """A trader-readable label, e.g. "QQQ 702 CALL 0DTE" — the raw OCC symbol
    (``O:QQQ260805C00702000``) is exact but not something anyone reads at a
    glance. Falls back to the raw symbol if it cannot be parsed, since a
    slightly ugly label beats a missing one."""
    from qqq_alpha.data.massive import parse_occ_symbol

    try:
        underlying, expiry, option_type, strike = parse_occ_symbol(occ_symbol)
    except (ValueError, IndexError):
        return occ_symbol

    direction_word = "CALL" if option_type.value == "CALL" else "PUT"
    strike_label = f"{strike:.0f}" if strike == int(strike) else f"{strike:.1f}"
    dte = (expiry - as_of.astimezone(MARKET_TZ).date()).days
    dte_label = "0DTE" if dte <= 0 else f"{dte}DTE"
    return f"{underlying} {strike_label} {direction_word} {dte_label}"


def format_signal(trade: Trade, delayed: bool) -> str:
    decision: Decision = trade.decision
    local = trade.opened_at.astimezone(MARKET_TZ)
    direction = "CALL 📈" if decision.direction and decision.direction.value == "CALL" else "PUT 📉"

    lines = [
        f"🎯 توصية جديدة | {trade.snapshot_at_entry.underlying.symbol if trade.snapshot_at_entry else 'QQQ'}",
        "",
        f"العقد: {human_contract(trade.occ_symbol, trade.opened_at)}",
        f"الاتجاه: {direction}",
        f"سعر العقد الآن: ${trade.entry_price:.2f}",
    ]

    if decision.entry_zone:
        low, high = decision.entry_zone
        lines.append(f"منطقة الدخول: {low:.2f} – {high:.2f}")

    lines.append("")
    for target in decision.targets:
        lines.append(
            f"🎯 {target.label}: ${target.price:.2f}  (+{target.return_pct:.0f}%) "
            f"— اجنِ {target.take_pct}%"
        )

    if decision.stop_price is not None:
        lines.append(
            f"🛑 وقف الخسارة: ${decision.stop_price:.2f} ({decision.stop_return_pct:+.0f}%)"
        )

    lines += ["", "📊 سبب الدخول:", decision.thesis]

    if decision.invalidation:
        lines += ["", f"❌ يُلغى إذا: {decision.invalidation}"]

    if decision.risks:
        lines += ["", "⚠️ المخاطر:"] + [f"  • {risk}" for risk in decision.risks]

    if decision.overrides:
        lines += ["", "🔀 مخالفات لدليل اللعب:"] + [f"  • {o}" for o in decision.overrides]

    lines += [
        "",
        f"⚖️ الثقة: {decision.confidence}/10",
        f"⏱️ {local.strftime('%H:%M')} (نيويورك)",
    ]

    if delayed:
        lines += ["", "🕒 تنبيه: البيانات متأخرة ١٥ دقيقة — هذه إشارة اختبار وليست للتنفيذ"]

    lines += ["", f"⚠️ {DISCLAIMER}"]
    return "\n".join(lines)


def format_update(trade: Trade, update: TradeUpdate, delayed: bool) -> str:
    closed = update.note.startswith("closed:")
    icon = "🏁" if closed else "🔔"
    title = "إغلاق" if closed else "متابعة"
    sign = "✅" if update.return_pct > 0 else "❌"

    lines = [
        f"{icon} {title} | {human_contract(trade.occ_symbol, trade.opened_at)}",
        f"السعر الآن: ${update.price:.2f} ({update.return_pct:+.1f}%) {sign}",
        update.note,
    ]
    if closed:
        held = (
            int((update.ts - trade.opened_at).total_seconds() // 60)
            if trade.opened_at
            else 0
        )
        lines.append(f"المدة: {held} دقيقة | أقصى ربح وصلته: {trade.max_favorable_pct:+.1f}%")
    if delayed:
        lines.append("🕒 بيانات متأخرة — للاختبار فقط")
    return "\n".join(lines)


class ConsoleNotifier:
    """Prints signals exactly as a subscriber would receive them."""

    def __init__(self, console: Console | None = None):
        self.console = console or Console()

    async def signal(self, trade: Trade, delayed: bool) -> None:
        self.console.print(
            Panel(
                format_signal(trade, delayed),
                title="إشارة جديدة",
                border_style="green",
            )
        )

    async def update(self, trade: Trade, update: TradeUpdate, delayed: bool) -> None:
        closed = update.note.startswith("closed:")
        self.console.print(
            Panel(
                format_update(trade, update, delayed),
                border_style="cyan" if not closed else "magenta",
            )
        )

    async def note(self, text: str) -> None:
        self.console.print(f"[dim]{text}[/]")


class NullNotifier:
    """Swallows everything. For tests and for silent shadow runs."""

    def __init__(self) -> None:
        self.signals: list[Trade] = []
        self.updates: list[TradeUpdate] = []
        self.notes: list[str] = []

    async def signal(self, trade: Trade, delayed: bool) -> None:
        self.signals.append(trade)

    async def update(self, trade: Trade, update: TradeUpdate, delayed: bool) -> None:
        self.updates.append(update)

    async def note(self, text: str) -> None:
        self.notes.append(text)
