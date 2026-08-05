"""Behavioural commentary on closed trades.

The operator's own question was blunt: did it exit early, scared, or in a
hurry? That deserves an honest, checkable answer, not a vibe — so this is
deterministic and rule-based, derived only from numbers already on the
record (exit reason, hold time, how far price ran before the exit). It never
calls the model again: a plain reading of the record is more trustworthy
here than a second AI opinion that cannot be traced back to a number.
"""

from __future__ import annotations

from typing import Any

FAST_STOP_MINUTES = 8
BIG_GIVEBACK_PCT = 40.0
CLEAN_STOP_PEAK_PCT = 10.0
STRONG_RUNNER_PCT = 100.0


def classify_exit(trade: dict[str, Any]) -> str:
    """One Arabic sentence describing how the exit actually played out."""
    reason = trade.get("exit_reason") or ""
    return_pct = float(trade.get("return_pct") or 0.0)
    max_favorable = float(trade.get("max_favorable_pct") or 0.0)
    hold = trade.get("hold_minutes")

    if reason == "stop_hit" and hold is not None and hold < FAST_STOP_MINUTES:
        return "🏃 خروج سريع جدًا — الفكرة لم تُعطَ وقتها الطبيعي قبل ضرب وقف الخسارة"

    if reason == "stop_hit" and max_favorable < CLEAN_STOP_PEAK_PCT:
        return "🛡️ حماية رأس مال نظيفة — الفكرة لم تنجح من البداية ولم يتأخر الخروج"

    if reason == "time_stop":
        return "⏱️ خروج بانتهاء الوقت المتوقع — الفكرة لم تتفعل كما خُطط لها"

    if reason == "trail_stop" and (max_favorable - return_pct) >= BIG_GIVEBACK_PCT:
        return (
            f"📉 تراجع كبير من القمة (+{max_favorable:.0f}%) قبل الإغلاق — "
            "الخروج كان متأخرًا أكثر مما ينبغي"
        )

    if reason in {"target", "trail_stop"} and return_pct >= STRONG_RUNNER_PCT:
        return "🎯 إدارة قوية — استغل الفرصة الممتدة ولم يقفل الرابح مبكرًا"

    if reason == "session_rollover":
        return "🌙 أُغلقت قسرًا لانتهاء الجلسة — لم يكن قرار خروج طوعي"

    if not reason:
        return "▫️ لا تزال مفتوحة أو بلا سبب خروج مسجَّل"

    return "✅ إدارة طبيعية ضمن الخطة — لا نمط تعجّل أو تردد واضح"


def classify_entry(trade: dict[str, Any]) -> str | None:
    """Flag an entry the AI itself marked as a deliberate departure from the playbook."""
    decision = trade.get("decision") or {}
    overrides = decision.get("overrides") or []
    if overrides:
        return "⚡ دخول جريء — خالف دفتر التشغيل بقرار موثّق: " + " | ".join(overrides[:2])
    return None
