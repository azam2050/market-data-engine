"""The contract desk: the brain reads each TradingView signal before a
contract is chosen.

The indicator has already decided the direction. What is left is the part
that decides whether a right call still pays: which expiry, which strike,
at what premium, with how much time on the clock. The rules in ``tvbridge``
make a sane first pick; this module shows the brain the signal, the clock
and the real candidates on the chain, and lets it name the one contract it
would buy — with a one-line reason the channel can read.

The bridge never trusts the answer blindly: the pick must be one of the
candidates it was shown, or the rule pick stands. A silent brain (no key,
no model, an outage, a refusal) also leaves the rule pick standing, so a
signal is never lost to the analysis step.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from qqq_alpha.config import Settings

log = logging.getLogger(__name__)

ANALYSIS_TIMEOUT_SEC = 45
ANALYSIS_MAX_TOKENS = 1500

SYSTEM_PROMPT = """أنت مكتب العقود في قناة إشارات خيارات أمريكية.
المؤشر قرر الاتجاه (كول أو بوت) ولا تناقشه. مهمتك واحدة: من قائمة العقود
المرشحة الحقيقية المعروضة عليك، اختر العقد الواحد الذي يمنح هذه الصفقة أفضل
فرصة أن يتحرك السعر معها أولاً ويُترجم ذلك إلى ربح على العقد.

قواعد المكتب:
- اختر عقداً من القائمة فقط، بالرمز (occ) حرفياً كما هو مكتوب.
- الأصل ينتهي اليوم في QQQ وSPY، وأقرب جمعة في الأسهم. الانتهاء الأبعد
  مسموح عندما يكون الوقت المتبقي للإغلاق قصيراً، أو الهدف الأول بعيداً عن
  السعر، أو حين يكون فارق السعر (spread) في عقد اليوم عريضاً.
- فضّل دلتا بين ٠٫٣٥ و٠٫٥٥، وفارق سعر ضيق، وسيولة حقيقية (حجم أو عقود مفتوحة).
- الستررايك عند السعر أو خارجه بخطوة واحدة؛ لا تختر ستررايك بعيداً يحتاج
  حركة أكبر من الهدف الأول ليربح.
- الإشارة في آخر الجلسة تُحجز للانتهاء التالي عمداً.
- اكتب السبب بسطر واحد بالعربية يفهمه مشترك غير تقني، وتحذيراً قصيراً إن
  كان في الصفقة ما يستحق الانتباه (فارق واسع، وقت قصير، حدث قريب)."""

CONTRACT_TOOL: dict[str, Any] = {
    "name": "choose_contract",
    "description": "Name the one contract to buy for this signal, from the candidates shown.",
    "input_schema": {
        "type": "object",
        "properties": {
            "occ": {
                "type": "string",
                "description": "The OCC symbol of the chosen candidate, copied exactly.",
            },
            "why": {
                "type": "string",
                "description": "One Arabic line: why this expiry and strike.",
            },
            "caution": {
                "type": "string",
                "description": "One short Arabic warning, or an empty string.",
            },
            "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
        },
        "required": ["occ", "why"],
    },
}


@dataclass
class ContractChoice:
    occ: str
    why: str
    caution: str = ""
    confidence: int | None = None


def render_context(ctx: dict[str, Any]) -> str:
    """The signal, the clock and the candidates as the brain sees them."""
    sig = ctx.get("signal", {})
    side = "كول (CALL)" if sig.get("side", 0) > 0 else "بوت (PUT)"
    lines = [
        f"الإشارة: {sig.get('symbol')} {side}"
        + (f" · فريم {sig.get('tf')}" if sig.get("tf") else ""),
        f"سعر الإشارة: {sig.get('price')} · الوقف: {sig.get('stop')}",
    ]
    if sig.get("targets"):
        lines.append("الأهداف: " + " · ".join(str(t) for t in sig["targets"]))
    if sig.get("reason"):
        lines.append(f"سبب المؤشر: {sig['reason']}")
    lines += [
        f"الوقت في نيويورك: {ctx.get('now_ny')} · متبقٍ للإغلاق: {ctx.get('minutes_to_close')} دقيقة"
        + (" · آخر الجلسة" if ctx.get("late_session") else "")
        + (" · إشارة 🌙" if ctx.get("moon") else ""),
        f"الأصل المتداول: {ctx.get('underlying')} عند {ctx.get('spot')}",
        f"اختيار القواعد: {ctx.get('rule_pick') or 'لا شيء'} (انتهاء {ctx.get('rule_expiry')})",
        "",
        "العقود المرشحة (JSON):",
        json.dumps(ctx.get("candidates", []), ensure_ascii=False),
    ]
    return "\n".join(lines)


class ContractAnalyst:
    """Asks the brain to choose among real candidates. Returns None when it
    cannot answer, so the caller falls back to the rules."""

    def __init__(self, settings: Settings, client: Any | None = None):
        self.settings = settings
        self._client = client

    @property
    def configured(self) -> bool:
        return bool(self.settings.anthropic_api_key and self.settings.anthropic_model)

    def _get_client(self) -> Any:
        if self._client is None:
            from anthropic import AsyncAnthropic

            self._client = AsyncAnthropic(api_key=self.settings.anthropic_api_key)
        return self._client

    async def choose(self, ctx: dict[str, Any]) -> dict[str, Any] | None:
        if not self.configured or not ctx.get("candidates"):
            return None
        client = self._get_client()
        try:
            response = await asyncio.wait_for(
                client.messages.create(
                    model=self.settings.anthropic_model,
                    max_tokens=ANALYSIS_MAX_TOKENS,
                    system=[
                        {
                            "type": "text",
                            "text": SYSTEM_PROMPT,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    tools=[CONTRACT_TOOL],
                    tool_choice={"type": "tool", "name": "choose_contract"},
                    messages=[{"role": "user", "content": render_context(ctx)}],
                ),
                timeout=ANALYSIS_TIMEOUT_SEC,
            )
        except Exception as exc:  # noqa: BLE001 - the rules stand in for a silent brain
            log.warning("contract analysis failed: %s", exc)
            return None
        if getattr(response, "stop_reason", None) in {"refusal", "max_tokens"}:
            return None
        for block in getattr(response, "content", []) or []:
            if getattr(block, "type", None) == "tool_use":
                return dict(block.input)
        return None


def parse_choice(payload: dict[str, Any] | None, allowed: set[str]) -> ContractChoice | None:
    """A choice is only a choice when it names a candidate we showed."""
    if not payload:
        return None
    occ = str(payload.get("occ") or "").strip().upper()
    if occ not in allowed:
        return None
    conf = payload.get("confidence")
    return ContractChoice(
        occ=occ,
        why=str(payload.get("why") or "").strip()[:300],
        caution=str(payload.get("caution") or "").strip()[:200],
        confidence=int(conf) if isinstance(conf, int | float) else None,
    )
