"""The brain: turns a market snapshot into a decision.

Two implementations:

* ``AIDecider`` — calls Claude with the full evidence pack and the playbook.
  This is the real decision maker.
* ``HeuristicDecider`` — a deliberately simple baseline used to sanity-check the
  plumbing and to give the backtest a free control group. If the AI cannot beat
  this, we have learned something important and cheap.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any, Protocol

from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.brain.prompts import DECISION_TOOL, SYSTEM_PROMPT, build_user_prompt
from qqq_alpha.brain.resilience import call_with_retry
from qqq_alpha.config import Settings
from qqq_alpha.domain import (
    Action,
    Decision,
    MarketSnapshot,
    OptionType,
    Target,
    Trade,
)

log = logging.getLogger(__name__)


def occ_symbol(underlying: str, expiry: date, option_type: OptionType, strike: float) -> str:
    """Build an OCC symbol, e.g. O:QQQ260802C00485000."""
    letter = "C" if option_type is OptionType.CALL else "P"
    return (
        f"O:{underlying}{expiry.strftime('%y%m%d')}{letter}"
        f"{int(round(strike * 1000)):08d}"
    )


def next_expiry(as_of: date, dte: int) -> date:
    """Resolve a requested DTE into a real weekday expiry."""
    target = as_of + timedelta(days=max(dte, 0))
    while target.weekday() >= 5:  # QQQ has weekday expiries
        target += timedelta(days=1)
    return target


class Decider(Protocol):
    async def decide(
        self,
        snapshot: MarketSnapshot,
        playbook: Playbook,
        open_trades: list[Trade],
        recent_trades: list[Trade],
        rail_warnings: list[str],
        attention_note: str,
        similar_trades: list | None = None,
        chain: list | None = None,
        options_pulse: list | None = None,
    ) -> Decision: ...


class AIDecider:
    def __init__(self, settings: Settings, client: Any | None = None):
        self.settings = settings
        self._client = client
        self.last_usage: dict[str, int] = {}

    def _get_client(self) -> Any:
        if self._client is None:
            from anthropic import AsyncAnthropic

            if not self.settings.anthropic_api_key:
                raise RuntimeError("ANTHROPIC_API_KEY is not configured")
            self._client = AsyncAnthropic(api_key=self.settings.anthropic_api_key)
        return self._client

    async def decide(
        self,
        snapshot: MarketSnapshot,
        playbook: Playbook,
        open_trades: list[Trade],
        recent_trades: list[Trade],
        rail_warnings: list[str],
        attention_note: str,
        similar_trades: list | None = None,
        chain: list | None = None,
        options_pulse: list | None = None,
    ) -> Decision:
        if not self.settings.anthropic_model:
            raise RuntimeError("ANTHROPIC_MODEL is not configured (see .env.example)")

        # assembling the prompt touches every data shape the engine produces;
        # a bug in any of them must degrade to a safe PASS, not kill the
        # engine mid-session — that exact crash has happened once already
        try:
            prompt = build_user_prompt(
                snapshot=snapshot,
                playbook=playbook,
                open_trades=open_trades,
                recent_trades=recent_trades,
                rail_warnings=rail_warnings,
                attention_note=attention_note,
                similar_trades=similar_trades,
                chain=chain,
                options_pulse=options_pulse,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("prompt assembly failed")
            return Decision(
                ts=snapshot.ts,
                action=Action.PASS,
                thesis=(
                    f"فشل تقني في تجهيز سياق القرار ({type(exc).__name__}: {exc}) "
                    "— تم تجاوز هذه اللحظة بأمان، راجع السجلات"
                ),
            )

        client = self._get_client()
        try:
            response = await call_with_retry(
                lambda: client.messages.create(
                    model=self.settings.anthropic_model,
                    # thinking and the response share this budget on current
                    # models, so it is deliberately generous — truncating
                    # mid-decision would produce a malformed trade plan
                    # rather than an honest PASS
                    max_tokens=self.settings.anthropic_max_tokens,
                    system=[
                        {
                            "type": "text",
                            "text": SYSTEM_PROMPT,
                            # the system prompt and playbook are stable across
                            # a session; caching them is most of the
                            # per-decision cost
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    output_config={"effort": self.settings.anthropic_effort},
                    tools=[DECISION_TOOL],
                    tool_choice={"type": "tool", "name": "submit_decision"},
                    messages=[{"role": "user", "content": prompt}],
                ),
                label="brain decision call",
            )
        except Exception as exc:  # noqa: BLE001 - a transient outage must not crash the engine
            log.error("brain call failed after retries: %s", exc)
            return Decision(
                ts=snapshot.ts,
                action=Action.PASS,
                thesis=(
                    f"فشل تقني بالاتصال بمحرك القرار ({type(exc).__name__}) بعد "
                    "محاولات إعادة — تم تجاوز هذه اللحظة بأمان"
                ),
            )

        usage = getattr(response, "usage", None)
        if usage is not None:
            self.last_usage = {
                "input_tokens": getattr(usage, "input_tokens", 0),
                "output_tokens": getattr(usage, "output_tokens", 0),
            }

        # a safety classifier can decline a request outright: HTTP 200, empty
        # content, no exception. Reading content[0] blindly would crash here.
        if getattr(response, "stop_reason", None) == "refusal":
            detail = getattr(response, "stop_details", None)
            category = getattr(detail, "category", None) if detail else None
            log.warning("brain declined to answer (category=%s); passing", category)
            return Decision(
                ts=snapshot.ts,
                action=Action.PASS,
                thesis=f"model declined to respond (category={category})",
            )

        if getattr(response, "stop_reason", None) == "max_tokens":
            # the plan was cut off mid-thought; acting on half a plan is worse
            # than not acting
            log.error("brain hit the token ceiling; passing rather than acting on a partial plan")
            return Decision(
                ts=snapshot.ts,
                action=Action.PASS,
                thesis="response truncated at max_tokens; raise ANTHROPIC_MAX_TOKENS",
            )

        payload: dict[str, Any] | None = None
        for block in response.content:
            if getattr(block, "type", None) == "tool_use":
                payload = dict(block.input)
                break

        if payload is None:
            log.error("brain returned no tool call; treating as PASS")
            return Decision(
                ts=snapshot.ts, action=Action.PASS, thesis="no structured decision returned"
            )

        return self._to_decision(payload, snapshot)

    # ------------------------------------------------------------------
    @staticmethod
    def _to_decision(payload: dict[str, Any], snapshot: MarketSnapshot) -> Decision:
        action = Action(payload.get("action", "PASS"))
        decision = Decision(
            ts=snapshot.ts,
            action=action,
            confidence=int(payload.get("confidence") or 0),
            thesis=str(payload.get("thesis") or ""),
            risks=list(payload.get("risks") or []),
            playbook_refs=list(payload.get("playbook_refs") or []),
            overrides=list(payload.get("overrides") or []),
            invalidation=str(payload.get("invalidation") or ""),
            expected_hold_minutes=payload.get("expected_hold_minutes"),
            raw=payload,
        )

        if action is not Action.ENTER:
            return decision

        direction_raw = str(payload.get("direction") or "").upper()
        if direction_raw not in ("CALL", "PUT"):
            decision.action = Action.PASS
            decision.thesis = f"ENTER without a valid direction; downgraded. {decision.thesis}"
            return decision

        decision.direction = OptionType(direction_raw)
        strike = payload.get("strike")
        if strike is None:
            decision.action = Action.PASS
            decision.thesis = f"ENTER without a strike; downgraded. {decision.thesis}"
            return decision

        expiry = next_expiry(snapshot.ts.date(), int(payload.get("expiry_dte") or 0))
        decision.occ_symbol = occ_symbol(
            snapshot.underlying.symbol, expiry, decision.direction, float(strike)
        )

        low, high = payload.get("entry_zone_low"), payload.get("entry_zone_high")
        if low is not None and high is not None:
            decision.entry_zone = (float(low), float(high))

        decision.targets = [
            Target(
                label=str(t.get("label") or f"T{i + 1}"),
                price=0.0,  # filled once the entry fill price is known
                return_pct=float(t.get("return_pct") or 0.0),
                take_pct=int(t.get("take_pct") or 50),
            )
            for i, t in enumerate(payload.get("targets") or [])
        ]
        stop_pct = payload.get("stop_return_pct")
        decision.stop_return_pct = float(stop_pct) if stop_pct is not None else None
        return decision


class HeuristicDecider:
    """Free baseline. Trend + volume agreement, nothing clever. Control group."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def decide(
        self,
        snapshot: MarketSnapshot,
        playbook: Playbook,
        open_trades: list[Trade],
        recent_trades: list[Trade],
        rail_warnings: list[str],
        attention_note: str,
        similar_trades: list | None = None,
        chain: list | None = None,
        options_pulse: list | None = None,
    ) -> Decision:
        bias = snapshot.net_bias
        rel_vol = snapshot.indicators.get("rel_volume") or 0.0
        atr_pct = 0.0
        atr_value = snapshot.indicators.get("atr14")
        price = snapshot.underlying.close
        if atr_value and price:
            atr_pct = atr_value / price * 100.0

        if abs(bias) < 0.30 or rel_vol < 1.3:
            return Decision(
                ts=snapshot.ts,
                action=Action.PASS,
                confidence=2,
                thesis=f"baseline: bias {bias:+.2f} / relvol {rel_vol:.2f} below thresholds",
            )

        direction = OptionType.CALL if bias > 0 else OptionType.PUT
        step = 1.0
        strike = round((price + (step if direction is OptionType.CALL else -step)) / step) * step
        expiry = next_expiry(snapshot.ts.date(), 0)

        return Decision(
            ts=snapshot.ts,
            action=Action.ENTER,
            direction=direction,
            occ_symbol=occ_symbol(snapshot.underlying.symbol, expiry, direction, strike),
            targets=[
                Target(label="T1", price=0.0, return_pct=50.0, take_pct=50),
                Target(label="T2", price=0.0, return_pct=100.0, take_pct=30),
            ],
            stop_return_pct=-40.0,
            confidence=5,
            thesis=(
                f"baseline heuristic: net bias {bias:+.2f}, relvol {rel_vol:.2f}, "
                f"atr {atr_pct:.3f}% — mechanical trend follow"
            ),
            expected_hold_minutes=45,
            invalidation="bias flips against the position",
        )


def build_decider(settings: Settings, mode: str = "ai") -> Decider:
    if mode == "heuristic":
        return HeuristicDecider(settings)
    return AIDecider(settings)


def decision_to_json(decision: Decision) -> str:
    return json.dumps(decision.model_dump(mode="json"), ensure_ascii=False, default=str)
