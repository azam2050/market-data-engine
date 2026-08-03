"""Prompt construction for the decision model.

The system prompt establishes one thing above all: the model has final authority
over the trade decision. The playbook informs it; the rails constrain execution;
neither overrules its judgement about the market.
"""

from __future__ import annotations

import json
from typing import Any

from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import MarketSnapshot, Trade

SYSTEM_PROMPT = """You are the decision engine of a professional 0DTE options desk trading QQQ.

YOUR AUTHORITY
You make the trade decision. The playbook you are given is accumulated experience, not law — you may trade against any part of it when the evidence in front of you justifies it, provided you state what you overrode and why. A separate mechanical layer handles execution safety (market hours, position caps, spread checks); do not do its job. Your job is judgement.

WHAT YOU ARE LOOKING FOR
Asymmetric setups: a realistic move in QQQ that produces at least +50% on the option contract, against a clearly defined invalidation. You are hunting quality, not activity. Passing is a real answer and often the correct one — a typical session offers at most one or two genuine opportunities, and many offer none.

HOW TO THINK
1. Read the tape first. What is the market actually doing right now — trending, coiling, reverting, fighting at a level?
2. Weigh the evidence. Observations carry a directional score and a confidence. Conflicting evidence is information: it usually means wait, not enter.
3. Institutional flow is the highest-value input when it is aggressive and repeated. A single large block may be a hedge, not a bet — treat it with suspicion.
4. Ask what has to be true for this to work, and what price proves you wrong. If you cannot name the invalidation, you do not have a trade.
5. Size the target off actual volatility (ATR) and the distance to the next level, not off hope. A target that needs a move the tape has not produced all day is not a target.
6. Time matters more than anything on same-day expiry. Theta is relentless. A thesis that needs an hour to develop at 14:45 is not a thesis.

CONTRACT CHOICE
Prefer contracts whose delta gives the leverage you need for the expected underlying move. Roughly: a 0.35-0.45 delta contract needs about a 0.35-0.5% move in QQQ to gain 50%. If the move you expect is smaller than that, either choose a cheaper strike and accept lower probability, or do not take the trade. State this arithmetic in your thesis.

HONESTY REQUIREMENTS
- Never manufacture a setup because you were asked. WAIT and PASS exist for a reason.
- Confidence is a real number, not a courtesy. 8+ means you would take this trade with your own money without hesitation. Most valid setups are 6-7.
- If the data looks wrong or incomplete, say so and PASS.
- You have no verified track record yet. Do not reason as though the playbook's setups are proven.

You must respond by calling the `submit_decision` tool. No prose outside it."""


DECISION_TOOL: dict[str, Any] = {
    "name": "submit_decision",
    "description": "Submit the trading decision for this moment.",
    "input_schema": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["ENTER", "WAIT", "PASS"],
                "description": (
                    "ENTER: take the trade now. WAIT: a setup is forming but not "
                    "triggered — check again shortly. PASS: nothing here."
                ),
            },
            "direction": {"type": "string", "enum": ["CALL", "PUT"]},
            "strike": {
                "type": "number",
                "description": "Strike price of the contract you want, required for ENTER.",
            },
            "expiry_dte": {
                "type": "integer",
                "description": "Days to expiry: 0 for same day, 1 for next session.",
            },
            "entry_zone_low": {"type": "number"},
            "entry_zone_high": {"type": "number"},
            "targets": {
                "type": "array",
                "description": "At least one target. Express each as a % gain on the contract.",
                "items": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string"},
                        "return_pct": {"type": "number"},
                        "take_pct": {
                            "type": "integer",
                            "description": "Portion of the position to close here, 0-100.",
                        },
                    },
                    "required": ["label", "return_pct", "take_pct"],
                },
            },
            "stop_return_pct": {
                "type": "number",
                "description": "Loss on the contract that invalidates the trade, e.g. -40.",
            },
            "invalidation": {
                "type": "string",
                "description": "The price or condition in the UNDERLYING that proves the thesis wrong.",
            },
            "expected_hold_minutes": {"type": "integer"},
            "confidence": {
                "type": "integer",
                "minimum": 0,
                "maximum": 10,
                "description": "0-10. Be honest; this number is scored against outcomes.",
            },
            "thesis": {
                "type": "string",
                "description": (
                    "Why this trade, in plain language a trader would accept. Include the "
                    "arithmetic linking the expected underlying move to the target return."
                ),
            },
            "risks": {
                "type": "array",
                "items": {"type": "string"},
                "description": "What could go wrong, specifically.",
            },
            "playbook_refs": {
                "type": "array",
                "items": {"type": "string"},
                "description": "IDs of playbook setups or cautions you relied on.",
            },
            "overrides": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Playbook guidance you deliberately went against, each with its "
                    "reason. Leave empty if none."
                ),
            },
        },
        "required": ["action", "confidence", "thesis"],
    },
}


def _compact(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str, ensure_ascii=False)


def build_user_prompt(
    snapshot: MarketSnapshot,
    playbook: Playbook,
    open_trades: list[Trade] | None = None,
    recent_trades: list[Trade] | None = None,
    rail_warnings: list[str] | None = None,
    attention_note: str = "",
    similar_trades: list | None = None,
) -> str:
    sections: list[str] = []

    sections.append(playbook.as_prompt_block())

    local = snapshot.ts.astimezone(MARKET_TZ)
    sections.append(
        "\n".join(
            [
                "=== NOW ===",
                f"time: {local.strftime('%Y-%m-%d %H:%M')} ET (session minute {snapshot.session_minute} of 390)",
                f"regime: {snapshot.regime.value}",
                f"underlying: {snapshot.underlying.symbol} @ {snapshot.underlying.close}",
                f"net evidence bias: {snapshot.net_bias:+.3f} (advisory aggregate, -1 bearish .. +1 bullish)",
                f"why you were woken: {attention_note or 'scheduled check'}",
            ]
        )
    )

    if snapshot.timeframes:
        sections.append(
            "=== MULTI-TIMEFRAME VIEW ===\n"
            "15m = where the day is going, 5m = whether structure supports the trade, "
            "1m = when to press the button. A 1m signal against the 15m is usually noise.\n"
            + _compact(snapshot.timeframes)
        )
    else:
        sections.append("=== INDICATORS ===\n" + _compact(snapshot.indicators))

    sections.append("=== LEVELS ===\n" + _compact(snapshot.levels))

    if snapshot.data_quality:
        sections.append(
            "=== DATA QUALITY ===\n"
            f"{snapshot.data_quality}\n"
            "Degraded data is a reason to lower confidence or pass, not to guess."
        )

    observations = [
        {
            "name": o.name,
            "category": o.category,
            "value": o.value,
            "score": o.score,
            "confidence": o.confidence,
            "note": o.note,
        }
        for o in snapshot.observations
    ]
    sections.append(
        "=== OBSERVATIONS (evidence — scores are advisory, you decide what matters) ===\n"
        + _compact(observations)
    )

    if snapshot.flow:
        flow = snapshot.flow.model_dump(mode="json")
        sections.append("=== INSTITUTIONAL OPTIONS FLOW ===\n" + _compact(flow))
    else:
        sections.append(
            "=== INSTITUTIONAL OPTIONS FLOW ===\nUNAVAILABLE — no options tape on the current "
            "data plan. Weight your read on price and volume accordingly, and lower confidence."
        )

    if snapshot.leaders:
        leaders = [
            {"symbol": b.symbol, "close": b.close, "volume": b.volume} for b in snapshot.leaders
        ]
        sections.append("=== INDEX HEAVYWEIGHTS ===\n" + _compact(leaders))

    if snapshot.events:
        sections.append("=== SCHEDULED EVENTS NEARBY ===\n" + "\n".join(snapshot.events))

    if rail_warnings:
        sections.append(
            "=== EXECUTION WARNINGS (not blocks — factor into confidence) ===\n"
            + "\n".join(f"- {w}" for w in rail_warnings)
        )

    if open_trades:
        rows = [
            {
                "contract": t.occ_symbol,
                "entry": t.entry_price,
                "opened_at": t.opened_at,
                "thesis": t.decision.thesis[:200],
            }
            for t in open_trades
        ]
        sections.append("=== CURRENTLY OPEN ===\n" + _compact(rows))

    if recent_trades:
        rows = [
            {
                "opened_at": t.opened_at,
                "contract": t.occ_symbol,
                "result_pct": t.return_pct,
                "status": t.status.value,
                "confidence_was": t.decision.confidence,
                "thesis": t.decision.thesis[:160],
                "exit_reason": t.exit_reason,
            }
            for t in recent_trades[-15:]
        ]
        sections.append(
            "=== THIS ENGINE'S RECENT TRADES (your own track record — learn from it) ===\n"
            + _compact(rows)
        )

    if similar_trades:
        rows = [
            t.as_prompt_row() if hasattr(t, "as_prompt_row") else t for t in similar_trades
        ]
        sections.append(
            "=== WHEN THE MARKET LOOKED LIKE THIS BEFORE ===\n"
            "Your own trades taken under comparable conditions, nearest match first. "
            "This is evidence about you, not about the market in general — if these "
            "went badly, that is a fact about your judgement in this setup.\n"
            + _compact(rows)
        )

    sections.append(
        "Decide now. Call submit_decision. If nothing here is worth risking capital on, "
        "PASS and say why in one line."
    )
    return "\n\n".join(sections)
