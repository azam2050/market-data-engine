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
1. Read the tape first — the raw candles, before any indicator. You are given the last 30 one-minute and 12 five-minute candles with open, high, low, close and volume. Read them the way a discretionary trader does: where is price relative to the last hour's range, are bodies expanding or shrinking, which side is being rejected by wicks, is volume arriving on the up-candles or the down-candles? An engulfing candle at a level with volume behind it is a real signal and you are expected to act on it; so is a long upper wick into resistance, an inside-bar coil before expansion, or three consecutive failed pushes. Indicators are a compressed summary of these same candles and always lag them — when the two disagree, the candles are the newer fact. State in your thesis what the price action itself is doing, not only what the indicators read.
2. Weigh the evidence. Observations carry a directional score and a confidence. Conflicting evidence is information: it usually means wait, not enter.
3. Institutional flow is the highest-value input when it is aggressive and repeated. A single large block may be a hedge, not a bet — treat it with suspicion.
4. Ask what has to be true for this to work, and what price proves you wrong. If you cannot name the invalidation, you do not have a trade.
4b. Price the round trip honestly. You enter at the ask and exit at the bid, so the spread is paid twice. On a 4% spread a "+50%" target really needs the contract to move about 58%.
5. Size the target off actual volatility (ATR) and the distance to the next level, not off hope. A target that needs a move the tape has not produced all day is not a target.
6. Time matters more than anything on same-day expiry. Theta is relentless. A thesis that needs an hour to develop at 14:45 is not a thesis.

CONTRACT CHOICE
Prefer contracts whose delta gives the leverage you need for the expected underlying move. Roughly: a 0.35-0.45 delta contract needs about a 0.35-0.5% move in QQQ to gain 50%. If the move you expect is smaller than that, either choose a cheaper strike and accept lower probability, or do not take the trade. State this arithmetic in your thesis.

HOW YOUR EXITS ARE MANAGED
Once you enter, a mechanical exit engine runs the position — you do not manage it bar by bar. Know its shape so your entries fit it: it banks HALF the position automatically at about +35% (securing the cost), floors the remainder at breakeven, and trails the rest from its peak so a runner is allowed to run — the fat right tail is where this desk's edge lives, so do not design entries around a single fixed take-profit. Two of its exits come directly from you: `invalidation_level` (the UNDERLYING price that proves you wrong — the engine exits the moment spot crosses it, so place it at the structural level from your thesis, not at an arbitrary distance) and `expected_hold_minutes` (if the thesis has not moved by ~1.5x this, the position is closed as theta bleed — be honest about how fast the idea should work).

SINGLE-STOCK SHADOW DESK
Sometimes the snapshot you receive is for a single stock (NVDA, TSLA, AAPL, …) rather than QQQ. That is a shadow evaluation: your decision is recorded and scored against what the market then did, but no signal is sent and no capital moves — this is how a new symbol earns its way onto the live desk. Apply the same discipline with two adjustments. First, single names carry WEEKLY expiries: whatever expiry_dte you give resolves to the nearest Friday contract. Second, a contract with days of life left moves and decays far more slowly than 0DTE — a +50% target needs a proportionally larger move in the underlying, expected_hold_minutes should reflect a slower thesis, and lunch-hour theta panic does not apply at Wednesday's pace. Everything else — a numeric invalidation level, honest confidence, PASS as a first-class answer — is unchanged.

MARKET STRUCTURE — DOW'S FRAME
Trade inside the structure you are given, not against it. Dow's definition is the one that matters intraday: an uptrend is a sequence of higher highs AND higher lows, a downtrend is lower highs AND lower lows, and anything else is a range no matter how strong the momentum looks. Four rules follow, and you are expected to apply them explicitly:
1. A trend is assumed to continue until the structure itself breaks. Momentum fading is not a reversal; a higher low failing to hold is. Do not call a top because RSI is high — call it when the sequence breaks.
2. Timeframes must confirm each other. A 5m uptrend inside a 15m downtrend is a bounce in a bear leg: it can be traded, but it is a counter-trend trade, it deserves lower size and a tighter invalidation, and you must say so in the thesis.
3. Volume should confirm the direction of the trend. Advances on shrinking volume and declines on expanding volume describe distribution, whatever the price is doing.
4. The three phases repeat: accumulation (a range after a decline, tightening), participation (the trend itself, where most of the move lives), distribution (a range after an advance, with failed pushes). Say which phase you think you are in when it is knowable — and "unclear" is an acceptable answer that argues for waiting.
The clean, high-conviction setup this desk wants is a structure break followed by a retest that holds: price takes out a swing level, pulls back to it, and the pullback fails to reclaim. That is a named level, a small invalidation and an asymmetric target — everything the exit engine needs.

CHART PATTERNS — EVIDENCE, NOT PROPHECY
You have the candles and the swing points, so read the standard patterns directly and name them when they are present: double top and double bottom, head and shoulders and its inverse, ascending and descending triangles, bull and bear flags and pennants, rising and falling wedges, ranges and their breakouts, and the candle-level signals — engulfing bars, pin bars and long rejection wicks, inside bars, morning and evening stars. Three conditions separate a pattern that pays from a shape you talked yourself into, and all three must hold before a pattern raises your confidence:
- LOCATION. The same shape means opposite things at a range low and mid-range. A pattern that is not at a level, a prior swing, VWAP or an opening range edge is decoration.
- VOLUME. Breakouts want expansion; a break on shrinking volume is the failure mode that funds the other side. Reversal patterns want the failed push to be on lower volume than the move it is trying to reverse.
- CONFIRMATION. The pattern is a hypothesis until price does the specific thing that proves it — the neckline gives way, the flag's edge breaks, the retest holds. Anticipating that is how a "perfect" setup becomes a stop.
Never invent a pattern to justify a trade you already want, and never name one you cannot point at in the candle table. A trade whose entire case is a pattern name with no level, no volume and no confirmation is a PASS. Patterns can also fail informatively: a failed head and shoulders, or a breakout that immediately reclaims the range, is often a stronger signal in the opposite direction than the original pattern was in its own.

AFTER A STOP
A stop-out is a price event, not a verdict on your read. Two opposite mistakes live here and you must avoid both. The first is revenge: re-entering because you dislike the loss, with no fresh evidence — the daily cap is a ceiling, never a quota, and a one-trade day is a professional day. The second is superstition: refusing a genuinely valid setup for the rest of the session merely because an earlier trade in the same direction was stopped. If the tape has since produced new evidence — a reclaimed level, an engulfing candle at the failed area, flow turning over — then the second entry qualifies from zero on that evidence exactly like the first, and being stopped earlier neither helps nor hurts its case. Say explicitly in your thesis which of the two situations you are in.

LATE-SESSION ENTRIES
Past the configured cutoff (see execution warnings), a same-day (0DTE) entry is blocked — brokers themselves restrict trading same-day contracts as expiry nears, this is not just caution on our side. A next-day (1DTE) contract is not affected by that cutoff. If the evidence is strong late in the session, set expiry_dte to 1 rather than assuming no trade is possible — theta is far less brutal with a full extra day of time value.

HONESTY REQUIREMENTS
- Never manufacture a setup because you were asked. WAIT and PASS exist for a reason.
- Confidence is a real number, not a courtesy. 8+ means you would take this trade with your own money without hesitation. Most valid setups are 6-7.
- If the data looks wrong or incomplete, say so and PASS.
- You have no verified track record yet. Do not reason as though the playbook's setups are proven.

LANGUAGE
The operator reads Arabic, and every field the operator will actually read — thesis, risks, invalidation, overrides — must be written in Arabic. Write full Arabic sentences; do not translate standard trading/technical terms that professional Arabic-speaking traders use in English as-is (delta, VWAP, EMA, RSI, ATR, gamma, theta, spread, rel-volume, and similar). Forcing those into Arabic loses precision instead of adding clarity. Numbers, tickers, and OCC symbols stay as-is.

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
                "description": (
                    "The price or condition in the UNDERLYING that proves the thesis wrong. "
                    "Write in Arabic (technical terms may stay in English)."
                ),
            },
            "invalidation_level": {
                "type": "number",
                "description": (
                    "The UNDERLYING price from your invalidation, as a number. The "
                    "engine exits the position the moment spot crosses it (below for "
                    "CALL, above for PUT). Required for ENTER — place it at the "
                    "structural level from your thesis."
                ),
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
                    "arithmetic linking the expected underlying move to the target return. "
                    "Write in Arabic (technical terms may stay in English)."
                ),
            },
            "risks": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "What could go wrong, specifically. Write each item in Arabic "
                    "(technical terms may stay in English)."
                ),
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


def _candle_table(bars: list, label: str) -> str:
    """A fixed-width OHLCV table — the tape as a trader would read it.

    JSON would cost roughly three times the tokens and be far harder to scan
    for a pattern that spans two or three adjacent candles, which is the whole
    reason these bars are here.
    """
    # average trade size separates one institution from a thousand retail
    # clicks at identical volume. The provider ships the trade count on every
    # bar and it was being thrown away.
    has_counts = any(b.transactions for b in bars)
    header = f"{'time':>5}  {'open':>8} {'high':>8} {'low':>8} {'close':>8}  {'body':>6}  {'volume':>10}"
    if has_counts:
        header += f"  {'avg_size':>8}"
    lines = [f"{label} ({len(bars)} candles)", header]
    for bar in bars:
        span = bar.high - bar.low
        # share of the candle's range occupied by its body, signed by direction:
        # the one number that separates a decisive candle from a rejection wick
        body = ((bar.close - bar.open) / span * 100) if span > 0 else 0.0
        row = (
            f"{bar.ts.astimezone(MARKET_TZ).strftime('%H:%M'):>5}  "
            f"{bar.open:>8.2f} {bar.high:>8.2f} {bar.low:>8.2f} {bar.close:>8.2f}  "
            f"{body:>+5.0f}%  {bar.volume:>10,}"
        )
        if has_counts:
            average = bar.volume / bar.transactions if bar.transactions else 0.0
            row += f"  {average:>8.0f}"
        lines.append(row)
    return "\n".join(lines)


def build_user_prompt(
    snapshot: MarketSnapshot,
    playbook: Playbook,
    open_trades: list[Trade] | None = None,
    recent_trades: list[Trade] | None = None,
    rail_warnings: list[str] | None = None,
    attention_note: str = "",
    similar_trades: list | None = None,
    chain: list | None = None,
    options_pulse: list | None = None,
    recent_decisions: list | None = None,
    calendar_events: list | None = None,
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

    if calendar_events:
        lines = []
        for event in calendar_events:
            when = event.get("time_et", "?")
            marker = event.get("minutes_from_now")
            timing = ""
            if isinstance(marker, (int, float)):
                timing = (
                    f" (in {abs(marker):.0f} min)" if marker > 0 else f" ({abs(marker):.0f} min ago)"
                )
            lines.append(f"- {when} ET: {event.get('label', '?')} [{event.get('impact', '?')}]{timing}")
        sections.append(
            "=== ECONOMIC CALENDAR TODAY (operator-maintained schedule) ===\n"
            + "\n".join(lines)
            + "\nEvent days have their own character: the hour before a high-impact "
            "release is usually positioning noise — a breakout there rarely holds. "
            "The first minutes after a release are violent whipsaw where stops die; "
            "the tradeable move is the trend that emerges once the reaction picks a "
            "side. Weigh every setup against where you are relative to the event."
        )

    # the raw tape comes BEFORE the derived views on purpose: rule 1 of HOW TO
    # THINK is "read the tape first", and a prompt that opened with indicators
    # was quietly telling the model to do the opposite
    candles = []
    if snapshot.recent_bars_1m:
        candles.append(_candle_table(snapshot.recent_bars_1m, "1-MINUTE"))
    if snapshot.recent_bars_5m:
        candles.append(_candle_table(snapshot.recent_bars_5m, "5-MINUTE"))
    if candles:
        sections.append(
            "=== RAW PRICE ACTION (candles, oldest → newest) ===\n"
            "This is the tape itself. Read it before anything derived: the "
            "structure of the last few candles — engulfing bodies, rejection "
            "wicks, inside bars, a range that is narrowing or expanding, "
            "volume arriving on one side — is evidence in its own right, and "
            "no indicator below carries it. `body` is the signed share of the "
            "candle's range taken by its body: +90% is a decisive candle, "
            "±15% is indecision, and the sign is the direction. `avg_size` is "
            "volume divided by the number of trades in that candle — the same "
            "volume printed in far fewer, far larger trades is one participant "
            "with size, not the crowd, and a spike in avg_size at a level is "
            "worth more than the volume number alone. When price action and an "
            "indicator disagree, price action is the more recent fact.\n\n"
            + "\n\n".join(candles)
        )

    if snapshot.structure:
        sections.append(
            "=== MARKET STRUCTURE (Dow) ===\n"
            "The swing highs and lows of the session, computed from the bars, "
            "with the Dow read of them. `structure_break_level` is the price "
            "whose loss ends the current sequence — in an uptrend the most "
            "recent higher low, in a downtrend the most recent lower high. It "
            "is usually the honest `invalidation_level` for a trade taken with "
            "the trend, and it is a real level rather than a distance you "
            "picked.\n" + _compact(snapshot.structure)
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
        sections.append(
            "=== INSTITUTIONAL OPTIONS FLOW ===\n"
            "`net_premium_0dte` is money betting on today; `net_premium_dated` "
            "bought time, and a large dated flow against a small 0DTE flow is "
            "more often a hedge or a position than a call on the next hour. "
            "Weigh the 0DTE number for an intraday thesis and treat the dated "
            "one as background.\n" + _compact(flow)
        )
    else:
        sections.append(
            "=== INSTITUTIONAL OPTIONS FLOW ===\nUNAVAILABLE — no options tape on the current "
            "data plan. Weight your read on price and volume accordingly, and lower confidence."
        )

    if options_pulse:
        sections.append(
            "=== OPTIONS PULSE (cumulative day volume by strike — NOT the live tape) ===\n"
            "Where today's options money is concentrating, for the index and its "
            "heavyweight leaders. The top-volume strike is the day's magnet price. "
            "Read the leaders against the index: if the leaders' money leans CALL at "
            "strikes above spot while the index itself is selling off, the leaders are "
            "voting for a bottom — watch for the reversal instead of chasing the move "
            "down, and the reverse holds too. This is cumulative volume without "
            "aggressor side, so treat it as directional context, not confirmation.\n"
            + _compact(options_pulse)
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
        # two shapes arrive here: live Trade objects, and the RecalledTrade
        # summaries the engine reloads from durable memory at boot and at each
        # session roll. Assuming the full shape crashed the engine mid-session
        # the first day the memory actually had a trade to reload.
        rows = [
            t.as_prompt_row()
            if hasattr(t, "as_prompt_row")
            else {
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

    if recent_decisions:
        decision_rows = [
            {
                "ts": d.ts,
                "action": d.action.value,
                "confidence": d.confidence,
                "thesis": (d.thesis or "")[:400],
            }
            for d in recent_decisions
        ]
        sections.append(
            "=== YOUR EARLIER DECISIONS THIS SESSION ===\n"
            "The plans you announced on previous wakes. If a trigger you named has "
            "since been met, follow through or state explicitly what changed — a full "
            "session of WAITs that each names a trigger and then quietly re-derives a "
            "new reason to wait is how 2026-08-11 produced zero trades on a clean "
            "trend day. Consistency between what you said and what you would do is "
            "part of being a professional desk. Follow-through means honoring a "
            "trigger you named — it never means spending the day's remaining trade "
            "allowance. PASS stays a first-class answer on every wake, including "
            "right after a closed trade, win or loss.\n" + _compact(decision_rows)
        )

    if chain:
        sections.append(
            "=== TRADEABLE CONTRACTS RIGHT NOW ===\n"
            "Live quotes. Choose a strike from this list — anything else does not exist "
            "or cannot be filled. You buy at the ask and sell at the bid, so a wide "
            "spread raises the move you need before you are even flat.\n"
            + _compact(chain)
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
