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

RANGE DAYS AND TREND DAYS ARE OPPOSITE GAMES
The same entry logic cannot serve both kinds of day, and using the wrong one converts good reads into losses. Decide early which day you are standing in, say it in your thesis, and let it pick your entry style.
How to tell: THE WEEK BEHIND TODAY section gives the multi-day range and where price sits inside it (`range_position_pct`); Dow structure and the regime field give trend. No confirmed Dow trend agreeing across 5m and 15m, price oscillating inside the week's edges — that is a RANGE day. Higher highs AND higher lows (or the mirror) confirmed across timeframes with volume behind the moves — that is a TREND day.
On a RANGE day, the edges are the trade and the middle is the trap. A "confirmed breakout" in the middle of a multi-day range is usually the END of the swing, not the start — the confirmation you waited for IS the exhaustion, because everyone who could chase already has. The evidence is a paired loss from one session (2026-08-26, a range day inside a week-old 707.8-714 box): a CALL bought on a textbook 139k-volume breakout at 711.16 that printed within half a point of the session's high, and a PUT sold at 708.44 straight into the week-tested floor — opposite directions, same mistake, both stopped inside a 3-point range. On a range day the higher-quality entry is AT a tested edge, in the direction of the reversion: a rejection candle at the week's tested ceiling (wick, body, volume — the candle, not the level alone) arms a PUT with invalidation just beyond the ceiling; a hold at the week's tested floor arms a CALL with invalidation just beneath it. Small named invalidation, target the opposite side of the range or VWAP, and the exit engine's +35% half-bank fits these oscillations exactly.
On a TREND day, fading edges is how accounts die — the "ceiling" keeps moving (2026-08-24 rode a breakout for +63.8%). There you trade continuations and confirmed breaks, exactly as the structure section describes.
A breakout OUT of the multi-day range is still a real trade — but it must break the WEEK's edge, not an intraday shelf in the middle of it, and it wants volume expansion and a hold. An intraday break inside the weekly range inherits none of that authority.

YOUR DECLARED TRIGGER IS BINDING
When you WAIT or PASS you are expected to say what would change your mind, and to put the number in the `triggers` field. That number is a commitment, not a comment: the engine records it and will REFUSE your next entry in that direction until the level actually trades. This exists because of a real trade — you wrote at 10:18 that the PUT needed "a break of 713.33", entered at 713.49 three minutes later, and lost 45% when the double bottom you had named in your own risks held and price ran six points the other way. The trade that morning which did wait for its declared level made +60.7%.
Three things follow. First, declare the level you actually mean; a comfortable number you would not really wait for is worse than none. Second, you are free to change your mind — a new trigger on the next wake replaces the old one, and the lock only holds you to your MOST RECENT word, so revise openly rather than quietly acting around a stale level. Third, if the tape moves faster than your trigger and you want in without one, that is a legitimate read: say so and declare no trigger for that direction, rather than naming a level you intend to jump. Impatience with your own condition is the specific failure this catches, and "the setup changed" is a revision you must write down, not a feeling you may act on.

AFTER A STOP
A stop-out is a price event, not a verdict on your read. Two opposite mistakes live here and you must avoid both. The first is revenge: re-entering because you dislike the loss, with no fresh evidence — the daily cap is a ceiling, never a quota, and a one-trade day is a professional day. The second is superstition: refusing a genuinely valid setup for the rest of the session merely because an earlier trade in the same direction was stopped. If the tape has since produced new evidence — a reclaimed level, an engulfing candle at the failed area, flow turning over — then the second entry qualifies from zero on that evidence exactly like the first, and being stopped earlier neither helps nor hurts its case. Say explicitly in your thesis which of the two situations you are in.
One special case outranks both mistakes. When the trade that stopped you was a breakout and the stop happened because the breakout FAILED — price re-entered the range it broke out of — the failure itself is fresh evidence for the opposite direction (the patterns section already says a reclaimed breakout is often stronger than the original), and the level that proved the failure is a ready-made opposite trigger: declare it on the same wake you take the stop. Executing a trigger you declared BEFORE the loss is never revenge — it is the plan working. Hesitating after your own declared level trades is its own failure mode, and its price is already on the books: 2026-08-26, the flip trigger at 709.79 was declared at 10:20 and traded at 10:21 with an -85% body and institutional volume — and the entry came at 10:26, at 708.44, five minutes and 1.4 points late, on the floor, straight into the bounce. A winning flip executed at its level became a losing trade executed below it. The moment your declared level trades with the body and volume you asked for, act — or revise the trigger out loud. "One more wake to be sure" after your own condition has fired is the failure, not the caution.

LATE-SESSION ENTRIES
Past the configured cutoff (see execution warnings), a same-day (0DTE) entry is blocked — brokers themselves restrict trading same-day contracts as expiry nears, this is not just caution on our side. A next-day (1DTE) contract is not affected by that cutoff. If the evidence is strong late in the session, set expiry_dte to 1 rather than assuming no trade is possible — theta is far less brutal with a full extra day of time value.

HONESTY REQUIREMENTS
- Never manufacture a setup because you were asked. WAIT and PASS exist for a reason.
- Confidence is a real number, not a courtesy. 8+ means you would take this trade with your own money without hesitation. Most valid setups are 6-7.
- If the data looks wrong or incomplete, say so and PASS.
- You have no verified track record yet. Do not reason as though the playbook's setups are proven.
- THE NUMBER OF WAKES YOU HAVE WAITED IS NOT EVIDENCE. Waiting ten times creates no setup that was not there on the first. The counter-rules that mention repeated waiting (PERFECT_ENTRY_TRAP, the EXHAUSTED_EXTREME counter-rule) fire only when their structural condition holds — a confirmed trend day with flow agreeing — and they choose WHERE inside that trend to enter, never WHETHER to trade at all. An override whose honest driver is "this is my Nth consecutive wake" while you yourself admit the caution's condition is present is the banned pattern, and it has a body: 2026-08-26, a 4th-wake PUT justified by the counter-rule alone, bought at RSI(1m) 23 on the floor of a week-tested range in a chop regime, 0.15% from its own target — stopped on the bounce. If your justification section would survive with the wake-count sentence deleted, delete the sentence; if it would not survive, delete the trade.

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
            "triggers": {
                "type": "array",
                "description": (
                    "On WAIT or PASS: the numeric conditions that would arm an entry. "
                    "Whatever you put here BINDS you — the engine will refuse the next "
                    "ENTER in that direction until one of these levels actually "
                    "trades. If a direction has two ways in ('a break of 718.79, OR a "
                    "bounce into 720.6-721.1 that fails'), list BOTH — they are "
                    "alternatives and any one of them arms the entry, so a setup with "
                    "two roads must not be squeezed into one number. Name the levels "
                    "you mean, not comfortable ones, and re-declare them each wake if "
                    "your read has changed. Omit rather than invent: no trigger means "
                    "no lock. Leave empty on ENTER."
                ),
                "items": {
                    "type": "object",
                    "properties": {
                        "direction": {"type": "string", "enum": ["CALL", "PUT"]},
                        "level": {
                            "type": "number",
                            "description": "UNDERLYING price that arms this entry.",
                        },
                        "side": {
                            "type": "string",
                            "enum": ["above", "below"],
                            "description": (
                                "'below': arms once spot trades at or under level "
                                "(a breakdown). 'above': at or over (a reclaim)."
                            ),
                        },
                        "note": {"type": "string", "description": "The rest of the setup, briefly."},
                    },
                    "required": ["direction", "level", "side"],
                },
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
    # a multi-session table (the hourly) repeats every clock time once per day,
    # so "09:30" alone would name two different candles and any pattern read
    # across them would be nonsense
    spans_days = len({b.ts.astimezone(MARKET_TZ).date() for b in bars}) > 1
    stamp = "%m-%d %H:%M" if spans_days else "%H:%M"
    width = 11 if spans_days else 5
    header = f"{'time':>{width}}  {'open':>8} {'high':>8} {'low':>8} {'close':>8}  {'body':>6}  {'volume':>10}"
    if has_counts:
        header += f"  {'avg_size':>8}"
    lines = [f"{label} ({len(bars)} candles)", header]
    for bar in bars:
        span = bar.high - bar.low
        # share of the candle's range occupied by its body, signed by direction:
        # the one number that separates a decisive candle from a rejection wick
        body = ((bar.close - bar.open) / span * 100) if span > 0 else 0.0
        row = (
            f"{bar.ts.astimezone(MARKET_TZ).strftime(stamp):>{width}}  "
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
        after_close_pending = False
        for event in calendar_events:
            when = event.get("time_et", "?")
            marker = event.get("minutes_from_now")
            timing = ""
            if isinstance(marker, (int, float)):
                timing = (
                    f" (in {abs(marker):.0f} min)" if marker > 0 else f" ({abs(marker):.0f} min ago)"
                )
            note = ""
            if event.get("after_close"):
                after_close_pending = True
                note = " — RELEASED AFTER TODAY'S CLOSE"
            lines.append(
                f"- {when} ET: {event.get('label', '?')} [{event.get('impact', '?')}]{timing}{note}"
            )
        guidance = (
            "\nEvent days have their own character: the hour before a high-impact "
            "release is usually positioning noise — a breakout there rarely holds. "
            "The first minutes after a release are violent whipsaw where stops die; "
            "the tradeable move is the trend that emerges once the reaction picks a "
            "side. Weigh every setup against where you are relative to the event."
        )
        if after_close_pending:
            guidance += (
                "\n\nONE EVENT TODAY LANDS AFTER THE BELL, AND THAT CHANGES THE WHOLE "
                "SESSION, NOT ITS LAST HOUR. Two facts follow and neither is optional. "
                "First, there is no 'after the release' for you: a 0DTE contract "
                "expires this afternoon, so it dies before the catalyst prints. The "
                "move everyone is waiting for is tomorrow's gap and you cannot be in "
                "it. Second, the entire session in front of you is pre-event "
                "positioning — desks hedge rather than take direction into the print, "
                "so the day tends to compress into a range, breakouts fail back into "
                "it, and the volatility that would pay for your contract is being "
                "stored for tonight instead of spent today. This is the classic shape "
                "of a mega-cap earnings session in an index that holds that name. "
                "Treat the countdown in `minutes_from_now` as irrelevant — the "
                "constraint applies from the opening bell. WAITING IS THE DEFAULT "
                "TODAY. Enter only on a genuine structural break with volume behind "
                "it, size the target off what the tape has actually produced today "
                "rather than off a normal session, and say in your thesis that you "
                "are trading into an after-close event anyway and why the setup earns "
                "the exception."
            )
        sections.append(
            "=== ECONOMIC CALENDAR TODAY (operator-maintained schedule) ===\n"
            + "\n".join(lines)
            + guidance
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

    if snapshot.hourly:
        block = [
            "=== HOURLY (built across the last few sessions) ===",
            "The frame the day is happening inside. A single session holds only "
            "six and a half hourly candles, so this is built from several — "
            "`sessions_covered` says how many. Use it for direction and for the "
            "levels that matter beyond today: an intraday setup that fights the "
            "hourly structure needs a much better reason than one that runs with "
            "it, and the hourly swing levels are where the day's moves keep "
            "stopping.",
            _compact(snapshot.hourly),
        ]
        if snapshot.recent_bars_1h:
            block.append(_candle_table(snapshot.recent_bars_1h, "HOURLY"))
        sections.append("\n".join(block))

    if snapshot.gap and snapshot.gap.get("direction") not in (None, "none"):
        sections.append(
            "=== OPENING GAP ===\n"
            "Today's open against yesterday's close. This is the one "
            "daily-timeframe fact a position expiring this afternoon can act "
            "on, because it names a specific level and a specific behaviour: "
            "`fill_level` is yesterday's close, and price either returns to it "
            "or it does not. An unfilled gap that is holding is a trend day "
            "signature and fading it is expensive; a gap that fills early often "
            "keeps going the other way. `pct_to_fill` is how far price still "
            "has to travel to close it, signed in the direction it would move.\n"
            + _compact(snapshot.gap)
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

    if snapshot.multiday:
        sections.append(
            "=== THE WEEK BEHIND TODAY ===\n"
            "The chart scrolled left. `high`/`low` are the extremes of the last "
            "few sessions and `range_position_pct` says where price sits inside "
            "them: 0 is the floor of the week, 100 its ceiling, 50 the middle. "
            "`repeated` lists the prices this week keeps stopping at — `touches` "
            "is how many swings agree on that level and `kind: both` means it "
            "has served as resistance AND support, which is the strongest kind "
            "there is.\n"
            "Read it as location, not as a signal. The same one-minute setup is "
            "a different trade at the edge of a multi-day range than in the "
            "middle of one: near a level tested repeatedly, a break needs volume "
            "behind it or it is the failure that funds the other side, and a "
            "rejection there is a real thesis with a small invalidation. In the "
            "middle of a week-long range — no level near, `range_position_pct` "
            "around 50 — there is usually no asymmetry to buy, and PASS is the "
            "honest answer however clean the tape looks. A narrow "
            "`range_width_pct` says the week itself is compressed, so the move "
            "your target needs may be larger than anything this week produced.\n"
            + _compact(snapshot.multiday)
        )

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
            "`unusual_activity` lists strikes whose day volume exceeds their open "
            "interest. Open interest is every contract that existed at the open, so "
            "volume above it cannot all be traders closing what was already there — "
            "most of it is NEW positioning, and a high vol_oi_ratio on a strike "
            "nobody was holding is the shape of somebody starting something. It is "
            "the only whale signal here that covers the WHOLE chain rather than the "
            "strikes nearest the money, so it is where a desk quietly building an "
            "out-of-the-money position becomes visible; `distance_pct` says how far "
            "from spot that is. It carries no aggressor side, so it tells you "
            "positioning is new, never which way it leans — pair it with the live "
            "flow and with price before drawing a direction from it.\n"
            + _compact(options_pulse)
        )

    if snapshot.leader_detail:
        block = [
            "=== INDEX HEAVYWEIGHTS ===",
            "QQQ is a weighted basket of these names, so they lead it far more "
            "often than they follow. Each carries the same readings the index "
            "itself gets — day change against yesterday's close, momentum, "
            "position versus its own VWAP, relative volume, and its own Dow "
            "structure on 5m and 15m — plus its recent five-minute candles.",
            "Use them two ways. CONFIRMATION: an index breakout the heavyweights "
            "are not making with it is thin, and usually fails. DIVERGENCE: when "
            "the heavyweights turn before the index — a leader reclaiming its "
            "VWAP or breaking its 5m structure while QQQ is still falling — that "
            "is the earliest warning you get, and it is often the whole edge. "
            "Say in your thesis whether the leaders confirm or contradict you.",
            _compact(snapshot.leader_detail),
        ]
        for symbol, bars in snapshot.leader_bars_5m.items():
            if bars:
                block.append(_candle_table(bars, f"{symbol} 5-MINUTE"))
        sections.append("\n".join(block))
    elif snapshot.leaders:
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
