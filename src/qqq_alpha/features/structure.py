"""Market structure: swing points and the Dow-theory read of them.

Indicators answer "how fast" and "how stretched". They never answer the
question a discretionary trader asks first — *is this thing making higher highs
or lower highs, and has that changed?* That is Dow's original definition of a
trend, it is the frame most price-action traders actually work in, and no
moving average encodes it.

Computed here rather than described to the model in prose, because a swing high
is an arithmetic fact about the bars. The model should spend its judgement on
what the structure means, not on re-deriving it from a candle table.
"""

from __future__ import annotations

from dataclasses import dataclass

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import Bar

# a pivot needs this many bars on each side to count. Two is the standard
# fractal: tight enough to catch intraday swings, wide enough that a single
# noisy bar cannot mint a swing point of its own.
PIVOT_WIDTH = 2


@dataclass
class Swing:
    ts: object
    price: float
    kind: str  # "high" or "low"

    def as_row(self) -> dict:
        return {
            "time": self.ts.astimezone(MARKET_TZ).strftime("%H:%M"),
            "price": round(self.price, 2),
            "kind": self.kind,
        }


def swing_points(bars: list[Bar], width: int = PIVOT_WIDTH) -> list[Swing]:
    """Fractal pivots: a bar whose high tops its neighbours, or low bottoms them.

    Consecutive same-kind pivots are collapsed to the more extreme one, so the
    output alternates high/low the way a hand-drawn swing chart does.
    """
    if len(bars) < 2 * width + 1:
        return []

    found: list[Swing] = []
    for index in range(width, len(bars) - width):
        window = bars[index - width : index + width + 1]
        bar = bars[index]
        if bar.high >= max(b.high for b in window):
            found.append(Swing(bar.ts, bar.high, "high"))
        elif bar.low <= min(b.low for b in window):
            found.append(Swing(bar.ts, bar.low, "low"))

    collapsed: list[Swing] = []
    for swing in found:
        if collapsed and collapsed[-1].kind == swing.kind:
            previous = collapsed[-1]
            better = (
                swing.price > previous.price
                if swing.kind == "high"
                else swing.price < previous.price
            )
            if better:
                collapsed[-1] = swing
            continue
        collapsed.append(swing)
    return collapsed


def classify(swings: list[Swing]) -> tuple[str, str]:
    """The Dow read of a swing series: (trend, the reason in one line).

    Deliberately conservative. Two confirming pairs make a trend; anything else
    is a range, and a range called a trend is how a desk buys the top of a
    box. The reason string is returned so the model can see the evidence
    rather than being handed a verdict to trust.
    """
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return "undefined", "لا توجد نقاط تأرجح كافية بعد"

    higher_high = highs[-1].price > highs[-2].price
    higher_low = lows[-1].price > lows[-2].price
    lower_high = highs[-1].price < highs[-2].price
    lower_low = lows[-1].price < lows[-2].price

    if higher_high and higher_low:
        return "uptrend", (
            f"higher high {highs[-1].price:.2f} > {highs[-2].price:.2f} and "
            f"higher low {lows[-1].price:.2f} > {lows[-2].price:.2f}"
        )
    if lower_high and lower_low:
        return "downtrend", (
            f"lower high {highs[-1].price:.2f} < {highs[-2].price:.2f} and "
            f"lower low {lows[-1].price:.2f} < {lows[-2].price:.2f}"
        )
    if higher_high and lower_low:
        return "expanding_range", "higher high AND lower low — a widening range, not a trend"
    if lower_high and higher_low:
        return "contracting_range", "lower high AND higher low — a coil; expect expansion"
    return "range", "highs and lows disagree — no confirmed Dow trend"


def describe(bars: list[Bar], width: int = PIVOT_WIDTH, keep: int = 6) -> dict:
    """One timeframe's structure, ready to hand to the model."""
    swings = swing_points(bars, width)
    trend, reason = classify(swings)
    recent = swings[-keep:]

    # the level that would break the structure: the last opposing swing. In an
    # uptrend that is the most recent higher low — lose it and the sequence of
    # higher lows is over, which is the definition of the trend ending.
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if trend == "uptrend" and lows:
        break_level, break_note = lows[-1].price, "كسره ينهي تتابع القيعان الصاعدة"
    elif trend == "downtrend" and highs:
        break_level, break_note = highs[-1].price, "اختراقه ينهي تتابع القمم الهابطة"
    else:
        break_level, break_note = None, ""

    return {
        "trend": trend,
        "why": reason,
        "structure_break_level": round(break_level, 2) if break_level else None,
        "structure_break_note": break_note,
        "swings": [s.as_row() for s in recent],
    }
