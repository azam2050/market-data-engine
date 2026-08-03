"""Data quality inspection.

A trading engine that cannot tell good data from bad will eventually act on bad
data with full confidence. That failure is silent, which makes it the worst kind.

So every session is inspected before it is used, and the verdict travels with the
snapshot: how complete the session is, where the gaps are, whether prices have
frozen. The safety rails treat unusable data as an execution blocker (it is), and
the brain is told about degraded-but-usable data so it can lower its confidence
rather than pretend the picture is clean.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from qqq_alpha.config import MARKET_TZ, REGULAR_OPEN
from qqq_alpha.domain import Bar

# a session with fewer than this share of its expected minutes is not a session
MIN_USABLE_COMPLETENESS = 0.80
# consecutive identical closes that suggest a frozen or stale feed
FROZEN_BAR_THRESHOLD = 15


@dataclass
class DataQuality:
    expected_bars: int = 0
    actual_bars: int = 0
    completeness: float = 0.0
    gaps: list[tuple[datetime, int]] = field(default_factory=list)
    duplicates: int = 0
    zero_volume_bars: int = 0
    longest_frozen_run: int = 0
    issues: list[str] = field(default_factory=list)

    @property
    def is_usable(self) -> bool:
        return (
            self.completeness >= MIN_USABLE_COMPLETENESS
            and self.longest_frozen_run < FROZEN_BAR_THRESHOLD
        )

    @property
    def is_pristine(self) -> bool:
        return not self.issues

    def summary(self) -> str:
        if self.is_pristine:
            return f"clean ({self.actual_bars} bars)"
        return f"{self.completeness:.0%} complete; " + "; ".join(self.issues)


def inspect_session(bars: list[Bar], expected_minutes: int | None = None) -> DataQuality:
    """Check one session's minute bars for the failures that actually happen."""
    quality = DataQuality()
    if not bars:
        quality.issues.append("no bars at all")
        return quality

    ordered = sorted(bars, key=lambda b: b.ts)
    quality.actual_bars = len(ordered)

    # --- how much of the session did we actually receive? ---
    first_local = ordered[0].ts.astimezone(MARKET_TZ)
    last_local = ordered[-1].ts.astimezone(MARKET_TZ)
    if expected_minutes is None:
        session_open = first_local.replace(
            hour=REGULAR_OPEN.hour, minute=REGULAR_OPEN.minute, second=0, microsecond=0
        )
        elapsed = int((last_local - session_open).total_seconds() // 60) + 1
        expected_minutes = max(elapsed, 1)

    quality.expected_bars = expected_minutes
    quality.completeness = round(min(len(ordered) / expected_minutes, 1.0), 4)
    if quality.completeness < MIN_USABLE_COMPLETENESS:
        quality.issues.append(
            f"only {quality.completeness:.0%} of expected bars present"
        )

    # --- gaps and duplicates ---
    seen: set[datetime] = set()
    frozen_run = 1
    for index, bar in enumerate(ordered):
        minute = bar.ts.replace(second=0, microsecond=0)
        if minute in seen:
            quality.duplicates += 1
        seen.add(minute)

        if bar.volume <= 0:
            quality.zero_volume_bars += 1

        if index > 0:
            delta = int((bar.ts - ordered[index - 1].ts).total_seconds() // 60)
            if delta > 1:
                quality.gaps.append((ordered[index - 1].ts, delta - 1))

            if bar.close == ordered[index - 1].close:
                frozen_run += 1
                quality.longest_frozen_run = max(quality.longest_frozen_run, frozen_run)
            else:
                frozen_run = 1

    if quality.duplicates:
        quality.issues.append(f"{quality.duplicates} duplicate timestamps")

    missing = sum(count for _, count in quality.gaps)
    if missing:
        quality.issues.append(f"{missing} missing minutes across {len(quality.gaps)} gaps")

    if quality.zero_volume_bars > len(ordered) * 0.2:
        quality.issues.append(f"{quality.zero_volume_bars} bars with no volume")

    if quality.longest_frozen_run >= FROZEN_BAR_THRESHOLD:
        quality.issues.append(
            f"price frozen for {quality.longest_frozen_run} consecutive bars — feed may be stalled"
        )

    return quality


def dedupe(bars: list[Bar]) -> list[Bar]:
    """Keep the last bar for each minute. Providers occasionally revise a bar."""
    by_minute: dict[datetime, Bar] = {}
    for bar in sorted(bars, key=lambda b: b.ts):
        by_minute[bar.ts.replace(second=0, microsecond=0)] = bar
    return [by_minute[key] for key in sorted(by_minute)]


def fill_gaps(bars: list[Bar], max_fill: int = 3) -> list[Bar]:
    """Bridge short gaps with flat synthetic bars so indicators stay aligned.

    Only short gaps are filled: a missing minute in a thin market is normal, but
    a ten-minute hole is a data problem and must stay visible rather than be
    papered over. Filled bars carry zero volume so they cannot fake conviction.
    """
    if len(bars) < 2:
        return list(bars)

    ordered = sorted(bars, key=lambda b: b.ts)
    out: list[Bar] = [ordered[0]]

    for previous, current in zip(ordered, ordered[1:], strict=False):
        missing = int((current.ts - previous.ts).total_seconds() // 60) - 1
        if 0 < missing <= max_fill:
            for step in range(1, missing + 1):
                out.append(
                    Bar(
                        symbol=previous.symbol,
                        ts=previous.ts + timedelta(minutes=step),
                        open=previous.close,
                        high=previous.close,
                        low=previous.close,
                        close=previous.close,
                        volume=0,
                        vwap=previous.vwap,
                    )
                )
        out.append(current)

    return out
