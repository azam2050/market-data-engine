"""Institutional options flow — the 'whale' layer.

A print becomes interesting when it is (a) large in premium, (b) aggressive
(lifting the offer or hitting the bid rather than resting at mid), and (c) fast
— several exchanges hit inside a short window, which is what a sweep is.

None of this vetoes a trade. It produces evidence the brain weighs.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import FlowEvent, FlowKind, FlowSummary, OptionType

BLOCK_PREMIUM_USD = 100_000.0
NOTABLE_PREMIUM_USD = 50_000.0
SWEEP_WINDOW_SEC = 3
SWEEP_MIN_LEGS = 3


def classify_aggressor(price: float, bid: float | None, ask: float | None) -> str:
    """Where in the spread the trade printed tells you who was in a hurry."""
    if bid is None or ask is None or ask <= bid:
        return "MID"
    mid = (bid + ask) / 2.0
    if price >= ask - (ask - bid) * 0.1:
        return "BUY"
    if price <= bid + (ask - bid) * 0.1:
        return "SELL"
    return "BUY" if price > mid else "SELL"


def detect_sweeps(events: list[FlowEvent]) -> list[FlowEvent]:
    """Group same-contract prints inside a short window into sweeps."""
    by_contract: dict[str, list[FlowEvent]] = defaultdict(list)
    for event in events:
        by_contract[event.occ_symbol].append(event)

    marked: list[FlowEvent] = []
    for contract_events in by_contract.values():
        contract_events.sort(key=lambda e: e.ts)
        i = 0
        while i < len(contract_events):
            window = [contract_events[i]]
            j = i + 1
            while (
                j < len(contract_events)
                and (contract_events[j].ts - contract_events[i].ts).total_seconds() <= SWEEP_WINDOW_SEC
            ):
                window.append(contract_events[j])
                j += 1

            if len(window) >= SWEEP_MIN_LEGS:
                total_size = sum(e.size for e in window)
                total_premium = sum(e.premium for e in window)
                head = window[0]
                marked.append(
                    head.model_copy(
                        update={
                            "kind": FlowKind.SWEEP,
                            "size": total_size,
                            "premium": round(total_premium, 2),
                            "exchanges": len(window),
                        }
                    )
                )
                i = j
                continue

            event = contract_events[i]
            if event.premium >= BLOCK_PREMIUM_USD:
                marked.append(event.model_copy(update={"kind": FlowKind.BLOCK}))
            else:
                marked.append(event)
            i += 1

    marked.sort(key=lambda e: e.ts)
    return marked


def summarize_flow(
    events: list[FlowEvent], now: datetime, window_minutes: int = 15
) -> FlowSummary:
    """Roll recent prints into the one object the brain reads."""
    cutoff = now - timedelta(minutes=window_minutes)
    recent = [e for e in events if e.ts >= cutoff]
    summary = FlowSummary(window_minutes=window_minutes)
    if not recent:
        return summary

    for event in recent:
        # a bought call and a sold put both express upside
        directional_premium = event.premium if event.is_bullish else -event.premium
        if event.option_type is OptionType.CALL:
            summary.call_premium += event.premium
        else:
            summary.put_premium += event.premium
        summary.net_premium += directional_premium
        if event.expiry <= now.astimezone(MARKET_TZ).date():
            summary.net_premium_0dte += directional_premium
        else:
            summary.net_premium_dated += directional_premium

        if event.kind is FlowKind.SWEEP:
            summary.sweep_count += 1
        elif event.kind is FlowKind.BLOCK:
            summary.block_count += 1

    summary.net_premium_0dte = round(summary.net_premium_0dte, 2)
    summary.net_premium_dated = round(summary.net_premium_dated, 2)
    summary.call_premium = round(summary.call_premium, 2)
    summary.put_premium = round(summary.put_premium, 2)
    summary.net_premium = round(summary.net_premium, 2)

    if summary.put_premium > 0:
        summary.call_put_ratio = round(summary.call_premium / summary.put_premium, 2)

    total_premium = summary.call_premium + summary.put_premium
    aggression = (summary.sweep_count * 2 + summary.block_count) / 10.0
    size_factor = min(total_premium / 5_000_000.0, 1.0)
    summary.urgency = round(min(aggression * 0.6 + size_factor * 0.4, 1.0), 3)

    summary.notable = sorted(
        [e for e in recent if e.premium >= NOTABLE_PREMIUM_USD],
        key=lambda e: e.premium,
        reverse=True,
    )[:8]
    return summary


def flow_bias(summary: FlowSummary | None) -> float:
    """Directional lean of the tape, -1..+1. Advisory input to the brain."""
    if summary is None:
        return 0.0
    total = summary.call_premium + summary.put_premium
    if total <= 0:
        return 0.0
    raw = summary.net_premium / total
    # scale by urgency: quiet tape should not swing the brain much
    return round(max(-1.0, min(1.0, raw)) * (0.4 + 0.6 * summary.urgency), 3)
