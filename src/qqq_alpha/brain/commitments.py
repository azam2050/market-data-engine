"""The declared-trigger lock.

On 2026-08-19 at 10:18 ET the brain wrote, in its own words, the condition it
would require before shorting:

    "the structure needed for the PUT is a failed bounce under 715.0-716.0
     followed by a down body >= 60% on volume above 200k **and a break of
     713.33**"

Three minutes later it entered a PUT at 713.49 — sixteen cents *above* the
level it had just set for itself. 713.33 was never traded again; price turned
and ran six points the other way. The trade lost 45%.

The same morning, the trade that *did* wait for its declared level (a break of
pivot 718.56, announced one wake earlier and executed when it happened)
returned +60.7%.

Same brain, same day, same method. The only difference was whether it honoured
the number it had written down three minutes before.

So this module holds one rule: **a numeric trigger the brain declares binds it
until the trigger expires or the brain replaces it.** That is not an opinion
about the market — the engine forms no view here about whether 713.33 was the
right level. It is the engine declining to let the brain contradict, minutes
later, a commitment it made in writing.

Deliberate escape hatches, so this stays a discipline check and never becomes a
cage:

* only the *most recent* commitment per direction counts — the brain revises
  its own level simply by naming a new one on the next wake;
* commitments expire (``Settings.trigger_ttl_minutes``) — a level named half an
  hour ago describes a market that no longer exists;
* a commitment in the other direction never blocks;
* an absurd level (further from spot than ``MAX_LEVEL_DISTANCE_PCT``) is
  discarded rather than enforced — a fat-fingered number must not freeze the
  desk;
* a touch anywhere in the bars since the commitment arms it, not just the price
  at this instant. "Break 713.33" means the tape printed it, and a break that
  snaps back is still a break the brain is entitled to act on.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from qqq_alpha.domain import Action, Decision, MarketSnapshot, OptionType, Trigger

# a level this far from spot is not an intraday trigger on a 0DTE contract, it
# is a typo or a misplaced decimal. Enforcing it would block every entry for
# the rest of its life, so it is discarded instead.
MAX_LEVEL_DISTANCE_PCT = 5.0


def live_commitment(
    decisions: list[Decision],
    direction: OptionType,
    now: datetime,
    ttl_minutes: int,
    spot: float | None = None,
) -> tuple[Trigger, datetime] | None:
    """The newest un-expired, plausible trigger declared for ``direction``, and when.

    Reads backwards and stops at the first decision that carries a trigger for
    this direction: an older commitment the brain has already superseded must
    not outlive the revision.
    """
    horizon = now - timedelta(minutes=max(ttl_minutes, 0))
    for decision in reversed(decisions):
        # an ENTER acts, it does not promise; and its own bookkeeping should
        # never become the commitment that judges it
        if decision.action is Action.ENTER:
            continue
        if decision.ts < horizon:
            break
        match = next((t for t in decision.triggers if t.direction is direction), None)
        if match is None:
            continue
        if not _plausible(match, spot):
            return None
        return match, decision.ts
    return None


def _plausible(trigger: Trigger, spot: float | None) -> bool:
    if trigger.level <= 0:
        return False
    if not spot or spot <= 0:
        return True
    return abs(trigger.level - spot) / spot * 100.0 <= MAX_LEVEL_DISTANCE_PCT


def armed(trigger: Trigger, snapshot: MarketSnapshot, since: datetime) -> bool:
    """Has the tape reached the declared level since it was declared?"""
    if trigger.satisfied_by(snapshot.underlying.close):
        return True
    bars = [b for b in snapshot.recent_bars_1m if b.ts >= since]
    if not bars:
        return False
    extreme = min(b.low for b in bars) if trigger.side == "below" else max(b.high for b in bars)
    return trigger.satisfied_by(extreme)


def check(
    decision: Decision,
    snapshot: MarketSnapshot,
    prior_decisions: list[Decision],
    ttl_minutes: int,
) -> str | None:
    """``None`` when the entry is honest; otherwise the rail block explaining why not."""
    if decision.action is not Action.ENTER or decision.direction is None:
        return None

    found = live_commitment(
        prior_decisions,
        decision.direction,
        snapshot.ts,
        ttl_minutes,
        spot=snapshot.underlying.close,
    )
    if found is None:
        return None

    commitment, declared_at = found
    if armed(commitment, snapshot, declared_at):
        return None

    word = "below" if commitment.side == "below" else "above"
    return (
        f"declared_trigger_unmet: you said {decision.direction.value} arms at "
        f"{word} {commitment.level:.2f} "
        f"({declared_at.strftime('%H:%M')}); spot is {snapshot.underlying.close:.2f} "
        "and the level has not traded since"
    )
