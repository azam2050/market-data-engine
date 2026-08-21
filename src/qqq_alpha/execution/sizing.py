"""Turning a dollar budget into a whole number of contracts.

The engine reasons in percentages and the operator reasons in dollars, and
neither of them can be spent: a broker sells contracts, each worth its price
times a hundred shares. Somewhere that has to be reconciled, and reconciling
it by rounding quietly is how a $1,000 idea becomes a $1,800 position on the
one day the contract was expensive.

So the budget carries a band. A size is only acceptable if the money it puts
at risk lands inside it, and when no whole number of contracts does — which
happens whenever a single contract costs more than the band is wide — this
returns nothing and says why. The trade is skipped. That is a real cost, and
it is smaller than the alternative, which is silently taking a position half
again as large as intended on exactly the contracts that are expensive
because the market expects a violent move.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

# an options contract covers 100 shares; the price is quoted per share
CONTRACT_MULTIPLIER = 100


@dataclass(frozen=True)
class Sizing:
    """A decision about size, and the arithmetic that produced it."""

    contracts: int
    notional: float
    target: float
    low: float
    high: float
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.contracts > 0

    def describe(self) -> str:
        if self.ok:
            return (
                f"{self.contracts} عقدًا = {self.notional:,.0f}$ "
                f"(الهدف {self.target:,.0f}$، المدى {self.low:,.0f}–{self.high:,.0f}$)"
            )
        return self.reason


def size_order(
    price: float,
    target_dollars: float,
    tolerance_pct: float,
    max_contracts: int,
) -> Sizing:
    """How many contracts of ``price`` fit a ``target_dollars`` budget.

    Both neighbours of the ideal fractional size are considered, and the one
    closest to the target wins — so a $1,000 budget against a $3.00 contract
    takes three ($900) rather than refusing because four ($1,200) overshoots.

    ``max_contracts`` is not a sizing preference. It is a backstop against a
    bad price: a stale or broken quote of $0.05 would make the arithmetic ask
    for hundreds of contracts, and no budget check catches that because the
    arithmetic is correct — the input is not.
    """
    tolerance = max(tolerance_pct, 0.0) / 100.0
    low = target_dollars * (1.0 - tolerance)
    high = target_dollars * (1.0 + tolerance)
    if price <= 0 or target_dollars <= 0:
        return Sizing(
            0, 0.0, target_dollars, low, high, reason=f"سعر أو ميزانية غير صالحة ({price})"
        )

    per_contract = price * CONTRACT_MULTIPLIER
    ideal = target_dollars / per_contract

    candidates = sorted(
        {int(ideal), int(ideal) + 1},
        key=lambda n: abs(n * per_contract - target_dollars),
    )
    for count in candidates:
        if count <= 0:
            continue
        notional = count * per_contract
        if not (low <= notional <= high):
            continue
        if count > max_contracts:
            return Sizing(
                0,
                0.0,
                target_dollars,
                low,
                high,
                reason=(
                    f"الحجم المطلوب {count} عقدًا يتجاوز الحد الأقصى {max_contracts} — "
                    f"السعر {price:.2f}$ يبدو خاطئًا أو قديمًا، فلم يُرسَل شيء"
                ),
            )
        return Sizing(count, round(notional, 2), target_dollars, low, high)

    # nothing fits: name the two neighbours so the miss is auditable
    nearest = candidates[0] if candidates else 0
    return Sizing(
        0,
        0.0,
        target_dollars,
        low,
        high,
        reason=(
            f"لا يوجد عدد عقود يقع داخل المدى: العقد بـ {price:.2f}$ "
            f"({per_contract:,.0f}$ للعقد) — "
            f"{max(nearest, 1)} عقدًا = {max(nearest, 1) * per_contract:,.0f}$ "
            f"و{max(nearest, 1) + 1} = {(max(nearest, 1) + 1) * per_contract:,.0f}$، "
            f"والمدى المسموح {low:,.0f}–{high:,.0f}$"
        ),
    )


def split_for_scale_out(held: int, fraction: float) -> int:
    """How many contracts to sell when the exit engine banks part of a position.

    Rounds rather than truncates, because the purpose of the scale-out is to
    take the cost off the table, and under-banking fails at that purpose while
    over-banking merely leaves a smaller runner. Always leaves at least one
    contract running and always sells at least one, so an odd position is
    split unevenly rather than not at all.

    Not ``round()``: that breaks ties to even, so five contracts would bank two
    and run three — the opposite of the intent, on exactly the odd counts this
    function exists for.
    """
    if held < 2:
        return 0
    half = math.floor(held * fraction + 0.5)
    return max(1, min(held - 1, half))
