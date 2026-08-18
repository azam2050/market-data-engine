"""Options pulse — where today's options money is concentrating.

The data plan has no live options tape (individual sweeps and blocks with
aggressor side), but the chain snapshot it does have carries each contract's
cumulative day volume and open interest. Summarised per underlying, that gives
a trader's "price of the day": the strike attracting the heaviest call money,
the heaviest put money, and which side of the book is dominating.

Computed for QQQ *and* for the index heavyweights, this is the leader signal
the operator asked for: when the leaders' options money leans CALL at strikes
above spot while the index itself opens down, the leaders are voting for a
bottom — and the reverse holds. It is cumulative volume, not the tape, so it
is context rather than confirmation, and it is labelled that way in the prompt.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from qqq_alpha.config import Settings
from qqq_alpha.domain import OptionContract, OptionType

log = logging.getLogger(__name__)

# leaders' pulses change slowly (cumulative day volume) — refetching faster
# than this buys nothing and spends API budget
LEADER_TTL_SEC = 600
# how many leaders to track; each costs two chain requests per refresh
MAX_PULSE_LEADERS = 3
TOP_STRIKES = 3
# "unusual activity": a strike whose day volume exceeds its open interest.
# Open interest is every contract that existed at the open, so volume above it
# cannot be traders closing what was already there — most of it has to be new
# positioning. This is the standard proxy for opening trades, and it is the
# only whale signal that covers the WHOLE chain: the live tape is polled for a
# handful of near-the-money strikes, so a desk quietly building a far out-of-
# the-money position was invisible to it. Here it is not.
UNUSUAL_MIN_VOLUME = 500  # below this, the ratio is noise on a thin strike
UNUSUAL_MIN_RATIO = 1.0
UNUSUAL_TOP_N = 4


def nearest_weekly_expiry(as_of: date) -> date:
    """The nearest Friday (or today, if today is Friday).

    Single names carry weekly options, not the daily expiries QQQ has, so the
    leaders' near-dated money lives on the Friday chain.
    """
    days_ahead = (4 - as_of.weekday()) % 7
    return as_of + timedelta(days=days_ahead)


def chain_pulse(
    underlying: str,
    contracts: list[OptionContract],
    top_n: int = TOP_STRIKES,
    spot: float | None = None,
) -> dict[str, Any] | None:
    """Summarise one chain into the strikes that matter.

    Returns None when the chain has no volume at all — an empty pulse rendered
    as zeros would read like "balanced", which is not what no-data means.
    ``spot`` is optional and only used to label how far each unusual strike
    sits from the money.
    """
    calls = [c for c in contracts if c.option_type is OptionType.CALL]
    puts = [c for c in contracts if c.option_type is OptionType.PUT]

    call_volume = sum(c.volume for c in calls)
    put_volume = sum(c.volume for c in puts)
    if call_volume + put_volume == 0:
        return None

    # calls at 1.0 means perfectly balanced; the thresholds are loose on
    # purpose — a lean should mean something a trader would call a lean
    ratio = put_volume / call_volume if call_volume else float("inf")
    if ratio <= 0.7:
        lean = "CALL"
    elif ratio >= 1.4:
        lean = "PUT"
    else:
        lean = "BALANCED"

    def top(side: list[OptionContract]) -> list[dict[str, Any]]:
        ranked = sorted(side, key=lambda c: c.volume, reverse=True)[:top_n]
        return [
            {"strike": c.strike, "volume": c.volume, "open_interest": c.open_interest}
            for c in ranked
            if c.volume > 0
        ]

    def oi_wall(side: list[OptionContract]) -> dict[str, Any] | None:
        if not side:
            return None
        heaviest = max(side, key=lambda c: c.open_interest)
        if heaviest.open_interest <= 0:
            return None
        return {"strike": heaviest.strike, "open_interest": heaviest.open_interest}

    pulse = {
        "underlying": underlying,
        "call_volume": call_volume,
        "put_volume": put_volume,
        "put_call_ratio": round(ratio, 2) if call_volume else None,
        "lean": lean,
        "top_call_strikes": top(calls),
        "top_put_strikes": top(puts),
        "call_oi_wall": oi_wall(calls),
        "put_oi_wall": oi_wall(puts),
    }
    unusual = unusual_activity(contracts, spot)
    if unusual:
        pulse["unusual_activity"] = unusual
    return pulse


def unusual_activity(
    contracts: list[OptionContract], spot: float | None = None, top_n: int = UNUSUAL_TOP_N
) -> list[dict[str, Any]]:
    """Strikes where today's volume exceeds the open interest.

    Ranked by the volume/OI ratio rather than by raw volume, because the
    at-the-money strike always wins on volume and almost never on this. A high
    ratio on a strike nobody was holding is the shape of somebody starting
    something.

    Honest about what it is: cumulative day volume carries no aggressor side,
    so this says positioning is NEW, not which way the position leans. A large
    ratio on a put strike could be a desk buying protection or selling it.
    """
    rows: list[dict[str, Any]] = []
    for contract in contracts:
        if contract.volume < UNUSUAL_MIN_VOLUME or contract.open_interest <= 0:
            continue
        ratio = contract.volume / contract.open_interest
        if ratio < UNUSUAL_MIN_RATIO:
            continue
        row: dict[str, Any] = {
            "strike": contract.strike,
            "type": contract.option_type.value,
            "volume": contract.volume,
            "open_interest": contract.open_interest,
            "vol_oi_ratio": round(ratio, 2),
        }
        if spot and spot > 0:
            row["distance_pct"] = round((contract.strike - spot) / spot * 100.0, 2)
        rows.append(row)

    rows.sort(key=lambda r: r["vol_oi_ratio"], reverse=True)
    return rows[:top_n]


@dataclass
class PulseTracker:
    """Keeps the leaders' pulses fresh without hammering the API.

    QQQ's own pulse costs nothing — it is computed from the chain the pricer
    already holds. The leaders each cost two requests, so they are cached and
    refreshed on a slow clock, and a failed fetch degrades to "no pulse for
    that symbol" rather than delaying the decision.
    """

    settings: Settings
    ttl_sec: int = LEADER_TTL_SEC
    _cache: dict[str, dict[str, Any]] = field(default_factory=dict)
    _fetched_at: datetime | None = None

    @property
    def leaders(self) -> list[str]:
        return self.settings.leader_symbols[:MAX_PULSE_LEADERS]

    async def refresh_leaders(self, day: date, spots: dict[str, float] | None = None) -> None:
        if (
            self._fetched_at is not None
            and (datetime.now(UTC) - self._fetched_at).total_seconds() < self.ttl_sec
        ):
            return

        from qqq_alpha.data.massive import MassiveClient

        expiry = nearest_weekly_expiry(day)
        try:
            async with MassiveClient(self.settings) as client:
                for symbol in self.leaders:
                    contracts = await client.option_chain(symbol, expiry)
                    # the leaders' chains are already being fetched; running
                    # the unusual-activity scan over them is the whale signal
                    # for the leaders at no additional request cost
                    pulse = chain_pulse(
                        symbol, contracts, spot=(spots or {}).get(symbol)
                    )
                    if pulse is not None:
                        pulse["expiry"] = expiry.isoformat()
                        self._cache[symbol] = pulse
        except Exception as exc:  # noqa: BLE001 - pulse is context, never a blocker
            log.warning("leader pulse refresh failed: %s", exc)
        finally:
            # stamped even on failure: a broken pulse feed must cost one warning
            # per TTL window, not a retry storm in front of every decision
            self._fetched_at = datetime.now(UTC)

    def rows(self, primary_pulse: dict[str, Any] | None) -> list[dict[str, Any]]:
        """Everything the prompt should see: the index first, then the leaders."""
        rows = [primary_pulse] if primary_pulse else []
        rows.extend(self._cache[s] for s in self.leaders if s in self._cache)
        return rows
