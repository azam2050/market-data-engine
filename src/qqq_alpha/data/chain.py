"""Live option chain.

Replaces modelled contract prices with the real ones. This matters more than it
sounds: a strategy targeting +50% on a contract lives or dies on the few cents
between the bid, the mid, and the ask.

So this module is deliberate about which side of the spread it uses:

* **Entry** fills at the **ask**. You pay the offer.
* **Exit and marking** use the **bid**. You receive the bid.

Pricing both at the mid — the tempting shortcut — quietly manufactures profit
that does not exist. On a contract with a 4% spread, mid-pricing flatters every
round trip by roughly 4%, and on a stream of small winners that is the entire
result. A backtest or a track record built that way is fiction.

The chain is also what lets the brain pick a strike that actually exists and can
actually be filled, instead of naming a number and hoping.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from qqq_alpha.config import Settings
from qqq_alpha.domain import OptionContract, OptionType

log = logging.getLogger(__name__)

DEFAULT_TTL_SEC = 30
STALE_CHAIN_SEC = 120


@dataclass
class ChainSnapshot:
    fetched_at: datetime
    expiry: date
    contracts: dict[str, OptionContract] = field(default_factory=dict)

    @property
    def age_sec(self) -> float:
        return (datetime.now(UTC) - self.fetched_at).total_seconds()

    @property
    def is_stale(self) -> bool:
        return self.age_sec > STALE_CHAIN_SEC


class LiveChainPricer:
    """Serves real contract prices, with Black-Scholes only as a labelled fallback.

    Implements the OptionPricer protocol, so the live engine, the trade manager
    and the backtester all consume it without knowing the difference.
    """

    def __init__(
        self,
        settings: Settings,
        fallback: Any | None = None,
        ttl_sec: int = DEFAULT_TTL_SEC,
        symbol: str | None = None,
    ):
        self.settings = settings
        # which underlying's chain this instance serves. Defaults to the
        # primary symbol, which is every existing call site; the shadow desk
        # passes a leader so its single names get the same real quotes QQQ has
        # instead of a modelled approximation of them.
        self.symbol = (symbol or settings.primary_symbol).upper()
        self.ttl = timedelta(seconds=ttl_sec)
        self.fallback = fallback
        self.snapshot: ChainSnapshot | None = None
        self.fallback_uses = 0
        self.refreshes = 0
        self.last_error: str | None = None

    # ------------------------------------------------------------------
    @property
    def is_approximation(self) -> bool:
        """True only when we are not actually serving live quotes."""
        return self.snapshot is None or self.snapshot.is_stale

    @property
    def status(self) -> str:
        if self.snapshot is None:
            return "no chain loaded (using modelled prices)"
        return (
            f"{len(self.snapshot.contracts)} contracts, "
            f"{self.snapshot.age_sec:.0f}s old, "
            f"{self.fallback_uses} fallback prices used"
        )

    # ------------------------------------------------------------------
    async def refresh(self, expiry: date, force: bool = False) -> bool:
        """Pull the chain if the cached one has expired. Returns True on success."""
        if (
            not force
            and self.snapshot is not None
            and self.snapshot.expiry == expiry
            and datetime.now(UTC) - self.snapshot.fetched_at < self.ttl
        ):
            return True

        from qqq_alpha.data.massive import MassiveClient

        try:
            async with MassiveClient(self.settings) as client:
                contracts = await client.option_chain(self.symbol, expiry)
        except Exception as exc:  # noqa: BLE001 - a failed refresh must not stop trading
            self.last_error = str(exc)
            log.warning("chain refresh failed for %s %s: %s", self.symbol, expiry, exc)
            return False

        if not contracts:
            self.last_error = f"empty chain for {self.symbol} {expiry}"
            log.warning("chain for %s %s came back empty", self.symbol, expiry)
            return False

        self.snapshot = ChainSnapshot(
            fetched_at=datetime.now(UTC),
            expiry=expiry,
            contracts={c.occ_symbol: c for c in contracts},
        )
        self.refreshes += 1
        self.last_error = None
        return True

    # ------------------------------------------------------------------
    def contract(self, occ_symbol: str) -> OptionContract | None:
        if self.snapshot is None:
            return None
        return self.snapshot.contracts.get(occ_symbol)

    def price_at(
        self, occ_symbol: str, ts: datetime, spot: float, side: str = "mid"
    ) -> float | None:
        """Price a contract. `side` decides which end of the spread you get.

        "entry" -> ask (you pay up), "exit" -> bid (you get hit), "mid" -> mark.
        """
        contract = self.contract(occ_symbol)

        if contract is not None:
            price = _side_price(contract, side)
            if price is not None and price > 0:
                return price

        # no live quote: fall back, and count it so the report can say how often
        if self.fallback is not None:
            self.fallback_uses += 1
            return self.fallback.price_at(occ_symbol, ts, spot)
        return None

    def entry_price(self, occ_symbol: str, ts: datetime, spot: float) -> float | None:
        return self.price_at(occ_symbol, ts, spot, side="entry")

    def exit_price(self, occ_symbol: str, ts: datetime, spot: float) -> float | None:
        return self.price_at(occ_symbol, ts, spot, side="exit")

    # ------------------------------------------------------------------
    def nearby(
        self,
        spot: float,
        option_type: OptionType | None = None,
        count: int = 6,
        max_distance_pct: float = 2.0,
    ) -> list[OptionContract]:
        """Tradeable strikes around the money, nearest first.

        This is what the brain is shown so it names a contract that exists and
        can be filled, rather than a strike it invented.
        """
        if self.snapshot is None or spot <= 0:
            return []

        candidates = [
            contract
            for contract in self.snapshot.contracts.values()
            if (option_type is None or contract.option_type is option_type)
            and abs(contract.strike - spot) / spot * 100.0 <= max_distance_pct
            and contract.mid is not None
            and contract.mid > 0
        ]
        candidates.sort(key=lambda c: abs(c.strike - spot))
        return candidates[:count]

    def chain_context(self, spot: float, count: int = 6) -> list[dict[str, Any]]:
        """The chain rendered for the prompt: everything needed to choose a strike."""
        rows: list[dict[str, Any]] = []
        for option_type in (OptionType.CALL, OptionType.PUT):
            for contract in self.nearby(spot, option_type, count=count):
                rows.append(
                    {
                        "symbol": contract.occ_symbol,
                        "type": contract.option_type.value,
                        "strike": contract.strike,
                        "expiry": contract.expiry.isoformat(),
                        "bid": contract.bid,
                        "ask": contract.ask,
                        "mid": contract.mid,
                        "spread_pct": contract.spread_pct,
                        "delta": contract.delta,
                        "iv": contract.implied_volatility,
                        "volume": contract.volume,
                        "open_interest": contract.open_interest,
                        "distance_pct": round((contract.strike - spot) / spot * 100, 2),
                    }
                )
        return rows


def _side_price(contract: OptionContract, side: str) -> float | None:
    """Which end of the spread applies. Never silently substitutes the mid."""
    if side == "entry":
        return contract.ask if contract.ask and contract.ask > 0 else contract.mid
    if side == "exit":
        return contract.bid if contract.bid and contract.bid > 0 else contract.mid
    return contract.mid
