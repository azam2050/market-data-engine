"""Option pricing.

Two implementations behind one interface:

* ``HistoricalPricer`` replays real contract bars — used whenever the data
  subscription provides them. This is the truth.
* ``BlackScholesPricer`` synthesises a price from the underlying. Used for
  offline demos and to fill gaps in thin contracts. Clearly marked as an
  approximation so backtest reports never confuse the two.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Protocol

from qqq_alpha.domain import Bar, OptionType

TRADING_MINUTES_PER_YEAR = 252 * 390
RISK_FREE_RATE = 0.04


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def black_scholes(
    spot: float,
    strike: float,
    years_to_expiry: float,
    volatility: float,
    option_type: OptionType,
    rate: float = RISK_FREE_RATE,
) -> float:
    """European option value. Good enough for index options like QQQ."""
    if years_to_expiry <= 0 or volatility <= 0:
        intrinsic = (
            spot - strike if option_type is OptionType.CALL else strike - spot
        )
        return max(intrinsic, 0.0)

    sqrt_t = math.sqrt(years_to_expiry)
    d1 = (math.log(spot / strike) + (rate + 0.5 * volatility**2) * years_to_expiry) / (
        volatility * sqrt_t
    )
    d2 = d1 - volatility * sqrt_t
    discount = math.exp(-rate * years_to_expiry)

    if option_type is OptionType.CALL:
        return spot * _norm_cdf(d1) - strike * discount * _norm_cdf(d2)
    return strike * discount * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def implied_volatility(
    market_price: float,
    spot: float,
    strike: float,
    years_to_expiry: float,
    option_type: OptionType,
) -> float | None:
    """Bisection solve. Returns None when the price is outside a sane range."""
    if market_price <= 0 or years_to_expiry <= 0:
        return None
    low, high = 0.01, 5.0
    for _ in range(60):
        mid = (low + high) / 2.0
        value = black_scholes(spot, strike, years_to_expiry, mid, option_type)
        if abs(value - market_price) < 1e-4:
            return round(mid, 4)
        if value > market_price:
            high = mid
        else:
            low = mid
    return round((low + high) / 2.0, 4)


def years_to_expiry(now: datetime, expiry: date, close_hour_utc: int = 20) -> float:
    """0DTE-aware time value. Expiry is 16:00 ET on the expiry date."""
    expiry_dt = datetime(
        expiry.year, expiry.month, expiry.day, close_hour_utc, 0, tzinfo=now.tzinfo
    )
    seconds = (expiry_dt - now).total_seconds()
    if seconds <= 0:
        return 0.0
    # calendar time, not trading time — theta on 0DTE is brutal either way
    return seconds / (365 * 24 * 3600)


class OptionPricer(Protocol):
    def price_at(
        self, occ_symbol: str, ts: datetime, spot: float, side: str = "mid"
    ) -> float | None: ...

    @property
    def is_approximation(self) -> bool: ...


class HistoricalPricer:
    """Serves real recorded contract prices, minute by minute."""

    def __init__(self, bars_by_contract: dict[str, list[Bar]]):
        self._index: dict[str, dict[datetime, Bar]] = {
            occ: {bar.ts.replace(second=0, microsecond=0): bar for bar in bars}
            for occ, bars in bars_by_contract.items()
        }

    @property
    def is_approximation(self) -> bool:
        return False

    def price_at(
        self, occ_symbol: str, ts: datetime, spot: float, side: str = "mid"
    ) -> float | None:
        series = self._index.get(occ_symbol)
        if not series:
            return None
        bar = series.get(ts.replace(second=0, microsecond=0))
        return bar.close if bar else None

    def high_low_at(self, occ_symbol: str, ts: datetime) -> tuple[float, float] | None:
        series = self._index.get(occ_symbol)
        if not series:
            return None
        bar = series.get(ts.replace(second=0, microsecond=0))
        return (bar.high, bar.low) if bar else None


class BlackScholesPricer:
    """Synthesises contract prices from the underlying. Approximation only."""

    def __init__(self, volatility: float = 0.22):
        self.volatility = volatility

    @property
    def is_approximation(self) -> bool:
        return True

    def price_at(
        self, occ_symbol: str, ts: datetime, spot: float, side: str = "mid"
    ) -> float | None:
        from qqq_alpha.data.massive import parse_occ_symbol

        _, expiry, option_type, strike = parse_occ_symbol(occ_symbol)
        tau = years_to_expiry(ts, expiry)
        value = black_scholes(spot, strike, tau, self.volatility, option_type)
        return round(max(value, 0.01), 2)
