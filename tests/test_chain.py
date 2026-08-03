"""Tests for live chain pricing.

The theme here is one specific form of self-deception: pricing both sides of a
trade at the mid. It is the easiest way to build a track record that cannot be
reproduced with real money, so the spread handling is tested directly.
"""

from datetime import UTC, date, datetime, timedelta

from qqq_alpha.config import Settings
from qqq_alpha.data.chain import ChainSnapshot, LiveChainPricer
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.domain import OptionContract, OptionType

EXPIRY = date(2026, 8, 4)
NOW = datetime(2026, 8, 3, 14, 0, tzinfo=UTC)


def _contract(strike: float, option_type: OptionType, bid: float, ask: float, **kwargs):
    letter = "C" if option_type is OptionType.CALL else "P"
    return OptionContract(
        occ_symbol=f"O:QQQ260804{letter}{int(strike * 1000):08d}",
        underlying="QQQ",
        option_type=option_type,
        strike=strike,
        expiry=EXPIRY,
        bid=bid,
        ask=ask,
        volume=kwargs.get("volume", 500),
        open_interest=kwargs.get("open_interest", 2000),
        delta=kwargs.get("delta", 0.42),
        implied_volatility=kwargs.get("iv", 0.19),
    )


def _pricer(contracts, fallback=None, age_sec: float = 0.0) -> LiveChainPricer:
    pricer = LiveChainPricer(Settings(massive_api_key="k"), fallback=fallback)
    pricer.snapshot = ChainSnapshot(
        fetched_at=datetime.now(UTC) - timedelta(seconds=age_sec),
        expiry=EXPIRY,
        contracts={c.occ_symbol: c for c in contracts},
    )
    return pricer


# ------------------------------------------------------------- spread handling
def test_entry_fills_at_the_ask_and_exit_at_the_bid():
    """Mid-pricing both sides manufactures profit that will not exist."""
    contract = _contract(485, OptionType.CALL, bid=1.00, ask=1.10)
    pricer = _pricer([contract])
    symbol = contract.occ_symbol

    assert pricer.price_at(symbol, NOW, 485.0, side="entry") == 1.10
    assert pricer.price_at(symbol, NOW, 485.0, side="exit") == 1.00
    assert pricer.price_at(symbol, NOW, 485.0) == 1.05  # mark only


def test_the_spread_is_paid_twice_on_a_round_trip():
    """Buy at ask, sell at bid: a flat contract is already a loss."""
    contract = _contract(485, OptionType.CALL, bid=1.00, ask=1.10)
    pricer = _pricer([contract])
    symbol = contract.occ_symbol

    entry = pricer.entry_price(symbol, NOW, 485.0)
    exit_now = pricer.exit_price(symbol, NOW, 485.0)

    assert entry > exit_now
    round_trip_pct = (exit_now - entry) / entry * 100
    assert round_trip_pct < -8  # ~9% lost before the market moves at all


def test_missing_quote_falls_back_to_the_bid_side_of_nothing():
    """A contract quoted only on one side still returns something usable."""
    contract = _contract(485, OptionType.CALL, bid=0.0, ask=1.20)
    pricer = _pricer([contract])
    # bid is unusable, so exit falls back to the mid rather than returning zero
    assert pricer.price_at(contract.occ_symbol, NOW, 485.0, side="exit") == contract.mid


# ------------------------------------------------------------- honesty flags
def test_pricer_reports_itself_as_real_only_when_it_is():
    contract = _contract(485, OptionType.CALL, bid=1.0, ask=1.1)

    fresh = _pricer([contract])
    assert not fresh.is_approximation

    stale = _pricer([contract], age_sec=600)
    assert stale.is_approximation

    empty = LiveChainPricer(Settings(massive_api_key="k"))
    assert empty.is_approximation


def test_fallback_is_used_and_counted():
    """Silent fallbacks would hide how much of a record is modelled."""
    pricer = _pricer([], fallback=BlackScholesPricer())
    price = pricer.price_at("O:QQQ260804C00485000", NOW, 485.0)

    assert price is not None
    assert pricer.fallback_uses == 1
    assert "1 fallback prices used" in pricer.status


def test_no_fallback_means_no_invented_price():
    pricer = _pricer([])
    assert pricer.price_at("O:QQQ260804C00485000", NOW, 485.0) is None


# ------------------------------------------------------------- strike selection
def test_nearby_returns_strikes_closest_to_the_money_first():
    contracts = [
        _contract(strike, OptionType.CALL, bid=1.0, ask=1.1)
        for strike in (480, 483, 485, 487, 490)
    ]
    pricer = _pricer(contracts)

    nearby = pricer.nearby(485.2, OptionType.CALL, count=3)
    assert [c.strike for c in nearby] == [485.0, 487.0, 483.0]


def test_nearby_excludes_far_strikes_and_unquoted_contracts():
    contracts = [
        _contract(485, OptionType.CALL, bid=1.0, ask=1.1),
        _contract(600, OptionType.CALL, bid=0.01, ask=0.02),  # far out of range
        _contract(486, OptionType.CALL, bid=0.0, ask=0.0),    # no quote at all
    ]
    pricer = _pricer(contracts)

    strikes = [c.strike for c in pricer.nearby(485.0, OptionType.CALL, count=10)]
    assert strikes == [485.0]


def test_chain_context_gives_the_brain_what_it_needs_to_choose():
    contracts = [
        _contract(485, OptionType.CALL, bid=1.00, ask=1.08),
        _contract(484, OptionType.PUT, bid=0.90, ask=0.98),
    ]
    rows = _pricer(contracts).chain_context(485.0, count=2)

    assert len(rows) == 2
    for row in rows:
        # everything required to size a target and judge the cost of entry
        for field in ("symbol", "strike", "bid", "ask", "spread_pct", "delta", "open_interest"):
            assert field in row
    assert {row["type"] for row in rows} == {"CALL", "PUT"}


def test_chain_context_is_empty_without_a_chain():
    assert LiveChainPricer(Settings(massive_api_key="k")).chain_context(485.0) == []


# ------------------------------------------------------------- rails integration
def test_rails_can_now_reject_an_illiquid_contract():
    """With a real chain the rails validate the contract, not a guess."""
    from qqq_alpha.brain.rails import SafetyRails
    from qqq_alpha.domain import Action, Decision, Target

    wide = _contract(485, OptionType.CALL, bid=1.00, ask=1.60)
    decision = Decision(
        ts=NOW,
        action=Action.ENTER,
        direction=OptionType.CALL,
        occ_symbol=wide.occ_symbol,
        targets=[Target(label="T1", price=2.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40,
        confidence=8,
        thesis="test",
    )

    verdict = SafetyRails(Settings()).post_check(decision, wide)
    assert not verdict.allowed
    assert any("spread_too_wide" in block for block in verdict.blocks)
