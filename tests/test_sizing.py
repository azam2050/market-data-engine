"""Turning a dollar budget into contracts.

Pure arithmetic, wired to nothing yet — but the arithmetic is where a $1,000
idea silently becomes a $1,800 position, so it is tested before it is used.
"""

from __future__ import annotations

import pytest

from qqq_alpha.execution.sizing import size_order, split_for_scale_out

BUDGET = 1000.0
BAND = 10.0  # ±$100 on a $1,000 target


def _contracts(price: float, budget: float = BUDGET, band: float = BAND) -> int:
    return size_order(price, budget, band, max_contracts=40).contracts


# ---------------------------------------------------------------- the money is equal
@pytest.mark.parametrize(
    ("price", "expected", "notional"),
    [
        (1.25, 8, 1000.0),  # exact
        (2.00, 5, 1000.0),  # exact
        (0.60, 17, 1020.0),  # rounds up: 17 is nearer $1,000 than 16 ($960)
        (3.00, 3, 900.0),  # rounds down: 4 would be $1,200, outside the band
        (0.35, 29, 1015.0),
    ],
)
def test_a_fixed_budget_buys_different_contract_counts(price, expected, notional):
    """Equal dollars, unequal contracts — the price decides the count."""
    sizing = size_order(price, BUDGET, BAND, max_contracts=40)

    assert sizing.contracts == expected
    assert sizing.notional == pytest.approx(notional)
    assert sizing.ok


def test_the_nearer_neighbour_wins_even_when_both_fit():
    """$1.10 → 9 contracts is $990 and 10 is $1,100; both are inside the band
    and $990 is nearer the target."""
    assert _contracts(1.10) == 9


# ---------------------------------------------------------------- the band bites
def test_an_expensive_contract_has_no_valid_size(capsys):
    """One contract at $6.00 is $600 and two are $1,200 — the band contains
    neither, so nothing is sent.

    This is the tolerance doing its job on exactly the contracts that are
    expensive *because* the market expects a violent move.
    """
    sizing = size_order(6.00, BUDGET, BAND, max_contracts=40)

    assert not sizing.ok
    assert sizing.contracts == 0
    assert "لا يوجد عدد عقود يقع داخل المدى" in sizing.reason
    assert "600" in sizing.reason and "1,200" in sizing.reason


def test_a_wider_band_takes_the_trade_the_narrow_one_refused():
    """Non-vacuity: the refusal above is the band, not broken arithmetic."""
    assert size_order(6.00, BUDGET, 25.0, max_contracts=40).contracts == 2


def test_a_contract_costing_more_than_the_ceiling_is_refused():
    sizing = size_order(15.00, BUDGET, BAND, max_contracts=40)

    assert not sizing.ok


# ---------------------------------------------------------------- bad input
def test_a_suspiciously_cheap_quote_is_refused_not_bought_in_bulk():
    """A stale $0.02 quote asks for 500 contracts. The arithmetic is right and
    the input is wrong, which is what the ceiling exists to catch."""
    sizing = size_order(0.02, BUDGET, BAND, max_contracts=40)

    assert not sizing.ok
    assert "يتجاوز الحد الأقصى" in sizing.reason


def test_a_zero_or_negative_price_is_refused():
    assert not size_order(0.0, BUDGET, BAND, max_contracts=40).ok
    assert not size_order(-1.0, BUDGET, BAND, max_contracts=40).ok


def test_a_zero_budget_buys_nothing():
    assert not size_order(1.25, 0.0, BAND, max_contracts=40).ok


def test_a_zero_tolerance_demands_an_exact_fit():
    assert _contracts(1.25, band=0.0) == 8  # $1,000 exactly
    assert _contracts(0.60, band=0.0) == 0  # nothing lands on $1,000


# ---------------------------------------------------------------- conviction sizing
@pytest.mark.parametrize(
    ("budget", "expected"),
    [(1000.0, 8), (750.0, 6), (500.0, 4), (250.0, 2)],
)
def test_the_budget_scales_with_conviction(budget, expected):
    """$1.25 contracts at each of the four sizes the engine already uses."""
    assert _contracts(1.25, budget=budget) == expected


def test_the_tolerance_stays_proportional_at_smaller_sizes():
    """10% of $250 is $25 — a band that would be meaningless if it stayed
    fixed at $100 while the target shrank to a quarter."""
    sizing = size_order(1.25, 250.0, BAND, max_contracts=40)

    assert (sizing.low, sizing.high) == (225.0, 275.0)


# ---------------------------------------------------------------- the scale-out split
@pytest.mark.parametrize(
    ("held", "sold"),
    [(8, 4), (5, 3), (3, 2), (2, 1), (17, 9)],
)
def test_banking_half_rounds_toward_securing_the_cost(held, sold):
    """An odd position cannot split evenly. Under-banking fails the purpose of
    the scale-out; over-banking just leaves a smaller runner."""
    assert split_for_scale_out(held, 0.5) == sold


def test_a_scale_out_always_leaves_a_runner():
    for held in range(2, 40):
        assert split_for_scale_out(held, 0.5) < held


def test_a_single_contract_cannot_be_split():
    assert split_for_scale_out(1, 0.5) == 0
    assert split_for_scale_out(0, 0.5) == 0


def test_the_description_names_the_arithmetic():
    assert "8 عقدًا" in size_order(1.25, BUDGET, BAND, max_contracts=40).describe()
