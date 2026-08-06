"""The options pulse: the "price of the day" summary shown to the brain."""

from datetime import date

from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.brain.prompts import build_user_prompt
from qqq_alpha.data.pulse import chain_pulse, nearest_weekly_expiry
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import OptionContract, OptionType
from qqq_alpha.features.snapshot import SnapshotBuilder


def _contract(option_type, strike, volume, open_interest=0):
    return OptionContract(
        occ_symbol=f"O:QQQ260807{'C' if option_type is OptionType.CALL else 'P'}{int(strike * 1000):08d}",
        underlying="QQQ",
        option_type=option_type,
        strike=strike,
        expiry=date(2026, 8, 7),
        bid=1.0,
        ask=1.05,
        volume=volume,
        open_interest=open_interest,
    )


def test_pulse_finds_the_days_heaviest_strikes():
    contracts = [
        _contract(OptionType.CALL, 710, volume=9000, open_interest=500),
        _contract(OptionType.CALL, 712, volume=3000, open_interest=12000),
        _contract(OptionType.PUT, 705, volume=2000, open_interest=800),
        _contract(OptionType.PUT, 700, volume=1000, open_interest=15000),
    ]
    pulse = chain_pulse("QQQ", contracts)
    assert pulse is not None
    assert pulse["call_volume"] == 12000
    assert pulse["put_volume"] == 3000
    assert pulse["lean"] == "CALL"
    # the "price of the day" — the strike attracting the heaviest call money
    assert pulse["top_call_strikes"][0]["strike"] == 710
    # OI walls come from open interest, not volume
    assert pulse["call_oi_wall"]["strike"] == 712
    assert pulse["put_oi_wall"]["strike"] == 700


def test_pulse_leans_put_when_puts_dominate():
    contracts = [
        _contract(OptionType.CALL, 710, volume=1000),
        _contract(OptionType.PUT, 705, volume=5000),
    ]
    pulse = chain_pulse("QQQ", contracts)
    assert pulse["lean"] == "PUT"
    assert pulse["put_call_ratio"] == 5.0


def test_pulse_is_balanced_in_the_middle():
    contracts = [
        _contract(OptionType.CALL, 710, volume=1000),
        _contract(OptionType.PUT, 705, volume=1000),
    ]
    assert chain_pulse("QQQ", contracts)["lean"] == "BALANCED"


def test_pulse_returns_none_for_a_dead_chain():
    """Zero volume must read as "no data", never as "balanced"."""
    contracts = [_contract(OptionType.CALL, 710, volume=0)]
    assert chain_pulse("QQQ", contracts) is None
    assert chain_pulse("QQQ", []) is None


def test_nearest_weekly_expiry_lands_on_friday():
    assert nearest_weekly_expiry(date(2026, 8, 3)) == date(2026, 8, 7)  # Monday
    assert nearest_weekly_expiry(date(2026, 8, 7)) == date(2026, 8, 7)  # Friday itself
    assert nearest_weekly_expiry(date(2026, 8, 8)) == date(2026, 8, 14)  # Saturday


def test_prompt_carries_the_pulse_section():
    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=11)
    snapshot = SnapshotBuilder("QQQ").build(bars[:120])
    pulse_rows = [
        chain_pulse(
            "NVDA",
            [
                _contract(OptionType.CALL, 710, volume=8000),
                _contract(OptionType.PUT, 700, volume=1000),
            ],
        )
    ]
    prompt = build_user_prompt(snapshot, Playbook(), options_pulse=pulse_rows)
    assert "OPTIONS PULSE" in prompt
    assert "NVDA" in prompt
    assert "voting for a bottom" in prompt


def test_prompt_omits_the_section_without_pulse_data():
    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=11)
    snapshot = SnapshotBuilder("QQQ").build(bars[:120])
    prompt = build_user_prompt(snapshot, Playbook(), options_pulse=None)
    assert "OPTIONS PULSE" not in prompt
