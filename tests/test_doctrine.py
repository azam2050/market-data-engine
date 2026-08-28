"""The entry doctrine born of 2026-08-26: a range day traded like a trend day.

Two losses in one session, opposite directions, same root cause — a CALL
bought on a textbook breakout that printed at the session high, and a PUT
forced four wakes later by counting the wakes, sold straight into a
week-tested floor. Plus five afternoon setups refused because the budget was
already spent. These tests pin the doctrine that answers each of those.

They are deliberately assertions on the PROMPT and the PLAYBOOK, not on model
behaviour: the doctrine is an input we control and can regression-test; the
model's compliance shows up in the decision journal.
"""

from __future__ import annotations

from pathlib import Path

from qqq_alpha.brain.playbook import load_playbook
from qqq_alpha.brain.prompts import SYSTEM_PROMPT

PLAYBOOK_PATH = Path(__file__).parent.parent / "src" / "qqq_alpha" / "brain" / "playbook.yaml"


# ------------------------------------------------------- range vs trend days
def test_the_brain_is_told_range_and_trend_days_are_opposite_games():
    assert "RANGE DAYS AND TREND DAYS ARE OPPOSITE GAMES" in SYSTEM_PROMPT


def test_range_day_doctrine_names_the_edges_as_the_trade_and_middle_as_trap():
    assert "the edges are the trade and the middle is the trap" in SYSTEM_PROMPT
    # a mid-range "confirmed breakout" is the end of the swing, not the start —
    # the exact shape of the 2026-08-26 CALL loss
    assert "IS the exhaustion" in SYSTEM_PROMPT


def test_range_day_entries_demand_the_rejection_candle_not_the_level_alone():
    """Fading a level just because it exists is how the PUT died on the floor."""
    assert "the candle, not the level alone" in SYSTEM_PROMPT


def test_trend_days_keep_the_breakout_doctrine():
    """The counter-example that must survive: 2026-08-24 rode a breakout for
    +63.8%. Edge-fading must be scoped to range days only."""
    assert "fading edges is how accounts die" in SYSTEM_PROMPT


def test_a_real_breakout_must_break_the_weeks_edge_not_an_intraday_shelf():
    assert "break the WEEK's edge" in SYSTEM_PROMPT


# ------------------------------------------------- the failed-breakout flip
def test_a_failed_breakout_stop_converts_into_an_opposite_trigger():
    assert "ready-made opposite trigger" in SYSTEM_PROMPT


def test_executing_a_pre_declared_trigger_is_never_revenge():
    """The five minutes of 'is this revenge?' hesitation after 709.79 broke
    cost 1.4 points and turned a winning flip into a loss."""
    assert "Executing a trigger you declared BEFORE the loss is never revenge" in SYSTEM_PROMPT
    assert "five minutes and 1.4 points late" in SYSTEM_PROMPT


# ------------------------------------------------- wake-counting is not evidence
def test_wake_count_is_banned_as_entry_evidence():
    assert "THE NUMBER OF WAKES YOU HAVE WAITED IS NOT EVIDENCE" in SYSTEM_PROMPT


def test_the_deletion_test_is_spelled_out():
    """The operational form of the ban: a justification that dies when the
    wake-count sentence is deleted was never a justification."""
    assert "delete the sentence" in SYSTEM_PROMPT
    assert "delete the trade" in SYSTEM_PROMPT


def test_the_playbook_counter_rule_carries_its_own_gate():
    """The counter-rule that forced the 2026-08-26 PUT now names its
    precondition as a gate: no aligned trend, no counter-rule — however many
    wakes have passed."""
    playbook = load_playbook(PLAYBOOK_PATH)
    exhausted = next(c for c in playbook.caution if c["id"] == "EXHAUSTED_EXTREME_ENTRY")
    assert "COUNTER-RULE'S OWN GATE" in exhausted["note"]
    assert "the wake-count is not evidence" in exhausted["note"]
    # and the gate travels into the prompt the brain actually reads
    assert "COUNTER-RULE'S OWN GATE" in playbook.as_prompt_block()


def test_the_trade_cap_still_defaults_to_three():
    """The playbook says the cap was widened so a genuine third setup is not
    turned away by arithmetic; the code default must agree. (The live value
    is the MAX_TRADES_PER_DAY env var — this guards the default.)"""
    from qqq_alpha.config import Settings

    assert Settings.model_fields["max_trades_per_day"].default == 3


# ================================================================ the rebuild
# 2026-08-28: after a week where every loss was a B-grade improvisation and
# the one win was a list setup, the operator asked for a trader, not a
# rule-checker. The rebuild has three parts, each pinned here.


def test_the_first_act_of_every_wake_is_a_free_chart_read():
    assert "READ THE DAY BEFORE THE RULES" in SYSTEM_PROMPT
    # the three questions that define the read
    assert "What kind of day is this?" in SYSTEM_PROMPT
    assert "Where are we inside the week?" in SYSTEM_PROMPT
    assert "Who is driving?" in SYSTEM_PROMPT


def test_rule_citing_is_demoted_from_analysis_to_guardrail():
    assert "cite a caution only when it actually changes what you do" in SYSTEM_PROMPT
    assert "mostly rule names strung together has failed" in SYSTEM_PROMPT


def test_the_chart_outranks_the_labels():
    """The Aug-27 failure in one sentence: regime said RANGING while an
    unfilled gap was holding, and the label won. Now the chart wins."""
    assert "trade the chart, not the label" in SYSTEM_PROMPT
    assert "OVERRIDES the computed regime label" in SYSTEM_PROMPT


def test_entries_must_name_a_setup_from_the_hunting_list():
    assert "ENTRIES COME FROM THE HUNTING LIST" in SYSTEM_PROMPT
    assert "must NAME the setup it matches" in SYSTEM_PROMPT
    # off-list trades are an explicit exception, not a habit
    assert "off-list entry is allowed only as an explicit exception" in SYSTEM_PROMPT


def test_the_operators_setup_is_on_the_hunting_list():
    """A floor that held twice, a bounce with volume, a retest that holds —
    the operator named it, and 2026-08-27's 715.00 double bottom proved it."""
    playbook = load_playbook(PLAYBOOK_PATH)
    ids = [s["id"] for s in playbook.setups]
    assert "TESTED_FLOOR_RECLAIM" in ids
    reclaim = next(s for s in playbook.setups if s["id"] == "TESTED_FLOOR_RECLAIM")
    # the bounce alone is knife-catching; the held retest is the entry
    assert "the held retest is" in reclaim["description"]


def test_gap_and_go_is_on_the_hunting_list_and_gap_fading_is_cautioned():
    playbook = load_playbook(PLAYBOOK_PATH)
    setup_ids = [s["id"] for s in playbook.setups]
    caution_ids = [c["id"] for c in playbook.caution]
    assert "GAP_AND_GO" in setup_ids
    assert "UNFILLED_GAP_FADE" in caution_ids
    fade = next(c for c in playbook.caution if c["id"] == "UNFILLED_GAP_FADE")
    # the caution carries the paired 2026-08-27 loss as evidence
    assert "2026-08-27" in fade["note"]
    # and both travel into the prompt the brain actually reads
    block = load_playbook(PLAYBOOK_PATH).as_prompt_block()
    assert "GAP_AND_GO" in block and "UNFILLED_GAP_FADE" in block


def test_the_gap_section_of_the_live_prompt_names_the_doctrine():
    from datetime import date as _date

    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    day = _date(2026, 8, 27)
    session = synthetic_session("QQQ", day, seed=7)
    # a prior day closing well below today's open produces a gap in the snapshot
    prior = synthetic_session("QQQ", _date(2026, 8, 26), seed=8)
    prior_daily = prior[-1].model_copy(update={"close": session[0].open * 0.99})
    snap = SnapshotBuilder("QQQ").build(session[:60], prior_day=prior_daily)
    assert snap.gap.get("direction") == "up"

    prompt = build_user_prompt(snap, Playbook())
    assert "GAP_AND_GO" in prompt
    assert "UNFILLED_GAP_FADE" in prompt
