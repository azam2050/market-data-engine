"""The week behind today.

Every other level in the system describes today and yesterday. That window is
where a 0DTE position lives, but it is not where the levels that decide the
position are set: a range whose edges were drawn three sessions ago still
governs whether today's break runs or fades, and a trade opened in the middle
of that range is a coin flip however clean the one-minute tape looks.

The live symptom that produced these tests: a week where QQQ oscillated between
roughly 707.8 and 714, and a session spent entirely inside the middle of it —
invisible to an engine whose furthest memory was yesterday's high.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import Bar
from qqq_alpha.features import levels

SESSION_HOURS = 7  # 09:30-16:00 rounds to seven hourly candles


def _bar(index: int, high: float, low: float, close: float | None = None) -> Bar:
    """One hourly candle. Bars wrap into the next session every seven, the way
    an hourly chart actually runs — so any fixture longer than a day spans
    real sessions rather than piling twenty candles onto one afternoon."""
    day, hour = divmod(index, SESSION_HOURS)
    ts = datetime(2026, 8, 20, 9, 0, tzinfo=MARKET_TZ) + timedelta(
        days=day, hours=hour
    )
    return Bar(
        symbol="QQQ",
        ts=ts,
        open=(high + low) / 2,
        high=high,
        low=low,
        close=close if close is not None else (high + low) / 2,
        volume=1_000_000,
    )


def _week_in_a_range() -> list[Bar]:
    """Five oscillations between a floor near 707.8 and a ceiling near 714.

    Deliberately not identical prices: real levels are touched within a
    tolerance, and a clusterer that only merges exact matches would find
    nothing on a real chart.
    """
    ceiling = [714.02, 713.95, 714.08, 713.90]
    floor = [707.78, 707.85, 707.72, 707.90]
    bars: list[Bar] = []
    index = 0
    for top, bottom in zip(ceiling, floor, strict=True):
        # up into the ceiling, then down into the floor, with filler between so
        # the fractal pivot has neighbours to be a pivot against
        bars.append(_bar(index, top - 3.0, bottom + 1.0))
        bars.append(_bar(index + 1, top, top - 2.0))  # the touch of the ceiling
        bars.append(_bar(index + 2, top - 2.5, bottom + 2.0))
        bars.append(_bar(index + 3, bottom + 2.0, bottom))  # the touch of the floor
        index += 4
    return bars


# ------------------------------------------------------------- the extremes
def test_the_week_extremes_and_where_price_sits_inside_them():
    bars = _week_in_a_range()
    multi = levels.multi_day_levels(bars, price=711.0)

    assert multi["high"] == 714.08
    assert multi["low"] == 707.72
    # 711 is close to the middle of a 707.7-714.1 range
    assert 45.0 < multi["range_position_pct"] < 55.0
    assert multi["sessions"] >= 1


def test_range_position_reads_0_at_the_floor_and_100_at_the_ceiling():
    bars = _week_in_a_range()
    assert levels.multi_day_levels(bars, price=707.72)["range_position_pct"] == 0.0
    assert levels.multi_day_levels(bars, price=714.08)["range_position_pct"] == 100.0


def test_range_width_is_reported_as_a_percentage_of_price():
    """A compressed week is the reason a normal target cannot be reached."""
    multi = levels.multi_day_levels(_week_in_a_range(), price=711.0)
    # 714.08 - 707.72 is about 6.4 points on 711 — a hair under 0.9%
    assert 0.8 < multi["range_width_pct"] < 1.0


# ------------------------------------------------------- the repeated levels
def test_a_level_touched_across_the_week_is_found_and_counted():
    multi = levels.multi_day_levels(_week_in_a_range(), price=711.0)
    repeated = multi["repeated"]
    assert repeated, "a week spent between two levels must surface them"

    prices = [level["price"] for level in repeated]
    assert any(713.5 < p < 714.5 for p in prices), prices
    assert any(707.3 < p < 708.3 for p in prices), prices
    # each edge was visited on four separate swings, not one
    assert max(level["touches"] for level in repeated) >= 3


def test_nearby_swings_collapse_into_one_level_rather_than_several():
    """714.02, 713.95, 714.08 and 713.90 are one level a trader would draw."""
    multi = levels.multi_day_levels(_week_in_a_range(), price=711.0)
    near_ceiling = [lv for lv in multi["repeated"] if 713.0 < lv["price"] < 715.0]
    assert len(near_ceiling) == 1, near_ceiling


def test_a_single_touch_is_a_high_not_a_level():
    """One spike does not make a level; the clusterer must not name it."""
    bars = _week_in_a_range()
    bars.append(_bar(99, 730.0, 728.0))  # a lone spike far above everything
    multi = levels.multi_day_levels(bars, price=711.0)
    assert not [lv for lv in multi["repeated"] if lv["price"] > 720.0]


def test_a_level_tested_from_both_sides_is_marked_as_both():
    """Resistance that later held as support is the strongest kind there is.

    710 is rejected from below early in the week (a swing HIGH), and later
    holds as a floor (a swing LOW). One price, two roles.
    """
    shape = [
        (706.0, 705.0),
        (708.0, 706.0),
        (710.0, 708.0),  # swing high — rejected at 710
        (708.0, 706.0),
        (706.0, 704.0),  # swing low
        (708.0, 705.0),
        (712.0, 708.0),
        (715.0, 712.0),  # swing high
        (713.0, 710.0),
        (712.0, 709.95),  # swing low — 710 now holding as support
        (714.0, 711.0),
        (716.0, 713.0),
        (717.0, 714.0),
        (718.0, 715.0),
    ]
    bars = [_bar(i, high, low) for i, (high, low) in enumerate(shape)]
    multi = levels.multi_day_levels(bars, price=711.0)
    around_710 = [lv for lv in multi["repeated"] if 709.0 < lv["price"] < 711.0]
    assert around_710, multi["repeated"]
    assert around_710[0]["kind"] == "both"


def test_repeated_levels_are_ordered_by_conviction_then_proximity():
    multi = levels.multi_day_levels(_week_in_a_range(), price=711.0)
    touches = [level["touches"] for level in multi["repeated"]]
    assert touches == sorted(touches, reverse=True)


def test_distance_is_signed_toward_the_level():
    multi = levels.multi_day_levels(_week_in_a_range(), price=711.0)
    for level in multi["repeated"]:
        if level["price"] > 711.0:
            assert level["distance_pct"] > 0
        else:
            assert level["distance_pct"] < 0


# --------------------------------------------------------------- edge cases
def test_no_history_yields_nothing_rather_than_a_guess():
    assert levels.multi_day_levels([], price=711.0) == {}
    assert levels.multi_day_levels(_week_in_a_range(), price=0.0) == {}


def test_one_session_is_not_a_week():
    """Today's own high and low relabelled as "the range of the week" would be
    the same two numbers the model already has, wearing a name that claims far
    more authority than they earned."""
    one_day = [_bar(i, 712.0 + i * 0.1, 708.0 - i * 0.1) for i in range(SESSION_HOURS)]
    assert len({b.ts.date() for b in one_day}) == 1
    assert levels.multi_day_levels(one_day, price=710.0) == {}


def test_a_flat_window_does_not_divide_by_zero():
    flat = [_bar(i, 710.0, 710.0) for i in range(21)]
    multi = levels.multi_day_levels(flat, price=710.0)
    assert multi["high"] == multi["low"] == 710.0
    assert "range_position_pct" not in multi  # undefined, not a fabricated 50


# ------------------------------------------------------------- the merge
def test_the_week_folds_into_the_session_levels_so_proximity_sees_it():
    """The point of merging: `nearest_levels` starts counting the week without
    knowing the week exists."""
    session = {"session_high": 712.0, "session_low": 708.0}
    multi = levels.multi_day_levels(_week_in_a_range(), price=711.0)
    merged = levels.merge_multi_day(session, multi)

    assert merged["session_high"] == 712.0  # nothing lost
    assert merged["week_high"] == 714.08
    assert merged["week_low"] == 707.72
    assert any(name.startswith("tested_") for name in merged)

    nearby = levels.nearest_levels(711.0, merged)
    names = [name for name, _, _ in nearby["resistance"] + nearby["support"]]
    assert any(n.startswith("week_") or n.startswith("tested_") for n in names), names


def test_merging_nothing_leaves_the_session_levels_untouched():
    session = {"session_high": 712.0}
    assert levels.merge_multi_day(session, {}) == session


# ----------------------------------------------------------- reaching the brain
def test_the_week_reaches_the_brains_prompt_with_its_location_guidance():
    """A level the engine computes but never shows the model is not a fix."""
    from datetime import date

    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    day = date(2026, 8, 27)
    session = synthetic_session("QQQ", day, seed=7)
    history = synthetic_session("QQQ", date(2026, 8, 26), seed=8) + synthetic_session(
        "QQQ", date(2026, 8, 25), seed=9
    )

    snap = SnapshotBuilder("QQQ").build(session[:120], history_bars=history)
    assert snap.multiday, "several sessions of history must produce a week view"

    prompt = build_user_prompt(snap, Playbook())
    assert "THE WEEK BEHIND TODAY" in prompt
    # the guidance that makes the numbers actionable — location, not signal
    assert "range_position_pct" in prompt
    assert "middle" in prompt


def test_a_first_day_with_no_history_simply_omits_the_week():
    """No history is silence, not a one-session 'week' presented as a range."""
    from datetime import date

    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    session = synthetic_session("QQQ", date(2026, 8, 27), seed=7)
    snap = SnapshotBuilder("QQQ").build(session[:120])
    prompt = build_user_prompt(snap, Playbook())
    assert "THE WEEK BEHIND TODAY" not in prompt
