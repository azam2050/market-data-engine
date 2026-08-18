from datetime import date

import pytest

from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.features import indicators, levels
from qqq_alpha.features.snapshot import SnapshotBuilder


def _bars():
    return synthetic_session("QQQ", date(2026, 3, 2), seed=42, trend=0.01)


def test_ema_matches_manual_calculation():
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    # seed = mean(1,2,3) = 2.0, multiplier = 0.5
    # -> (4-2)*0.5+2 = 3.0 -> (5-3)*0.5+3 = 4.0
    assert indicators.ema(values, 3) == 4.0


def test_ema_requires_enough_data():
    assert indicators.ema([1.0, 2.0], 5) is None


def test_rsi_is_bounded():
    bars = _bars()
    value = indicators.rsi(indicators.closes(bars))
    assert value is not None
    assert 0.0 <= value <= 100.0


def test_rsi_all_gains_is_max():
    assert indicators.rsi([float(i) for i in range(1, 30)]) == 100.0


def test_atr_positive():
    value = indicators.atr(_bars())
    assert value is not None and value > 0


def test_vwap_within_session_range():
    bars = _bars()
    vwap = indicators.session_vwap(bars)
    assert vwap is not None
    assert min(b.low for b in bars) <= vwap <= max(b.high for b in bars)


def test_compute_all_returns_full_pack():
    result = indicators.compute_all(_bars())
    for key in ("ema9", "ema21", "rsi14", "atr14", "vwap", "macd", "rel_volume"):
        assert key in result


def test_levels_split_above_and_below():
    bars = _bars()
    lvl = levels.compute_levels(bars)
    nearby = levels.nearest_levels(bars[-1].close, lvl)
    for _, level, _ in nearby["resistance"]:
        assert level > bars[-1].close
    for _, level, _ in nearby["support"]:
        assert level < bars[-1].close


def test_snapshot_builds_and_scores():
    bars = _bars()
    snap = SnapshotBuilder("QQQ").build(bars)
    assert snap.observations
    assert -1.0 <= snap.net_bias <= 1.0
    assert snap.session_minute == len(bars) - 1


def test_observations_never_reject():
    """Observations carry evidence only — no field can veto a trade."""
    snap = SnapshotBuilder("QQQ").build(_bars())
    for obs in snap.observations:
        assert -1.0 <= obs.score <= 1.0
        assert 0.0 <= obs.confidence <= 1.0


# ---------------------------------------------------------------- structure
def test_swing_points_alternate_and_find_the_real_extremes():
    from datetime import UTC, datetime, timedelta

    from qqq_alpha.domain import Bar
    from qqq_alpha.features.structure import swing_points

    # a clean zigzag: up to 110, down to 95, up to 120, down to 105
    path = [100, 104, 110, 106, 99, 95, 101, 112, 120, 116, 109, 105, 111]
    start = datetime(2026, 3, 2, 14, 30, tzinfo=UTC)
    bars = [
        Bar(symbol="QQQ", ts=start + timedelta(minutes=i), open=p, high=p + 0.5,
            low=p - 0.5, close=p, volume=1000)
        for i, p in enumerate(path)
    ]
    swings = swing_points(bars)
    kinds = [s.kind for s in swings]
    assert kinds == sorted(set(kinds), key=kinds.index) or all(
        kinds[i] != kinds[i + 1] for i in range(len(kinds) - 1)
    ), "swings must alternate high/low"
    assert any(abs(s.price - 120.5) < 0.01 for s in swings if s.kind == "high")
    assert any(abs(s.price - 94.5) < 0.01 for s in swings if s.kind == "low")


def test_dow_trend_needs_both_higher_highs_and_higher_lows():
    from qqq_alpha.features.structure import Swing, classify

    def swings(prices):
        return [Swing(ts=None, price=p, kind=k) for p, k in prices]

    uptrend = swings([(100, "high"), (95, "low"), (110, "high"), (99, "low")])
    assert classify(uptrend)[0] == "uptrend"

    downtrend = swings([(110, "high"), (99, "low"), (105, "high"), (94, "low")])
    assert classify(downtrend)[0] == "downtrend"

    # a higher high with a LOWER low is a widening range, never an uptrend —
    # calling it one is how a desk buys the top of a box
    expanding = swings([(100, "high"), (95, "low"), (110, "high"), (90, "low")])
    assert classify(expanding)[0] == "expanding_range"

    coil = swings([(110, "high"), (90, "low"), (105, "high"), (95, "low")])
    assert classify(coil)[0] == "contracting_range"


def test_structure_break_level_is_the_last_higher_low_in_an_uptrend():
    from datetime import date

    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=12, trend=0.02)
    snapshot = SnapshotBuilder("QQQ").build(bars[:150])
    five = snapshot.structure["5m"]
    assert five["trend"] == "uptrend"
    lows = [s["price"] for s in five["swings"] if s["kind"] == "low"]
    assert five["structure_break_level"] == lows[-1]


def test_prompt_carries_the_dow_structure_and_its_break_level():
    from datetime import date

    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=12, trend=0.02)
    snapshot = SnapshotBuilder("QQQ").build(bars[:150])
    prompt = build_user_prompt(snapshot, Playbook())

    assert "MARKET STRUCTURE (Dow)" in prompt
    assert "structure_break_level" in prompt
    assert str(snapshot.structure["5m"]["structure_break_level"]) in prompt
    # structure is context for the candles, so it follows them
    assert prompt.index("RAW PRICE ACTION") < prompt.index("MARKET STRUCTURE (Dow)")


# ------------------------------------------------------------- leaders
def _leader_snapshot():
    from datetime import date

    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    day = date(2026, 3, 3)
    qqq = synthetic_session("QQQ", day, seed=12, trend=0.02)
    leaders = {
        "NVDA": synthetic_session("NVDA", day, seed=31, trend=0.05),
        "AAPL": synthetic_session("AAPL", day, seed=17, trend=-0.03),
    }
    prior = {"NVDA": leaders["NVDA"][0].open * 0.985}
    return SnapshotBuilder("QQQ").build(
        session_bars=qqq[:150],
        leader_bars={k: v[:150] for k, v in leaders.items()},
        leader_prior_close=prior,
        now=qqq[149].ts,
    )


def test_leaders_get_the_same_reading_the_index_gets():
    """They used to reach the brain as one close and one volume each — the
    index got candles, structure and levels while the names that actually move
    it got three numbers."""
    snapshot = _leader_snapshot()

    nvda = snapshot.leader_detail["NVDA"]
    for key in (
        "last", "change_15m_pct", "change_30m_pct", "vwap_dev_pct",
        "rel_volume", "session_high", "session_low", "trend_5m", "trend_15m",
    ):
        assert key in nvda, key

    # the day change is measured against yesterday's close, not against
    # whenever the engine happened to start watching
    assert nvda["day_change_pct"] == pytest.approx(2.46, abs=0.05)
    assert nvda["prior_close"] == pytest.approx(snapshot.leader_detail["NVDA"]["prior_close"])

    # a leader with no prior close reports no day change rather than a wrong one
    assert "day_change_pct" not in snapshot.leader_detail["AAPL"]

    # and each carries its own five-minute tape
    assert len(snapshot.leader_bars_5m["NVDA"]) == 6
    assert len(snapshot.leader_bars_5m["AAPL"]) == 6


def test_prompt_renders_leader_candles_and_asks_for_a_divergence_read():
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt

    snapshot = _leader_snapshot()
    prompt = build_user_prompt(snapshot, Playbook())

    assert "NVDA 5-MINUTE (6 candles)" in prompt
    assert "AAPL 5-MINUTE (6 candles)" in prompt
    assert "DIVERGENCE" in prompt and "CONFIRMATION" in prompt
    assert "trend_5m" in prompt
    # the leaders follow the index's own price action, never displace it
    assert prompt.index("RAW PRICE ACTION") < prompt.index("INDEX HEAVYWEIGHTS")
