from datetime import date

import pytest

from qqq_alpha.config import MARKET_TZ
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


# ------------------------------------------------------- hourly and gaps
def _multi_day():
    from datetime import timedelta

    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.domain import Bar

    today = date(2026, 3, 6)  # Friday
    days = [today - timedelta(days=d) for d in (7, 6, 5, 4, 1)]
    history = [b for d in days for b in synthetic_session("QQQ", d, seed=d.day)]
    prior = synthetic_session("QQQ", today - timedelta(days=1), seed=5)
    prior_day = Bar(
        symbol="QQQ", ts=prior[-1].ts, open=prior[0].open,
        high=max(b.high for b in prior), low=min(b.low for b in prior),
        close=prior[-1].close, volume=sum(b.volume for b in prior),
    )
    return today, history, prior_day


def test_hourly_needs_several_sessions_to_be_a_chart_at_all():
    """A regular session is 390 minutes: six and a half hourly candles. Not
    enough for an EMA, a swing high, or a trend."""
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.timeframes import hourly

    one_day = hourly(synthetic_session("QQQ", date(2026, 3, 6), seed=6))
    assert len(one_day) == 7  # 09:30..15:30, the last one half-length

    _, history, _ = _multi_day()
    many = hourly(history)
    assert len(many) > 30
    # buckets are anchored per session, so days never bleed together
    assert len({b.ts.astimezone(MARKET_TZ).date() for b in many}) == 5


def test_hourly_pack_drops_the_readings_that_lie_on_this_timeframe():
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    today, history, prior_day = _multi_day()
    bars = synthetic_session("QQQ", today, seed=12)
    snapshot = SnapshotBuilder("QQQ").build(
        session_bars=bars[:150], history_bars=history, prior_day=prior_day,
        now=bars[149].ts,
    )
    pack = snapshot.hourly["indicators"]

    # the newest hourly bar is half-formed, so relative volume would read as
    # "volume is dying" when only part of the hour exists
    assert "rel_volume" not in pack
    # momentum counts BARS: on this timeframe that is hours, not minutes
    assert "mom_5m" not in pack and "mom_5_hours" in pack
    assert snapshot.hourly["sessions_covered"] == 6  # five prior plus today
    assert len(snapshot.recent_bars_1h) == 10


def test_multi_session_tables_carry_the_date_or_09_30_names_two_candles():
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.brain.prompts import build_user_prompt
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    today, history, prior_day = _multi_day()
    bars = synthetic_session("QQQ", today, seed=12)
    snapshot = SnapshotBuilder("QQQ").build(
        session_bars=bars[:150], history_bars=history, prior_day=prior_day,
        now=bars[149].ts,
    )
    prompt = build_user_prompt(snapshot, Playbook())

    hourly_block = prompt[prompt.index("HOURLY (10 candles)") : prompt.index("=== OPENING GAP")]
    assert "03-06 09:30" in hourly_block and "03-05 09:30" in hourly_block
    # and the swing list too — the same clock time recurs every session
    swings = snapshot.hourly["structure"]["swings"]
    assert all("-" in s["time"] for s in swings)
    # the single-session tables stay short: no date noise where it adds nothing
    minute_block = prompt[prompt.index("1-MINUTE (30 candles)") : prompt.index("5-MINUTE")]
    assert "03-06" not in minute_block


def test_opening_gap_names_the_fill_level_and_whether_it_was_reached():
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.domain import Bar
    from qqq_alpha.features.levels import opening_gap

    bars = synthetic_session("QQQ", date(2026, 3, 6), seed=12)
    session_low = min(b.low for b in bars)

    def prior_with_close(close: float) -> Bar:
        return Bar(symbol="QQQ", ts=bars[0].ts, open=close, high=close,
                   low=close, close=close, volume=1)

    # a gap up that price never traded back through
    unfilled = opening_gap(bars, prior_with_close(session_low - 5.0))
    assert unfilled["direction"] == "up"
    assert unfilled["filled"] is False
    assert unfilled["fill_level"] == round(session_low - 5.0, 2)

    # a gap up whose fill level sits inside the day's range: filled
    filled = opening_gap(bars, prior_with_close(bars[0].open - 0.5))
    assert filled["filled"] is True

    # a hair's difference is not a gap
    flat = opening_gap(bars, prior_with_close(bars[0].open * 1.0001))
    assert flat["direction"] == "none" and "fill_level" not in flat

    assert opening_gap(bars, None) is None
