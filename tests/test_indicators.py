from datetime import date

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
