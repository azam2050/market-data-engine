"""Tests for the data layer: session splitting, resampling, quality checks.

These guard the failures that silently corrupt a trading system: extended-hours
prints leaking into the regular session, timeframes that disagree with each
other, and gaps nobody noticed.
"""

from datetime import date, datetime, timedelta

import pytest

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.data.quality import dedupe, fill_gaps, inspect_session
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Bar
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.features.timeframes import (
    TimeframeSet,
    regular_session,
    resample,
    split_session,
)

DAY = date(2026, 3, 2)


def _bar(hour: int, minute: int, close: float = 480.0, volume: int = 1000) -> Bar:
    base = datetime(DAY.year, DAY.month, DAY.day, hour, 0, tzinfo=MARKET_TZ)
    return Bar(
        symbol="QQQ",
        ts=base + timedelta(minutes=minute),
        open=close,
        high=close + 0.1,
        low=close - 0.1,
        close=close,
        volume=volume,
        vwap=close,
    )


# ---------------------------------------------------------------- sessions
def test_split_session_isolates_regular_hours():
    bars = [_bar(7, 0), _bar(9, 29), _bar(9, 30), _bar(12, 0), _bar(15, 59), _bar(17, 0)]
    split = split_session(bars)

    assert len(split.premarket) == 2
    assert len(split.regular) == 3
    assert len(split.afterhours) == 1
    for bar in split.regular:
        local = bar.ts.astimezone(MARKET_TZ).time()
        assert local.hour * 60 + local.minute >= 9 * 60 + 30


def test_premarket_levels_are_preserved_not_discarded():
    """Pre-market high/low are among the most-watched levels of the day."""
    bars = [_bar(7, 0, close=482.0), _bar(8, 0, close=478.0), _bar(10, 0, close=480.0)]
    split = split_session(bars)
    assert split.premarket_high == 482.1
    assert split.premarket_low == 477.9


def test_premarket_never_leaks_into_session_vwap():
    """The bug this whole module exists to prevent."""
    premarket = [_bar(6, 0, close=400.0, volume=999_999)]
    session = [_bar(10, 0, close=480.0), _bar(10, 1, close=481.0)]
    cleaned = regular_session(premarket + session)

    assert len(cleaned) == 2
    snap = SnapshotBuilder("QQQ").build(cleaned)
    vwap = snap.indicators["vwap"]
    assert vwap is not None and vwap > 479  # would collapse toward 400 if leaked


# ---------------------------------------------------------------- resampling
def test_resample_is_exact_arithmetic():
    bars = [_bar(9, 30 + i, close=480.0 + i) for i in range(5)]
    bars[2] = bars[2].model_copy(update={"high": 500.0})
    bars[3] = bars[3].model_copy(update={"low": 400.0})

    five = resample(bars, 5)
    assert len(five) == 1
    candle = five[0]
    assert candle.open == bars[0].open
    assert candle.close == bars[-1].close
    assert candle.high == 500.0
    assert candle.low == 400.0
    assert candle.volume == sum(b.volume for b in bars)


def test_resample_anchors_to_the_open():
    bars = [_bar(9, 30 + i) for i in range(12)]
    five = resample(bars, 5)
    starts = [b.ts.astimezone(MARKET_TZ).strftime("%H:%M") for b in five]
    assert starts == ["09:30", "09:35", "09:40"]


def test_resample_keeps_partial_final_bucket():
    """A forming 5m bar is real information; hiding it would delay decisions."""
    bars = [_bar(9, 30 + i) for i in range(7)]
    five = resample(bars, 5)
    assert len(five) == 2
    assert five[-1].volume == 2 * 1000


def test_timeframes_are_mutually_consistent():
    bars = synthetic_session("QQQ", DAY, seed=21)
    tfs = TimeframeSet.build(bars)

    assert sum(b.volume for b in tfs.m5) == sum(b.volume for b in bars)
    assert sum(b.volume for b in tfs.m15) == sum(b.volume for b in bars)
    assert max(b.high for b in tfs.m15) == max(b.high for b in bars)
    assert min(b.low for b in tfs.m5) == min(b.low for b in bars)
    assert tfs.m5[-1].close == bars[-1].close


# ---------------------------------------------------------------- quality
def test_clean_session_is_pristine():
    bars = synthetic_session("QQQ", DAY, seed=9)
    quality = inspect_session(bars)
    assert quality.is_usable
    assert quality.is_pristine, quality.issues
    assert quality.completeness == 1.0


def test_gaps_are_detected():
    bars = [_bar(9, 30), _bar(9, 31), _bar(9, 40), _bar(9, 41)]
    quality = inspect_session(bars, expected_minutes=12)
    assert quality.gaps
    assert any("missing minutes" in issue for issue in quality.issues)


def test_frozen_feed_is_flagged_as_unusable():
    bars = [_bar(9, 30 + i, close=480.0) for i in range(40)]
    quality = inspect_session(bars)
    assert quality.longest_frozen_run >= 15
    assert not quality.is_usable
    assert any("frozen" in issue for issue in quality.issues)


def test_dedupe_keeps_the_latest_revision():
    first = _bar(9, 30, close=480.0)
    revised = _bar(9, 30, close=481.0)
    result = dedupe([first, revised])
    assert len(result) == 1
    assert result[0].close == 481.0


def test_fill_gaps_bridges_short_holes_only():
    bars = [_bar(9, 30), _bar(9, 33), _bar(9, 50)]
    filled = fill_gaps(bars, max_fill=3)

    stamps = [b.ts.astimezone(MARKET_TZ).strftime("%H:%M") for b in filled]
    assert stamps == ["09:30", "09:31", "09:32", "09:33", "09:50"]
    # synthetic fills must never fake conviction
    assert all(b.volume == 0 for b in filled if b.ts.minute in (31, 32))


def test_unusable_data_blocks_at_the_rails():
    from qqq_alpha.brain.rails import DayState, SafetyRails
    from qqq_alpha.config import Settings

    bars = synthetic_session("QQQ", DAY, seed=4)
    snap = SnapshotBuilder("QQQ").build(bars[:120])
    snap.data_age_sec = 5
    snap.data_usable = False
    snap.data_quality = "price frozen for 40 consecutive bars"

    verdict = SafetyRails(Settings()).pre_check(snap, DayState())
    assert not verdict.allowed
    assert any(b.startswith("unusable_data") for b in verdict.blocks)


# ---------------------------------------------------------------- snapshot
def test_snapshot_exposes_all_three_timeframes():
    bars = synthetic_session("QQQ", DAY, seed=31)
    # EMA9 on the 15m frame needs 9 completed 15m bars = 135 minutes of session
    snap = SnapshotBuilder("QQQ").build(bars[:200])

    assert set(snap.timeframes) == {"1m", "5m", "15m"}
    assert snap.timeframes["5m"].get("ema9") is not None
    assert snap.timeframes["15m"].get("ema9") is not None
    assert any(o.name == "timeframe_alignment" for o in snap.observations)


def test_alignment_confidence_drops_when_timeframes_disagree():
    bars = synthetic_session("QQQ", DAY, seed=77)
    snap = SnapshotBuilder("QQQ").build(bars[:150])
    alignment = next(o for o in snap.observations if o.name == "timeframe_alignment")

    if "disagree" in alignment.note:
        assert alignment.confidence < 0.5
        assert alignment.score == 0.0
    else:
        assert alignment.confidence == 1.0


def test_transactions_survive_resampling():
    bars = [_bar(9, 30 + i).model_copy(update={"transactions": 10}) for i in range(5)]
    five = resample(bars, 5)
    assert five[0].transactions == 50


def _unused(_: timedelta) -> None:  # pragma: no cover
    return None


# ------------------------------------------------------- reference levels
def test_daily_bar_rolls_the_session_into_one_candle():
    from datetime import date

    from qqq_alpha.data.massive import TradingSession
    from qqq_alpha.data.synthetic import synthetic_session

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=7)
    session = TradingSession(symbol="QQQ", day=date(2026, 3, 2), regular=bars)
    daily = session.daily_bar

    assert daily is not None
    assert daily.open == bars[0].open
    assert daily.close == bars[-1].close
    assert daily.high == max(b.high for b in bars)
    assert daily.low == min(b.low for b in bars)
    assert daily.volume == sum(b.volume for b in bars)
    assert TradingSession(symbol="QQQ", day=date(2026, 3, 2)).daily_bar is None


@pytest.mark.asyncio
async def test_live_engine_is_no_longer_blind_to_yesterday(tmp_path):
    """The backtester always passed prior_day; the live engine never did, so
    live ran without yesterday's high/low/close — and therefore without the
    classic pivot, R1 and S1 — while its own backtest had them."""
    from datetime import date, timedelta

    from qqq_alpha.brain.decider import HeuristicDecider
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.config import Settings
    from qqq_alpha.data.massive import TradingSession
    from qqq_alpha.data.pricing import BlackScholesPricer
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.journal import Journal
    from qqq_alpha.live.engine import LiveEngine
    from qqq_alpha.live.notifier import NullNotifier

    today = date(2026, 3, 3)  # a Tuesday
    settings = Settings(
        massive_api_key="k", journal_dir=tmp_path / "j", data_dir=tmp_path / "d",
        shadow_symbols_csv="",
    )
    engine = LiveEngine(
        settings=settings, decider=HeuristicDecider(settings),
        pricer=BlackScholesPricer(), playbook=Playbook(),
        journal=Journal(tmp_path / "j", session_tag="test"), notifier=NullNotifier(),
    )

    class _Client:
        async def session(self, symbol, day):
            return TradingSession(
                symbol=symbol, day=day,
                regular=synthetic_session("QQQ", day, seed=day.day),
            )

    await engine._load_prior_day(_Client(), today)
    assert engine.prior_day is not None
    yesterday = synthetic_session("QQQ", today - timedelta(days=1), seed=2)
    assert engine.prior_day.high == max(b.high for b in yesterday)

    engine.overnight_high, engine.overnight_low = 512.40, 508.10
    engine.session_bars = synthetic_session("QQQ", today, seed=3)
    snapshot = engine.builder.build(
        session_bars=engine.session_bars,
        prior_day=engine.prior_day,
        overnight_high=engine.overnight_high,
        overnight_low=engine.overnight_low,
    )
    for level in ("prior_high", "prior_low", "prior_close", "pivot", "r1", "s1"):
        assert snapshot.levels.get(level) is not None, level
    assert snapshot.levels["overnight_high"] == 512.40


@pytest.mark.asyncio
async def test_a_holiday_is_walked_past_when_looking_for_yesterday(tmp_path):
    from datetime import date

    from qqq_alpha.brain.decider import HeuristicDecider
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.config import Settings
    from qqq_alpha.data.massive import TradingSession
    from qqq_alpha.data.pricing import BlackScholesPricer
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.journal import Journal
    from qqq_alpha.live.engine import LiveEngine
    from qqq_alpha.live.notifier import NullNotifier

    settings = Settings(
        massive_api_key="k", journal_dir=tmp_path / "j", data_dir=tmp_path / "d",
        shadow_symbols_csv="",
    )
    engine = LiveEngine(
        settings=settings, decider=HeuristicDecider(settings),
        pricer=BlackScholesPricer(), playbook=Playbook(),
        journal=Journal(tmp_path / "j", session_tag="test"), notifier=NullNotifier(),
    )
    asked: list[date] = []

    class _HolidayClient:
        async def session(self, symbol, day):
            asked.append(day)
            # Thursday was a holiday: no bars at all
            if day == date(2026, 3, 5):
                return TradingSession(symbol=symbol, day=day)
            return TradingSession(
                symbol=symbol, day=day, regular=synthetic_session("QQQ", day, seed=5)
            )

    await engine._load_prior_day(_HolidayClient(), date(2026, 3, 6))  # Friday
    assert asked == [date(2026, 3, 5), date(2026, 3, 4)]  # skipped the empty day
    assert engine.prior_day is not None


def test_resample_refuses_a_partial_trade_count():
    """A partial sum of trade counts would sit beside a complete volume total,
    and the average-trade-size the brain reads would be inflated by exactly the
    fraction of bars that were missing their count."""
    start = datetime(2026, 3, 2, 14, 30, tzinfo=MARKET_TZ)
    bars = [
        Bar(symbol="QQQ", ts=start + timedelta(minutes=i), open=100, high=101,
            low=99, close=100, volume=1000, transactions=(10 if i < 3 else None))
        for i in range(5)
    ]
    rolled = resample(bars, 5)
    assert len(rolled) == 1
    assert rolled[0].volume == 5000
    assert rolled[0].transactions is None  # not 30

    complete = [b.model_copy(update={"transactions": 10}) for b in bars]
    assert resample(complete, 5)[0].transactions == 50
