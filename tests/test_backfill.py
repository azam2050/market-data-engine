"""Repairing minutes the live stream dropped, and naming the cause.

The tests weight the refusals as heavily as the repairs: a gap filled at the
wrong moment, or in the wrong place in the series, is worse than the gap it
replaced.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from qqq_alpha.config import Settings
from qqq_alpha.data.backfill import (
    MAX_REPAIR_AGE_MIN,
    GapRepairer,
    RepairLog,
    merge_bars,
    missing_minutes,
)
from qqq_alpha.data.health import assess
from qqq_alpha.data.quality import inspect_session
from qqq_alpha.domain import Bar

START = datetime(2026, 8, 21, 14, 0, tzinfo=UTC)


def _bar(minute: int, close: float = 100.0) -> Bar:
    return Bar(
        symbol="QQQ",
        ts=START + timedelta(minutes=minute),
        open=close,
        high=close + 0.1,
        low=close - 0.1,
        close=close,
        volume=1000,
    )


def _settings(tmp_path) -> Settings:
    return Settings(
        massive_api_key="k", journal_dir=tmp_path / "j", data_dir=tmp_path / "d"
    )


class _FakeClient:
    """Stands in for the provider, and records what was asked of it."""

    def __init__(self, bars: list[Bar], fail: bool = False):
        self.bars = bars
        self.fail = fail
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def minute_bars(self, symbol: str, day):
        self.calls += 1
        if self.fail:
            raise RuntimeError("provider unavailable")
        return list(self.bars)


def _repairer(tmp_path, client: _FakeClient) -> GapRepairer:
    return GapRepairer(_settings(tmp_path), client_factory=lambda: client)


# ---------------------------------------------------------------- detection
def test_a_gap_is_found_by_clock_arithmetic():
    """10:00 then 10:03 means 10:01 and 10:02 are missing. That is all it is."""
    assert missing_minutes([_bar(0), _bar(3)]) == [
        START + timedelta(minutes=1),
        START + timedelta(minutes=2),
    ]


def test_a_continuous_run_has_no_gaps():
    assert missing_minutes([_bar(i) for i in range(10)]) == []


def test_a_single_bar_cannot_have_a_gap():
    assert missing_minutes([_bar(0)]) == []
    assert missing_minutes([]) == []


def test_gaps_are_found_across_an_unordered_list():
    assert len(missing_minutes([_bar(5), _bar(0), _bar(1)])) == 3


# ---------------------------------------------------------------- merging
def test_a_recovered_bar_is_slotted_into_place_not_appended():
    """Order is not cosmetic: indicators read this list as a sequence, so a
    bar on the end corrupts the calculation it was meant to repair."""
    merged = merge_bars([_bar(0), _bar(3)], [_bar(1), _bar(2)])

    assert [b.ts for b in merged] == [_bar(i).ts for i in range(4)]


def test_a_live_bar_always_beats_a_fetched_one():
    """The live bar came from the tape; the fetched one is a reconstruction."""
    live = _bar(1, close=111.0)
    merged = merge_bars([_bar(0), live, _bar(2)], [_bar(1, close=999.0)])

    assert len(merged) == 3
    assert merged[1].close == 111.0


def test_merging_nothing_changes_nothing():
    original = [_bar(0), _bar(1)]
    assert merge_bars(original, []) is original


# ---------------------------------------------------------------- repair
@pytest.mark.asyncio
async def test_a_recent_gap_is_fetched_and_filled(tmp_path):
    client = _FakeClient([_bar(1), _bar(2)])
    repairer = _repairer(tmp_path, client)
    now = START + timedelta(minutes=4)

    repaired = await repairer.repair("QQQ", [_bar(0), _bar(3)], now=now)

    assert [b.ts for b in repaired] == [_bar(i).ts for i in range(4)]
    assert repairer.log.recovered == 2
    assert client.calls == 1


@pytest.mark.asyncio
async def test_an_old_gap_is_left_alone(tmp_path):
    """Pulling old history into a live session can move an indicator far
    enough to fire a stop retroactively — exiting on late information."""
    client = _FakeClient([_bar(1)])
    repairer = _repairer(tmp_path, client)
    now = START + timedelta(minutes=MAX_REPAIR_AGE_MIN + 30)

    repaired = await repairer.repair("QQQ", [_bar(0), _bar(2)], now=now)

    assert len(repaired) == 2
    assert client.calls == 0
    assert repairer.log.skipped_too_old == 1


@pytest.mark.asyncio
async def test_a_minute_that_only_just_closed_is_given_time_to_arrive(tmp_path):
    """It is not lost yet — the live bar may still be in flight, and the
    provider may not have settled it either."""
    client = _FakeClient([_bar(1)])
    repairer = _repairer(tmp_path, client)
    # minute 1 is 90 seconds old: one bar interval has passed, which is not
    # enough to tell a lost minute from a slow one
    now = START + timedelta(minutes=2, seconds=30)

    await repairer.repair("QQQ", [_bar(0), _bar(2)], now=now)

    assert client.calls == 0


@pytest.mark.asyncio
async def test_the_same_minute_is_fetched_once_it_is_genuinely_late(tmp_path):
    """Non-vacuity for the test above: the wait is a delay, not a refusal."""
    client = _FakeClient([_bar(1)])
    repairer = _repairer(tmp_path, client)
    now = START + timedelta(minutes=3, seconds=30)

    repaired = await repairer.repair("QQQ", [_bar(0), _bar(2)], now=now)

    assert client.calls == 1
    assert len(repaired) == 3


@pytest.mark.asyncio
async def test_a_minute_that_never_traded_is_counted_not_chased(tmp_path):
    """No trades means no bar exists — ordinary on a single name at lunchtime.
    It is recorded once rather than retried forever."""
    client = _FakeClient([])  # the provider has nothing for that minute either
    repairer = _repairer(tmp_path, client)
    now = START + timedelta(minutes=4)

    repaired = await repairer.repair("QQQ", [_bar(0), _bar(3)], now=now)

    assert len(repaired) == 2
    assert repairer.log.recovered == 0
    assert repairer.log.unavailable == 2


@pytest.mark.asyncio
async def test_a_failed_fetch_leaves_the_series_untouched(tmp_path):
    """A broken repair must never be worse than the hole it was closing."""
    client = _FakeClient([], fail=True)
    repairer = _repairer(tmp_path, client)

    original = [_bar(0), _bar(3)]
    repaired = await repairer.repair("QQQ", original, now=START + timedelta(minutes=4))

    assert repaired is original
    assert repairer.log.failures == 1


@pytest.mark.asyncio
async def test_a_clean_series_costs_no_request(tmp_path):
    client = _FakeClient([])
    repairer = _repairer(tmp_path, client)

    await repairer.repair("QQQ", [_bar(i) for i in range(5)], now=START + timedelta(minutes=6))

    assert client.calls == 0


@pytest.mark.asyncio
async def test_repairs_are_capped_so_a_bad_session_cannot_run_away(tmp_path):
    from qqq_alpha.data.backfill import MAX_REPAIRS_PER_SESSION

    client = _FakeClient([])
    repairer = _repairer(tmp_path, client)
    repairer.repairs_this_session = MAX_REPAIRS_PER_SESSION

    await repairer.repair("QQQ", [_bar(0), _bar(3)], now=START + timedelta(minutes=4))

    assert client.calls == 0


@pytest.mark.asyncio
async def test_the_session_reset_clears_the_counters(tmp_path):
    repairer = _repairer(tmp_path, _FakeClient([]))
    repairer.repairs_this_session = 5
    repairer.log.requested = 9

    repairer.reset()

    assert repairer.repairs_this_session == 0
    assert repairer.log.requested == 0


# ---------------------------------------------------------------- the verdict
def _quality(bars: list[Bar]):
    return inspect_session(bars, expected_minutes=len(bars) + 2)


def test_clean_data_says_so():
    health = assess(_quality([_bar(i) for i in range(10)]), 0, RepairLog())
    assert "لا فجوات" in health.verdict


def test_reconnects_point_the_finger_at_the_connection():
    health = assess(_quality([_bar(0), _bar(5)]), reconnects=3, repair=RepairLog())

    assert "البثّ انقطع" in health.verdict
    assert "3" in health.message()


def test_no_reconnects_but_missing_minutes_names_the_other_two_suspects():
    """The distinction the operator could not make on his own: the socket
    never dropped, and the minutes vanished anyway."""
    health = assess(_quality([_bar(0), _bar(5)]), reconnects=0, repair=RepairLog())

    assert "الاتصال لم ينقطع" in health.verdict
    assert "المزوّد" in health.verdict and "الاستضافة" in health.verdict


def test_a_quiet_market_is_not_reported_as_a_fault():
    """Asked for, and told they never existed — nobody is at fault."""
    repair = RepairLog(requested=4, recovered=0, unavailable=4)
    health = assess(_quality([_bar(0), _bar(5)]), reconnects=0, repair=repair)

    assert "لم تُتداول أصلًا" in health.verdict


def test_the_report_carries_the_repair_result():
    repair = RepairLog(requested=4, recovered=3, unavailable=1)
    message = assess(_quality([_bar(0), _bar(5)]), 1, repair).message()

    assert "صحة البيانات اليوم" in message
    assert "استُرجعت 3" in message
    assert "لم تُتداول أصلًا" in message


# ---------------------------------------------------------------- in the engine
@pytest.mark.asyncio
async def test_the_engine_repairs_its_session_before_deciding(tmp_path):
    """The repair has to land before anything reads the series: marking a
    position or building a snapshot on a gapped history is the mistake."""
    from datetime import date

    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.data.pricing import BlackScholesPricer
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.domain import Action, Decision
    from qqq_alpha.journal import Journal
    from qqq_alpha.live.engine import LiveEngine
    from qqq_alpha.live.notifier import NullNotifier

    day = date(2026, 3, 2)
    bars = synthetic_session("QQQ", day, seed=8)
    # drop three minutes out of the middle of the run
    dropped = [b for i, b in enumerate(bars[:120]) if i not in (60, 61, 62)]

    seen: list[int] = []

    class _Watcher:
        async def decide(self, snapshot, **kwargs):
            seen.append(len(snapshot.recent_bars_1m))
            return Decision(ts=snapshot.ts, action=Action.PASS, confidence=3, thesis="t")

    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        max_data_age_sec=10**9,
        attention_threshold=0.0,
        attention_cooldown_sec=0,
        shadow_symbols_csv="",
    )
    settings.ensure_dirs()
    engine = LiveEngine(
        settings=settings,
        decider=_Watcher(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )
    engine._current_day = day
    engine.repairer = _repairer(tmp_path, _FakeClient([bars[60], bars[61], bars[62]]))

    for bar in dropped:
        await engine._on_bar(bar)

    assert engine.repairer.log.recovered == 3
    stamps = [b.ts for b in engine.session_bars]
    assert stamps == sorted(stamps), "recovered bars must be in time order"
    assert len(engine.session_bars) == len(dropped) + 3


@pytest.mark.asyncio
async def test_the_engine_reports_data_health_after_the_bell(tmp_path):
    from datetime import date

    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.data.pricing import BlackScholesPricer
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.journal import Journal
    from qqq_alpha.live.engine import LiveEngine
    from qqq_alpha.live.notifier import NullNotifier

    notes: list[str] = []

    class _Capture(NullNotifier):
        async def note(self, message: str) -> None:
            notes.append(message)

    day = date(2026, 3, 2)
    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        shadow_symbols_csv="",
    )
    settings.ensure_dirs()
    engine = LiveEngine(
        settings=settings,
        decider=None,
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=_Capture(),
    )
    bars = synthetic_session("QQQ", day, seed=8)
    engine.session_bars = [b for i, b in enumerate(bars[:100]) if i != 50]
    engine.status.reconnects = 2

    await engine._report_data_health()

    assert notes and "صحة البيانات اليوم" in notes[0]
    assert "البثّ انقطع" in notes[0], "two reconnects point at the connection"
