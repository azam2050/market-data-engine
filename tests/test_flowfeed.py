"""The live flow feed: raw tape rows in, classified institutional prints out."""

from datetime import UTC, date, datetime, timedelta

import pytest

from qqq_alpha.config import Settings
from qqq_alpha.data.chain import ChainSnapshot, LiveChainPricer
from qqq_alpha.data.massive import MassiveError
from qqq_alpha.domain import FlowKind, OptionContract, OptionType
from qqq_alpha.live.flowfeed import LiveFlowFeed, rows_to_events

NOW = datetime(2026, 8, 7, 14, 30, tzinfo=UTC)


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000_000)


def _row(ts: datetime, price: float, size: int) -> dict:
    return {"sip_timestamp": _ns(ts), "price": price, "size": size}


def _contract(option_type: OptionType, strike: float, volume: int = 100) -> OptionContract:
    letter = "C" if option_type is OptionType.CALL else "P"
    return OptionContract(
        occ_symbol=f"O:QQQ260807{letter}{int(strike * 1000):08d}",
        underlying="QQQ",
        option_type=option_type,
        strike=strike,
        expiry=date(2026, 8, 7),
        bid=1.00,
        ask=1.10,
        volume=volume,
        open_interest=500,
    )


@pytest.fixture
def settings():
    return Settings(massive_api_key="test", anthropic_api_key="test", anthropic_model="test")


@pytest.fixture
def pricer(settings):
    contracts = [
        _contract(OptionType.CALL, 702),
        _contract(OptionType.CALL, 703),
        _contract(OptionType.PUT, 701),
        _contract(OptionType.PUT, 700),
    ]
    p = LiveChainPricer(settings)
    p.snapshot = ChainSnapshot(
        fetched_at=datetime.now(UTC),
        expiry=date(2026, 8, 7),
        contracts={c.occ_symbol: c for c in contracts},
    )
    return p


def test_rows_become_classified_events():
    occ = "O:QQQ260807C00702000"
    rows = [_row(NOW, price=1.09, size=100)]  # $10,900 premium, printed at the ask
    events = rows_to_events(occ, rows, bid=1.00, ask=1.10)
    assert len(events) == 1
    assert events[0].aggressor == "BUY"
    assert events[0].is_bullish
    assert events[0].premium == pytest.approx(10_900)


def test_retail_noise_is_filtered_out():
    """A $500 print is somebody's lunch money, not institutional intent."""
    occ = "O:QQQ260807C00702000"
    rows = [_row(NOW, price=0.05, size=100), _row(NOW, price=0.0, size=100)]
    assert rows_to_events(occ, rows, bid=1.0, ask=1.1) == []


@pytest.mark.asyncio
async def test_poll_collects_prints_and_marks_sweeps(settings, pricer):
    feed = LiveFlowFeed(settings, pricer)

    async def fake_fetch(targets, since):
        # three rapid prints on the first target — a sweep — nothing elsewhere
        sweep = [
            _row(NOW - timedelta(seconds=2), 1.09, 100),
            _row(NOW - timedelta(seconds=1), 1.09, 120),
            _row(NOW, 1.09, 150),
        ]
        return [sweep] + [[] for _ in targets[1:]]

    feed._fetch = fake_fetch
    events = await feed.poll(NOW, spot=702.0)

    assert feed.polls == 1
    sweeps = [e for e in events if e.kind is FlowKind.SWEEP]
    assert len(sweeps) == 1
    assert sweeps[0].size == 370
    assert sweeps[0].exchanges == 3


@pytest.mark.asyncio
async def test_poll_disables_itself_when_the_plan_lacks_the_tape(settings, pricer):
    feed = LiveFlowFeed(settings, pricer)

    async def fake_fetch(targets, since):
        return [MassiveError("GET /v3/trades -> 403: NOT_AUTHORIZED") for _ in targets]

    feed._fetch = fake_fetch
    await feed.poll(NOW, spot=702.0)
    assert feed.disabled
    assert "403" in (feed.last_error or "")

    # once disabled it must not fetch again
    async def exploding_fetch(targets, since):  # pragma: no cover - must not run
        raise AssertionError("fetch called after disable")

    feed._fetch = exploding_fetch
    assert await feed.poll(NOW, spot=702.0) == []


@pytest.mark.asyncio
async def test_window_drops_stale_prints(settings, pricer):
    feed = LiveFlowFeed(settings, pricer)

    async def first_fetch(targets, since):
        return [[_row(NOW - timedelta(minutes=60), 1.09, 100)]] + [[] for _ in targets[1:]]

    feed._fetch = first_fetch
    events = await feed.poll(NOW, spot=702.0)
    assert events == []  # an hour-old print is outside the 45-minute window


@pytest.mark.asyncio
async def test_a_failed_poll_never_raises(settings, pricer):
    feed = LiveFlowFeed(settings, pricer)

    async def broken_fetch(targets, since):
        raise RuntimeError("network down")

    feed._fetch = broken_fetch
    assert await feed.poll(NOW, spot=702.0) == []
    assert not feed.disabled  # transient failure is not an entitlement problem
