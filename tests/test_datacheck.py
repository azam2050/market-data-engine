"""فحص البيانات: does the check actually catch a provider that returns
half a chain, a stalled feed, or a side with no tradeable contract?

Every case here is a way real data goes wrong, and the test asserts the
report says so rather than passing it off as healthy."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from qqq_alpha.dashboard.app import create_app
from qqq_alpha.domain import Bar, OptionContract, OptionType
from qqq_alpha.live.datacheck import BAD, GOOD, WARN, expiry_to_check, inspect_bars, run_data_check
from qqq_alpha.live.engine import LiveEngine
from tests.test_desk import _settings

NY = ZoneInfo("America/New_York")
DAY = date(2026, 9, 8)  # a Tuesday
NOON = datetime(DAY.year, DAY.month, DAY.day, 12, 0, tzinfo=NY).astimezone(UTC)


def _bars(symbol: str, until_minute: int = 150, spot: float = 720.0, drop: set[int] | None = None,
          frozen: bool = False) -> list[Bar]:
    out: list[Bar] = []
    px = spot
    for m in range(until_minute):
        if drop and m in drop:
            continue
        ts = datetime(DAY.year, DAY.month, DAY.day, 9, 30, tzinfo=NY) + timedelta(minutes=m)
        px = px if frozen else px + 0.01
        out.append(Bar(symbol=symbol, ts=ts.astimezone(UTC), open=px, high=px + 0.05, low=px - 0.05, close=px, volume=1000))
    return out


def _chain(side: OptionType, lo: int, hi: int, spot: float = 720.0, quoted: bool = True,
           delta: bool = True, expiry: date = DAY) -> list[OptionContract]:
    out = []
    for k in range(lo, hi + 1):
        mid = max(0.2, 3.0 - abs(k - spot) * 0.25)
        out.append(OptionContract(
            occ_symbol=f"O:QQQ{'C' if side is OptionType.CALL else 'P'}{k}",
            underlying="QQQ", option_type=side, strike=float(k), expiry=expiry,
            bid=round(mid - 0.03, 2) if quoted else None,
            ask=round(mid + 0.03, 2) if quoted else None,
            last=mid, volume=800, open_interest=900, implied_volatility=0.2,
            delta=(0.5 if side is OptionType.CALL else -0.5) if delta else None,
            gamma=None, theta=None,
        ))
    return out


class _Provider:
    """A provider that can be told exactly how to misbehave."""

    def __init__(self, **kw):
        self.opts = kw
        self.chain_calls: list[tuple] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def range_minute_bars(self, symbol, minutes, start, end):
        if self.opts.get("bars_fail"):
            raise RuntimeError("upstream 502")
        return _bars(symbol, **(self.opts.get("bars") or {}))

    async def option_chain(self, underlying, expiry, option_type=None, around=None, window=None):
        self.chain_calls.append((underlying, expiry, option_type, around))
        if self.opts.get("chain_fail"):
            raise RuntimeError("NOT_AUTHORIZED")
        spec = (self.opts.get("chain") or {}).get(option_type, {"lo": 700, "hi": 740})
        if spec is None:
            return []
        return _chain(option_type, spec["lo"], spec["hi"], quoted=spec.get("quoted", True),
                      delta=spec.get("delta", True), expiry=expiry)

    async def option_trades_since(self, occ, since, limit=5000):
        if self.opts.get("tape_fail"):
            raise RuntimeError("NOT_AUTHORIZED: options trades")
        return [{"price": 1.0, "size": 3}]


async def _run(**kw):
    from qqq_alpha.config import Settings

    provider = _Provider(**kw)
    settings = Settings(admin_username="a", admin_password="b", massive_api_key="k")
    report = await run_data_check(settings, ("QQQ",), NOON, client_factory=lambda: provider)
    return report, provider


def _check(report, name):
    return next(c for c in report.checks if c.name == name)


# ---------------------------------------------------------------- healthy
@pytest.mark.asyncio
async def test_a_healthy_feed_reports_both_sides_with_their_picks():
    report, provider = await _run()
    assert report.status == GOOD
    bars = _check(report, "شموع QQQ")
    assert bars.status == GOOD and "دقيقة ليوم 2026-09-08" in bars.detail
    for label in ("كول", "بوت"):
        chain = _check(report, f"عقود QQQ {label}")
        assert chain.status == GOOD and "المختار" in chain.detail
        side = report.data["QQQ"]["chain"][label]
        assert side["covers_money"] and side["pick"]["mid"] > 0 and side["pick"]["delta"] is not None
    # both sides were asked for, each one centred on the last traded price
    assert {c[2] for c in provider.chain_calls} == {OptionType.CALL, OptionType.PUT}
    spot = report.data["QQQ"]["bars"]["last_close"]
    assert [c[3] for c in provider.chain_calls] == [spot, spot]
    assert {c[1] for c in provider.chain_calls} == {DAY}  # today's expiry
    assert _check(report, "شريط الصفقات").status == GOOD


# ---------------------------------------------------------------- the chain
@pytest.mark.asyncio
async def test_a_chain_that_stops_short_of_the_money_is_called_out():
    """The exact production failure: the page ends below the spot, so the
    contract the desk needs was never in the data."""
    report, _ = await _run(chain={OptionType.CALL: {"lo": 400, "hi": 650},
                                 OptionType.PUT: {"lo": 700, "hi": 740}})
    call = _check(report, "عقود QQQ كول")
    assert call.status == BAD and "لا تغطي السعر" in call.detail
    assert report.data["QQQ"]["chain"]["كول"]["covers_money"] is False
    assert _check(report, "عقود QQQ بوت").status == GOOD  # the other side is judged separately
    assert report.status == BAD


@pytest.mark.asyncio
async def test_an_empty_side_is_reported_even_when_the_other_side_is_full():
    report, _ = await _run(chain={OptionType.CALL: {"lo": 700, "hi": 740}, OptionType.PUT: None})
    assert _check(report, "عقود QQQ بوت").status == BAD
    assert "لم يصل أي عقد" in _check(report, "عقود QQQ بوت").detail
    assert _check(report, "عقود QQQ كول").status == GOOD


@pytest.mark.asyncio
async def test_a_chain_without_quotes_or_greeks_is_a_warning_not_a_pass():
    report, _ = await _run(chain={OptionType.CALL: {"lo": 700, "hi": 740, "quoted": False, "delta": False},
                                  OptionType.PUT: {"lo": 700, "hi": 740}})
    call = _check(report, "عقود QQQ كول")
    assert call.status == WARN and "بلا سعرين" in call.detail and "بلا دلتا" in call.detail


@pytest.mark.asyncio
async def test_a_chain_request_that_fails_is_reported_as_a_failure():
    report, _ = await _run(chain_fail=True)
    assert report.status == BAD
    assert all(_check(report, f"عقود QQQ {s}").status == BAD for s in ("كول", "بوت"))
    assert "NOT_AUTHORIZED" in _check(report, "عقود QQQ كول").detail


# ---------------------------------------------------------------- the bars
@pytest.mark.asyncio
async def test_missing_minutes_and_a_frozen_price_are_both_named():
    report, _ = await _run(bars={"drop": {30, 31, 32}, "until_minute": 150})
    bars = _check(report, "شموع QQQ")
    assert bars.status == WARN and "ناقص 3 دقيقة" in bars.detail
    info = report.data["QQQ"]["bars"]
    assert info["missing"] == 3 and info["gaps"][0][1] == 3


@pytest.mark.asyncio
async def test_a_feed_standing_still_during_the_session_is_a_failure():
    """The last bar is an hour old while the market is open: the desk would
    be pricing yesterday's tape without noticing."""
    report, _ = await _run(bars={"until_minute": 60})  # last bar 10:29, checked at 12:00
    bars = _check(report, "شموع QQQ")
    assert bars.status == BAD and "الفيد متأخر" in bars.detail
    assert report.data["QQQ"]["bars"]["age_min"] > 60


@pytest.mark.asyncio
async def test_bars_that_never_arrive_are_reported_and_stop_that_symbol():
    report, _ = await _run(bars_fail=True)
    assert _check(report, "شموع QQQ").status == BAD
    assert "فشل الجلب" in _check(report, "شموع QQQ").detail
    assert report.data["QQQ"]["bars"]["error"]


@pytest.mark.asyncio
async def test_the_tape_entitlement_is_a_warning_not_a_blocker():
    report, _ = await _run(tape_fail=True)
    tape = _check(report, "شريط الصفقات")
    assert tape.status == WARN and "لا تشمل صفقات الأوبشنز" in tape.detail
    assert report.status == WARN  # the screens still work


@pytest.mark.asyncio
async def test_no_key_is_reported_before_any_request_is_made():
    from qqq_alpha.config import Settings

    report = await run_data_check(Settings(admin_username="a", admin_password="b"), ("QQQ",), NOON)
    assert report.status == BAD and "MASSIVE_API_KEY" in report.checks[0].detail


# ---------------------------------------------------------------- pieces
def test_inspect_bars_counts_what_arrived():
    info = inspect_bars(_bars("QQQ", 120), NOON)
    assert info["bars"] == 120 and info["first"] == "09:30" and info["last"] == "11:29"
    assert info["missing"] == 0 and info["duplicates"] == 0 and info["frozen"] == 1
    frozen = inspect_bars(_bars("QQQ", 120, frozen=True), NOON)
    assert frozen["frozen"] == 120


def test_the_expiry_checked_is_todays_for_the_index_and_friday_for_a_stock():
    assert expiry_to_check("QQQ", NOON) == DAY
    assert expiry_to_check("SPY", NOON) == DAY
    assert expiry_to_check("NVDA", NOON) == date(2026, 9, 11)  # that Friday
    # the Sunday before Labor Day: the next session is Tuesday, not Monday
    sunday = datetime(2026, 9, 6, 12, 0, tzinfo=NY).astimezone(UTC)
    assert expiry_to_check("QQQ", sunday) == DAY


@pytest.mark.asyncio
async def test_the_report_reads_as_one_message():
    report, _ = await _run(chain={OptionType.CALL: {"lo": 400, "hi": 650},
                                  OptionType.PUT: {"lo": 700, "hi": 740}})
    text = report.as_text()
    assert text.startswith("❌ خلل في البيانات")
    assert "بتوقيت نيويورك" in text
    assert "❌ عقود QQQ كول" in text and "✅ عقود QQQ بوت" in text
    assert len(text.splitlines()) == 3 + len(report.checks)


# ---------------------------------------------------------------- the wiring
def test_the_operator_words_reach_the_data_check_without_stealing_فحص():
    assert LiveEngine._wants_data_check("فحص البيانات")
    assert LiveEngine._wants_data_check("افحص بيانات")
    assert LiveEngine._wants_data_check("البيانات")
    assert LiveEngine._wants_data_check("/data")
    # the older channel test keeps its own word
    assert not LiveEngine._wants_data_check("فحص")
    assert not LiveEngine._wants_data_check("")
    assert not LiveEngine._wants_data_check("شاشتي")


def test_the_dashboard_route_is_behind_the_admin_password(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings), follow_redirects=False)
    assert client.get("/data-check").status_code == 401
    assert client.get("/api/data-check").status_code == 401

    async def fake(_settings, *a, **kw):
        from qqq_alpha.live.datacheck import Report

        r = Report(now=NOON)
        r.add("شموع QQQ", GOOD, "وصلت")
        return r

    monkeypatch.setattr("qqq_alpha.live.datacheck.run_data_check", fake)
    page = client.get("/data-check", auth=("admin", "secret"))
    assert page.status_code == 200 and "شموع QQQ" in page.text
    api = client.get("/api/data-check", auth=("admin", "secret"))
    assert api.status_code == 200 and api.json()["status"] == GOOD
