"""قائد اليوم: the block, the rule replayed bar by bar, the board and its
routes. The synthetic days are quiet with a drift, so the leader's trend is
known; signals are injected through the block dictionary the rule reads."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from qqq_alpha.dashboard.app import create_app
from qqq_alpha.domain import Bar
from qqq_alpha.live import leader as LD
from qqq_alpha.live import mirsad9
from qqq_alpha.live.desk import DeskService
from qqq_alpha.live.engine import LiveEngine
from qqq_alpha.live.leader import LeaderService, Opportunity, block_series, impulses, replay
from qqq_alpha.memory import Memory
from tests.test_desk import _FakeClient, _minutes, _settings

NY = ZoneInfo("America/New_York")
DAY = date(2026, 9, 8)  # Tuesday


# ---------------------------------------------------------------- helpers
def _bar(sym: str, ts: datetime, o: float, h: float, lo: float, c: float) -> Bar:
    return Bar(symbol=sym, ts=ts, open=o, high=h, low=lo, close=c, volume=1000)


def _quiet_days(sym: str, days: int, start: float = 100.0, drift: float = 0.01, seed: int = 1) -> list[Bar]:
    """Five-minute bars over ``days`` sessions ending the day before DAY."""
    return mirsad9.resample(_minutes(sym, days + 1, start, drift, seed), 5)[:-78]


def _slot(k: int, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, 9, 30, tzinfo=NY) + timedelta(minutes=5 * k)


def _with_day(base: list[Bar], sym: str, k_signal: int, script, sig=(0.30, 0.02, 0.25)) -> list[Bar]:
    """Append DAY to ``base``: quiet bars up to ``k_signal``, a signal bar
    that closes up (``sig`` = high, low, close offsets from the open), then
    whatever ``script(level, atr, signal_bar)`` yields per bar."""
    px = base[-1].close
    bars = list(base)
    for k in range(k_signal):
        bars.append(_bar(sym, _slot(k), px, px + 0.03, px - 0.03, px))
    bars.append(_bar(sym, _slot(k_signal), px, px + sig[0], px - sig[1], px + sig[2]))
    core = mirsad9.core(bars, 5, LD.RAW)
    k = core[-1]
    level = min(k.e9, bars[-1].close)
    for j, b in enumerate(script(level, k.atr, bars[-1]), start=k_signal + 1):
        bars.append(_bar(sym, _slot(j), *b))
    return bars


def _block_at(bars: list[Bar], k: int, net: int) -> dict[datetime, int]:
    return {_slot(k): net}


# ---------------------------------------------------------------- impulses & block
def test_impulses_marks_a_strong_break_and_skips_the_first_bar_and_the_close():
    base = _quiet_days("NVDA", 4)
    px = base[-1].close
    bars = list(base)
    # first bar of the day is a huge impulse: never counted; a mid-day one is
    bars.append(_bar("NVDA", _slot(0), px, px + 5, px - 0.01, px + 4.8))
    for k in range(1, 30):
        bars.append(_bar("NVDA", _slot(k), px, px + 0.03, px - 0.03, px))
    bars.append(_bar("NVDA", _slot(30), px, px + 5, px - 0.01, px + 4.8))
    for k in range(31, 76):
        bars.append(_bar("NVDA", _slot(k), px, px + 0.03, px - 0.03, px))
    bars.append(_bar("NVDA", _slot(76), px, px + 5, px - 0.01, px + 4.8))  # 15:50: too late
    sides = impulses(bars, len(bars))
    assert sides.get(_slot(0)) is None
    assert sides.get(_slot(30)) == 1
    assert sides.get(_slot(76)) is None
    # the forming bar is never counted
    assert impulses(bars, len(bars) - 1).get(_slot(76)) is None


def test_block_series_sums_the_basket():
    a = _quiet_days("AAPL", 4)
    b = _quiet_days("MSFT", 4, seed=2)
    pa, pb = a[-1].close, b[-1].close
    a = a + [_bar("AAPL", _slot(k), pa, pa + 0.03, pa - 0.03, pa) for k in range(10)] + [_bar("AAPL", _slot(10), pa, pa + 5, pa - 0.01, pa + 4.8)]
    b = b + [_bar("MSFT", _slot(k), pb, pb + 0.03, pb - 0.03, pb) for k in range(10)] + [_bar("MSFT", _slot(10), pb, pb + 0.01, pb - 5, pb - 4.8)]
    net = block_series({"AAPL": a, "MSFT": b}, None)
    assert net[_slot(10)] == 0
    net = block_series({"AAPL": a, "MSFT": b[:-1]}, None)
    assert net[_slot(10)] == 1


# ---------------------------------------------------------------- the rule
def test_a_block_signal_fills_at_the_zone_and_exits_at_the_target():
    base = _quiet_days("QQQ", 4, drift=0.01)

    def script(level, atr, sig):
        yield (level + 0.02, level + 0.03, level - 0.01, level + 0.02)          # touches the zone
        yield (level + 0.02, level + atr * 1.2, level - 0.05, level + atr)      # reaches the target

    bars = _with_day(base, "QQQ", 10, script)
    opps = replay(bars, _block_at(bars, 10, 3), None)
    assert len(opps) == 1
    p = opps[0]
    assert p.side == 1 and p.trend == 1 and p.n == 1 and p.grade == "أ"
    assert p.status == "closed" and p.via_zone and p.how == "target"
    assert p.entry == pytest.approx(min(bars[-2].open, p.level))
    assert p.target == pytest.approx(p.entry + p.atr)
    assert p.stop == pytest.approx(p.entry - 1.5 * p.atr)
    assert p.r == pytest.approx(1 / 1.5, abs=1e-6)
    d = p.as_dict()
    assert d["how_text"] == "الهدف" and d["side_text"] == "كول" and d["entry_time"] == "10:25"


def test_the_stop_is_checked_before_the_target_and_costs_one_r():
    base = _quiet_days("QQQ", 4, drift=0.01)

    def script(level, atr, sig):
        yield (level + 0.02, level + 0.03, level - 0.01, level + 0.02)
        yield (level, level + 2 * atr, level - 2 * atr, level)  # both in one bar: the stop wins

    bars = _with_day(base, "QQQ", 10, script)
    p = replay(bars, _block_at(bars, 10, 3), None)[0]
    assert p.status == "closed" and p.how == "stop" and p.r == pytest.approx(-1.0)


def test_no_touch_but_a_close_beyond_the_signal_bar_fills_at_the_next_open():
    base = _quiet_days("QQQ", 4, drift=0.01)

    def script(level, atr, sig):
        hi = sig.high
        assert hi + 0.02 - level <= 1.5 * atr  # the confirmation stays within reach of the zone
        yield (hi, hi + 0.04, level + 0.03, hi + 0.02)       # no touch; closes above the signal high
        yield (hi + 0.05, hi + 0.06, hi + 0.01, hi + 0.03)   # fills at this open
        yield (hi + 0.03, hi + 0.05, hi - 0.01, hi + 0.02)

    bars = _with_day(base, "QQQ", 10, script, sig=(0.05, 0.02, 0.04))
    p = replay(bars, _block_at(bars, 10, 3), None)[0]
    assert p.status == "open" and not p.via_zone
    assert p.entry == pytest.approx(bars[-2].open)
    assert p.entry_ts == _slot(12)


def test_a_signal_without_touch_or_confirmation_is_cancelled_after_eight_bars():
    base = _quiet_days("QQQ", 4, drift=0.01)

    def script(level, atr, sig):
        for _ in range(10):
            yield (sig.close, sig.close + 0.01, sig.close - 0.01, sig.close)  # drifts sideways above the zone

    bars = _with_day(base, "QQQ", 10, script)
    p = replay(bars, _block_at(bars, 10, 3), None)[0]
    assert p.status == "cancelled" and "٨ شموع" in p.reason


def test_a_signal_against_the_leaders_trend_is_refused_and_spends_no_slot():
    base = _quiet_days("QQQ", 4, drift=0.01)  # trend up
    px = base[-1].close
    bars = list(base) + [_bar("QQQ", _slot(k), px, px + 0.03, px - 0.03, px) for k in range(10)]
    bars.append(_bar("QQQ", _slot(10), px, px + 0.02, px - 0.30, px - 0.25))  # closes down with a down block
    bars += [_bar("QQQ", _slot(k), px, px + 0.03, px - 0.03, px) for k in range(11, 20)]
    opps = replay(bars, {_slot(10): -3}, None)
    assert len(opps) == 1
    assert opps[0].status == "rejected" and opps[0].n == 0 and opps[0].side == -1 and opps[0].trend == 1


def test_two_slots_a_day_and_one_opportunity_at_a_time():
    base = _quiet_days("QQQ", 4, drift=0.01)

    def script(level, atr, sig):
        yield (level + 0.02, level + 0.03, level - 0.01, level + 0.02)   # k=11 fill
        yield (level + 0.02, level + 0.05, level, level + 0.04)          # k=12: a second signal while open
        yield (level + 0.04, level + 0.05, level + 0.02, level + 0.03)
        yield (level + 0.03, level + 0.06, level + 0.02, level + 0.05)   # k=14: a third signal, no slot left
        for _ in range(6):
            yield (level + 0.05, level + 0.06, level + 0.04, level + 0.05)

    bars = _with_day(base, "QQQ", 10, script)
    block = {_slot(10): 3, _slot(12): 3, _slot(14): 3}
    opps = replay(bars, block, None)
    assert [p.status for p in opps] == ["open", "skipped"]
    assert opps[1].n == 2 and "صفقة قائمة" in opps[1].reason
    # the third never appears: the day's two slots are spent
    assert len(opps) == 2


def test_the_bell_closes_what_is_open_and_the_forming_bar_never_signals():
    base = _quiet_days("QQQ", 4, drift=0.01)

    def script(level, atr, sig):
        yield (level + 0.02, level + 0.03, level - 0.01, level + 0.02)
        for _ in range(64):
            yield (level + 0.02, level + 0.04, level, level + 0.02)

    bars = _with_day(base, "QQQ", 10, script)
    assert bars[-1].ts == _slot(75)
    # a signal on the last bar while it is still forming: nothing yet
    forming_now = bars[-1].ts + timedelta(minutes=2)
    opps = replay(bars, {_slot(10): 3, _slot(75): 3}, forming_now)
    assert [p.status for p in opps] == ["open"]
    # extend to the 15:55 bar: the open trade leaves at its close once confirmed
    px = bars[-1].close
    bars += [_bar("QQQ", _slot(k), px, px + 0.02, px - 0.02, px) for k in range(76, 78)]
    opps = replay(bars, {_slot(10): 3}, bars[-1].ts + timedelta(minutes=2))
    assert opps[0].status == "open"
    opps = replay(bars, {_slot(10): 3}, bars[-1].ts + timedelta(minutes=6))
    assert opps[0].status == "closed" and opps[0].how == "eod" and opps[0].exit == pytest.approx(px)


def test_replay_needs_history_and_signals_only_inside_the_window():
    base = _quiet_days("QQQ", 4, drift=0.01)
    assert replay(base[:100], {}, None) == []

    def script(level, atr, sig):
        for _ in range(3):
            yield (sig.close, sig.close + 0.01, sig.close - 0.01, sig.close)

    # a signal on the second bar of the day (09:35) is too early
    bars = _with_day(base, "QQQ", 1, script)
    assert replay(bars, _block_at(bars, 1, 3), None) == []
    # one on the 15:45 bar is too late
    bars = _with_day(base, "QQQ", 75, script)
    assert replay(bars, _block_at(bars, 75, 3), None) == []


# ---------------------------------------------------------------- the day
def test_daily_atr_and_the_morning_read():
    bars = mirsad9.resample(_minutes("QQQ", 9, drift=0.01), 5)
    atr = LD.daily_atr(bars)
    assert atr[DAY] is not None and atr[DAY] > 0
    at_1030 = datetime(DAY.year, DAY.month, DAY.day, 10, 31, tzinfo=NY)
    read = LD.morning_read(bars, at_1030)
    assert read["gap"] is not None and read["first_hour"] is not None and read["second_hour"] is None
    assert [s["done"] for s in read["steps"]] == [True, True, False]
    at_1200 = datetime(DAY.year, DAY.month, DAY.day, 12, 0, tzinfo=NY)
    read = LD.morning_read(bars, at_1200)
    assert read["second_hour"] is not None and read["steps"][2]["done"]
    before = datetime(DAY.year, DAY.month, DAY.day, 9, 0, tzinfo=NY)
    read = LD.morning_read(bars, before)
    assert read["gap"] is not None  # yesterday's close and today's open exist in the bars


def test_a_flat_first_hour_is_not_called_a_direction():
    """A first hour worth 0.0 of the day's range is quiet, not 'صاعدة 0.0'."""
    quiet = mirsad9.resample(_minutes("QQQ", 9, drift=0.0, seed=7), 5)
    at_1030 = datetime(DAY.year, DAY.month, DAY.day, 10, 31, tzinfo=NY)
    read = LD.morning_read(quiet, at_1030)
    step = read["steps"][1]
    if abs(read["first_hour"]) < 0.15:
        assert step["title"] == "الساعة الأولى شبه ثابتة" and step["tone"] == "hold"
    else:
        assert "من المدى" in step["title"] and step["tone"] == "on"


# ---------------------------------------------------------------- the service
def _service(tmp_path, client, now):
    settings = _settings(tmp_path)
    mem = Memory(settings.data_dir / "memory.db")
    desk = DeskService(settings, mem, client_factory=lambda: client, now_fn=lambda: now)
    return LeaderService(desk), mem


@pytest.mark.asyncio
async def test_board_shape_and_cache(tmp_path):
    client = _FakeClient()
    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, mem = _service(tmp_path, client, now)
    b = await svc.board()
    assert b["session"]["open"] is True
    assert [ld["symbol"] for ld in b["leaders"]] == ["QQQ", "SPY"]
    for ld in b["leaders"]:
        assert ld["state"] in {"idle", "waiting", "chase", "open", "done"}
        assert ld["trend"] in (-1, 0, 1) and ld["plan"] and ld["say"]["text"]
        assert len(ld["closes"]) == 40
    assert len(b["block"]["tiles"]) == 10
    bl = b["block"]
    assert bl["up"] - bl["down"] == bl["net"]  # the tiles say exactly what the rule counted
    assert bl["flat"] + sum(1 for t in bl["tiles"] if t["side"]) == 10
    assert b["verdict"]["tone"] in {"up", "dn", "wait", "off"} and b["verdict"]["title"]
    assert b["morning"]["steps"][0]["time"] == "09:30"
    assert b["measured"]["trades"] == LD.MEASURED["trades"]
    assert b["journal"]["today"] == [] or all("status" in p for p in b["journal"]["today"])
    fetched = sorted(c[1] for c in client.calls if c[0] == "bars")
    assert fetched == sorted(LD.LEADERS + LD.BASKET)
    # a second call inside the TTL is served from the cache
    await svc.board()
    assert sorted(c[1] for c in client.calls if c[0] == "bars") == fetched


@pytest.mark.asyncio
async def test_a_basket_name_is_pulled_over_fewer_days_than_a_leader(tmp_path):
    """A basket name is only ever asked whether it impulsed; asking each of
    the ten for a leader's history is bandwidth and CPU spent for nothing."""

    class Counting(_FakeClient):
        def __init__(self):
            super().__init__()
            self.days: dict[str, int] = {}

        async def range_minute_bars(self, symbol, minutes, start, end):
            self.days[symbol] = (end - start).days
            return await super().range_minute_bars(symbol, minutes, start, end)

    client = Counting()
    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, _ = _service(tmp_path, client, now)
    await svc.board()
    assert {client.days[s] for s in LD.LEADERS} == {LD.LOOKBACK_DAYS}
    assert {client.days[s] for s in LD.BASKET} == {LD.BASKET_LOOKBACK_DAYS}
    assert LD.BASKET_LOOKBACK_DAYS < LD.LOOKBACK_DAYS


@pytest.mark.asyncio
async def test_the_price_line_has_its_own_cheap_beat(tmp_path):
    """The page asks for the price every second; that must cost one request
    for both leaders, shared by everyone watching."""

    class Tape(_FakeClient):
        def __init__(self):
            super().__init__()
            self.tape = 0

        async def last_prices(self, symbols):
            self.tape += 1
            return {s: {"price": 700.0 + self.tape, "ts": None, "size": 1, "change_pct": 0.1} for s in symbols}

    client = Tape()
    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, _ = _service(tmp_path, client, now)
    first = await svc.ticks()
    assert set(first["prices"]) == set(LD.LEADERS)
    assert first["prices"]["QQQ"]["price"] == 701.0
    assert await svc.ticks() == first and client.tape == 1  # inside the cache
    svc._ticks = None
    assert (await svc.ticks())["prices"]["QQQ"]["price"] == 702.0


@pytest.mark.asyncio
async def test_a_dead_tape_leaves_the_screen_standing(tmp_path):
    class Broken(_FakeClient):
        async def last_prices(self, symbols):
            raise RuntimeError("upstream 500")

    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, _ = _service(tmp_path, Broken(), now)
    out = await svc.ticks()
    assert out["prices"] == {} and out["now"]


@pytest.mark.asyncio
async def test_the_ticks_carry_the_levels_of_an_open_trade(tmp_path):
    """So the marker on the rail can move with the tape without waiting for
    the board to rebuild."""

    class Tape(_FakeClient):
        async def last_prices(self, symbols):
            return {s: {"price": 100.0, "ts": None, "size": 1, "change_pct": 0.0} for s in symbols}

    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, _ = _service(tmp_path, Tape(), now)
    board = await svc.board()
    assert await svc.ticks() is not None
    svc._ticks = None
    # plant an open trade in the cached board the way a real session would
    board["leaders"][0]["active"] = {"status": "open", "entry": 100.0, "stop": 98.5, "target": 101.0, "side": 1}
    svc._cache = (svc._cache[0], board)
    out = await svc.ticks()
    lv = out["open"][board["leaders"][0]["symbol"]]
    assert lv["stop"] == 98.5 and lv["target"] == 101.0 and lv["side"] == 1
    assert board["leaders"][1]["symbol"] not in out["open"]  # only what is open


@pytest.mark.asyncio
async def test_board_survives_a_missing_basket_name(tmp_path):
    class Broken(_FakeClient):
        async def range_minute_bars(self, symbol, minutes, start, end):
            if symbol == "AVGO":
                raise RuntimeError("no such ticker")
            return await super().range_minute_bars(symbol, minutes, start, end)

    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, _ = _service(tmp_path, Broken(), now)
    b = await svc.board()
    assert any("AVGO" in e for e in b["errors"])
    tile = next(t for t in b["block"]["tiles"] if t["symbol"] == "AVGO")
    assert tile["unavailable"] is True
    assert [ld["symbol"] for ld in b["leaders"]] == ["QQQ", "SPY"]


def test_plan_and_say_cover_every_state():
    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    base = dict(symbol="QQQ", n=1, side=1, net=3, signal_i=200, signal_ts=_slot(10), atr=1.0, level=100.0, sig_hi=100.5, sig_lo=99.8, trend=1, grade="أ")
    k = mirsad9.CoreBar(False, False, 0.0, 1.0, 100.0, 1, "", 0, False, 11 * 60, 18)
    waiting = Opportunity(**base)
    assert LeaderService._plan("QQQ", 101.0, 1.0, 100.0, waiting, 1, 1, 1, now)[0]["key"] == "touch"
    assert "بانتظار الرجوع" in LeaderService._say("QQQ", 101.0, waiting, 1, 1, 3, 1, now, k)["text"]
    opened = Opportunity(**base)
    opened.fill(205, _slot(11), 100.0, True)
    plan = LeaderService._plan("QQQ", 100.4, 1.0, 100.0, opened, 1, 1, 1, now)
    assert plan[0]["key"] == "hold" and "درجة أ" in plan[0]["title"]
    assert "فوق الدخول" in LeaderService._say("QQQ", 100.4, opened, 1, 1, 3, 1, now, k)["text"]
    idle = LeaderService._plan("QQQ", 100.0, 1.0, 99.8, None, 0, 1, 0, now)
    assert [p["key"] for p in idle] == ["if_up", "against", "none"]
    assert "هادئة" in LeaderService._say("QQQ", 100.0, None, 0, 1, 1, 0, now, k)["text"]
    assert "ضد الميل" in LeaderService._say("QQQ", 100.0, None, -1, 1, -3, 0, now, k)["text"]
    assert LeaderService._plan("QQQ", 100.0, 1.0, 99.8, None, 0, 1, 2, now)[0]["key"] == "done"
    assert "انتهت" in LeaderService._say("QQQ", 100.0, None, 0, 1, 0, 2, now, k)["text"]
    closed_day = datetime(2026, 9, 6, 12, 0, tzinfo=NY).astimezone(UTC)  # Sunday
    assert "مغلق" in LeaderService._say("QQQ", 100.0, None, 0, 1, 0, 0, closed_day, k)["text"]


def test_tiles_count_each_impulse_so_the_screen_matches_the_rule():
    """A name impulsing on both of the last two bars is what the rule counts
    twice; the tiles must say so rather than showing one name and a net of 2."""
    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    base = _quiet_days("NVDA", 4)
    px = base[-1].close
    bars = list(base) + [_bar("NVDA", _slot(k), px, px + 0.03, px - 0.03, px) for k in range(15)]
    bars.append(_bar("NVDA", _slot(15), px, px + 5, px - 0.01, px + 4.8))
    p2 = bars[-1].close
    bars.append(_bar("NVDA", _slot(16), p2, p2 + 5, p2 - 0.01, p2 + 4.8))
    basket = {"NVDA": bars}
    block = block_series(basket, now)
    assert block[_slot(15)] == 1 and block[_slot(16)] == 1
    tiles = LeaderService._tiles(basket, block, now)
    nvda = next(t for t in tiles["tiles"] if t["symbol"] == "NVDA")
    assert nvda["side"] == 1 and nvda["count"] == 2
    assert tiles["up"] == 2 and tiles["net"] == 2
    assert tiles["up"] - tiles["down"] == tiles["net"]
    assert tiles["flat"] == len(LD.BASKET) - 1


@pytest.mark.asyncio
async def test_the_leader_card_and_the_verdict_read_the_same_block(tmp_path):
    """The card's 'block now' is what the next signal would read: the last
    two *confirmed* bars, never the forming one."""
    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, _ = _service(tmp_path, _FakeClient(), now)
    b = await svc.board()
    for ld in b["leaders"]:
        assert ld["net_now"] == b["block"]["net"]


@pytest.mark.asyncio
async def test_the_contract_stays_todays_after_half_past_three(tmp_path):
    """The rule is flat by the bell, so the card must never quote tomorrow's
    contract for a trade held in today's."""
    late = datetime(DAY.year, DAY.month, DAY.day, 15, 40, tzinfo=NY).astimezone(UTC)
    client = _FakeClient()
    svc, _ = _service(tmp_path, client, late)
    bars = mirsad9.resample(await client.range_minute_bars("QQQ", 1, None, None), 5)
    async with svc.desk._client() as c:
        out = await svc._contract(c, "QQQ", 1, bars[-1].close, None, 1.0, bars[-1].close, late)
    assert out["expiry"] == DAY.isoformat() and out["expiry_text"] == "ينتهي اليوم"


@pytest.mark.asyncio
async def test_a_close_decided_on_the_forming_bar_is_not_recorded_yet(tmp_path):
    """A target reached on a bar that is still forming can still become a
    stop before it closes: nothing is written until the bar is done."""
    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, mem = _service(tmp_path, _FakeClient(), now)
    p = Opportunity(symbol="QQQ", n=1, side=1, net=3, signal_i=1, signal_ts=_slot(10),
                    atr=1.0, level=100.0, sig_hi=100.5, sig_lo=99.8, trend=1, grade="أ")
    p.fill(2, _slot(11), 100.0, True)
    p.close(4, _slot(13), 101.0, "target")     # the 12:35 bar, still forming at 12:37
    svc._record([p], _slot(13) + timedelta(minutes=2))
    assert mem.leader_trades_between(DAY, DAY) == []
    svc._record([p], _slot(13) + timedelta(minutes=6))  # the bar has closed
    rows = mem.leader_trades_between(DAY, DAY)
    assert len(rows) == 1 and rows[0]["how"] == "target"
    # …and it is never written twice, however often the board rebuilds
    svc._record([p], _slot(13) + timedelta(minutes=30))
    assert len(mem.leader_trades_between(DAY, DAY)) == 1


def test_the_live_record_is_read_once_a_day_and_after_a_new_trade(tmp_path):
    now = datetime(DAY.year, DAY.month, DAY.day, 11, 0, tzinfo=NY).astimezone(UTC)
    svc, mem = _service(tmp_path, _FakeClient(), now)
    reads = []
    real = mem.leader_trades_between
    mem.leader_trades_between = lambda a, b: (reads.append(1), real(a, b))[1]
    today = DAY
    assert svc._live_record(today)["trades"] == 0
    svc._live_record(today)
    assert len(reads) == 1  # the second refresh reads the cache
    p = Opportunity(symbol="QQQ", n=1, side=1, net=3, signal_i=1, signal_ts=_slot(10, DAY - timedelta(days=1)),
                    atr=1.0, level=100.0, sig_hi=100.5, sig_lo=99.8, trend=1, grade="أ")
    p.fill(2, _slot(11, DAY - timedelta(days=1)), 100.0, True)
    p.close(4, _slot(13, DAY - timedelta(days=1)), 101.0, "target")
    svc._record([p], now)
    assert svc._live_record(today)["trades"] == 1 and len(reads) == 2


# ---------------------------------------------------------------- memory & links
def test_leader_trades_are_recorded_once(tmp_path):
    mem = Memory(tmp_path / "m.db")
    p = Opportunity(symbol="QQQ", n=1, side=1, net=3, signal_i=1, signal_ts=_slot(10), atr=1.0, level=100.0, sig_hi=100.5, sig_lo=99.8, trend=1, grade="أ")
    p.fill(2, _slot(11), 100.0, True)
    p.close(4, _slot(13), 101.0, "target")
    row = p.as_dict()
    row["day"] = p.day.isoformat()
    assert mem.record_leader_trade(row) is True
    assert mem.record_leader_trade(row) is False
    rows = mem.leader_trades_between(DAY, DAY)
    assert len(rows) == 1 and rows[0]["how"] == "target" and rows[0]["r"] == pytest.approx(0.67, abs=0.01)
    assert mem.leader_trades_between(DAY + timedelta(days=1), DAY + timedelta(days=2)) == []


def test_the_screens_have_an_address_even_when_nobody_set_one(monkeypatch):
    """A subscriber asking for their screen must not be told the platform is
    not configured because a variable nobody knew about is empty."""
    from qqq_alpha.config import Settings

    monkeypatch.delenv("PUBLIC_BASE_URL", raising=False)
    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "example.up.railway.app")
    assert Settings().public_base_url == "https://example.up.railway.app"
    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "https://example.up.railway.app/")
    assert Settings().public_base_url == "https://example.up.railway.app"
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://chosen.example")
    assert Settings().public_base_url == "https://chosen.example"  # an explicit value wins
    monkeypatch.delenv("PUBLIC_BASE_URL")
    monkeypatch.delenv("RAILWAY_PUBLIC_DOMAIN")
    assert Settings().public_base_url == ""


def test_leader_link_and_bot_words(tmp_path):
    settings = _settings(tmp_path)
    mem = Memory(settings.data_dir / "memory.db")
    desk = DeskService(settings, mem, client_factory=lambda: _FakeClient())
    link = desk.link_for("1", "leader")
    assert link.startswith("https://desk.example/desk/login?k=") and link.endswith("&next=leader")
    assert "next=" not in desk.link_for("1")
    assert LiveEngine._wants_leader("القائد") and LiveEngine._wants_leader("قائد اليوم") and LiveEngine._wants_leader("/leader")
    assert LiveEngine._wants_desk("القائد") and not LiveEngine._wants_leader("شاشتي")


# ---------------------------------------------------------------- the web
class _FakeLeader:
    async def ticks(self):
        return {"now": datetime.now(UTC).isoformat(), "prices": {"QQQ": {"price": 708.31}}, "open": {}}

    async def board(self):
        return {"now": datetime.now(UTC).isoformat(), "session": {"open": False, "text": "x", "phase": "closed"},
                "verdict": {"tone": "off", "title": "t", "text": "", "chips": []}, "leaders": [], "block": {"tiles": [], "up": 0, "down": 0, "flat": 0, "net": 0},
                "morning": {"steps": []}, "journal": {"today": [], "recent": [], "live": {}}, "notes": [], "measured": LD.MEASURED, "errors": []}


class _FakeDesk:
    def __init__(self, mem):
        self.memory = mem

    def settings_for(self, chat_id):
        return {"symbols": ["QQQ"], "frame": 3, "expiry": "auto"}

    def has_access(self, chat_id, now=None):
        return chat_id in {"1", "777"}


def _web(tmp_path):
    settings = _settings(tmp_path)
    mem = Memory(settings.data_dir / "memory.db")
    now = datetime.now(UTC)
    mem.add_subscriber("1", "u", "Ahmed", now, now + timedelta(days=3))
    mem.add_subscriber("2", "v", "Sara", now - timedelta(days=9), now - timedelta(days=1))
    app = create_app(settings, desk=_FakeDesk(mem), leader=_FakeLeader())
    return TestClient(app, follow_redirects=False), mem


def test_leader_routes_are_gated_like_the_desk(tmp_path):
    client, mem = _web(tmp_path)
    assert client.get("/leader").status_code == 200 and "شاشتي" in client.get("/leader").text
    assert client.get("/api/leader").status_code == 401
    token = mem.issue_desk_token("1", datetime.now(UTC))
    r = client.get(f"/desk/login?k={token}&next=leader")
    assert r.status_code == 303 and r.headers["location"] == "/leader"
    client.cookies.set("mirsad_desk", token)
    page = client.get("/leader")
    assert page.status_code == 200 and "قائد اليوم" in page.text and "Ahmed" in page.text
    api = client.get("/api/leader")
    assert api.status_code == 200 and api.json()["verdict"]["title"] == "t"
    # the price line the page polls every second is gated exactly like the board
    tick = client.get("/api/leader/tick")
    assert tick.status_code == 200 and tick.json()["prices"]["QQQ"]["price"] == 708.31
    expired = mem.issue_desk_token("2", datetime.now(UTC))
    client.cookies.set("mirsad_desk", expired)
    assert client.get("/leader").status_code == 403
    assert client.get("/api/leader").status_code == 401
    assert client.get("/api/leader/tick").status_code == 401
    client.cookies.clear()
    assert client.get("/api/leader/tick").status_code == 401
    assert client.get("/leader/preview").status_code == 401
    assert client.get("/leader/preview", auth=("admin", "secret")).status_code == 200
    assert client.get("/api/leader", auth=("admin", "secret")).status_code == 200
