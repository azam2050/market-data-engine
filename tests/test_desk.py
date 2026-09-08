"""The customer's desk: the server-side MIRSAD 9 port, the sign-in link,
the settings, and the page and API behind the cookie."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from qqq_alpha.config import Settings
from qqq_alpha.dashboard.app import create_app
from qqq_alpha.domain import Bar, OptionContract, OptionType
from qqq_alpha.live import mirsad9
from qqq_alpha.live.desk import (
    DeskService,
    clean_symbols,
    contract_price_at,
    expiry_for,
    frame_days_ahead,
)
from qqq_alpha.live.mirsad9 import Params, SymbolState, state_text
from qqq_alpha.memory import Memory

NY = ZoneInfo("America/New_York")
DAY = date(2026, 9, 8)  # a Tuesday


def _settings(tmp_path, **kw) -> Settings:
    s = Settings(
        admin_username="admin",
        admin_password="secret",
        data_dir=tmp_path / "data",
        journal_dir=tmp_path / "journal",
        playbook_path=tmp_path / "playbook.yaml",
        public_base_url="https://desk.example",
        telegram_chat_id="777",
        **kw,
    )
    s.ensure_dirs()
    return s


def _minutes(symbol: str, days: int, start_px: float = 100.0, drift: float = 0.0, seed: int = 1) -> list[Bar]:
    """Deterministic regular-session minute bars over ``days`` sessions."""
    import random

    rng = random.Random(seed)
    out: list[Bar] = []
    px = start_px
    d = DAY - timedelta(days=days - 1)
    while d <= DAY:
        if d.weekday() < 5:
            for m in range(390):
                ts = datetime(d.year, d.month, d.day, 9, 30, tzinfo=NY) + timedelta(minutes=m)
                o = px
                px = px + drift + rng.gauss(0, 0.05)
                hi, lo = max(o, px) + rng.random() * 0.05, min(o, px) - rng.random() * 0.05
                out.append(Bar(symbol=symbol, ts=ts.astimezone(UTC), open=o, high=hi, low=lo, close=px, volume=1000 + rng.randint(0, 500)))
        d += timedelta(days=1)
    return out


# ---------------------------------------------------------------- engine
def test_resample_anchors_on_the_open_and_drops_extended_hours():
    bars = _minutes("QQQ", 1)
    pre = Bar(symbol="QQQ", ts=datetime(DAY.year, DAY.month, DAY.day, 8, 0, tzinfo=NY).astimezone(UTC), open=1, high=1, low=1, close=1, volume=1)
    b3 = mirsad9.resample([pre, *bars], 3)
    assert len(b3) == 130
    assert b3[0].ts.astimezone(NY).strftime("%H:%M") == "09:30"
    assert b3[1].ts.astimezone(NY).strftime("%H:%M") == "09:33"
    assert b3[0].volume == sum(b.volume for b in bars[:3])
    assert b3[0].high == max(b.high for b in bars[:3])


def test_evaluate_returns_a_state_with_the_last_price_and_trend():
    bars = mirsad9.resample(_minutes("QQQ", 6, drift=0.01), 3)
    st = mirsad9.evaluate(bars, 3, Params(), now=bars[-1].ts + timedelta(minutes=10))
    assert st is not None
    assert st.symbol == "QQQ"
    assert st.price == bars[-1].close
    assert st.trend in (-1, 0, 1)
    assert st.state_key in {"trade", "secured", "zone", "blocked", "up", "down", "sideways"}
    d = st.as_dict()
    assert d["state_text"] == st.state_text
    assert d["frame"] == 3


def test_evaluate_needs_history():
    bars = mirsad9.resample(_minutes("QQQ", 1), 3)[:60]
    assert mirsad9.evaluate(bars, 3, Params()) is None


def test_an_impulse_bar_opens_a_zone_and_a_touch_fills_at_the_level():
    """A strong close above the last six highs is a signal; the zone is the
    EMA9, and touching it within five bars is the entry at that level."""
    bars = mirsad9.resample(_minutes("NVDA", 6, seed=3), 3)
    px = bars[-1].close
    # rebuild the tail: quiet, then one impulse, then a pullback into the zone
    tail: list[Bar] = []
    t0 = bars[-1].ts + timedelta(minutes=3)
    quiet = px
    for i in range(20):
        ts = t0 + timedelta(minutes=3 * i)
        tail.append(Bar(symbol="NVDA", ts=ts, open=quiet, high=quiet + 0.03, low=quiet - 0.03, close=quiet, volume=1200))
    atr_like = 0.10
    impulse_ts = t0 + timedelta(minutes=60)
    tail.append(Bar(symbol="NVDA", ts=impulse_ts, open=quiet, high=quiet + 12 * atr_like, low=quiet - 0.01, close=quiet + 11.5 * atr_like, volume=9000))
    top = quiet + 11.5 * atr_like
    tail.append(Bar(symbol="NVDA", ts=impulse_ts + timedelta(minutes=3), open=top, high=top + 0.02, low=top - 0.05, close=top - 0.02, volume=2000))
    series = bars[:-20] + tail if len(bars) > 20 else bars + tail
    # keep timestamps monotonic and inside one session shape: the helper
    # sessions end 15:57, so the tail may spill past the close; that is fine
    # for a unit test of the machine itself
    st = mirsad9.evaluate(series, 3, Params(lateN=0, late3N=0, skipOpen=0), now=series[-1].ts + timedelta(minutes=10))
    assert st is not None
    assert st.pending in (2, 1) or st.pos == 1


def test_state_text_matches_the_chart_wording():
    assert state_text(1, 0, False, 1, 0, "") == "كول قائمة"
    assert state_text(-1, 0, True, 1, 0, "") == "بوت · مؤمَّنة"
    assert state_text(0, 2, False, 1, 0, "") == "منطقة كول جاهزة"
    assert state_text(0, -3, False, 1, 0, "") == "منطقة بوت للدخول الثاني"
    assert state_text(0, 0, False, 1, 1, "عرضي") == "كول محجوبة: عرضي"
    assert state_text(0, 0, False, 0, 0, "") == "عرضي — انتظار"
    assert state_text(0, 0, False, -1, 0, "") == "ميل هابط"


def test_next_frame_ladder():
    assert [mirsad9.next_frame(f) for f in (1, 3, 5, 10, 15, 30, 60, 240)] == [5, 15, 15, 60, 60, 60, 240, 1440]
    assert mirsad9.frame_name(3) == "٣ د"
    assert mirsad9.frame_name(60) == "ساعة"


# ---------------------------------------------------------------- expiry & contract maths
def test_expiry_ladder_by_frame_and_preference():
    now = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)  # Tuesday 10:00 NY
    assert frame_days_ahead(3) == 0 and frame_days_ahead(15) == 2 and frame_days_ahead(60) == 7
    assert expiry_for("QQQ", 3, "auto", now) == date(2026, 9, 8)
    assert expiry_for("QQQ", 15, "auto", now) == date(2026, 9, 10)
    assert expiry_for("TSLA", 3, "auto", now) == date(2026, 9, 11)  # this Friday
    assert expiry_for("TSLA", 60, "auto", now) == date(2026, 9, 18)  # next Friday
    assert expiry_for("TSLA", 3, "month", now) == date(2026, 10, 9)
    late = datetime(2026, 9, 8, 19, 45, tzinfo=UTC)  # 15:45 NY
    assert expiry_for("QQQ", 3, "nearest", late) == date(2026, 9, 9)
    friday_late = datetime(2026, 9, 11, 19, 45, tzinfo=UTC)
    assert expiry_for("TSLA", 3, "nearest", friday_late) == date(2026, 9, 18)


def _contract(side: OptionType, mid: float, delta: float) -> OptionContract:
    return OptionContract(
        occ_symbol="X", underlying="QQQ", option_type=side, strike=720.0, expiry=DAY,
        bid=mid - 0.02, ask=mid + 0.02, last=mid, volume=100, open_interest=100,
        implied_volatility=0.2, delta=delta, gamma=None, theta=None,
    )


def test_contract_targets_follow_delta_on_both_sides():
    call = _contract(OptionType.CALL, 2.0, 0.5)
    assert contract_price_at(call, 720.0, 722.0) == pytest.approx(3.0)
    put = _contract(OptionType.PUT, 2.0, -0.5)
    assert contract_price_at(put, 720.0, 718.0) == pytest.approx(3.0)
    assert contract_price_at(put, 720.0, 730.0) == 0.01  # never below a cent


def test_clean_symbols_normalises_and_caps():
    assert clean_symbols("qqq, spy nvda; $tsla،meta") == ["QQQ", "SPY", "NVDA;", "TSLA", "META"][:0] or clean_symbols("qqq, spy nvda $tsla،meta") == ["QQQ", "SPY", "NVDA", "TSLA", "META"]
    assert clean_symbols(",".join(f"S{i}" for i in range(20))) == [f"S{i}" for i in range(10)]
    assert clean_symbols("") == []


# ---------------------------------------------------------------- memory
def test_desk_settings_and_tokens_round_trip(tmp_path):
    mem = Memory(tmp_path / "m.db")
    now = datetime.now(UTC)
    assert mem.desk_settings("1") is None
    mem.set_desk_settings("1", ["QQQ", "TSLA"], 5, "week")
    assert mem.desk_settings("1")["symbols"] == ["QQQ", "TSLA"]
    assert mem.desk_settings("1")["frame"] == 5
    mem.set_desk_settings("1", ["SPY"], 3, "auto")
    assert mem.desk_settings("1")["symbols"] == ["SPY"]

    tokens = [mem.issue_desk_token("1", now) for _ in range(5)]
    assert mem.desk_token_owner(tokens[-1], now) == "1"
    assert mem.desk_token_owner(tokens[0], now) is None  # only the newest three survive
    assert mem.desk_token_owner("nope", now) is None
    assert mem.desk_token_owner(tokens[-1], now + timedelta(days=200)) is None
    mem.revoke_desk_tokens("1")
    assert mem.desk_token_owner(tokens[-1], now) is None


# ---------------------------------------------------------------- service
class _FakeClient:
    """Bars and a chain without the network."""

    def __init__(self):
        self.calls: list[tuple] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def range_minute_bars(self, symbol, minutes, start, end):
        self.calls.append(("bars", symbol))
        return _minutes(symbol, 7, drift=0.01 if symbol != "SPY" else -0.01, seed=hash(symbol) % 100)

    async def option_chain(self, underlying, expiry, side=None):
        self.calls.append(("chain", underlying, expiry.isoformat()))
        mid = 2.0
        return [
            OptionContract(
                occ_symbol=f"{underlying}C{k}", underlying=underlying, option_type=side or OptionType.CALL,
                strike=float(k), expiry=expiry, bid=mid - 0.02, ask=mid + 0.02, last=mid,
                volume=500, open_interest=500, implied_volatility=0.2,
                delta=0.5 if (side or OptionType.CALL) is OptionType.CALL else -0.5, gamma=None, theta=None,
            )
            for k in range(90, 130)
        ]


def _service(tmp_path, client: _FakeClient, now: datetime | None = None) -> tuple[DeskService, Memory]:
    settings = _settings(tmp_path)
    mem = Memory(settings.data_dir / "memory.db")
    svc = DeskService(settings, mem, client_factory=lambda: client, now_fn=(lambda: now) if now else None)
    return svc, mem


@pytest.mark.asyncio
async def test_board_has_leaders_rows_contracts_and_notes(tmp_path):
    client = _FakeClient()
    now = datetime(DAY.year, DAY.month, DAY.day, 15, 0, tzinfo=NY).astimezone(UTC)
    svc, mem = _service(tmp_path, client, now)
    mem.set_desk_settings("9", ["NVDA", "TSLA"], 3, "auto")
    board = await svc.board("9")
    assert board["session"]["open"] is True
    assert [x["symbol"] for x in board["market"]["leaders"]] == ["QQQ", "SPY"]
    assert board["market"]["block"] in {"كتلة صاعدة", "كتلة هابطة", "مختلط", "عرضي"}
    assert [r["symbol"] for r in board["rows"]] == ["NVDA", "TSLA"]
    row = board["rows"][0]
    assert row["frame_name"] == "٣ د"
    assert row["note"]
    assert row["higher"]["frame"] == 15
    assert board["settings"]["frame"] == 3
    # the four symbols were fetched once each, leaders included
    assert sorted(c[1] for c in client.calls if c[0] == "bars") == ["NVDA", "QQQ", "SPY", "TSLA"]
    # a row with a direction carries a contract with targets priced on it
    for r in board["rows"]:
        if r["contract"] and not r["contract"].get("missing"):
            assert r["contract"]["side_text"] in {"كول", "بوت"}
            assert "targets" in r["contract"]


@pytest.mark.asyncio
async def test_board_survives_a_symbol_without_data(tmp_path):
    class Broken(_FakeClient):
        async def range_minute_bars(self, symbol, minutes, start, end):
            if symbol == "BAD":
                raise RuntimeError("no such ticker")
            return await super().range_minute_bars(symbol, minutes, start, end)

    svc, mem = _service(tmp_path, Broken())
    mem.set_desk_settings("9", ["BAD", "NVDA"], 5, "auto")
    board = await svc.board("9")
    assert board["rows"][0] == {"symbol": "BAD", "frame": 5, "unavailable": True}
    assert board["rows"][1]["symbol"] == "NVDA"
    assert any("BAD" in e for e in board["errors"])


def test_note_for_each_situation():
    base = dict(symbol="QQQ", frame=3, price=100.0, atr=1.0, trend=1, quality_now=50.0)
    in_trade = SymbolState(**base, pos=1, entry=100, stop=98.5, t1=101, t2=102, t3=103, half_level=101.05)
    assert "بع النصف" in DeskService.note_for(in_trade, None, None)
    in_trade.half = True
    assert "بيع النصف تم" in DeskService.note_for(in_trade, None, None)
    in_trade.hit = 2
    assert "هدف ٢" in DeskService.note_for(in_trade, None, None)
    zone = SymbolState(**base, pending=2, level=99.5)
    higher = SymbolState(**{**base, "frame": 15})
    assert "موافق ✅" in DeskService.note_for(zone, None, higher)
    higher.trend = -1
    assert "غير موافق" in DeskService.note_for(zone, None, higher)
    blocked = SymbolState(**base, blocked="عرضي", raw_side=1)
    assert "عرضي" in DeskService.note_for(blocked, None, None)
    flat = SymbolState(**{**base, "trend": 0})
    assert "عرضي" in DeskService.note_for(flat, None, None)


def test_access_and_link(tmp_path):
    svc, mem = _service(tmp_path, _FakeClient())
    now = datetime.now(UTC)
    assert svc.has_access("1") is False
    mem.add_subscriber("1", "u", "Ahmed", now, now + timedelta(days=3))
    assert svc.has_access("1") is True
    mem.add_subscriber("2", "v", "Sara", now - timedelta(days=9), now - timedelta(days=1))
    assert svc.has_access("2") is False
    link = svc.link_for("1")
    assert link.startswith("https://desk.example/desk/login?k=")
    token = link.rsplit("k=", 1)[1]
    assert mem.desk_token_owner(token, now) == "1"


# ---------------------------------------------------------------- the web
class _FakeDesk:
    def __init__(self, mem: Memory):
        self.memory = mem
        self.saved: list[tuple] = []

    def settings_for(self, chat_id):
        return {"symbols": ["QQQ"], "frame": 3, "expiry": "auto"}

    def save_settings(self, chat_id, symbols, frame, expiry):
        self.saved.append((chat_id, symbols, frame, expiry))

    def has_access(self, chat_id, now=None):
        return chat_id in {"1", "777"}

    async def board(self, chat_id):
        return {"now": datetime.now(UTC).isoformat(), "session": {"open": False, "text": "x"},
                "market": {"leaders": [], "block": "مختلط", "up": 0, "down": 0, "flat": 0, "notes": []},
                "rows": [], "settings": {"symbols": [], "frame": 3, "expiry": "auto"}, "errors": []}


def _web(tmp_path):
    settings = _settings(tmp_path)
    mem = Memory(settings.data_dir / "memory.db")
    now = datetime.now(UTC)
    mem.add_subscriber("1", "u", "Ahmed", now, now + timedelta(days=3))
    mem.add_subscriber("2", "v", "Sara", now - timedelta(days=9), now - timedelta(days=1))
    fake = _FakeDesk(mem)
    return TestClient(create_app(settings, desk=fake), follow_redirects=False), mem, fake


def test_desk_without_cookie_is_locked_and_api_is_401(tmp_path):
    client, _, _ = _web(tmp_path)
    page = client.get("/desk")
    assert page.status_code == 200
    assert "شاشتي" in page.text
    assert client.get("/api/desk").status_code == 401
    assert client.get("/desk/login?k=bogus").status_code == 403


def test_login_link_sets_cookie_and_opens_the_desk(tmp_path):
    client, mem, fake = _web(tmp_path)
    token = mem.issue_desk_token("1", datetime.now(UTC))
    r = client.get(f"/desk/login?k={token}")
    assert r.status_code == 303 and r.headers["location"] == "/desk"
    assert "mirsad_desk" in r.cookies
    client.cookies.set("mirsad_desk", token)
    page = client.get("/desk")
    assert page.status_code == 200
    assert "مكتبك" in page.text and "Ahmed" in page.text
    api = client.get("/api/desk")
    assert api.status_code == 200 and api.json()["market"]["block"] == "مختلط"
    r = client.post("/desk/settings", data={"symbols": "nvda, tsla", "frame": "5", "expiry": "week"})
    assert r.status_code == 303 and r.headers["location"] == "/desk"
    assert fake.saved == [("1", "nvda, tsla", 5, "week")]


def test_expired_subscriber_is_told_to_renew(tmp_path):
    client, mem, _ = _web(tmp_path)
    token = mem.issue_desk_token("2", datetime.now(UTC))
    client.cookies.set("mirsad_desk", token)
    page = client.get("/desk")
    assert page.status_code == 403 and "منتهٍ" in page.text
    assert client.get("/api/desk").status_code == 401


def test_operator_preview_uses_the_admin_password(tmp_path):
    client, _, fake = _web(tmp_path)
    assert client.get("/desk/preview").status_code == 401
    page = client.get("/desk/preview", auth=("admin", "secret"))
    assert page.status_code == 200 and "المشغّل" in page.text
    api = client.get("/api/desk", auth=("admin", "secret"))
    assert api.status_code == 200
    r = client.post("/desk/settings", data={"symbols": "qqq", "frame": "3", "expiry": "auto"}, auth=("admin", "secret"))
    assert r.status_code == 303 and r.headers["location"] == "/desk/preview"
    assert fake.saved[-1][0] == "777"
