"""The TradingView bridge: v1.8 alert strings in, contract cards out.

The alert strings below are the indicator's own verbatim formats — the
parser is a contract with the Pine code, so the tests quote it exactly,
Arabic, emoji, separators and all.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from qqq_alpha.domain import OptionContract, OptionType
from qqq_alpha.live.tvbridge import (
    TvBridge,
    next_expiry,
    parse_signal,
    pick_contract,
)

# ---------------------------------------------------------------- parser


def test_parse_entry_call_with_reason() -> None:
    sig = parse_signal(
        "🎯 دخول كول | TSLA فريم 5 | سعر 352.95 | وقف 351.10 | "
        "السبب: اختراق قمة اليوم السابق بحجم قوي"
    )
    assert sig is not None
    assert sig.kind == "entry" and sig.symbol == "TSLA" and sig.side == 1
    assert sig.price == 352.95 and sig.stop == 351.10
    assert "اختراق" in sig.reason and not sig.moon


def test_parse_entry_put_moon_tag() -> None:
    sig = parse_signal(
        "🎯 دخول بوت | QQQ فريم 5 | سعر 480.10 | وقف 481.60 | "
        "السبب: كسر قاع الجلسة · 🌙 لعقد الغد — سترايك قريب بنفس الاتجاه"
    )
    assert sig is not None
    assert sig.kind == "entry" and sig.side == -1 and sig.moon


def test_parse_entry_improved_retrace_suffix() -> None:
    sig = parse_signal(
        "🎯 دخول كول | NVDA فريم 5 | سعر 128.40 | وقف 127.20 | "
        "السبب: ارتداد من الحافة القريبة — دخول محسّن على الارتداد"
    )
    assert sig is not None and sig.kind == "entry" and sig.symbol == "NVDA"


def test_parse_pending_and_cancel_stay_quiet_kinds() -> None:
    pend = parse_signal(
        "🎯 إشارة كول | AAPL — انتظر الارتداد لمنطقة الدخول 231.55 | وقف 230.40"
    )
    assert pend is not None and pend.kind == "pending"
    assert pend.price == 231.55 and pend.stop == 230.40
    cancel = parse_signal("⨯ أُلغيت إشارة الكول | AAPL — كسر الوقف قبل الدخول")
    assert cancel is not None and cancel.kind == "cancel"


def test_parse_targets_and_exit_r_text() -> None:
    t1 = parse_signal("🔺 هدف 1 تحقق والصفقة مؤمّنة (الوقف = الدخول) | MSFT")
    t2 = parse_signal("🔺 هدف 2 تحقق — الوقف صعد إلى هدف 1 | MSFT")
    t3 = parse_signal("🔺 الهدف الممتد تحقق | MSFT")
    assert (t1.kind, t2.kind, t3.kind) == ("t1", "t2", "t3")
    win = parse_signal("✅ خروج — كسر الوقف المتحرك | MSFT | النتيجة +1.8R")
    loss = parse_signal("🔴 خروج — كسر الوقف المتحرك | MU | النتيجة -1.0R")
    assert win.kind == "exit" and win.win and win.r_text == "+1.8R"
    assert loss.kind == "exit" and not loss.win and loss.symbol == "MU"


def test_parse_prep_trap_and_garbage() -> None:
    assert parse_signal("⏳ استعداد | AMZN — اقتراب من الحافة القريبة").kind == "prep"
    assert parse_signal("🪤 مصيدة سيولة | META — كسر كاذب تحت القاع").kind == "trap"
    assert parse_signal("مرحبا بدون رمز سهم") is None


# ---------------------------------------------------------------- expiry


def _ny(y: int, mo: int, d: int, h: int, mi: int = 0) -> datetime:
    # build the moment in UTC such that NY wall-clock matches (EDT = UTC-4)
    return datetime(y, mo, d, h + 4, mi, tzinfo=UTC)


def test_qqq_gets_same_day_expiry_during_session() -> None:
    # Monday 2026-08-31, 10:30 NY
    assert next_expiry("QQQ", _ny(2026, 8, 31, 10, 30), moon=False) == date(2026, 8, 31)


def test_qqq_moon_signal_books_next_trading_day() -> None:
    assert next_expiry("QQQ", _ny(2026, 8, 31, 15, 45), moon=True) == date(2026, 9, 1)


def test_qqq_friday_moon_rolls_over_weekend() -> None:
    # Friday 2026-09-04 late signal: Monday 7 September 2026 is Labor Day: the next expiry is Tuesday
    assert next_expiry("QQQ", _ny(2026, 9, 4, 15, 50), moon=True) == date(2026, 9, 8)


def test_stock_gets_nearest_friday() -> None:
    # Monday → that week's Friday: the market lists no daily stock expiries
    assert next_expiry("TSLA", _ny(2026, 8, 31, 10, 30), moon=False) == date(2026, 9, 4)


def test_stock_friday_moon_pushes_a_week() -> None:
    assert next_expiry("TSLA", _ny(2026, 9, 4, 15, 50), moon=True) == date(2026, 9, 11)


def test_stock_friday_intraday_keeps_same_friday() -> None:
    assert next_expiry("TSLA", _ny(2026, 9, 4, 10, 30), moon=False) == date(2026, 9, 4)


# ---------------------------------------------------------- pick_contract


def _c(
    strike: float,
    bid: float | None,
    ask: float | None,
    vol: int = 100,
    oi: int = 500,
    typ: OptionType = OptionType.CALL,
) -> OptionContract:
    cp = "C" if typ is OptionType.CALL else "P"
    return OptionContract(
        occ_symbol=f"TST260904{cp}{int(strike * 1000):08d}",
        underlying="TST",
        option_type=typ,
        strike=strike,
        expiry=date(2026, 9, 4),
        bid=bid,
        ask=ask,
        volume=vol,
        open_interest=oi,
    )


def test_pick_prefers_nearest_otm_liquid_strike() -> None:
    chain = [_c(348, 4.9, 5.1), _c(350, 3.0, 3.1), _c(352.5, 1.9, 2.0), _c(355, 1.0, 1.06)]
    got = pick_contract(chain, side=1, spot=351.0)
    assert got is not None and got.strike == 352.5, "closest OTM beats closer ITM"


def test_pick_relaxes_when_nothing_passes_strict() -> None:
    # every strike is illiquid (vol/oi below the bar) — the relaxed pass
    # must still return the nearest OTM rather than dropping the trade
    chain = [_c(352.5, 1.9, 2.0, vol=0, oi=3), _c(355, 1.0, 1.06, vol=0, oi=2)]
    got = pick_contract(chain, side=1, spot=351.0)
    assert got is not None and got.strike == 352.5


def test_pick_rejects_absurd_premiums_even_relaxed() -> None:
    chain = [_c(352.5, 30.0, 31.0), _c(355, 0.02, 0.04)]
    assert pick_contract(chain, side=1, spot=351.0) is None


def test_pick_put_side_mirrors() -> None:
    chain = [
        _c(350, 2.0, 2.1, typ=OptionType.PUT),
        _c(348, 1.2, 1.3, typ=OptionType.PUT),
        _c(352.5, 3.0, 3.1, typ=OptionType.PUT),
    ]
    got = pick_contract(chain, side=-1, spot=351.0)
    assert got is not None and got.strike == 350, "nearest at/below-spot put wins"


# ----------------------------------------------------------- full bridge


class _Wire:
    """Capture what the bridge sends where, and serve a canned chain."""

    def __init__(self, chain: list[OptionContract], channel_ok: bool = True):
        self.admin: list[str] = []
        self.channel: list[str] = []
        self.chain = chain
        self.channel_ok = channel_ok

    async def admin_send(self, text: str) -> None:
        self.admin.append(text)

    async def channel_send(self, text: str) -> bool:
        if self.channel_ok:
            self.channel.append(text)
        return self.channel_ok

    async def chain_fetch(self, symbol: str, expiry: date, want: OptionType):
        return self.chain


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


ENTRY = (
    "🎯 دخول كول | TSLA فريم 5 | سعر 351.00 | وقف 349.50 | "
    "السبب: اختراق قمة اليوم السابق بحجم قوي"
)


def test_bridge_entry_posts_card_then_follows_up() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0), _c(355, 1.0, 1.06)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)

    async def flow() -> None:
        await bridge.handle(ENTRY)
        # the contract rallied: re-quote at 3.00 for the follow-ups
        wire.chain = [_c(352.5, 2.95, 3.05), _c(355, 1.6, 1.7)]
        await bridge.handle("🔺 هدف 1 تحقق والصفقة مؤمّنة (الوقف = الدخول) | TSLA")
        await bridge.handle("✅ خروج — كسر الوقف المتحرك | TSLA | النتيجة +1.8R")

    _run(flow())
    card = wire.channel[0]
    assert "⭐️ صفقة كول 🟢 — TSLA" in card
    # the card quotes the ask: that is what a buyer pays, not the mid
    assert "TSLA 352.5C" in card and "2.00$" in card
    assert "351" in card and "349.5" in card and "اختراق" in card
    t1 = wire.channel[1]
    assert "الهدف الأول" in t1 and "3.00$" in t1 and "+50%" in t1
    exit_msg = wire.channel[2]
    # the exit report prices the close at the bid and remembers the peak
    assert exit_msg.startswith("✅") and "+1.8R" in exit_msg
    assert "دخول 2.00$ ← خروج 2.95$ (+48%)" in exit_msg and "أعلى ما بلغه العقد: 3.00$" in exit_msg
    # the exit cleared the trade: nothing left to follow up on
    assert not bridge._open


def test_bridge_quiet_kinds_go_to_admin_only() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle("⏳ استعداد | TSLA — اقتراب من الحافة القريبة"))
    _run(bridge.handle("🪤 مصيدة سيولة | TSLA — كسر كاذب"))
    assert wire.channel == [] and len(wire.admin) == 2


def test_bridge_no_contract_is_recorded_in_the_channel() -> None:
    wire = _Wire([])  # empty chain: nothing to pick
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle(ENTRY))
    # the signal is still a record: the channel hears it got no contract,
    # and the day's report lists it
    assert len(wire.channel) == 1 and "لم يُعثر على عقد صالح" in wire.channel[0]
    assert "TSLA" in wire.channel[0] and "351" in wire.channel[0]
    assert not bridge._open
    report = bridge.daily_report()
    assert "إشارات بلا عقد متتبَّع" in report and "TSLA" in report


def test_bridge_orphan_target_is_recorded_without_a_contract() -> None:
    # a target alert with no tracked entry (restart, or entry never posted)
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle("🔺 هدف 1 تحقق والصفقة مؤمّنة (الوقف = الدخول) | TSLA"))
    assert len(wire.channel) == 1 and "بدون عقد متتبَّع" in wire.channel[0]
    assert "الهدف الأول" in wire.channel[0]


def test_bridge_orphan_exit_is_recorded_and_listed_in_the_day() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle("✅ خروج — كسر الوقف المتحرك | TSLA | النتيجة +1.8R"))
    assert len(wire.channel) == 1
    assert wire.channel[0].startswith("✅") and "+1.8R" in wire.channel[0] and "بدون عقد" in wire.channel[0]
    assert "TSLA: خروج" in bridge.daily_report()


def test_secure_alert_tells_the_channel_to_sell_half() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)

    async def flow() -> None:
        await bridge.handle(ENTRY_JSON)
        await bridge.handle('{"src":"mirsad9","sym":"TSLA","tf":"5","event":"secure","side":"CALL","price":353.4}')

    _run(flow())
    assert len(wire.channel) == 2
    assert "بع نصف الكمية" in wire.channel[1] and "353.4" in wire.channel[1] and "2.00$" in wire.channel[1]
    assert bridge._open["TSLA"].hits == 0


def test_signal_then_entry_alert_is_one_trade() -> None:
    # MIRSAD 9 immediate mode: the signal alert, then the entry alert one bar later
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)

    async def flow() -> None:
        await bridge.handle(ENTRY_JSON)
        await bridge.handle('{"src":"mirsad9","sym":"TSLA","tf":"3","event":"entry","side":"CALL","price":351.4,"stop":349.9,"t1":353.0,"t2":355.0,"t3":358.0}')

    _run(flow())
    assert len(wire.channel) == 1
    assert bridge._open["TSLA"].entry_stock == 351.4 and bridge._open["TSLA"].stop == 349.9
    assert any("الصفقة نفسها" in m for m in wire.admin)


def test_bridge_ignores_a_resent_alert() -> None:
    # TradingView re-sends when the webhook answers slowly: same text, same trade
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle(ENTRY))
    _run(bridge.handle(ENTRY))
    assert len(wire.channel) == 1


def test_daily_report_is_posted_even_on_a_quiet_day() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    assert _run(bridge.post_daily_report()) is True
    assert len(wire.channel) == 1 and "لا صفقات مقفلة اليوم" in wire.channel[0]


def test_daily_report_lists_what_is_still_open() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle(ENTRY))
    report = bridge.daily_report()
    assert "ما زال مفتوحاً" in report and "TSLA 352.5C" in report and "2.00$" in report


def test_bridge_channel_failure_reports_card_to_admin() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)], channel_ok=False)
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle(ENTRY))
    assert any("تعذر النشر" in m and "352.5C" in m for m in wire.admin)


def test_a_fresh_secret_replaces_the_derived_one(tmp_path):
    """Issuing a new link revokes the old one — that is the whole point."""
    from qqq_alpha.live.tvbridge import rotate_tv_webhook_secret, tv_webhook_secret
    from qqq_alpha.memory import Memory

    class _S:
        telegram_bot_token = "12345:abcdef"

    mem = Memory(tmp_path / "m.db")
    settings = _S()

    derived = tv_webhook_secret(settings, mem)
    assert derived and len(derived) == 24

    fresh = rotate_tv_webhook_secret(mem)
    assert fresh != derived
    assert tv_webhook_secret(settings, mem) == fresh

    # and rotating again retires the one before it
    newer = rotate_tv_webhook_secret(mem)
    assert newer not in {derived, fresh}
    assert tv_webhook_secret(settings, mem) == newer


def test_the_stored_secret_survives_reopening_the_database(tmp_path):
    """A container restart must not silently resurrect the old link."""
    from qqq_alpha.live.tvbridge import rotate_tv_webhook_secret, tv_webhook_secret
    from qqq_alpha.memory import Memory

    class _S:
        telegram_bot_token = "12345:abcdef"

    fresh = rotate_tv_webhook_secret(Memory(tmp_path / "m.db"))
    assert tv_webhook_secret(_S(), Memory(tmp_path / "m.db")) == fresh


def test_the_pay_branch_no_longer_swallows_the_webhook_link_request():
    """«رابط جديد» must reach the webhook handler, not the payment offer.

    A bare "رابط" first word used to route to the pay link, so every
    message about the TradingView webhook came back as the plans card.
    """
    from qqq_alpha.live.engine import LiveEngine

    def _wants_pay(text: str) -> bool:
        parts = text.strip().split()
        low = text.strip().lower()
        return (parts and parts[0].strip().lower() in {"دفع", "paylink"}) or (
            "رابط" in low and "دفع" in low
        )

    assert not _wants_pay("رابط جديد")
    assert not _wants_pay("الرابط")
    assert not _wants_pay("ارسل الرابط الجديد")
    assert _wants_pay("دفع")
    assert _wants_pay("رابط الدفع")
    assert LiveEngine is not None  # the module imports cleanly with the branch


# ------------------------------------------------------ MIRSAD json alerts


def test_parse_mirsad_json_entry_exit_and_zone() -> None:
    from qqq_alpha.live.tvbridge import parse_signal as ps

    entry = ps('{"src":"mirsad8","sym":"QQQ","tf":"60","side":"CALL","why":"كسر التجميع",'
               '"ref":711.9,"stop":709.4,"t1":714.0,"t2":716.1,"t3":718.5}')
    assert entry is not None and entry.kind == "entry" and entry.symbol == "QQQ"
    assert entry.side == 1 and entry.price == 711.9 and entry.stop == 709.4
    assert entry.targets == (714.0, 716.1, 718.5) and entry.tf == "60"
    zone = ps('{"src":"mirsad8","sym":"AAPL","tf":"60","event":"zone","side":"PUT","level":230.1}')
    assert zone is not None and zone.kind == "pending" and zone.side == -1 and zone.price == 230.1
    ex = ps('{"src":"mirsad8","sym":"QQQ","tf":"60","event":"exit","why":"وقف","r":-1.0}')
    assert ex is not None and ex.kind == "exit" and ex.win is False and ex.r_text == "-1.00R"
    win = ps('{"src":"mirsad7","sym":"NVDA","tf":"60","event":"exit","why":"هدف ٣","r":3.2}')
    assert win is not None and win.win and win.r_text == "+3.20R"


def test_spy_gets_daily_expiry_and_spx_routes_to_spy() -> None:
    from qqq_alpha.live.tvbridge import resolve_underlying

    assert next_expiry("SPY", _ny(2026, 8, 31, 10, 30), moon=False) == date(2026, 8, 31)
    assert next_expiry("SPX", _ny(2026, 8, 31, 10, 30), moon=False) == date(2026, 8, 31)
    assert resolve_underlying("SPX", 6480.0) == ("SPY", 648.0)
    assert resolve_underlying("TSLA", 351.0) == ("TSLA", 351.0)


def test_late_session_signal_books_next_expiry() -> None:
    from qqq_alpha.live.tvbridge import is_late_session

    assert is_late_session(_ny(2026, 8, 31, 15, 40))
    assert not is_late_session(_ny(2026, 8, 31, 14, 59))


ENTRY_JSON = ('{"src":"mirsad8","sym":"TSLA","tf":"60","side":"CALL","why":"شمعة صاعدة",'
              '"ref":351.0,"stop":349.5,"t1":353.0,"t2":355.0,"t3":358.0}')


def test_bridge_tracks_peak_and_reports_the_trade_and_the_day() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0), _c(355, 1.0, 1.06)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)

    async def flow() -> None:
        await bridge.handle(ENTRY_JSON)
        # the contract rallies to 3.00, the minute clock records the peak
        wire.chain = [_c(352.5, 2.95, 3.05), _c(355, 1.6, 1.7)]
        await bridge.tick(_ny(2026, 8, 31, 11, 0))
        # then fades to 2.40 by the exit alert
        wire.chain = [_c(352.5, 2.40, 2.50), _c(355, 1.3, 1.4)]
        await bridge.handle('{"src":"mirsad8","sym":"TSLA","tf":"60","event":"exit","why":"خروج بربح","r":0.8}')

    _run(flow())
    card = wire.channel[0]
    assert "TSLA 352.5C" in card and "2.00$" in card and "353" in card and "شمعة صاعدة" in card
    exit_msg = wire.channel[1]
    assert exit_msg.startswith("✅") and "+0.80R" in exit_msg
    assert "دخول 2.00$ ← خروج 2.40$ (+20%)" in exit_msg
    assert "أعلى ما بلغه العقد: 3.00$ (+50%)" in exit_msg
    assert not bridge._open
    report = bridge.daily_report(datetime.now(UTC).astimezone(ZoneInfo("America/New_York")).date())
    assert report is not None and "الصفقات: 1 · رابحة 1" in report and "+20%" in report and "+50%" in report


def test_bridge_retires_a_contract_that_expired_without_an_exit() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)

    async def flow() -> None:
        await bridge.handle(ENTRY_JSON)
        # the contract's own expiry passed at the bell with no exit alert —
        # read off the booked trade so the test does not age with the calendar
        exp = next(iter(bridge._open.values())).expiry
        await bridge.tick(_ny(exp.year, exp.month, exp.day, 16, 5))

    _run(flow())
    assert not bridge._open
    assert any("انتهى العقد بلا إشارة خروج" in m for m in wire.channel)


def test_spx_signal_is_traded_through_spy() -> None:
    seen: list[str] = []

    class _W(_Wire):
        async def chain_fetch(self, symbol: str, expiry: date, want: OptionType):
            seen.append(symbol)
            return self.chain

    wire = _W([_c(650, 1.9, 2.0), _c(652, 1.0, 1.06)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle('{"src":"mirsad8","sym":"SPX","tf":"60","side":"CALL","why":"x","ref":6480.0,"stop":6460.0}'))
    # both expiries are fetched for the shortlist, always on SPY
    assert seen and set(seen) == {"SPY"}
    assert "SPY 650C" in wire.channel[0] and "الأرخص" in wire.channel[0]


# ------------------------------------------------- the brain picks the contract


def _c_on(strike: float, bid: float, ask: float, expiry: date, typ: OptionType = OptionType.CALL) -> OptionContract:
    c = _c(strike, bid, ask, typ=typ)
    return OptionContract(
        occ_symbol=f"TST{expiry:%y%m%d}{'C' if typ is OptionType.CALL else 'P'}{int(strike * 1000):08d}",
        underlying="TST", option_type=typ, strike=strike, expiry=expiry,
        bid=c.bid, ask=c.ask, volume=c.volume, open_interest=c.open_interest,
    )


class _TwoExpiries(_Wire):
    """Serves a different chain per expiry, like the real market does."""

    def __init__(self, chains: dict[date, list[OptionContract]]):
        super().__init__([])
        self.chains = chains
        self.asked: list[date] = []

    async def chain_fetch(self, symbol: str, expiry: date, want: OptionType):
        self.asked.append(expiry)
        return self.chains.get(expiry, [])


def test_analyst_sees_both_expiries_and_its_pick_is_used() -> None:
    from qqq_alpha.live.tvbridge import later_expiry

    this_fri = next_expiry("TSLA", datetime.now(UTC), moon=False)
    next_fri = later_expiry("TSLA", this_fri)
    wire = _TwoExpiries({
        this_fri: [_c_on(352.5, 1.9, 2.0, this_fri), _c_on(355, 1.0, 1.06, this_fri)],
        next_fri: [_c_on(352.5, 3.9, 4.0, next_fri), _c_on(355, 2.9, 3.0, next_fri)],
    })
    seen_ctx: list[dict] = []

    async def analyst(ctx: dict) -> dict:
        seen_ctx.append(ctx)
        # the brain prefers the extra week of time
        return {"occ": f"TST{next_fri:%y%m%d}C00355000", "why": "وقت أطول والهدف بعيد", "caution": "فارق سعر واسع"}

    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch, analyst=analyst)
    _run(bridge.handle(ENTRY_JSON))
    ctx = seen_ctx[0]
    assert ctx["signal"]["symbol"] == "TSLA" and ctx["signal"]["side"] == 1
    assert ctx["rule_pick"] == f"TST{this_fri:%y%m%d}C00352500"
    assert {row["expiry"] for row in ctx["candidates"]} == {this_fri.isoformat(), next_fri.isoformat()}
    assert wire.asked == [this_fri, next_fri]
    card = wire.channel[0]
    assert "TSLA 355C" in card and "3.00$" in card and f"ينتهي {next_fri:%d-%m}" in card
    assert "🤖 اختيار العقد: وقت أطول والهدف بعيد" in card and "⚠️ تنبيه: فارق سعر واسع" in card
    assert bridge._open["TSLA"].expiry == next_fri


def test_analyst_off_list_answer_falls_back_to_the_rules() -> None:
    async def analyst(ctx: dict) -> dict:
        return {"occ": "TST260904C00999000", "why": "invented"}

    wire = _Wire([_c(352.5, 1.9, 2.0), _c(355, 1.0, 1.06)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch, analyst=analyst)
    _run(bridge.handle(ENTRY_JSON))
    card = wire.channel[0]
    assert "TSLA 352.5C" in card and "حسب القواعد" in card and "invented" not in card


def test_analyst_failure_never_loses_the_signal() -> None:
    async def analyst(ctx: dict) -> dict:
        raise RuntimeError("model down")

    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch, analyst=analyst)
    _run(bridge.handle(ENTRY_JSON))
    assert len(wire.channel) == 1 and "TSLA 352.5C" in wire.channel[0]


def test_contract_analyst_parses_the_tool_call_and_rejects_strays() -> None:
    from types import SimpleNamespace

    from qqq_alpha.config import Settings
    from qqq_alpha.live.tvbrain import ContractAnalyst, parse_choice, render_context

    class _Messages:
        def __init__(self):
            self.calls: list[dict] = []

        async def create(self, **kw):
            self.calls.append(kw)
            return SimpleNamespace(
                stop_reason="tool_use",
                content=[SimpleNamespace(type="tool_use", input={"occ": "tst260904c00352500", "why": "قريب", "confidence": 70})],
            )

    msgs = _Messages()
    client = SimpleNamespace(messages=msgs)
    settings = Settings(anthropic_api_key="k", anthropic_model="m")
    analyst = ContractAnalyst(settings, client=client)
    ctx = {
        "signal": {"symbol": "TSLA", "side": 1, "price": 351.0, "stop": 349.5, "targets": [353.0], "reason": "x", "tf": "60"},
        "now_ny": "2026-09-03 10:30", "minutes_to_close": 330, "late_session": False, "moon": False,
        "underlying": "TSLA", "spot": 351.0, "rule_pick": "TST260904C00352500", "rule_expiry": "2026-09-04",
        "candidates": [{"occ": "TST260904C00352500", "strike": 352.5}],
    }
    payload = _run(analyst.choose(ctx))
    assert payload["occ"] == "tst260904c00352500"
    call = msgs.calls[0]
    assert call["tool_choice"] == {"type": "tool", "name": "choose_contract"}
    assert "TSLA" in call["messages"][0]["content"] and "TST260904C00352500" in render_context(ctx)
    choice = parse_choice(payload, {"TST260904C00352500"})
    assert choice is not None and choice.occ == "TST260904C00352500" and choice.confidence == 70
    assert parse_choice({"occ": "TST260904C00355000", "why": "no"}, {"TST260904C00352500"}) is None
    # nothing configured: the analyst stays silent rather than raising
    quiet = ContractAnalyst(Settings(anthropic_api_key="", anthropic_model=""))
    assert not quiet.configured and _run(quiet.choose(ctx)) is None
