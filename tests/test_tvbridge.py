"""The TradingView bridge: v1.8 alert strings in, contract cards out.

The alert strings below are the indicator's own verbatim formats — the
parser is a contract with the Pine code, so the tests quote it exactly,
Arabic, emoji, separators and all.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

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
    # Friday 2026-09-04 late signal → Monday 2026-09-07
    assert next_expiry("QQQ", _ny(2026, 9, 4, 15, 50), moon=True) == date(2026, 9, 7)


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
    assert "TSLA 352.5C" in card and "1.95$" in card
    assert "351" in card and "349.5" in card and "اختراق" in card
    t1 = wire.channel[1]
    assert "الهدف الأول" in t1 and "3.00$" in t1 and "+54%" in t1
    exit_msg = wire.channel[2]
    assert exit_msg.startswith("✅") and "+1.8R" in exit_msg and "أقفل على" in exit_msg
    # the exit cleared the trade: nothing left to follow up on
    assert not bridge._open


def test_bridge_quiet_kinds_go_to_admin_only() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle("⏳ استعداد | TSLA — اقتراب من الحافة القريبة"))
    _run(bridge.handle("🪤 مصيدة سيولة | TSLA — كسر كاذب"))
    assert wire.channel == [] and len(wire.admin) == 2


def test_bridge_no_contract_stays_off_the_channel() -> None:
    wire = _Wire([])  # empty chain: nothing to pick
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle(ENTRY))
    assert wire.channel == []
    assert any("تعذر اختيار عقد" in m for m in wire.admin)


def test_bridge_orphan_target_never_reaches_channel() -> None:
    # a target alert with no tracked entry (restart, or entry never posted)
    wire = _Wire([_c(352.5, 1.9, 2.0)])
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle("🔺 هدف 1 تحقق والصفقة مؤمّنة (الوقف = الدخول) | TSLA"))
    assert wire.channel == [] and len(wire.admin) == 1


def test_bridge_channel_failure_reports_card_to_admin() -> None:
    wire = _Wire([_c(352.5, 1.9, 2.0)], channel_ok=False)
    bridge = TvBridge(wire.admin_send, wire.channel_send, wire.chain_fetch)
    _run(bridge.handle(ENTRY))
    assert any("تعذر النشر" in m and "352.5C" in m for m in wire.admin)
