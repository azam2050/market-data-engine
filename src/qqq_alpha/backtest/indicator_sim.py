"""Python twin of the TradingView indicator, run on our own 12-month data.

Why this exists: TradingView's chart only loads a few months of 5-minute
history on most plans, and the operator tests from a tablet where the
Strategy Tester tab is a scavenger hunt. Our data feed already holds a year
of bars — so the engine replays the indicator's exact doctrine over them and
answers in Telegram: trades, win rate, average R, best and worst trade.

Fidelity notes (kept deliberately identical to the Pine code):
- signals only on closed bars; retrace entry at the signal candle's midpoint
  with a 5-bar wait and an invalidation stop, otherwise the signal cancels;
- entries: liquidity traps at the edges, confirmed bounces, volume breaks;
- management: T1/T2/T3 at 1.5/2.6/4.2 ATR, stop to entry at T1, stop to T1
  at T2, then a chandelier + structural-pivot trail; exit on close beyond
  the stop; R is measured to the actual exit close, never the best target.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, time
from zoneinfo import ZoneInfo

from qqq_alpha.domain import Bar

NY = ZoneInfo("America/New_York")

# same defaults as the indicator's inputs
MIN_SCORE = 65
NEED_ALIGN = 2
COOL_BARS = 10
TRAP_VOL_X = 1.5
MAX_CND_ATR = 1.8
WAIT_BARS = 5
TRAIL_ATR = 2.5
END_GUARD_MIN = 30  # 🌙 mode: entries in the last window are allowed and carry


@dataclass
class SimTrade:
    direction: int
    entry: float
    stop0: float
    entry_ts: datetime
    exit_ts: datetime | None = None
    r_mult: float = 0.0
    t1_hit: bool = False
    kind: str = ""
    score: int = 0
    align: int = 0
    rel_vol: float = 0.0
    hour_ny: int = 0


@dataclass
class SimResult:
    symbol: str
    frame_min: int
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    trades: list[SimTrade] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.trades)

    @property
    def wins(self) -> int:
        return sum(1 for t in self.trades if t.t1_hit)

    @property
    def win_rate(self) -> float:
        return 100.0 * self.wins / self.total if self.total else 0.0

    @property
    def avg_r(self) -> float:
        return sum(t.r_mult for t in self.trades) / self.total if self.total else 0.0

    @property
    def best_r(self) -> float:
        return max((t.r_mult for t in self.trades), default=0.0)

    @property
    def worst_r(self) -> float:
        return min((t.r_mult for t in self.trades), default=0.0)


def _rma(values: list[float], length: int) -> list[float]:
    out: list[float] = []
    avg: float | None = None
    for i, v in enumerate(values):
        if avg is None:
            window = values[: i + 1]
            avg = sum(window) / len(window)
            if i + 1 < length:
                out.append(avg)
                continue
        else:
            avg = (avg * (length - 1) + v) / length
        out.append(avg)
    return out


def _ema_step(prev: float | None, value: float, length: int) -> float:
    if prev is None:
        return value
    alpha = 2.0 / (length + 1.0)
    return prev + alpha * (value - prev)


class _HigherFrame:
    """EMA9/21 direction of an aggregated frame, updated as base bars arrive.

    Mirrors live behaviour: EMAs advance on completed higher-frame bars, and
    the direction test uses the current (developing) close against them.
    """

    def __init__(self, mult: int):
        self.mult = mult
        self.count = 0
        self.close: float | None = None
        self.ema_f: float | None = None
        self.ema_s: float | None = None

    def update(self, close: float, new_session: bool) -> int:
        if new_session:
            self.count = 0
        self.close = close
        self.count += 1
        if self.count >= self.mult:
            self.ema_f = _ema_step(self.ema_f, close, 9)
            self.ema_s = _ema_step(self.ema_s, close, 21)
            self.count = 0
        if self.ema_f is None or self.ema_s is None:
            return 0
        if self.ema_f > self.ema_s and close > self.ema_s:
            return 1
        if self.ema_f < self.ema_s and close < self.ema_s:
            return -1
        return 0


def _score(align: int, d_side_ok: bool, near_my: bool, trap_my: bool,
           mid_range: bool, delta_share: float, rel_vol: float) -> int:
    s_a = align * 10
    s_d = min(25, round(abs(delta_share) / 4)) if d_side_ok else 0
    s_l = 25 if trap_my else 20 if near_my else 0 if mid_range else 10
    s_v = min(15, round(rel_vol * 6))
    s_t = 5 if trap_my else 0
    return min(100, s_a + s_d + s_l + s_v + s_t)


def run_simulation(
    bars: list[Bar], symbol: str, frame_min: int, day_close: bool = False
) -> SimResult:
    """Replay the indicator over RTH bars of one symbol/frame.

    day_close=True mirrors the daily-contracts reality: any open trade is
    flattened at the session's last bar and pendings die with the day, so an
    overnight gap can never turn a -1R stop into a -17R disaster.
    """
    rth = [
        b for b in bars
        if time(9, 30) <= b.ts.astimezone(NY).time() < time(16, 0)
    ]
    res = SimResult(symbol=symbol, frame_min=frame_min)
    n = len(rth)
    if n < 60:
        return res
    res.first_ts, res.last_ts = rth[0].ts, rth[-1].ts

    highs = [b.high for b in rth]
    lows = [b.low for b in rth]
    closes = [b.close for b in rth]
    opens = [b.open for b in rth]
    vols = [float(b.volume) for b in rth]
    days = [b.ts.astimezone(NY).date() for b in rth]
    weeks = [d.isocalendar()[:2] for d in days]
    mins_left = [
        (16 * 60) - (t.hour * 60 + t.minute)
        for t in (b.ts.astimezone(NY).time() for b in rth)
    ]

    tr = [highs[0] - lows[0]] + [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, n)
    ]
    atr = _rma(tr, 14)

    # previous completed day / week extremes (the edges)
    pd_h: list[float] = [math.nan] * n
    pd_l: list[float] = [math.nan] * n
    pw_h: list[float] = [math.nan] * n
    pw_l: list[float] = [math.nan] * n
    prev_day: tuple[float, float] | None = None
    prev_week: tuple[float, float] | None = None
    cur_day: tuple[float, float] | None = None
    cur_week: tuple[float, float] | None = None
    for i in range(n):
        if i > 0 and days[i] != days[i - 1]:
            prev_day = cur_day
            cur_day = None
        if i > 0 and weeks[i] != weeks[i - 1]:
            prev_week = cur_week
            cur_week = None
        cur_day = (highs[i], lows[i]) if cur_day is None else (
            max(cur_day[0], highs[i]), min(cur_day[1], lows[i]))
        cur_week = (highs[i], lows[i]) if cur_week is None else (
            max(cur_week[0], highs[i]), min(cur_week[1], lows[i]))
        if prev_day:
            pd_h[i], pd_l[i] = prev_day
        if prev_week:
            pw_h[i], pw_l[i] = prev_week

    f1 = _HigherFrame(1)
    f2 = _HigherFrame(2)
    f3 = _HigherFrame(4)

    vol_sum = 0.0
    vol_hist: list[float] = []
    d_ema_p: float | None = None
    d_ema_s: float | None = None

    last_piv_l: float | None = None
    last_piv_h: float | None = None
    last_piv_l_bar = -1
    last_piv_h_bar = -1

    day_hi = day_lo = math.nan
    prev_day_hi = prev_day_lo = math.nan
    bars_today = 0
    bull_trap_since = -(10**9)
    bear_trap_since = -(10**9)

    in_trade = False
    trade: SimTrade | None = None
    stop_p = t1p = t2p = t3p = 0.0
    t1h = t2h = t3h = False
    entry_bar = -1
    last_exit = -(10**9)

    pend_dir = 0
    pend_lvl = pend_stp = 0.0
    pend_until = pend_since = -1
    pend_kind = ""
    pend_score = pend_align = 0
    pend_relvol = 0.0

    for i in range(n):
        new_day = i == 0 or days[i] != days[i - 1]
        last_of_day = i + 1 == n or days[i + 1] != days[i]
        if day_close and last_of_day:
            if in_trade and trade is not None:
                risk = abs(trade.entry - trade.stop0)
                d = trade.direction
                trade.r_mult = (
                    ((closes[i] - trade.entry) if d > 0 else (trade.entry - closes[i])) / risk
                    if risk > 0 else 0.0
                )
                trade.exit_ts = rth[i].ts
                res.trades.append(trade)
                in_trade = False
                trade = None
                last_exit = i
            pend_dir = 0
        if new_day:
            prev_day_hi, prev_day_lo = day_hi, day_lo
            day_hi, day_lo = highs[i], lows[i]
            bars_today = 0
        else:
            day_hi = max(day_hi, highs[i])
            day_lo = min(day_lo, lows[i])
            bars_today += 1

        # confirmed swing pivots (2,2), known two bars late
        j = i - 2
        if 2 <= j < n - 0 and j + 2 <= i:
            if j >= 2 and lows[j] == min(lows[j - 2:j + 3]):
                last_piv_l, last_piv_l_bar = lows[j], j
            if j >= 2 and highs[j] == max(highs[j - 2:j + 3]):
                last_piv_h, last_piv_h_bar = highs[j], j

        vol_hist.append(vols[i])
        vol_sum += vols[i]
        if len(vol_hist) > 20:
            vol_sum -= vol_hist.pop(0)
        vol_sma = vol_sum / len(vol_hist)
        rel_vol = vols[i] / vol_sma if vol_sma > 0 else 1.0

        rng = max(highs[i] - lows[i], 0.01)
        delta = vols[i] * (closes[i] - opens[i]) / rng
        d_ema_p = _ema_step(d_ema_p, abs(delta), 20)
        d_ema_s = _ema_step(d_ema_s, delta, 20)
        delta_share = 100.0 * d_ema_s / d_ema_p if d_ema_p else 0.0

        c1 = f1.update(closes[i], new_day)
        c2 = f2.update(closes[i], new_day)
        c3 = f3.update(closes[i], new_day)
        align_call = sum(1 for c in (c1, c2, c3) if c > 0)
        align_put = sum(1 for c in (c1, c2, c3) if c < 0)

        a = atr[i]
        tol = 0.05 * a
        have_edges = not math.isnan(pd_l[i]) and not math.isnan(pw_l[i])

        sess_swp_l = (
            bars_today > 6 and not math.isnan(prev_day_lo)
            and lows[i] < prev_day_lo - tol and closes[i] > prev_day_lo
        )
        sess_swp_h = (
            bars_today > 6 and not math.isnan(prev_day_hi)
            and highs[i] > prev_day_hi + tol and closes[i] < prev_day_hi
        )
        bull_trap = have_edges and (
            (lows[i] < pd_l[i] - tol and closes[i] > pd_l[i])
            or (lows[i] < pw_l[i] - tol and closes[i] > pw_l[i])
            or sess_swp_l
        ) and rel_vol > TRAP_VOL_X and delta > 0
        bear_trap = have_edges and (
            (highs[i] > pd_h[i] + tol and closes[i] < pd_h[i])
            or (highs[i] > pw_h[i] + tol and closes[i] < pw_h[i])
            or sess_swp_h
        ) and rel_vol > TRAP_VOL_X and delta < 0
        if bull_trap:
            bull_trap_since = i
        if bear_trap:
            bear_trap_since = i
        trap_rec_up = i - bull_trap_since <= 6
        trap_rec_dn = i - bear_trap_since <= 6

        day_rng = max(day_hi - day_lo, 0.01)
        pos_in_day = (closes[i] - day_lo) / day_rng
        mid_range = 0.40 < pos_in_day < 0.60

        near_floor = have_edges and 0 <= closes[i] - max(pd_l[i], pw_l[i]) < 1.2 * a
        near_ceil = have_edges and 0 <= min(pd_h[i], pw_h[i]) - closes[i] < 1.2 * a
        body_ratio = abs(closes[i] - opens[i]) / rng
        bull_bounce = near_floor and closes[i] > opens[i] and body_ratio > 0.55 and delta > 0 and rel_vol > 1.2
        bear_bounce = near_ceil and closes[i] < opens[i] and body_ratio > 0.55 and delta < 0 and rel_vol > 1.2

        call_score = _score(align_call, delta_share > 0, near_floor, trap_rec_up, mid_range, delta_share, rel_vol)
        put_score = _score(align_put, delta_share < 0, near_ceil, trap_rec_dn, mid_range, delta_share, rel_vol)

        # ------------------------------------------------ trade management
        if in_trade and trade is not None and i > entry_bar:
            d = trade.direction
            if not t1h and (highs[i] >= t1p if d > 0 else lows[i] <= t1p):
                t1h = True
                trade.t1_hit = True
                stop_p = trade.entry
            if t1h and not t2h and (highs[i] >= t2p if d > 0 else lows[i] <= t2p):
                t2h = True
                stop_p = t1p
            if t2h and not t3h and (highs[i] >= t3p if d > 0 else lows[i] <= t3p):
                t3h = True
            if t1h:
                chand = closes[i] - TRAIL_ATR * a if d > 0 else closes[i] + TRAIL_ATR * a
                if d > 0:
                    struct = last_piv_l - 0.25 * a if (last_piv_l is not None and last_piv_l_bar > entry_bar) else chand
                    stop_p = max(stop_p, max(chand, struct))
                else:
                    struct = last_piv_h + 0.25 * a if (last_piv_h is not None and last_piv_h_bar > entry_bar) else chand
                    stop_p = min(stop_p, min(chand, struct))
            if (closes[i] < stop_p) if d > 0 else (closes[i] > stop_p):
                risk = abs(trade.entry - trade.stop0)
                trade.r_mult = ((closes[i] - trade.entry) if d > 0 else (trade.entry - closes[i])) / risk if risk > 0 else 0.0
                trade.exit_ts = rth[i].ts
                res.trades.append(trade)
                in_trade = False
                trade = None
                last_exit = i

        # ------------------------------------------- pending retrace entry
        if not in_trade and pend_dir != 0 and i > pend_since:
            touched = lows[i] <= pend_lvl if pend_dir > 0 else highs[i] >= pend_lvl
            broken = closes[i] < pend_stp if pend_dir > 0 else closes[i] > pend_stp
            if touched and not broken:
                trade = SimTrade(direction=pend_dir, entry=pend_lvl, stop0=pend_stp,
                                 entry_ts=rth[i].ts, kind=pend_kind,
                                 score=pend_score, align=pend_align,
                                 rel_vol=pend_relvol,
                                 hour_ny=rth[i].ts.astimezone(NY).hour)
                in_trade = True
                entry_bar = i
                stop_p = pend_stp
                t1p = pend_lvl + 1.5 * a if pend_dir > 0 else pend_lvl - 1.5 * a
                t2p = pend_lvl + 2.6 * a if pend_dir > 0 else pend_lvl - 2.6 * a
                t3p = pend_lvl + 4.2 * a if pend_dir > 0 else pend_lvl - 4.2 * a
                t1h = t2h = t3h = False
                pend_dir = 0
            elif broken or i > pend_until:
                pend_dir = 0

        # ------------------------------------------------------ new signals
        can_enter = (
            not in_trade and pend_dir == 0 and (i - last_exit) > COOL_BARS
            and (highs[i] - lows[i]) <= MAX_CND_ATR * a and i >= 30
        )
        if not can_enter:
            continue

        prev_close = closes[i - 1]
        long_trap = bull_trap and call_score >= MIN_SCORE and align_call >= NEED_ALIGN
        short_trap = bear_trap and put_score >= MIN_SCORE and align_put >= NEED_ALIGN
        long_bnc = not long_trap and bull_bounce and call_score >= MIN_SCORE and align_call >= NEED_ALIGN
        short_bnc = not short_trap and bear_bounce and put_score >= MIN_SCORE and align_put >= NEED_ALIGN
        brk_up = (
            not long_trap and not long_bnc and have_edges
            and ((prev_close <= pd_h[i] < closes[i]) or (prev_close <= pw_h[i] < closes[i]))
            and rel_vol > 1.3 and delta > 0 and call_score >= MIN_SCORE
            and align_call >= NEED_ALIGN and not mid_range
        )
        brk_dn = (
            not short_trap and not short_bnc and have_edges
            and ((prev_close >= pd_l[i] > closes[i]) or (prev_close >= pw_l[i] > closes[i]))
            and rel_vol > 1.3 and delta < 0 and put_score >= MIN_SCORE
            and align_put >= NEED_ALIGN and not mid_range
        )

        sig_dir = 1 if (long_trap or long_bnc or brk_up) else -1 if (short_trap or short_bnc or brk_dn) else 0
        if sig_dir == 0:
            continue
        if sig_dir > 0:
            stop = lows[i] - 0.25 * a if long_trap else min(lows[i], lows[i - 1]) - 0.25 * a
            kind = "مصيدة" if long_trap else "ارتداد" if long_bnc else "اختراق"
        else:
            stop = highs[i] + 0.25 * a if short_trap else max(highs[i], highs[i - 1]) + 0.25 * a
            kind = "مصيدة" if short_trap else "رفض سقف" if short_bnc else "كسر"
        pend_dir = sig_dir
        pend_lvl = (highs[i] + lows[i]) / 2.0
        pend_stp = stop
        pend_until = i + WAIT_BARS
        pend_since = i
        pend_kind = kind + (" 🌙" if mins_left[i] <= END_GUARD_MIN else "")
        pend_score = call_score if sig_dir > 0 else put_score
        pend_align = align_call if sig_dir > 0 else align_put
        pend_relvol = rel_vol

    return res


def format_report(results: list[SimResult], months: int) -> str:
    """One Telegram message: the matrix the operator was building by hand."""
    lines = [f"🧪 فحص المؤشر على بياناتنا — آخر {months} شهراً (الجلسة الرسمية فقط)"]
    for r in results:
        if r.total == 0:
            lines.append(f"\n▪️ {r.symbol} · {r.frame_min} د — لا صفقات (بيانات غير كافية)")
            continue
        span = ""
        if r.first_ts and r.last_ts:
            span = f" · من {r.first_ts.astimezone(NY):%Y-%m-%d} إلى {r.last_ts.astimezone(NY):%Y-%m-%d}"
        avg = r.avg_r
        lines.append(
            f"\n▪️ {r.symbol} · {r.frame_min} د{span}\n"
            f"   الصفقات: {r.total} · نجاح {r.win_rate:.1f}%"
            f" · متوسط {'+' if avg >= 0 else ''}{avg:.2f}R\n"
            f"   أفضل صفقة +{r.best_r:.1f}R · أسوأ صفقة {r.worst_r:.1f}R"
        )
    lines.append(
        "\nℹ️ نفس عقل المؤشر حرفياً: دخول ارتدادي، وقف يتأمن عند الهدف الأول،"
        " تسلق بنيوي، وR بسعر الخروج الفعلي."
    )
    return "\n".join(lines)


def _slice_line(label: str, trades: list[SimTrade]) -> str:
    n = len(trades)
    if n == 0:
        return f"   {label}: —"
    wins = sum(1 for t in trades if t.t1_hit)
    avg = sum(t.r_mult for t in trades) / n
    return (
        f"   {label}: {n} صفقة · نجاح {100.0 * wins / n:.0f}%"
        f" · متوسط {'+' if avg >= 0 else ''}{avg:.2f}R"
    )


def format_diagnosis(results: list[SimResult], months: int) -> str:
    """Where the losses actually live: by setup, hour, alignment, and score.

    This is the operator's question "why does it fail?" answered with the
    year's own trades — the raw material for a data-driven quality mode.
    """
    trades = [t for r in results for t in r.trades]
    if not trades:
        return "🔎 لا صفقات كافية للتشخيص"
    lines = [f"🔎 تشريح صفقات {months} شهراً — {len(trades)} صفقة (الوضع اليومي)"]

    lines.append("\n🎯 حسب نوع الدخول:")
    base_kind = lambda t: t.kind.replace(" 🌙", "")  # noqa: E731
    for kind in sorted({base_kind(t) for t in trades}):
        lines.append(_slice_line(kind, [t for t in trades if base_kind(t) == kind]))

    lines.append("\n🕐 حسب وقت الدخول (بتوقيت السوق):")
    hours = [
        ("الافتتاح 9–10", {9}),
        ("الصباح 10–12", {10, 11}),
        ("الظهيرة 12–14", {12, 13}),
        ("العصر 14–16", {14, 15}),
    ]
    for label, hs in hours:
        lines.append(_slice_line(label, [t for t in trades if t.hour_ny in hs]))

    lines.append("\n🧭 حسب توافق الفريمات:")
    for a in (2, 3):
        lines.append(_slice_line(f"توافق {a}/3", [t for t in trades if t.align == a]))

    lines.append("\n💪 حسب قوة الإشارة:")
    for label, lo, hi in (("65–74", 65, 74), ("75–84", 75, 84), ("85+", 85, 200)):
        lines.append(_slice_line(label, [t for t in trades if lo <= t.score <= hi]))

    lines.append("\n🌙 صفقات آخر الجلسة:")
    lines.append(_slice_line("🌙", [t for t in trades if "🌙" in t.kind]))

    worst = sorted(trades, key=lambda t: t.r_mult)[:3]
    lines.append("\n📉 أسوأ ثلاث صفقات:")
    for t in worst:
        lines.append(
            f"   {t.entry_ts.astimezone(NY):%Y-%m-%d %H:%M} · {t.kind}"
            f" · قوة {t.score} · {t.r_mult:.1f}R"
        )
    return "\n".join(lines)
