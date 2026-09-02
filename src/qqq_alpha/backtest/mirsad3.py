"""Python twin of مرصاد ٣ — three specialists under one referee.

Written so the operator never has to read a report tab again: it replays the
same doctrine as the Pine over our own bars, across timeframes and symbols,
and answers in Telegram.

The doctrine, and why it is split three ways: a trap and a wave are not the
same trade wearing different clothes. A trap's fuel is trapped traders being
forced out, and it is spent within a few bars, so it must be taken quickly. A
wave's fuel is a trend that breathes for days, so it must be given room. One
management for both kills both — which is exactly what the previous engine
did, and what its numbers said.

The referee reads the market with two measures that carry no volume and no
absolute price, so they mean the same thing on an index with no volume, on an
emerging market, and on any timeframe: how much of the distance travelled
became actual progress, and where current volatility sits among its own last
hundred bars.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from qqq_alpha.domain import Bar

TRAP, WAVE, BREAK = "صيّاد الفخاخ", "راكب الموجة", "مخترق الانضغاط"


@dataclass
class T3:
    side: int
    engine: str
    entry: float
    stop0: float
    entry_i: int
    r: float = 0.0
    bars: int = 0
    why: str = ""
    exit_why: str = ""
    hour: int = -1


@dataclass
class Res3:
    symbol: str
    minutes: int
    trades: list[T3] = field(default_factory=list)
    rejected_by_law: int = 0
    # counters that say why an engine was silent, so a quiet engine can be
    # diagnosed on real bars instead of guessed at on synthetic ones
    brk_gate: int = 0      # a squeeze released with an expanding bar
    brk_beyond: int = 0    # and that bar closed outside the value box
    trap_seen: int = 0     # a level was pierced and reclaimed
    wave_armed: int = 0    # a pull-back to the mean was armed

    def of(self, engine: str) -> list[T3]:
        return [t for t in self.trades if t.engine == engine]


def _rma(v: list[float], n: int) -> list[float]:
    out: list[float] = []
    acc = 0.0
    for i, x in enumerate(v):
        if i < n:
            acc += x
            out.append(acc / (i + 1))
        else:
            out.append(out[-1] + (x - out[-1]) / n)
    return out


def _ema(v: list[float], n: int) -> list[float]:
    k = 2.0 / (n + 1)
    out: list[float] = []
    for i, x in enumerate(v):
        out.append(x if i == 0 else out[-1] + k * (x - out[-1]))
    return out


def _atr(bars: list[Bar], n: int = 14) -> list[float]:
    tr = []
    for i, b in enumerate(bars):
        if i == 0:
            tr.append(b.high - b.low)
        else:
            p = bars[i - 1].close
            tr.append(max(b.high - b.low, abs(b.high - p), abs(b.low - p)))
    return _rma(tr, n)


def _calm(v: list[float], i: int, n: int = 100) -> float:  # noqa: D401
    """Current volatility as a fraction of its own longer baseline.

    A percentile rank was the obvious choice and the wrong one: after a
    hundred quiet bars the whole window is quiet, the rank drifts back to the
    middle, and the squeeze vanishes exactly when it has gone deepest. A ratio
    keeps its meaning however long the stillness lasts, and stays unitless, so
    it reads the same on any instrument."""
    lo = max(0, i - n)
    window = v[lo:i]
    if not window:
        return 1.0
    base = sum(window) / len(window)
    return v[i] / base if base > 0 else 1.0


def _round_levels(p: float) -> tuple[float, float]:
    step = 0.1 if p < 5 else 0.5 if p < 50 else 5.0 if p < 500 else 25.0 if p < 5000 else 100.0
    import math as _m
    return _m.ceil(p / step) * step, _m.floor(p / step) * step


def run3(bars: list[Bar], symbol: str, minutes: int, *, er_len: int = 10,
         er_trend: float = 0.45, er_range: float = 0.25, sqz_calm: float = 0.75,
         max_risk: float = 1.2, min_risk: float = 0.25, cool: int = 3,
         trap_t1: float = 1.5, trap_t2: float = 2.5, trap_life: int = 8,
         wave_ma: int = 20, wave_trail: float = 2.5, wave_life: int = 14,
         brk_exp: float = 1.5, brk_half: float = 2.0, brk_trail: float = 2.0,
         brk_life: int = 10, sqz_box: int = 20, brk_grace: int = 3,
         trap_tol: float = 0.15,
         trap_gap: int = 20, cost_r: float = 0.15) -> Res3:
    res = Res3(symbol=symbol, minutes=minutes)
    n = len(bars)
    if n < 150:
        return res

    closes = [b.close for b in bars]
    atr = _atr(bars)
    ma = _ema(closes, wave_ma)
    rngs = [max(b.high - b.low, 1e-9) for b in bars]
    avg_rng = _ema(rngs, 20)
    # the stillness test needs a fast measure. A fourteen-period ATR still
    # reads "quiet" a dozen bars after a move has begun, so the box kept being
    # redrawn around a price that was already running away.
    fast_rng = _ema(rngs, 5)

    # levels that need no indicator to see
    day_h = day_l = or_h = or_l = None
    pd_h = pd_l = None
    day_key = None
    day_bar = 0
    or_bars = max(1, round(60 / max(1, minutes)))
    sw_h = sw_l = None

    arm_up = arm_dn = False
    last_trap_lvl = None
    last_trap_i = -10_000
    sqz_hi = sqz_lo = None
    sqz_age = sqz_wait = 0

    pos = 0
    eng = ""
    entry = stop0 = stop = t1 = t2 = 0.0
    hit1 = False
    booked = 0.0
    wgt = 1.0
    entry_i = 0
    last_exit = -10_000
    why = ""
    t_hour = -1

    for i in range(110, n):
        b = bars[i]
        a = atr[i]
        if not a or a <= 0:
            continue

        key = b.ts.date() if isinstance(b.ts, datetime) else None
        if key != day_key:
            pd_h, pd_l = day_h, day_l
            day_key = key
            day_h, day_l = b.high, b.low
            or_h, or_l = b.high, b.low
            day_bar = 0
        else:
            day_h = max(day_h, b.high)
            day_l = min(day_l, b.low)
            day_bar += 1
            if day_bar <= or_bars:
                or_h = max(or_h, b.high)
                or_l = min(or_l, b.low)
        if i >= 5 and bars[i - 5].high == max(x.high for x in bars[i - 10:i + 1]):
            sw_h = bars[i - 5].high
        if i >= 5 and bars[i - 5].low == min(x.low for x in bars[i - 10:i + 1]):
            sw_l = bars[i - 5].low

        rnd_up, rnd_dn = _round_levels(b.close)
        # a level worth trapping at is one the whole market can name: the
        # session's own edges and a clear swing. Round numbers sit every few
        # ticks and would turn every wobble into a signal.
        ups = [(p, nm) for p, nm in ((pd_h, "قمة الأمس"), (day_h, "قمة اليوم"),
                                     (or_h, "قمة الافتتاح"), (sw_h, "قمة متأرجحة"))
               if p and p > b.close]
        dns = [(p, nm) for p, nm in ((pd_l, "قاع الأمس"), (day_l, "قاع اليوم"),
                                     (or_l, "قاع الافتتاح"), (sw_l, "قاع متأرجح"))
               if p and p < b.close]
        lvl_up, up_nm = min(ups, key=lambda x: x[0]) if ups else (None, "")
        lvl_dn, dn_nm = max(dns, key=lambda x: x[0]) if dns else (None, "")

        # ---- the referee
        chg = abs(closes[i] - closes[i - er_len])
        path = sum(abs(closes[j] - closes[j - 1]) for j in range(i - er_len + 1, i + 1))
        er = chg / path if path > 0 else 0.0
        squeeze = _calm(fast_rng, i) <= sqz_calm
        impulse = er >= er_trend
        ranging = er <= er_range

        if squeeze:
            # the box is where the market accepted value while it was quiet,
            # so it is drawn from closes. Built from wicks instead, its edge
            # sits above every high of the stillness and no bar can ever close
            # beyond it - fifty-four setups produced zero breaks that way.
            win = bars[max(0, i - sqz_box + 1):i + 1]
            sqz_hi = max(x.close for x in win)
            sqz_lo = min(x.close for x in win)
            sqz_age += 1
            sqz_wait = 0
        else:
            sqz_wait += 1
            if (0 < sqz_age < 3) or sqz_wait > 10:
                sqz_age = 0
                sqz_hi = sqz_lo = None

        # ---- management first: a position owns the bar
        if pos != 0:
            r = abs(entry - stop0)
            life = trap_life if eng == TRAP else brk_life if eng == BREAK else wave_life
            if not hit1 and (b.high >= t1 if pos > 0 else b.low <= t1):
                hit1 = True
                be = entry + pos * 0.05 * r
                stop = max(stop, be) if pos > 0 else min(stop, be)
                if eng == BREAK:
                    booked = 0.5 * brk_half
                    wgt = 0.5
            if hit1:
                tr = wave_trail if eng == WAVE else brk_trail if eng == BREAK else 0.0
                if tr:
                    ch = b.close - tr * a if pos > 0 else b.close + tr * a
                    stop = max(stop, ch) if pos > 0 else min(stop, ch)
            hard = not hit1 and (b.low <= stop0 if pos > 0 else b.high >= stop0)
            soft = hit1 and (b.close < stop if pos > 0 else b.close > stop)
            done = eng == TRAP and (b.high >= t2 if pos > 0 else b.low <= t2)
            timed = not hit1 and (i - entry_i) >= life
            if hard or soft or done or timed:
                gap = b.open < stop0 if pos > 0 else b.open > stop0
                px = (b.open if gap else stop0) if hard else t2 if done else b.close
                rest = ((px - entry) if pos > 0 else (entry - px)) / r if r else 0.0
                res.trades.append(T3(pos, eng, entry, stop0, entry_i,
                                     booked + wgt * rest - cost_r, i - entry_i, why,
                                     "وقف" if hard else "الهدف" if done else "مهلة" if timed else "متحرك",
                                     t_hour))
                pos = 0
                last_exit = i
            continue

        if (i - last_exit) <= cool:
            continue

        # ---- the three specialists, each only in its own state
        side = 0
        e = ""
        st = None
        w = ""
        tol = trap_tol * a
        upper = (b.close - b.low) / rngs[i] > 0.55
        lower = (b.high - b.close) / rngs[i] > 0.55

        # the break is the release, not the stillness. Allowing this to fire
        # while the market is still quiet meant testing whether price had left
        # a box that is redrawn around it every bar - it never had, in fifty
        # four attempts.
        if (not squeeze) and sqz_age >= 3 and sqz_wait <= brk_grace and rngs[i] > brk_exp * avg_rng[i]:
            res.brk_gate += 1
            # A failed break is proven the moment price is accepted back
            # inside the value area - not when it reaches the far side of the
            # box. Stopping at the far edge made the risk wider than the law
            # allows during a quiet stretch, and every squeeze trade we ever
            # generated was thrown away by that arithmetic.
            # the entry belongs within a few bars of the release. Allowing ten
            # put the stop four times the law's ceiling away, because the risk
            # became the whole move since the stillness rather than the box.
            # The invalidation is a return below the edge that was broken -
            # not a journey to the far side of the box. A twenty-bar range is
            # three to five ATR wide by nature, so stopping there asked for a
            # risk the law can never allow and silenced this engine entirely.
            if sqz_hi and b.close > sqz_hi + 0.1 * a and b.close > b.open:
                side, e = 1, BREAK
                st = min(sqz_hi, b.low) - 0.05 * a
                w = "اتساع بعد سكون"
            elif sqz_lo and b.close < sqz_lo - 0.1 * a and b.close < b.open:
                side, e = -1, BREAK
                st = max(sqz_lo, b.high) + 0.05 * a
                w = "اتساع بعد سكون"
            if side:
                res.brk_beyond += 1
                sqz_age = 0

        if not side and impulse:
            wdir = 1 if (b.close > ma[i] and b.close > closes[i - er_len]) else \
                   -1 if (b.close < ma[i] and b.close < closes[i - er_len]) else 0
            if wdir > 0 and b.low <= ma[i] + 0.25 * a:
                if not arm_up:
                    res.wave_armed += 1
                arm_up, arm_dn = True, False
            if wdir < 0 and b.high >= ma[i] - 0.25 * a:
                arm_dn, arm_up = True, False
            if wdir <= 0:
                arm_up = False
            if wdir >= 0:
                arm_dn = False
            if arm_up and b.close > b.open and b.close > bars[i - 1].high and b.close > ma[i]:
                side, e, st, w = 1, WAVE, min(b.low, bars[i - 1].low) - 0.2 * a, "استئناف بعد ارتداد"
                arm_up = False
            elif arm_dn and b.close < b.open and b.close < bars[i - 1].low and b.close < ma[i]:
                side, e, st, w = -1, WAVE, max(b.high, bars[i - 1].high) + 0.2 * a, "استئناف بعد ارتداد"
                arm_dn = False

        if not side and ranging:
            # the same level, trapped twice running, is no longer a trap:
            # whoever was going to be caught there already has been
            def fresh(lvl: float, prev: float | None = last_trap_lvl,
                      since: int = i - last_trap_i, unit: float = a) -> bool:
                return prev is None or abs(lvl - prev) > 0.5 * unit or since > trap_gap

            if (lvl_dn and b.low < lvl_dn - tol and b.close > lvl_dn) or \
               (lvl_up and b.high > lvl_up + tol and b.close < lvl_up):
                res.trap_seen += 1
            if lvl_dn and b.low < lvl_dn - tol and b.close > lvl_dn and upper and fresh(lvl_dn):
                side, e, st, w = 1, TRAP, b.low - 0.15 * a, f"فشل كسر {dn_nm}"
                last_trap_lvl, last_trap_i = lvl_dn, i
            elif lvl_up and b.high > lvl_up + tol and b.close < lvl_up and lower and fresh(lvl_up):
                side, e, st, w = -1, TRAP, b.high + 0.15 * a, f"فشل كسر {up_nm}"
                last_trap_lvl, last_trap_i = lvl_up, i

        if not side or st is None:
            continue

        # ---- the law above all three
        #
        # The two ends of this rule answer different questions, so they are
        # measured against different volatilities. The floor asks "is this
        # stop inside today's noise?" - today's ATR. The ceiling asks "is this
        # more than the move can repay?" - the market's normal ATR. Measuring
        # both against a squeezed ATR rejected every breakout twice over: too
        # wide by the ceiling, then too tight by the floor.
        window = atr[max(0, i - 100):i]
        base_a = sum(window) / len(window) if window else a
        risk = abs(b.close - st)
        if risk > max_risk * max(a, base_a) or risk < min_risk * a:
            res.rejected_by_law += 1
            continue

        pos, eng, entry, stop0, stop = side, e, b.close, st, st
        entry_i, hit1, booked, wgt, why = i, False, 0.0, 1.0, w
        t_hour = b.ts.hour if isinstance(b.ts, datetime) else -1
        t1x = trap_t1 if e == TRAP else brk_half if e == BREAK else 1.0
        t2x = trap_t2 if e == TRAP else (brk_half * 2 if e == BREAK else 3.0)
        t1 = entry + side * t1x * risk
        t2 = entry + side * t2x * risk

    return res


def _stat(ts: list[T3]) -> tuple[int, float, float, float]:
    if not ts:
        return 0, 0.0, 0.0, 100.0
    won = [t.r for t in ts if t.r > 0]
    lost = [-t.r for t in ts if t.r <= 0]
    pf = (sum(won) / sum(lost)) if lost and sum(lost) > 0 else (99.0 if won else 0.0)
    avg = sum(t.r for t in ts) / len(ts)
    payoff = (sum(won) / len(won)) / (sum(lost) / len(lost)) if won and lost else 0.0
    need = 100.0 / (1 + payoff) if payoff > 0 else 100.0
    return len(ts), pf, avg, need


def _row(label: str, ts: list[T3], floor: int = 30) -> str:
    n, pf, avg, need = _stat(ts)
    if not n:
        return f"   {label}: —"
    wr = 100.0 * len([t for t in ts if t.r > 0]) / n
    mark = "✅" if pf >= 1.2 and n >= floor else "🟡" if pf >= 1.0 else "🔴"
    thin = " ⚠️عينة صغيرة" if n < floor else ""
    return (f"   {mark} {label}: {n} · عامل {pf:.2f} · متوسط "
            f"{'+' if avg >= 0 else ''}{avg:.2f}R · نجاح {wr:.0f}% مقابل {need:.0f}%{thin}")


def format3(rows: list[Res3], symbols: int, months: int) -> str:
    """Each engine judged on its own, because pooling them hides which one
    works — the mistake that cost us the last two weeks."""
    out = [f"🧭 مرصاد ٣ — {symbols} رمز · {months} شهر", "كل الأرقام صافية بعد التكلفة.", ""]
    for r in rows:
        n, pf, avg, _ = _stat(r.trades)
        head = f"■ {r.minutes}د — {n} صفقة · عامل {pf:.2f} · متوسط {'+' if avg >= 0 else ''}{avg:.2f}R"
        out.append(head)
        out.append(_row("صيّاد الفخاخ", r.of(TRAP)))
        out.append(_row("راكب الموجة", r.of(WAVE)))
        out.append(_row("مخترق الانضغاط", r.of(BREAK)))
        out.append(f"   (رفضها القانون: {r.rejected_by_law} · انضغاط تحرر: "
                   f"{r.brk_gate} منها خرجت: {r.brk_beyond} · فخاخ رُصدت: {r.trap_seen})")
        out.append("")
    best: tuple[str, float, int] | None = None
    for r in rows:
        for name, ts in ((TRAP, r.of(TRAP)), (WAVE, r.of(WAVE)), (BREAK, r.of(BREAK))):
            k, pf, _, _ = _stat(ts)
            if k >= 40 and pf >= 1.2 and (best is None or pf > best[1]):
                best = (f"{name} على {r.minutes}د", pf, k)
    out.append(f"🏆 الأقوى: {best[0]} — عامل {best[1]:.2f} على {best[2]} صفقة."
               if best else
               "⚠️ لا محرك على أي فريم يعبر عامل ربح 1.2 بعينة محترمة.")
    return "\n".join(out)
