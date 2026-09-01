"""A faithful Python port of the current مرصاد VIP engine, run over our own bars.

The Pine script can only be judged one chart at a time, from a tablet, by a
human reading a report tab. This module replays the same doctrine over the
data feed we already pay for, so a single Telegram word answers the question
the operator actually has: which timeframe carries an edge, and which does
not.

Every rule below mirrors the indicator deliberately. When the Pine changes,
this changes with it, and a disagreement between them is a bug in one of the
two — never a difference of opinion:

- the personality of the chart's timeframe sets targets, stop, trail, the
  time stop and the cooldown;
- entries arm a limit into the pull-back, plus a breakout order only when the
  spark bar is genuinely impulsive, and the earlier anticipation order placed
  at the zone itself without waiting for the resumption bar to close;
- the first target only secures the contract's own cost, nothing trails until
  the second is banked, and no target count ever closes a live winner;
- a trade that has not reached its first target inside the time stop is cut;
- three consecutive losses end the day;
- every result is booked net of the contract's round-trip spread, because a
  trade that exits where it entered is a loss.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from qqq_alpha.domain import Bar

# the spread paid on the way in and on the way out, in units of the risk
COST_R = 0.15


@dataclass(frozen=True)
class Profile:
    """What the trader on this timeframe is actually trying to do."""

    name: str
    tp1: float
    tp2: float
    stop: float
    trail: float
    time_stop: int
    cool: int
    max_targets: int


def profile_for(minutes: int) -> Profile:
    if minutes <= 1:
        return Profile("حارق", 0.5, 0.9, 0.5, 1.5, 6, 3, 2)
    if minutes <= 5:
        return Profile("سريع", 0.8, 1.5, 0.7, 2.5, 10, 6, 99)
    if minutes <= 30:
        return Profile("نصف يوم", 1.2, 2.0, 1.0, 3.2, 12, 6, 99)
    if minutes <= 60:
        return Profile("يوم", 1.8, 3.0, 1.3, 4.0, 14, 8, 99)
    return Profile("أسبوع", 2.5, 4.0, 1.8, 5.0, 20, 8, 99)


@dataclass
class Trade:
    side: int
    entry: float
    stop0: float
    entry_i: int
    exit_i: int = 0
    r_net: float = 0.0
    bars: int = 0
    reason: str = ""


@dataclass
class Outcome:
    symbol: str
    minutes: int
    profile: str
    trades: list[Trade] = field(default_factory=list)
    cancelled: int = 0

    @property
    def total(self) -> int:
        return len(self.trades)

    @property
    def wins(self) -> list[Trade]:
        return [t for t in self.trades if t.r_net > 0]

    @property
    def losses(self) -> list[Trade]:
        return [t for t in self.trades if t.r_net <= 0]

    @property
    def win_rate(self) -> float:
        return 100.0 * len(self.wins) / self.total if self.total else 0.0

    @property
    def avg_r(self) -> float:
        return sum(t.r_net for t in self.trades) / self.total if self.total else 0.0

    @property
    def total_r(self) -> float:
        return sum(t.r_net for t in self.trades)

    @property
    def profit_factor(self) -> float:
        gain = sum(t.r_net for t in self.wins)
        pain = -sum(t.r_net for t in self.losses)
        if pain <= 0:
            return 99.0 if gain > 0 else 0.0
        return gain / pain

    @property
    def payoff(self) -> float:
        """Average win divided by average loss — the ratio that, with the win
        rate, decides whether the system can pay at all."""
        if not self.wins or not self.losses:
            return 0.0
        avg_w = sum(t.r_net for t in self.wins) / len(self.wins)
        avg_l = -sum(t.r_net for t in self.losses) / len(self.losses)
        return avg_w / avg_l if avg_l > 0 else 0.0

    @property
    def breakeven_win_rate(self) -> float:
        """The win rate this payoff would need. The gap to the real one is the
        whole distance between a losing system and a paying one."""
        p = self.payoff
        return 100.0 / (1.0 + p) if p > 0 else 100.0

    @property
    def avg_bars_win(self) -> float:
        return sum(t.bars for t in self.wins) / len(self.wins) if self.wins else 0.0

    @property
    def avg_bars_loss(self) -> float:
        return sum(t.bars for t in self.losses) / len(self.losses) if self.losses else 0.0

    @property
    def max_loss_run(self) -> int:
        run = best = 0
        for t in self.trades:
            run = 0 if t.r_net > 0 else run + 1
            best = max(best, run)
        return best


def _rma(values: list[float], length: int) -> list[float]:
    out: list[float] = []
    acc = 0.0
    for i, v in enumerate(values):
        if i < length:
            acc += v
            out.append(acc / (i + 1))
        else:
            out.append(out[-1] + (v - out[-1]) / length)
    return out


def _ema(values: list[float], length: int) -> list[float]:
    k = 2.0 / (length + 1)
    out: list[float] = []
    for i, v in enumerate(values):
        out.append(v if i == 0 else out[-1] + k * (v - out[-1]))
    return out


def _wma(values: list[float], length: int) -> list[float]:
    out: list[float] = []
    denom = length * (length + 1) / 2
    for i in range(len(values)):
        if i + 1 < length:
            out.append(values[i])
            continue
        s = sum(values[i - length + 1 + j] * (j + 1) for j in range(length))
        out.append(s / denom)
    return out


def _hma(values: list[float], length: int) -> list[float]:
    half = max(1, length // 2)
    sqrt_len = max(1, int(length ** 0.5))
    a = _wma(values, half)
    b = _wma(values, length)
    diff = [2 * a[i] - b[i] for i in range(len(values))]
    return _wma(diff, sqrt_len)


def _atr(bars: list[Bar], length: int = 14) -> list[float]:
    tr: list[float] = []
    for i, b in enumerate(bars):
        if i == 0:
            tr.append(b.high - b.low)
        else:
            prev = bars[i - 1].close
            tr.append(max(b.high - b.low, abs(b.high - prev), abs(b.low - prev)))
    return _rma(tr, length)


def _rsi(closes: list[float], length: int = 14) -> list[float]:
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = _rma(gains, length)
    al = _rma(losses, length)
    return [100.0 if al[i] == 0 else 100 - 100 / (1 + ag[i] / al[i]) for i in range(len(closes))]


def _adx(bars: list[Bar], length: int = 14) -> list[float]:
    plus: list[float] = [0.0]
    minus: list[float] = [0.0]
    tr: list[float] = [bars[0].high - bars[0].low] if bars else []
    for i in range(1, len(bars)):
        up = bars[i].high - bars[i - 1].high
        dn = bars[i - 1].low - bars[i].low
        plus.append(up if up > dn and up > 0 else 0.0)
        minus.append(dn if dn > up and dn > 0 else 0.0)
        prev = bars[i - 1].close
        tr.append(max(bars[i].high - bars[i].low, abs(bars[i].high - prev), abs(bars[i].low - prev)))
    atr = _rma(tr, length)
    pdi = _rma(plus, length)
    mdi = _rma(minus, length)
    out: list[float] = []
    dx: list[float] = []
    for i in range(len(bars)):
        if atr[i] <= 0:
            dx.append(0.0)
            continue
        p = 100 * pdi[i] / atr[i]
        m = 100 * mdi[i] / atr[i]
        dx.append(0.0 if p + m == 0 else 100 * abs(p - m) / (p + m))
    out = _rma(dx, length)
    return out


def run(bars: list[Bar], symbol: str, minutes: int, *, min_quality: int = 60,
        anticipate: bool = True, max_loss_day: int = 3) -> Outcome:
    """Replay the engine over one symbol's bars on one timeframe."""
    prof = profile_for(minutes)
    res = Outcome(symbol=symbol, minutes=minutes, profile=prof.name)
    n = len(bars)
    if n < 120:
        return res

    closes = [b.close for b in bars]
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    vols = [float(getattr(b, "volume", 0) or 0) for b in bars]
    atr = _atr(bars)
    rsi = _rsi(closes)
    adx = _adx(bars)
    fast = _hma(closes, 21)
    slow = _ema(closes, 55)
    ema9 = _ema(closes, 9)
    vol_ma = _ema(vols, 20)

    # state
    arm_up = arm_dn = False
    p_dir = 0
    p_limit = p_break = p_stop = 0.0
    p_bar = 0
    in_trade = False
    side = 0
    entry = stop0 = stop = tp1 = tp2 = 0.0
    hits = 0
    entry_i = 0
    last_exit = -10_000
    loss_run_day = 0
    cur_day = None

    for i in range(60, n):
        b = bars[i]
        a = atr[i] or 0.0
        if a <= 0:
            continue
        day = b.ts.date() if isinstance(b.ts, datetime) else None
        if day != cur_day:
            cur_day = day
            loss_run_day = 0

        rng = max(b.high - b.low, 1e-9)
        body = abs(b.close - b.open)
        cloud_dir = 1 if fast[i] > slow[i] else -1
        vol_spike = vols[i] > 1.2 * vol_ma[i] if vol_ma[i] > 0 else body > 0.7 * a
        pb_up = max(fast[i], ema9[i])
        pb_dn = min(fast[i], ema9[i])

        # bias: the cloud plus momentum, standing in for the multi-timeframe row
        bias = 0
        if cloud_dir > 0 and b.close > slow[i]:
            bias = 1
        elif cloud_dir < 0 and b.close < slow[i]:
            bias = -1

        if bias > 0 and b.low <= pb_up + 0.20 * a:
            arm_up, arm_dn = True, False
        if bias < 0 and b.high >= pb_dn - 0.20 * a:
            arm_dn, arm_up = True, False
        if bias <= 0:
            arm_up = False
        if bias >= 0:
            arm_dn = False

        cont_up = arm_up and b.close > b.open and b.close > highs[i - 1] and b.close > fast[i] and body > 0.25 * a
        cont_dn = arm_dn and b.close < b.open and b.close < lows[i - 1] and b.close < fast[i] and body > 0.25 * a
        ant_up = anticipate and arm_up and b.low <= pb_up + 0.20 * a and b.close > (b.high + b.low) / 2 and b.close > slow[i]
        ant_dn = anticipate and arm_dn and b.high >= pb_dn - 0.20 * a and b.close < (b.high + b.low) / 2 and b.close < slow[i]
        if cont_up or ant_up:
            arm_up = False
        if cont_dn or ant_dn:
            arm_dn = False

        mom_up = (rsi[i] > 50 and rsi[i] > rsi[i - 1]) or rsi[i] < 32
        mom_dn = (rsi[i] < 50 and rsi[i] < rsi[i - 1]) or rsi[i] > 68
        q_up = (20 if bias > 0 else 0) + (20 if vol_spike else 0) + \
               (20 if (cont_up or ant_up) else 0) + (20 if cloud_dir > 0 else 0) + (20 if mom_up else 0)
        q_dn = (20 if bias < 0 else 0) + (20 if vol_spike else 0) + \
               (20 if (cont_dn or ant_dn) else 0) + (20 if cloud_dir < 0 else 0) + (20 if mom_dn else 0)

        impulse = body / a + (min(2.0, vols[i] / vol_ma[i]) if vol_ma[i] > 0 else 1.0) + \
            (1.0 if adx[i] >= 25 and adx[i] > adx[i - 1] else 0.0)
        strong = impulse >= 3.4

        # ---------------------------------------------------------- management
        if in_trade:
            if hits < 1 and (b.high >= tp1 if side > 0 else b.low <= tp1):
                hits = 1
            if hits < 2 and (b.high >= tp2 if side > 0 else b.low <= tp2):
                hits = 2
            if hits >= 1:
                be = entry + COST_R * abs(entry - stop0) * (1 if side > 0 else -1)
                stop = max(stop, be) if side > 0 else min(stop, be)
            if hits >= 2:
                ch = b.close - prof.trail * a if side > 0 else b.close + prof.trail * a
                stop = max(stop, ch) if side > 0 else min(stop, ch)

            hard = hits < 1 and (b.low <= stop0 if side > 0 else b.high >= stop0)
            soft = hits >= 1 and (b.close < stop if side > 0 else b.close > stop)
            dead = hits < 1 and (i - entry_i) >= prof.time_stop
            if hard or soft or dead:
                risk = abs(entry - stop0)
                gapped = b.open < stop0 if side > 0 else b.open > stop0
                exit_px = (b.open if gapped else stop0) if hard else b.close
                gross = ((exit_px - entry) if side > 0 else (entry - exit_px)) / risk if risk else 0.0
                t = Trade(side, entry, stop0, entry_i, i, gross - COST_R, i - entry_i,
                          "وقف" if hard else "مهلة" if dead else "متحرك")
                res.trades.append(t)
                loss_run_day = 0 if t.r_net > 0 else loss_run_day + 1
                in_trade = False
                last_exit = i
            continue

        # ------------------------------------------------------------- filling
        if p_dir != 0:
            hit_lim = b.low <= p_limit if p_dir > 0 else b.high >= p_limit
            hit_brk = p_break > 0 and (b.high >= p_break if p_dir > 0 else b.low <= p_break)
            if hit_lim or hit_brk:
                fill = p_break if hit_brk else (min(b.open, p_limit) if p_dir > 0 else max(b.open, p_limit))
                side = p_dir
                entry = fill
                stop = min(p_stop, entry - 0.5 * a) if side > 0 else max(p_stop, entry + 0.5 * a)
                stop0 = stop
                tp1 = entry + prof.tp1 * a * side
                tp2 = entry + prof.tp2 * a * side
                hits = 0
                entry_i = i
                in_trade = True
                p_dir = 0
                continue
            if (i - p_bar) >= 3 or (b.close < p_stop if p_dir > 0 else b.close > p_stop):
                res.cancelled += 1
                p_dir = 0

        # ------------------------------------------------------------- arming
        if p_dir != 0 or (i - last_exit) <= prof.cool:
            continue
        if max_loss_day and loss_run_day >= max_loss_day:
            continue
        long_ok = (cont_up or ant_up) and q_up >= min_quality
        short_ok = (cont_dn or ant_dn) and q_dn >= min_quality and not long_ok
        if not long_ok and not short_ok:
            continue
        d = 1 if long_ok else -1
        at_zone = (ant_up and not cont_up) if d > 0 else (ant_dn and not cont_dn)
        d_lim = 0.3 if strong else 0.5
        if at_zone:
            p_limit = min(pb_up, b.close) if d > 0 else max(pb_dn, b.close)
        else:
            p_limit = b.high - d_lim * rng if d > 0 else b.low + d_lim * rng
        p_break = (b.high + 0.06 * a if d > 0 else b.low - 0.06 * a) if strong else 0.0
        base = min(b.low, p_limit) if d > 0 else max(b.high, p_limit)
        p_stop = base - prof.stop * a if d > 0 else base + prof.stop * a
        p_dir = d
        p_bar = i

    return res


def format_sweep(rows: list[Outcome], symbol_count: int, months: int) -> str:
    """One message that answers which timeframe carries an edge."""
    if not rows:
        return "لا نتائج — تعذر جلب الشموع."
    lines = [
        f"🔬 مسح الفريمات — {symbol_count} رمز · {months} شهر",
        "كل الأرقام صافية بعد تكلفة العقد.",
        "",
    ]
    ranked = sorted(rows, key=lambda r: r.profit_factor, reverse=True)
    for r in ranked:
        if r.total == 0:
            lines.append(f"▪️ {r.minutes}د ({r.profile}): لا صفقات")
            continue
        gap = r.win_rate - r.breakeven_win_rate
        verdict = "✅" if r.profit_factor >= 1.3 else "🟡" if r.profit_factor >= 1.0 else "🔴"
        lines.append(
            f"{verdict} {r.minutes}د ({r.profile}) — {r.total} صفقة\n"
            f"   عامل الربح {r.profit_factor:.2f} · متوسط "
            f"{'+' if r.avg_r >= 0 else ''}{r.avg_r:.2f}R · المحصلة "
            f"{'+' if r.total_r >= 0 else ''}{r.total_r:.0f}R\n"
            f"   نجاح {r.win_rate:.0f}% · التعادل يحتاج "
            f"{r.breakeven_win_rate:.0f}% · الفارق "
            f"{'+' if gap >= 0 else ''}{gap:.0f} نقطة\n"
            f"   عمر الرابحة {r.avg_bars_win:.0f} شمعة · الخاسرة "
            f"{r.avg_bars_loss:.0f} · أطول سلسلة خسائر {r.max_loss_run}"
        )
    best = ranked[0]
    if best.total and best.profit_factor >= 1.0:
        lines.append(f"\n🏆 الأفضل: {best.minutes} دقيقة ({best.profile}).")
    else:
        lines.append(
            "\n⚠️ لا فريم يعبر عامل ربح 1.0. المشكلة في الدخول لا في الإدارة،"
            " وضبط الأرقام بعد هذا تفصيل على مقاس الماضي."
        )
    return "\n".join(lines)
