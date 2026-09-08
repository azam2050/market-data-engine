"""MIRSAD 9 on the server.

A faithful port of ``tradingview/mirsad9.pine``: the same impulse engine,
the same zone / confirmation entry, the same stop, targets, secure level
and trailing — evaluated over a symbol's bars so the customer's desk can
show, without TradingView, exactly what the chart would show.

Everything is a pure function of the bars: no state survives between
calls, so a refresh simply replays the day. Prices are those a customer
can actually get — the zone level on a touch, otherwise the open of the
bar *after* the one that confirmed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from qqq_alpha.domain import Bar

NY = ZoneInfo("America/New_York")

# minutes since midnight New York
OPEN_MIN = 9 * 60 + 30
CLOSE_MIN = 16 * 60

STATE_TEXT_UP = "ميل صاعد"
STATE_TEXT_DOWN = "ميل هابط"
STATE_TEXT_SIDEWAYS = "عرضي — انتظار"


@dataclass
class Params:
    """The indicator's defaults, named as in the Pine inputs."""

    bodyK: float = 0.8
    closeP: float = 0.65
    brkN: int = 6
    orbN: int = 20
    revK: float = 1.5
    adxOn: bool = True
    adxLvl: float = 18.0
    boxN: int = 30
    coolN: int = 2
    htfOn: bool = False
    skipOpen: int = 1
    lateN: int = 15
    late3N: int = 120
    lifeN: int = 5
    chaseN: int = 8
    maxChase: float = 1.5
    reEntry: bool = True
    reLife: int = 40
    minQ: float = 0.0
    stopX: float = 1.5
    x1: float = 1.0
    x2: float = 2.0
    x3: float = 3.0
    secPct: float = 35.0
    trailN: int = 3
    t3Follow: bool = False
    eodOut: bool = True
    chaseOn: bool = True
    waitZone: bool = True


@dataclass
class LastTrade:
    side: int
    entry: float
    exit_text: str
    r: float
    hit: int
    half: bool
    bars: int
    closed_at: datetime


@dataclass
class SymbolState:
    """What the corner card shows for one symbol on one frame."""

    symbol: str
    frame: int
    price: float
    atr: float
    trend: int  # +1 up, -1 down, 0 sideways
    quality_now: float
    # the trade / zone machine
    pos: int = 0
    pending: int = 0  # Pine's pdxMain: ±1 entering next bar, ±2 zone ready, ±3 second-entry zone
    level: float | None = None  # zone level while a zone lives
    entry: float | None = None
    stop: float | None = None
    t1: float | None = None
    t2: float | None = None
    t3: float | None = None
    half_level: float | None = None
    hit: int = 0
    half: bool = False
    secured: bool = False
    bars_in: int = 0
    quality: float | None = None
    nth: int = 0
    entered_at: datetime | None = None
    blocked: str = ""
    raw_side: int = 0
    last: LastTrade | None = None
    fm_win: int = 0
    fm_tot: int = 0
    last_bar: datetime | None = None
    late_window: bool = False
    bars_today: int = 0
    closes: list[float] | None = None  # the last closes, for a sparkline

    @property
    def side(self) -> int:
        """The direction the contract would be on: the trade, else the zone."""
        if self.pos:
            return self.pos
        if self.pending:
            return 1 if self.pending > 0 else -1
        return 0

    @property
    def direction(self) -> int:
        """Pine's mainDir: trade, else zone, else trend."""
        return self.side or self.trend

    @property
    def state_key(self) -> str:
        if self.pos:
            return "secured" if self.secured else "trade"
        if self.pending:
            return "zone"
        if self.blocked:
            return "blocked"
        return {1: "up", -1: "down"}.get(self.trend, "sideways")

    @property
    def state_text(self) -> str:
        return state_text(self.pos, self.pending, self.secured, self.trend, self.raw_side, self.blocked) + (
            " · دخول ثاني" if self.pos and self.nth >= 2 else ""
        )

    def as_dict(self) -> dict[str, Any]:
        d = {
            "symbol": self.symbol,
            "frame": self.frame,
            "price": self.price,
            "atr": self.atr,
            "trend": self.trend,
            "side": self.side,
            "direction": self.direction,
            "state": self.state_key,
            "state_text": self.state_text,
            "pos": self.pos,
            "pending": self.pending,
            "level": self.level,
            "entry": self.entry,
            "stop": self.stop,
            "t1": self.t1,
            "t2": self.t2,
            "t3": self.t3,
            "half_level": self.half_level,
            "hit": self.hit,
            "half": self.half,
            "secured": self.secured,
            "bars_in": self.bars_in,
            "quality": self.quality,
            "quality_now": self.quality_now,
            "nth": self.nth,
            "entered_at": self.entered_at.isoformat() if self.entered_at else None,
            "blocked": self.blocked,
            "fm_win": self.fm_win,
            "fm_tot": self.fm_tot,
            "last_bar": self.last_bar.isoformat() if self.last_bar else None,
            "late_window": self.late_window,
            "closes": self.closes or [],
            "last": None,
        }
        if self.last:
            d["last"] = {
                "side": self.last.side,
                "entry": self.last.entry,
                "exit_text": self.last.exit_text,
                "r": self.last.r,
                "hit": self.last.hit,
                "half": self.last.half,
                "bars": self.last.bars,
                "closed_at": self.last.closed_at.isoformat(),
            }
        return d


def state_text(pos: int, pending: int, secured: bool, trend: int, raw: int, blocked: str) -> str:
    """Pine's f_stateTxt, word for word."""
    if pos == 1:
        return "كول · مؤمَّنة" if secured else "كول قائمة"
    if pos == -1:
        return "بوت · مؤمَّنة" if secured else "بوت قائمة"
    texts = {
        1: "كول على الافتتاح", -1: "بوت على الافتتاح",
        2: "منطقة كول جاهزة", -2: "منطقة بوت جاهزة",
        3: "منطقة كول للدخول الثاني", -3: "منطقة بوت للدخول الثاني",
    }
    if pending in texts:
        return texts[pending]
    if raw and blocked:
        return ("كول محجوبة: " if raw == 1 else "بوت محجوبة: ") + blocked
    if trend == 0:
        return STATE_TEXT_SIDEWAYS
    return STATE_TEXT_UP if trend == 1 else STATE_TEXT_DOWN


# ---------------------------------------------------------------- bars
def _ny(ts: datetime) -> datetime:
    return ts.astimezone(NY)


def _minute_of_day(ts: datetime) -> int:
    local = _ny(ts)
    return local.hour * 60 + local.minute


def regular_only(bars: list[Bar]) -> list[Bar]:
    """Regular session only (09:30–16:00 New York), sorted, de-duplicated."""
    seen: dict[datetime, Bar] = {}
    for b in bars:
        m = _minute_of_day(b.ts)
        if OPEN_MIN <= m < CLOSE_MIN:
            seen[b.ts] = b
    return [seen[k] for k in sorted(seen)]


def resample(bars: list[Bar], minutes: int) -> list[Bar]:
    """Minute bars into ``minutes`` bars, slots anchored on the 09:30 open
    exactly as TradingView draws them. The last slot may be incomplete."""
    out: list[Bar] = []
    cur: Bar | None = None
    cur_slot: tuple[Any, int] | None = None
    for b in regular_only(bars):
        local = _ny(b.ts)
        slot = (local.date(), (local.hour * 60 + local.minute - OPEN_MIN) // minutes)
        if cur is None or slot != cur_slot:
            if cur is not None:
                out.append(cur)
            start_min = OPEN_MIN + slot[1] * minutes
            start = local.replace(
                hour=start_min // 60, minute=start_min % 60, second=0, microsecond=0
            )
            cur = Bar(
                symbol=b.symbol, ts=start, open=b.open, high=b.high, low=b.low,
                close=b.close, volume=b.volume,
            )
            cur_slot = slot
        else:
            cur = Bar(
                symbol=b.symbol, ts=cur.ts, open=cur.open, high=max(cur.high, b.high),
                low=min(cur.low, b.low), close=b.close, volume=cur.volume + b.volume,
            )
    if cur is not None:
        out.append(cur)
    return out


# ---------------------------------------------------------------- maths
def _rma(values: list[float], n: int) -> list[float]:
    out = [float("nan")] * len(values)
    if len(values) < n:
        return out
    s = sum(values[:n]) / n
    out[n - 1] = s
    for i in range(n, len(values)):
        s = (s * (n - 1) + values[i]) / n
        out[i] = s
    return out


def _ema(values: list[float], n: int) -> list[float]:
    out = [0.0] * len(values)
    if not values:
        return out
    a = 2.0 / (n + 1)
    out[0] = values[0]
    for i in range(1, len(values)):
        out[i] = a * values[i] + (1 - a) * out[i - 1]
    return out


def _isnan(x: float) -> bool:
    return x != x


@dataclass
class _Core:
    """Per-bar values of the Pine ``f_core``."""

    okL: bool
    okS: bool
    q: float
    atr: float
    e9: float
    state: int  # 9 sideways, 1 up, -1 down
    block: str
    raw: int
    late: bool
    minute: int
    bar_of_day: int


def _core(bars: list[Bar], frame: int, p: Params, confirmed_upto: int) -> list[_Core]:
    n = len(bars)
    o = [b.open for b in bars]
    h = [b.high for b in bars]
    lo = [b.low for b in bars]
    c = [b.close for b in bars]
    v = [float(b.volume) for b in bars]
    tr = [h[0] - lo[0]] + [
        max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1])) for i in range(1, n)
    ]
    atr = _rma(tr, 14)
    e9 = _ema(c, 9)
    eH1 = _ema(c, 27)
    eH2 = _ema(c, 63)
    up = [0.0] + [h[i] - h[i - 1] for i in range(1, n)]
    dn = [0.0] + [lo[i - 1] - lo[i] for i in range(1, n)]
    dmp = [u if (u > d and u > 0) else 0.0 for u, d in zip(up, dn, strict=True)]
    dmm = [d if (d > u and d > 0) else 0.0 for u, d in zip(up, dn, strict=True)]
    sp, sm, satr = _rma(dmp, 14), _rma(dmm, 14), _rma(tr, 14)
    dip = [100 * sp[i] / satr[i] if not _isnan(satr[i]) and satr[i] > 0 else 0.0 for i in range(n)]
    dim = [100 * sm[i] / satr[i] if not _isnan(satr[i]) and satr[i] > 0 else 0.0 for i in range(n)]
    dx = [
        100 * abs(dip[i] - dim[i]) / (dip[i] + dim[i]) if (dip[i] + dim[i]) > 0 else 0.0
        for i in range(n)
    ]
    adx = _rma(dx, 14)
    vavg = [sum(v[max(0, i - 19): i + 1]) / min(i + 1, 20) for i in range(n)]

    out: list[_Core] = []
    last_bar = -100000
    or_hi = or_lo = None
    for i in range(n):
        minute = _minute_of_day(bars[i].ts)
        bar_day = (minute - OPEN_MIN) // frame
        bars_today = max(bar_day, 0)
        first_bar = i == 0 or _ny(bars[i].ts).date() != _ny(bars[i - 1].ts).date()
        if first_bar:
            or_hi, or_lo = h[i], lo[i]
        a = atr[i]
        rng = h[i] - lo[i]
        body = c[i] - o[i]
        hiN, loN = h[i - 1] if i else h[i], lo[i - 1] if i else lo[i]
        for k in range(2, p.brkN + 1):
            if k <= bars_today and i - k >= 0:
                hiN = max(hiN, h[i - k])
                loN = min(loN, lo[i - k])
        ready = i >= 120 and not _isnan(a) and not _isnan(adx[i])
        side0 = 0
        q = 0.0
        if ready and rng > 0:
            up_close = (c[i] - lo[i]) / rng >= p.closeP
            dn_close = (h[i] - c[i]) / rng >= p.closeP
            impL = body >= p.bodyK * a and up_close and c[i] > hiN
            impS = -body >= p.bodyK * a and dn_close and c[i] < loN
            revL = p.revK > 0 and body >= p.revK * a and up_close and i > 0 and c[i] > h[i - 1]
            revS = p.revK > 0 and -body >= p.revK * a and dn_close and i > 0 and c[i] < lo[i - 1]
            orb_ok = p.orbN > 0 and 1 <= bars_today <= p.orbN and or_hi is not None and i > 0
            orbL = orb_ok and body >= p.bodyK * a and up_close and c[i] > or_hi and c[i - 1] <= or_hi
            orbS = orb_ok and -body >= p.bodyK * a and dn_close and c[i] < or_lo and c[i - 1] >= or_lo
            side0 = 1 if (impL or revL or orbL) else -1 if (impS or revS or orbS) else 0
            margin = (c[i] - hiN) if side0 == 1 else (loN - c[i]) if side0 == -1 else 0.0
            htf_ok = (eH1[i] > eH2[i]) if side0 == 1 else (eH1[i] < eH2[i]) if side0 == -1 else False
            q = (
                30.0 * min(abs(body) / a, 2.0) / 2.0
                + 20.0 * min(max(margin, 0.0) / a, 1.0)
                + (20.0 if htf_ok else 0.0)
                + 15.0 * min(adx[i] / 40.0, 1.0)
                + (15.0 * min(v[i] / vavg[i], 2.0) / 2.0 if vavg[i] > 0 else 0.0)
            )
        else:
            htf_ok = False
        box_hi = max(h[max(0, i - p.boxN): i]) if i > 0 else h[i]
        box_lo = min(lo[max(0, i - p.boxN): i]) if i > 0 else lo[i]
        box_w = box_hi - box_lo
        inside = box_lo + 0.25 * box_w < c[i] < box_hi - 0.25 * box_w
        sideways = p.adxOn and not _isnan(adx[i]) and adx[i] < p.adxLvl and inside
        late_eff = max(p.lateN, p.late3N) if (frame <= 3 and p.late3N > 0) else p.lateN
        late = late_eff > 0 and minute >= CLOSE_MIN - late_eff
        in_cool = i - last_bar <= p.coolN
        common = (
            ready and side0 != 0 and not sideways and (not p.htfOn or htf_ok)
            and q >= p.minQ and not in_cool and bar_day >= p.skipOpen and not late
        )
        okL = common and side0 == 1
        okS = common and side0 == -1
        if (okL or okS) and i < confirmed_upto:
            last_bar = i
        block = ""
        if side0 != 0:
            block = (
                "راحة" if in_cool else "عرضي" if sideways
                else "ضد الفريم الأكبر" if (p.htfOn and not htf_ok)
                else "جودة منخفضة" if q < p.minQ else ""
            )
        state = 9 if sideways else 1 if eH1[i] > eH2[i] else -1
        out.append(_Core(okL, okS, q, a, e9[i], state, block, side0, late, minute, bar_day))
    return out


# ---------------------------------------------------------------- the trade
def evaluate(
    bars: list[Bar],
    frame: int,
    params: Params | None = None,
    now: datetime | None = None,
    trace: list[LastTrade] | None = None,
) -> SymbolState | None:
    """Replay the Pine trade machine over ``bars`` (already on ``frame``)
    and return the state at the last bar. ``now`` decides whether the last
    bar is still forming: signals only stamp on confirmed bars, while
    stops and targets — as on the chart — react to the forming one.
    ``trace``, when given, collects every closed trade of the replay."""
    p = params or Params()
    if len(bars) < 130:
        return None
    from datetime import timedelta

    last_ts = bars[-1].ts
    forming = now is not None and now < last_ts + timedelta(minutes=frame)
    confirmed_upto = len(bars) - 1 if forming else len(bars)
    core = _core(bars, frame, p, confirmed_upto)

    n = len(bars)
    h = [b.high for b in bars]
    lo = [b.low for b in bars]
    o = [b.open for b in bars]
    c = [b.close for b in bars]

    pSide = 0
    pLvl = pExp = pBorn = 0
    pHi = pLo = pAtr = pQ = 0.0
    zSide = 0
    zLvl = 0.0
    zBorn = zUses = 0
    zStopped = False
    fmOn = False
    fmUp = fmDn = 0.0
    fmBars = fmWin = fmTot = 0
    pos = pend = 0
    chase_pend = 0
    viaZone = False
    fillLvl = 0.0
    touched_at_fill = False
    eP = sP = t1 = t2 = t3 = halfLvl = unit = 0.0
    qT = 0.0
    hit = 0
    secured = half = False
    nth = barsIn = 0
    exitTxt = ""
    lastR = 0.0
    lastSide = 0
    last: LastTrade | None = None
    entered_at: datetime | None = None

    for i in range(n):
        k = core[i]
        confirmed = i < confirmed_upto
        closed = False
        first_bar = i == 0 or _ny(bars[i].ts).date() != _ny(bars[i - 1].ts).date()
        if first_bar:
            pSide = 0
            zSide = 0
            chase_pend = 0
        eod_bar = p.eodOut and k.minute >= 15 * 60 + 55
        trail_lo = min(lo[max(0, i - p.trailN + 1): i + 1])
        trail_hi = max(h[max(0, i - p.trailN + 1): i + 1])

        # a confirmation seen at the previous close fills at this open
        if chase_pend and pos == 0 and pend == 0:
            pend = chase_pend
            viaZone = False
            zUses = 1
            chase_pend = 0

        touched = (
            confirmed and pos == 0 and pend == 0 and pSide != 0 and i <= pExp
            and ((lo[i] <= pLvl) if pSide == 1 else (h[i] >= pLvl))
        )
        reTouch = (
            p.reEntry and confirmed and pos == 0 and pend == 0 and pSide == 0 and zSide != 0
            and zUses == 1 and not zStopped and i <= zBorn + p.reLife
            and ((lo[i] <= zLvl) if zSide == 1 else (h[i] >= zLvl))
        )
        chased = (
            p.chaseOn and confirmed and pos == 0 and pend == 0 and pSide != 0 and not touched
            and i <= pBorn + p.chaseN and ((c[i] > pHi) if pSide == 1 else (c[i] < pLo))
            and abs(c[i] - pLvl) <= p.maxChase * pAtr
        )
        touched_at_fill = False
        if touched:
            pend, pSide, viaZone, fillLvl, zUses = pSide, 0, True, pLvl, 1
            touched_at_fill = True
        elif chased:
            chase_pend, pSide = pSide, 0
        elif reTouch:
            pend, viaZone, fillLvl, zUses = zSide, True, zLvl, 2

        if pend != 0 and pos == 0 and i > 0 and not _isnan(core[i - 1].atr):
            pos = pend
            nth = zUses
            eP = (min(o[i], fillLvl) if pos == 1 else max(o[i], fillLvl)) if viaZone else o[i]
            aRef = pAtr if touched_at_fill else core[i - 1].atr
            qT = pQ if touched_at_fill else core[i - 1].q
            unit = p.stopX * aRef
            sP = eP - unit if pos == 1 else eP + unit
            fmOn, fmBars = True, 0
            fmUp = eP + aRef if pos == 1 else eP - aRef
            fmDn = eP - aRef if pos == 1 else eP + aRef
            m1, m2, m3 = p.x1 * aRef, p.x2 * aRef, p.x3 * aRef
            t1 = eP + m1 if pos == 1 else eP - m1
            t2 = eP + m2 if pos == 1 else eP - m2
            t3 = eP + m3 if pos == 1 else eP - m3
            halfLvl = eP + p.secPct / 100.0 * m3 if pos == 1 else eP - p.secPct / 100.0 * m3
            hit, secured, half, barsIn, exitTxt = 0, False, False, 0, ""
            pend, viaZone = 0, False
            entered_at = bars[i].ts
        elif pend != 0 and pos == 0:
            pend, viaZone = 0, False

        if fmOn:
            fmBars += 1
            s_ = 1 if fmUp > fmDn else -1
            agn = lo[i] <= fmDn if s_ == 1 else h[i] >= fmDn
            wth = h[i] >= fmUp if s_ == 1 else lo[i] <= fmUp
            if agn or wth or fmBars > 20:
                fmTot += 1
                if wth and not agn:
                    fmWin += 1
                fmOn = False

        if pos != 0:
            barsIn += 1
            hit_stop = lo[i] <= sP if pos == 1 else h[i] >= sP
            if hit_stop:
                lastR = (sP - eP) / unit * pos
                gain = (sP - eP) * pos
                exitTxt = (
                    "خروج بعد هدف ٣" if hit >= 3 else "خروج بربح" if gain > 0
                    else "خروج على التعادل" if gain == 0 else "وقف"
                )
                if gain < 0:
                    zStopped = True
                lastSide, pos, closed = pos, 0, True
            else:
                if pos == 1:
                    hit = 3 if h[i] >= t3 else 2 if h[i] >= t2 else 1 if h[i] >= t1 else hit
                else:
                    hit = 3 if lo[i] <= t3 else 2 if lo[i] <= t2 else 1 if lo[i] <= t1 else hit
                if not half and ((h[i] >= halfLvl) if pos == 1 else (lo[i] <= halfLvl)):
                    half = True
                if hit >= 3 and not p.t3Follow:
                    lastR = (t3 - eP) / unit * pos
                    exitTxt, lastSide, pos, closed = "هدف ٣", pos, 0, True
                elif eod_bar and confirmed:
                    lastR = (c[i] - eP) / unit * pos
                    exitTxt, lastSide, pos, closed = "إغلاق الجلسة", pos, 0, True
                else:
                    if half or hit >= 1:
                        sP = max(sP, eP) if pos == 1 else min(sP, eP)
                    if hit >= 2:
                        sP = max(sP, t1, trail_lo) if pos == 1 else min(sP, t1, trail_hi)
                    if hit >= 3:
                        sP = max(sP, t2, trail_lo) if pos == 1 else min(sP, t2, trail_hi)
                    secured = sP >= eP if pos == 1 else sP <= eP
        if closed:
            last = LastTrade(lastSide, eP, exitTxt, lastR, hit, half, barsIn, bars[i].ts)
            if trace is not None:
                trace.append(last)

        if confirmed and pos == 0 and pend == 0 and chase_pend == 0 and not closed:
            side0 = 1 if k.okL else -1 if k.okS else 0
            if side0 != 0:
                lvl = min(k.e9, c[i]) if side0 == 1 else max(k.e9, c[i])
                zSide, zLvl, zBorn, zStopped = side0, lvl, i, False
                if p.waitZone:
                    pSide, pLvl, pExp, pBorn = side0, lvl, i + p.lifeN, i
                    pHi, pLo, pAtr, pQ = h[i], lo[i], k.atr, k.q
                    zUses = 0
                else:
                    pend, zUses = side0, 1
        if pSide != 0 and (i > (max(pExp, pBorn + p.chaseN) if p.chaseOn else pExp) or pos != 0):
            pSide = 0
        if zSide != 0 and pos == 0 and pend == 0 and (
            i > zBorn + p.reLife or zUses >= 2 or zStopped or (zUses == 0 and pSide == 0)
        ):
            zSide = 0

    k = core[-1]
    pending = (
        pend if pend else (pSide * 2 if pSide else (chase_pend if chase_pend else 0))
    )
    if not pending and zSide and zUses == 1 and not zStopped and pos == 0:
        pending = zSide * 3
    trend = 0 if k.state == 9 else k.state
    st = SymbolState(
        symbol=bars[-1].symbol,
        frame=frame,
        price=c[-1],
        atr=0.0 if _isnan(k.atr) else k.atr,
        trend=trend,
        quality_now=k.q,
        pos=pos,
        pending=pending,
        level=(pLvl if pSide else zLvl if zSide else None),
        entry=eP if pos else None,
        stop=sP if pos else None,
        t1=t1 if pos else None,
        t2=t2 if pos else None,
        t3=t3 if pos else None,
        half_level=halfLvl if pos else None,
        hit=hit if pos else 0,
        half=half if pos else False,
        secured=secured if pos else False,
        bars_in=barsIn if pos else 0,
        quality=qT if pos else (pQ if pSide else None),
        nth=nth if pos else 0,
        entered_at=entered_at if pos else None,
        blocked=k.block,
        raw_side=k.raw,
        last=last,
        fm_win=fmWin,
        fm_tot=fmTot,
        last_bar=bars[-1].ts,
        late_window=k.late,
        bars_today=k.bar_of_day + 1,
        closes=[round(x, 4) for x in c[-40:]],
    )
    if last is not None and pos == 0:
        st.entry = None
    return st


# ---------------------------------------------------------------- frames
def next_frame(frame: int) -> int:
    """The chart's higher frame, the same ladder as the Pine card."""
    if frame <= 1:
        return 5
    if frame <= 5:
        return 15
    if frame <= 30:
        return 60
    if frame <= 60:
        return 240
    return 1440


def frame_name(frame: int) -> str:
    return {
        1: "١ د", 2: "٢ د", 3: "٣ د", 5: "٥ د", 10: "١٠ د", 15: "١٥ د", 30: "٣٠ د",
        45: "٤٥ د", 60: "ساعة", 120: "ساعتان", 240: "٤ س", 1440: "يومي",
    }.get(frame, f"{frame} د")
