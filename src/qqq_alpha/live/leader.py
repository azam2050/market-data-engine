"""قائد اليوم — the leader of the day.

The rule the lab kept after sixty sessions of QQQ and SPY on the
five-minute frame: when three of the ten largest index names print a
MIRSAD impulse the same way within the same two bars, the leader's own bar
closes that way, and the leader's own trend (EMA 27 over EMA 63) points
that way too, the leader is taken at its MIRSAD zone with the stop 1.5 ATR
away and a full exit one ATR ahead — at most two accepted signals a day,
flat by the bell. A signal against the leader's trend is shown and refused:
it was measured at 69% against 78% with the trend. Nothing else — hour
filters, wider baskets, a second target — was measured and lost.

Everything is a pure replay of the bars, like ``mirsad9.evaluate``: a
refresh cannot disagree with the previous one, and the record the customer
sees is the record the rule produced. The service adds the day's contract,
the basket tiles, the morning read and the honest reason for "no trade".
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from qqq_alpha.data.calendar import is_trading_day
from qqq_alpha.domain import Bar
from qqq_alpha.live import mirsad9
from qqq_alpha.live.desk import (
    DeskService,
    _day_notes,
    _expiry_text,
    _pct,
    _session_info,
    contract_price_at,
    expiry_for,
)
from qqq_alpha.live.mirsad9 import NY, Params
from qqq_alpha.live.tvbridge import pick_contract

log = logging.getLogger(__name__)

LEADERS: tuple[str, ...] = ("QQQ", "SPY")
LEADER_NAMES = {"QQQ": "ناسداك ١٠٠", "SPY": "إس آند بي ٥٠٠"}
# the ten largest weights of QQQ and SPY: the basket that carried the signal.
# Re-measured monthly; the names change with the index.
BASKET: tuple[str, ...] = ("NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "AVGO", "TSLA", "NFLX", "COST")
FRAME = 5
BLOCK_MIN = 3          # basket names moving the same way over this bar and the last
DAILY_CAP = 2          # opportunities a day, filled or not
FIRST_SIGNAL_BAR = 2   # the third bar of the day (09:40) is the first that may signal
LAST_SIGNAL_BAR = 75   # bars 0..74 (09:30..15:40) may signal; nothing from 15:45
TOUCH_BARS = 5         # the zone lives this many bars
CHASE_BARS = 8         # a close beyond the signal bar within this many bars confirms
MAX_CHASE_ATR = 1.5    # …unless the price is already this far from the zone
STOP_ATR = 1.5
TARGET_ATR = 1.0
EOD_MINUTE = 15 * 60 + 55
# one opportunity at a time: a signal that arrives while an earlier one is
# still waiting for its fill, or while a trade is open, is shown and refused
# — and still spends one of the day's two slots. Measured against the other
# readings of the lab's rule, this was the most even in and out of sample.
EXCLUSIVE = "any"
SKIPPED_COUNT = True
LOOKBACK_DAYS = 16
BOARD_TTL_SEC = 20

# the raw impulse: MIRSAD's candle with every optional filter off
RAW = Params(adxOn=False, coolN=0, minQ=0.0, htfOn=False, skipOpen=1, lateN=15, late3N=0)

# what the sixty-session measurement said; shown as-is and re-measured monthly
MEASURED = {
    "sessions": 60,
    "from": "2026-06-12",
    "to": "2026-09-08",
    "trades": 99,
    "win_pct": 80,
    "profit_factor": 2.60,
    "in_sample_pf": 2.61,
    "out_sample_pf": 2.60,
    "avg_r": 0.32,
    "contract_win_pct": 79,
    "contract_median_pct": 36,
    "contract_q1_pct": 11,
    "contract_q3_pct": 44,
    "per_day": 1.65,
    "days_without": 17,
    "longest_losing_streak": 3,
    "max_drawdown_r": -3.3,
    "exits": {"target": 78, "stop": 20, "eod": 1},
    "grade_a": {"trades": 75, "win_pct": 83, "profit_factor": 3.18},
    "grade_b": {"trades": 24, "win_pct": 71, "profit_factor": 1.54},
    "against_trend": {"trades": 110, "win_pct": 65, "profit_factor": 1.32},
}


# ---------------------------------------------------------------- the rule
@dataclass
class Opportunity:
    """One signal on a leader, from the bar that fired it to its exit."""

    symbol: str
    n: int                      # 1 or 2: which accepted signal of the day (0 when refused)
    side: int
    net: int                    # the basket count that fired it
    signal_i: int
    signal_ts: datetime
    atr: float
    level: float                # the MIRSAD zone (EMA9 or the signal close)
    sig_hi: float
    sig_lo: float
    trend: int = 0              # the leader's own trend at the signal (EMA 27 vs 63)
    grade: str = ""             # "أ" when the day's direction agrees too, else "ب"
    status: str = "waiting"     # waiting | chase | open | closed | cancelled | rejected
    reason: str = ""            # why it was cancelled or refused
    entry_i: int | None = None
    entry_ts: datetime | None = None
    entry: float | None = None
    stop: float | None = None
    target: float | None = None
    via_zone: bool = False
    exit_i: int | None = None
    exit_ts: datetime | None = None
    exit: float | None = None
    how: str = ""               # target | stop | eod
    r: float = 0.0
    best: float | None = None   # the best price seen while open
    bars_in: int = 0

    @property
    def day(self) -> date:
        return self.signal_ts.astimezone(NY).date()

    def fill(self, i: int, ts: datetime, px: float, via_zone: bool) -> None:
        self.status, self.entry_i, self.entry_ts, self.entry, self.via_zone = "open", i, ts, px, via_zone
        self.stop = px - self.side * STOP_ATR * self.atr
        self.target = px + self.side * TARGET_ATR * self.atr
        self.best = px

    def close(self, i: int, ts: datetime, px: float, how: str) -> None:
        self.status, self.exit_i, self.exit_ts, self.exit, self.how = "closed", i, ts, px, how
        if self.entry is not None and self.atr > 0:
            self.r = (px - self.entry) * self.side / (STOP_ATR * self.atr)

    def cancel(self, reason: str) -> None:
        self.status, self.reason = "cancelled", reason

    @property
    def move_pct(self) -> float | None:
        if self.entry is None or self.exit is None or self.entry <= 0:
            return None
        return round(100.0 * (self.exit - self.entry) * self.side / self.entry, 2)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "n": self.n,
            "side": self.side,
            "side_text": "كول" if self.side > 0 else "بوت",
            "net": self.net,
            "signal_ts": self.signal_ts.isoformat(),
            "signal_time": _hhmm(self.signal_ts),
            "atr": round(self.atr, 4),
            "level": round(self.level, 2),
            "trend": self.trend,
            "grade": self.grade,
            "status": self.status,
            "reason": self.reason,
            "entry_ts": self.entry_ts.isoformat() if self.entry_ts else None,
            "entry_time": _hhmm(self.entry_ts) if self.entry_ts else None,
            "entry": _r2(self.entry),
            "stop": _r2(self.stop),
            "target": _r2(self.target),
            "via_zone": self.via_zone,
            "exit_ts": self.exit_ts.isoformat() if self.exit_ts else None,
            "exit_time": _hhmm(self.exit_ts) if self.exit_ts else None,
            "exit": _r2(self.exit),
            "how": self.how,
            "how_text": {"target": "الهدف", "stop": "الوقف", "eod": "إغلاق الجلسة"}.get(self.how, ""),
            "r": round(self.r, 2),
            "move_pct": self.move_pct,
            "best": _r2(self.best),
            "bars_in": self.bars_in,
        }


def _hhmm(ts: datetime) -> str:
    return ts.astimezone(NY).strftime("%H:%M")


def _r2(x: float | None) -> float | None:
    return None if x is None else round(x, 2)


def confirmed_count(bars: list[Bar], now: datetime | None) -> int:
    """How many bars are closed: the last one is still forming while ``now``
    is inside its slot."""
    if not bars:
        return 0
    forming = now is not None and now < bars[-1].ts + timedelta(minutes=FRAME)
    return len(bars) - 1 if forming else len(bars)


def impulses(
    bars: list[Bar], confirmed_upto: int, core: list[mirsad9.CoreBar] | None = None
) -> dict[datetime, int]:
    """The raw MIRSAD impulse side of every confirmed bar that has one,
    keyed by the bar's timestamp. The first bar of the day and the last
    fifteen minutes never count, as in the indicator."""
    out: dict[datetime, int] = {}
    for i, k in enumerate(core if core is not None else mirsad9.core(bars, FRAME, RAW)):
        if i >= confirmed_upto:
            break
        if k.raw and k.bar_of_day >= RAW.skipOpen and not k.late:
            out[bars[i].ts] = k.raw
    return out


def block_series(
    basket: dict[str, list[Bar]],
    now: datetime | None,
    cores: dict[str, list[mirsad9.CoreBar]] | None = None,
) -> dict[datetime, int]:
    """Per bar: basket impulses up minus impulses down. The rule reads this
    bar plus the last, so a name that impulses on both counts twice — two
    impulses, exactly as the lab summed them."""
    net: dict[datetime, int] = {}
    for sym, bars in basket.items():
        core = (cores or {}).get(sym)
        for ts, side in impulses(bars, confirmed_count(bars, now), core).items():
            net[ts] = net.get(ts, 0) + side
    return net


def replay(
    bars: list[Bar],
    block: dict[datetime, int],
    now: datetime | None = None,
    exclusive: str = EXCLUSIVE,
    skipped_count: bool = SKIPPED_COUNT,
    core: list[mirsad9.CoreBar] | None = None,
) -> list[Opportunity]:
    """Run the rule over a leader's five-minute bars against the basket's
    block and return every opportunity, oldest first. Signals stamp on
    confirmed bars only; fills, stops and targets react to the forming bar
    as they do on the chart.

    ``exclusive`` says what a new signal meets: "pending" refuses it while
    an earlier one still waits for its fill (the lab's rule, made causal),
    "any" also while a trade is open, "none" never. ``skipped_count`` says
    whether a refused signal still spends one of the day's slots."""
    n = len(bars)
    if n < 130:
        return []
    upto = confirmed_count(bars, now)
    if core is None:
        core = mirsad9.core(bars, FRAME, RAW)
    o = [b.open for b in bars]
    h = [b.high for b in bars]
    lo = [b.low for b in bars]
    c = [b.close for b in bars]
    opps: list[Opportunity] = []
    pending: list[Opportunity] = []
    open_: list[Opportunity] = []
    per_day: Counter[date] = Counter()
    busy = -1
    day_open = o[0]
    for i in range(n):
        confirmed = i < upto
        ts = bars[i].ts
        day = ts.astimezone(NY).date()
        k = core[i]
        if i == 0 or day != bars[i - 1].ts.astimezone(NY).date():
            day_open = o[i]
            for t in open_:
                t.close(i - 1, bars[i - 1].ts, c[i - 1], "eod")
            open_ = []
            for p in pending:
                p.cancel("انتهت الجلسة قبل التعبئة")
            pending = []

        # fills: a confirmation seen at the previous close fills at this open,
        # a touch of the zone fills at the zone
        still: list[Opportunity] = []
        for p in pending:
            kk = i - p.signal_i
            if p.status == "chase":
                p.fill(i, ts, o[i], False)
                open_.append(p)
                busy = i
                continue
            if not confirmed:
                still.append(p)
                continue
            touched = kk <= TOUCH_BARS and ((lo[i] <= p.level) if p.side > 0 else (h[i] >= p.level))
            if touched:
                px = min(o[i], p.level) if p.side > 0 else max(o[i], p.level)
                p.fill(i, ts, px, True)
                open_.append(p)
                busy = i
                continue
            beyond = (c[i] > p.sig_hi) if p.side > 0 else (c[i] < p.sig_lo)
            if kk <= CHASE_BARS and beyond and abs(c[i] - p.level) <= MAX_CHASE_ATR * p.atr:
                p.status = "chase"
                still.append(p)
                continue
            if kk >= CHASE_BARS:
                p.cancel("لا رجوع للمنطقة ولا تأكيد خلال ٨ شموع")
                continue
            still.append(p)
        pending = still

        # manage: the stop first, then the target, then the bell
        kept: list[Opportunity] = []
        for t in open_:
            t.bars_in += 1
            t.best = max(t.best or h[i], h[i]) if t.side > 0 else min(t.best or lo[i], lo[i])
            hit_stop = (lo[i] <= t.stop) if t.side > 0 else (h[i] >= t.stop)
            hit_target = (h[i] >= t.target) if t.side > 0 else (lo[i] <= t.target)
            if hit_stop:
                t.close(i, ts, float(t.stop), "stop")
            elif hit_target:
                t.close(i, ts, float(t.target), "target")
            elif confirmed and k.minute >= EOD_MINUTE:
                t.close(i, ts, c[i], "eod")
            else:
                kept.append(t)
        open_ = kept

        # a new signal: this bar and the last, the basket leaning one way,
        # the leader's bar and its own trend agreeing
        if not confirmed or i == 0 or per_day[day] >= DAILY_CAP:
            continue
        if not (FIRST_SIGNAL_BAR <= k.bar_of_day < LAST_SIGNAL_BAR) or k.atr != k.atr or k.atr <= 0:
            continue
        net = block.get(ts, 0) + block.get(bars[i - 1].ts, 0)
        side = 1 if (net >= BLOCK_MIN and c[i] > o[i]) else -1 if (net <= -BLOCK_MIN and c[i] < o[i]) else 0
        if not side:
            continue
        trend = 1 if k.state == 1 else -1 if k.state == -1 else 0
        level = min(k.e9, c[i]) if side > 0 else max(k.e9, c[i])
        opp = Opportunity(
            symbol=bars[i].symbol, n=0, side=side, net=net, signal_i=i, signal_ts=ts,
            atr=k.atr, level=level, sig_hi=h[i], sig_lo=lo[i], trend=trend,
        )
        opps.append(opp)
        if trend != side:
            opp.status, opp.reason = "rejected", "ضد ميل القائد: لا تُؤخذ"
            continue
        opp.grade = "أ" if (c[i] - day_open) * side > 0 else "ب"
        blocked_by = (
            "فرصة سابقة بانتظار التعبئة" if (exclusive in ("pending", "any") and pending)
            else "صفقة قائمة" if (exclusive == "any" and open_)
            else "جاءت في شمعة تعبئة الفرصة السابقة" if i <= busy
            else ""
        )
        if blocked_by and not skipped_count:
            opp.status, opp.reason = "skipped", blocked_by
            continue
        per_day[day] += 1
        opp.n = per_day[day]
        if blocked_by:
            opp.status, opp.reason = "skipped", blocked_by
        else:
            pending.append(opp)
    return opps


# ---------------------------------------------------------------- the day
def daily_atr(bars: list[Bar], n: int = 20) -> dict[date, float | None]:
    """The day's true-range average over the previous ``n`` sessions, keyed
    by day, from the five-minute bars. None until five sessions exist."""
    days: dict[date, list[float]] = {}
    for b in bars:
        d = b.ts.astimezone(NY).date()
        if d not in days:
            days[d] = [b.high, b.low, b.close]
        else:
            days[d][0] = max(days[d][0], b.high)
            days[d][1] = min(days[d][1], b.low)
            days[d][2] = b.close
    out: dict[date, float | None] = {}
    trs: list[float] = []
    prev: float | None = None
    for d in sorted(days):
        hi, lo, close = days[d]
        tr = hi - lo if prev is None else max(hi - lo, abs(hi - prev), abs(lo - prev))
        out[d] = statistics.mean(trs[-n:]) if len(trs) >= 5 else None
        trs.append(tr)
        prev = close
    return out


def morning_read(bars: list[Bar], now: datetime) -> dict[str, Any]:
    """The 09:30 / 10:30 / 11:30 reads of the leader's day, in units of its
    daily range, with what a year of sessions said about each."""
    local = now.astimezone(NY)
    today = local.date()
    todays = [b for b in bars if b.ts.astimezone(NY).date() == today]
    atr_by_day = daily_atr(bars)
    a = atr_by_day.get(today)
    prev_close = next((b.close for b in reversed(bars) if b.ts.astimezone(NY).date() < today), None)

    def close_at(hh: int, mm: int) -> float | None:
        for b in todays:
            t = b.ts.astimezone(NY)
            if (t.hour, t.minute) == (hh, mm):
                return b.close
        return None

    steps: list[dict[str, Any]] = []
    gap = first = second = None
    if todays and a and prev_close:
        gap = (todays[0].open - prev_close) / a
        c1 = close_at(10, 25)
        c2 = close_at(11, 25)
        if c1 is not None and (local.hour, local.minute) >= (10, 30):
            first = (c1 - todays[0].open) / a
        if c1 is not None and c2 is not None and (local.hour, local.minute) >= (11, 30):
            second = (c2 - c1) / a
    steps.append({
        "time": "09:30", "done": gap is not None,
        "title": ("فتح بفجوة " + ("صاعدة" if gap > 0.1 else "هابطة" if gap < -0.1 else "بسيطة") + f" {abs(gap):.1f} من مدى اليوم") if gap is not None else "الافتتاح",
        "note": "الفجوة وحدها ما تقول شيئاً عن بقية اليوم في القياس." if gap is not None else "بانتظار الافتتاح.",
        "tone": "on" if gap is not None else "",
    })
    flat_first = first is not None and abs(first) < 0.15
    steps.append({
        "time": "10:30", "done": first is not None,
        "title": ("الساعة الأولى شبه ثابتة" if flat_first
                  else f"الساعة الأولى {'صاعدة' if first > 0 else 'هابطة'} {abs(first):.1f} من المدى") if first is not None else "الساعة الأولى",
        "note": (("افتتاح هادئ: في القياس أغلب هذه الأيام تبقى ضيقة، والصفقة تأتي من الكتلة لا من الاتجاه."
                  if flat_first
                  else "الأيام مثلها تكمل الاتجاه ٥ إلى ٦ من ١٠ فقط. لهذا لا ندخل على الاتجاه، ندخل على الكتلة.")
                 if first is not None else "لا قراءة قبل ١٠:٣٠."),
        "tone": ("hold" if flat_first else "on") if first is not None else "",
    })
    quiet = second is not None and abs(second) < 0.3
    steps.append({
        "time": "11:30", "done": second is not None,
        "title": ("ساعة ثانية هادئة" if quiet else f"الساعة الثانية تتحرك {abs(second):.1f} من المدى") if second is not None else "الساعة الثانية",
        "note": ("٧ من ١٠ من هذه الأيام تبقى ميتة للإغلاق: خذ الكتلة إن جاءت ولا تنتظر يوم اتجاه." if quiet
                 else "اليوم حي: الكتلة إن جاءت لها مجال.") if second is not None else "لا قراءة قبل ١١:٣٠.",
        "tone": ("hold" if quiet else "on") if second is not None else "",
    })
    return {"daily_atr": _r2(a), "gap": None if gap is None else round(gap, 2), "first_hour": None if first is None else round(first, 2),
            "second_hour": None if second is None else round(second, 2), "quiet": quiet, "steps": steps}


# ---------------------------------------------------------------- the service
class LeaderService:
    """Builds the leader board — the same for every subscriber, so it is
    built once and shared for a few seconds. Bars come through the desk's
    cache; contracts through the desk's chain cache."""

    def __init__(self, desk: DeskService):
        self.desk = desk
        self.memory = desk.memory
        self._now = getattr(desk, "_now", None) or (lambda: datetime.now(UTC))
        self._cache: tuple[float, dict[str, Any]] | None = None
        self._lock = asyncio.Lock()
        self._recorded: set[tuple[str, str]] = set()
        self._live: tuple[date, dict[str, Any]] | None = None

    async def board(self) -> dict[str, Any]:
        async with self._lock:
            if self._cache and time.monotonic() - self._cache[0] < BOARD_TTL_SEC:
                return self._cache[1]
            payload = await self._build()
            self._cache = (time.monotonic(), payload)
            return payload

    async def _build(self) -> dict[str, Any]:
        now = self._now()
        errors: list[str] = []
        async with self.desk._client() as client:
            async def one(sym: str) -> tuple[str, list[Bar]]:
                try:
                    return sym, mirsad9.resample(await self.desk._minute_bars(client, sym, LOOKBACK_DAYS), FRAME)
                except Exception as exc:  # noqa: BLE001 - one name must not blank the board
                    log.warning("leader: %s failed: %s", sym, exc)
                    errors.append(f"{sym}: تعذر جلب البيانات")
                    return sym, []

            fetched = dict(await asyncio.gather(*(one(s) for s in (*LEADERS, *BASKET))))
            # the maths once per name; everything below reads it
            cores = {s: mirsad9.core(b, FRAME, RAW) for s, b in fetched.items() if b}
            basket = {s: fetched[s] for s in BASKET if fetched.get(s)}
            block = block_series(basket, now, cores)
            leaders: list[dict[str, Any]] = []
            history: list[Opportunity] = []
            for sym in LEADERS:
                bars = fetched.get(sym) or []
                opps = replay(bars, block, now, core=cores.get(sym)) if bars else []
                history.extend(opps)
                leaders.append(await self._leader(client, sym, bars, opps, block, now, cores.get(sym)))
        self._record(history, now)
        today = now.astimezone(NY).date()
        session = _session_info(now)
        tiles = self._tiles(basket, block, now, cores)
        payload = {
            "now": now.isoformat(),
            "session": session,
            "verdict": self._verdict(leaders, tiles, session, now),
            "leaders": leaders,
            "block": tiles,
            "morning": morning_read(fetched.get("QQQ") or [], now),
            "journal": self._journal(history, today),
            "notes": _day_notes(now),
            "measured": MEASURED,
            "basket": list(BASKET),
            "rule": {"block_min": BLOCK_MIN, "of": len(BASKET), "cap": DAILY_CAP, "frame": FRAME,
                     "stop_atr": STOP_ATR, "target_atr": TARGET_ATR},
            "errors": errors,
        }
        return payload

    # ------------------------------------------------------------ one leader
    async def _leader(
        self, client: Any, sym: str, bars: list[Bar], opps: list[Opportunity],
        block: dict[datetime, int], now: datetime, core: list[mirsad9.CoreBar] | None = None,
    ) -> dict[str, Any]:
        today = now.astimezone(NY).date()
        todays = [p for p in opps if p.day == today]
        if not bars:
            return {"symbol": sym, "name": LEADER_NAMES.get(sym, sym), "unavailable": True, "today": [], "used": 0}
        if core is None:
            core = mirsad9.core(bars, FRAME, RAW)
        k = core[-1]
        price = bars[-1].close
        atr = 0.0 if k.atr != k.atr else k.atr
        taken = [p for p in todays if p.status != "rejected"]
        active = next((p for p in taken if p.status == "open"), None) or next(
            (p for p in taken if p.status in ("waiting", "chase")), None
        )
        state = active.status if active else ("done" if len(taken) >= DAILY_CAP else "idle")
        # the block the next signal would read: the last two confirmed bars
        upto = confirmed_count(bars, now)
        net_now = sum(block.get(bars[j].ts, 0) for j in range(max(0, upto - 2), upto))
        lean = 1 if net_now >= BLOCK_MIN else -1 if net_now <= -BLOCK_MIN else 0
        trend = 1 if k.state == 1 else -1 if k.state == -1 else 0
        contract = None
        want_side = active.side if active else (lean if lean == trend else 0)
        if want_side and _session_info(now)["phase"] in ("open", "pre"):
            try:
                contract = await self._contract(client, sym, want_side, price, active, atr, k.e9, now)
            except Exception as exc:  # noqa: BLE001
                log.warning("leader: chain for %s failed: %s", sym, exc)
                contract = {"missing": True, "error": True}
        return {
            "symbol": sym,
            "name": LEADER_NAMES.get(sym, sym),
            "unavailable": False,
            "price": round(price, 2),
            "atr": round(atr, 3),
            "zone": _r2(k.e9),
            "trend": trend,
            "trend_text": "صاعد" if trend > 0 else "هابط" if trend < 0 else "بلا ميل",
            "closes": [round(x, 3) for x in (b.close for b in bars[-40:])],
            "bar_time": _hhmm(bars[-1].ts),
            "state": state,
            "active": active.as_dict() if active else None,
            "today": [p.as_dict() for p in todays],
            "used": len(taken),
            "net_now": net_now,
            "lean": lean,
            "contract": contract,
            "plan": self._plan(sym, price, atr, k.e9, active, lean, trend, len(taken), now),
            "say": self._say(sym, price, active, lean, trend, net_now, len(taken), now, k),
        }

    async def _contract(
        self, client: Any, sym: str, side: int, spot: float, active: Opportunity | None,
        atr: float, e9: float, now: datetime,
    ) -> dict[str, Any]:
        """Today's at-the-money contract on ``side`` and where it should
        trade at the levels — the open trade's, else the ones a signal now
        would set. The rule lives and dies inside the session, so the
        contract is today's whenever today trades, even in the last hour."""
        today = now.astimezone(NY).date()
        expiry = today if is_trading_day(today) else expiry_for(sym, FRAME, "nearest", now)
        chain = await self.desk._chain(client, sym, expiry, side)
        contract = pick_contract(chain, side, spot)
        if contract is None:
            return {"expiry": expiry.isoformat(), "missing": True}
        if active and active.entry is not None:
            levels = {"entry": active.entry, "stop": active.stop, "target": active.target}
        elif active:
            entry = active.level
            levels = {"entry": entry, "stop": entry - side * STOP_ATR * active.atr, "target": entry + side * TARGET_ATR * active.atr}
        else:
            entry = min(e9, spot) if side > 0 else max(e9, spot)
            levels = {"entry": entry, "stop": entry - side * STOP_ATR * atr, "target": entry + side * TARGET_ATR * atr}
        mid = contract.mid
        out: dict[str, Any] = {
            "occ": contract.occ_symbol, "strike": contract.strike, "side": side,
            "side_text": "كول" if side > 0 else "بوت", "expiry": expiry.isoformat(),
            "expiry_text": _expiry_text(expiry, now), "price": mid, "bid": contract.bid, "ask": contract.ask,
            "delta": contract.delta, "spread_pct": contract.spread_pct, "missing": False, "targets": {},
        }
        entry_px = contract_price_at(contract, spot, float(levels["entry"]))
        for name, lvl in levels.items():
            if lvl is None:
                continue
            px = contract_price_at(contract, spot, float(lvl))
            out["targets"][name] = {"stock": round(float(lvl), 2), "contract": px, "pct": _pct(px, entry_px or mid)}
        return out

    @staticmethod
    def _plan(
        sym: str, price: float, atr: float, e9: float, active: Opportunity | None,
        lean: int, trend: int, used: int, now: datetime,
    ) -> list[dict[str, Any]]:
        """The scenarios a desk trader writes before the trade, with the
        numbers of the moment: the base case, the chase, what cancels it,
        and what is refused."""
        left = DAILY_CAP - used
        plans: list[dict[str, Any]] = []
        if active and active.status == "open":
            plans.append({"key": "hold", "tone": "up" if active.side > 0 else "dn", "title": f"الفرضية القائمة · درجة {active.grade}",
                          "text": f"الصفقة مفتوحة من {active.entry:.2f}. لا تسوي شيئاً حتى يلمس السعر الهدف {active.target:.2f} أو الوقف {active.stop:.2f}."})
            plans.append({"key": "eod", "tone": "wait", "title": "لو ما وصل أحدهما",
                          "text": "تُغلق على سعر السوق عند ١٥:٥٥ مهما كان. عقد اليوم لا يُبات."})
            return plans
        if active and active.status in ("waiting", "chase"):
            zone = active.level
            plans.append({"key": "touch", "tone": "up" if active.side > 0 else "dn", "title": "الفرضية الأساسية · الرجوع للمنطقة",
                          "text": f"لو لمس السعر {zone:.2f} خلال ٥ شموع: الدخول هناك، الهدف {zone + active.side * TARGET_ATR * active.atr:.2f}، الوقف {zone - active.side * STOP_ATR * active.atr:.2f}."})
            plans.append({"key": "chase", "tone": "wait", "title": "الفرضية البديلة · التأكيد",
                          "text": (f"لو ما رجع لكن أغلق {'فوق قمة' if active.side > 0 else 'تحت قاع'} شمعة الإشارة "
                                   f"({(active.sig_hi if active.side > 0 else active.sig_lo):.2f}) خلال ٨ شموع وهو قريب من المنطقة: الدخول على افتتاح الشمعة التالية.")
                          if active.status == "waiting" else "التأكيد تم: الدخول على افتتاح الشمعة القادمة."})
            plans.append({"key": "cancel", "tone": "off", "title": "فرضية الإلغاء",
                          "text": "لا لمس ولا تأكيد خلال ٨ شموع، أو بعيد أكثر من ١٫٥ ATR عن المنطقة: الفرصة تُلغى ولا نطارد."})
            return plans
        if left <= 0:
            plans.append({"key": "done", "tone": "off", "title": "انتهت فرص اليوم",
                          "text": "فرصتان في اليوم هي القاعدة. الفرصة الثالثة في القياس خسرت أكثر مما ربحت."})
            return plans
        if atr > 0 and trend:
            side = trend
            entry = min(e9, price) if side > 0 else max(e9, price)
            tone = "up" if side > 0 else "dn"
            mark = " ← الكتلة تميل هنا الآن" if lean == side else ""
            plans.append({"key": f"if_{tone}", "tone": tone, "title": f"الفرضية الأساسية · {'صعود' if side > 0 else 'هبوط'} مع الميل{mark}",
                          "text": (f"ميل {sym} {'صاعد' if side > 0 else 'هابط'}. لو جاءت {BLOCK_MIN} اندفاعات {'صاعدة' if side > 0 else 'هابطة'} من أكبر {len(BASKET)} شركات "
                                   f"خلال شمعتين وأغلقت شمعة {sym} معهم: "
                                   f"الدخول قرب {entry:.2f}، الهدف {entry + side * TARGET_ATR * atr:.2f} (+{TARGET_ATR * atr:.2f}$)، "
                                   f"الوقف {entry - side * STOP_ATR * atr:.2f} (−{STOP_ATR * atr:.2f}$).")})
            other = "هبوط" if side > 0 else "صعود"
            plans.append({"key": "against", "tone": "off", "title": f"فرضية مرفوضة · {other} ضد الميل",
                          "text": f"لو اندفعت الكتلة {'لتحت' if side > 0 else 'لفوق'} وميل {sym} ما زال {'صاعداً' if side > 0 else 'هابطاً'}: الإشارة تظهر ولا تُؤخذ. القياس: ضد الميل أضعف بكثير من معه."})
        elif atr > 0:
            plans.append({"key": "notrend", "tone": "off", "title": "بلا ميل واضح",
                          "text": f"متوسطا {sym} متعانقان: أي إشارة الآن تُرفض حتى يظهر الميل."})
        plans.append({"key": "none", "tone": "off", "title": "بلا كتلة لا صفقة",
                      "text": f"أقل من {BLOCK_MIN} اندفاعات بنفس الاتجاه = لا صفقة مهما كان شكل الشارت. باقي اليوم {left} من {DAILY_CAP}."})
        return plans

    @staticmethod
    def _say(
        sym: str, price: float, active: Opportunity | None, lean: int, trend: int, net_now: int,
        used: int, now: datetime, k: mirsad9.CoreBar,
    ) -> dict[str, str]:
        """One honest sentence: what to do now, and why."""
        phase = _session_info(now)["phase"]
        if phase == "closed":
            return {"tone": "off", "text": "السوق مغلق اليوم. الشاشة تعود مع الافتتاح."}
        if phase == "pre":
            return {"tone": "off", "text": "قبل الافتتاح. أول إشارة ممكنة عند ٠٩:٤٠ بعد إغلاق الشمعة الثالثة."}
        if active and active.status == "open":
            up = (price - active.entry) * active.side
            where = "فوق الدخول" if up > 0 else "تحت الدخول" if up < 0 else "عند الدخول"
            return {"tone": "up" if active.side > 0 else "dn",
                    "text": f"الدخول تم عند {active.entry:.2f}. السعر {where} بـ {abs(up):.2f}$. ما تسوي شي لين يلمس الهدف أو الوقف."}
        if active and active.status == "chase":
            return {"tone": "wait", "text": f"تأكيد على {sym}: الدخول على افتتاح الشمعة القادمة، الهدف +{TARGET_ATR * active.atr:.2f}$ والوقف −{STOP_ATR * active.atr:.2f}$."}
        if active and active.status == "waiting":
            gap = abs(price - active.level)
            return {"tone": "wait", "text": f"إشارة على {sym}، بانتظار الرجوع إلى {active.level:.2f} (على بعد {gap:.2f}$). لا تدخل من هنا: الدخول من فوق يعني وقف أبعد وعقد أغلى."}
        if phase == "post":
            return {"tone": "off", "text": "انتهت الجلسة. لا شيء يُحمل لليوم التالي."}
        if used >= DAILY_CAP:
            return {"tone": "off", "text": "انتهت فرصتا اليوم. لا صفقة ثالثة بالقاعدة."}
        if k.late or k.bar_of_day >= LAST_SIGNAL_BAR:
            return {"tone": "off", "text": "آخر ربع ساعة: لا إشارات جديدة، عقد اليوم ما بقي فيه وقت."}
        if lean and trend and lean != trend:
            return {"tone": "off", "text": f"الكتلة تميل {'لفوق' if lean > 0 else 'لتحت'} ({abs(net_now)} اندفاعات) لكن ميل {sym} {'هابط' if trend < 0 else 'صاعد'}: إشارة ضد الميل لا تُؤخذ."}
        if lean:
            return {"tone": "wait", "text": f"الكتلة تميل {'لفوق' if lean > 0 else 'لتحت'} ({abs(net_now)} اندفاعات) لكن شمعة {sym} ما أغلقت معهم بعد. الإشارة تُقبل عند إغلاق شمعة معهم."}
        return {"tone": "off", "text": f"لا صفقة على {sym} الآن. السبب بصدق: الكتلة هادئة ({abs(net_now)} اندفاع فقط من أكبر {len(BASKET)} شركات)، والقاعدة تحتاج {BLOCK_MIN}."}

    # ------------------------------------------------------------ the basket
    @staticmethod
    def _tiles(
        basket: dict[str, list[Bar]], block: dict[datetime, int], now: datetime,
        cores: dict[str, list[mirsad9.CoreBar]] | None = None,
    ) -> dict[str, Any]:
        """One tile per basket name over the last two confirmed bars: its
        side and how many impulses it printed (a name firing on both bars
        is two, as the rule counts it), so up minus down equals the net."""
        tiles: list[dict[str, Any]] = []
        last_ts: datetime | None = None
        for sym in BASKET:
            bars = basket.get(sym) or []
            if not bars:
                tiles.append({"symbol": sym, "side": 0, "count": 0, "unavailable": True})
                continue
            upto = confirmed_count(bars, now)
            sides = impulses(bars, upto, (cores or {}).get(sym))
            recent = bars[max(0, upto - 2): upto]
            hits = [sides.get(b.ts, 0) for b in recent]
            side = sum(hits)
            side = 1 if side > 0 else -1 if side < 0 else 0
            tiles.append({"symbol": sym, "side": side, "count": sum(1 for x in hits if x), "price": round(bars[-1].close, 2), "unavailable": False})
            if recent:
                last_ts = recent[-1].ts if last_ts is None else max(last_ts, recent[-1].ts)
        up = sum(t["count"] for t in tiles if t["side"] > 0)
        down = sum(t["count"] for t in tiles if t["side"] < 0)
        flat = sum(1 for t in tiles if t["side"] == 0)
        net_now = 0
        if last_ts is not None:
            net_now = block.get(last_ts, 0) + block.get(last_ts - timedelta(minutes=FRAME), 0)
        return {"tiles": tiles, "up": up, "down": down, "flat": flat, "net": net_now,
                "bar_time": _hhmm(last_ts) if last_ts else None,
                "rule_text": (f"القاعدة: {BLOCK_MIN} اندفاعات بنفس الاتجاه من أكبر {len(BASKET)} شركات خلال شمعتين = كتلة "
                              "(الاسم الذي يندفع في الشمعتين يُحسب مرتين). أقل من ذلك = لا صفقة مهما كان شكل الشارت.")}

    # ------------------------------------------------------------ the verdict
    @staticmethod
    def _verdict(leaders: list[dict[str, Any]], tiles: dict[str, Any], session: dict[str, Any], now: datetime) -> dict[str, Any]:
        used = sum(int(ld.get("used") or 0) for ld in leaders)
        chips = [f"الكتلة {tiles['up']}↑ {tiles['down']}↓ اندفاعاً", f"فرص اليوم {used} من {DAILY_CAP * len(LEADERS)}", f"فريم {FRAME} دقائق"]
        opened = [ld for ld in leaders if ld.get("active") and ld["active"]["status"] == "open"]
        waiting = [ld for ld in leaders if ld.get("active") and ld["active"]["status"] in ("waiting", "chase")]
        if not session["open"]:
            title = "السوق مغلق." if session["phase"] == "closed" else "قبل الافتتاح." if session["phase"] == "pre" else "انتهت الجلسة."
            text = ("الشاشة تعود مع الافتتاح، وأول إشارة ممكنة عند ٠٩:٤٠." if session["phase"] != "post"
                    else "لا شيء يُحمل لليوم التالي. النتائج في سجل اليوم تحت.")
            return {"tone": "off", "title": title, "text": text, "chips": chips}
        if opened:
            ld = opened[0]
            a = ld["active"]
            return {"tone": "up" if a["side"] > 0 else "dn",
                    "title": f"صفقة {a['side_text']} قائمة على {ld['symbol']}.",
                    "text": f"الدخول {a['entry']:.2f}، الهدف {a['target']:.2f}، الوقف {a['stop']:.2f}. بع الكل عند الهدف.", "chips": chips}
        if waiting:
            ld = waiting[0]
            a = ld["active"]
            return {"tone": "wait", "title": f"إشارة {a['side_text']} على {ld['symbol']}، بانتظار التعبئة.",
                    "text": f"الكتلة اندفعت ({abs(a['net'])} أسماء). الدخول عند الرجوع إلى {a['level']:.2f} أو بتأكيد، لا من هنا.", "chips": chips}
        if used >= DAILY_CAP * len(LEADERS):
            return {"tone": "off", "title": "انتهت فرص اليوم.", "text": "أربع فرص كحد أقصى على القائدين. الباقي مراقبة فقط.", "chips": chips}
        lean = tiles["net"]
        if abs(lean) >= BLOCK_MIN:
            side = 1 if lean > 0 else -1
            with_trend = [ld for ld in leaders if ld.get("trend") == side]
            if not with_trend:
                return {"tone": "off", "title": "الكتلة تتحرك " + ("لفوق" if lean > 0 else "لتحت") + " ضد ميل القائدين.",
                        "text": "إشارة ضد الميل لا تُؤخذ. القياس قال إنها أضعف بكثير من الإشارة مع الميل.", "chips": chips}
            return {"tone": "wait", "title": "الكتلة تتحرك " + ("لفوق" if lean > 0 else "لتحت") + ".",
                    "text": "بانتظار شمعة القائد تغلق معهم. لا تسبق الإشارة.", "chips": chips}
        return {"tone": "off", "title": "لا صفقة الآن.",
                "text": (f"السبب بصدق: الكتلة هادئة ({tiles['up']} اندفاعات صاعدة و{tiles['down']} هابطة في آخر شمعتين "
                         f"من أكبر {len(BASKET)} شركات)، والقاعدة تحتاج {BLOCK_MIN} بنفس الاتجاه."), "chips": chips}

    # ------------------------------------------------------------ the record
    def _journal(self, history: list[Opportunity], today: date) -> dict[str, Any]:
        by_day: dict[date, list[Opportunity]] = {}
        for p in history:
            by_day.setdefault(p.day, []).append(p)
        days = sorted(by_day)
        recent = []
        for d in days[-10:]:
            rows = sorted(by_day[d], key=lambda p: p.signal_ts)
            closed = [p for p in rows if p.status == "closed"]
            recent.append({
                "day": d.isoformat(),
                "trades": [p.as_dict() for p in rows],
                "r": round(sum(p.r for p in closed), 2),
                "wins": sum(1 for p in closed if p.r > 0),
                "losses": sum(1 for p in closed if p.r < 0),
            })
        return {
            "today": [p.as_dict() for p in sorted(by_day.get(today, []), key=lambda p: p.signal_ts)],
            "recent": recent,
            "live": self._live_record(today),
        }

    def _live_record(self, today: date) -> dict[str, Any]:
        """The running record since the board went live: every closed trade
        ever recorded before today, read once a day (and again after a new
        record lands), not on every refresh."""
        if self._live and self._live[0] == today:
            return self._live[1]
        try:
            rows = self.memory.leader_trades_between(date(2000, 1, 1), today - timedelta(days=1))
        except Exception:  # noqa: BLE001
            rows = []
        rs = [float(r.get("r") or 0.0) for r in rows]
        wins = sum(1 for r in rs if r > 0)
        gp = sum(r for r in rs if r > 0)
        gl = -sum(r for r in rs if r < 0)
        live = {"trades": len(rs), "win_pct": round(100 * wins / len(rs)) if rs else None,
                "profit_factor": round(gp / gl, 2) if gl else None, "days": len({r.get("day") for r in rows})}
        self._live = (today, live)
        return live

    def _record(self, history: list[Opportunity], now: datetime) -> None:
        """Persist closed trades whose exit bar has closed — a close decided
        on the forming bar can still change — once each per process."""
        for p in history:
            if p.status != "closed" or p.exit_ts is None:
                continue
            if now < p.exit_ts + timedelta(minutes=FRAME):
                continue
            key = (p.symbol, p.signal_ts.isoformat())
            if key in self._recorded:
                continue
            try:
                row = p.as_dict()
                row["day"] = p.day.isoformat()
                if self.memory.record_leader_trade(row):
                    self._live = None
                self._recorded.add(key)
            except Exception:  # noqa: BLE001 - the record is a courtesy to tomorrow, not today
                log.debug("leader record failed", exc_info=True)
