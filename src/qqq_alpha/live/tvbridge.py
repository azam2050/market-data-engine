"""TradingView bridge: the indicator's alerts become contract-backed cards.

The indicator is the eyes; this module is the hands. Every alert the
operator's TradingView account fires lands here through the secret webhook,
gets parsed (the MIRSAD JSON alerts and the older v1.8 Arabic strings), and:

- an entry makes the bridge open the LIVE option chain and shortlist
  tradeable contracts by the operator's rules: single stocks get the nearest
  weekly Friday and the nearest strike; QQQ and SPY get a same-day expiry;
  an SPX signal is routed to SPY, the cheaper contract on the same index; a
  late session signal (or a 🌙 tag) is pushed to the next expiry on purpose.
  The brain (``tvbrain``) then reads the signal, the clock and the shortlist
  and names the contract; the rule pick stands whenever the brain is silent
  or names something that was not on the list. The card carries the real
  contract price, never an estimate;
- while the trade is open the bridge marks the contract every minute and
  keeps its peak, so the exit report can say what the contract reached;
- zone / pending / trap alerts go to the operator only, so the channel
  carries decisions, not noise — but every TRADE is recorded in the
  private channel: the entry card, the target updates, the exit report,
  and also the cases that used to stay with the operator (no contract
  found, a contract that expired without an exit, an exit or target with
  no tracked entry), so the channel's record is complete;
- an exit posts the trade report (entry, peak, exit, duration on the
  contract) and books it into the day; the bell posts the day's report
  even on a day with nothing to report, so the record has no gaps.

State is per-symbol and in-memory: a container restart forgets open
follow-ups (the entry cards themselves are already posted), which is an
accepted cost for the first live weeks.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from qqq_alpha.domain import OptionContract, OptionType
from qqq_alpha.live.tvbrain import ContractChoice, parse_choice

log = logging.getLogger(__name__)

NY = ZoneInfo("America/New_York")

# contracts the channel can actually trade: priced within reach, quoted on
# both sides, and not a ghost line nobody makes a market in
MIN_PREMIUM = 0.15
MAX_PREMIUM = 20.0
MIN_LIQUIDITY = 25  # volume or open interest, whichever saves it

# underlyings that list an expiry every trading day
DAILY_EXPIRY = {"QQQ", "SPY", "SPX", "SPXW", "XSP", "NDX"}
# an index signal is traded through the cheaper listed product on the same index
CHEAPER_PROXY: dict[str, tuple[str, float]] = {"SPX": ("SPY", 0.1), "SPXW": ("SPY", 0.1)}
# a signal this late books the next expiry: a dying contract has no room left
LATE_SESSION_HOUR, LATE_SESSION_MINUTE = 15, 30

_TICKER = re.compile(r"\b([A-Z]{1,6})\b")
_NUM = re.compile(r"(\d+(?:\.\d+)?)")

# the operator-owned key that opens the webhook, stored in the database so
# it can be replaced from the bot without a redeploy
TV_SECRET_KEY = "tv_webhook_secret"


def tv_webhook_secret(settings: object, memory: object) -> str:
    """The one secret the webhook accepts right now.

    A secret issued from the bot wins. Until one is issued, the value derived
    from the bot token stands in, so the very first deployment already has a
    working link and nothing has to be configured by hand. Issuing a new
    secret therefore retires the derived one too — which is the point: the
    old link stops working the moment a new one is handed out.
    """
    stored = memory.app_setting(TV_SECRET_KEY)
    if stored:
        return stored
    token = getattr(settings, "telegram_bot_token", "") or ""
    return hashlib.sha256(f"{token}:tv-webhook".encode()).hexdigest()[:24]


def rotate_tv_webhook_secret(memory: object) -> str:
    """Issue a brand-new secret and make it the only one accepted."""
    fresh = secrets.token_urlsafe(18).replace("-", "").replace("_", "")[:24]
    memory.set_app_setting(TV_SECRET_KEY, fresh)
    return fresh


@dataclass
class TvSignal:
    kind: str  # prep | trap | pending | cancel | entry | t1 | t2 | t3 | exit
    symbol: str
    side: int = 0  # 1 call, -1 put, 0 unknown/irrelevant
    price: float | None = None
    stop: float | None = None
    reason: str = ""
    moon: bool = False
    win: bool | None = None
    r_text: str = ""
    raw: str = ""
    tf: str = ""
    targets: tuple[float, ...] = ()


def _first_ticker(text: str) -> str:
    m = _TICKER.search(text)
    return m.group(1) if m else ""


def _num_after(text: str, anchor: str) -> float | None:
    pos = text.find(anchor)
    if pos < 0:
        return None
    m = _NUM.search(text[pos + len(anchor):])
    return float(m.group(1)) if m else None


def _f(v: object) -> float | None:
    try:
        x = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return x if x == x else None  # NaN guard


def _parse_json_signal(data: dict) -> TvSignal | None:
    """The MIRSAD family's JSON alerts (src mirsad7 / mirsad8 / mirsad80).

    Entry: {"src","sym","tf","side":"CALL|PUT","why","ref","stop","t1","t2","t3"}
    or     {"event":"entry","side","price","stop","t1","t2","t3"}.
    Zone:  {"event":"zone"|"level", "side", "level"}  → operator only.
    Exit:  {"event":"exit","why","r"}.
    """
    sym = str(data.get("sym") or data.get("symbol") or "").upper().split(":")[-1]
    if not sym:
        return None
    side_raw = str(data.get("side") or "").lower()
    side = 1 if side_raw in {"call", "long", "buy"} else -1 if side_raw in {"put", "short", "sell"} else 0
    tf = str(data.get("tf") or "")
    event = str(data.get("event") or "").lower()
    raw = json.dumps(data, ensure_ascii=False)
    if event in {"zone", "level"}:
        return TvSignal("pending", sym, side, price=_f(data.get("level")), raw=raw, tf=tf)
    if event == "secure":
        return TvSignal("secure", sym, side, price=_f(data.get("price")), raw=raw, tf=tf)
    if event == "exit":
        r = _f(data.get("r"))
        why = str(data.get("why") or "")
        return TvSignal(
            "exit", sym, side, win=(r is not None and r > 0), r_text=f"{r:+.2f}R" if r is not None else "",
            reason=why, raw=raw, tf=tf,
        )
    if event in {"", "entry"} and side != 0:
        price = _f(data.get("price")) or _f(data.get("ref"))
        targets = tuple(t for t in (_f(data.get("t1")), _f(data.get("t2")), _f(data.get("t3"))) if t)
        return TvSignal(
            "entry", sym, side, price=price, stop=_f(data.get("stop")),
            reason=str(data.get("why") or ""), raw=raw, tf=tf, targets=targets,
        )
    return None


def parse_signal(raw: str) -> TvSignal | None:
    """Understand the indicator's alerts: MIRSAD JSON first, then the v1.8 strings."""
    text = raw.strip()
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except ValueError:
            data = None
        if isinstance(data, dict):
            return _parse_json_signal(data)
    side = 1 if "كول" in text else -1 if "بوت" in text else 0
    moon = "🌙" in text
    sym = _first_ticker(text)
    if not sym:
        return None
    if text.startswith("⏳"):
        return TvSignal("prep", sym, side, raw=text)
    if text.startswith("🪤"):
        return TvSignal("trap", sym, side, raw=text)
    if text.startswith("⨯"):
        return TvSignal("cancel", sym, side, raw=text)
    if text.startswith("🎯 إشارة"):
        return TvSignal(
            "pending", sym, side,
            price=_num_after(text, "منطقة الدخول"),
            stop=_num_after(text, "وقف"),
            moon=moon, raw=text,
        )
    if text.startswith("🎯 دخول"):
        reason = text.split("السبب:", 1)[1].strip() if "السبب:" in text else ""
        return TvSignal(
            "entry", sym, side,
            price=_num_after(text, "سعر"),
            stop=_num_after(text, "وقف"),
            reason=reason, moon=moon or "🌙" in reason, raw=text,
        )
    if text.startswith("🔺"):
        # order matters: the T2 alert mentions "هدف 1" in its stop-moved text
        kind = "t2" if "هدف 2" in text else "t1" if "هدف 1" in text else "t3"
        return TvSignal(kind, sym, side, raw=text)
    if text.startswith(("✅", "🔴")) and "خروج" in text:
        r_text = text.split("النتيجة", 1)[1].strip(" |") if "النتيجة" in text else ""
        return TvSignal(
            "exit", sym, side, win=text.startswith("✅"), r_text=r_text, raw=text
        )
    return None


def is_late_session(now_utc: datetime) -> bool:
    now = now_utc.astimezone(NY)
    return (now.hour, now.minute) >= (LATE_SESSION_HOUR, LATE_SESSION_MINUTE)


def next_expiry(symbol: str, now_utc: datetime, moon: bool) -> date:
    """QQQ and SPY list an expiry every trading day; single stocks only Fridays.

    A 🌙 signal deliberately skips today's dying contract and books the next
    expiry — the operator's own rule, learned from a trade that signalled at
    the close and paid the next morning.
    """
    from qqq_alpha.data.calendar import is_trading_day, next_trading_day

    now = now_utc.astimezone(NY)
    today = now.date()

    if symbol.upper() in DAILY_EXPIRY:
        usable_today = is_trading_day(today) and now.hour < 16 and not moon
        return today if usable_today else next_trading_day(today)

    def weekly(friday: date) -> date:
        # a Friday the exchange is shut (Good Friday) expires the day before
        return friday if is_trading_day(friday) else friday - timedelta(days=1)

    friday = today + timedelta(days=(4 - today.weekday()) % 7)
    expiry = weekly(friday)
    same_week_ok = expiry > today or (expiry == today and now.hour < 16 and not moon)
    if not same_week_ok:
        expiry = weekly(friday + timedelta(days=7))
    return expiry


def resolve_underlying(symbol: str, spot: float | None) -> tuple[str, float | None]:
    """The product actually traded for a signal: SPX goes through SPY, the
    cheaper contract on the same index, with the spot scaled to match."""
    sym = symbol.upper()
    proxy = CHEAPER_PROXY.get(sym)
    if proxy is None:
        return sym, spot
    target, ratio = proxy
    return target, (spot * ratio if spot else spot)


def pick_contract(
    chain: list[OptionContract], side: int, spot: float
) -> OptionContract | None:
    """Nearest tradeable strike on the signal's side of the spot.

    First choice is the closest at/out-of-the-money strike with a real
    two-sided quote, sane premium and some life in it; if the strict pass
    finds nothing the liquidity bar drops, because a slightly sleepy strike
    beats silently dropping the trade.
    """
    want = OptionType.CALL if side > 0 else OptionType.PUT
    pool = [c for c in chain if c.option_type == want and c.mid]

    def otm_first(c: OptionContract) -> tuple[int, float]:
        otm = (c.strike >= spot) if side > 0 else (c.strike <= spot)
        return (0 if otm else 1, abs(c.strike - spot))

    def viable(c: OptionContract, strict: bool) -> bool:
        mid = c.mid or 0.0
        if not (MIN_PREMIUM <= mid <= MAX_PREMIUM):
            return False
        if strict:
            if c.bid is None or c.ask is None or c.bid <= 0:
                return False
            if (c.ask - c.bid) > max(0.15 * mid, 0.06):
                return False
            if max(c.volume, c.open_interest) < MIN_LIQUIDITY:
                return False
        return True

    for strict in (True, False):
        for c in sorted(pool, key=otm_first):
            if viable(c, strict):
                return c
    return None


def later_expiry(symbol: str, expiry: date) -> date:
    """The expiry after this one: the next trading day for daily-listed
    products, the following Friday for everything else. This is the
    alternative the brain is allowed to prefer when the clock is short."""
    if symbol.upper() in DAILY_EXPIRY:
        d = expiry + timedelta(days=1)
        while d.weekday() >= 5:
            d += timedelta(days=1)
        return d
    return expiry + timedelta(days=7)


def candidate_rows(
    chain: list[OptionContract], side: int, spot: float, count: int = 5
) -> list[dict[str, Any]]:
    """The tradeable strikes around the spot, rendered for the brain.

    Same bar as ``pick_contract``'s relaxed pass — a real premium and a
    price on the screen — nearest to the money first, at most ``count``.
    """
    want = OptionType.CALL if side > 0 else OptionType.PUT
    pool = [
        c for c in chain
        if c.option_type == want and c.mid and MIN_PREMIUM <= (c.mid or 0.0) <= MAX_PREMIUM
    ]
    pool.sort(key=lambda c: abs(c.strike - spot))
    rows = []
    for c in pool[:count]:
        rows.append(
            {
                "occ": c.occ_symbol,
                "expiry": c.expiry.isoformat(),
                "strike": c.strike,
                "bid": c.bid,
                "ask": c.ask,
                "mid": c.mid,
                "spread_pct": c.spread_pct,
                "delta": c.delta,
                "iv": c.implied_volatility,
                "volume": c.volume,
                "open_interest": c.open_interest,
                "distance_pct": round((c.strike - spot) / spot * 100, 2) if spot else None,
            }
        )
    return rows


def _pct(now: float, entry: float) -> float:
    return 100.0 * (now - entry) / entry if entry > 0 else 0.0


def _signed(pct: float) -> str:
    return f"{'+' if pct >= 0 else ''}{pct:.0f}%"


def _dur(seconds: float) -> str:
    m = int(seconds // 60)
    return f"{m} د" if m < 60 else f"{m // 60} س {m % 60} د"


@dataclass
class _OpenTrade:
    signal_symbol: str
    underlying: str
    side: int
    entry_stock: float | None
    stop: float | None
    occ: str
    strike: float
    expiry: date
    contract_entry: float
    reason: str = ""
    tf: str = ""
    targets: tuple[float, ...] = ()
    opened: datetime = field(default_factory=lambda: datetime.now(UTC))
    peak: float = 0.0
    peak_at: datetime | None = None
    last_mark: float | None = None
    hits: int = 0

    def label(self) -> str:
        cp = "C" if self.side > 0 else "P"
        return f"{self.underlying} {self.strike:g}{cp}"


@dataclass
class ClosedTrade:
    signal_symbol: str
    label: str
    side: int
    entry: float
    exit: float
    peak: float
    opened: datetime
    closed: datetime
    r_text: str = ""
    how: str = ""

    @property
    def pct(self) -> float:
        return _pct(self.exit, self.entry)

    @property
    def peak_pct(self) -> float:
        return _pct(self.peak, self.entry)


@dataclass
class _DayNote:
    """A channel record that is not a contract trade: a signal with no
    contract, an exit with no tracked entry. It belongs in the day's report
    so the ledger shows every signal, not only the ones that got a card."""

    symbol: str
    text: str
    at: datetime = field(default_factory=lambda: datetime.now(UTC))


# TradingView re-sends an alert when the webhook answers slowly; the same
# text inside this window is the same signal, not a second trade
DUPLICATE_WINDOW_SEC = 90


class TvBridge:
    def __init__(
        self,
        admin_send: Callable[[str], Awaitable[None]],
        channel_send: Callable[[str], Awaitable[bool]],
        chain_fetch: Callable[[str, date, OptionType], Awaitable[list[OptionContract]]],
        analyst: Callable[[dict[str, Any]], Awaitable[dict[str, Any] | None]] | None = None,
    ):
        self._admin = admin_send
        self._channel = channel_send
        self._chain = chain_fetch
        self._analyst = analyst
        self._open: dict[str, _OpenTrade] = {}
        self._closed: list[ClosedTrade] = []
        self._notes: list[_DayNote] = []
        self._recent: dict[str, datetime] = {}

    def _is_duplicate(self, raw: str) -> bool:
        now = datetime.now(UTC)
        key = hashlib.sha1(raw.strip().encode()).hexdigest()  # noqa: S324 - dedupe key, not security
        self._recent = {k: t for k, t in self._recent.items() if (now - t).total_seconds() < DUPLICATE_WINDOW_SEC}
        if key in self._recent:
            return True
        self._recent[key] = now
        return False

    async def _record(self, text: str, admin_prefix: str = "📡 ") -> bool:
        """Post to the private channel, and echo to the operator either way."""
        posted = await self._channel(text)
        await self._admin((admin_prefix if posted else "⚠️ تعذر النشر في القناة:\n") + text)
        return posted

    async def handle(self, raw: str) -> None:
        if self._is_duplicate(raw):
            return
        sig = parse_signal(raw)
        if sig is None:
            await self._admin("📡 إشارة من TradingView (غير مصنّفة):\n" + raw[:500])
            return
        if sig.kind in {"prep", "trap", "pending", "cancel"}:
            await self._admin("📡 " + self._quiet_text(sig))
            return
        if sig.kind == "entry":
            await self._on_entry(sig)
        elif sig.kind in {"t1", "t2", "t3", "secure"}:
            await self._on_target(sig)
        elif sig.kind == "exit":
            await self._on_exit(sig)

    @staticmethod
    def _quiet_text(sig: TvSignal) -> str:
        if sig.kind == "pending" and sig.raw.startswith("{"):
            side = "كول" if sig.side > 0 else "بوت"
            lvl = f" عند {sig.price:g}" if sig.price else ""
            return f"منطقة {side} جاهزة | {sig.symbol}{lvl} — ننتظر رجوع السعر إليها"
        return sig.raw[:500]

    # ------------------------------------------------------------- entry
    async def _on_entry(self, sig: TvSignal) -> None:
        side_txt = "كول 🟢" if sig.side > 0 else "بوت 🔴"
        now = datetime.now(UTC)
        # MIRSAD 9 in immediate mode sends the signal alert and, one bar
        # later, the entry alert with the fill price: the same trade twice.
        # The second one refines the entry price instead of opening a twin.
        live = self._open.get(sig.symbol)
        if live is not None and live.side == sig.side and (now - live.opened) <= timedelta(minutes=20):
            if sig.price:
                live.entry_stock = sig.price
            if sig.stop:
                live.stop = sig.stop
            await self._admin(f"📡 {sig.symbol}: تأكيد الدخول عند {sig.price:g} — الصفقة نفسها، لا بطاقة جديدة" if sig.price else f"📡 {sig.symbol}: تأكيد الدخول — الصفقة نفسها")
            return
        moon = sig.moon or is_late_session(now)
        underlying, spot = resolve_underlying(sig.symbol, sig.price)
        expiry = next_expiry(underlying, now, moon)
        want = OptionType.CALL if sig.side > 0 else OptionType.PUT

        # the rule pick and the shortlist the brain is allowed to choose from:
        # the rule expiry first, the one after it as the permitted alternative
        contract: OptionContract | None = None
        by_occ: dict[str, OptionContract] = {}
        candidates: list[dict[str, Any]] = []
        err = ""
        if spot:
            for exp in (expiry, later_expiry(underlying, expiry)):
                try:
                    chain = await self._chain(underlying, exp, want)
                except Exception as exc:  # noqa: BLE001 - a data hiccup must not kill the card
                    err = err or str(exc)
                    continue
                if exp == expiry:
                    contract = pick_contract(chain, sig.side, spot)
                for row in candidate_rows(chain, sig.side, spot):
                    row["expiry"] = exp.isoformat()
                    candidates.append(row)
                by_occ.update({c.occ_symbol: c for c in chain})

        if contract is None:
            note = (
                f"⚠️ إشارة {side_txt} {sig.symbol}" + (f" · فريم {sig.tf}" if sig.tf else "")
                + f" — لم يُعثر على عقد صالح (انتهاء {expiry:%d-%m})، لا صفقة على هذه الإشارة"
                + (f"\n📍 سعر الإشارة {sig.price:g}" if sig.price else "")
                + (f" · 🛑 وقف {sig.stop:g}" if sig.stop else "")
            )
            self._notes.append(_DayNote(sig.symbol, f"{side_txt} بلا عقد"))
            await self._record(note + (f"\nالسبب التقني: {err[:200]}" if err else ""))
            return

        # the brain reads the signal and the shortlist, then names the contract
        choice = await self._analyse(sig, now, moon, underlying, spot, expiry, contract, candidates, set(by_occ))
        if choice is not None:
            contract = by_occ[choice.occ]
            expiry = contract.expiry

        entry_px = float(contract.ask or contract.mid or 0.0)
        trade = _OpenTrade(
            signal_symbol=sig.symbol, underlying=underlying, side=sig.side,
            entry_stock=sig.price, stop=sig.stop,
            occ=contract.occ_symbol, strike=contract.strike, expiry=expiry,
            contract_entry=entry_px, reason=sig.reason, tf=sig.tf, targets=sig.targets,
            opened=now, peak=entry_px, peak_at=now, last_mark=entry_px,
        )
        self._open[sig.symbol] = trade
        exp_label = "ينتهي اليوم" if expiry == now.astimezone(NY).date() else f"ينتهي {expiry:%d-%m}"
        lines = [
            f"⭐️ صفقة {side_txt} — {sig.symbol}" + (f" · فريم {sig.tf}" if sig.tf else ""),
            "━━━━━━━━━━━━━━",
            f"📄 العقد: {trade.label()} · {exp_label}",
            f"💵 سعر العقد الآن: {entry_px:.2f}$",
        ]
        if underlying != sig.symbol:
            lines.append(f"↪️ الإشارة على {sig.symbol} والعقد على {underlying} لأنه الأرخص على نفس المؤشر")
        if sig.price:
            lines.append(f"📍 دخول السهم: {sig.price:g}" + (f" · 🛑 وقف: {sig.stop:g}" if sig.stop else ""))
        if sig.targets:
            lines.append("🎯 الأهداف: " + " · ".join(f"{t:g}" for t in sig.targets))
        if sig.reason:
            lines.append(f"🧠 السبب: {sig.reason}")
        if choice is not None:
            lines.append(f"🤖 اختيار العقد: {choice.why}" if choice.why else "🤖 العقد اختاره التحليل")
            if choice.caution:
                lines.append(f"⚠️ تنبيه: {choice.caution}")
        else:
            lines.append("📐 العقد بأقرب ستررايك حسب القواعد")
        if moon:
            lines.append("🌙 إشارة آخر الجلسة — العقد محجوز للانتهاء التالي عمداً")
        lines.append("🔔 الأهداف والوقف يُداران آلياً — التحديثات هنا أولاً بأول")
        card = "\n".join(lines)
        await self._record(card, admin_prefix="✅ نُشرت في القناة:\n")

    async def _analyse(
        self,
        sig: TvSignal,
        now: datetime,
        moon: bool,
        underlying: str,
        spot: float | None,
        expiry: date,
        rule_pick: OptionContract,
        candidates: list[dict[str, Any]],
        allowed: set[str],
    ) -> ContractChoice | None:
        """Show the brain what the desk sees; accept only a candidate it was shown."""
        if self._analyst is None or not candidates:
            return None
        ny = now.astimezone(NY)
        close = ny.replace(hour=16, minute=0, second=0, microsecond=0)
        ctx = {
            "signal": {
                "symbol": sig.symbol, "side": sig.side, "price": sig.price, "stop": sig.stop,
                "targets": list(sig.targets), "reason": sig.reason, "tf": sig.tf,
            },
            "now_ny": ny.strftime("%Y-%m-%d %H:%M"),
            "minutes_to_close": max(0, int((close - ny).total_seconds() // 60)),
            "late_session": is_late_session(now),
            "moon": moon,
            "underlying": underlying,
            "spot": spot,
            "rule_pick": rule_pick.occ_symbol,
            "rule_expiry": expiry.isoformat(),
            "candidates": candidates,
        }
        try:
            payload = await self._analyst(ctx)
        except Exception as exc:  # noqa: BLE001 - the rules stand in for a silent brain
            log.warning("contract analyst failed for %s: %s", sig.symbol, exc)
            return None
        choice = parse_choice(payload, allowed)
        if payload and choice is None:
            log.warning("contract analyst named something off the list for %s: %r", sig.symbol, payload)
        return choice

    # ------------------------------------------------------------ targets
    async def _on_target(self, sig: TvSignal) -> None:
        label = {
            "t1": "🔺 تحقق الهدف الأول — الصفقة مؤمّنة (الوقف عند الدخول)",
            "t2": "🔺 تحقق الهدف الثاني — الوقف صعد إلى الهدف الأول",
            "t3": "🏆 الهدف الممتد تحقق",
            "secure": "🔔 أمّن الصفقة — بع نصف الكمية الآن والوقف عند سعر الدخول"
            + (f" (السهم عند {sig.price:g})" if sig.price else ""),
        }[sig.kind]
        trade = self._open.get(sig.symbol)
        if trade is not None:
            trade.hits = max(trade.hits, {"t1": 1, "t2": 2, "t3": 3}.get(sig.kind, 0))
        suffix = await self._contract_suffix(sig.symbol)
        if trade is None:
            suffix = " — بدون عقد متتبَّع (الإشارة قبل تشغيل المتابعة)"
        await self._record(f"{sig.symbol}: {label}{suffix}")

    # -------------------------------------------------------------- exit
    async def _on_exit(self, sig: TvSignal) -> None:
        trade = self._open.pop(sig.symbol, None)
        head = "✅" if sig.win else "🔴"
        r_part = f" — النتيجة {sig.r_text}" if sig.r_text else ""
        why = f" ({sig.reason})" if sig.reason else ""
        if trade is None:
            self._notes.append(_DayNote(sig.symbol, f"خروج{why}{r_part} — بدون عقد متتبَّع"))
            await self._record(
                f"{head} {sig.symbol}: خروج{why}{r_part}\n"
                "📄 بدون عقد متتبَّع — الإشارة سبقت تشغيل المتابعة، النتيجة أعلاه على السهم"
            )
            return
        exit_px = await self._mark(trade, side="exit")
        closed = self._book(trade, exit_px, how=sig.reason or "خروج", r_text=sig.r_text)
        await self._record(f"{head} {sig.symbol}: خروج{why}{r_part}\n" + self._trade_report(closed))

    def _book(self, trade: _OpenTrade, exit_px: float | None, how: str, r_text: str = "") -> ClosedTrade:
        px = exit_px if exit_px is not None else (trade.last_mark or 0.0)
        closed = ClosedTrade(
            signal_symbol=trade.signal_symbol, label=trade.label(), side=trade.side,
            entry=trade.contract_entry, exit=px, peak=max(trade.peak, px),
            opened=trade.opened, closed=datetime.now(UTC), r_text=r_text, how=how,
        )
        self._closed.append(closed)
        return closed

    @staticmethod
    def _trade_report(c: ClosedTrade) -> str:
        peak_note = f" (عند {c.peak_pct:+.0f}% قبل الخروج)" if c.peak > c.exit and c.entry > 0 else ""
        return (
            f"📄 {c.label}: دخول {c.entry:.2f}$ ← خروج {c.exit:.2f}$ ({_signed(c.pct)})\n"
            f"📈 أعلى ما بلغه العقد: {c.peak:.2f}$ ({_signed(c.peak_pct)}){peak_note}\n"
            f"⏱ مدة الصفقة: {_dur((c.closed - c.opened).total_seconds())}"
        )

    # ------------------------------------------------------------ marking
    async def _mark(self, trade: _OpenTrade, side: str = "mid") -> float | None:
        """Quote the contract now. Entry side pays the ask, exit side gets the bid."""
        try:
            want = OptionType.CALL if trade.side > 0 else OptionType.PUT
            chain = await self._chain(trade.underlying, trade.expiry, want)
            now_c = next((c for c in chain if c.occ_symbol == trade.occ), None)
            if now_c is None:
                return None
            px = now_c.bid if side == "exit" and now_c.bid else now_c.mid
            if not px:
                return None
            trade.last_mark = px
            if px > trade.peak:
                trade.peak, trade.peak_at = px, datetime.now(UTC)
            return px
        except Exception:  # noqa: BLE001 - a quote hiccup must not block the update
            return None

    async def tick(self, now_utc: datetime | None = None) -> None:
        """Called every minute: mark every open trade, keep its peak, and retire
        contracts that expired without an exit alert (feed loss, missed alert)."""
        now = now_utc or datetime.now(UTC)
        ny = now.astimezone(NY)
        for sym, trade in list(self._open.items()):
            expired = ny.date() > trade.expiry or (ny.date() == trade.expiry and ny.hour >= 16)
            if expired:
                self._open.pop(sym, None)
                closed = self._book(trade, trade.last_mark, how="انتهى العقد بلا إشارة خروج")
                await self._record(f"⌛️ {sym}: انتهى العقد بلا إشارة خروج\n" + self._trade_report(closed))
                continue
            if ny.weekday() < 5 and 9 <= ny.hour < 16:
                await self._mark(trade)

    async def _contract_suffix(self, symbol: str, closing: bool = False) -> str:
        trade = self._open.get(symbol)
        if trade is None or trade.contract_entry <= 0:
            return ""
        mid = await self._mark(trade, side="exit" if closing else "mid")
        if not mid:
            return ""
        pct = _pct(mid, trade.contract_entry)
        word = "أقفل على" if closing else "العقد الآن"
        return f"\n💵 {word} {mid:.2f}$ ({_signed(pct)} من {trade.contract_entry:.2f}$)"

    # ------------------------------------------------------- daily report
    def open_trades(self) -> list[_OpenTrade]:
        return list(self._open.values())

    def daily_report(self, day: date | None = None) -> str:
        """The day's record: every closed contract, every signal that got no
        contract, and what is still open. Never empty — a quiet day is a
        line that says so, so the channel's ledger has no missing days."""
        day = day or datetime.now(NY).date()
        todays = [c for c in self._closed if c.closed.astimezone(NY).date() == day]
        notes = [n for n in self._notes if n.at.astimezone(NY).date() == day]
        lines = [f"📊 تقرير اليوم {day:%d-%m} — إشارات TradingView", "━━━━━━━━━━━━━━"]
        if todays:
            wins = [c for c in todays if c.pct > 0]
            best = max(todays, key=lambda c: c.pct)
            worst = min(todays, key=lambda c: c.pct)
            total = sum(c.pct for c in todays)
            lines += [
                f"الصفقات: {len(todays)} · رابحة {len(wins)} · خاسرة {len(todays) - len(wins)}",
                f"مجموع نتائج العقود: {_signed(total)} · متوسط الصفقة {_signed(total / len(todays))}",
                f"الأفضل: {best.signal_symbol} {best.label} {_signed(best.pct)} (أعلى {_signed(best.peak_pct)})",
                f"الأسوأ: {worst.signal_symbol} {worst.label} {_signed(worst.pct)}",
                "",
            ]
            for c in todays:
                side = "كول" if c.side > 0 else "بوت"
                lines.append(
                    f"• {c.signal_symbol} {side} {c.label}: {c.entry:.2f} ← {c.exit:.2f} ({_signed(c.pct)})"
                    f" · أعلى {_signed(c.peak_pct)} · {_dur((c.closed - c.opened).total_seconds())}"
                    + (f" · {c.r_text}" if c.r_text else "")
                )
        else:
            lines.append("لا صفقات مقفلة اليوم")
        if notes:
            lines += ["", "إشارات بلا عقد متتبَّع:"]
            lines += [f"• {n.symbol}: {n.text}" for n in notes]
        if self._open:
            lines += ["", "ما زال مفتوحاً:"]
            for t in self._open.values():
                mark = f" · الآن {t.last_mark:.2f}$ ({_signed(_pct(t.last_mark, t.contract_entry))})" if t.last_mark else ""
                lines.append(f"• {t.signal_symbol} {t.label()} · دخول {t.contract_entry:.2f}${mark}")
        return "\n".join(lines)

    async def post_daily_report(self, day: date | None = None) -> bool:
        day = day or datetime.now(NY).date()
        text = self.daily_report(day)
        posted = await self._record(text, admin_prefix="📊 ")
        self._closed = [c for c in self._closed if c.closed.astimezone(NY).date() != day]
        self._notes = [n for n in self._notes if n.at.astimezone(NY).date() != day]
        return posted
