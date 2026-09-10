"""The customer's desk: MIRSAD 9 for their own symbols, on their own frame,
priced as contracts, in one screen that refreshes while the market is open.

The desk *shows and explains*; the customer executes at their broker. It
never places, closes or sizes an order, and it holds no broker key.

Every number on the screen comes from three sources, in this order:
the bars (Massive), the indicator (``mirsad9.evaluate``, the same rules
as the published Pine), and the option chain (Massive). The notes are
rules over those numbers — plain sentences a trader would say — and
nothing on the page is an opinion of a language model.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

from qqq_alpha.config import Settings
from qqq_alpha.data.calendar import is_trading_day, next_trading_day, todays_events
from qqq_alpha.domain import Bar, OptionContract, OptionType
from qqq_alpha.live import mirsad9
from qqq_alpha.live.mirsad9 import NY, Params, SymbolState
from qqq_alpha.live.tvbridge import DAILY_EXPIRY, pick_contract, resolve_underlying
from qqq_alpha.memory import Memory

log = logging.getLogger(__name__)

DEFAULT_SYMBOLS = ["QQQ", "SPY", "NVDA", "TSLA", "AAPL", "META"]
LEADERS = ["QQQ", "SPY"]
FRAMES = (3, 5, 10, 15, 30, 60)
MAX_SYMBOLS = 10

# how far out the contract is, in days, by preference and — for "auto" —
# by the frame the signal lives on: a 3-minute trade wants today's contract,
# an hourly one wants the week
EXPIRY_CHOICES: dict[str, str] = {
    "auto": "حسب الفريم",
    "nearest": "الأقرب (اليوم للصناديق)",
    "2d": "يومان",
    "week": "أسبوع",
    "2w": "أسبوعان",
    "month": "شهر",
}
_EXPIRY_DAYS = {"nearest": 0, "2d": 2, "week": 7, "2w": 14, "month": 28}

BARS_TTL_SEC = 50
CHAIN_TTL_SEC = 50
REGULAR_OPEN = (9, 30)
REGULAR_CLOSE = (16, 0)


def frame_days_ahead(frame: int) -> int:
    """The "auto" ladder: expiry distance by the signal's frame."""
    if frame <= 5:
        return 0
    if frame <= 15:
        return 2
    if frame <= 60:
        return 7
    if frame <= 240:
        return 14
    return 28


def expiry_for(symbol: str, frame: int, pref: str, now: datetime) -> date:
    """The listed expiry that fits the preference: the first trading day
    (daily-listed products) or Friday (everything else) at least ``days``
    ahead; today's contract only while the session still has life in it."""
    days = frame_days_ahead(frame) if pref not in _EXPIRY_DAYS else _EXPIRY_DAYS[pref]
    local = now.astimezone(NY)
    today = local.date()
    late = (local.hour, local.minute) >= (15, 30)
    target = today + timedelta(days=days)
    daily = symbol.upper() in DAILY_EXPIRY
    if daily:
        d = target
        while not is_trading_day(d):
            d = next_trading_day(d)
        if d == today and late:
            d = next_trading_day(today)
        return d
    friday = target + timedelta(days=(4 - target.weekday()) % 7)
    if friday == today and late:
        friday += timedelta(days=7)
    while not is_trading_day(friday):
        friday -= timedelta(days=1)
    return friday


def contract_price_at(contract: OptionContract, spot: float, stock_target: float) -> float | None:
    """Where the contract should trade when the stock reaches ``stock_target``:
    the mid moved by delta. An estimate, stated as one on the screen."""
    mid = contract.mid
    if mid is None or mid <= 0:
        return None
    delta = contract.delta
    if delta is None:
        delta = 0.5 if contract.option_type is OptionType.CALL else -0.5
    return max(0.01, round(mid + delta * (stock_target - spot), 2))


def _pct(now: float | None, base: float | None) -> float | None:
    if now is None or base is None or base <= 0:
        return None
    return round(100.0 * (now - base) / base, 1)


def clean_symbols(raw: str) -> list[str]:
    out: list[str] = []
    for token in raw.replace("،", ",").replace("\n", ",").replace(" ", ",").split(","):
        s = token.strip().upper().lstrip("$")
        if s and s.replace(".", "").isalnum() and len(s) <= 6 and s not in out:
            out.append(s)
    return out[:MAX_SYMBOLS]


class DeskService:
    """Builds the desk payload for one subscriber. Bars and chains are
    cached briefly and shared across everyone watching the same symbol."""

    def __init__(
        self,
        settings: Settings,
        memory: Memory,
        client_factory: Any | None = None,
        now_fn: Any | None = None,
    ):
        self.settings = settings
        self.memory = memory
        self._client_factory = client_factory
        self._now = now_fn or (lambda: datetime.now(UTC))
        self._bars: dict[str, tuple[float, list[Bar]]] = {}
        self._chains: dict[tuple[str, str, int], tuple[float, list[OptionContract]]] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------ settings
    def settings_for(self, chat_id: str) -> dict[str, Any]:
        row = self.memory.desk_settings(chat_id)
        if row is None:
            return {"symbols": list(DEFAULT_SYMBOLS), "frame": 3, "expiry": "auto"}
        return {"symbols": row["symbols"] or list(DEFAULT_SYMBOLS), "frame": row["frame"], "expiry": row["expiry"]}

    def save_settings(self, chat_id: str, symbols_raw: str, frame: int, expiry: str) -> dict[str, Any]:
        symbols = clean_symbols(symbols_raw) or list(DEFAULT_SYMBOLS)
        frame = frame if frame in FRAMES else 3
        expiry = expiry if expiry in EXPIRY_CHOICES else "auto"
        self.memory.set_desk_settings(chat_id, symbols, frame, expiry)
        return {"symbols": symbols, "frame": frame, "expiry": expiry}

    def has_access(self, chat_id: str, now: datetime | None = None) -> bool:
        """Any subscriber whose window is still open — trial or paid."""
        row = self.memory.subscriber(chat_id)
        if row is None:
            return False
        try:
            expires = datetime.fromisoformat(str(row["expires_at"]))
        except ValueError:
            return False
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=UTC)
        return expires > (now or self._now())

    def link_for(self, chat_id: str, page: str = "desk") -> str:
        """A fresh sign-in link; ``page`` is where it lands ("desk" or "leader")."""
        base = self.settings.public_base_url.rstrip("/")
        token = self.memory.issue_desk_token(chat_id, self._now())
        if not base:
            return ""
        return f"{base}/desk/login?k={token}" + ("&next=leader" if page == "leader" else "")

    # ------------------------------------------------------------ data
    def _client(self):
        if self._client_factory is not None:
            return self._client_factory()
        from qqq_alpha.data.massive import MassiveClient

        return MassiveClient(self.settings)

    async def _minute_bars(self, client: Any, symbol: str, lookback_days: int) -> list[Bar]:
        key = f"{symbol}:{lookback_days}"
        cached = self._bars.get(key)
        if cached and time.monotonic() - cached[0] < BARS_TTL_SEC:
            return cached[1]
        end = self._now().astimezone(NY).date()
        start = end - timedelta(days=lookback_days)
        bars = await client.range_minute_bars(symbol, 1, start, end)
        self._bars[key] = (time.monotonic(), bars)
        return bars

    async def _chain(self, client: Any, underlying: str, expiry: date, side: int) -> list[OptionContract]:
        key = (underlying, expiry.isoformat(), side)
        cached = self._chains.get(key)
        if cached and time.monotonic() - cached[0] < CHAIN_TTL_SEC:
            return cached[1]
        want = OptionType.CALL if side > 0 else OptionType.PUT
        chain = await client.option_chain(underlying, expiry, want)
        self._chains[key] = (time.monotonic(), chain)
        return chain

    async def states(
        self, client: Any, symbol: str, frame: int
    ) -> tuple[SymbolState | None, SymbolState | None]:
        """The symbol on the customer's frame and on the frame above it."""
        higher = mirsad9.next_frame(frame)
        lookback = 12 if higher <= 15 else 35 if higher <= 60 else 120
        minute = await self._minute_bars(client, symbol, lookback)
        now = self._now()
        main = mirsad9.evaluate(mirsad9.resample(minute, frame), frame, Params(), now=now)
        up = mirsad9.evaluate(mirsad9.resample(minute, higher), higher, Params(), now=now)
        return main, up

    # ------------------------------------------------------------ rows
    async def contract_for(
        self, client: Any, state: SymbolState, frame: int, pref: str
    ) -> dict[str, Any] | None:
        side = state.side
        if not side:
            return None
        now = self._now()
        underlying, spot = resolve_underlying(state.symbol, state.price)
        spot = spot or state.price
        expiry = expiry_for(underlying, frame, pref, now)
        chain = await self._chain(client, underlying, expiry, side)
        contract = pick_contract(chain, side, spot)
        if contract is None:
            return {"expiry": expiry.isoformat(), "missing": True}
        # the stock levels the desk converts, scaled when the traded product
        # is a proxy of the charted one (SPX through SPY)
        scale = (spot / state.price) if state.price else 1.0
        levels = {
            "entry": state.entry, "stop": state.stop, "half": state.half_level,
            "t1": state.t1, "t2": state.t2, "t3": state.t3, "level": state.level,
        }
        mid = contract.mid
        out: dict[str, Any] = {
            "occ": contract.occ_symbol,
            "underlying": underlying,
            "strike": contract.strike,
            "side": side,
            "side_text": "كول" if side > 0 else "بوت",
            "expiry": expiry.isoformat(),
            "expiry_text": _expiry_text(expiry, now),
            "price": mid,
            "bid": contract.bid,
            "ask": contract.ask,
            "delta": contract.delta,
            "spread_pct": contract.spread_pct,
            "missing": False,
            "targets": {},
        }
        for name, lvl in levels.items():
            if lvl is None:
                continue
            px = contract_price_at(contract, spot, lvl * scale)
            out["targets"][name] = {"stock": round(lvl, 2), "contract": px, "pct": _pct(px, mid)}
        return out

    @staticmethod
    def note_for(state: SymbolState, contract: dict[str, Any] | None, higher: SymbolState | None) -> str:
        """One sentence a desk trader would say about this row, from the rules."""
        agree = higher is not None and state.direction != 0 and state.direction == higher.direction
        if state.pos:
            if state.hit >= 3:
                return "هدف ٣ تحقق. الباقي يتبع الحركة ولا ينزل تحت هدف ٢."
            if state.hit >= 2:
                return "هدف ٢ تحقق. الوقف يتبع قاع الحركة فوق هدف ١، خذ ما يعطيك السوق."
            if state.half:
                return "✅ بيع النصف تم عند ٣٥٪ من الطريق. الوقف عند الدخول، والباقي إلى هدف ٢ و٣."
            half = contract["targets"].get("half") if contract and not contract.get("missing") else None
            if half and half.get("pct") is not None:
                return f"بع النصف عند {half['contract']:.2f} (+{half['pct']:.0f}٪) وأمّن الصفقة. الوقف {state.stop:.2f} على السهم."
            return f"بع النصف عند {state.half_level:.2f} على السهم وأمّن الصفقة. الوقف {state.stop:.2f}."
        if state.pending:
            if abs(state.pending) == 1:
                return "دخول على افتتاح الشمعة القادمة."
            if abs(state.pending) == 3:
                return f"دخول ثانٍ إذا رجع السعر إلى المنطقة {state.level:.2f}."
            txt = f"المنطقة عند {state.level:.2f}: الدخول عند لمسها، أو تأكيد بإغلاق شمعة فوق قمة الإشارة."
            return txt + (" الفريم الأعلى موافق ✅." if agree else " الفريم الأعلى غير موافق: قلّل الحجم.")
        if state.blocked:
            return {
                "عرضي": "إشارة داخل عرضي: لا دخول حتى يخرج السعر من الصندوق.",
                "راحة": "إشارة أثناء الراحة بعد إشارة سابقة: تجاهلها.",
            }.get(state.blocked, f"إشارة محجوبة: {state.blocked}.")
        if state.late_window:
            return "آخر الجلسة: لا إشارات جديدة على هذا الفريم."
        if state.trend == 0:
            return "عرضي: انتظر خروجاً من الصندوق قبل أي دخول."
        word = "صاعد" if state.trend > 0 else "هابط"
        if agree:
            return f"ميل {word} على الفريمين. انتظر شمعة اندفاع مع الاتجاه."
        return f"ميل {word} على فريمك فقط. بلا توافق مع الأعلى تُقبل الإشارة بحذر."

    # ------------------------------------------------------------ board
    async def board(self, chat_id: str) -> dict[str, Any]:
        prefs = self.settings_for(chat_id)
        symbols = prefs["symbols"]
        frame = prefs["frame"]
        now = self._now()
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        leaders: dict[str, SymbolState | None] = {}

        async with self._client() as client:
            wanted = list(dict.fromkeys(LEADERS + symbols))

            async def one(sym: str):
                try:
                    return sym, await self.states(client, sym, frame)
                except Exception as exc:  # noqa: BLE001 - one bad symbol must not blank the desk
                    log.warning("desk: %s failed: %s", sym, exc)
                    errors.append(f"{sym}: تعذر جلب البيانات")
                    return sym, (None, None)

            results = dict(await asyncio.gather(*(one(s) for s in wanted)))
            for sym in LEADERS:
                leaders[sym] = (results.get(sym) or (None, None))[0]
            for sym in symbols:
                main, higher = results.get(sym) or (None, None)
                if main is None:
                    rows.append({"symbol": sym, "frame": frame, "unavailable": True})
                    continue
                contract = None
                try:
                    contract = await self.contract_for(client, main, frame, prefs["expiry"])
                except Exception as exc:  # noqa: BLE001
                    log.warning("desk: chain for %s failed: %s", sym, exc)
                    contract = {"missing": True, "error": True}
                rows.append(self._row(main, higher, contract, frame))

        return {
            "now": now.isoformat(),
            "session": _session_info(now),
            "market": self._market(leaders, rows, now),
            "rows": rows,
            "settings": {**prefs, "frame_name": mirsad9.frame_name(frame)},
            "errors": errors,
        }

    def _row(
        self, state: SymbolState, higher: SymbolState | None, contract: dict[str, Any] | None, frame: int
    ) -> dict[str, Any]:
        d = state.as_dict()
        d["frame_name"] = mirsad9.frame_name(frame)
        d["contract"] = contract
        d["note"] = self.note_for(state, contract, higher)
        if higher is not None:
            d["higher"] = {
                "frame": higher.frame,
                "frame_name": mirsad9.frame_name(higher.frame),
                "state_text": higher.state_text,
                "direction": higher.direction,
                "agree": state.direction != 0 and state.direction == higher.direction,
            }
        else:
            d["higher"] = None
        return d

    def _market(
        self, leaders: dict[str, SymbolState | None], rows: list[dict[str, Any]], now: datetime
    ) -> dict[str, Any]:
        lead = [
            {
                "symbol": sym,
                "direction": st.direction if st else 0,
                "state_text": st.state_text if st else "غير متاح",
                "price": st.price if st else None,
                "closes": (st.closes or []) if st else [],
            }
            for sym, st in leaders.items()
        ]
        dirs = [r.get("direction", 0) for r in rows if not r.get("unavailable")]
        up = sum(1 for d in dirs if d > 0)
        down = sum(1 for d in dirs if d < 0)
        flat = len(dirs) - up - down
        lead_dirs = [x["direction"] for x in lead if x["direction"]]
        block = "مختلط"
        if dirs:
            if up >= 0.7 * len(dirs) and all(d >= 0 for d in lead_dirs):
                block = "كتلة صاعدة"
            elif down >= 0.7 * len(dirs) and all(d <= 0 for d in lead_dirs):
                block = "كتلة هابطة"
            elif flat >= 0.6 * len(dirs):
                block = "عرضي"
        notes = _day_notes(now)
        if block == "مختلط":
            notes.append("السوق مختلط: اقبل الإشارات التي يوافقها القائد فقط.")
        elif block.startswith("كتلة"):
            notes.append(f"{block}: الإشارات مع الاتجاه أولى، وعكسها بحذر.")
        else:
            notes.append("السوق عرضي: قلّل المحاولات وانتظر الكسر.")
        return {"leaders": lead, "block": block, "up": up, "down": down, "flat": flat, "notes": notes}


def _expiry_text(expiry: date, now: datetime) -> str:
    today = now.astimezone(NY).date()
    if expiry == today:
        return "ينتهي اليوم"
    days = (expiry - today).days
    if days == 1:
        return "ينتهي غداً"
    return f"ينتهي {expiry.day:02d}/{expiry.month:02d}"


def _session_info(now: datetime) -> dict[str, Any]:
    local = now.astimezone(NY)
    weekday = local.weekday() < 5 and is_trading_day(local.date())
    t = (local.hour, local.minute)
    if not weekday:
        return {"open": False, "text": "السوق مغلق اليوم", "phase": "closed"}
    if t < REGULAR_OPEN:
        return {"open": False, "text": "قبل الافتتاح", "phase": "pre"}
    if t >= REGULAR_CLOSE:
        return {"open": False, "text": "بعد الإغلاق", "phase": "post"}
    minutes_left = REGULAR_CLOSE[0] * 60 + REGULAR_CLOSE[1] - (t[0] * 60 + t[1])
    return {"open": True, "text": f"السوق مفتوح · باقي {minutes_left // 60}س {minutes_left % 60}د", "phase": "open"}


def _day_notes(now: datetime) -> list[str]:
    local = now.astimezone(NY)
    today = local.date()
    notes: list[str] = []
    if is_trading_day(today):
        prev = today - timedelta(days=1)
        while not is_trading_day(prev):
            prev -= timedelta(days=1)
        if (today - prev).days > (3 if today.weekday() == 0 else 1):
            notes.append("اليوم بعد إجازة: السيولة عادة أضعف، انتظر توافق الفريمين قبل الدخول.")
    try:
        for ev in todays_events(now):
            when = ev.get("time_et", "?")
            label = ev.get("label", "")
            notes.append(f"حدث اليوم {when} بتوقيت نيويورك: {label}. لا دخول جديد قبله بربع ساعة.")
    except Exception:  # noqa: BLE001 - the calendar is a courtesy, not a dependency
        log.debug("calendar unavailable", exc_info=True)
    return notes
