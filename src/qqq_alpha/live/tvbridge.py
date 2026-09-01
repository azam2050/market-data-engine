"""TradingView bridge: the indicator's alerts become contract-backed cards.

The indicator is the eyes (ten symbols, quality-fast recipe on 5-minute
charts); this module is the hands. Every alert the operator's TradingView
account fires lands here through the secret webhook, gets parsed, and:

- an entry (🎯 دخول) makes the bridge open the LIVE option chain, pick a
  tradeable contract — QQQ gets a same-day expiry, single stocks get the
  nearest weekly Friday because that is all the market lists for them, and
  a 🌙 late-session signal is pushed to the next expiry on purpose — and
  post a card to the private subscribers channel with the real contract
  price, never an estimate;
- target and exit alerts (🔺 / ✅ / 🔴) become follow-up messages on the
  same trade, quoting the contract's live price against its entry;
- preparation, trap, pending and cancel alerts go to the operator only,
  so the channel carries decisions, not noise.

State is per-symbol and in-memory: a container restart forgets open
follow-ups (the entry cards themselves are already posted), which is an
accepted cost for the first live week.
"""

from __future__ import annotations

import hashlib
import logging
import re
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from qqq_alpha.domain import OptionContract, OptionType

log = logging.getLogger(__name__)

NY = ZoneInfo("America/New_York")

# contracts the channel can actually trade: priced within reach, quoted on
# both sides, and not a ghost line nobody makes a market in
MIN_PREMIUM = 0.15
MAX_PREMIUM = 20.0
MIN_LIQUIDITY = 25  # volume or open interest, whichever saves it

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


def _first_ticker(text: str) -> str:
    m = _TICKER.search(text)
    return m.group(1) if m else ""


def _num_after(text: str, anchor: str) -> float | None:
    pos = text.find(anchor)
    if pos < 0:
        return None
    m = _NUM.search(text[pos + len(anchor):])
    return float(m.group(1)) if m else None


def parse_signal(raw: str) -> TvSignal | None:
    """Understand the indicator's own Arabic alert strings (v1.8 formats)."""
    text = raw.strip()
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


def next_expiry(symbol: str, now_utc: datetime, moon: bool) -> date:
    """QQQ lists an expiry every trading day; single stocks only Fridays.

    A 🌙 signal deliberately skips today's dying contract and books the next
    expiry — the operator's own rule, learned from a trade that signalled at
    the close and paid the next morning.
    """
    now = now_utc.astimezone(NY)
    today = now.date()

    def next_trading_day(d: date) -> date:
        d += timedelta(days=1)
        while d.weekday() >= 5:
            d += timedelta(days=1)
        return d

    if symbol.upper() == "QQQ":
        usable_today = today.weekday() < 5 and now.hour < 16 and not moon
        return today if usable_today else next_trading_day(today)

    friday = today + timedelta(days=(4 - today.weekday()) % 7)
    same_friday_ok = friday > today or (now.hour < 16 and not moon)
    if not same_friday_ok:
        friday += timedelta(days=7)
    return friday


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


@dataclass
class _OpenTrade:
    side: int
    entry_stock: float | None
    stop: float | None
    occ: str
    strike: float
    expiry: date
    contract_entry: float
    opened: datetime = field(default_factory=lambda: datetime.now(UTC))


class TvBridge:
    def __init__(
        self,
        admin_send: Callable[[str], Awaitable[None]],
        channel_send: Callable[[str], Awaitable[bool]],
        chain_fetch: Callable[[str, date, OptionType], Awaitable[list[OptionContract]]],
    ):
        self._admin = admin_send
        self._channel = channel_send
        self._chain = chain_fetch
        self._open: dict[str, _OpenTrade] = {}

    async def handle(self, raw: str) -> None:
        sig = parse_signal(raw)
        if sig is None:
            await self._admin("📡 إشارة من TradingView (غير مصنّفة):\n" + raw[:500])
            return
        if sig.kind in {"prep", "trap", "pending", "cancel"}:
            await self._admin("📡 " + sig.raw[:500])
            return
        if sig.kind == "entry":
            await self._on_entry(sig)
        elif sig.kind in {"t1", "t2", "t3"}:
            await self._on_target(sig)
        elif sig.kind == "exit":
            await self._on_exit(sig)

    # ------------------------------------------------------------- entry
    async def _on_entry(self, sig: TvSignal) -> None:
        side_txt = "كول 🟢" if sig.side > 0 else "بوت 🔴"
        expiry = next_expiry(sig.symbol, datetime.now(UTC), sig.moon)
        contract: OptionContract | None = None
        err = ""
        if sig.price:
            try:
                want = OptionType.CALL if sig.side > 0 else OptionType.PUT
                chain = await self._chain(sig.symbol, expiry, want)
                contract = pick_contract(chain, sig.side, sig.price)
            except Exception as exc:  # noqa: BLE001 - a data hiccup must not kill the card
                err = str(exc)
        if contract is None:
            await self._admin(
                f"⚠️ إشارة {sig.symbol} وصلت لكن تعذر اختيار عقد"
                f" (انتهاء {expiry:%d-%m}) — لم تُنشر"
                + (f"\nالسبب: {err[:200]}" if err else "")
                + "\n" + sig.raw[:300]
            )
            return
        self._open[sig.symbol] = _OpenTrade(
            side=sig.side, entry_stock=sig.price, stop=sig.stop,
            occ=contract.occ_symbol, strike=contract.strike,
            expiry=expiry, contract_entry=float(contract.mid or 0.0),
        )
        cp = "C" if sig.side > 0 else "P"
        exp_label = "ينتهي اليوم" if expiry == datetime.now(NY).date() else f"ينتهي {expiry:%d-%m}"
        lines = [
            f"⭐️ صفقة {side_txt} — {sig.symbol}",
            "━━━━━━━━━━━━━━",
            f"📄 العقد: {sig.symbol} {contract.strike:g}{cp} · {exp_label}",
            f"💵 سعر العقد الآن: {contract.mid:.2f}$",
        ]
        if sig.price:
            lines.append(f"📍 دخول السهم: {sig.price:g}" + (f" · 🛑 وقف: {sig.stop:g}" if sig.stop else ""))
        if sig.reason:
            lines.append(f"🧠 السبب: {sig.reason}")
        if sig.moon:
            lines.append("🌙 إشارة آخر الجلسة — العقد محجوز للانتهاء التالي عمداً")
        lines.append("🔔 الأهداف والوقف يُداران آلياً — التحديثات هنا أولاً بأول")
        card = "\n".join(lines)
        posted = await self._channel(card)
        await self._admin(("✅ نُشرت في القناة:\n" if posted else "⚠️ تعذر النشر في القناة — البطاقة:\n") + card)

    # ------------------------------------------------------------ targets
    async def _on_target(self, sig: TvSignal) -> None:
        label = {
            "t1": "🔺 تحقق الهدف الأول — الصفقة مؤمّنة (الوقف عند الدخول)",
            "t2": "🔺 تحقق الهدف الثاني — الوقف صعد إلى الهدف الأول",
            "t3": "🏆 الهدف الممتد تحقق",
        }[sig.kind]
        suffix = await self._contract_suffix(sig.symbol)
        msg = f"{sig.symbol}: {label}{suffix}"
        trade = self._open.get(sig.symbol)
        if trade is not None:
            await self._channel(msg)
        await self._admin("📡 " + msg)

    # -------------------------------------------------------------- exit
    async def _on_exit(self, sig: TvSignal) -> None:
        head = "✅" if sig.win else "🔴"
        r_part = f" — النتيجة {sig.r_text}" if sig.r_text else ""
        suffix = await self._contract_suffix(sig.symbol, closing=True)
        msg = f"{head} {sig.symbol}: خروج على الوقف المتحرك{r_part}{suffix}"
        trade = self._open.pop(sig.symbol, None)
        if trade is not None:
            await self._channel(msg)
        await self._admin("📡 " + msg)

    async def _contract_suffix(self, symbol: str, closing: bool = False) -> str:
        trade = self._open.get(symbol)
        if trade is None or trade.contract_entry <= 0:
            return ""
        try:
            want = OptionType.CALL if trade.side > 0 else OptionType.PUT
            chain = await self._chain(symbol, trade.expiry, want)
            now_c = next((c for c in chain if c.occ_symbol == trade.occ), None)
            mid = now_c.mid if now_c else None
            if not mid:
                return ""
            pct = 100.0 * (mid - trade.contract_entry) / trade.contract_entry
            word = "أقفل على" if closing else "العقد الآن"
            return f"\n💵 {word} {mid:.2f}$ ({'+' if pct >= 0 else ''}{pct:.0f}% من {trade.contract_entry:.2f}$)"
        except Exception:  # noqa: BLE001 - a quote hiccup must not block the update
            return ""
