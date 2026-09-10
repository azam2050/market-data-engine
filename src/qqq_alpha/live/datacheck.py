"""فحص البيانات — does the provider's data actually arrive, whole?

Three questions, asked of the live provider and answered with numbers:

* **the bars** — did today's minutes arrive, are there gaps or duplicates,
  is the last bar recent or is the feed standing still?
* **the chain** — for each side, calls *and* puts, did the strikes around
  the money arrive, or did the page stop short of them? Which contract
  would the desk pick right now, and does it have a two-sided quote, a
  delta, and a spread anyone would trade?
* **the tape** — is the account entitled to the option prints the flow
  feed reads?

Nothing here trades or writes; it reads and reports. Every failure is
reported as a failure, never smoothed over: a check that says "fine" when
the money is missing from the chain is worse than no check at all.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from qqq_alpha.config import Settings
from qqq_alpha.data.calendar import is_trading_day, next_trading_day
from qqq_alpha.domain import Bar, OptionContract, OptionType
from qqq_alpha.live.mirsad9 import NY, regular_only
from qqq_alpha.live.tvbridge import DAILY_EXPIRY, pick_contract

log = logging.getLogger(__name__)

SYMBOLS: tuple[str, ...] = ("QQQ", "SPY")
LOOKBACK_DAYS = 5
# a live feed whose newest minute is older than this is standing still
STALE_MINUTES = 6
# the money is "covered" when the chain reaches this far past the spot,
# measured in strike steps, on both sides of it
ATM_STEPS = 3
GOOD, WARN, BAD = "ok", "warn", "bad"


@dataclass
class Check:
    name: str
    status: str
    detail: str

    @property
    def mark(self) -> str:
        return {GOOD: "✅", WARN: "⚠️", BAD: "❌"}[self.status]


@dataclass
class Report:
    now: datetime
    checks: list[Check] = field(default_factory=list)
    data: dict[str, Any] = field(default_factory=dict)

    def add(self, name: str, status: str, detail: str) -> Check:
        check = Check(name, status, detail)
        self.checks.append(check)
        return check

    @property
    def status(self) -> str:
        if any(c.status == BAD for c in self.checks):
            return BAD
        if any(c.status == WARN for c in self.checks):
            return WARN
        return GOOD

    def as_dict(self) -> dict[str, Any]:
        return {
            "now": self.now.isoformat(),
            "status": self.status,
            "checks": [{"name": c.name, "status": c.status, "detail": c.detail} for c in self.checks],
            "data": self.data,
        }

    def as_text(self) -> str:
        head = {GOOD: "✅ البيانات كاملة وسليمة", WARN: "⚠️ البيانات تصل مع ملاحظات", BAD: "❌ خلل في البيانات"}[self.status]
        local = self.now.astimezone(NY)
        lines = [f"{head}", f"الفحص {local:%Y-%m-%d %H:%M} بتوقيت نيويورك", ""]
        lines += [f"{c.mark} {c.name}: {c.detail}" for c in self.checks]
        return "\n".join(lines)


# ---------------------------------------------------------------- bars
def _strike_step(strikes: list[float]) -> float:
    """The usual distance between neighbouring strikes."""
    ordered = sorted(set(strikes))
    gaps = [round(b - a, 4) for a, b in zip(ordered, ordered[1:], strict=False) if b > a]
    return statistics.median(gaps) if gaps else 1.0


def inspect_bars(bars: list[Bar], now: datetime) -> dict[str, Any]:
    """What arrived for the most recent session in the pull."""
    session = regular_only(bars)
    if not session:
        return {"bars": 0, "day": None}
    day = session[-1].ts.astimezone(NY).date()
    todays = [b for b in session if b.ts.astimezone(NY).date() == day]
    minutes = {b.ts.astimezone(NY).replace(second=0, microsecond=0) for b in todays}
    first, last = todays[0].ts.astimezone(NY), todays[-1].ts.astimezone(NY)
    expected = int((last - first).total_seconds() // 60) + 1
    gaps: list[tuple[str, int]] = []
    ordered = sorted(minutes)
    for a, b in zip(ordered, ordered[1:], strict=False):
        missing = int((b - a).total_seconds() // 60) - 1
        if missing > 0:
            gaps.append((a.strftime("%H:%M"), missing))
    frozen = run = 1
    for a, b in zip(todays, todays[1:], strict=False):
        run = run + 1 if b.close == a.close else 1
        frozen = max(frozen, run)
    return {
        "bars": len(todays),
        "day": day.isoformat(),
        "first": first.strftime("%H:%M"),
        "last": last.strftime("%H:%M"),
        "expected": expected,
        "missing": expected - len(minutes),
        "gaps": gaps[:5],
        "duplicates": len(todays) - len(minutes),
        "frozen": frozen,
        "age_min": round((now - todays[-1].ts).total_seconds() / 60, 1),
        "last_close": round(todays[-1].close, 2),
        "zero_volume": sum(1 for b in todays if b.volume <= 0),
    }


# ---------------------------------------------------------------- chain
def inspect_side(chain: list[OptionContract], side: int, spot: float) -> dict[str, Any]:
    """One side of the chain: did it reach the money, and is it quotable?"""
    want = OptionType.CALL if side > 0 else OptionType.PUT
    rows = [c for c in chain if c.option_type is want]
    out: dict[str, Any] = {
        "side": "كول" if side > 0 else "بوت",
        "count": len(rows),
        "wrong_side": len(chain) - len(rows),
    }
    if not rows:
        out["covers_money"] = False
        return out
    strikes = [c.strike for c in rows]
    step = _strike_step(strikes)
    lo, hi = min(strikes), max(strikes)
    reach = ATM_STEPS * step
    out.update({
        "low": lo, "high": hi, "step": step,
        "covers_money": lo <= spot - reach and hi >= spot + reach,
        "nearest_gap": round(min(abs(s - spot) for s in strikes), 2),
        "two_sided": sum(1 for c in rows if c.bid and c.ask and c.bid > 0),
        "with_delta": sum(1 for c in rows if c.delta is not None),
        "with_iv": sum(1 for c in rows if c.implied_volatility),
        "traded": sum(1 for c in rows if max(c.volume, c.open_interest) > 0),
    })
    picked = pick_contract(rows, side, spot)
    if picked is None:
        out["pick"] = None
        return out
    out["pick"] = {
        "occ": picked.occ_symbol,
        "strike": picked.strike,
        "mid": picked.mid,
        "bid": picked.bid,
        "ask": picked.ask,
        "delta": picked.delta,
        "spread_pct": picked.spread_pct,
        "volume": picked.volume,
        "open_interest": picked.open_interest,
        "moneyness": round(picked.strike - spot, 2),
    }
    return out


def expiry_to_check(symbol: str, now: datetime) -> date:
    """The expiry the desk would price right now: today for the daily-listed
    products while today trades, otherwise the next session."""
    today = now.astimezone(NY).date()
    if symbol.upper() in DAILY_EXPIRY:
        return today if is_trading_day(today) else next_trading_day(today)
    d = today + timedelta(days=(4 - today.weekday()) % 7)
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d


# ---------------------------------------------------------------- the run
async def run_data_check(
    settings: Settings,
    symbols: tuple[str, ...] = SYMBOLS,
    now: datetime | None = None,
    client_factory: Any | None = None,
) -> Report:
    now = now or datetime.now(UTC)
    report = Report(now=now)

    if not settings.massive_api_key and client_factory is None:
        report.add("مفتاح المزود", BAD, "MASSIVE_API_KEY غير مضبوط — لا بيانات إطلاقاً")
        return report

    def _client():
        if client_factory is not None:
            return client_factory()
        from qqq_alpha.data.massive import MassiveClient

        return MassiveClient(settings)

    end = now.astimezone(NY).date()
    start = end - timedelta(days=LOOKBACK_DAYS)

    async with _client() as client:
        for symbol in symbols:
            # ---------------- bars
            try:
                bars = await client.range_minute_bars(symbol, 1, start, end)
            except Exception as exc:  # noqa: BLE001 - the report must name the failure
                report.add(f"شموع {symbol}", BAD, f"فشل الجلب: {exc}"[:180])
                report.data.setdefault(symbol, {})["bars"] = {"error": str(exc)[:180]}
                continue

            info = inspect_bars(bars, now)
            report.data.setdefault(symbol, {})["bars"] = info
            if not info["bars"]:
                report.add(f"شموع {symbol}", BAD, "لم تصل أي شمعة في آخر خمسة أيام")
                continue
            open_now = _market_open(now)
            stale = open_now and info["age_min"] > STALE_MINUTES
            status = BAD if stale else (WARN if (info["missing"] or info["duplicates"] or info["frozen"] >= 15) else GOOD)
            detail = (
                f"{info['bars']} دقيقة ليوم {info['day']} من {info['first']} إلى {info['last']}، "
                f"آخر سعر {info['last_close']} (عمر آخر شمعة {info['age_min']:.0f} د)"
            )
            if info["missing"]:
                detail += f" · ناقص {info['missing']} دقيقة"
                if info["gaps"]:
                    detail += " (" + "، ".join(f"{t}+{n}" for t, n in info["gaps"]) + ")"
            if info["duplicates"]:
                detail += f" · مكرر {info['duplicates']}"
            if info["frozen"] >= 15:
                detail += f" · السعر ثابت {info['frozen']} شمعة"
            if stale:
                detail += " · الفيد متأخر والسوق مفتوح"
            report.add(f"شموع {symbol}", status, detail)

            # ---------------- chain, both sides
            spot = info["last_close"]
            expiry = expiry_to_check(symbol, now)
            for side in (1, -1):
                label = "كول" if side > 0 else "بوت"
                try:
                    chain = await client.option_chain(
                        symbol, expiry, OptionType.CALL if side > 0 else OptionType.PUT, spot
                    )
                except Exception as exc:  # noqa: BLE001
                    report.add(f"عقود {symbol} {label}", BAD, f"فشل الجلب: {exc}"[:180])
                    report.data[symbol].setdefault("chain", {})[label] = {"error": str(exc)[:180]}
                    continue
                side_info = inspect_side(chain, side, spot)
                side_info["expiry"] = expiry.isoformat()
                report.data[symbol].setdefault("chain", {})[label] = side_info
                report.add(*_chain_verdict(symbol, label, side_info, spot, expiry))

    # ---------------- the tape entitlement, once
    probe = _first_pick(report)
    if probe:
        try:
            async with _client() as client:
                prints = await client.option_trades_since(probe, now - timedelta(days=1))
            report.add("شريط الصفقات", GOOD, f"مسموح ({len(prints)} صفقة على {probe} خلال يوم)")
        except Exception as exc:  # noqa: BLE001
            text = str(exc)
            hint = "الباقة لا تشمل صفقات الأوبشنز" if ("NOT_AUTHORIZED" in text or "403" in text) else text[:140]
            report.add("شريط الصفقات", WARN, f"{hint} — الشاشة تعمل بدونه")
    return report


def _chain_verdict(symbol: str, label: str, info: dict[str, Any], spot: float, expiry: date) -> tuple[str, str, str]:
    name = f"عقود {symbol} {label}"
    when = f"انتهاء {expiry.isoformat()}"
    if not info["count"]:
        return name, BAD, f"{when}: لم يصل أي عقد"
    if info.get("wrong_side"):
        return name, BAD, f"{when}: وصلت عقود من الجهة الأخرى ({info['wrong_side']})"
    base = (
        f"{when}: {info['count']} عقداً من {info['low']:g} إلى {info['high']:g} "
        f"(خطوة {info['step']:g}) والسعر {spot:g}"
    )
    if not info["covers_money"]:
        return name, BAD, base + " — السلسلة لا تغطي السعر: العقد المطلوب خارجها"
    pick = info.get("pick")
    if not pick:
        return name, BAD, base + " — لا عقد صالح للتداول (لا أسعار أو خارج حدود السعر)"
    quote = (
        f" · المختار {pick['strike']:g} بسعر {pick['mid']:.2f}"
        + (f" ({pick['bid']:.2f}/{pick['ask']:.2f})" if pick["bid"] and pick["ask"] else "")
        + (f" فرق {pick['spread_pct']:.0f}٪" if pick["spread_pct"] is not None else "")
        + (f" دلتا {abs(pick['delta']):.2f}" if pick["delta"] is not None else " · بلا دلتا")
    )
    missing = []
    if info["two_sided"] < info["count"] * 0.5:
        missing.append("أغلب العقود بلا سعرين")
    if info["with_delta"] < info["count"] * 0.5:
        missing.append("أغلبها بلا دلتا")
    if pick["spread_pct"] is not None and pick["spread_pct"] > 25:
        missing.append("فرق السعرين واسع")
    status = WARN if missing else GOOD
    return name, status, base + quote + (" · " + "، ".join(missing) if missing else "")


def _first_pick(report: Report) -> str | None:
    for payload in report.data.values():
        for side in (payload.get("chain") or {}).values():
            pick = side.get("pick") if isinstance(side, dict) else None
            if pick and pick.get("occ"):
                return str(pick["occ"])
    return None


def _market_open(now: datetime) -> bool:
    local = now.astimezone(NY)
    return is_trading_day(local.date()) and (9, 30) <= (local.hour, local.minute) < (16, 0)


async def check_text(settings: Settings, **kw: Any) -> str:
    report = await run_data_check(settings, **kw)
    return report.as_text()
