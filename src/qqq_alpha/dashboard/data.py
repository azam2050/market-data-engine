"""Data access for the dashboard.

Every function here reads the journal (JSONL, full detail — including the
model's actual thesis/risks/invalidation text) or the long-term memory
(SQLite, structured aggregates and pending lessons). Nothing is computed or
cached separately from what the engine itself already persists.
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from typing import Any

from qqq_alpha.config import MARKET_TZ, Settings
from qqq_alpha.dashboard.behavior import classify_entry, classify_exit
from qqq_alpha.live.review import ReviewStats, load_period, read_jsonl, review
from qqq_alpha.memory import Memory


def _latest_trades(settings: Settings) -> list[dict[str, Any]]:
    """Every trade, final state only — the journal is append-only per update."""
    rows = read_jsonl(sorted(settings.journal_dir.glob("trades-*.jsonl")))
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        trade_id = row.get("trade_id")
        if trade_id:
            latest[trade_id] = row
    return sorted(
        latest.values(), key=lambda r: r.get("opened_at") or "", reverse=True
    )


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def recent_trades(settings: Settings, limit: int = 100) -> list[dict[str, Any]]:
    trades = _latest_trades(settings)[:limit]
    for trade in trades:
        # hold_minutes is not a Trade field — it only exists once a trade has
        # both ends, so it is derived here rather than stored redundantly
        opened, closed = _parse_ts(trade.get("opened_at")), _parse_ts(trade.get("closed_at"))
        trade["hold_minutes"] = (
            round((closed - opened).total_seconds() / 60, 1) if opened and closed else None
        )
        trade["behavior"] = classify_exit(trade)
        trade["entry_note"] = classify_entry(trade)
    return trades


def open_trades(settings: Settings) -> list[dict[str, Any]]:
    return [t for t in _latest_trades(settings) if not t.get("closed_at")]


def _readable_list(value: object) -> list[str]:
    """Repair a list field the journal stored letter-by-letter.

    When the model answered a list field with one prose string, the old parser
    wrapped it in ``list()`` and split it into characters, so the page showed
    ``ا · ل · س · ...``. The parser no longer does that, but the history on disk
    is already written and this page reads history. A list whose every entry is
    a single character can only be that accident — real risk notes are phrases —
    so it is glued back into the sentence it started as.
    """
    if isinstance(value, str):
        return [value] if value.strip() else []
    if not isinstance(value, list):
        return []
    items = [str(item) for item in value]
    if len(items) > 5 and all(len(item) <= 1 for item in items):
        joined = "".join(items).strip()
        return [joined] if joined else []
    return [item for item in items if item.strip()]


def recent_decisions(settings: Settings, limit: int = 150) -> list[dict[str, Any]]:
    """Every decision the brain made, including a plain PASS — the full reasoning."""
    rows = read_jsonl(sorted(settings.journal_dir.glob("decisions-*.jsonl")))
    rows.sort(key=lambda r: r.get("ts") or "", reverse=True)
    rows = rows[:limit]
    for row in rows:
        row["risks"] = _readable_list(row.get("risks"))
    return rows


# Which rail turned a setup away, in the operator's language. The journal has
# always carried the raw code in ``blocked_by``; the page only ever said "the
# rails refused it", which is the one thing you already knew. Naming the rail
# is what turns the cost of a guard into something measurable: a column full of
# ``position_cap`` means the single-open-trade rule is what you are paying for,
# and a column full of ``declared_trigger`` means the lock is.
RAIL_LABELS: dict[str, str] = {
    "daily_trade_cap": "بلغ سقف الصفقات اليومي",
    "position_cap": "صفقة أخرى كانت مفتوحة",
    "circuit_breaker": "قاطع الخسارة اليومي",
    "declared_trigger_unmet": "المستوى الذي أعلنه لم يتحقق بعد",
    "spread_too_wide": "الفرق بين العرض والطلب واسع",
    "thin_contract": "سيولة العقد ضعيفة",
    "contract_not_found": "العقد غير موجود للتسعير",
    "contract_untradeable": "لا يوجد سعر صالح للعقد",
    "no_targets": "دخول بلا هدف محدّد",
    "no_stop": "دخول بلا مستوى إبطال",
    "invalid_stop": "مستوى الوقف غير صالح",
    "below_target_bar": "العائد المتوقع تحت الحد الأدنى",
    "stale_data": "البيانات متأخرة",
    "unusable_data": "بيانات غير صالحة",
    "outside_session": "خارج جلسة التداول",
}


def rail_label(blocked_by: object) -> str:
    """Turn the raw rail codes on a missed row into one readable Arabic reason."""
    if not isinstance(blocked_by, list) or not blocked_by:
        return "رفضه الذكاء بنفسه"
    seen: list[str] = []
    for entry in blocked_by:
        if not isinstance(entry, str):
            continue
        code = entry.split(":", 1)[0].strip()
        label = RAIL_LABELS.get(code, code)
        if label not in seen:
            seen.append(label)
    return " + ".join(seen) if seen else "رفضته الحواجز"


def recent_missed(settings: Settings, limit: int = 100) -> list[dict[str, Any]]:
    rows = read_jsonl(sorted(settings.journal_dir.glob("missed-*.jsonl")))
    rows.sort(key=lambda r: r.get("ts") or "", reverse=True)
    rows = rows[:limit]
    for row in rows:
        row["rail_label"] = rail_label(row.get("blocked_by"))
        row["rail_detail"] = " | ".join(
            e for e in (row.get("blocked_by") or []) if isinstance(e, str)
        )
    return rows


BIAS_THRESHOLD = 0.2  # the same line the missed-opportunity ledger draws


def bias_study(settings: Settings) -> dict[str, Any]:
    """Is the engine long-blind, or did the market simply never offer longs?

    Every trade in the record is a PUT, which permits two very different
    explanations: a genuine bearish stretch honestly traded, or a machine
    whose bar for CALLs sits higher than its bar for PUTs. Opinions cannot
    separate those; three ledgers it already keeps can.

    **Decisions** carry the snapshot's net bias, so bullish moments and
    bearish moments can be counted, and the entry rate compared per side —
    the cleanest test, because it asks the same question of both directions.

    **Missed opportunities** carry a direction and a peak, so the CALLs it
    refused that then paid can be counted and priced. Peaks are ceilings the
    tape touched, not realistic exits — the page says so rather than letting
    a flattering number stand.

    **Trades** show what was actually taken.

    The verdict is deliberately conservative: a small bullish sample returns
    "unproven", not "innocent" — absence of evidence, stated as such.
    """
    # -- what was actually traded
    closed = [
        t
        for t in _latest_trades(settings)
        if t.get("closed_at") and t.get("return_pct") is not None
    ]
    trades: dict[str, dict[str, Any]] = {}
    for row in closed:
        direction = (row.get("decision") or {}).get("direction") or "?"
        bucket = trades.setdefault(
            direction, {"count": 0, "wins": 0, "net_pct": 0.0, "best": None}
        )
        result = float(row["return_pct"])
        bucket["count"] += 1
        bucket["wins"] += int(result > 0)
        bucket["net_pct"] = round(bucket["net_pct"] + result, 1)
        bucket["best"] = result if bucket["best"] is None else max(bucket["best"], result)

    # -- how the brain behaved when the tape leaned each way
    behaviour = {
        side: {"moments": 0, "entered": 0, "waited": 0, "passed": 0}
        for side in ("bullish", "bearish")
    }
    for row in read_jsonl(sorted(settings.journal_dir.glob("decisions-*.jsonl"))):
        bias = (row.get("snapshot") or {}).get("net_bias")
        if bias is None or abs(bias) < BIAS_THRESHOLD:
            continue
        side = behaviour["bullish" if bias > 0 else "bearish"]
        side["moments"] += 1
        action = row.get("action")
        if action == "ENTER":
            side["entered"] += 1
        elif action == "WAIT":
            side["waited"] += 1
        else:
            side["passed"] += 1
    for side in behaviour.values():
        side["enter_rate"] = (
            round(side["entered"] / side["moments"] * 100, 1) if side["moments"] else None
        )

    # -- the opportunities it turned away, priced at their (ceiling) peaks
    missed: dict[str, dict[str, Any]] = {
        "CALL": {"count": 0, "declined_by_brain": 0, "sum_peak": 0.0, "max_peak": None},
        "PUT": {"count": 0, "declined_by_brain": 0, "sum_peak": 0.0, "max_peak": None},
    }
    for row in read_jsonl(sorted(settings.journal_dir.glob("missed-*.jsonl"))):
        bucket = missed.get(row.get("would_be_direction") or "")
        if bucket is None:
            continue
        peak = float(row.get("peak_return_pct") or 0.0)
        bucket["count"] += 1
        if not row.get("blocked_by"):
            bucket["declined_by_brain"] += 1
        bucket["sum_peak"] = round(bucket["sum_peak"] + peak, 1)
        bucket["max_peak"] = peak if bucket["max_peak"] is None else max(bucket["max_peak"], peak)

    # -- the verdict, stated no stronger than the sample allows
    bull, bear = behaviour["bullish"], behaviour["bearish"]
    call_declined = missed["CALL"]["declined_by_brain"]
    if bull["moments"] < 10:
        verdict = (
            f"العيّنة الصاعدة غير كافية للحكم: {bull['moments']} لحظة صاعدة فقط في "
            f"السجل مقابل {bear['moments']} هابطة. السوق لم يعرض ما يُدان به المحرك "
            "أو يُبرَّأ — الامتحان الحقيقي هو أول أسبوع صاعد."
        )
        status = "unproven"
    elif (
        call_declined >= 3
        and bear["enter_rate"] is not None
        and bull["enter_rate"] is not None
        and bear["enter_rate"] >= 2 * max(bull["enter_rate"], 0.1)
    ):
        verdict = (
            f"الانحياز مثبت بالأرقام: في اللحظات الهابطة دخل بمعدل {bear['enter_rate']}% "
            f"مقابل {bull['enter_rate']}% في الصاعدة، ورفض {call_declined} فرصة كول "
            f"بلغت قممها {missed['CALL']['sum_peak']:+.0f}% مجتمعة. الحاجز أعلى على "
            "جهة الشراء فعلًا."
        )
        status = "biased"
    else:
        verdict = (
            f"لا دليل كافيًا على انحياز: اللحظات الصاعدة {bull['moments']} والدخول فيها "
            f"{bull['enter_rate']}% مقابل {bear['enter_rate']}% في الهابطة، وفرص الكول "
            f"المرفوضة {call_declined}. الفارق داخل حدود ما يفسّره اتجاه السوق نفسه."
        )
        status = "clear"

    return {
        "trades": trades,
        "behaviour": behaviour,
        "missed": missed,
        "verdict": verdict,
        "status": status,
    }


def execution_orders(settings: Settings, limit: int = 200) -> dict[str, Any]:
    """What the wallet did, next to what the paper record said.

    The paper trade is the intent and the order journal is what happened to
    it, so the interesting number is neither on its own — it is the gap. That
    gap is slippage, and it is the one cost a paper record structurally cannot
    show. Measuring it is the whole reason both are written down.

    Works with execution switched off, where every row is a withheld intent:
    the file fills with what would have been sent long before anything is,
    and the page is legible on the first live day rather than new.
    """
    rows = read_jsonl(sorted(settings.journal_dir.glob("orders-*.jsonl")))
    rows.sort(key=lambda r: r.get("ts") or "", reverse=True)
    rows = rows[:limit]

    priced = {t.get("trade_id"): t for t in _latest_trades(settings)}
    sent = 0
    slippage: list[float] = []
    for row in rows:
        order = row.get("order") or {}
        fill = order.get("average_fill_price")
        row["fill_price"] = fill
        row["notional"] = round((row.get("quantity") or 0) * (row.get("limit_price") or 0) * 100, 2)
        row["armed"] = bool(row.get("armed"))
        if row["armed"] and row.get("outcome") == "submitted":
            sent += 1
        # the asked price versus the paid one, signed so a buy filling above
        # and a sell filling below both read as a cost
        if fill and row.get("limit_price"):
            raw = (fill - row["limit_price"]) / row["limit_price"] * 100.0
            row["slippage_pct"] = round(raw if row.get("side") == "BUY" else -raw, 2)
            slippage.append(row["slippage_pct"])
        else:
            row["slippage_pct"] = None
        trade = priced.get(row.get("trade_id"))
        row["paper_return_pct"] = trade.get("return_pct") if trade else None

    return {
        "rows": rows,
        "total": len(rows),
        "sent": sent,
        "withheld": len(rows) - sent,
        "avg_slippage_pct": round(sum(slippage) / len(slippage), 2) if slippage else None,
        "measured": len(slippage),
    }


def pending_lessons(settings: Settings) -> list[dict[str, Any]]:
    return Memory(settings.data_dir / "memory.db").pending_lessons()


def memory_counts(settings: Settings) -> dict[str, int]:
    memory = Memory(settings.data_dir / "memory.db")
    counts = memory.counts()
    counts["subscribers"] = memory.subscriber_counts()["trial"]
    return counts


def report_card(settings: Settings) -> dict[str, Any]:
    """Where the engine wins and loses: closed trades bucketed by regime,
    entry hour, stated confidence, and exit reason.

    After enough trades this page answers the questions that matter — "does
    confidence 6 ever pay?", "should the first hour be off-limits?" — with
    arithmetic instead of memory. Below ~10 trades per bucket it is a sketch,
    and the template says so.
    """
    closed = [
        t
        for t in _latest_trades(settings)
        if t.get("closed_at") and t.get("return_pct") is not None
    ]

    def _rows(label_of) -> list[dict[str, Any]]:
        buckets: dict[str, list[float]] = {}
        for trade in closed:
            label = label_of(trade)
            if label is None:
                continue
            buckets.setdefault(str(label), []).append(trade["return_pct"])
        rows = [
            {
                "bucket": bucket,
                "trades": len(returns),
                "win_rate": round(100.0 * sum(r > 0 for r in returns) / len(returns)),
                "avg_return": round(sum(returns) / len(returns), 1),
                "best": round(max(returns), 1),
                "worst": round(min(returns), 1),
            }
            for bucket, returns in buckets.items()
        ]
        return sorted(rows, key=lambda r: r["avg_return"], reverse=True)

    def _hour(trade: dict[str, Any]) -> str | None:
        opened = _parse_ts(trade.get("opened_at"))
        if opened is None:
            return None
        local = opened.astimezone(MARKET_TZ)
        return f"{local.hour:02d}:00-{local.hour + 1:02d}:00 ET"

    returns = [t["return_pct"] for t in closed]
    return {
        "total": len(closed),
        "avg_return": round(sum(returns) / len(returns), 1) if returns else 0.0,
        "win_rate": (
            round(100.0 * sum(r > 0 for r in returns) / len(returns)) if returns else 0
        ),
        "dimensions": [
            {"title": "حسب نظام السوق", "rows": _rows(
                lambda t: (t.get("snapshot_at_entry") or {}).get("regime")
            )},
            {"title": "حسب ساعة الدخول", "rows": _rows(_hour)},
            {"title": "حسب الثقة المعلنة", "rows": _rows(
                lambda t: f"{(t.get('decision') or {}).get('confidence', '?')}/10"
            )},
            {"title": "حسب سبب الخروج", "rows": _rows(
                lambda t: t.get("exit_reason") or None
            )},
        ],
    }


def shadow_overview(settings: Settings) -> dict[str, Any]:
    """The shadow stock desk's record: per-symbol scoreboard plus the raw
    decisions and simulated trades behind it.

    Reads the shadow subdirectory only — the live QQQ pages glob the top-level
    journal dir, and the two records must never mix. Every number here is a
    simulation on model-priced weekly contracts, and the page says so.
    """
    from qqq_alpha.data.massive import parse_occ_symbol

    shadow_dir = settings.journal_dir / "shadow"

    def _symbol_of(row: dict[str, Any]) -> str | None:
        occ = row.get("occ_symbol")
        if occ:
            try:
                return parse_occ_symbol(occ)[0]
            except (ValueError, IndexError):
                pass
        return (row.get("snapshot") or row.get("snapshot_at_entry") or {}).get("symbol")

    trades_latest: dict[str, dict[str, Any]] = {}
    for row in read_jsonl(sorted(shadow_dir.glob("trades-*.jsonl"))):
        if row.get("trade_id"):
            trades_latest[row["trade_id"]] = row
    trades = sorted(
        trades_latest.values(), key=lambda r: r.get("opened_at") or "", reverse=True
    )
    for trade in trades:
        trade["symbol"] = _symbol_of(trade)

    decisions = read_jsonl(sorted(shadow_dir.glob("decisions-*.jsonl")))
    decisions.sort(key=lambda r: r.get("ts") or "", reverse=True)
    for row in decisions:
        row["symbol"] = _symbol_of(row)

    symbols = []
    for symbol in settings.shadow_symbols:
        mine = [t for t in trades if t.get("symbol") == symbol]
        closed = [
            t for t in mine if t.get("closed_at") and t.get("return_pct") is not None
        ]
        returns = [t["return_pct"] for t in closed]
        my_decisions = [d for d in decisions if d.get("symbol") == symbol]
        symbols.append(
            {
                "symbol": symbol,
                "decisions": len(my_decisions),
                "entries": sum(1 for d in my_decisions if d.get("action") == "ENTER"),
                "open": sum(1 for t in mine if not t.get("closed_at")),
                "closed": len(closed),
                "win_rate": (
                    round(100.0 * sum(r > 0 for r in returns) / len(returns))
                    if returns
                    else None
                ),
                "avg_return": (
                    round(sum(returns) / len(returns), 1) if returns else None
                ),
                "total_return": round(sum(returns), 1) if returns else None,
            }
        )

    return {"symbols": symbols, "decisions": decisions[:60], "trades": trades[:60]}


def daily_report(settings: Settings, day: date) -> ReviewStats:
    return review(load_period(settings.journal_dir, since=day, until=day))


def weekly_report(settings: Settings, end_day: date) -> ReviewStats:
    return review(load_period(settings.journal_dir, since=end_day - timedelta(days=6), until=end_day))


def today_et() -> date:
    return datetime.now(MARKET_TZ).date()


SUBSCRIBER_STATUS_AR = {
    "trial": "تجريبي نشط",
    "expired": "منتهي",
}


def subscribers(settings: Settings) -> list[dict]:
    """The roster, ready to render: who, since when, and how long is left.

    Shows expired rows too. The overview's headline counts only live trials,
    so an operator who knows two friends signed up can be shown "1" with no
    way to tell whether the second lapsed or never finished signing up. This
    list is where that question gets answered.
    """
    memory = Memory(settings.data_dir / "memory.db")
    now = datetime.now(UTC)
    talk = memory.conversation_summary()
    rows = []
    for row in memory.all_subscribers():
        chat = talk.get(str(row.get("chat_id")), {})
        expires = _as_utc(row.get("expires_at"))
        joined = _as_utc(row.get("joined_at"))
        # a row can still read 'trial' after its expiry: the sweep that flips
        # it runs at session roll, so believe the clock rather than the column
        lapsed = expires is not None and expires <= now
        status = "expired" if lapsed else (row.get("status") or "trial")
        rows.append(
            {
                "chat_id": row.get("chat_id"),
                "name": row.get("first_name") or row.get("username") or row.get("chat_id"),
                "username": row.get("username"),
                "joined_at": joined,
                "expires_at": expires,
                "days_left": (
                    max(0, math.ceil((expires - now).total_seconds() / 86400))
                    if expires and not lapsed
                    else 0
                ),
                "days_in": (
                    max(0, (now - joined).days) if joined else None
                ),
                "status": status,
                "status_label": SUBSCRIBER_STATUS_AR.get(status, status),
                "plan": (
                    f"مدفوع — {row.get('plan')}" if row.get("plan")
                    else f"تجريبي مجاني — {settings.trial_days} يومًا"
                ),
                "active": not lapsed,
                "tv_username": row.get("tv_username") or "",
                "consented_at": _as_utc(row.get("consented_at")),
                "message_count": chat.get("count", 0),
                "last_message_at": _as_utc(chat.get("last_at")),
            }
        )
    return rows


def conversation(settings: Settings, chat_id: str) -> dict:
    """One subscriber's chat with the bot, oldest first, with the row itself."""
    memory = Memory(settings.data_dir / "memory.db")
    row = memory.subscriber(chat_id) or {}
    lines = []
    for m in memory.messages_for(chat_id):
        lines.append(
            {
                "direction": m["direction"],
                "text": m["text"],
                "at": _as_utc(m["at"]),
            }
        )
    return {
        "chat_id": chat_id,
        "name": row.get("first_name") or row.get("username") or chat_id,
        "username": row.get("username") or "",
        "tv_username": row.get("tv_username") or "",
        "status": row.get("status") or "",
        "expires_at": _as_utc(row.get("expires_at")),
        "lines": lines,
    }


def start_languages(settings: Settings) -> list[dict]:
    """Everyone who ever pressed /start, grouped by Telegram app language.

    The number that decides whether an English channel is ever worth
    building — measured at the top of the funnel, before consent filters
    anyone out.
    """
    counts = Memory(settings.data_dir / "memory.db").start_language_counts()
    total = sum(counts.values()) or 1
    return [
        {"language": lang, "count": count, "pct": round(count / total * 100)}
        for lang, count in counts.items()
    ]


def _as_utc(stamp: str | None) -> datetime | None:
    if not stamp:
        return None
    try:
        parsed = datetime.fromisoformat(stamp)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
