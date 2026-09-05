"""The public channel — the storefront that proves the record.

Everything here is marketing built out of honesty rather than adjectives:

* Two trades a week, on days chosen at random, are shared LIVE — the entry
  card goes out before anyone knows the outcome, then the follow-ups, then
  the close, green or red. Competitors post winners after the fact; this
  channel posts commitments before the fact.
* A daily report after every session — including the days the desk chose not
  to trade, because "no trade today" is capital preservation taught by
  example rather than preached.
* A weekly report with the same accounting the operator sees: a trade counts
  as whatever it actually closed at, never "touched a target once".
* A capital-preservation lesson twice a week, each one explaining a rule the
  engine itself lives by.

Publishing is strictly best-effort: the channel is a shop window, and no
broken window is ever allowed to stop the desk inside.
"""

from __future__ import annotations

import logging
import random
from datetime import date

import httpx

from qqq_alpha.domain import Trade, TradeUpdate
from qqq_alpha.live.notifier import (
    DISCLAIMER,
    EXIT_REASON_AR,
    human_contract,
)
from qqq_alpha.live.telegram import TelegramNotifier

log = logging.getLogger(__name__)

WEEKDAYS = range(5)  # Monday..Friday
SHARES_PER_WEEK = 2

def share_days_for_week(anchor: date, salt: str) -> set[int]:
    """The two weekdays whose first trade goes to the channel, for the week
    containing ``anchor``.

    Deterministic per ISO week — a restart mid-week keeps the same choice —
    but salted with a secret so nobody outside can predict the days and
    free-ride the schedule.
    """
    year, week, _ = anchor.isocalendar()
    rng = random.Random(f"{year}-w{week:02d}-{salt}")
    return set(rng.sample(list(WEEKDAYS), SHARES_PER_WEEK))


class ChannelPublisher:
    """Posts to the public channel. Every method is best-effort by contract:
    a channel failure is logged and swallowed — the desk never stops for the
    shop window."""

    def __init__(self, token: str, channel_id: str, client: httpx.AsyncClient | None = None):
        self.channel_id = channel_id
        self._salt = token[-10:]  # unpredictable outside, stable across restarts
        self._notifier = TelegramNotifier(token, channel_id, client=client)
        # the live share's entry-card message id, so heartbeats can refresh
        # the card in place ("still in the trade — now +X%")
        self._live_messages: dict[str, int] = {}

    # ------------------------------------------------------------------
    def is_share_day(self, day: date) -> bool:
        return day.weekday() in share_days_for_week(day, self._salt)

    # ------------------------------------------------------------------
    async def post_text(self, text: str) -> None:
        try:
            await self._notifier._send(text)
        except Exception:  # noqa: BLE001
            log.exception("channel text post failed")

    async def _post_card(self, png: bytes | None, caption: str, fallback: str) -> int | None:
        try:
            delivered = None
            if png is not None:
                delivered = await self._notifier._post_photo(png, caption=caption)
            if not delivered:
                await self._notifier._send(fallback)
                return None
            return delivered
        except Exception:  # noqa: BLE001
            log.exception("channel card post failed")
            return None

    # ------------------------------------------------------------------
    async def post_trade_entry(self, trade: Trade, delayed: bool) -> None:
        from qqq_alpha.live.telegram import BroadcastNotifier

        png = BroadcastNotifier._render_card("entry", trade, None, delayed)
        contract = human_contract(trade.occ_symbol, trade.opened_at)
        caption = (
            "🔓 دراسة الحالة الأسبوعية المجانية — تُنشر قبل معرفة نتيجتها، "
            "وتُوثَّق هنا حتى خلاصتها.\n"
            f"⚠️ {DISCLAIMER}"
        )
        message_id = await self._post_card(
            png, caption, f"🔓 دراسة حالة: {contract}\n⚠️ {DISCLAIMER}"
        )
        if message_id and message_id > 0:
            self._live_messages[trade.trade_id] = message_id

    async def post_trade_update(self, trade: Trade, update: TradeUpdate, delayed: bool) -> None:
        from qqq_alpha.live.notifier import format_update
        from qqq_alpha.live.telegram import BroadcastNotifier

        if update.note.startswith("status:"):
            # the living card: refresh the posted entry card's badge in place
            message_id = self._live_messages.get(trade.trade_id)
            if message_id and message_id > 0:
                png = BroadcastNotifier._render_card("entry_live", trade, update, delayed)
                if png is not None:
                    try:
                        await self._notifier._edit_photo(self.channel_id, message_id, png)
                    except Exception:  # noqa: BLE001
                        log.exception("live card refresh failed")
            return

        if update.note.startswith("closed:"):
            png = BroadcastNotifier._render_card("close", trade, update, delayed)
            self._live_messages.pop(trade.trade_id, None)
            lesson = EXIT_REASON_AR.get(trade.exit_reason, "")
            caption = (
                f"🔓 خلاصة الحالة: {update.return_pct:+.1f}%"
                + (f"\nالدرس المستفاد: {lesson}" if lesson else "")
            )
        elif update.note.startswith("scale_out"):
            png = BroadcastNotifier._render_card("scale_out", trade, update, delayed)
            caption = "🔓 لحظة مفصلية: تم تأمين التكلفة — بيع نصف الكمية آليًا"
        elif update.note.startswith("target:"):
            png = BroadcastNotifier._render_card("target", trade, update, delayed)
            caption = (
                f"🔓 مجريات الحالة: المحطة تحققت عند "
                f"{update.return_pct:+.1f}% — الحالة ما زالت مفتوحة"
            )
        else:
            return  # anything unrecognised stays out of the channel

        if png is None:
            await self.post_text(f"🔓 مجريات الحالة\n{format_update(trade, update, delayed)}")
        else:
            await self._post_card(png, caption, format_update(trade, update, delayed))

    # ------------------------------------------------------------------
    async def post_watch(self, png: bytes | None, text: str) -> None:
        """The blue under-watch card, shown publicly only on live-share days
        so the audience sees the discipline behind the week's free trades."""
        await self._post_card(png, "🔵 حالة قيد التكوّن — لم تصدر دراستها بعد", text)

    # ------------------------------------------------------------------
    async def post_daily_report(self, day: date, closed_trades: list[Trade]) -> None:
        title = f"📅 تقرير اليوم — {day.isoformat()}"
        if not closed_trades:
            await self.post_text(
                f"{title}\n\n"
                "لم نتداول اليوم.\n"
                "النظام راقب الجلسة كاملة ولم تكتمل شروط أي حالة تستحق المخاطرة — "
                "وحماية رأس المال قرار تداول كامل الأركان. "
                "يوم بلا صفقة أفضل دائمًا من صفقة بلا سبب.\n\n"
                f"⚠️ {DISCLAIMER}"
            )
            return

        rows = [
            {
                "label": human_contract(t.occ_symbol, t.opened_at),
                "return_pct": t.return_pct or 0.0,
                "shared": t.shared_to_channel,
            }
            for t in closed_trades
        ]
        # the table renders as a branded card; the text version below is the
        # fallback of record if drawing or photo delivery ever fails
        lines = [title, ""]
        total = 0.0
        for row in rows:
            result = float(row["return_pct"])
            total += result
            icon = "🟢" if result > 1 else ("⚪" if result >= -1 else "🔴")
            tag = " 🔓" if row["shared"] else ""
            lines.append(f"{icon} {row['label']}: {result:+.1f}%{tag}")
        returns = [float(r["return_pct"]) for r in rows]
        gross_win = sum(r for r in returns if r > 0)
        gross_loss = sum(r for r in returns if r < 0)
        lines += [
            "",
            f"💚 إجمالي الأرباح: {gross_win:+.1f}%",
            f"🔴 إجمالي الخسائر: {gross_loss:+.1f}%",
            f"💰 الصافي: {total:+.1f}%",
            "النتائج كما أُغلقت فعليًا — لا نحسب حالة رابحة لمجرد أنها لامست مستوى ثم انعكست.",
            "",
            f"⚠️ {DISCLAIMER}",
        ]
        png = self._render_report("daily", day=day, rows=rows)
        await self._post_card(png, f"📅 تقرير اليوم — {day.isoformat()}", "\n".join(lines))

    # ------------------------------------------------------------------
    async def post_weekly_report(self, stats, channel_trades: list[dict]) -> None:
        """The weekly scoreboard, with our accounting spelled out."""
        if stats.closed == 0:
            return
        lines = [
            "📊 التقرير الأسبوعي — بوت عقود الخيارات",
            "",
            f"📌 إجمالي الحالات المغلقة: {stats.closed}",
            f"✅ الرابحة: {stats.wins}",
            f"❌ الخاسرة: {stats.losses}",
            f"📈 نسبة الحالات الرابحة: {stats.win_rate:.0f}%",
            f"💚 إجمالي الأرباح: {stats.avg_win_pct * stats.wins:+.1f}%",
            f"🔴 إجمالي الخسائر: {stats.avg_loss_pct * stats.losses:+.1f}%",
            f"💰 الصافي: {stats.expectancy_pct * stats.closed:+.1f}%",
            f"💵 متوسط نتيجة الحالة: {stats.expectancy_pct:+.1f}%",
            f"🏆 أفضل حالة: {stats.best_pct:+.1f}% | أسوأ حالة: {stats.worst_pct:+.1f}%",
        ]
        if channel_trades:
            lines += ["", "🔓 حالات وُثّقت هنا في القناة قبل نتيجتها:"]
            for row in channel_trades:
                lines.append(
                    f"   • {row.get('label', row.get('occ_symbol', '?'))}: "
                    f"{float(row.get('return_pct') or 0):+.1f}%"
                )
        lines += [
            "",
            "منهجيتنا في الحساب: نتيجة الحالة هي ما أُغلق عليه فعليًا — "
            "لا نحسب حالة ناجحة لمجرد أنها لامست محطة ثم انعكست، "
            "والخسائر تُنشر بنفس وضوح الأرباح.",
            "",
            f"⚠️ {DISCLAIMER}",
        ]
        png = self._render_report("weekly", stats=stats, channel_rows=channel_trades)
        await self._post_card(png, "📊 التقرير الأسبوعي — بوت عقود الخيارات", "\n".join(lines))

    async def post_monthly_report(
        self,
        month: date,
        stats,
        daily_returns: list[tuple[date, float]],
        channel_trades: list[dict],
    ) -> None:
        """The month's statement — the curve, the weeks, the drawdown.

        Posted once, after the last session of the month. Where the daily card
        is a receipt and the weekly one a summary, this is the document a
        prospective subscriber judges the desk by, so it leads with the shape
        of the month rather than with its best number.
        """
        if stats.closed == 0:
            return
        from qqq_alpha.live.cards import ARABIC_MONTHS

        label = f"{ARABIC_MONTHS[month.month - 1]} {month.year}"
        net = sum(value for _, value in daily_returns)
        green = sum(1 for _, v in daily_returns if v > 1)
        red = sum(1 for _, v in daily_returns if v < -1)
        peak = drawdown = 0.0
        running = 0.0
        for _, value in daily_returns:
            running += value
            peak = max(peak, running)
            drawdown = min(drawdown, running - peak)

        lines = [
            f"🗓️ البيان الشهري — {label}",
            "",
            f"💰 صافي الشهر: {net:+.1f}%",
            f"📌 الحالات المغلقة: {stats.closed}",
            f"✅ الرابحة: {stats.wins} | ❌ الخاسرة: {stats.losses}"
            f" | 📈 النسبة: {stats.win_rate:.0f}%",
            f"🟢 جلسات رابحة: {green} | 🔴 جلسات خاسرة: {red}",
            f"🏆 أفضل حالة: {stats.best_pct:+.1f}% | أسوأ حالة: {stats.worst_pct:+.1f}%",
            f"📉 أقصى تراجع خلال الشهر: {drawdown:+.1f}%",
            f"💵 متوسط نتيجة الحالة: {stats.expectancy_pct:+.1f}%",
        ]
        if channel_trades:
            lines += ["", "🔓 حالات وُثّقت هنا قبل نتيجتها:"]
            for row in channel_trades:
                lines.append(
                    f"   • {row.get('label', '?')}: "
                    f"{float(row.get('return_pct') or 0):+.1f}%"
                )
        lines += [
            "",
            "أقصى تراجع يعني أكبر هبوط من قمة المسار التراكمي إلى قاعه خلال "
            "الشهر — ننشره لأن الرقم الصافي وحده لا يخبرك كيف كان شعور الطريق.",
            "",
            f"⚠️ {DISCLAIMER}",
        ]
        png = self._render_report(
            "monthly", month=month, stats=stats,
            daily_returns=daily_returns, channel_rows=channel_trades,
        )
        await self._post_card(png, f"🗓️ البيان الشهري — {label}", "\n".join(lines))

    # ------------------------------------------------------------------
    @staticmethod
    def _render_report(kind: str, **kwargs) -> bytes | None:
        """Best-effort report card. A drawing bug degrades to the text post."""
        try:
            from qqq_alpha.live import cards

            if kind == "daily":
                return cards.render_daily_report_card(kwargs["day"], kwargs["rows"])
            if kind == "weekly":
                return cards.render_weekly_report_card(
                    kwargs["stats"], kwargs["channel_rows"]
                )
            if kind == "monthly":
                return cards.render_monthly_report_card(
                    kwargs["month"], kwargs["stats"],
                    kwargs["daily_returns"], kwargs["channel_rows"],
                )
        except Exception:  # noqa: BLE001 - the table is garnish; the numbers must arrive
            log.exception("report card rendering failed; posting text instead")
        return None

    # ------------------------------------------------------------------
    async def aclose(self) -> None:
        await self._notifier.aclose()
