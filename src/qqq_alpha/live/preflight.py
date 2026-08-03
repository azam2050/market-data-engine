"""Startup self-test.

Every dependency this engine has is external and can fail silently: a data plan
that does not cover options, an expired key, a WebSocket that authenticates but
returns nothing, a Telegram bot that was never added to the chat.

Discovering any of those at 09:31 on a Monday — from a container with no
terminal attached — is not acceptable. So the engine tests itself on every boot
and reports the result to the same phone the signals go to. The operator needs
no command line and no log access to know whether the system is healthy.

Failures are classified. A missing options plan is fatal and stops the engine; a
Telegram outage is not, because the engine still trades and still journals.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from qqq_alpha.config import MARKET_TZ, Settings

log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    fatal: bool = False

    @property
    def icon(self) -> str:
        if self.ok:
            return "✅"
        return "❌" if self.fatal else "⚠️"


@dataclass
class PreflightReport:
    checks: list[CheckResult] = field(default_factory=list)
    ran_at: datetime | None = None

    @property
    def passed(self) -> bool:
        return all(c.ok for c in self.checks if c.fatal)

    @property
    def all_green(self) -> bool:
        return all(c.ok for c in self.checks)

    def as_message(self) -> str:
        lines = ["🔍 فحص الإقلاع", ""]
        for check in self.checks:
            lines.append(f"{check.icon} {check.name}")
            lines.append(f"    {check.detail}")

        lines.append("")
        if self.all_green:
            lines.append("✅ كل شيء سليم — المحرك يعمل الآن")
        elif self.passed:
            lines.append("⚠️ المحرك يعمل، لكن راجع التحذيرات أعلاه")
        else:
            lines.append("❌ المحرك متوقف — أصلح الأخطاء الحمراء ثم أعد التشغيل")
        return "\n".join(lines)


async def _check_config(settings: Settings) -> CheckResult:
    missing = [
        name
        for name, value in (
            ("MASSIVE_API_KEY", settings.massive_api_key),
            ("ANTHROPIC_API_KEY", settings.anthropic_api_key),
            ("ANTHROPIC_MODEL", settings.anthropic_model),
        )
        if not value
    ]
    if missing:
        return CheckResult(
            "الإعدادات", False, f"مفقود: {', '.join(missing)}", fatal=True
        )
    return CheckResult(
        "الإعدادات",
        True,
        f"{settings.primary_symbol} + {len(settings.leader_symbols)} أسهم قيادية | "
        f"وضع البيانات: {settings.massive_feed_mode}",
    )


async def _check_market_data(settings: Settings) -> CheckResult:
    from qqq_alpha.data.massive import MassiveClient

    try:
        async with MassiveClient(settings) as client:
            today = datetime.now(MARKET_TZ).date()
            bars = await client.daily_bars(
                settings.primary_symbol, today - timedelta(days=10), today
            )
    except Exception as exc:  # noqa: BLE001 - any failure here is a real failure
        return CheckResult("بيانات الأسهم", False, f"فشل: {exc}"[:200], fatal=True)

    if not bars:
        return CheckResult(
            "بيانات الأسهم", False, "الاتصال نجح لكن لم تصل بيانات", fatal=True
        )
    return CheckResult(
        "بيانات الأسهم",
        True,
        f"{settings.primary_symbol} آخر إغلاق {bars[-1].close} بتاريخ "
        f"{bars[-1].ts.astimezone(MARKET_TZ).date()}",
    )


async def _check_option_chain(settings: Settings) -> CheckResult:
    from qqq_alpha.brain.decider import next_expiry
    from qqq_alpha.data.chain import LiveChainPricer

    pricer = LiveChainPricer(settings)
    expiry = next_expiry(datetime.now(MARKET_TZ).date(), 0)

    if not await pricer.refresh(expiry, force=True):
        detail = pricer.last_error or "سبب غير معروف"
        if "NOT_AUTHORIZED" in detail or "403" in detail:
            detail = "اشتراك الخيارات لا يغطي سلسلة العقود — راجع الباقة"
        return CheckResult("سلسلة الخيارات", False, detail[:200], fatal=True)

    count = len(pricer.snapshot.contracts) if pricer.snapshot else 0
    return CheckResult(
        "سلسلة الخيارات", True, f"{count} عقدًا لتاريخ استحقاق {expiry}"
    )


async def _check_stream(settings: Settings) -> CheckResult:
    """Authenticate only. A full data test is meaningless outside market hours."""
    import json

    import websockets

    from qqq_alpha.live.stream import StreamAuthError

    try:
        async with websockets.connect(
            settings.massive_ws_stocks_url, ping_interval=20, close_timeout=5
        ) as socket:
            await socket.send(
                json.dumps({"action": "auth", "params": settings.massive_api_key})
            )
            deadline = asyncio.get_running_loop().time() + 15
            while asyncio.get_running_loop().time() < deadline:
                raw = await asyncio.wait_for(socket.recv(), timeout=15)
                messages = json.loads(raw)
                for message in messages if isinstance(messages, list) else [messages]:
                    if message.get("ev") != "status":
                        continue
                    if message.get("status") == "auth_success":
                        return CheckResult("البث اللحظي", True, "المصادقة نجحت")
                    if message.get("status") in ("auth_failed", "error"):
                        return CheckResult(
                            "البث اللحظي",
                            False,
                            f"رُفضت المصادقة: {message.get('message')}"[:200],
                            fatal=True,
                        )
            raise StreamAuthError("لم تصل استجابة مصادقة خلال ١٥ ثانية")
    except Exception as exc:  # noqa: BLE001
        return CheckResult("البث اللحظي", False, f"فشل: {exc}"[:200], fatal=True)


async def _check_brain(settings: Settings) -> CheckResult:
    """One minimal call. Costs a fraction of a cent and proves the key works."""
    try:
        from anthropic import AsyncAnthropic

        client = AsyncAnthropic(api_key=settings.anthropic_api_key)
        await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=1,
            messages=[{"role": "user", "content": "ok"}],
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult("محرك القرار", False, f"فشل: {exc}"[:200], fatal=True)

    return CheckResult("محرك القرار", True, "المفتاح والموديل يعملان")


async def _check_delivery(settings: Settings) -> CheckResult:
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return CheckResult(
            "تلقرام",
            False,
            "غير مضبوط — الإشارات لن تصل جوالك (المحرك سيعمل ويسجّل رغم ذلك)",
        )

    from qqq_alpha.live.telegram import verify_telegram

    ok, message = await verify_telegram(
        settings.telegram_bot_token, settings.telegram_chat_id
    )
    return CheckResult("تلقرام", ok, message[:200])


async def run_preflight(settings: Settings, include_brain: bool = True) -> PreflightReport:
    """Run every startup check. Never raises — the report is the result."""
    report = PreflightReport(ran_at=datetime.now(MARKET_TZ))

    config = await _check_config(settings)
    report.checks.append(config)
    if not config.ok:
        # nothing else can be tested without credentials
        return report

    checks = [_check_market_data(settings), _check_option_chain(settings), _check_stream(settings)]
    if include_brain:
        checks.append(_check_brain(settings))
    checks.append(_check_delivery(settings))

    results = await asyncio.gather(*checks, return_exceptions=True)
    for result in results:
        if isinstance(result, CheckResult):
            report.checks.append(result)
        else:
            report.checks.append(
                CheckResult("فحص غير متوقع", False, str(result)[:200], fatal=True)
            )

    return report


def next_market_open(now: datetime | None = None) -> date:
    """Next weekday session. Used to tell the operator when to expect signals."""
    current = (now or datetime.now(MARKET_TZ)).date()
    if (now or datetime.now(MARKET_TZ)).hour >= 16:
        current += timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current
