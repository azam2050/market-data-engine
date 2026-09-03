"""The admin dashboard app.

Read-only over the journal and memory, with one write path: approving or
rejecting a pending lesson — the same action the Telegram command listener
exposes, offered here too because a screen is sometimes easier than a phone
reply. Nothing here can place, close, or alter a trade.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from qqq_alpha import payments as pay_gateway
from qqq_alpha.brain.playbook import Playbook, load_playbook
from qqq_alpha.config import Settings
from qqq_alpha.dashboard import data
from qqq_alpha.dashboard.auth import require_login
from qqq_alpha.learning import apply_lesson, with_applied_lessons
from qqq_alpha.live.tvbridge import tv_webhook_secret
from qqq_alpha.memory import Memory

log = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

# Moyasar's Apple Pay domain verification file. Apple fetches this exact
# path unauthenticated before it will show the Apple Pay button on /pay —
# it is meant for public hosting, not a secret.
APPLE_PAY_ASSOCIATION_FILE = (
    Path(__file__).parent / "well_known" / "apple-developer-merchantid-domain-association"
)

# an upper bound on a single grant, so a slipped keystroke in the days box
# cannot hand out a decade of free access
MAX_TRIAL_EXTENSION_DAYS = 365


def create_app(
    settings: Settings,
    status: Any | None = None,
    on_lesson_applied: Callable[[Playbook], None] | None = None,
    on_subscriber_change: Callable[[str, dict, int | None], Awaitable[None]] | None = None,
    channel_roster: Callable[[list[str]], Awaitable[dict]] | None = None,
    on_payment: Callable[[str, str, dict], Awaitable[None]] | None = None,
    on_tv_signal: Callable[[str], Awaitable[None]] | None = None,
) -> FastAPI:
    """Build the dashboard app.

    ``status`` is the live engine's own ``LiveStatus`` object when the
    dashboard runs embedded in a live session — read directly, never copied,
    so the overview page always reflects the current run. ``on_lesson_applied``
    lets an embedded dashboard update the engine's *running* playbook the
    moment a lesson is approved here, not just the file on disk.

    ``on_subscriber_change`` is how an operator action on the subscribers
    page reaches Telegram — extending a trial should tell the subscriber,
    and removing one must also remove them from the private channel. Left
    unset (the standalone dashboard), those edits touch the database only,
    and the page says so rather than implying a door was closed.
    """
    app = FastAPI(title="QQQ Alpha — لوحة التحكم", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    login = require_login(settings)

    def _ctx(**extra: Any) -> dict[str, Any]:
        return {"status": status, **extra}

    @app.get("/health")
    def health():
        """Unauthenticated liveness probe — deliberately the one open route.

        A process that has been killed cannot report that it was killed. The
        only way to learn that the engine died is for something outside it to
        notice, so this endpoint exists for an external uptime monitor to poll
        every few minutes. It exposes no trades, no keys and no subscriber
        data: just "this process is answering, and here is when it last saw a
        bar".
        """
        last_bar = getattr(status, "last_bar_at", None) if status else None
        age = (datetime.now(UTC) - last_bar).total_seconds() if last_bar else None
        return {
            "ok": True,
            "started_at": getattr(status, "started_at", None) if status else None,
            "last_bar_at": last_bar,
            "last_bar_age_sec": round(age) if age is not None else None,
            "trades_today": getattr(status, "trades_today", None) if status else None,
            "reconnects": getattr(status, "reconnects", None) if status else None,
        }

    @app.api_route(
        "/.well-known/apple-developer-merchantid-domain-association",
        methods=["GET", "HEAD"],
    )
    @app.api_route(
        "/.well-known/apple-developer-merchantid-domain-association.txt",
        methods=["GET", "HEAD"],
    )
    def apple_pay_domain_association():
        """Proof of domain ownership for Apple Pay — Apple fetches this
        unauthenticated at exactly this path before enabling the button.
        A 404 here is why Apple Pay opens and immediately closes itself.
        HEAD is explicit: FastAPI 405s a bare @app.get on HEAD, and domain
        validators commonly probe with HEAD before the real GET.
        Both the bare and .txt spellings are served: Apple's original spec
        named the file with .txt, and Moyasar's validator rejects a domain
        with "must show the verification text file" when only the bare
        path answers."""
        if not APPLE_PAY_ASSOCIATION_FILE.exists():
            return PlainTextResponse("", status_code=404)
        return PlainTextResponse(
            APPLE_PAY_ASSOCIATION_FILE.read_text(), media_type="text/plain"
        )

    # ------------------------------------------------------------------
    # Payments — the three deliberately public routes. The pay page carries
    # the CHANNEL's identity (the shared gateway account belongs to another
    # brand), the signature stops link forgery, and the webhook trusts
    # nothing it is told: the payment is re-fetched from Moyasar with the
    # secret key before a single day is granted.
    @app.get("/pay")
    def pay(request: Request, u: str = "", t: str = "", p: str = ""):
        base = settings.public_base_url.rstrip("/")
        if not pay_gateway.payments_configured(settings):
            return templates.TemplateResponse(
                request,
                "pay_done.html",
                {
                    "ok": False,
                    "message": "الدفع غير مفعّل بعد — سيصلك إشعار عند إتاحته.",
                    "brand_name": settings.brand_name,
                },
            )
        if not pay_gateway.verify_chat_signature(settings, u, t):
            return templates.TemplateResponse(
                request,
                "pay_done.html",
                {
                    "ok": False,
                    "message": "هذا الرابط غير صالح — اطلب رابط الدفع الخاص بك من البوت.",
                    "brand_name": settings.brand_name,
                },
            )
        plan = p if p in pay_gateway.PLAN_LABELS else pay_gateway.DEFAULT_PLAN
        plan_label = pay_gateway.PLAN_LABELS[plan]
        return templates.TemplateResponse(
            request,
            "pay.html",
            {
                "brand_name": settings.brand_name,
                "brand_logo_url": settings.brand_logo_url,
                "price": pay_gateway.plan_price_sar(settings, plan),
                "days": settings.subscription_days,
                "plan": plan,
                "plan_label": plan_label,
                "amount_halalas": pay_gateway.expected_amount_halalas(settings, plan),
                "description": f"اشتراك شهري {plan_label} — {settings.brand_name}",
                "publishable_key": settings.moyasar_publishable_key,
                "callback_url": f"{base}/pay/done",
                "product": pay_gateway.PRODUCT_TAG,
                "chat_id": u,
                "signature": t,
                "statement_name": settings.statement_name,
            },
        )

    @app.get("/pay/done")
    def pay_done(request: Request, status: str = "", message: str = ""):
        return templates.TemplateResponse(
            request,
            "pay_done.html",
            {
                "ok": status == "paid",
                "message": message,
                "brand_name": settings.brand_name,
            },
        )

    # -------------------------------------------------- TradingView webhook
    # the indicator's eyes reach the engine here: the operator pastes this
    # secret URL once into each TradingView alert, and every signal the
    # indicator fires lands in this process within a second.
    #
    # Two secrets can open this door, and only ever one at a time. The
    # operator can issue a fresh one from the bot; the moment they do, it is
    # stored and the derived fallback below stops being accepted — so a link
    # that reached the wrong hands, or an account no longer in use, is
    # revoked by issuing a new one rather than by a redeploy.
    _tv_tasks: set[asyncio.Task[None]] = set()

    @app.post("/tv/{secret}")
    async def tradingview_signal(secret: str, request: Request):
        if not settings.telegram_bot_token:
            return JSONResponse({"ok": False}, status_code=404)
        expected = tv_webhook_secret(settings, Memory(settings.data_dir / "memory.db"))
        if not secrets.compare_digest(secret, expected):
            return JSONResponse({"ok": False}, status_code=404)
        raw = (await request.body())[:2000].decode("utf-8", errors="replace").strip()
        if raw and on_tv_signal is not None:
            # answer TradingView at once: the analysis and the chain lookups
            # take seconds, and a slow answer makes TradingView re-send the
            # same alert. The task set keeps the work alive until it is done.
            task = asyncio.create_task(on_tv_signal(raw))
            _tv_tasks.add(task)
            task.add_done_callback(_tv_tasks.discard)
            # let the handler take its first step (parse, dedupe) before the
            # reply goes out; it carries on in the background from there
            await asyncio.sleep(0)
        return {"ok": True}

    @app.post("/moyasar/webhook")
    async def moyasar_webhook(request: Request):
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001 - malformed input is not our problem
            return JSONResponse({"ok": False}, status_code=400)
        if (
            settings.moyasar_webhook_secret
            and body.get("secret_token") != settings.moyasar_webhook_secret
        ):
            return JSONResponse({"ok": False}, status_code=403)
        if body.get("type") != "payment_paid":
            return {"ok": True, "ignored": "event type"}

        claimed = body.get("data") or {}
        claimed_meta = claimed.get("metadata") or {}
        if claimed_meta.get("product") != pay_gateway.PRODUCT_TAG:
            # the shared account's other app — none of our business
            return {"ok": True, "ignored": "other product"}
        payment_id = str(claimed.get("id") or "")
        if not payment_id or not pay_gateway.payments_configured(settings):
            return {"ok": True, "ignored": "unconfigured"}

        # the webhook body is a claim; Moyasar's API is the truth. A 503 on
        # fetch failure makes Moyasar retry later instead of losing the event.
        payment = await pay_gateway.fetch_payment(settings, payment_id)
        if payment is None:
            return JSONResponse({"ok": False, "retry": True}, status_code=503)

        meta = payment.get("metadata") or {}
        chat_id = str(meta.get("telegram_id") or "")
        problems = pay_gateway.payment_problems(settings, payment)
        if problems:
            log.warning("payment %s rejected: %s", payment_id, "; ".join(problems))
            if on_payment is not None:
                await on_payment(
                    "rejected", chat_id, {"payment_id": payment_id, "problems": problems}
                )
            return {"ok": True, "activated": False}

        now = datetime.now(UTC)
        plan = str(meta.get("plan") or "")
        memory = Memory(settings.data_dir / "memory.db")
        if not memory.record_payment(
            payment_id, chat_id, int(payment.get("amount") or 0),
            str(payment.get("currency") or "SAR"), now, plan=plan,
        ):
            return {"ok": True, "duplicate": True}
        if memory.subscriber(chat_id) is None:
            # paid without ever /starting: legal, rare — register on the spot
            # with a zero-length window for the extension to build on
            memory.add_subscriber(chat_id, "", "", joined_at=now, expires_at=now)
        row = memory.extend_subscriber(
            chat_id, settings.subscription_days, now, plan=plan
        )
        memory.clear_reminder(chat_id)
        if on_payment is not None:
            await on_payment(
                "activated",
                chat_id,
                {
                    "payment_id": payment_id,
                    "row": row or {},
                    "amount": int(payment.get("amount") or 0),
                    "plan": plan,
                },
            )
        return {"ok": True, "activated": True}

    @app.get("/")
    def overview(request: Request, _: str = Depends(login)):
        counts = data.memory_counts(settings)
        return templates.TemplateResponse(
            request,
            "overview.html",
            _ctx(
                counts=counts,
                open_trades=data.open_trades(settings),
                recent_decisions=data.recent_decisions(settings, limit=10),
                pending_count=len(data.pending_lessons(settings)),
            ),
        )

    @app.get("/trades")
    def trades(request: Request, _: str = Depends(login)):
        return templates.TemplateResponse(
            request, "trades.html", _ctx(trades=data.recent_trades(settings))
        )

    @app.get("/decisions")
    def decisions(request: Request, _: str = Depends(login)):
        return templates.TemplateResponse(
            request, "decisions.html", _ctx(decisions=data.recent_decisions(settings))
        )

    @app.get("/bias")
    def bias(request: Request, _: str = Depends(login)):
        """Measurement only: is the engine long-blind, or was the market short?

        Renders the three-ledger study — behaviour per bias side, refused
        opportunities priced at their ceilings, and what was actually traded —
        with a verdict that refuses to outrun its sample size.
        """
        return templates.TemplateResponse(
            request, "bias.html", _ctx(study=data.bias_study(settings))
        )

    @app.get("/orders")
    def orders(request: Request, _: str = Depends(login)):
        """Operator-only: what the wallet did, next to what the paper said.

        Deliberately not part of anything a subscriber sees. The channel gets
        the analysis; the slippage between that analysis and a real fill is
        the operator's business alone.
        """
        return templates.TemplateResponse(
            request, "orders.html", _ctx(orders=data.execution_orders(settings))
        )

    @app.get("/missed")
    def missed(request: Request, _: str = Depends(login)):
        return templates.TemplateResponse(
            request, "missed.html", _ctx(missed=data.recent_missed(settings))
        )

    @app.get("/lessons")
    def lessons(request: Request, _: str = Depends(login)):
        # approved lessons come from durable memory, not the (ephemeral) file
        book = with_applied_lessons(
            load_playbook(settings.playbook_path),
            Memory(settings.data_dir / "memory.db"),
        )
        return templates.TemplateResponse(
            request,
            "lessons.html",
            _ctx(
                pending=data.pending_lessons(settings),
                applied=list(reversed(book.lessons)),
                playbook_version=book.version,
            ),
        )

    @app.post("/lessons/{lesson_id}/approve")
    def approve(lesson_id: int, _: str = Depends(login)):
        memory = Memory(settings.data_dir / "memory.db")
        try:
            book = apply_lesson(
                memory, load_playbook(settings.playbook_path), lesson_id, settings
            )
            if on_lesson_applied is not None:
                on_lesson_applied(book)
        except ValueError:
            log.warning("dashboard: lesson %s was not pending", lesson_id)
        return RedirectResponse(url="/lessons", status_code=303)

    @app.post("/lessons/{lesson_id}/reject")
    def reject(lesson_id: int, _: str = Depends(login)):
        Memory(settings.data_dir / "memory.db").set_lesson_status(lesson_id, "rejected")
        return RedirectResponse(url="/lessons", status_code=303)

    @app.get("/report-card")
    def report_card(request: Request, _: str = Depends(login)):
        return templates.TemplateResponse(
            request, "report_card.html", _ctx(card=data.report_card(settings))
        )

    @app.get("/shadow")
    def shadow(request: Request, _: str = Depends(login)):
        return templates.TemplateResponse(
            request, "shadow.html", _ctx(shadow=data.shadow_overview(settings))
        )

    @app.get("/subscribers")
    async def subscribers(request: Request, _: str = Depends(login)):
        rows = data.subscribers(settings)
        roster: dict = {}
        if channel_roster is not None:
            try:
                roster = await channel_roster([row["chat_id"] for row in rows])
            except Exception:  # noqa: BLE001 - a Telegram hiccup must not blank the page
                log.exception("channel roster probe failed")
        inside = roster.get("inside") or {}
        for row in rows:
            row["in_channel"] = inside.get(row["chat_id"])
        return templates.TemplateResponse(
            request,
            "subscribers.html",
            _ctx(
                subscribers=rows,
                connected=on_subscriber_change is not None,
                channel_total=roster.get("channel_total"),
                start_languages=data.start_languages(settings),
            ),
        )

    @app.post("/subscribers/{chat_id}/extend")
    async def extend_subscriber(chat_id: str, request: Request, _: str = Depends(login)):
        """Grant more free days, and tell the subscriber they were granted.

        Async on purpose: the embedded dashboard shares the engine's event
        loop, so the Telegram side-effect can simply be awaited here instead
        of being handed across a thread boundary.

        The form body is parsed with the stdlib rather than FastAPI's ``Form``,
        which would pull in python-multipart. A urlencoded form needs three
        lines to read, and a new runtime dependency is a rebuild this live
        deployment does not need to risk for them.
        """
        raw = parse_qs((await request.body()).decode())
        try:
            days = int(raw.get("days", ["30"])[0])
        except (TypeError, ValueError):
            days = 30
        days = max(1, min(days, MAX_TRIAL_EXTENSION_DAYS))
        row = Memory(settings.data_dir / "memory.db").extend_subscriber(
            chat_id, days, datetime.now(UTC)
        )
        if row is not None and on_subscriber_change is not None:
            await on_subscriber_change("extended", row, days)
        return RedirectResponse(url="/subscribers", status_code=303)

    @app.post("/subscribers/{chat_id}/remove")
    async def remove_subscriber(chat_id: str, _: str = Depends(login)):
        """Delete the record AND close the door.

        Removing the row on its own would leave the person inside the private
        channel still receiving every signal, off the books — worse than not
        removing them at all, because nothing would show they were still there.
        """
        row = Memory(settings.data_dir / "memory.db").remove_subscriber(chat_id)
        if row is not None and on_subscriber_change is not None:
            await on_subscriber_change("removed", row, None)
        return RedirectResponse(url="/subscribers", status_code=303)

    @app.get("/reports")
    def reports(request: Request, day: str | None = None, _: str = Depends(login)):
        target = date.fromisoformat(day) if day else data.today_et()
        return templates.TemplateResponse(
            request,
            "reports.html",
            _ctx(
                target_day=target,
                daily=data.daily_report(settings, target),
                weekly=data.weekly_report(settings, target),
                prev_day=(target - timedelta(days=1)).isoformat(),
                next_day=(target + timedelta(days=1)).isoformat(),
            ),
        )

    return app
