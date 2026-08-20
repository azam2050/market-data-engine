"""The admin dashboard app.

Read-only over the journal and memory, with one write path: approving or
rejecting a pending lesson — the same action the Telegram command listener
exposes, offered here too because a screen is sometimes easier than a phone
reply. Nothing here can place, close, or alter a trade.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from fastapi import Depends, FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from qqq_alpha.brain.playbook import Playbook, load_playbook
from qqq_alpha.config import Settings
from qqq_alpha.dashboard import data
from qqq_alpha.dashboard.auth import require_login
from qqq_alpha.learning import apply_lesson, with_applied_lessons
from qqq_alpha.memory import Memory

log = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

# an upper bound on a single grant, so a slipped keystroke in the days box
# cannot hand out a decade of free access
MAX_TRIAL_EXTENSION_DAYS = 365


def create_app(
    settings: Settings,
    status: Any | None = None,
    on_lesson_applied: Callable[[Playbook], None] | None = None,
    on_subscriber_change: Callable[[str, dict, int | None], Awaitable[None]] | None = None,
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
    def subscribers(request: Request, _: str = Depends(login)):
        return templates.TemplateResponse(
            request,
            "subscribers.html",
            _ctx(subscribers=data.subscribers(settings), connected=on_subscriber_change is not None),
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
