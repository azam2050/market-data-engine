"""The admin dashboard app.

Read-only over the journal and memory, with one write path: approving or
rejecting a pending lesson — the same action the Telegram command listener
exposes, offered here too because a screen is sometimes easier than a phone
reply. Nothing here can place, close, or alter a trade.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

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


def create_app(
    settings: Settings,
    status: Any | None = None,
    on_lesson_applied: Callable[[Playbook], None] | None = None,
) -> FastAPI:
    """Build the dashboard app.

    ``status`` is the live engine's own ``LiveStatus`` object when the
    dashboard runs embedded in a live session — read directly, never copied,
    so the overview page always reflects the current run. ``on_lesson_applied``
    lets an embedded dashboard update the engine's *running* playbook the
    moment a lesson is approved here, not just the file on disk.
    """
    app = FastAPI(title="QQQ Alpha — لوحة التحكم", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    login = require_login(settings)

    def _ctx(**extra: Any) -> dict[str, Any]:
        return {"status": status, **extra}

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
