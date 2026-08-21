"""Crash-safe session state.

A live engine runs for months. It will be restarted — by a deploy, a reboot, an
OOM kill, or a network partition. If that happens at 11:15 with a position open,
an in-memory-only engine wakes up believing it has no positions, and the trade
that was published to subscribers is never followed up or closed. Silence after
an entry is the single worst thing this system can do to someone relying on it.

So the session is written to disk after every change, and restored on boot when
it belongs to the current session day. Stale state from a previous day is
discarded rather than resumed.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from qqq_alpha.domain import Trade

log = logging.getLogger(__name__)

STATE_VERSION = 1


class SessionState(BaseModel):
    version: int = STATE_VERSION
    session_day: date | None = None
    saved_at: datetime | None = None
    trades_today: int = 0
    realized_pct: float = 0.0
    signals_sent: int = 0
    brain_calls: int = 0
    open_trades: list[Trade] = Field(default_factory=list)
    closed_trades: list[Trade] = Field(default_factory=list)
    # contracts actually held per trade id, as the broker filled them and as
    # the scale-out decremented them. Restored with the positions themselves,
    # because a recovered trade whose size is unknown cannot be sold: the
    # engine would either dump more than it holds or leave a leg behind.
    executed: dict[str, int] = Field(default_factory=dict)

    def belongs_to(self, day: date) -> bool:
        return self.session_day == day


class StateStore:
    """Atomic JSON persistence. Small enough to rewrite on every change."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, state: SessionState) -> None:
        state.saved_at = datetime.now()
        payload = state.model_dump(mode="json")

        # write-then-rename: a crash mid-write must not leave a truncated file
        # that would fail to load and lose the session entirely
        temp = self.path.with_suffix(".tmp")
        temp.write_text(
            json.dumps(payload, ensure_ascii=False, default=str, indent=2), encoding="utf-8"
        )
        temp.replace(self.path)

    def load(self, expected_day: date | None = None) -> SessionState | None:
        if not self.path.exists():
            return None

        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            state = SessionState.model_validate(data)
        except Exception as exc:  # noqa: BLE001 - corrupt state must not crash boot
            log.error("could not read session state (%s); starting fresh", exc)
            return None

        if state.version != STATE_VERSION:
            log.warning("state version %s is not %s; discarding", state.version, STATE_VERSION)
            return None

        if expected_day is not None and not state.belongs_to(expected_day):
            log.info(
                "stored state is from %s, today is %s; discarding",
                state.session_day,
                expected_day,
            )
            return None

        return state

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()
