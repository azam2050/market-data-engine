"""Playbook loading and evolution.

The playbook is data, not code. That is deliberate: the learning loop can
propose amendments, and you approve them, without anyone shipping a release.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class Lesson(BaseModel):
    id: str
    learned_on: date
    statement: str
    evidence: str
    sample_size: int
    confidence: float = Field(ge=0.0, le=1.0)


class Playbook(BaseModel):
    version: int = 1
    updated: str = ""
    notes: str = ""
    mission: str = ""
    setups: list[dict[str, Any]] = Field(default_factory=list)
    caution: list[dict[str, Any]] = Field(default_factory=list)
    contract_selection: dict[str, Any] = Field(default_factory=dict)
    exits: dict[str, Any] = Field(default_factory=dict)
    lessons: list[Lesson] = Field(default_factory=list)

    def as_prompt_block(self) -> str:
        """Render the playbook the way the brain should read it."""
        lines = [f"PLAYBOOK v{self.version} (guidance, not law)", ""]
        if self.mission:
            lines += ["MISSION:", self.mission.strip(), ""]

        if self.setups:
            lines.append("KNOWN SETUPS:")
            for setup in self.setups:
                lines.append(f"  [{setup.get('id')}] {setup.get('name')}")
                lines.append(f"      what: {str(setup.get('description', '')).strip()}")
                if setup.get("typical_window"):
                    lines.append(f"      when: {setup['typical_window']}")
                if setup.get("fails_when"):
                    lines.append(f"      fails: {setup['fails_when']}")
            lines.append("")

        if self.caution:
            lines.append("CAUTION CONDITIONS (lower confidence, never a hard block):")
            for item in self.caution:
                lines.append(f"  [{item.get('id')}] {item.get('condition')} — {item.get('note')}")
            lines.append("")

        if self.contract_selection:
            lines.append("CONTRACT SELECTION PREFERENCES:")
            for key, value in self.contract_selection.items():
                lines.append(f"  {key}: {str(value).strip()}")
            lines.append("")

        if self.exits:
            lines.append("EXIT MANAGEMENT:")
            for key, value in self.exits.items():
                lines.append(f"  {key}: {str(value).strip()}")
            lines.append("")

        if self.lessons:
            lines.append("LESSONS LEARNED FROM THIS ENGINE'S OWN TRADE HISTORY:")
            for lesson in self.lessons:
                lines.append(
                    f"  [{lesson.id}] {lesson.statement} "
                    f"(n={lesson.sample_size}, confidence={lesson.confidence:.2f})"
                )
                lines.append(f"      evidence: {lesson.evidence}")
            lines.append("")
        else:
            lines.append(
                "LESSONS: none yet. This engine has no verified track record, so treat "
                "every setup above as an untested prior and weight live evidence higher."
            )

        return "\n".join(lines)


def load_playbook(path: Path) -> Playbook:
    if not path.exists():
        return Playbook()
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return Playbook.model_validate(data)


def save_playbook(playbook: Playbook, path: Path) -> None:
    payload = playbook.model_dump(mode="json")
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True, width=100),
        encoding="utf-8",
    )


def append_lesson(playbook: Playbook, lesson: Lesson) -> Playbook:
    """Return a new playbook with the lesson added and the version bumped."""
    return playbook.model_copy(
        update={
            "version": playbook.version + 1,
            "lessons": [*playbook.lessons, lesson],
        }
    )
