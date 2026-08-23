"""The daily concept lesson: taught by Claude, guarded by the gate.

The gate is the safety property here: a lesson that instructs, reveals the
desk's own management, or quotes price levels must never reach a channel —
dropped, not edited into compliance.
"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from qqq_alpha.brain.tutor import TOPICS, ConceptTutor, gate_violations
from qqq_alpha.config import Settings


def _settings(**overrides) -> Settings:
    return Settings(
        anthropic_api_key="test-key", anthropic_model="test-model", **overrides
    )


CLEAN_LESSON = (
    "📚 درس اليوم — الدلتا: سرعة عقدك مع السهم\n\n"
    "الدلتا ببساطة: كم يتحرك عقدك إذا تحرك السهم دولاراً واحداً. دلتا 0.50 "
    "تعني أن العقد يتحرك 50 سنتاً. وكلما اقترب العقد من سعر السهم زادت "
    "سرعته صعوداً وهبوطاً معاً — نسبة قد تتجاوز 90% في الحالات العميقة.\n\n"
    "القاعدة: الدلتا تصف سرعة العقد — والسرعة العالية طريق أسرع للهدف وللخسارة معاً."
)


# ---------------------------------------------------------------- the gate
def test_a_clean_concept_lesson_passes_the_gate():
    assert gate_violations(CLEAN_LESSON) == []


def test_advisory_language_is_caught_by_name():
    violations = gate_violations("الدلتا مرتفعة اليوم — ادخل الآن قبل فوات الفرصة")
    assert violations and "توصية" in violations[0]


def test_revealing_the_desk_machinery_is_caught():
    for leak in ("نظامنا يتعامل مع هذا آلياً", "عند +35% يحدث كذا", "بيع النصف يؤمّن"):
        assert gate_violations(leak), leak


def test_price_level_numbers_are_caught_but_percentages_pass():
    assert gate_violations("السهم عند مستوى 732.50 يشكل دعماً")  # a level
    assert gate_violations("العقد بسترايك 730 هو الأنشط")  # a strike
    assert gate_violations("قد يرتفع العقد 400% في يوم واحد") == []  # a percentage


# ------------------------------------------------------------- the syllabus
def test_topics_rotate_deterministically_and_cover_everything():
    day = date(2026, 8, 24)
    first_cycle = [ConceptTutor.topic_for(day + timedelta(days=i)) for i in range(len(TOPICS))]
    assert len(set(first_cycle)) == len(TOPICS)  # no repeats within a cycle
    # a restart on the same day picks the same topic — no stored cursor
    assert ConceptTutor.topic_for(day) == ConceptTutor.topic_for(day)


# ------------------------------------------------------------------ compose
class _FakeClaude:
    def __init__(self, text: str):
        self._text = text
        self.messages = SimpleNamespace(create=self._create)

    async def _create(self, **kwargs):
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=self._text)])


@pytest.mark.asyncio
async def test_compose_returns_the_lesson_when_the_gate_is_clean():
    tutor = ConceptTutor(_settings(), client=_FakeClaude(CLEAN_LESSON))
    lesson = await tutor.compose(date(2026, 8, 24))
    assert lesson == CLEAN_LESSON


@pytest.mark.asyncio
async def test_compose_drops_a_lesson_that_trips_the_gate():
    leaky = CLEAN_LESSON + "\n\nولهذا يبيع نظامنا النصف عند +35% دائماً."
    tutor = ConceptTutor(_settings(), client=_FakeClaude(leaky))
    assert await tutor.compose(date(2026, 8, 24)) is None


@pytest.mark.asyncio
async def test_compose_drops_an_empty_reply():
    tutor = ConceptTutor(_settings(), client=_FakeClaude("   "))
    assert await tutor.compose(date(2026, 8, 24)) is None
