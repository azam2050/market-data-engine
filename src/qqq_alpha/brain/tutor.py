"""The daily concept lesson: Claude teaches, and never reveals the kitchen.

One short lesson a day, on a rotating syllabus of option-market concepts
(delta, gamma, theta, implied volatility, ...), written in plain Arabic with
one analogy and one closing rule. The operator's standing constraints are
structural here, not stylistic:

- no imperative or advisory language — the lesson explains phenomena, it
  never tells anyone to do anything;
- no reference to this desk's own system, strategy, or management numbers —
  curiosity about "how is this handled in practice?" is what the channel's
  case studies are for;
- no prices, strikes, or levels — a lesson must still be true next month.

Every generated lesson passes through :func:`gate_violations` before it can
be published. A lesson that trips the gate is dropped and logged, never
edited into compliance — an automated editor would eventually learn to
launder exactly the content the gate exists to stop.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

from qqq_alpha.brain.resilience import call_with_retry
from qqq_alpha.config import Settings

log = logging.getLogger(__name__)

# the syllabus rotates by calendar day, so it needs no stored cursor and
# survives restarts; a holiday skips a topic, which is harmless — the cycle
# still covers everything before repeating
TOPICS: list[tuple[str, str]] = [
    ("الدلتا", "كم يتحرك سعر العقد عندما يتحرك السهم دولاراً واحداً — سرعة العقد"),
    ("الجاما", "لماذا تنقلب عقود اليوم الأخير فجأة — تسارع الدلتا نفسها"),
    ("الثيتا", "الإيجار اليومي الذي يدفعه حامل العقد للزمن — تبخر القيمة مع كل ساعة"),
    ("التقلب الضمني", "الترقب المسعّر داخل العقد — لماذا يغلى قبل الحدث ويرخص بعده"),
    ("القيمة الزمنية والقيمة الجوهرية", "مما يتركب سعر العقد فعلياً — جزء حقيقي وجزء أمل"),
    ("حجم التداول والعقود المفتوحة", "كيف تُقرأ سيولة عقد قبل الاقتراب منه"),
    ("فارق العرض والطلب", "الباب الضيق — التكلفة الخفية التي تُدفع مرتين، دخولاً وخروجاً"),
    ("الفيغا", "حساسية سعر العقد لتغير التقلب — الربح والخسارة بلا حركة سهم"),
    ("داخل النطاق وخارجه", "ITM وATM وOTM — ماذا تعني المسافة عن سعر السهم"),
    ("الرافعة في العقود", "لماذا يتضاعف العقد بينما السهم يتحرك واحداً بالمئة"),
    ("المتوسط المرجّح بالحجم VWAP", "السعر العادل للجلسة — المرجع الذي تقيس به الحركة"),
    ("الدعم والمقاومة", "ذاكرة الأسعار — لماذا تتباطأ الحركة عند مستويات بعينها"),
    ("فجوات الافتتاح", "عندما يفتح السوق بعيداً عن إغلاقه — ماذا تقول الفجوة وماذا لا تقول"),
    ("عقود اليوم الواحد 0DTE", "يوم كامل من حياة عقد يولد ويموت في جلسة واحدة"),
]

# ---------------------------------------------------------------- the gate
# Advisory verbs and phrases: the lesson describes, it never instructs.
_IMPERATIVE = [
    "ادخل",
    "اشترِ",
    "اشتري الآن",
    "بِع ",
    "بع الآن",
    "ننصح",
    "نوصي",
    "توصية",
    "استهدف",
    "ضع وقف",
    "فرصة شراء",
    "فرصة بيع",
    "لا تفوت",
]
# The kitchen: any reference to this desk's own machinery or its numbers.
_KITCHEN = [
    "نظامنا",
    "استراتيجيت",
    "خوارزم",
    "محركنا",
    "بوتنا",
    "نبيع النصف",
    "بيع النصف",
    "35%",
    "+35",
]


def gate_violations(text: str) -> list[str]:
    """Everything about this text that makes it unpublishable, by name.

    Returning the reasons (rather than a bool) is what makes a dropped
    lesson diagnosable from the operator note alone.
    """
    violations: list[str] = []
    for phrase in _IMPERATIVE:
        if phrase in text:
            violations.append(f"صيغة توصية: «{phrase.strip()}»")
    for phrase in _KITCHEN:
        if phrase in text:
            violations.append(f"كشف للمطبخ: «{phrase}»")
    # strikes and price levels live in 3+ digit numbers; percentages and the
    # small numbers a concept needs (دلتا 0.50، سنتات) pass freely
    for match in re.finditer(r"\d{3,}(?:\.\d+)?", text):
        tail = text[match.end() : match.end() + 1]
        if tail != "%":
            violations.append(f"رقم يشبه مستوى سعري: {match.group()}")
    return violations


class ConceptTutor:
    """Writes the daily lesson. Stateless between days on purpose."""

    def __init__(self, settings: Settings, client: Any | None = None):
        self.settings = settings
        self._client = client

    def _get_client(self) -> Any:
        if self._client is None:
            from anthropic import AsyncAnthropic

            if not self.settings.anthropic_api_key:
                raise RuntimeError("ANTHROPIC_API_KEY is not configured")
            self._client = AsyncAnthropic(api_key=self.settings.anthropic_api_key)
        return self._client

    @staticmethod
    def topic_for(day: date) -> tuple[str, str]:
        return TOPICS[day.toordinal() % len(TOPICS)]

    async def compose(self, day: date) -> str | None:
        """One lesson, or None — silence over a lesson that broke the rules."""
        topic, angle = self.topic_for(day)
        system = (
            "أنت معلّم أسواق مالية عربي محترف تكتب درساً يومياً قصيراً لقناة "
            "تعليمية متخصصة في عقود خيارات صندوق QQQ.\n\n"
            "قواعد صارمة لا استثناء فيها:\n"
            "- الدرس يشرح الظاهرة فقط. ممنوع أي فعل أمر أو نصيحة أو توصية أو "
            "دعوة لفعل شيء.\n"
            "- ممنوع ذكر أي نظام أو استراتيجية أو طريقة إدارة خاصة بالقناة، "
            "وممنوع كشف أي أرقام قواعد إدارة.\n"
            "- ممنوع ذكر أسعار أو مستويات أو سترايكات حقيقية، وممنوع أي رقم "
            "من ثلاث خانات فأكثر إلا نسبة مئوية.\n"
            "- ممنوع اختلاق أحداث محددة لجلسة اليوم — تكلم عن الظاهرة عموماً.\n"
            "- اللغة عربية فصيحة بسيطة يفهمها المبتدئ، والمصطلح الإنجليزي بين "
            "قوسين عند أول ذكر فقط.\n\n"
            "البنية الإلزامية:\n"
            "السطر الأول: 📚 درس اليوم — {عنوان جذاب قصير}\n"
            "ثم فقرة أو فقرتان (٤ إلى ٧ جمل) تبدأ بتشبيه من الحياة اليومية "
            "يبسّط المفهوم، ثم تشرح الظاهرة في سوق العقود.\n"
            "السطر الأخير: القاعدة: {خلاصة من جملة واحدة، وصفية لا أمرية}.\n"
            "أخرج نص الدرس فقط دون أي مقدمات."
        )
        user = f"موضوع اليوم: {topic} — {angle}"

        client = self._get_client()
        response = await call_with_retry(
            lambda: client.messages.create(
                model=self.settings.anthropic_model,
                max_tokens=1500,
                system=system,
                messages=[{"role": "user", "content": user}],
            ),
            label="daily lesson call",
        )
        text = "".join(
            block.text for block in response.content if getattr(block, "type", "") == "text"
        ).strip()
        if not text:
            log.warning("daily lesson came back empty")
            return None

        violations = gate_violations(text)
        if violations:
            log.warning("daily lesson gated out: %s", "; ".join(violations))
            return None
        return text
