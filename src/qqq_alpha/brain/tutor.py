"""The daily concept lesson: Claude teaches market-reading, and never reveals
the kitchen.

One short lesson a day, on a rotating syllabus of market-reading concepts
(support and resistance, false breakouts, reversal patterns, ...), written in
plain Arabic as a purely generic, illustrative phenomenon — never framed as
something that happened in a real, specific session. The operator's standing
constraints are structural here, not stylistic:

- no imperative or advisory language — the lesson explains phenomena, it
  never tells anyone to do anything;
- no reference to this desk's own system, strategy, trades, or management
  actions — not even worded neutrally. "دخلنا"، "خرجنا"، "أدرنا"، "هدفنا"،
  "صفقتنا" are all off-limits regardless of phrasing, because a number
  attached to any of them is traceable back to a real trade this desk made;
- no prices, strikes, or levels presented as real data — every example must
  stay illustrative and true in any month, for any stock.

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
    ("الدعم والمقاومة", "كيف \"تتذكر\" السوق منطقة سعرية توقفت عندها الحركة من قبل"),
    ("القاع المزدوج", "لماذا يميل السهم للارتداد عند اختباره منطقة قاع سبق أن صمدت"),
    ("القمة المزدوجة", "لماذا تفشل المحاولة الثانية غالباً عند نفس السقف السابق"),
    ("الاختراق الكاذب", "متى يخترق السعر مستوى ثم يتراجع عنه سريعاً بلا استمرار"),
    ("إغلاق الفجوة السعرية", "لماذا يميل السعر أحياناً للعودة نحو الفجوة التي تركها خلفه"),
    ("تأكيد الحجم", "لماذا يحتاج اختراق أي مستوى حجم تداول يدعمه حتى يُؤخذ على محمل الجد"),
    ("الاتجاه العام مقابل التصحيح المؤقت", "كيف يُفرَّق بين تراجع داخل اتجاه وانعكاس للاتجاه نفسه"),
    ("خطوط الاتجاه", "كيف يعمل خط صاعد أو هابط كدعم أو مقاومة متحركة مع الوقت"),
    ("تباعد السعر عن الزخم", "حين يصنع السعر قمة أعلى بينما يضعف الزخم خلفها — إشارة إنذار مبكر"),
    ("النطاق السعري الجانبي", "حين يتحرك السعر بين سقف وقاع واضحين دون اتجاه حقيقي"),
    ("المتوسطات المتحركة كدعم متحرك", "كيف يتحول متوسط سعري إلى مستوى يُختبر ويُحترم مع الوقت"),
    ("نطاق الافتتاح", "ما تقوله أول حركة في الجلسة عن بقيتها، وما لا تقوله"),
    ("الشمعة الانعكاسية", "إشارة قصيرة على الرسم البياني تُقرأ كتردد عند نهاية موجة"),
    ("تسارع الزخم", "كيف تبدو حركة سعرية متتابعة بلا فترات راحة، وماذا يعنيه ذلك عادة"),
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
# The kitchen: any reference to this desk's own machinery, or any first-person
# system-action verb — a neutral-sounding "دخلنا" or "أدرنا" is still a leak,
# because whatever number rides along with it traces back to a real trade.
_KITCHEN = [
    "نظامنا",
    "استراتيجيت",
    "خوارزم",
    "محركنا",
    "بوتنا",
    "حسابنا",
    "مركزنا",
    "صفقتنا",
    "قرارنا",
    "هدفنا",
    "دخلنا",
    "خرجنا",
    "أدرنا",
    "ندير",
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
            "أنت معلّم أسواق مالية عربي محترف تكتب درساً يومياً قصيراً في "
            "قراءة السوق (تحليل الحركة السعرية) لقناة تعليمية.\n\n"
            "قواعد صارمة لا استثناء فيها:\n"
            "- الدرس يشرح ظاهرة سوقية عامة فقط. ممنوع أي فعل أمر أو نصيحة أو "
            "توصية أو دعوة لفعل شيء.\n"
            "- الدرس مثال توضيحي عام يصلح لأي سهم وأي شهر — وليس وصفاً لما "
            "حدث في جلسة حقيقية اليوم أو أمس. لا تذكر أرقاماً أو مستويات على "
            "أنها بيانات فعلية حدثت، واستخدم صياغة عامة مثل \"قد يحدث\" أو "
            "\"في كثير من الحالات\" بدل السرد الزمني المحدد.\n"
            "- ممنوع نهائياً أي إشارة من قريب أو بعيد إلى نظام أو استراتيجية "
            "أو حساب أو صفقة أو قرار خاص بهذه القناة، وممنوع أي صيغة متكلم "
            "جماعي تصف فعلاً تداولياً مثل \"دخلنا\" أو \"خرجنا\" أو \"أدرنا\" أو "
            "\"هدفنا\" — حتى لو بدت الصياغة محايدة.\n"
            "- ممنوع ذكر أسعار أو مستويات أو سترايكات حقيقية، وممنوع أي رقم "
            "من ثلاث خانات فأكثر إلا نسبة مئوية.\n"
            "- اللغة عربية فصيحة بسيطة يفهمها المبتدئ، والمصطلح الإنجليزي بين "
            "قوسين عند أول ذكر فقط.\n\n"
            "البنية الإلزامية:\n"
            "السطر الأول: قراءة السوق — {عنوان قصير للمفهوم}\n"
            "ثم فقرة أو فقرتان (٤ إلى ٧ جمل) تبدأ بتشبيه من الحياة اليومية "
            "يبسّط المفهوم، ثم تشرح الظاهرة كمثال توضيحي عام غير مرتبط بجلسة "
            "أو سهم بعينه.\n"
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
