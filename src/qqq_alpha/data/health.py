"""The day's verdict on its own data, and what caused the holes.

The engine already knew its data was incomplete — it said so inside almost
every decision, and lowered its confidence accordingly. What it never said was
*why*, so the operator was left comparing his own guesses: the network, the
host, the provider. Each of those has a different fix and one of them is
expensive, which makes guessing the wrong thing to do.

The evidence needed to tell them apart is already collected; it was just never
put side by side. A dropped websocket increments the reconnect counter. A host
that freezes the process leaves the counter untouched while the minutes vanish
anyway. And a provider that simply never printed a bar is distinguishable from
both, because the repair pass asked for those minutes and was told they do not
exist.
"""

from __future__ import annotations

from dataclasses import dataclass

from qqq_alpha.data.backfill import RepairLog
from qqq_alpha.data.quality import DataQuality


@dataclass
class DataHealth:
    completeness: float
    missing_minutes: int
    gap_count: int
    longest_gap: int
    reconnects: int
    repair: RepairLog

    @property
    def verdict(self) -> str:
        """Which of the three causes the evidence actually points at."""
        if not self.missing_minutes:
            return "البيانات كاملة — لا فجوات"
        if self.repair.unavailable and not self.repair.recovered:
            return (
                "الدقائق الناقصة لم تُتداول أصلًا — لا خلل عندنا ولا عند المزوّد، "
                "السوق كان هادئًا في تلك اللحظات"
            )
        if self.reconnects:
            return (
                f"البثّ انقطع وأعاد الاتصال {self.reconnects} مرة — "
                "السبب في الاتصال، والفجوات نتيجة طبيعية له"
            )
        return (
            "الاتصال لم ينقطع ولا مرة، ومع ذلك ضاعت دقائق — "
            "إما المزوّد أسقطها أو الاستضافة جمّدت العملية لحظات"
        )

    def message(self) -> str:
        """The after-the-bell report, for the operator's phone."""
        lines = [
            "📊 صحة البيانات اليوم",
            "",
            f"اكتمال: {self.completeness:.0%}",
            f"دقائق ضائعة: {self.missing_minutes} عبر {self.gap_count} فجوة",
        ]
        if self.longest_gap:
            lines.append(f"أطول انقطاع: {self.longest_gap} دقيقة")
        lines.append(f"إعادة اتصال البثّ: {self.reconnects}")
        if self.repair.attempted:
            lines.append(f"الإصلاح التلقائي: {self.repair.summary()}")
        lines += ["", f"🔍 {self.verdict}"]
        return "\n".join(lines)


def assess(quality: DataQuality, reconnects: int, repair: RepairLog) -> DataHealth:
    """Put the day's three witnesses next to each other."""
    missing = sum(count for _, count in quality.gaps)
    longest = max((count for _, count in quality.gaps), default=0)
    return DataHealth(
        completeness=quality.completeness,
        missing_minutes=missing,
        gap_count=len(quality.gaps),
        longest_gap=longest,
        reconnects=reconnects,
        repair=repair,
    )
