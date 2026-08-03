"""The learning loop.

This is what makes the engine improve rather than merely persist. It reads the
accumulated trade memory, finds what the record actually supports, and proposes
amendments to the playbook — which you approve or reject. Nothing edits the
playbook on its own.

Three deliberate constraints, because an automatic learner with none of them
reliably teaches itself nonsense:

1. **Minimum sample.** No lesson from fewer than 8 trades. Patterns in 3 trades
   are noise wearing a costume.
2. **Effect size, not just direction.** A bucket must differ from the overall
   average by a meaningful margin, not merely be above it.
3. **Human approval.** Proposals are written to the memory as pending and
   reviewed by you. An engine that rewrites its own rules unattended will
   eventually rewrite them badly, at 3am, with no witness.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from statistics import mean

from qqq_alpha.brain.playbook import Lesson, Playbook, append_lesson, save_playbook
from qqq_alpha.config import Settings
from qqq_alpha.memory import Memory

log = logging.getLogger(__name__)

MIN_SAMPLE = 8
MIN_EFFECT_PCT = 25.0  # a bucket must beat/lag the average by this much to count
MIN_TOTAL_TRADES = 20  # below this, the record cannot support any lesson at all


@dataclass
class Finding:
    """One statistically defensible observation about the engine's own record."""

    key: str
    statement: str
    evidence: str
    sample_size: int
    effect_pct: float
    confidence: float
    direction: str  # "favourable" | "unfavourable"


@dataclass
class LearningReport:
    total_trades: int = 0
    baseline_return: float = 0.0
    findings: list[Finding] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def has_findings(self) -> bool:
        return bool(self.findings)


def _confidence(sample: int, effect: float) -> float:
    """Crude but honest: more trades and a bigger effect mean more confidence.

    Deliberately capped below 1.0. No amount of intraday options data from one
    engine over a few months justifies certainty.
    """
    sample_factor = min(sample / 40.0, 1.0)
    effect_factor = min(abs(effect) / 80.0, 1.0)
    return round(min(0.3 + 0.45 * sample_factor + 0.25 * effect_factor, 0.9), 2)


def analyse(memory: Memory, since: date | None = None) -> LearningReport:
    """Find what the trade record actually supports. No AI involved — just maths."""
    trades = memory.closed_trades(since=since)
    report = LearningReport(total_trades=len(trades))

    if len(trades) < MIN_TOTAL_TRADES:
        report.notes.append(
            f"only {len(trades)} closed trades on record; "
            f"{MIN_TOTAL_TRADES} is the minimum before any pattern can be trusted"
        )
        return report

    returns = [t["return_pct"] for t in trades if t["return_pct"] is not None]
    baseline = round(mean(returns), 1)
    report.baseline_return = baseline

    def _consider(
        key: str,
        label: str,
        rows: list[dict],
        template_good: str,
        template_bad: str,
    ) -> None:
        for row in rows:
            if row["trades"] < MIN_SAMPLE:
                continue
            effect = round(row["avg_return"] - baseline, 1)
            if abs(effect) < MIN_EFFECT_PCT:
                continue

            favourable = effect > 0
            template = template_good if favourable else template_bad
            statement = template.format(bucket=row["bucket"], effect=abs(effect))
            evidence = (
                f"{row['trades']} trades in {label}={row['bucket']}: "
                f"average {row['avg_return']:+.1f}% vs {baseline:+.1f}% overall, "
                f"win rate {row['win_rate']:.0f}%, "
                f"best {row['best']:+.0f}% / worst {row['worst']:+.0f}%"
            )
            report.findings.append(
                Finding(
                    key=f"{key}:{row['bucket']}",
                    statement=statement,
                    evidence=evidence,
                    sample_size=row["trades"],
                    effect_pct=effect,
                    confidence=_confidence(row["trades"], effect),
                    direction="favourable" if favourable else "unfavourable",
                )
            )

    _consider(
        "regime",
        "regime",
        memory.performance_by("regime", MIN_SAMPLE),
        "The {bucket} regime has been this engine's strongest, outperforming its own "
        "average by {effect:.0f} points. Weight setups in this regime more heavily.",
        "The {bucket} regime has cost this engine {effect:.0f} points below its average. "
        "Demand a clearer edge before entering in it.",
    )

    _consider(
        "direction",
        "direction",
        memory.performance_by("direction", MIN_SAMPLE),
        "{bucket} trades have outperformed by {effect:.0f} points.",
        "{bucket} trades have underperformed by {effect:.0f} points — check whether the "
        "entry criteria are symmetric or quietly biased.",
    )

    _consider(
        "alignment",
        "timeframe_aligned",
        memory.performance_by("timeframe_aligned", MIN_SAMPLE),
        "Trades taken with all timeframes aligned (flag={bucket}) beat the average by "
        "{effect:.0f} points.",
        "Trades taken with timeframes in conflict (flag={bucket}) lag the average by "
        "{effect:.0f} points.",
    )

    # --- is stated confidence worth anything at all? ---
    by_confidence = memory.performance_by("confidence", MIN_SAMPLE)
    if len(by_confidence) >= 2:
        ordered = sorted(by_confidence, key=lambda r: r["bucket"])
        low, high = ordered[0], ordered[-1]
        gap = round(high["avg_return"] - low["avg_return"], 1)
        if gap <= 0:
            report.findings.append(
                Finding(
                    key="calibration",
                    statement=(
                        f"Stated confidence is currently uninformative: "
                        f"confidence {high['bucket']} averages {high['avg_return']:+.1f}% "
                        f"while confidence {low['bucket']} averages {low['avg_return']:+.1f}%. "
                        "Treat your own confidence score with suspicion until it separates."
                    ),
                    evidence=f"{high['trades']} high-confidence vs {low['trades']} low-confidence trades",
                    sample_size=high["trades"] + low["trades"],
                    effect_pct=gap,
                    confidence=_confidence(high["trades"] + low["trades"], gap),
                    direction="unfavourable",
                )
            )

    # --- which exits are doing the damage? ---
    for row in memory.performance_by("exit_reason", MIN_SAMPLE):
        if row["bucket"] == "time_stop" and row["avg_return"] < baseline - MIN_EFFECT_PCT:
            report.findings.append(
                Finding(
                    key="exit:time_stop",
                    statement=(
                        "Trades exited on the time stop average "
                        f"{row['avg_return']:+.1f}% — theses that need longer than expected "
                        "are not slow, they are wrong. Consider cutting them sooner."
                    ),
                    evidence=f"{row['trades']} time-stopped trades, win rate {row['win_rate']:.0f}%",
                    sample_size=row["trades"],
                    effect_pct=round(row["avg_return"] - baseline, 1),
                    confidence=_confidence(row["trades"], row["avg_return"] - baseline),
                    direction="unfavourable",
                )
            )

    hours = memory.performance_by_hour(MIN_SAMPLE)
    if hours:
        best = max(hours, key=lambda r: r["avg_return"])
        worst = min(hours, key=lambda r: r["avg_return"])
        if best["avg_return"] - worst["avg_return"] >= MIN_EFFECT_PCT * 2:
            report.findings.append(
                Finding(
                    key="hours",
                    statement=(
                        f"Time of day matters: {best['session_hour']} averages "
                        f"{best['avg_return']:+.1f}% while {worst['session_hour']} averages "
                        f"{worst['avg_return']:+.1f}%."
                    ),
                    evidence=f"{best['trades']} vs {worst['trades']} trades respectively",
                    sample_size=best["trades"] + worst["trades"],
                    effect_pct=round(best["avg_return"] - worst["avg_return"], 1),
                    confidence=_confidence(
                        best["trades"] + worst["trades"],
                        best["avg_return"] - worst["avg_return"],
                    ),
                    direction="favourable",
                )
            )

    if not report.findings:
        report.notes.append(
            "no pattern in the record is strong enough to justify changing the playbook — "
            "which is itself a finding, and a common one early on"
        )

    return report


def propose(memory: Memory, report: LearningReport) -> list[int]:
    """Store findings as pending lessons awaiting approval."""
    ids: list[int] = []
    existing = {row["statement"] for row in memory.pending_lessons()}

    for finding in report.findings:
        if finding.statement in existing:
            continue
        ids.append(
            memory.save_lesson(
                statement=finding.statement,
                evidence=finding.evidence,
                sample_size=finding.sample_size,
                confidence=finding.confidence,
            )
        )
    return ids


def apply_lesson(
    memory: Memory, playbook: Playbook, lesson_id: int, settings: Settings
) -> Playbook:
    """Promote an approved lesson into the playbook and bump its version."""
    pending = {row["id"]: row for row in memory.pending_lessons()}
    row = pending.get(lesson_id)
    if row is None:
        raise ValueError(f"lesson {lesson_id} is not pending")

    lesson = Lesson(
        id=f"L{lesson_id:03d}",
        learned_on=datetime.fromisoformat(row["created_at"]).date(),
        statement=row["statement"],
        evidence=row["evidence"],
        sample_size=row["sample_size"],
        confidence=row["confidence"],
    )

    updated = append_lesson(playbook, lesson)
    updated.updated = datetime.now().date().isoformat()
    save_playbook(updated, settings.playbook_path)
    memory.set_lesson_status(lesson_id, "applied")
    log.info("playbook advanced to v%s with lesson %s", updated.version, lesson.id)
    return updated
