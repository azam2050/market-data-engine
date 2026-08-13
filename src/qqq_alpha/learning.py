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
from datetime import UTC, date, datetime, timedelta
from statistics import mean

from qqq_alpha.brain.playbook import Lesson, Playbook, append_lesson
from qqq_alpha.config import Settings
from qqq_alpha.memory import Memory

log = logging.getLogger(__name__)

MIN_SAMPLE = 8
MIN_EFFECT_PCT = 25.0  # a bucket must beat/lag the average by this much to count
MIN_TOTAL_TRADES = 20  # below this, the record cannot support any lesson at all

# --- governance of missed-opportunity lessons, learned the hard way ---
# a "lower your bar" lesson trained on a dozen declines from two days produced
# behaviour that oscillated daily; these gates make the evidence earn the change
MIN_MISSED_SAMPLE = 25  # declines needed before caution itself goes on trial
MISSED_CUTOFF_MINUTE = 330  # ignore declines after 15:00 ET — those passes are policy
MIN_SPAN_DAYS = 7  # the sample must cover at least two calendar weeks...
MIN_DISTINCT_DAYS = 4  # ...and at least this many separate sessions
COOLING_DAYS = 7  # one behaviour change at a time: no proposals right after one


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


def _consider_missed(memory: Memory, settings: Settings) -> list[Finding]:
    """Is the engine's caution earning its keep, or leaving money on the table?

    A declined setup — blocked by the rails, or the AI's own PASS — is graded
    against the same bar a taken trade must clear: the configured minimum
    target return. A regime where declines routinely would have cleared it by
    a wide margin is a regime where the engine is saying no too readily.
    """
    findings: list[Finding] = []
    rows = memory.missed_performance_by(
        "regime", MIN_MISSED_SAMPLE, max_session_minute=MISSED_CUTOFF_MINUTE
    )
    for row in rows:
        effect = round(row["avg_peak"] - settings.min_target_return_pct, 1)
        if effect < MIN_EFFECT_PCT:
            continue
        # the sample must span real market variety, not one bad week's mood:
        # a lesson built on two consecutive sessions describes those sessions,
        # not the engine
        span_ok = False
        if row.get("first_day") and row.get("last_day"):
            span = (date.fromisoformat(row["last_day"]) - date.fromisoformat(row["first_day"])).days
            span_ok = span >= MIN_SPAN_DAYS and (row.get("distinct_days") or 0) >= MIN_DISTINCT_DAYS
        if not span_ok:
            continue
        findings.append(
            Finding(
                key=f"missed:regime:{row['bucket']}",
                statement=(
                    f"In the {row['bucket']} regime, {row['count']} declined setups would "
                    f"have cleared the target by an average of {effect:.0f} extra points if "
                    "taken. Caution here is costing real opportunities — consider a lower "
                    "bar for entry confidence in this regime."
                ),
                evidence=(
                    f"{row['count']} declined setups, avg peak {row['avg_peak']:+.1f}% vs a "
                    f"{settings.min_target_return_pct:+.0f}% target, best {row['best']:+.1f}%"
                ),
                sample_size=row["count"],
                effect_pct=effect,
                confidence=_confidence(row["count"], effect),
                direction="unfavourable",  # unfavourable to the current caution level
            )
        )
    return findings


def analyse(
    memory: Memory, since: date | None = None, settings: Settings | None = None
) -> LearningReport:
    """Find what the trade record actually supports. No AI involved — just maths."""
    settings = settings or Settings()
    trades = memory.closed_trades(since=since)
    report = LearningReport(total_trades=len(trades))

    # independent of the closed-trade count below: a handful of declined
    # setups that clearly would have paid is its own kind of evidence, and
    # waiting for 20 taken trades to say so would waste it
    report.findings.extend(_consider_missed(memory, settings))

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

    # --- which exits are doing the damage? every reason, not just the time stop ---
    _consider(
        "exit",
        "exit_reason",
        memory.performance_by("exit_reason", MIN_SAMPLE),
        "Trades exited via {bucket} have outperformed by {effect:.0f} points — whatever "
        "triggers this exit is catching genuine strength. Let it run further before this "
        "point where the setup allows.",
        "Trades exited via {bucket} have underperformed by {effect:.0f} points — theses "
        "that end this way are not unlucky, they are wrong going in. Consider tightening "
        "entry criteria that lead here, or cutting these positions sooner.",
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
    """Store findings as pending lessons awaiting approval.

    Deduplicated by finding key against everything ever proposed — pending,
    applied or rejected. The statements carry live numbers that drift as data
    accumulates, so an already-approved lesson would otherwise come back the
    very next morning wearing slightly different figures (this happened: L002
    was approved on 2026-08-10 and re-proposed verbatim-in-spirit on 08-11).

    Two pacing rules on top of the dedupe:
    - a cooling period after any applied lesson, so one behaviour change is
      measured before the next is even suggested
    - at most ONE new proposal per run — the strongest finding. A stack of
      simultaneous amendments cannot be attributed when results move.
    """
    applied_at = memory.last_applied_at()
    if applied_at is not None and datetime.now(UTC) - applied_at < timedelta(days=COOLING_DAYS):
        report.notes.append(
            f"a lesson was applied within the last {COOLING_DAYS} days — proposals "
            "are paused so its effect can be measured on its own"
        )
        return []

    pending = memory.pending_lessons()
    if pending:
        report.notes.append(
            "a proposal is already awaiting the operator's ruling — "
            "no new ones until that decision is made"
        )
        return []

    ids: list[int] = []
    seen_keys = memory.lesson_keys()
    seen_statements = {row["statement"] for row in pending}

    candidates = sorted(report.findings, key=lambda f: f.confidence, reverse=True)
    for finding in candidates:
        if finding.key in seen_keys or finding.statement in seen_statements:
            continue
        ids.append(
            memory.save_lesson(
                statement=finding.statement,
                evidence=finding.evidence,
                sample_size=finding.sample_size,
                confidence=finding.confidence,
                key=finding.key,
            )
        )
        break  # one change per measurement period
    return ids


def _lesson_from_row(row: dict) -> Lesson:
    return Lesson(
        id=f"L{row['id']:03d}",
        learned_on=datetime.fromisoformat(row["created_at"]).date(),
        statement=row["statement"],
        evidence=row["evidence"] or "",
        sample_size=row["sample_size"] or 0,
        confidence=row["confidence"] or 0.0,
    )


def with_applied_lessons(playbook: Playbook, memory: Memory) -> Playbook:
    """The seed playbook plus every operator-approved lesson from durable memory.

    Approved lessons used to be written into the playbook *file*, which lives
    on the container's ephemeral filesystem — one redeploy and the operator's
    approval silently vanished. The database is the durable record, so the
    playbook is composed from it on every boot instead: the file in the repo
    stays a seed, and the lessons ride on top.
    """
    updated = playbook
    for row in memory.applied_lessons():
        updated = append_lesson(updated, _lesson_from_row(row))
    return updated


def apply_lesson(
    memory: Memory, playbook: Playbook, lesson_id: int, settings: Settings
) -> Playbook:
    """Promote an approved lesson into the live playbook.

    The durable act is the status flip in memory; the returned playbook is
    recomposed seed+lessons so the caller can swap it in without ever
    double-appending.
    """
    pending = {row["id"]: row for row in memory.pending_lessons()}
    if lesson_id not in pending:
        raise ValueError(f"lesson {lesson_id} is not pending")

    memory.set_lesson_status(lesson_id, "applied")
    from qqq_alpha.brain.playbook import load_playbook

    updated = with_applied_lessons(load_playbook(settings.playbook_path), memory)
    updated.updated = datetime.now().date().isoformat()
    log.info("playbook advanced to v%s with lesson L%03d", updated.version, lesson_id)
    return updated
