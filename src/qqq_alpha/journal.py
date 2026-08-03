"""Append-only journal.

Records three things, and the third is the one most systems forget:

1. Trades taken, with the full snapshot that justified them.
2. Decisions not to trade, with the reasoning.
3. Moments the engine never even looked at, with the attention score.

(2) and (3) are what let the weekly review answer "what is our caution costing
us?" instead of only "how did our trades do?". A system that only journals its
winners and losers can never discover that it is too timid.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from qqq_alpha.domain import Decision, MarketSnapshot, MissedOpportunity, Trade


class Journal:
    def __init__(self, directory: Path, session_tag: str | None = None):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        tag = session_tag or datetime.now().strftime("%Y%m%d-%H%M%S")
        self.decisions_path = directory / f"decisions-{tag}.jsonl"
        self.trades_path = directory / f"trades-{tag}.jsonl"
        self.attention_path = directory / f"attention-{tag}.jsonl"
        self.missed_path = directory / f"missed-{tag}.jsonl"

    # ------------------------------------------------------------------
    @staticmethod
    def _append(path: Path, record: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def log_decision(
        self,
        decision: Decision,
        snapshot: MarketSnapshot,
        rail_blocks: list[str] | None = None,
        rail_warnings: list[str] | None = None,
        attention_score: float | None = None,
    ) -> None:
        self._append(
            self.decisions_path,
            {
                "ts": decision.ts,
                "action": decision.action.value,
                "direction": decision.direction.value if decision.direction else None,
                "occ_symbol": decision.occ_symbol,
                "confidence": decision.confidence,
                "thesis": decision.thesis,
                "risks": decision.risks,
                "playbook_refs": decision.playbook_refs,
                "overrides": decision.overrides,
                "invalidation": decision.invalidation,
                "targets": [t.model_dump(mode="json") for t in decision.targets],
                "stop_return_pct": decision.stop_return_pct,
                "rail_blocks": rail_blocks or [],
                "rail_warnings": rail_warnings or [],
                "attention_score": attention_score,
                "snapshot": _snapshot_digest(snapshot),
            },
        )

    def log_attention(
        self, ts: datetime, score: float, woke: bool, summary: str, suppressed_by: str | None
    ) -> None:
        self._append(
            self.attention_path,
            {
                "ts": ts,
                "score": score,
                "woke_brain": woke,
                "signals": summary,
                "suppressed_by": suppressed_by,
            },
        )

    def log_trade(self, trade: Trade) -> None:
        record = trade.model_dump(mode="json")
        record["snapshot_at_entry"] = (
            _snapshot_digest(trade.snapshot_at_entry) if trade.snapshot_at_entry else None
        )
        self._append(self.trades_path, record)

    def log_missed(self, missed: MissedOpportunity) -> None:
        self._append(self.missed_path, missed.model_dump(mode="json"))

    # ------------------------------------------------------------------
    def read(self, path: Path) -> Iterator[dict[str, Any]]:
        if not path.exists():
            return
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    yield json.loads(line)


def _snapshot_digest(snapshot: MarketSnapshot | None) -> dict[str, Any] | None:
    """Store enough of the snapshot to re-derive the decision, not the whole world."""
    if snapshot is None:
        return None
    return {
        "ts": snapshot.ts,
        "session_minute": snapshot.session_minute,
        "price": snapshot.underlying.close,
        "regime": snapshot.regime.value,
        "net_bias": snapshot.net_bias,
        "indicators": snapshot.indicators,
        "levels": snapshot.levels,
        "flow": snapshot.flow.model_dump(mode="json") if snapshot.flow else None,
        "observations": [
            {"name": o.name, "value": o.value, "score": o.score, "confidence": o.confidence}
            for o in snapshot.observations
        ],
        "events": snapshot.events,
    }
