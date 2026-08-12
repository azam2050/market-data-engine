"""Long-term memory.

The journal records what happened. This makes it *usable*.

Two different kinds of recall matter to a trader, and the engine needs both:

* **Recent** — what have I been doing lately, and how is it going? Guards against
  repeating a mistake made twenty minutes ago.
* **Similar** — the market looked like this before; what happened those times?
  This is the recall that actually compounds. A human trader builds it over
  years and still remembers selectively. The engine builds it from the first
  trade and never flatters itself.

SQLite, deliberately. One engine, one file, no server to operate, atomic writes,
and it survives every restart. A database server would add an operational
failure mode without adding capability at this scale.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import closing, suppress
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import Decision, MarketSnapshot, MissedOpportunity, Trade

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    trade_id            TEXT PRIMARY KEY,
    session_day         TEXT NOT NULL,
    opened_at           TEXT NOT NULL,
    closed_at           TEXT,
    occ_symbol          TEXT,
    direction           TEXT,
    entry_price         REAL,
    exit_price          REAL,
    return_pct          REAL,
    max_favorable_pct   REAL,
    max_adverse_pct     REAL,
    hold_minutes        REAL,
    exit_reason         TEXT,
    confidence          INTEGER,
    thesis              TEXT,
    playbook_refs       TEXT,
    overrides           TEXT,
    -- market fingerprint at the moment of entry
    regime              TEXT,
    session_minute      INTEGER,
    net_bias            REAL,
    vwap_dev            REAL,
    rel_volume          REAL,
    rsi                 REAL,
    atr_pct             REAL,
    flow_urgency        REAL,
    timeframe_aligned   INTEGER,
    features            TEXT
);

CREATE INDEX IF NOT EXISTS idx_trades_day    ON trades(session_day);
CREATE INDEX IF NOT EXISTS idx_trades_regime ON trades(regime);
CREATE INDEX IF NOT EXISTS idx_trades_closed ON trades(closed_at);

CREATE TABLE IF NOT EXISTS decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL,
    session_day     TEXT NOT NULL,
    action          TEXT NOT NULL,
    confidence      INTEGER,
    regime          TEXT,
    session_minute  INTEGER,
    net_bias        REAL,
    thesis          TEXT,
    playbook_refs   TEXT,
    overrides       TEXT,
    attention_score REAL,
    blocked_by      TEXT
);

CREATE INDEX IF NOT EXISTS idx_decisions_day ON decisions(session_day);

CREATE TABLE IF NOT EXISTS lessons (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL,
    statement   TEXT NOT NULL,
    evidence    TEXT,
    sample_size INTEGER,
    confidence  REAL,
    status      TEXT NOT NULL DEFAULT 'pending',
    -- the finding key (e.g. "missed:regime:TRENDING_UP") — statements carry
    -- live numbers that drift as data accumulates, so text comparison cannot
    -- recognise "the same lesson again"; the key can
    key         TEXT
);

-- trial subscribers: anyone who /started the bot. Durable, because cutting a
-- paying-funnel trial short (or extending it forever) on every redeploy is
-- exactly the kind of silent bug the volume outage already taught us about.
CREATE TABLE IF NOT EXISTS subscribers (
    chat_id     TEXT PRIMARY KEY,
    username    TEXT,
    first_name  TEXT,
    joined_at   TEXT NOT NULL,
    expires_at  TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'trial'
);

-- setups the engine declined (rail-blocked or the AI's own PASS), priced
-- forward after the fact. Without this table the engine can only grade its
-- winners and losers, never its caution.
CREATE TABLE IF NOT EXISTS missed_opportunities (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                  TEXT NOT NULL,
    session_day         TEXT NOT NULL,
    reason              TEXT,
    would_be_direction  TEXT,
    occ_symbol          TEXT,
    hypothetical_entry  REAL,
    best_price_after    REAL,
    peak_return_pct     REAL,
    blocked_by          TEXT,
    regime              TEXT,
    session_minute      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_missed_day    ON missed_opportunities(session_day);
CREATE INDEX IF NOT EXISTS idx_missed_regime ON missed_opportunities(regime);
"""

# how much each dimension counts when judging "did the market look like this?"
SIMILARITY_WEIGHTS: dict[str, float] = {
    "net_bias": 1.0,
    "vwap_dev": 0.9,
    "rel_volume": 0.7,
    "rsi": 0.5,
    "atr_pct": 0.6,
    "flow_urgency": 0.8,
    "session_minute": 0.4,
}

# rough spread of each feature, used to put them on a comparable scale
SIMILARITY_SCALE: dict[str, float] = {
    "net_bias": 0.5,
    "vwap_dev": 0.35,
    "rel_volume": 1.0,
    "rsi": 20.0,
    "atr_pct": 0.05,
    "flow_urgency": 0.4,
    "session_minute": 120.0,
}


@dataclass
class RecalledTrade:
    trade_id: str
    opened_at: str
    direction: str | None
    return_pct: float | None
    max_favorable_pct: float | None
    confidence: int | None
    regime: str | None
    thesis: str
    exit_reason: str
    distance: float = 0.0

    def as_prompt_row(self) -> dict[str, Any]:
        return {
            "when": self.opened_at,
            "direction": self.direction,
            "result_pct": self.return_pct,
            "peak_pct": self.max_favorable_pct,
            "confidence_was": self.confidence,
            "regime": self.regime,
            "exit": self.exit_reason,
            "thesis": self.thesis[:180],
        }


def _fingerprint(snapshot: MarketSnapshot | None) -> dict[str, Any]:
    """The numbers that describe what the market looked like, for later recall."""
    if snapshot is None:
        return {}

    indicators = snapshot.indicators or {}
    price = indicators.get("price") or snapshot.underlying.close
    atr = indicators.get("atr14")
    aligned = next(
        (o for o in snapshot.observations if o.name == "timeframe_alignment"), None
    )

    return {
        "regime": snapshot.regime.value,
        "session_minute": snapshot.session_minute,
        "net_bias": snapshot.net_bias,
        "vwap_dev": indicators.get("vwap_dev_pct"),
        "rel_volume": indicators.get("rel_volume"),
        "rsi": indicators.get("rsi14"),
        "atr_pct": round(atr / price * 100, 4) if atr and price else None,
        "flow_urgency": snapshot.flow.urgency if snapshot.flow else None,
        "timeframe_aligned": (
            1 if aligned is not None and aligned.confidence >= 0.9 else 0
        ),
        "features": json.dumps(
            {
                "indicators": indicators,
                "timeframes": snapshot.timeframes,
                "observations": [
                    {"name": o.name, "value": o.value, "score": o.score}
                    for o in snapshot.observations
                ],
            },
            default=str,
        ),
    }


class Memory:
    """Durable trade memory. Safe to open from several processes."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)
            self._migrate(conn)
            self._purge_infeasible_missed(conn)
            conn.commit()

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Bring an existing database up to the current schema. Idempotent."""
        with suppress(sqlite3.OperationalError):  # column already exists
            conn.execute("ALTER TABLE lessons ADD COLUMN key TEXT")

    @staticmethod
    def _purge_infeasible_missed(conn: sqlite3.Connection) -> None:
        """Remove "missed opportunities" that were never opportunities.

        Early builds recorded rail declines even when the block meant the trade
        was impossible (market closed, broken data). Those rows fed the learning
        loop a bucket of pre-open "misses" and got it to propose loosening entry
        confidence over trades nobody could have taken. Idempotent, runs on
        every open, and the writers no longer produce such rows.
        """
        from qqq_alpha.brain.rails import INFEASIBLE_BLOCK_PREFIXES

        for prefix in INFEASIBLE_BLOCK_PREFIXES:
            removed = conn.execute(
                "DELETE FROM missed_opportunities WHERE blocked_by LIKE ?",
                (f'%"{prefix}%',),
            ).rowcount
            if removed:
                log.info("purged %d infeasible '%s' missed rows", removed, prefix)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        # survive an unclean shutdown without corrupting the file
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # ------------------------------------------------------------------
    def remember_trade(self, trade: Trade, snapshot: MarketSnapshot | None = None) -> None:
        """Upsert a trade. Called on entry and again on every update."""
        snapshot = snapshot or trade.snapshot_at_entry
        finger = _fingerprint(snapshot)

        hold = None
        if trade.closed_at and trade.opened_at:
            hold = round((trade.closed_at - trade.opened_at).total_seconds() / 60, 1)

        row = {
            "trade_id": trade.trade_id,
            "session_day": trade.opened_at.astimezone(MARKET_TZ).date().isoformat(),
            "opened_at": trade.opened_at.isoformat(),
            "closed_at": trade.closed_at.isoformat() if trade.closed_at else None,
            "occ_symbol": trade.occ_symbol,
            "direction": trade.decision.direction.value if trade.decision.direction else None,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "return_pct": trade.return_pct,
            "max_favorable_pct": trade.max_favorable_pct,
            "max_adverse_pct": trade.max_adverse_pct,
            "hold_minutes": hold,
            "exit_reason": trade.exit_reason,
            "confidence": trade.decision.confidence,
            "thesis": trade.decision.thesis,
            "playbook_refs": json.dumps(trade.decision.playbook_refs, ensure_ascii=False),
            "overrides": json.dumps(trade.decision.overrides, ensure_ascii=False),
            **{k: finger.get(k) for k in
               ("regime", "session_minute", "net_bias", "vwap_dev", "rel_volume",
                "rsi", "atr_pct", "flow_urgency", "timeframe_aligned", "features")},
        }

        columns = ", ".join(row)
        placeholders = ", ".join(f":{c}" for c in row)
        with closing(self._connect()) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO trades ({columns}) VALUES ({placeholders})", row
            )
            conn.commit()

    def remember_decision(
        self,
        decision: Decision,
        snapshot: MarketSnapshot,
        attention_score: float | None = None,
        blocked_by: list[str] | None = None,
    ) -> None:
        """Record decisions not to trade too — the passes are half the record."""
        with closing(self._connect()) as conn:
            conn.execute(
                """INSERT INTO decisions
                   (ts, session_day, action, confidence, regime, session_minute,
                    net_bias, thesis, playbook_refs, overrides, attention_score, blocked_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    decision.ts.isoformat(),
                    decision.ts.astimezone(MARKET_TZ).date().isoformat(),
                    decision.action.value,
                    decision.confidence,
                    snapshot.regime.value,
                    snapshot.session_minute,
                    snapshot.net_bias,
                    decision.thesis,
                    json.dumps(decision.playbook_refs, ensure_ascii=False),
                    json.dumps(decision.overrides, ensure_ascii=False),
                    attention_score,
                    json.dumps(blocked_by or [], ensure_ascii=False),
                ),
            )
            conn.commit()

    def remember_missed(self, missed: MissedOpportunity) -> None:
        """Record a declined setup that would have paid, priced forward.

        Covers both reasons a trade never happened: the rails blocked it, or
        the AI itself looked and passed. Both matter to the same question —
        is the engine's caution earning its keep?
        """
        with closing(self._connect()) as conn:
            conn.execute(
                """INSERT INTO missed_opportunities
                   (ts, session_day, reason, would_be_direction, occ_symbol,
                    hypothetical_entry, best_price_after, peak_return_pct,
                    blocked_by, regime, session_minute)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    missed.ts.isoformat(),
                    missed.ts.astimezone(MARKET_TZ).date().isoformat(),
                    missed.reason,
                    missed.would_be_direction.value,
                    missed.occ_symbol,
                    missed.hypothetical_entry,
                    missed.best_price_after,
                    missed.peak_return_pct,
                    json.dumps(missed.blocked_by, ensure_ascii=False),
                    missed.regime,
                    missed.session_minute,
                ),
            )
            conn.commit()

    def missed_performance_by(self, column: str, min_sample: int = 3) -> list[dict[str, Any]]:
        """Aggregate missed-opportunity cost grouped by regime or direction."""
        if column not in {"regime", "would_be_direction", "reason"}:
            raise ValueError(f"cannot group missed opportunities by {column}")

        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"""SELECT {column} AS bucket,
                           COUNT(*)             AS count,
                           AVG(peak_return_pct) AS avg_peak,
                           MAX(peak_return_pct) AS best,
                           MIN(peak_return_pct) AS worst
                    FROM missed_opportunities
                    WHERE {column} IS NOT NULL
                    GROUP BY bucket
                    HAVING count >= ?
                    ORDER BY avg_peak DESC""",
                (min_sample,),
            ).fetchall()

        return [
            {
                "bucket": row["bucket"],
                "count": row["count"],
                "avg_peak": round(row["avg_peak"], 1),
                "best": round(row["best"], 1),
                "worst": round(row["worst"], 1),
            }
            for row in rows
        ]

    def missed_count(self) -> int:
        with closing(self._connect()) as conn:
            return conn.execute("SELECT COUNT(*) FROM missed_opportunities").fetchone()[0]

    # ------------------------------------------------------------------
    def recent_trades(self, limit: int = 15, closed_only: bool = True) -> list[RecalledTrade]:
        clause = "WHERE closed_at IS NOT NULL" if closed_only else ""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT * FROM trades {clause} ORDER BY opened_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_to_recalled(row) for row in rows]

    def similar_trades(
        self, snapshot: MarketSnapshot, limit: int = 8, direction: str | None = None
    ) -> list[RecalledTrade]:
        """Past trades taken when the market looked like it does right now.

        Regime is used as a hard filter because a setup that works in a trend
        genuinely does not transfer to chop. Within the regime, trades are ranked
        by weighted distance across the fingerprint features. Features missing on
        either side are skipped rather than guessed — a partial match is honest,
        an imputed one is not.
        """
        target = _fingerprint(snapshot)
        if not target:
            return []

        query = "SELECT * FROM trades WHERE closed_at IS NOT NULL AND regime = ?"
        params: list[Any] = [target["regime"]]
        if direction:
            query += " AND direction = ?"
            params.append(direction)

        with closing(self._connect()) as conn:
            rows = conn.execute(query, params).fetchall()

        scored: list[RecalledTrade] = []
        for row in rows:
            distance, compared = 0.0, 0.0
            for feature, weight in SIMILARITY_WEIGHTS.items():
                mine, theirs = target.get(feature), row[feature]
                if mine is None or theirs is None:
                    continue
                scale = SIMILARITY_SCALE[feature]
                distance += weight * abs(float(mine) - float(theirs)) / scale
                compared += weight
            if compared == 0:
                continue

            recalled = _to_recalled(row)
            recalled.distance = round(distance / compared, 4)
            scored.append(recalled)

        scored.sort(key=lambda t: t.distance)
        return scored[:limit]

    # ------------------------------------------------------------------
    def performance_by(self, column: str, min_sample: int = 3) -> list[dict[str, Any]]:
        """Aggregate closed-trade performance grouped by any fingerprint column."""
        if column not in {
            "regime", "direction", "exit_reason", "confidence", "timeframe_aligned"
        }:
            raise ValueError(f"cannot group by {column}")

        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"""SELECT {column} AS bucket,
                           COUNT(*)          AS trades,
                           AVG(return_pct)   AS avg_return,
                           SUM(CASE WHEN return_pct > 0 THEN 1 ELSE 0 END) AS wins,
                           MAX(return_pct)   AS best,
                           MIN(return_pct)   AS worst
                    FROM trades
                    WHERE closed_at IS NOT NULL AND {column} IS NOT NULL
                    GROUP BY bucket
                    HAVING trades >= ?
                    ORDER BY avg_return DESC""",
                (min_sample,),
            ).fetchall()

        return [
            {
                "bucket": row["bucket"],
                "trades": row["trades"],
                "avg_return": round(row["avg_return"], 1),
                "win_rate": round(row["wins"] / row["trades"] * 100, 1),
                "best": round(row["best"], 1),
                "worst": round(row["worst"], 1),
            }
            for row in rows
        ]

    def performance_by_hour(self, min_sample: int = 3) -> list[dict[str, Any]]:
        """Which hours actually pay — the question asked from day one."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """SELECT session_minute / 60 AS hour_bucket,
                          COUNT(*)        AS trades,
                          AVG(return_pct) AS avg_return,
                          SUM(CASE WHEN return_pct > 0 THEN 1 ELSE 0 END) AS wins
                   FROM trades
                   WHERE closed_at IS NOT NULL AND session_minute IS NOT NULL
                   GROUP BY hour_bucket
                   HAVING trades >= ?
                   ORDER BY hour_bucket""",
                (min_sample,),
            ).fetchall()

        return [
            {
                # session minute 0 is 09:30 ET
                "session_hour": f"{9 + (row['hour_bucket'] or 0)}:30-{10 + (row['hour_bucket'] or 0)}:30 ET",
                "trades": row["trades"],
                "avg_return": round(row["avg_return"], 1),
                "win_rate": round(row["wins"] / row["trades"] * 100, 1),
            }
            for row in rows
        ]

    def counts(self) -> dict[str, int]:
        with closing(self._connect()) as conn:
            trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            closed = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE closed_at IS NOT NULL"
            ).fetchone()[0]
            decisions = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
            days = conn.execute("SELECT COUNT(DISTINCT session_day) FROM trades").fetchone()[0]
            missed = conn.execute("SELECT COUNT(*) FROM missed_opportunities").fetchone()[0]
        return {
            "trades": trades,
            "closed": closed,
            "decisions": decisions,
            "session_days": days,
            "missed": missed,
        }

    def sessions(self) -> list[date]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT DISTINCT session_day FROM trades ORDER BY session_day"
            ).fetchall()
        return [date.fromisoformat(row[0]) for row in rows]

    def closed_trades(self, since: date | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM trades WHERE closed_at IS NOT NULL"
        params: list[Any] = []
        if since:
            query += " AND session_day >= ?"
            params.append(since.isoformat())
        query += " ORDER BY opened_at"

        with closing(self._connect()) as conn:
            return [dict(row) for row in conn.execute(query, params).fetchall()]

    # ------------------------------------------------------------------
    def save_lesson(
        self,
        statement: str,
        evidence: str,
        sample_size: int,
        confidence: float,
        key: str = "",
    ) -> int:
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                """INSERT INTO lessons
                   (created_at, statement, evidence, sample_size, confidence, key)
                   VALUES (?,?,?,?,?,?)""",
                (
                    datetime.now().isoformat(),
                    statement,
                    evidence,
                    sample_size,
                    confidence,
                    key,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid or 0)

    def pending_lessons(self) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM lessons WHERE status = 'pending' ORDER BY id"
            ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    def add_subscriber(
        self,
        chat_id: str,
        username: str,
        first_name: str,
        joined_at: datetime,
        expires_at: datetime,
    ) -> bool:
        """Register a new trial. Returns False if the chat already signed up —
        re-/starting the bot must never reset someone's trial clock."""
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO subscribers
                   (chat_id, username, first_name, joined_at, expires_at)
                   VALUES (?,?,?,?,?)""",
                (
                    str(chat_id),
                    username,
                    first_name,
                    joined_at.isoformat(),
                    expires_at.isoformat(),
                ),
            )
            conn.commit()
            return cursor.rowcount > 0

    def subscriber(self, chat_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM subscribers WHERE chat_id = ?", (str(chat_id),)
            ).fetchone()
        return dict(row) if row else None

    def active_subscriber_ids(self, now: datetime) -> list[str]:
        """Everyone whose trial is still running — the broadcast list."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT chat_id FROM subscribers WHERE status = 'trial' AND expires_at > ?",
                (now.isoformat(),),
            ).fetchall()
        return [row["chat_id"] for row in rows]

    def expire_due_subscribers(self, now: datetime) -> list[dict[str, Any]]:
        """Flip finished trials to expired and return them for the farewell."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM subscribers WHERE status = 'trial' AND expires_at <= ?",
                (now.isoformat(),),
            ).fetchall()
            due = [dict(row) for row in rows]
            if due:
                conn.execute(
                    "UPDATE subscribers SET status = 'expired' "
                    "WHERE status = 'trial' AND expires_at <= ?",
                    (now.isoformat(),),
                )
                conn.commit()
        return due

    def subscriber_counts(self) -> dict[str, int]:
        with closing(self._connect()) as conn:
            trial = conn.execute(
                "SELECT COUNT(*) FROM subscribers WHERE status = 'trial'"
            ).fetchone()[0]
            expired = conn.execute(
                "SELECT COUNT(*) FROM subscribers WHERE status = 'expired'"
            ).fetchone()[0]
        return {"trial": trial, "expired": expired}

    # ------------------------------------------------------------------
    def applied_lessons(self) -> list[dict[str, Any]]:
        """Operator-approved lessons — the durable half of the live playbook."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM lessons WHERE status = 'applied' ORDER BY id"
            ).fetchall()
        return [dict(row) for row in rows]

    def lesson_keys(self) -> set[str]:
        """Every finding key ever proposed, regardless of what became of it.

        An approved lesson is already in the playbook, and a rejected one was
        turned down by the operator — re-proposing either every morning because
        the underlying numbers drifted is nagging, not learning.
        """
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT DISTINCT key FROM lessons WHERE key IS NOT NULL AND key != ''"
            ).fetchall()
        return {row["key"] for row in rows}

    def set_lesson_status(self, lesson_id: int, status: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute("UPDATE lessons SET status = ? WHERE id = ?", (status, lesson_id))
            conn.commit()


def _to_recalled(row: sqlite3.Row) -> RecalledTrade:
    return RecalledTrade(
        trade_id=row["trade_id"],
        opened_at=row["opened_at"],
        direction=row["direction"],
        return_pct=row["return_pct"],
        max_favorable_pct=row["max_favorable_pct"],
        confidence=row["confidence"],
        regime=row["regime"],
        thesis=row["thesis"] or "",
        exit_reason=row["exit_reason"] or "",
    )
