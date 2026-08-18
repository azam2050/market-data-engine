"""Assembles one MarketSnapshot: everything the brain sees at a moment in time.

The observations produced here are *evidence*, scored -1..+1. They never reject
a setup. That is the whole point of the design: the previous generation of this
system filtered opportunities away before anything intelligent could look at
them. Here, every reading is passed forward with its own note, and the brain
decides what matters today.
"""

from __future__ import annotations

from datetime import UTC, datetime

from qqq_alpha.config import MARKET_TZ, REGULAR_OPEN
from qqq_alpha.data.quality import DataQuality
from qqq_alpha.domain import (
    Bar,
    FlowEvent,
    MarketRegime,
    MarketSnapshot,
    Observation,
)
from qqq_alpha.features import indicators, levels, structure
from qqq_alpha.features.flow import flow_bias, summarize_flow
from qqq_alpha.features.timeframes import TimeframeSet, hourly

# how much raw tape travels with the snapshot: 30 one-minute candles (the last
# half hour, where a 0DTE entry actually lives) and 12 five-minute candles (the
# last hour of structure). Both are cheap in tokens and are the difference
# between a model that reads price and one that only reads indicators.
RECENT_1M_BARS = 30
RECENT_5M_BARS = 12
# each leader carries a compact five-minute tape of its own: enough to see
# a divergence forming against the index, short enough that three of them
# do not crowd out the index's own price action
LEADER_5M_BARS = 6
# how much of the hourly chart travels with the snapshot. Ten candles is
# roughly a day and a half of hourly structure — the frame a 0DTE trade is
# taken inside, without turning the prompt into a swing-trading brief.
RECENT_1H_BARS = 10


def _session_minute(ts: datetime) -> int:
    local = ts.astimezone(MARKET_TZ)
    open_dt = local.replace(
        hour=REGULAR_OPEN.hour, minute=REGULAR_OPEN.minute, second=0, microsecond=0
    )
    return max(0, int((local - open_dt).total_seconds() // 60))


def _clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return round(max(low, min(high, value)), 3)


class SnapshotBuilder:
    """Stateless builder. Feed it bars, get a snapshot."""

    def __init__(self, primary_symbol: str = "QQQ"):
        self.primary_symbol = primary_symbol

    def build(
        self,
        session_bars: list[Bar],
        leader_bars: dict[str, list[Bar]] | None = None,
        flow_events: list[FlowEvent] | None = None,
        prior_day: Bar | None = None,
        overnight_high: float | None = None,
        overnight_low: float | None = None,
        events: list[str] | None = None,
        now: datetime | None = None,
        quality: DataQuality | None = None,
        leader_prior_close: dict[str, float] | None = None,
        history_bars: list[Bar] | None = None,
    ) -> MarketSnapshot:
        if not session_bars:
            raise ValueError("cannot build a snapshot without bars")

        last = session_bars[-1]
        now = now or last.ts

        # the same session at three resolutions — 15m for trend, 5m for
        # structure, 1m for timing. Rolled up from the provider's minute bars,
        # so every timeframe is arithmetically consistent with the others.
        tfs = TimeframeSet.build(session_bars)
        ind = indicators.compute_all(session_bars)
        timeframe_packs = {
            "1m": ind,
            "5m": indicators.compute_all(tfs.m5) if len(tfs.m5) >= 3 else {},
            "15m": indicators.compute_all(tfs.m15) if len(tfs.m15) >= 3 else {},
        }

        lvl = levels.compute_levels(session_bars, prior_day, overnight_high, overnight_low)
        gap = levels.opening_gap(session_bars, prior_day) or {}

        # the hourly needs the previous sessions to exist at all; without
        # them it is left empty rather than rendered from a stub
        hourly_bars = hourly(list(history_bars or []) + list(session_bars))
        hourly_pack: dict = {}
        if len(hourly_bars) >= 6:
            pack = indicators.compute_all(hourly_bars)
            # two readings do not survive the change of timeframe honestly.
            # rel_volume compares the newest bar to its predecessors, and the
            # newest hourly bar is almost always half-formed — it would read as
            # "volume is dying" when only thirty minutes of it exist. And
            # mom_5m/15m/30m count BARS, not minutes, so on this timeframe they
            # mean five, fifteen and thirty HOURS; left under their intraday
            # names they would be read as something twelve times faster.
            pack.pop("rel_volume", None)
            for minutes in (5, 15, 30):
                value = pack.pop(f"mom_{minutes}m", None)
                if value is not None:
                    pack[f"mom_{minutes}_hours"] = value
            hourly_pack = {
                "indicators": pack,
                "structure": structure.describe(hourly_bars),
                "sessions_covered": len({b.ts.astimezone(MARKET_TZ).date() for b in hourly_bars}),
                "note": "the newest hourly candle is still forming",
            }
        # None means "no tape on this run" and renders as UNAVAILABLE; an empty
        # list means "tape is live but quiet" and renders as zeros — the brain
        # must be able to tell those apart
        flow = summarize_flow(flow_events, now) if flow_events is not None else None

        leaders_last: list[Bar] = []
        leader_alignment = 0.0
        leader_detail: dict[str, dict] = {}
        leader_bars_5m: dict[str, list[Bar]] = {}
        if leader_bars:
            moves: list[float] = []
            for symbol, bars in leader_bars.items():
                if not bars:
                    continue
                leaders_last.append(bars[-1])
                move = indicators.momentum_pct(bars, 15)
                if move is not None:
                    moves.append(move)
                leader_detail[symbol] = self._leader_digest(
                    bars, (leader_prior_close or {}).get(symbol)
                )
                leader_bars_5m[symbol] = TimeframeSet.build(bars).m5[-LEADER_5M_BARS:]
            if moves:
                positive = sum(1 for m in moves if m > 0)
                leader_alignment = (positive / len(moves)) * 2 - 1

        observations = self._observe(session_bars, ind, lvl, flow, leader_alignment)
        observations.extend(self._observe_timeframes(timeframe_packs))
        regime = self._classify_regime(timeframe_packs.get("5m") or ind)

        data_age = max(0.0, (datetime.now(UTC) - last.ts.astimezone(UTC)).total_seconds())

        return MarketSnapshot(
            ts=now,
            session_minute=_session_minute(now),
            underlying=last,
            # the last half hour minute-by-minute, and the last hour in 5m
            # candles — enough tape to read a pattern, short enough that the
            # prompt stays about price action rather than history
            recent_bars_1m=session_bars[-RECENT_1M_BARS:],
            recent_bars_5m=tfs.m5[-RECENT_5M_BARS:],
            recent_bars_1h=hourly_bars[-RECENT_1H_BARS:] if hourly_pack else [],
            hourly=hourly_pack,
            gap=gap,
            # the swing skeleton of the day, on the two timeframes a
            # 0DTE trade is actually framed by
            structure={
                "5m": structure.describe(tfs.m5),
                "15m": structure.describe(tfs.m15),
            },
            leaders=leaders_last,
            leader_detail=leader_detail,
            leader_bars_5m=leader_bars_5m,
            indicators=ind,
            timeframes=timeframe_packs,
            levels=lvl,
            flow=flow,
            regime=regime,
            observations=observations,
            events=events or [],
            data_age_sec=round(data_age, 1),
            data_quality=quality.summary() if quality else "",
            data_usable=quality.is_usable if quality else True,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _leader_digest(bars: list[Bar], prior_close: float | None) -> dict:
        """One heavyweight, read the way the index itself is read.

        A digest rather than three more candle tables: what a leader is *for*
        is confirmation and divergence — is it making a new high while the
        index is not, is it above its own VWAP, has its own swing structure
        broken — and each of those is a number, not a chart. The five-minute
        candles travel alongside for the price action itself.
        """
        tfs = TimeframeSet.build(bars)
        last = bars[-1]
        digest: dict = {
            "last": last.close,
            "change_15m_pct": indicators.momentum_pct(bars, 15),
            "change_30m_pct": indicators.momentum_pct(bars, 30),
            "vwap_dev_pct": indicators.vwap_deviation_pct(bars),
            "rel_volume": indicators.relative_volume(bars),
            "session_high": max(b.high for b in bars),
            "session_low": min(b.low for b in bars),
        }
        if prior_close and prior_close > 0:
            # the day change every screen in the world quotes, and the only way
            # to know whether a leader gapped and is now fading it
            digest["day_change_pct"] = round(
                (last.close - prior_close) / prior_close * 100.0, 2
            )
            digest["prior_close"] = prior_close

        five = structure.describe(tfs.m5)
        fifteen = structure.describe(tfs.m15)
        digest["trend_5m"] = five["trend"]
        digest["trend_15m"] = fifteen["trend"]
        digest["structure_break_5m"] = five["structure_break_level"]
        return digest

    # ------------------------------------------------------------------
    @staticmethod
    def _observe_timeframes(
        packs: dict[str, dict[str, float | None]],
    ) -> list[Observation]:
        """Higher-timeframe context, and whether the timeframes agree.

        Agreement across 1m/5m/15m is the single most useful piece of context a
        short-term trader has: it separates a real move from a wiggle inside a
        move going the other way.
        """
        out: list[Observation] = []
        directions: list[float] = []

        for label, weight in (("5m", 0.9), ("15m", 1.0)):
            pack = packs.get(label) or {}
            ema9, ema21 = pack.get("ema9"), pack.get("ema21")
            if not ema9 or not ema21:
                continue
            spread_pct = (ema9 - ema21) / ema21 * 100.0
            score = _clamp(spread_pct / 0.30)
            directions.append(score)
            out.append(
                Observation(
                    name=f"trend_{label}",
                    category="trend",
                    value=round(spread_pct, 3),
                    score=score,
                    confidence=weight,
                    note=f"{label} trend direction — the backdrop the 1m entry has to fight or ride",
                )
            )

            momentum = pack.get("mom_5m")
            if momentum is not None:
                out.append(
                    Observation(
                        name=f"momentum_{label}",
                        category="momentum",
                        value=momentum,
                        score=_clamp(momentum / 0.6),
                        confidence=weight * 0.8,
                        note=f"momentum over the last 5 {label} bars",
                    )
                )

        one_minute = packs.get("1m") or {}
        ema9, ema21 = one_minute.get("ema9"), one_minute.get("ema21")
        if ema9 and ema21 and directions:
            directions.append(_clamp((ema9 - ema21) / ema21 * 100.0 / 0.15))
            same_sign = all(d > 0 for d in directions) or all(d < 0 for d in directions)
            out.append(
                Observation(
                    name="timeframe_alignment",
                    category="context",
                    value=round(sum(directions) / len(directions), 3),
                    score=_clamp(sum(directions) / len(directions)) if same_sign else 0.0,
                    confidence=1.0 if same_sign else 0.4,
                    note=(
                        "all timeframes point the same way"
                        if same_sign
                        else "timeframes disagree — a move on one is noise on another"
                    ),
                )
            )

        return out

    # ------------------------------------------------------------------
    def _observe(
        self,
        bars: list[Bar],
        ind: dict[str, float | None],
        lvl: dict[str, float | None],
        flow,
        leader_alignment: float,
    ) -> list[Observation]:
        out: list[Observation] = []
        price = ind.get("price") or bars[-1].close

        # --- trend ---
        ema9, ema21, ema50 = ind.get("ema9"), ind.get("ema21"), ind.get("ema50")
        if ema9 and ema21:
            spread_pct = (ema9 - ema21) / ema21 * 100.0
            out.append(
                Observation(
                    name="ema9_vs_ema21",
                    category="trend",
                    value=round(spread_pct, 3),
                    score=_clamp(spread_pct / 0.15),
                    confidence=0.9,
                    note="short-term trend direction and separation",
                )
            )
        if ema50:
            out.append(
                Observation(
                    name="price_vs_ema50",
                    category="trend",
                    value=round((price - ema50) / ema50 * 100.0, 3),
                    score=_clamp((price - ema50) / ema50 * 100.0 / 0.4),
                    confidence=0.7,
                    note="session trend backdrop",
                )
            )

        # --- vwap ---
        vwap_dev = ind.get("vwap_dev_pct")
        if vwap_dev is not None:
            out.append(
                Observation(
                    name="vwap_deviation",
                    category="trend",
                    value=vwap_dev,
                    score=_clamp(vwap_dev / 0.35),
                    confidence=0.95,
                    note="institutional reference; extended readings mean-revert",
                )
            )

        # --- momentum ---
        for window, weight in (("mom_5m", 0.8), ("mom_15m", 1.0), ("mom_30m", 0.7)):
            value = ind.get(window)
            if value is not None:
                out.append(
                    Observation(
                        name=window,
                        category="momentum",
                        value=value,
                        score=_clamp(value / 0.35),
                        confidence=weight,
                        note=f"price change over {window.split('_')[1]}",
                    )
                )

        rsi_value = ind.get("rsi14")
        if rsi_value is not None:
            out.append(
                Observation(
                    name="rsi14",
                    category="momentum",
                    value=rsi_value,
                    score=_clamp((rsi_value - 50) / 25),
                    confidence=0.6,
                    note="momentum oscillator; extremes are context, not signals",
                )
            )

        macd_hist = ind.get("macd_hist")
        if macd_hist is not None:
            out.append(
                Observation(
                    name="macd_histogram",
                    category="momentum",
                    value=macd_hist,
                    score=_clamp(macd_hist / 0.12),
                    confidence=0.6,
                    note="momentum acceleration",
                )
            )

        # --- volatility / participation ---
        rel_vol = ind.get("rel_volume")
        if rel_vol is not None:
            out.append(
                Observation(
                    name="relative_volume",
                    category="volatility",
                    value=rel_vol,
                    score=0.0,
                    confidence=min(rel_vol / 2.0, 1.0),
                    note="conviction behind the current move (non-directional)",
                )
            )

        atr_value = ind.get("atr14")
        if atr_value is not None and price:
            atr_pct = atr_value / price * 100.0
            out.append(
                Observation(
                    name="atr_pct",
                    category="volatility",
                    value=round(atr_pct, 4),
                    score=0.0,
                    confidence=0.8,
                    note="expected per-minute range; drives realistic targets",
                )
            )

        # --- levels ---
        nearby = levels.nearest_levels(price, lvl)
        nearest_distance = levels.distance_to_nearest_pct(price, lvl)
        if nearest_distance is not None:
            resistance = nearby["resistance"][0] if nearby["resistance"] else None
            support = nearby["support"][0] if nearby["support"] else None
            note_parts = []
            if resistance:
                note_parts.append(f"resistance {resistance[0]} @ {resistance[1]} ({resistance[2]:+.2f}%)")
            if support:
                note_parts.append(f"support {support[0]} @ {support[1]} ({support[2]:+.2f}%)")
            out.append(
                Observation(
                    name="level_proximity",
                    category="level",
                    value=nearest_distance,
                    score=0.0,
                    confidence=0.9,
                    note="; ".join(note_parts) or "no significant level nearby",
                )
            )

        session_high, session_low = lvl.get("session_high"), lvl.get("session_low")
        if session_high and session_low and session_high > session_low:
            position = (price - session_low) / (session_high - session_low)
            out.append(
                Observation(
                    name="range_position",
                    category="level",
                    value=round(position, 3),
                    score=_clamp((position - 0.5) * 2 * 0.6),
                    confidence=0.7,
                    note="where price sits inside the session range (0=low, 1=high)",
                )
            )

        # --- flow ---
        if flow is not None:
            out.append(
                Observation(
                    name="options_flow_bias",
                    category="flow",
                    value=flow.net_premium,
                    score=flow_bias(flow),
                    confidence=0.5 + 0.5 * flow.urgency,
                    note=(
                        f"calls ${flow.call_premium:,.0f} vs puts ${flow.put_premium:,.0f}, "
                        f"{flow.sweep_count} sweeps, {flow.block_count} blocks, "
                        f"urgency {flow.urgency}"
                    ),
                )
            )

        # --- context ---
        if leader_alignment:
            out.append(
                Observation(
                    name="leader_alignment",
                    category="context",
                    value=round(leader_alignment, 3),
                    score=_clamp(leader_alignment * 0.8),
                    confidence=0.75,
                    note="how many index heavyweights agree with the move",
                )
            )

        return out

    @staticmethod
    def _classify_regime(ind: dict[str, float | None]) -> MarketRegime:
        ema9, ema21 = ind.get("ema9"), ind.get("ema21")
        atr_value, price = ind.get("atr14"), ind.get("price")
        mom = ind.get("mom_30m")

        if ema9 is None or ema21 is None or price is None:
            return MarketRegime.UNKNOWN

        separation_pct = abs(ema9 - ema21) / ema21 * 100.0
        atr_pct = (atr_value / price * 100.0) if atr_value and price else 0.0

        if separation_pct < 0.03:
            return MarketRegime.VOLATILE_CHOP if atr_pct > 0.08 else MarketRegime.RANGING
        if mom is not None and mom > 0.15 and ema9 > ema21:
            return MarketRegime.TRENDING_UP
        if mom is not None and mom < -0.15 and ema9 < ema21:
            return MarketRegime.TRENDING_DOWN
        return MarketRegime.RANGING
