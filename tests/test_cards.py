"""Signal cards: the rendered PNGs must be valid images for every trade shape
the manager can produce — entry, scale-out, trailed winner, stopped loser."""

from __future__ import annotations

from datetime import date, timedelta

from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, OptionType, Target
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.live import cards
from qqq_alpha.trades import TradeManager

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def _open_trade(manager: TradeManager, direction: OptionType = OptionType.CALL):
    bars = synthetic_session("QQQ", date(2026, 8, 13), seed=15)
    snap = SnapshotBuilder("QQQ").build(bars[:80])
    decision = Decision(
        ts=snap.ts,
        action=Action.ENTER,
        direction=direction,
        occ_symbol="O:QQQ260813C00726000",
        targets=[
            Target(label="T1", price=0.0, return_pct=50, take_pct=50),
            Target(label="T2", price=0.0, return_pct=100, take_pct=30),
        ],
        stop_return_pct=-40,
        confidence=7,
        thesis="أطروحة عربية كاملة مع مصطلحات مثل VWAP وdelta",
        invalidation_level=724.5,
        size_factor=0.75,
    )
    return manager.open_trade(decision, 1.79, snap)


def test_entry_card_is_a_valid_png():
    trade = _open_trade(TradeManager())
    png = cards.render_entry_card(trade, delayed=False)
    assert png.startswith(PNG_MAGIC) and len(png) > 10_000


def test_scale_out_and_close_cards_are_valid_pngs():
    manager = TradeManager()
    trade = _open_trade(manager)

    scale = manager.update(trade, 2.42, trade.opened_at + timedelta(minutes=9))
    assert scale is not None and scale.note.startswith("scale_out")
    assert cards.render_scale_out_card(trade, scale).startswith(PNG_MAGIC)

    manager.update(trade, 3.60, trade.opened_at + timedelta(minutes=20))
    close = manager.update(trade, 3.00, trade.opened_at + timedelta(minutes=26))
    assert close is not None and "closed:" in close.note
    assert cards.render_close_card(trade, close).startswith(PNG_MAGIC)


def test_losing_close_card_renders_without_flinching():
    """Honesty is the brand: the red card must render as reliably as the green."""
    manager = TradeManager()
    trade = _open_trade(manager, direction=OptionType.PUT)
    close = manager.update(trade, 1.05, trade.opened_at + timedelta(minutes=7))
    assert close is not None and "closed:stop_hit" in close.note

    png = cards.render_close_card(trade, close)
    assert png.startswith(PNG_MAGIC)


def test_report_cards_render_as_valid_pngs():
    from qqq_alpha.live.review import ReviewStats

    daily = cards.render_daily_report_card(
        date(2026, 8, 14),
        [
            {"label": "QQQ 731 PUT 0DTE", "return_pct": 68.1, "shared": True},
            {"label": "QQQ 733 CALL 0DTE", "return_pct": -3.9, "shared": False},
        ],
    )
    assert daily.startswith(PNG_MAGIC) and len(daily) > 10_000

    stats = ReviewStats(closed=7, wins=4, losses=3, win_rate=57.1,
                        expectancy_pct=12.4, best_pct=68.1, worst_pct=-41.7)
    weekly = cards.render_weekly_report_card(
        stats, [{"label": "QQQ 731 PUT 0DTE", "return_pct": 68.1}]
    )
    assert weekly.startswith(PNG_MAGIC) and len(weekly) > 10_000


def test_cards_survive_a_raqm_less_environment(monkeypatch):
    """Production once lost libraqm on a rebuild and shipped tofu boxes: the
    fallback path must render complete Arabic with the Amiri family, whose
    cmap covers every presentation form the reshaper emits."""
    monkeypatch.setattr(cards, "RAQM", False)
    monkeypatch.setattr(cards, "_FAMILY", ("Amiri-Regular.ttf", "Amiri-Bold.ttf"))
    monkeypatch.setattr(cards, "_fonts", {})

    trade = _open_trade(TradeManager())
    assert cards.render_entry_card(trade, delayed=False).startswith(PNG_MAGIC)
    from datetime import UTC, datetime
    png = cards.render_watch_card(
        "QQQ", "هبوط PUT", "ارتداد فاشل نحو VWAP", 6,
        datetime(2026, 8, 14, 15, 49, tzinfo=UTC), level=732.5,
    )
    assert png.startswith(PNG_MAGIC)
