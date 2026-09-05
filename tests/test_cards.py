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


# ---------------------------------------------------------------- self-test
def test_card_self_test_passes_on_the_shipped_configuration():
    from qqq_alpha.live import cards

    ok, message = cards.self_test()
    assert ok, message
    assert "سليم" in message


def test_card_self_test_catches_the_tofu_regression(monkeypatch):
    """The exact production failure: a rebuild loses libraqm, the renderer
    falls back to presentation forms, and the brand font has no glyphs for
    some of them. Nothing detected it — subscribers saw empty boxes. This
    reproduces that build and asserts the self-test refuses it."""
    from qqq_alpha.live import cards

    monkeypatch.setattr(cards, "RAQM", False)
    monkeypatch.setattr(cards, "_FAMILY", ("Tajawal-Regular.ttf", "Tajawal-Bold.ttf"))
    monkeypatch.setattr(cards, "_fonts", {})

    ok, message = cards.self_test()
    assert not ok
    assert "مربعات فارغة" in message
    assert "U+FE" in message  # the presentation forms Tajawal does not carry


def test_a_broken_renderer_is_reported_not_raised(monkeypatch):
    from qqq_alpha.live import cards

    def explode(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(cards, "render_close_card", explode)
    ok, message = cards.self_test()
    assert not ok
    assert "تعذّر الرسم" in message


def test_arabic_date_never_renders_as_a_reversed_iso_string():
    from datetime import date

    from qqq_alpha.live.cards import arabic_date

    assert arabic_date(date(2026, 8, 17)) == "17 أغسطس 2026"
    assert "-" not in arabic_date(date(2026, 8, 17))
