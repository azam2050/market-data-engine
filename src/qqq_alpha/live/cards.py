"""Signal cards: the rendered PNG a subscriber sees before any text.

A well-made card is read in two seconds on a phone lock screen — contract,
direction, the numbers that matter — where a text wall is skimmed or skipped.
The design borrows what works from the prettiest channels in this space and
drops what doesn't: every close is rendered with the same pride whether it is
green or red, because the record is the product.

Arabic is drawn through libraqm (bundled with Pillow's wheels), which applies
the font's own OpenType shaping — the only way joined script comes out right.
If a build ever lacks raqm, the reshaper+bidi fallback keeps the cards legible.

Rendering is best-effort by contract: every public function either returns PNG
bytes or raises, and the caller (BroadcastNotifier) treats any raise as "no
card today" and falls back to the text path. A drawing bug must never cost a
subscriber a signal.
"""

from __future__ import annotations

import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont, features

from qqq_alpha.config import MARKET_TZ
from qqq_alpha.domain import Trade, TradeUpdate
from qqq_alpha.live.notifier import EXIT_REASON_AR, human_contract, size_label

log = logging.getLogger(__name__)

FONT_DIR = Path(__file__).parent / "fonts"
RAQM = features.check("raqm")

W, H = 1080, 1420
MARGIN = 48

# palette — the same family as the dashboard so the brand reads as one thing
BG = "#0A1020"
GRID = "#131C33"
PANEL = "#111A30"
BORDER = "#2A3A5F"
TEXT = "#E9EDF5"
MUTED = "#8B93A3"
GREEN = "#35C78A"
RED = "#EF5F6B"
GOLD = "#E0B23E"
BLUE = "#4F8CFF"

_fonts: dict[tuple[str, int], ImageFont.FreeTypeFont] = {}


def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    key = ("bold" if bold else "regular", size)
    if key not in _fonts:
        name = "Tajawal-Bold.ttf" if bold else "Tajawal-Regular.ttf"
        _fonts[key] = ImageFont.truetype(str(FONT_DIR / name), size)
    return _fonts[key]


def _legacy_shape(text: str) -> str:
    import arabic_reshaper
    from bidi.algorithm import get_display

    return get_display(arabic_reshaper.reshape(text))


def _rtl(draw: ImageDraw.ImageDraw, xy, text: str, font, fill, anchor: str) -> None:
    """Arabic (or mixed) text with proper shaping, RTL base direction."""
    if RAQM:
        draw.text(xy, text, font=font, fill=fill, anchor=anchor, direction="rtl", language="ar")
    else:
        draw.text(xy, _legacy_shape(text), font=font, fill=fill, anchor=anchor)


def _rtl_length(draw: ImageDraw.ImageDraw, text: str, font) -> float:
    if RAQM:
        return draw.textlength(text, font=font, direction="rtl", language="ar")
    return draw.textlength(_legacy_shape(text), font=font)


def _wrap(draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> list[str]:
    """Word-wrap logical Arabic; shaping happens at draw time."""
    lines: list[str] = []
    current: list[str] = []
    for word in text.split():
        candidate = " ".join([*current, word])
        if _rtl_length(draw, candidate, font) <= max_width or not current:
            current.append(word)
        else:
            lines.append(" ".join(current))
            current = [word]
    if current:
        lines.append(" ".join(current))
    return lines


def _canvas() -> tuple[Image.Image, ImageDraw.ImageDraw]:
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    # a faint blueprint grid, then the outer frame
    for x in range(0, W, 54):
        draw.line([(x, 0), (x, H)], fill=GRID, width=1)
    for y in range(0, H, 54):
        draw.line([(0, y), (W, y)], fill=GRID, width=1)
    draw.rounded_rectangle([16, 16, W - 16, H - 16], radius=28, outline=BORDER, width=3)
    return img, draw


def _panel(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], outline: str = BORDER):
    draw.rounded_rectangle(box, radius=22, fill=PANEL, outline=outline, width=2)


def _header(draw: ImageDraw.ImageDraw, subtitle: str) -> int:
    _panel(draw, (MARGIN, 56, W - MARGIN, 196))
    draw.text((W / 2, 112), "QQQ Alpha", font=_font(58, bold=True), fill=TEXT, anchor="mm")
    _rtl(draw, (W / 2, 164), subtitle, _font(28), MUTED, "mm")
    return 226


def _titled_panel(
    draw: ImageDraw.ImageDraw, y: int, title: str, inner_rows: int, extra: int = 0
) -> tuple[int, int]:
    """A panel with a gold right-aligned title. Returns (first_row_y, next_y)."""
    height = 84 + inner_rows * 60 + extra
    _panel(draw, (MARGIN, y, W - MARGIN, y + height))
    _rtl(draw, (W - MARGIN - 30, y + 42), title, _font(32, bold=True), GOLD, "rm")
    return y + 110, y + height + 24


def _row(
    draw: ImageDraw.ImageDraw,
    y: int,
    label: str,
    value: str,
    value_fill: str = TEXT,
    value_rtl: bool = False,
) -> None:
    """One details row: Arabic label on the right, value on the left."""
    left, right = MARGIN + 36, W - MARGIN - 36
    draw.rounded_rectangle([left, y - 26, right, y + 26], radius=12, fill=BG, outline=GRID)
    _rtl(draw, (right - 22, y), label, _font(30, bold=True), MUTED, "rm")
    if value_rtl:
        _rtl(draw, (left + 22, y), value, _font(30, bold=True), value_fill, "lm")
    else:
        draw.text((left + 22, y), value, font=_font(32, bold=True), fill=value_fill, anchor="lm")


def _chip(draw: ImageDraw.ImageDraw, center_x: float, y: int, text: str, color: str) -> None:
    font = _font(30, bold=True)
    width = _rtl_length(draw, text, font) + 56
    draw.rounded_rectangle(
        [center_x - width / 2, y - 28, center_x + width / 2, y + 28],
        radius=14,
        outline=color,
        width=2,
    )
    _rtl(draw, (center_x, y), text, font, color, "mm")


def _footer(draw: ImageDraw.ImageDraw, when: datetime, delayed: bool, note: str = "") -> None:
    y = H - 128
    if note:
        _rtl(draw, (W / 2, y), note, _font(26), MUTED, "mm")
        y += 38
    if delayed:
        _rtl(
            draw, (W / 2, y),
            "بيانات متأخرة ١٥ دقيقة — للاختبار وليست للتنفيذ",
            _font(26, bold=True), RED, "mm",
        )
        y += 38
    stamp = when.astimezone(MARKET_TZ).strftime("%H:%M")
    _rtl(
        draw, (W / 2, y),
        f"التداول ينطوي على مخاطر عالية — القرار والمسؤولية عليك • {stamp} نيويورك",
        _font(24), MUTED, "mm",
    )


def _png(img: Image.Image) -> bytes:
    buffer = BytesIO()
    img.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


# ---------------------------------------------------------------- entry
def render_entry_card(trade: Trade, delayed: bool) -> bytes:
    decision = trade.decision
    is_call = bool(decision.direction and decision.direction.value == "CALL")
    accent = GREEN if is_call else RED
    contract = human_contract(trade.occ_symbol, trade.opened_at)

    img, draw = _canvas()
    y = _header(draw, "بوت الخيارات اليومي")

    # the contract, big enough to read from across a room
    _panel(draw, (MARGIN, y, W - MARGIN, y + 280), outline=accent)
    _chip(draw, W / 2, y + 50, "توصية جديدة", GOLD)
    draw.text((W / 2, y + 144), contract, font=_font(84, bold=True), fill=accent, anchor="mm")
    _rtl(
        draw, (W / 2, y + 228),
        f"الاتجاه: {'صعود CALL' if is_call else 'هبوط PUT'}",
        _font(34, bold=True), TEXT, "mm",
    )
    y += 310

    # the numbers a subscriber acts on
    rows: list[tuple[str, str, str, bool]] = [
        ("سعر الدخول", f"${trade.entry_price:.2f}", BLUE, False)
    ]
    if decision.stop_price is not None:
        rows.append(
            ("وقف الحماية", f"${decision.stop_price:.2f} ({decision.stop_return_pct:+.0f}%)", RED, False)
        )
    if decision.invalidation_level is not None:
        rows.append(("وقف الفكرة - على السهم", f"{decision.invalidation_level:.2f}", RED, False))
    rows.append(("الحجم المقترح", size_label(decision.size_factor), GOLD, True))
    rows.append(("الثقة", f"{decision.confidence}/10", TEXT, False))

    row_y, y = _titled_panel(draw, y, "تفاصيل الصفقة", len(rows))
    for label, value, fill, value_rtl in rows:
        _row(draw, row_y, label, value, fill, value_rtl)
        row_y += 60

    # targets, plus the management line that makes this desk different
    targets = decision.targets[:4]
    row_y, y = _titled_panel(draw, y, "الأهداف", len(targets), extra=90)
    for index, target in enumerate(targets, start=1):
        _row(draw, row_y, f"الهدف {index}", f"${target.price:.2f}  (+{target.return_pct:.0f}%)", GREEN)
        row_y += 60
    for line in _wrap(
        draw,
        "الإدارة: عند +35% نبيع النصف ونؤمن التكلفة — والباقي يركض بوقف متحرك",
        _font(26, bold=True),
        W - 2 * MARGIN - 120,
    )[:2]:
        _rtl(draw, (W / 2, row_y + 16), line, _font(26, bold=True), BLUE, "mm")
        row_y += 38

    _footer(draw, trade.opened_at, delayed)
    return _png(img)


# ---------------------------------------------------------------- scale-out
def render_scale_out_card(trade: Trade, update: TradeUpdate) -> bytes:
    img, draw = _canvas()
    y = _header(draw, "إدارة الصفقة")

    _panel(draw, (MARGIN, y, W - MARGIN, y + 330), outline=GREEN)
    _rtl(draw, (W / 2, y + 78), "تم تأمين التكلفة", _font(64, bold=True), GREEN, "mm")
    draw.text(
        (W / 2, y + 180), f"+{update.return_pct:.0f}%", font=_font(96, bold=True),
        fill=GREEN, anchor="mm",
    )
    _chip(draw, W / 2, y + 272, "بيع نصف الكمية الآن", GOLD)
    y += 360

    contract = human_contract(trade.occ_symbol, trade.opened_at)
    row_y, y = _titled_panel(draw, y, "التفاصيل", 3)
    _row(draw, row_y, "العقد", contract, TEXT)
    _row(draw, row_y + 60, "سعر البيع الجزئي", f"${update.price:.2f}", BLUE)
    _row(draw, row_y + 120, "وقف الباقي", "التعادل + وقف متحرك من القمة", GOLD, value_rtl=True)
    y += 16

    for line in _wrap(
        draw,
        "من هذه اللحظة لا يمكن لهذه الصفقة أن تخسر — النصف الباقي يطارد الامتداد",
        _font(30, bold=True),
        W - 2 * MARGIN - 60,
    )[:2]:
        _rtl(draw, (W / 2, y), line, _font(30, bold=True), TEXT, "mm")
        y += 44

    _footer(draw, update.ts, delayed=False)
    return _png(img)


# ---------------------------------------------------------------- close
def render_close_card(trade: Trade, update: TradeUpdate) -> bytes:
    result = trade.return_pct if trade.return_pct is not None else update.return_pct
    win = result > 1.0
    flat = -1.0 <= result <= 1.0
    accent = GREEN if win else (MUTED if flat else RED)
    verdict = "صفقة رابحة" if win else ("تعادل" if flat else "صفقة خاسرة")

    img, draw = _canvas()
    y = _header(draw, "نتيجة الصفقة — كما هي، ربحا أو خسارة")

    _panel(draw, (MARGIN, y, W - MARGIN, y + 290), outline=accent)
    draw.text(
        (W / 2, y + 108), f"{result:+.1f}%", font=_font(116, bold=True),
        fill=accent, anchor="mm",
    )
    _chip(draw, W / 2, y + 228, verdict, accent)
    y += 316

    reason = EXIT_REASON_AR.get(trade.exit_reason, "")
    if reason:
        for line in _wrap(draw, reason, _font(28, bold=True), W - 2 * MARGIN - 60)[:2]:
            _rtl(draw, (W / 2, y), line, _font(28, bold=True), MUTED, "mm")
            y += 38
        y += 8

    contract = human_contract(trade.occ_symbol, trade.opened_at)
    held = int((update.ts - trade.opened_at).total_seconds() // 60) if trade.opened_at else 0
    rows: list[tuple[str, str, str, bool]] = [
        ("العقد", contract, TEXT, False),
        ("سعر الدخول", f"${trade.entry_price:.2f}", BLUE, False),
        ("سعر الخروج", f"${update.price:.2f}", BLUE, False),
        ("أعلى ما وصلته", f"{trade.max_favorable_pct:+.1f}%", GREEN, False),
        ("مدة الصفقة", f"{held} دقيقة", TEXT, True),
    ]
    if trade.banked_return_pct > 0:
        rows.append(("تأمين النصف", f"تم — ساهم بـ {trade.banked_return_pct:+.1f}%", GREEN, True))

    row_y, y = _titled_panel(draw, y, "التفاصيل", len(rows))
    for label, value, fill, value_rtl in rows:
        _row(draw, row_y, label, value, fill, value_rtl)
        row_y += 60

    targets = trade.decision.targets[:3]
    if targets:
        row_y, y = _titled_panel(draw, y, "الأهداف", len(targets))
        for index, target in enumerate(targets, start=1):
            achieved = trade.max_favorable_pct >= target.return_pct
            _row(
                draw, row_y, f"الهدف {index} (+{target.return_pct:.0f}%)",
                "تحقق" if achieved else "لم يتحقق",
                GREEN if achieved else MUTED,
                value_rtl=True,
            )
            row_y += 60

    _footer(
        draw, update.ts, delayed=False,
        note="سجلنا كامل وشفاف — كل صفقة تنشر بنتيجتها الحقيقية",
    )
    return _png(img)
