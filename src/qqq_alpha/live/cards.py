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
from contextlib import contextmanager
from datetime import datetime
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageColor, ImageDraw, ImageFont, features

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

# stage tints — the whole card shifts color with the trade's life stage, so a
# follower reads the channel like traffic lights: blue "forming", the brand
# navy "a new study was posted", green "alive right now", green/red "closed
# as". Operator-approved palette; the entry card deliberately keeps the
# original navy — the gold full-tint was tried and rejected as too heavy.
_STAGE_THEMES: dict[str, tuple[str, str, str, str]] = {
    "watch": ("#050E22", "#0A1834", "#081530", "#2A4A8F"),
    "live": ("#04180F", "#082418", "#072015", "#1F6B4A"),
    "win": ("#04180F", "#082418", "#072015", "#1F6B4A"),
    "loss": ("#1C0709", "#2A0D11", "#260A0F", "#7A2733"),
    # the monthly statement: warmer ink and a gold-leaning frame, so it is
    # recognisable as the month's document at a glance and never mistaken for
    # one more daily receipt scrolling past
    "month": ("#0B0A06", "#181509", "#141208", "#5C4A1E"),
}


@contextmanager
def _stage(name: str | None):
    """Swap the card's base palette for one render. Rendering is synchronous
    single-threaded work, so a plain global swap-and-restore is safe."""
    global BG, GRID, PANEL, BORDER
    theme = _STAGE_THEMES.get(name or "")
    if theme is None:
        yield
        return
    previous = (BG, GRID, PANEL, BORDER)
    BG, GRID, PANEL, BORDER = theme
    try:
        yield
    finally:
        BG, GRID, PANEL, BORDER = previous

_fonts: dict[tuple[str, int], ImageFont.FreeTypeFont] = {}

# The brand font (Tajawal) needs libraqm to shape Arabic — its cmap lacks 17
# of the presentation forms the reshaper fallback emits, which is exactly how
# production cards once shipped with tofu boxes when a rebuild lost raqm.
# Amiri carries the complete presentation-form set, so the no-raqm path
# switches to it: a different (naskh) look beats broken letters every time.
_FAMILY = ("Tajawal-Regular.ttf", "Tajawal-Bold.ttf") if RAQM else (
    "Amiri-Regular.ttf", "Amiri-Bold.ttf"
)
log.info("cards: raqm=%s, family=%s", RAQM, _FAMILY[0].split("-")[0])


def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    key = ("bold" if bold else "regular", size)
    if key not in _fonts:
        _fonts[key] = ImageFont.truetype(str(FONT_DIR / _FAMILY[1 if bold else 0]), size)
    return _fonts[key]


def _legacy_shape(text: str) -> str:
    import arabic_reshaper
    from bidi.algorithm import get_display

    return get_display(arabic_reshaper.reshape(text))


# when a self-test is running, every string that goes to the canvas is recorded
# here. It is the only way to check the *actual* vocabulary a card draws rather
# than a guess at it — and guessing is what let 17 missing presentation forms
# reach production as tofu boxes.
_capture: list[str] | None = None


def _rtl(draw: ImageDraw.ImageDraw, xy, text: str, font, fill, anchor: str) -> None:
    """Arabic (or mixed) text with proper shaping, RTL base direction."""
    if _capture is not None:
        _capture.append(text)
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


def _canvas(height: int = H) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    img = Image.new("RGB", (W, height), BG)
    draw = ImageDraw.Draw(img)
    # a faint blueprint grid, then the outer frame
    for x in range(0, W, 54):
        draw.line([(x, 0), (x, height)], fill=GRID, width=1)
    for y in range(0, height, 54):
        draw.line([(0, y), (W, y)], fill=GRID, width=1)
    draw.rounded_rectangle([16, 16, W - 16, height - 16], radius=28, outline=BORDER, width=3)
    return img, draw


def _panel(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], outline: str = BORDER):
    draw.rounded_rectangle(box, radius=22, fill=PANEL, outline=outline, width=2)


def _header(draw: ImageDraw.ImageDraw, subtitle: str) -> int:
    _panel(draw, (MARGIN, 56, W - MARGIN, 196))
    _rtl(draw, (W / 2, 112), "بوت عقود الخيارات", _font(54, bold=True), GOLD, "mm")
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
        f"محتوى تعليمي وليس توصية استثمارية — الخيارات عالية المخاطر والقرار مسؤوليتك • {stamp} نيويورك",
        _font(22), MUTED, "mm",
    )


def _png(img: Image.Image) -> bytes:
    buffer = BytesIO()
    img.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


# ---------------------------------------------------------------- entry
def render_entry_card(trade: Trade, delayed: bool, live: TradeUpdate | None = None) -> bytes:
    """The entry card — and, via ``live``, the living version of it.

    Every ~15 minutes the engine re-renders this card with the current price
    and edits the already-posted message in place. The live version shifts
    the WHOLE card to the green stage tint — a glance says "alive right now"
    before a single word is read — while the fresh entry keeps the brand navy.
    """
    with _stage("live" if live is not None else None):
        return _draw_entry_card(trade, delayed, live)


def _draw_entry_card(trade: Trade, delayed: bool, live: TradeUpdate | None) -> bytes:
    decision = trade.decision
    is_call = bool(decision.direction and decision.direction.value == "CALL")
    accent = GREEN if is_call else RED
    contract = human_contract(trade.occ_symbol, trade.opened_at)

    img, draw = _canvas()
    y = _header(draw, "طروحات فنية تعليمية على عقود الخيارات")

    # the contract, big enough to read from across a room
    _panel(draw, (MARGIN, y, W - MARGIN, y + 280), outline=accent)
    if live is None:
        _chip(draw, W / 2, y + 50, "طرح تعليمي حي", GOLD)
    else:
        elapsed = int((live.ts - trade.opened_at).total_seconds() // 60)
        _chip(
            draw, W / 2, y + 50,
            f"ما زلنا في الطرح — الآن {live.return_pct:+.1f}% • {elapsed} دقيقة",
            GREEN if live.return_pct >= 0 else GOLD,
        )
    draw.text((W / 2, y + 144), contract, font=_font(84, bold=True), fill=accent, anchor="mm")
    _rtl(
        draw, (W / 2, y + 228),
        f"الاتجاه: {'صعود CALL' if is_call else 'هبوط PUT'}",
        _font(34, bold=True), TEXT, "mm",
    )
    y += 310

    # the numbers that define the study — labelled as a plan being followed,
    # never as an instruction to the reader
    rows: list[tuple[str, str, str, bool]] = [
        ("سعر الطرح", f"${trade.entry_price:.2f}", BLUE, False)
    ]
    if decision.stop_price is not None:
        rows.append(
            ("وقف الحماية", f"${decision.stop_price:.2f} ({decision.stop_return_pct:+.0f}%)", RED, False)
        )
    if decision.invalidation_level is not None:
        rows.append(("وقف الفكرة - على السهم", f"{decision.invalidation_level:.2f}", RED, False))
    rows.append(("نموذج إدارة رأس المال", size_label(decision.size_factor), GOLD, True))
    rows.append(("الثقة", f"{decision.confidence}/10", TEXT, False))

    row_y, y = _titled_panel(draw, y, "تفاصيل الطرح", len(rows))
    for label, value, fill, value_rtl in rows:
        _row(draw, row_y, label, value, fill, value_rtl)
        row_y += 60

    # follow-up levels, plus the management line that makes this desk different
    targets = decision.targets[:4]
    row_y, y = _titled_panel(draw, y, "مستويات المتابعة", len(targets), extra=90)
    for index, target in enumerate(targets, start=1):
        _row(draw, row_y, f"المستوى {index}", f"${target.price:.2f}  (+{target.return_pct:.0f}%)", GREEN)
        row_y += 60
    for line in _wrap(
        draw,
        "الإدارة الآلية: عند +35% يُباع النصف وتُؤمَّن التكلفة — والباقي يركض بوقف متحرك",
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
    y = _header(draw, "تطبيق عملي: تأمين التكلفة")

    _panel(draw, (MARGIN, y, W - MARGIN, y + 330), outline=GREEN)
    _rtl(draw, (W / 2, y + 78), "تم تأمين التكلفة", _font(64, bold=True), GREEN, "mm")
    draw.text(
        (W / 2, y + 180), f"+{update.return_pct:.0f}%", font=_font(96, bold=True),
        fill=GREEN, anchor="mm",
    )
    _chip(draw, W / 2, y + 272, "بِيع نصف الكمية آليًا", GOLD)
    y += 360

    contract = human_contract(trade.occ_symbol, trade.opened_at)
    row_y, y = _titled_panel(draw, y, "التفاصيل", 3)
    _row(draw, row_y, "العقد", contract, TEXT)
    _row(draw, row_y + 60, "سعر البيع الجزئي", f"${update.price:.2f}", BLUE)
    _row(draw, row_y + 120, "وقف الباقي", "التعادل + وقف متحرك من القمة", GOLD, value_rtl=True)
    y += 16

    for line in _wrap(
        draw,
        "الدرس: من هذه اللحظة لا يمكن لهذا الطرح أن يخسر — النصف الباقي يطارد الامتداد",
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
    stage = "win" if result > 1.0 else ("loss" if result < -1.0 else None)
    with _stage(stage):
        return _draw_close_card(trade, update)


def _draw_close_card(trade: Trade, update: TradeUpdate) -> bytes:
    result = trade.return_pct if trade.return_pct is not None else update.return_pct
    win = result > 1.0
    flat = -1.0 <= result <= 1.0
    accent = GREEN if win else (MUTED if flat else RED)
    verdict = "طرح رابح" if win else ("تعادل" if flat else "طرح خاسر")

    img, draw = _canvas()
    y = _header(draw, "نتيجة الطرح — كما هي، ربحا أو خسارة")

    _panel(draw, (MARGIN, y, W - MARGIN, y + 290), outline=accent)
    draw.text(
        (W / 2, y + 108), f"{result:+.1f}%", font=_font(116, bold=True),
        fill=accent, anchor="mm",
    )
    _chip(draw, W / 2, y + 228, verdict, accent)
    y += 316

    reason = EXIT_REASON_AR.get(trade.exit_reason, "")
    if reason:
        reason = f"الدرس المستفاد: {reason}"
        for line in _wrap(draw, reason, _font(28, bold=True), W - 2 * MARGIN - 60)[:2]:
            _rtl(draw, (W / 2, y), line, _font(28, bold=True), MUTED, "mm")
            y += 38
        y += 8

    contract = human_contract(trade.occ_symbol, trade.opened_at)
    held = int((update.ts - trade.opened_at).total_seconds() // 60) if trade.opened_at else 0
    rows: list[tuple[str, str, str, bool]] = [
        ("العقد", contract, TEXT, False),
        ("سعر الطرح", f"${trade.entry_price:.2f}", BLUE, False),
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
        row_y, y = _titled_panel(draw, y, "مستويات المتابعة", len(targets))
        for index, target in enumerate(targets, start=1):
            achieved = trade.max_favorable_pct >= target.return_pct
            _row(
                draw, row_y, f"المستوى {index} (+{target.return_pct:.0f}%)",
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


# ---------------------------------------------------------------- reports
METHODOLOGY_LINE = "النتائج كما أُغلقت فعليًا — لا نحسب طرحًا رابحًا لمجرد أنه لامس مستوى ثم انعكس"
MODEL_NOTE_LINE = "النموذج الافتراضي لأغراض التوضيح فقط — ليست نتائج حساب حقيقي"
DISCLAIMER_LINE = "محتوى تعليمي وليس توصية استثمارية — الخيارات عالية المخاطر والقرار مسؤوليتك"
# the hypothetical sizing behind every $ figure on the report cards: $1000
# per single-position unit, scaled by each trade's recommended size factor
MODEL_DOLLARS_PER_TRADE = 1000.0


def _report_tail(draw: ImageDraw.ImageDraw, y: int, height: int) -> None:
    """The methodology line and the disclaimers, shared by both report cards."""
    for line in _wrap(draw, METHODOLOGY_LINE, _font(26, bold=True), W - 2 * MARGIN - 60)[:2]:
        _rtl(draw, (W / 2, y), line, _font(26, bold=True), GOLD, "mm")
        y += 40
    _rtl(draw, (W / 2, height - 116), MODEL_NOTE_LINE, _font(22), MUTED, "mm")
    _rtl(draw, (W / 2, height - 78), DISCLAIMER_LINE, _font(22), MUTED, "mm")


def _dollars(pct_sum: float) -> str:
    value = pct_sum / 100.0 * MODEL_DOLLARS_PER_TRADE
    return f"${value:+,.0f}"


ARABIC_MONTHS = (
    "يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو",
    "يوليو", "أغسطس", "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر",
)
# how much vertical room _report_tail needs below the last panel: two wrapped
# methodology lines, then the model note and the disclaimer pinned to the base
REPORT_TAIL_HEIGHT = 246


def arabic_date(day) -> str:
    """"17 أغسطس 2026" rather than an ISO date.

    An ISO string inside an RTL line is three separate numeric runs, and the
    bidi algorithm reorders them: 2026-08-17 came out of the renderer as
    17-08-2026. Spelling the month removes the ambiguity entirely.
    """
    return f"{day.day} {ARABIC_MONTHS[day.month - 1]} {day.year}"


def _result_color(pct: float) -> str:
    return GREEN if pct > 1 else (MUTED if pct >= -1 else RED)


# ---------------------------------------------------------------- report parts
# The daily and the monthly report are deliberately different objects. A daily
# card is a receipt: what happened today, in order, small enough to read at a
# glance. A monthly card is a statement: shape over time, which is a picture,
# not a list. Drawing both from the same row-stack made them interchangeable —
# and a follower who cannot tell them apart stops reading either.
def _split_bar(
    draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], win: float, loss: float
) -> None:
    """One bar showing gross profit against gross loss, to scale.

    The single number at the top of a report hides whether it came from a calm
    month or a violent one. This does not.
    """
    left, top, right, bottom = box
    total = abs(win) + abs(loss)
    draw.rounded_rectangle(box, radius=10, fill=BG, outline=GRID)
    if total <= 0:
        _rtl(draw, ((left + right) / 2, (top + bottom) / 2), "لا نتائج", _font(24), MUTED, "mm")
        return
    # RTL: profit grows from the right edge, loss from the left
    win_w = (right - left) * (abs(win) / total)
    if win_w > 2:
        draw.rounded_rectangle([right - win_w, top, right, bottom], radius=10, fill=GREEN)
    loss_w = (right - left) * (abs(loss) / total)
    if loss_w > 2:
        draw.rounded_rectangle([left, top, left + loss_w, bottom], radius=10, fill=RED)


def _tile(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    label: str,
    value: str,
    color: str = TEXT,
    value_rtl: bool = False,
) -> None:
    """One KPI tile: a small Arabic caption over a large value."""
    left, top, right, bottom = box
    draw.rounded_rectangle(box, radius=18, fill=PANEL, outline=BORDER, width=2)
    _rtl(draw, ((left + right) / 2, top + 40), label, _font(26), MUTED, "mm")
    center = ((left + right) / 2, (top + bottom) / 2 + 26)
    if value_rtl:
        _rtl(draw, center, value, _font(44, bold=True), color, "mm")
    else:
        draw.text(center, value, font=_font(46, bold=True), fill=color, anchor="mm")


def _curve(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    series: list[float],
    labels: tuple[str, str] | None = None,
) -> None:
    """The month's cumulative result as a line above and below zero.

    Deliberately unsmoothed and unscaled beyond its own range: the drawdowns
    are part of the record, and a curve that hides them is a lie told with a
    nicer font.
    """
    left, top, right, bottom = box
    draw.rounded_rectangle(box, radius=18, fill=PANEL, outline=BORDER, width=2)
    if len(series) < 2:
        _rtl(draw, ((left + right) / 2, (top + bottom) / 2), "لا توجد بيانات كافية",
             _font(26), MUTED, "mm")
        return

    pad = 34
    inner_l, inner_r = left + pad, right - pad
    inner_t, inner_b = top + pad, bottom - pad
    high, low = max(series + [0.0]), min(series + [0.0])
    span = (high - low) or 1.0

    def point(index: int, value: float) -> tuple[float, float]:
        # time runs left to right even on an Arabic card: every chart the
        # reader has ever seen does, and mirroring the axis to match the script
        # makes a rising month look like a falling one
        x = inner_l + (inner_r - inner_l) * (index / (len(series) - 1))
        y = inner_b - (inner_b - inner_t) * ((value - low) / span)
        return x, y

    zero_y = point(0, 0.0)[1]
    for x in range(int(inner_l), int(inner_r), 16):  # dashed zero line
        draw.line([(x, zero_y), (x + 8, zero_y)], fill=BORDER, width=2)

    points = [point(i, v) for i, v in enumerate(series)]
    color = _result_color(series[-1])
    # translucent area under the curve — composited, because a flat fill either
    # disappears into the panel or fights the line for attention
    overlay = Image.new("RGBA", (int(right - left), int(bottom - top)), (0, 0, 0, 0))
    ImageDraw.Draw(overlay).polygon(
        [
            *[(x - left, y - top) for x, y in points],
            (points[-1][0] - left, zero_y - top),
            (points[0][0] - left, zero_y - top),
        ],
        fill=(*ImageColor.getrgb(color), 46),
    )
    draw._image.paste(overlay, (int(left), int(top)), overlay)

    draw.line(points, fill=color, width=5, joint="curve")
    draw.ellipse(
        [points[-1][0] - 9, points[-1][1] - 9, points[-1][0] + 9, points[-1][1] + 9],
        fill=color,
    )
    # the peak and the final value, labelled — the two numbers a reader looks
    # for on a curve and otherwise has to estimate by eye
    # the hero panel already states the final number, so the chart labels its
    # axis instead: which session it starts and ends on. Two fixed corners,
    # so no label can ever land on the line or on another label.
    if labels:
        _rtl(draw, (inner_l, bottom - 12), labels[0], _font(22), MUTED, "lb")
        _rtl(draw, (inner_r, bottom - 12), labels[-1], _font(22), MUTED, "rb")


def _week_bars(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    values: list[float],
    labels: list[str],
) -> None:
    """A column per week, above or below a shared baseline."""
    left, top, right, bottom = box
    draw.rounded_rectangle(box, radius=18, fill=PANEL, outline=BORDER, width=2)
    if not values:
        return
    pad, label_h = 30, 44
    inner_l, inner_r = left + pad, right - pad
    inner_t, inner_b = top + pad, bottom - pad - label_h
    scale = max(abs(v) for v in values) or 1.0
    zero_y = (inner_t + inner_b) / 2
    slot = (inner_r - inner_l) / len(values)
    width = min(slot * 0.52, 88)

    for index, value in enumerate(values):
        # same as the curve: week one on the left, because these bars are a
        # timeline and timelines do not flip with the script
        center_x = inner_l + slot * (index + 0.5)
        height = (inner_b - zero_y) * (abs(value) / scale)
        color = _result_color(value)
        if value >= 0:
            bar = [center_x - width / 2, zero_y - height, center_x + width / 2, zero_y]
        else:
            bar = [center_x - width / 2, zero_y, center_x + width / 2, zero_y + height]
        draw.rounded_rectangle(bar, radius=8, fill=color)
        draw.text(
            (center_x, zero_y - height - 22 if value >= 0 else zero_y + height + 22),
            f"{value:+.0f}%", font=_font(24, bold=True), fill=color, anchor="mm",
        )
        _rtl(draw, (center_x, inner_b + 26), labels[index], _font(24), MUTED, "mm")
    draw.line([(inner_l, zero_y), (inner_r, zero_y)], fill=BORDER, width=2)


def render_daily_report_card(day, rows: list[dict]) -> bytes:
    """The day's receipt: the net, how it split, and every study in order.

    ``rows``: label / return_pct / shared. Kept tight and chronological — a
    daily card is read once, on a phone, the evening it is posted.
    """
    returns = [float(r["return_pct"]) for r in rows]
    total = sum(returns)
    gross_win = sum(r for r in returns if r > 0)
    gross_loss = sum(r for r in returns if r < 0)
    wins = sum(1 for r in returns if r > 1)
    losses = sum(1 for r in returns if r < -1)
    accent = _result_color(total)

    height = 226 + 300 + 148 + (110 + len(rows) * 60 + 24) + REPORT_TAIL_HEIGHT
    img, draw = _canvas(height)
    y = _header(draw, f"بيان الجلسة — {arabic_date(day)}")

    # hero: the net as a percentage and as the model's dollars, side by side,
    # with the profit/loss split drawn to scale underneath
    _panel(draw, (MARGIN, y, W - MARGIN, y + 274), outline=accent)
    _chip(draw, W / 2, y + 44, "تقرير يومي — سجل شفاف", GOLD)
    draw.text((W / 2, y + 132), f"{total:+.1f}%", font=_font(96, bold=True),
              fill=accent, anchor="mm")
    draw.text((W / 2, y + 196), _dollars(total), font=_font(38, bold=True),
              fill=MUTED, anchor="mm")
    _split_bar(draw, (MARGIN + 40, y + 226, W - MARGIN - 40, y + 254), gross_win, gross_loss)
    y += 300

    # three tiles instead of another stack of rows — the daily card's own shape
    tile_w = (W - 2 * MARGIN - 2 * 20) / 3
    for index, (label, value, color) in enumerate(
        (
            ("طروحات اليوم", str(len(rows)), TEXT),
            ("رابحة", str(wins), GREEN),
            ("خاسرة", str(losses), RED),
        )
    ):
        left = MARGIN + index * (tile_w + 20)
        _tile(draw, (int(left), y, int(left + tile_w), y + 124), label, value, color)
    y += 148

    row_y, y = _titled_panel(draw, y, "طروحات اليوم", len(rows))
    for row in rows:
        result = float(row["return_pct"])
        label = str(row["label"])
        if row.get("shared"):
            label += " — نُشر حيًا هنا"
        _row(draw, row_y, label, f"{result:+.1f}%  ({_dollars(result)})", _result_color(result))
        row_y += 60

    _report_tail(draw, y + 10, height)
    return _png(img)


def render_monthly_report_card(
    month,
    stats,
    daily_returns: list[tuple[object, float]],
    channel_rows: list[dict] | None = None,
) -> bytes:
    """The month as a statement, not a list.

    A month of daily cards already told the reader what happened on each day.
    What a month adds is *shape* — the curve, the drawdowns, which weeks
    carried it — and shape has to be drawn. ``daily_returns`` is
    (date, net percent) per session, in order.
    """
    channel_rows = channel_rows or []
    values = [float(v) for _, v in daily_returns]
    cumulative: list[float] = []
    running = 0.0
    for value in values:
        running += value
        cumulative.append(running)
    total = running

    green_days = sum(1 for v in values if v > 1)
    red_days = sum(1 for v in values if v < -1)
    # peak-to-trough on the cumulative curve: the number that tells a reader
    # what holding this month would actually have felt like
    peak, drawdown = 0.0, 0.0
    for value in cumulative:
        peak = max(peak, value)
        drawdown = min(drawdown, value - peak)

    weeks: list[float] = []
    labels: list[str] = []
    for index in range(0, len(values), 5):
        weeks.append(sum(values[index : index + 5]))
        labels.append(f"الأسبوع {len(weeks)}")

    gross_win = stats.avg_win_pct * stats.wins
    gross_loss = stats.avg_loss_pct * stats.losses
    accent = _result_color(total)

    tiles = [
        ("الطروحات المغلقة", str(stats.closed), TEXT, False),
        ("نسبة الرابحة", f"{stats.win_rate:.0f}%", GOLD, False),
        ("متوسط الطرح", f"{stats.expectancy_pct:+.1f}%", _result_color(stats.expectancy_pct), False),
        ("أفضل طرح", f"{stats.best_pct:+.1f}%", GREEN, False),
        ("أسوأ طرح", f"{stats.worst_pct:+.1f}%", RED, False),
        ("أقصى تراجع", f"{drawdown:+.1f}%", RED, False),
        ("جلسات رابحة", str(green_days), GREEN, False),
        ("جلسات خاسرة", str(red_days), RED, False),
    ]
    tile_rows = (len(tiles) + 1) // 2
    channel_panel = (110 + len(channel_rows) * 60 + 24) if channel_rows else 0
    height = (
        226 + 274 + 300 + (tile_rows * 144 + 24) + 300
        + (110 + 3 * 60 + 24) + channel_panel + REPORT_TAIL_HEIGHT
    )

    with _stage("month"):
        img, draw = _canvas(height)
        label = f"{ARABIC_MONTHS[month.month - 1]} {month.year}"
        y = _header(draw, f"البيان الشهري — {label}")

        _panel(draw, (MARGIN, y, W - MARGIN, y + 250), outline=accent)
        _chip(draw, W / 2, y + 44, "حصيلة الشهر كما أُغلقت فعليًا", GOLD)
        draw.text((W / 2, y + 132), f"{total:+.1f}%", font=_font(104, bold=True),
                  fill=accent, anchor="mm")
        draw.text((W / 2, y + 202), _dollars(total), font=_font(40, bold=True),
                  fill=MUTED, anchor="mm")
        y += 274

        _rtl(draw, (W - MARGIN, y - 6), "مسار الشهر التراكمي", _font(30, bold=True), GOLD, "rm")
        span_labels = (
            (arabic_date(daily_returns[0][0]), arabic_date(daily_returns[-1][0]))
            if daily_returns
            else None
        )
        _curve(draw, (MARGIN, y + 16, W - MARGIN, y + 276), cumulative, span_labels)
        y += 300

        for index, (tile_label, value, color, rtl_value) in enumerate(tiles):
            column, row = index % 2, index // 2
            tile_w = (W - 2 * MARGIN - 20) / 2
            # RTL: the first tile of a pair belongs on the right
            left = MARGIN + (1 - column) * (tile_w + 20)
            top = y + row * 144
            _tile(draw, (int(left), int(top), int(left + tile_w), int(top + 124)),
                  tile_label, value, color, value_rtl=rtl_value)
        y += tile_rows * 144 + 24

        _rtl(draw, (W - MARGIN, y - 6), "أداء كل أسبوع", _font(30, bold=True), GOLD, "rm")
        _week_bars(draw, (MARGIN, y + 16, W - MARGIN, y + 276), weeks, labels)
        y += 300

        row_y, y = _titled_panel(draw, y, "الحصيلة — نموذج افتراضي 1000$ لكل طرح", 3)
        _row(draw, row_y, "إجمالي الأرباح", f"{_dollars(gross_win)}  ({gross_win:+.1f}%)", GREEN)
        _row(draw, row_y + 60, "إجمالي الخسائر",
             f"{_dollars(gross_loss)}  ({gross_loss:+.1f}%)", RED)
        _row(draw, row_y + 120, "الصافي", f"{_dollars(total)}  ({total:+.1f}%)", GOLD)

        if channel_rows:
            row_y, y = _titled_panel(
                draw, y, "طروحات نُشرت حية في القناة قبل نتيجتها", len(channel_rows)
            )
            for row in channel_rows:
                result = float(row.get("return_pct") or 0)
                _row(draw, row_y, str(row.get("label", "?")),
                     f"{result:+.1f}%", _result_color(result))
                row_y += 60

        _report_tail(draw, y + 10, height)
        return _png(img)


def render_weekly_report_card(stats, channel_rows: list[dict]) -> bytes:
    """The weekly scoreboard as a table, with the live-share proof section."""
    positive = stats.expectancy_pct > 0
    gross_win = stats.avg_win_pct * stats.wins
    gross_loss = stats.avg_loss_pct * stats.losses
    net = stats.expectancy_pct * stats.closed
    stat_rows: list[tuple[str, str, str]] = [
        ("إجمالي الطروحات المغلقة", str(stats.closed), TEXT),
        ("الرابحة", str(stats.wins), GREEN),
        ("الخاسرة", str(stats.losses), RED),
        ("نسبة الطروحات الرابحة", f"{stats.win_rate:.0f}%", GOLD),
        ("إجمالي الأرباح", f"{_dollars(gross_win)}  ({gross_win:+.1f}%)", GREEN),
        ("إجمالي الخسائر", f"{_dollars(gross_loss)}  ({gross_loss:+.1f}%)", RED),
        ("الصافي — نموذج 1000$ لكل طرح", f"{_dollars(net)}  ({net:+.1f}%)", GOLD),
        ("متوسط نتيجة الطرح", f"{stats.expectancy_pct:+.1f}%", GREEN if positive else RED),
        ("أفضل طرح", f"{stats.best_pct:+.1f}%", GREEN),
        ("أسوأ طرح", f"{stats.worst_pct:+.1f}%", RED),
    ]

    channel_panel = (110 + len(channel_rows) * 60 + 24) if channel_rows else 0
    height = 226 + (110 + len(stat_rows) * 60 + 24) + channel_panel + 110 + 60
    img, draw = _canvas(height)
    y = _header(draw, "التقرير الأسبوعي — الرابح والخاسر كما أُغلق فعليًا")

    row_y, y = _titled_panel(draw, y, "حصيلة الأسبوع", len(stat_rows))
    for label, value, fill in stat_rows:
        _row(draw, row_y, label, value, fill)
        row_y += 60

    if channel_rows:
        row_y, y = _titled_panel(
            draw, y, "طروحات نُشرت حية في القناة قبل نتيجتها", len(channel_rows)
        )
        for row in channel_rows:
            result = float(row.get("return_pct") or 0)
            fill = GREEN if result > 1 else (MUTED if result >= -1 else RED)
            _row(draw, row_y, str(row.get("label", "?")), f"{result:+.1f}%", fill)
            row_y += 60

    _report_tail(draw, y + 10, height)
    return _png(img)


# ---------------------------------------------------------------- watch
def render_watch_card(
    symbol: str,
    direction_hint: str,
    condition: str,
    confidence: int,
    ts: datetime,
    level: float | None = None,
) -> bytes:
    """The blue "under watch" card — a setup forming, no study posted yet.

    Discipline made visible: the card promises nothing, states the condition
    being waited for, and says out loud that no-entry is a valid outcome.
    """
    with _stage("watch"):
        img, draw = _canvas(1120)
        y = _header(draw, "رصد مبكر — فرصة قيد التكوين")

        _panel(draw, (MARGIN, y, W - MARGIN, y + 280), outline=BLUE)
        _chip(draw, W / 2, y + 50, "تحت المراقبة — ليس طرحًا بعد", BLUE)
        draw.text((W / 2, y + 144), symbol, font=_font(84, bold=True), fill=BLUE, anchor="mm")
        _rtl(
            draw, (W / 2, y + 228),
            f"الاتجاه المحتمل: {direction_hint}",
            _font(34, bold=True), TEXT, "mm",
        )
        y += 310

        rows = 2 if level is not None else 1
        row_y, y = _titled_panel(draw, y, "ماذا نراقب", rows, extra=96)
        _row(draw, row_y, "قوة الإشارة حتى الآن", f"{confidence}/10", TEXT)
        row_y += 60
        if level is not None:
            _row(draw, row_y, "مستوى المراقبة", f"{level:.2f}", BLUE)
            row_y += 60
        for line in _wrap(draw, f"الشرط المنتظر: {condition}", _font(28, bold=True),
                          W - 2 * MARGIN - 120)[:3]:
            _rtl(draw, (W / 2, row_y + 6), line, _font(28, bold=True), BLUE, "mm")
            row_y += 40

        y += 8
        for line in _wrap(
            draw,
            "إذا اكتمل الشرط يصدر طرح تعليمي كامل بتفاصيله — وإذا لم يكتمل فلن "
            "يصدر شيء، والانضباط أهم من الحماس",
            _font(28, bold=True), W - 2 * MARGIN - 60,
        )[:2]:
            _rtl(draw, (W / 2, y), line, _font(28, bold=True), MUTED, "mm")
            y += 42

        stamp = ts.astimezone(MARKET_TZ).strftime("%H:%M")
        _rtl(
            draw, (W / 2, 1120 - 78),
            f"محتوى تعليمي وليس توصية استثمارية — الخيارات عالية المخاطر والقرار مسؤوليتك • {stamp} نيويورك",
            _font(22), MUTED, "mm",
        )
        return _png(img)


# ---------------------------------------------------------------------------
# Self-test — proof, on every boot, that the cards actually render in Arabic.
# ---------------------------------------------------------------------------
def _sample_stats():
    """Stand-in period statistics for the self-test and the operator preview."""
    from qqq_alpha.live.review import ReviewStats

    return ReviewStats(
        closed=8, wins=5, losses=3, win_rate=62.5, expectancy_pct=8.9,
        avg_win_pct=23.0, avg_loss_pct=-14.8, best_pct=44.0, worst_pct=-21.0,
    )


def _glyph_is_missing(font: ImageFont.FreeTypeFont, char: str) -> bool:
    """True when the font has no glyph for this character.

    FreeType silently substitutes .notdef for anything a font does not cover —
    the empty box a subscriber reads as a broken product. Comparing a
    character's rendered bitmap against the bitmap of a codepoint no font maps
    detects that substitution directly, whatever shape .notdef happens to be
    in the current font. It tests the real drawing path rather than trusting a
    cmap table, which is the reason it will catch the next font regression too.
    """
    reference = font.getmask("", mode="L")  # private use area: never mapped
    candidate = font.getmask(char, mode="L")
    return (candidate.size, bytes(candidate)) == (reference.size, bytes(reference))


def self_test() -> tuple[bool, str]:
    """Render one of every card, then check every character actually drawn.

    Returns (ok, Arabic report). This is the automated version of the failure
    that shipped once already: a rebuild lost libraqm, the renderer fell back
    to presentation forms, the brand font was missing 17 of them, and cards
    went out as rows of tofu boxes. Nothing warned anyone — the operator saw it
    on their phone. Now the engine sees it first, at boot, and says so.
    """
    global _capture

    from datetime import date

    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.domain import Action, Decision, OptionType, Target, TradeUpdate
    from qqq_alpha.features.snapshot import SnapshotBuilder
    from qqq_alpha.trades import TradeManager

    problems: list[str] = []
    rendered = 0
    _capture = []
    try:
        bars = synthetic_session("QQQ", date(2026, 8, 14), seed=15)
        snapshot = SnapshotBuilder("QQQ").build(bars[:80])
        decision = Decision(
            ts=snapshot.ts,
            action=Action.ENTER,
            direction=OptionType.CALL,
            occ_symbol="O:QQQ260814C00580000",
            targets=[Target(label="T1", price=1.62, return_pct=50, take_pct=50)],
            stop_price=0.72, stop_return_pct=-40, invalidation_level=578.40,
            confidence=7, size_factor=0.5,
            thesis="ارتداد من دعم مع ابتلاع صاعد وحجم مؤكد",
            invalidation="كسر 578.40 يلغي الفكرة",
            risks=["تقلب قبل بيانات التضخم"],
        )
        manager = TradeManager()
        trade = manager.open_trade(decision, 1.08, snapshot)
        live = TradeUpdate(
            trade_id=trade.trade_id, ts=trade.opened_at, price=1.31,
            return_pct=21.3, note="status: open",
        )
        win = TradeUpdate(
            trade_id=trade.trade_id, ts=trade.opened_at, price=1.62,
            return_pct=50.0, note="closed:trail_stop (+50.0%)",
        )
        loss = TradeUpdate(
            trade_id=trade.trade_id, ts=trade.opened_at, price=0.72,
            return_pct=-33.3, note="closed:stop_hit (-33.3%)",
        )
        trade.exit_reason = "trail_stop"

        renders = [
            ("بطاقة الطرح", lambda: render_entry_card(trade, False)),
            ("البطاقة الحية", lambda: render_entry_card(trade, False, live=live)),
            ("بطاقة تأمين النصف", lambda: render_scale_out_card(trade, live)),
            ("بطاقة إغلاق رابح", lambda: render_close_card(trade, win)),
            ("بطاقة إغلاق خاسر", lambda: render_close_card(trade, loss)),
            ("بطاقة المراقبة", lambda: render_watch_card(
                "QQQ", "صعود CALL", "اختراق 580.10 بحجم", 7, trade.opened_at, level=578.40
            )),
            ("التقرير اليومي", lambda: render_daily_report_card(
                date(2026, 8, 14),
                [{"label": "QQQ 580 CALL", "return_pct": 50.0, "shared": True},
                 {"label": "QQQ 578 PUT", "return_pct": -33.3, "shared": False}],
            )),
            ("البيان الشهري", lambda: render_monthly_report_card(
                date(2026, 8, 1), _sample_stats(),
                [(date(2026, 8, 3 + i), value) for i, value in
                 enumerate([12.5, -8.0, 31.2, -15.4, 22.0, 5.5, -21.0, 44.0])],
                [{"label": "QQQ 580 CALL", "return_pct": 44.0}],
            )),
        ]
        for label, render in renders:
            try:
                png = render()
                if not png:
                    problems.append(f"{label}: خرجت فارغة")
                else:
                    rendered += 1
            except Exception as exc:  # noqa: BLE001 - collect, do not abort
                log.exception("card self-test failed on %s", label)
                problems.append(f"{label}: تعذّر الرسم ({exc})")

        vocabulary = "".join(_capture)
    finally:
        _capture = None

    # every character that reached the canvas, shaped exactly as the live draw
    # path shapes it, checked against the regular and bold faces we ship
    drawn = vocabulary if RAQM else _legacy_shape(vocabulary)
    missing: set[str] = set()
    for bold in (False, True):
        font = _font(40, bold=bold)
        for char in set(drawn):
            if char.isspace() or char in missing:
                continue
            if _glyph_is_missing(font, char):
                missing.add(char)
    if missing:
        codes = " ".join(f"U+{ord(c):04X}" for c in sorted(missing)[:8])
        problems.append(
            f"{len(missing)} حرفًا بلا شكل في الخط — ستظهر مربعات فارغة ({codes})"
        )

    engine = "libraqm" if RAQM else "الوضع الاحتياطي"
    family = _FAMILY[0].split("-")[0]
    if problems:
        return False, (
            "❌ فحص البطاقات فشل\n"
            f"الخط: {family} | التشكيل: {engine}\n" + "\n".join(f"• {p}" for p in problems)
        )
    return True, (
        f"✅ فحص البطاقات سليم — {rendered} بطاقة رُسمت، "
        f"و{len(set(drawn)) - 1} حرفًا عربيًا كلها لها أشكال\n"
        f"الخط: {family} | التشكيل: {engine}"
    )
