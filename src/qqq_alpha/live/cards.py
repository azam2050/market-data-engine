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
bytes or raises, and the caller (the notifier) treats any raise as "no
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


def _footer(
    draw: ImageDraw.ImageDraw,
    when: datetime,
    delayed: bool,
    note: str = "",
    height: int = H,
) -> None:
    """``height`` for cards that size themselves to their content — pinning the
    footer to the module's default canvas height silently overlapped it with
    the last panel the moment a card grew."""
    y = height - 128
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
    y = _header(draw, "دراسات حالة تعليمية على عقود الخيارات")

    # the contract, big enough to read from across a room
    _panel(draw, (MARGIN, y, W - MARGIN, y + 280), outline=accent)
    if live is None:
        _chip(draw, W / 2, y + 50, "دراسة حالة جديدة", GOLD)
    else:
        elapsed = int((live.ts - trade.opened_at).total_seconds() // 60)
        _chip(
            draw, W / 2, y + 50,
            f"مجريات الحالة — الآن {live.return_pct:+.1f}% • {elapsed} دقيقة",
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
        ("سعر الدخول", f"${trade.entry_price:.2f}", BLUE, False)
    ]
    if decision.stop_price is not None:
        rows.append(
            ("وقف الحماية", f"${decision.stop_price:.2f} ({decision.stop_return_pct:+.0f}%)", RED, False)
        )
    if decision.invalidation_level is not None:
        rows.append(("وقف الفكرة - على السهم", f"{decision.invalidation_level:.2f}", RED, False))
    rows.append(("نموذج إدارة رأس المال", size_label(decision.size_factor), GOLD, True))
    rows.append(("الثقة", f"{decision.confidence}/10", TEXT, False))

    row_y, y = _titled_panel(draw, y, "تفاصيل الحالة", len(rows))
    for label, value, fill, value_rtl in rows:
        _row(draw, row_y, label, value, fill, value_rtl)
        row_y += 60

    # follow-up levels, plus the management line that makes this desk different
    targets = decision.targets[:4]
    row_y, y = _titled_panel(draw, y, "محطات الدراسة", len(targets), extra=90)
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
    y = _header(draw, "لحظة مفصلية: تأمين التكلفة")

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
        "الدرس: من هذه اللحظة لا يمكن لهذه الحالة أن تخسر — النصف الباقي يطارد الامتداد",
        _font(30, bold=True),
        W - 2 * MARGIN - 60,
    )[:2]:
        _rtl(draw, (W / 2, y), line, _font(30, bold=True), TEXT, "mm")
        y += 44

    _footer(draw, update.ts, delayed=False)
    return _png(img)


def render_update_card(trade: Trade, update: TradeUpdate) -> bytes:
    """A watch level was reached while the study is still open.

    This moment used to go out as a line of text between two cards — the one
    beat in the whole lifecycle where a follower is most engaged (the level we
    named in advance just got hit) and it looked like a log entry. It is the
    same event as any other stage, so it gets the same card.
    """
    targets = trade.decision.targets[:3]
    note = (
        f"مضى على الحالة {int((update.ts - trade.opened_at).total_seconds() // 60)} دقيقة "
        "— لم يُغلق بعد. الإدارة الآلية مستمرة: النصف مؤمَّن والباقي بوقف متحرك من القمة"
    )
    probe = ImageDraw.Draw(Image.new("RGB", (10, 10)))
    note_lines = _wrap(probe, note, _font(28, bold=True), W - 2 * MARGIN - 60)[:2]
    # sized to its content: the targets panel is optional and the note wraps,
    # so a fixed canvas would either clip them or leave a hole
    height = (
        226 + 360 + (84 + 4 * 60 + 24)
        + ((84 + len(targets) * 60 + 24) if targets else 0)
        + len(note_lines) * 42 + 190
    )

    with _stage("live"):
        img, draw = _canvas(height)
        y = _header(draw, "مجريات الحالة: محطة تحققت")

        _panel(draw, (MARGIN, y, W - MARGIN, y + 330), outline=GREEN)
        _rtl(draw, (W / 2, y + 78), "الحالة ما زالت مفتوحة", _font(58, bold=True), GREEN, "mm")
        draw.text(
            (W / 2, y + 180), f"{update.return_pct:+.1f}%", font=_font(96, bold=True),
            fill=GREEN if update.return_pct > 0 else RED, anchor="mm",
        )
        # the note carries which level, e.g. "target:T1 reached (+50%)"
        label = update.note.split(":", 1)[-1].split(" reached")[0].strip() or "T1"
        _chip(draw, W / 2, y + 272, f"المحطة {label} تحققت", GOLD)
        y += 360

        row_y, y = _titled_panel(draw, y, "التفاصيل", 4)
        _row(draw, row_y, "العقد", human_contract(trade.occ_symbol, trade.opened_at), TEXT)
        _row(draw, row_y + 60, "سعر الدخول", f"${trade.entry_price:.2f}", MUTED)
        _row(draw, row_y + 120, "السعر الآن", f"${update.price:.2f}", GREEN)
        _row(draw, row_y + 180, "أقصى ربح وصله", f"{trade.max_favorable_pct:+.1f}%", GOLD)

        # what a follower actually wants while a study is still running: which
        # levels are already behind us and which are still ahead
        if targets:
            row_y, y = _titled_panel(draw, y, "محطات الدراسة", len(targets))
            for index, target in enumerate(targets, start=1):
                done = trade.max_favorable_pct >= target.return_pct
                _row(
                    draw, row_y,
                    f"المستوى {index} (+{target.return_pct:.0f}%)",
                    "تحقق" if done else "لم يتحقق",
                    GREEN if done else MUTED,
                    value_rtl=True,
                )
                row_y += 60
        y += 16
        for line in note_lines:
            _rtl(draw, (W / 2, y), line, _font(28, bold=True), TEXT, "mm")
            y += 42

        _footer(draw, update.ts, delayed=False, height=height)
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
    verdict = "خلاصة رابحة" if win else ("تعادل" if flat else "خلاصة خاسرة")

    img, draw = _canvas()
    y = _header(draw, "خلاصة الحالة — كما هي، ربحا أو خسارة")

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
        row_y, y = _titled_panel(draw, y, "محطات الدراسة", len(targets))
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
DISCLAIMER_LINE = "محتوى تعليمي وليس توصية استثمارية — الخيارات عالية المخاطر والقرار مسؤوليتك"


ARABIC_MONTHS = (
    "يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو",
    "يوليو", "أغسطس", "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر",
)
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
# The parts MIRSAD 9's report card is built from: a split bar, a KPI tile,
# a cumulative curve and a column per period.
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


# ---------------------------------------------------------------- MIRSAD 9 reports
# The indicator's scoreboard. One card family for the day, the week and the
# month, so a reader learns its shape once: the net at the top, how many and
# how well underneath, the ledger table, and — for a week or a month — the
# bars that show which sessions carried the number.
_IND_THEME = ("#070B17", "#0F1526", "#0C1222", "#3A4A7A")
_STAGE_THEMES["mirsad"] = _IND_THEME
_REPORT_KIND_AR = {"daily": "اليومي", "weekly": "الأسبوعي", "monthly": "الشهري"}
_SIDE_AR = {1: "كول", -1: "بوت"}


def _indicator_header(draw: ImageDraw.ImageDraw, kind: str, span: str) -> int:
    _panel(draw, (MARGIN, 56, W - MARGIN, 208))
    _rtl(draw, (W / 2, 108), "مِرصاد ٩", _font(58, bold=True), GOLD, "mm")
    _rtl(draw, (W / 2, 158), f"تقرير الأداء {_REPORT_KIND_AR.get(kind, kind)} — {span}",
         _font(30), TEXT, "mm")
    _rtl(draw, (W / 2, 190), "إشارات المؤشر كما وقعت، على عقود حقيقية من السوق", _font(22), MUTED, "mm")
    return 238


def _fit(draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> str:
    """Trim a cell to its column, with an ellipsis, so no row ever overruns."""
    if _rtl_length(draw, text, font) <= max_width:
        return text
    while text and _rtl_length(draw, text + "…", font) > max_width:
        text = text[:-1]
    return text.rstrip() + "…"


# ledger columns, from the right edge inwards: the contract, then the numbers,
# then the reason hugging the left edge
_COL_LABEL_R = W - MARGIN - 28
_COL_ENTRY, _COL_PEAK, _COL_EXIT, _COL_RESULT = 700, 600, 500, 388
_COL_REASON_L = MARGIN + 28
_REASON_W = 220
_LEDGER_ROW = 56


def _ledger_header(draw: ImageDraw.ImageDraw, y: int) -> None:
    font = _font(24, bold=True)
    _rtl(draw, (_COL_LABEL_R, y), "العقد", font, MUTED, "rm")
    for x, title in ((_COL_ENTRY, "الدخول"), (_COL_PEAK, "الأعلى"), (_COL_EXIT, "الخروج"),
                     (_COL_RESULT, "النتيجة")):
        _rtl(draw, (x, y), title, font, MUTED, "mm")
    _rtl(draw, (_COL_REASON_L, y), "السبب", font, MUTED, "lm")


def _ledger_row(draw: ImageDraw.ImageDraw, y: int, row: dict, with_day: bool) -> None:
    left, right = MARGIN + 16, W - MARGIN - 16
    draw.rounded_rectangle([left, y - 24, right, y + 24], radius=10, fill=BG, outline=GRID)
    pct = float(row["pct"])
    color = _result_color(pct)
    side = _SIDE_AR.get(int(row.get("side") or 0), "")
    label = f"{row['symbol']} {side} {row['label']}"
    if with_day and row.get("day") is not None:
        label = f"{row['day'].day}/{row['day'].month} · " + label
    _rtl(draw, (_COL_LABEL_R, y), _fit(draw, label, _font(26, bold=True), 250), _font(26, bold=True), TEXT, "rm")
    num = _font(26)
    draw.text((_COL_ENTRY, y), f"{row['entry']:.2f}", font=num, fill=TEXT, anchor="mm")
    draw.text((_COL_PEAK, y), f"{row['peak']:.2f}", font=num, fill=GREEN if row["peak"] > row["entry"] else TEXT, anchor="mm")
    draw.text((_COL_EXIT, y), f"{row['exit']:.2f}", font=num, fill=TEXT, anchor="mm")
    draw.text((_COL_RESULT, y), f"{pct:+.0f}%", font=_font(28, bold=True), fill=color, anchor="mm")
    reason = str(row.get("how") or row.get("reason") or "")
    _rtl(draw, (_COL_REASON_L, y), _fit(draw, reason, _font(22), _REASON_W), _font(22), MUTED, "lm")


def render_indicator_report_card(
    kind: str,
    since,
    until,
    rows: list[dict],
    open_rows: list[dict] | None = None,
) -> bytes:
    """MIRSAD 9's report card for a day, a week or a month.

    ``rows`` are the persisted closed contracts (symbol, label, side, entry,
    peak, exit, pct, peak_pct, how, day). A daily card lists every row and
    what is still open; a weekly card adds a bar per session; a monthly card
    adds the cumulative curve and a bar per week, and lists the best and the
    worst rather than everything.
    """
    kind = kind if kind in _REPORT_KIND_AR else "daily"
    open_rows = open_rows or []
    pcts = [float(r["pct"]) for r in rows]
    total = sum(pcts)
    wins = sum(1 for p in pcts if p > 0)
    losses = len(pcts) - wins
    hit = round(wins / len(pcts) * 100) if pcts else 0
    accent = _result_color(total) if pcts else MUTED
    span = arabic_date(until) if since == until else f"من {arabic_date(since)} إلى {arabic_date(until)}"

    if kind == "daily":
        listed = list(rows)
    elif kind == "weekly":
        listed = sorted(rows, key=lambda r: (r.get("day"), r.get("closed")))[-14:]
    else:
        ranked = sorted(rows, key=lambda r: float(r["pct"]), reverse=True)
        listed = ranked[:5] + [r for r in ranked[-3:] if r not in ranked[:5]]
    chart_h = 0 if kind == "daily" else 300
    ledger_h = 100 + max(len(listed), 1) * _LEDGER_ROW + 30
    open_h = (76 + len(open_rows) * 56 + 8) if (kind == "daily" and open_rows) else 0
    height = 238 + 250 + 150 + chart_h + ledger_h + open_h + 24 + 190

    with _stage("month" if kind == "monthly" else "mirsad"):
        img, draw = _canvas(height)
        y = _indicator_header(draw, kind, span)

        # hero: the net across every closed contract, the split drawn to scale
        _panel(draw, (MARGIN, y, W - MARGIN, y + 226), outline=accent)
        _chip(draw, W / 2, y + 42, "مجموع نتائج العقود", GOLD)
        draw.text((W / 2, y + 122), f"{total:+.1f}%" if pcts else "—", font=_font(92, bold=True),
                  fill=accent, anchor="mm")
        _split_bar(draw, (MARGIN + 40, y + 180, W - MARGIN - 40, y + 206),
                   sum(p for p in pcts if p > 0), sum(p for p in pcts if p < 0))
        y += 250

        tile_w = (W - 2 * MARGIN - 3 * 16) / 4
        for index, (label, value, color) in enumerate((
            ("الصفقات", str(len(pcts)), TEXT),
            ("رابحة", str(wins), GREEN),
            ("خاسرة", str(losses), RED),
            ("نسبة النجاح", f"{hit}%", accent),
        )):
            left = MARGIN + index * (tile_w + 16)
            _tile(draw, (int(left), y, int(left + tile_w), y + 124), label, value, color)
        y += 150

        if kind == "weekly":
            by_day: dict = {}
            for r in rows:
                by_day[r["day"]] = by_day.get(r["day"], 0.0) + float(r["pct"])
            days = sorted(by_day)
            _week_bars(draw, (MARGIN, y, W - MARGIN, y + 276), [by_day[d] for d in days],
                       [f"{d.day}/{d.month}" for d in days])
            y += chart_h
        elif kind == "monthly":
            by_day = {}
            for r in rows:
                by_day[r["day"]] = by_day.get(r["day"], 0.0) + float(r["pct"])
            days = sorted(by_day)
            running, series = 0.0, []
            for d in days:
                running += by_day[d]
                series.append(running)
            half = (W - 2 * MARGIN - 16) / 2
            _curve(draw, (MARGIN, y, int(MARGIN + half), y + 276), series,
                   (f"{days[0].day}/{days[0].month}", f"{days[-1].day}/{days[-1].month}") if days else None)
            by_week: dict = {}
            for d in days:
                by_week[d.isocalendar()[1]] = by_week.get(d.isocalendar()[1], 0.0) + by_day[d]
            weeks = sorted(by_week)
            _week_bars(draw, (int(W - MARGIN - half), y, W - MARGIN, y + 276),
                       [by_week[w] for w in weeks], [f"أسبوع {i + 1}" for i in range(len(weeks))])
            y += chart_h

        title = {"daily": "صفقات اليوم", "weekly": "صفقات الأسبوع",
                 "monthly": "الأفضل والأسوأ في الشهر"}[kind]
        _panel(draw, (MARGIN, y, W - MARGIN, y + ledger_h))
        _rtl(draw, (W - MARGIN - 28, y + 40), title, _font(30, bold=True), GOLD, "rm")
        if len(listed) < len(rows):
            _rtl(draw, (MARGIN + 28, y + 40), f"{len(listed)} من {len(rows)}", _font(24), MUTED, "lm")
        row_y = y + 84
        _ledger_header(draw, row_y)
        row_y += 44
        if listed:
            for r in listed:
                _ledger_row(draw, row_y, r, with_day=kind != "daily")
                row_y += _LEDGER_ROW
        else:
            _rtl(draw, (W / 2, row_y + 4), "لا صفقات مقفلة في هذه الفترة", _font(26), MUTED, "mm")
        y += ledger_h + 24

        if open_h:
            _panel(draw, (MARGIN, y, W - MARGIN, y + open_h))
            _rtl(draw, (W - MARGIN - 28, y + 40), "ما زال مفتوحاً", _font(30, bold=True), GOLD, "rm")
            oy = y + 90
            for t in open_rows:
                mark, entry = t.get("mark"), float(t.get("entry") or 0.0)
                now = f"{mark:.2f}$  ({(mark - entry) / entry * 100:+.0f}%)" if mark and entry else "—"
                _row(draw, oy, str(t["label"]) + f" · دخول {entry:.2f}$", now,
                     _result_color((mark - entry) / entry * 100) if mark and entry else MUTED)
                oy += 56
            y += open_h + 24

        foot = height - 150
        _rtl(draw, (W / 2, foot), "أسعار العقود من بيانات السوق وقت الإشارة — استرشادية، وقد تختلف عن تنفيذك",
             _font(24, bold=True), GOLD, "mm")
        _rtl(draw, (W / 2, foot + 42), "الإشارات على الشارت لحظية · النتائج كما أُغلقت فعلياً بلا انتقاء",
             _font(22), MUTED, "mm")
        _rtl(draw, (W / 2, foot + 80), DISCLAIMER_LINE, _font(22), MUTED, "mm")
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
        _chip(draw, W / 2, y + 50, "حالة قيد التكوّن — لم تصدر دراستها بعد", BLUE)
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
            "إذا اكتمل الشرط تصدر دراسة الحالة كاملة بتفاصيلها — وإذا لم يكتمل فلن "
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
        target = TradeUpdate(
            trade_id=trade.trade_id, ts=trade.opened_at, price=1.62,
            return_pct=50.0, note="target:T1 reached (+50%)",
        )
        loss = TradeUpdate(
            trade_id=trade.trade_id, ts=trade.opened_at, price=0.72,
            return_pct=-33.3, note="closed:stop_hit (-33.3%)",
        )
        trade.exit_reason = "trail_stop"

        renders = [
            ("بطاقة دراسة الحالة", lambda: render_entry_card(trade, False)),
            ("بطاقة المجريات", lambda: render_entry_card(trade, False, live=live)),
            ("بطاقة تأمين النصف", lambda: render_scale_out_card(trade, live)),
            ("بطاقة المحطات", lambda: render_update_card(trade, target)),
            ("بطاقة إغلاق رابح", lambda: render_close_card(trade, win)),
            ("بطاقة إغلاق خاسر", lambda: render_close_card(trade, loss)),
            ("بطاقة قيد التكوّن", lambda: render_watch_card(
                "QQQ", "صعود CALL", "اختراق 580.10 بحجم", 7, trade.opened_at, level=578.40
            )),
            ("تقرير مِرصاد ٩", lambda: render_indicator_report_card(
                "daily", date(2026, 8, 14), date(2026, 8, 14),
                [{"symbol": "NVDA", "label": "NVDA 180C", "side": 1, "entry": 2.1,
                  "peak": 3.4, "exit": 3.05, "pct": 45.2, "peak_pct": 61.9,
                  "how": "الهدف الثاني", "day": date(2026, 8, 14)}],
                [{"label": "AAPL 235C", "entry": 1.9, "mark": 2.35}],
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
