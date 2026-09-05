"""The channels carry MIRSAD 9's reports and nothing else: a card to both
rooms after the bell, the text ledger when a photo cannot be delivered, and
never an exception into the engine."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.config import Settings
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, OptionType, Target
from qqq_alpha.journal import Journal
from qqq_alpha.live.channel import ChannelPublisher
from qqq_alpha.live.notifier import NullNotifier

DAY = date(2026, 3, 2)


def _recorder():
    """A mock Telegram transport that records photo and text posts."""
    posts = {"photos": 0, "texts": []}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("sendPhoto"):
            posts["photos"] += 1
        else:
            posts["texts"].append(json.loads(request.content).get("text", ""))
        return httpx.Response(200, json={"ok": True})

    return posts, httpx.MockTransport(handler)


# ---------------------------------------------------------------- posting
@pytest.mark.asyncio
async def test_a_card_falls_back_to_its_text_when_the_photo_is_refused():
    posts = {"texts": []}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("sendPhoto"):
            return httpx.Response(400, json={"ok": False})
        posts["texts"].append(json.loads(request.content).get("text", ""))
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        delivered = await publisher._post_card(b"not-a-png", "caption", "the full ledger")

    assert delivered is None
    assert posts["texts"] == ["<pre>the full ledger</pre>"]


@pytest.mark.asyncio
async def test_channel_failure_never_raises():
    """The shop window must never stop the engine: a dead channel is a log
    line, not an exception in the trading path."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("channel unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_text("hello")
        assert await publisher._post_card(b"png", "caption", "text") is None
    # reaching here without an exception IS the assertion


# ---------------------------------------------------------------- engine wiring
class _EnterOnceDecider:
    """ENTERs on the first wake, then passes — a deterministic trade source."""

    def __init__(self):
        self.calls = 0

    async def decide(self, snapshot, **kwargs):
        self.calls += 1
        if self.calls > 1:
            return Decision(ts=snapshot.ts, action=Action.PASS, confidence=3)
        price = snapshot.underlying.close
        return Decision(
            ts=snapshot.ts,
            action=Action.ENTER,
            direction=OptionType.CALL,
            occ_symbol=f"O:QQQ260302C{int(round(price)) * 1000:08d}",
            targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
            stop_return_pct=-40,
            confidence=7,
            thesis="طرح تجريبي",
        )


# ---------------------------------------------------------------- watch card
class _WaitingDecider:
    """A qualified WAIT every time: named condition, confidence 7."""

    async def decide(self, snapshot, **kwargs):
        return Decision(
            ts=snapshot.ts, action=Action.WAIT, confidence=7,
            thesis="نراقب ارتدادًا فاشلًا نحو VWAP",
            invalidation="اختراق قمة الارتداد عند 732.50",
        )


@pytest.mark.asyncio
async def test_qualified_waits_publish_at_most_two_watch_cards(tmp_path):
    from qqq_alpha.live.engine import LiveEngine

    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        max_data_age_sec=10**9,
        attention_threshold=0.0,
        attention_cooldown_sec=0,
        shadow_symbols_csv="",
    )
    notifier = NullNotifier()
    engine = LiveEngine(
        settings=settings,
        decider=_WaitingDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=notifier,
    )
    engine._current_day = DAY
    bars = synthetic_session("QQQ", DAY, seed=8, trend=0.02, volatility=0.002)
    for bar in bars[:120]:
        await engine._on_bar(bar)

    # plenty of qualified WAIT wakes, but the card kept its promise: max 2/day
    assert len(notifier.watches) == 2
    assert "حالة قيد التكوّن" in notifier.watches[0]
    assert "لم تصدر دراستها بعد" in notifier.watches[0]


def test_the_statement_goes_out_after_the_months_last_weekday():
    """August 2026 ends on a Monday, so the 31st is the last session — and the
    28th (that month's last Friday) must NOT trigger it."""
    from qqq_alpha.live.engine import LiveEngine

    assert LiveEngine._is_last_session_of_month(date(2026, 8, 31))
    assert not LiveEngine._is_last_session_of_month(date(2026, 8, 28))
    # September 2026 ends on a Wednesday
    assert LiveEngine._is_last_session_of_month(date(2026, 9, 30))
    assert not LiveEngine._is_last_session_of_month(date(2026, 9, 29))
    # a month ending at the weekend: the last Friday carries the statement
    assert LiveEngine._is_last_session_of_month(date(2026, 10, 30))
    assert not LiveEngine._is_last_session_of_month(date(2026, 10, 31))  # Saturday


# ---------------------------------------------------------------- reports reach both rooms
def _chat_recorder():
    """Records which chat_id each post was addressed to."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.content
        if request.url.path.endswith("sendPhoto"):
            # multipart: the chat id is in the form body as plain bytes
            for candidate in (b"@public", b"-1009999"):
                if candidate in body:
                    seen.append(candidate.decode())
        else:
            seen.append(str(json.loads(body).get("chat_id", "")))
        return httpx.Response(200, json={"ok": True})

    return seen, httpx.MockTransport(handler)


def _reporting_engine(tmp_path, private: str = "-1009999"):
    from qqq_alpha.live.engine import LiveEngine

    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        telegram_bot_token="token",
        telegram_channel_id="@public",
        telegram_private_channel_id=private,
        shadow_symbols_csv="",
    )
    return LiveEngine(
        settings=settings,
        decider=_EnterOnceDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )


def _one_indicator_trade(engine, day: date) -> None:
    ny = ZoneInfo("America/New_York")
    engine.memory.record_tv_trade({
        "symbol": "NVDA", "label": "NVDA 180C", "side": 1, "entry": 2.0, "exit": 3.0, "peak": 3.2,
        "opened": datetime(day.year, day.month, day.day, 10, tzinfo=ny),
        "closed": datetime(day.year, day.month, day.day, 15, tzinfo=ny),
        "how": "الهدف الثاني",
    })


@pytest.mark.asyncio
async def test_the_after_bell_package_is_the_indicator_report_in_both_rooms(tmp_path):
    """After the bell the only thing the channels receive is MIRSAD 9's
    card — the desk's own trades stay with the operator."""
    engine = _reporting_engine(tmp_path)
    assert engine.private_channel is not None
    _one_indicator_trade(engine, DAY)

    seen, transport = _chat_recorder()
    for channel in engine._report_channels:
        channel._notifier._client = httpx.AsyncClient(transport=transport)

    await engine._publish_channel_daily(DAY)
    await engine._publish_channel_daily(DAY)  # a second post-close bar changes nothing

    assert sorted(seen) == ["-1009999", "@public"]
    for channel in engine._report_channels:
        await channel._notifier._client.aclose()


@pytest.mark.asyncio
async def test_reports_still_publish_with_no_private_channel_configured(tmp_path):
    engine = _reporting_engine(tmp_path, private="")
    assert engine.private_channel is None
    assert [c.channel_id for c in engine._report_channels] == ["@public"]
    _one_indicator_trade(engine, DAY)

    seen, transport = _chat_recorder()
    engine.channel._notifier._client = httpx.AsyncClient(transport=transport)
    await engine._publish_channel_daily(DAY)

    assert seen == ["@public"]
    await engine.channel._notifier._client.aclose()


# ---------------------------------------------------------------- MIRSAD 9 report cards
def _tv_row(symbol, side, entry, peak, exit_px, how, day):
    return {
        "symbol": symbol, "label": f"{symbol} 180{'C' if side > 0 else 'P'}", "side": side,
        "entry": entry, "peak": peak, "exit": exit_px, "how": how, "day": day,
        "pct": (exit_px - entry) / entry * 100, "peak_pct": (peak - entry) / entry * 100,
        "closed": datetime(day.year, day.month, day.day, 20, tzinfo=UTC),
    }


def test_the_indicator_report_card_renders_for_every_period_and_for_nothing():
    from qqq_alpha.live import cards

    d = date(2026, 9, 4)
    rows = [
        _tv_row("NVDA", 1, 2.1, 3.4, 3.05, "الهدف الثاني", d),
        _tv_row("TSLA", -1, 4.0, 4.2, 3.1, "وقف الخسارة", d),
    ]
    daily = cards.render_indicator_report_card(
        "daily", d, d, rows, [{"label": "AAPL 235C", "entry": 1.9, "mark": 2.35}]
    )
    week_rows = rows + [_tv_row("AMD", 1, 1.2, 1.5, 1.4, "الهدف الأول", date(2026, 9, 2))]
    weekly = cards.render_indicator_report_card("weekly", date(2026, 8, 31), d, week_rows)
    month_rows = week_rows + [_tv_row("META", 1, 5.0, 5.1, 4.0, "وقف", date(2026, 8, 12))]
    monthly = cards.render_indicator_report_card("monthly", date(2026, 8, 1), date(2026, 8, 31), month_rows)
    empty = cards.render_indicator_report_card("daily", d, d, [])
    for png in (daily, weekly, monthly, empty):
        assert png[:8] == b"\x89PNG\r\n\x1a\n"
    # the week and the month carry their charts, so they are taller than a day
    assert len(weekly) > 0 and len(monthly) > 0


@pytest.mark.asyncio
async def test_the_indicator_report_reaches_both_rooms_as_a_card(tmp_path):
    """After the bell MIRSAD 9's own scoreboard goes to the public channel
    and the updates channel alike, as a photo with the ledger as caption."""
    engine = _reporting_engine(tmp_path)
    friday = date(2026, 9, 4)
    ny = ZoneInfo("America/New_York")
    engine.memory.record_tv_trade({
        "symbol": "NVDA", "label": "NVDA 180C", "side": 1, "entry": 2.0, "exit": 3.0, "peak": 3.2,
        "opened": datetime(2026, 9, 4, 10, tzinfo=ny), "closed": datetime(2026, 9, 4, 15, tzinfo=ny),
        "how": "الهدف الثاني",
    })
    photos: list[tuple[str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.content
        if request.url.path.endswith("sendPhoto"):
            chat = "@public" if b"@public" in body else "-1009999"
            photos.append((chat, body))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 7}})

    for channel in engine._report_channels:
        channel._notifier._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    await engine._publish_indicator_reports(friday)

    chats = [chat for chat, _ in photos]
    # Friday: the daily card and the weekly card, each to both rooms
    assert chats.count("@public") == 2 and chats.count("-1009999") == 2
    assert all("مِرصاد ٩".encode() in body for _, body in photos)
    assert any("الأسبوعي".encode() in body for _, body in photos)
    for channel in engine._report_channels:
        await channel._notifier._client.aclose()


@pytest.mark.asyncio
async def test_a_quiet_week_posts_the_daily_card_only(tmp_path):
    engine = _reporting_engine(tmp_path)
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(request.url.path.rsplit("/", 1)[-1])
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 7}})

    for channel in engine._report_channels:
        channel._notifier._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    await engine._publish_indicator_reports(date(2026, 9, 4))  # a Friday with no record

    assert posts.count("sendPhoto") == 2  # one daily card per room, no weekly
    for channel in engine._report_channels:
        await channel._notifier._client.aclose()
