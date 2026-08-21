"""The public channel: live shares must be committed before their outcome,
chosen unpredictably, reported honestly, and never able to break the desk."""

from __future__ import annotations

import json
from datetime import date, timedelta

import httpx
import pytest

from qqq_alpha.brain.playbook import Playbook
from qqq_alpha.config import Settings
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.data.synthetic import synthetic_session
from qqq_alpha.domain import Action, Decision, OptionType, Target
from qqq_alpha.features.snapshot import SnapshotBuilder
from qqq_alpha.journal import Journal
from qqq_alpha.live.channel import (
    EDUCATION_SERIES,
    ChannelPublisher,
    share_days_for_week,
)
from qqq_alpha.live.notifier import NullNotifier
from qqq_alpha.trades import TradeManager

DAY = date(2026, 3, 2)


def _trade(shared: bool = False):
    bars = synthetic_session("QQQ", DAY, seed=21)
    snap = SnapshotBuilder("QQQ").build(bars[:80])
    decision = Decision(
        ts=snap.ts,
        action=Action.ENTER,
        direction=OptionType.CALL,
        occ_symbol="O:QQQ260302C00485000",
        targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40,
        confidence=7,
        thesis="طرح تجريبي",
    )
    trade = TradeManager().open_trade(decision, 1.00, snap)
    trade.shared_to_channel = shared
    return trade


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


# ---------------------------------------------------------------- scheduling
def test_share_days_are_two_stable_unpredictable_weekdays():
    days = share_days_for_week(DAY, salt="secret")
    assert len(days) == 2
    assert days <= set(range(5))
    # stable across the week (a restart keeps the same schedule)...
    for offset in range(5):
        assert share_days_for_week(DAY + timedelta(days=offset), "secret") == days
    # ...different salt (different bot token) → generally a different pick,
    # and a different week reshuffles
    weeks = {frozenset(share_days_for_week(DAY + timedelta(weeks=k), "secret")) for k in range(9)}
    assert len(weeks) > 1


# ---------------------------------------------------------------- posting
@pytest.mark.asyncio
async def test_live_share_posts_entry_card_and_close_card():
    posts, transport = _recorder()
    async with httpx.AsyncClient(transport=transport) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        trade = _trade(shared=True)
        await publisher.post_trade_entry(trade, delayed=False)

        manager = TradeManager()
        manager.open_trades = [trade]
        update = manager.update(trade, 0.55, trade.opened_at + timedelta(minutes=5))
        assert update is not None and "closed:stop_hit" in update.note
        await publisher.post_trade_update(trade, update, delayed=False)

    assert posts["photos"] == 2  # entry card + close card, red included


@pytest.mark.asyncio
async def test_no_trade_day_report_teaches_capital_preservation():
    posts, transport = _recorder()
    async with httpx.AsyncClient(transport=transport) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_daily_report(DAY, [])

    assert len(posts["texts"]) == 1
    text = posts["texts"][0]
    assert "لم نتداول اليوم" in text
    assert "حماية رأس المال" in text
    assert "ليس توصية استثمارية" in text


def _closed_pair():
    shared, private = _trade(shared=True), _trade()
    for trade in (shared, private):
        manager = TradeManager()
        manager.open_trades = [trade]
        manager.force_close(trade, 1.40, trade.opened_at + timedelta(minutes=30), "trail_stop")
    return shared, private


@pytest.mark.asyncio
async def test_daily_report_goes_out_as_a_table_card():
    posts, transport = _recorder()
    async with httpx.AsyncClient(transport=transport) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_daily_report(DAY, list(_closed_pair()))

    assert posts["photos"] == 1  # the branded table card
    assert posts["texts"] == []  # no duplicate text wall next to it


@pytest.mark.asyncio
async def test_daily_report_falls_back_to_tagged_text_when_photos_fail():
    posts = {"texts": []}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("sendPhoto"):
            return httpx.Response(400, json={"ok": False})
        posts["texts"].append(json.loads(request.content).get("text", ""))
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_daily_report(DAY, list(_closed_pair()))

    text = posts["texts"][0]
    assert text.count("🟢") == 2
    assert text.count("🔓") == 1  # only the live share carries the tag
    assert "كما أُغلقت فعليًا" in text


@pytest.mark.asyncio
async def test_education_series_cycles_without_repeating_adjacent_slots():
    captions: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        # the lesson now travels as a card; its title rides in the caption
        body = request.content.decode("utf-8", errors="ignore")
        captions.append(body)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        tuesday, thursday = date(2026, 3, 3), date(2026, 3, 5)
        await publisher.post_education(tuesday)
        await publisher.post_education(thursday)
        await publisher.post_education(tuesday + timedelta(weeks=1))

    titles = [
        next(
            (line for line in body.splitlines() if "سلسلة حماية رأس المال" in line),
            "",
        )
        for body in captions
    ]
    assert len(titles) == 3
    assert len(set(titles)) == 3  # three different lessons
    assert all(t for t in titles)
    assert len(EDUCATION_SERIES) >= 6


@pytest.mark.asyncio
async def test_channel_failure_never_raises():
    """The shop window must never stop the desk: a dead channel is a log
    line, not an exception in the trading path."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("channel unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_text("hello")
        await publisher.post_daily_report(DAY, [])
        await publisher.post_trade_entry(_trade(shared=True), delayed=False)
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


@pytest.mark.asyncio
async def test_first_trade_on_a_share_day_is_flagged_and_mirrored(tmp_path):
    from qqq_alpha.live.engine import LiveEngine

    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        telegram_bot_token="token",
        telegram_channel_id="@chan",
        max_data_age_sec=10**9,
        attention_threshold=0.0,
        attention_cooldown_sec=0,
        shadow_symbols_csv="",
    )
    engine = LiveEngine(
        settings=settings,
        decider=_EnterOnceDecider(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )
    assert engine.channel is not None

    posts, transport = _recorder()
    engine.channel._notifier._client = httpx.AsyncClient(transport=transport)
    # force every weekday to be a share day so the test is deterministic
    engine.channel.is_share_day = lambda day: True  # type: ignore[method-assign]
    # the model pricer has no live chain, so the contract-existence rail
    # would veto every entry; this test is about the channel wiring, not
    # contract validation
    from qqq_alpha.domain import RailVerdict

    engine.rails.post_check = lambda decision, contract: RailVerdict(allowed=True)  # type: ignore[method-assign]

    engine._current_day = DAY
    bars = synthetic_session("QQQ", DAY, seed=8, trend=0.02, volatility=0.002)
    for bar in bars[:240]:
        await engine._on_bar(bar)

    taken = [*engine.manager.open_trades, *engine.manager.closed_trades]
    assert taken, "the stub decider should have entered on its first wake"
    # exactly ONE trade is the day's live share, and it went to the channel
    assert sum(t.shared_to_channel for t in taken) == 1
    assert posts["photos"] >= 1
    await engine.channel._notifier._client.aclose()


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
    assert "تحت المراقبة" in notifier.watches[0]
    assert "ليس طرحًا بعد" in notifier.watches[0]


# ---------------------------------------------------------------- monthly
@pytest.mark.asyncio
async def test_monthly_statement_posts_as_a_card_with_the_drawdown():
    posts, transport = _recorder()
    from qqq_alpha.live.review import ReviewStats

    stats = ReviewStats(
        closed=8, wins=5, losses=3, win_rate=62.5, expectancy_pct=8.9,
        avg_win_pct=23.0, avg_loss_pct=-14.8, best_pct=44.0, worst_pct=-21.0,
    )
    series = [
        (date(2026, 8, 3) + timedelta(days=i), v)
        for i, v in enumerate([12.5, -8.0, 31.2, -15.4, 22.0, 5.5, -21.0, 44.0])
    ]
    async with httpx.AsyncClient(transport=transport) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_monthly_report(date(2026, 8, 1), stats, series, [])

    assert posts["photos"] == 1
    assert posts["texts"] == []


@pytest.mark.asyncio
async def test_an_empty_month_publishes_nothing():
    posts, transport = _recorder()
    from qqq_alpha.live.review import ReviewStats

    async with httpx.AsyncClient(transport=transport) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_monthly_report(date(2026, 8, 1), ReviewStats(), [], [])

    assert posts["photos"] == 0 and posts["texts"] == []


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


# ---------------------------------------------------------------- follow-ups
@pytest.mark.asyncio
async def test_a_reached_level_goes_out_as_a_card_not_a_line_of_text():
    """The moment a level we named in advance is hit is the most engaging beat
    in the lifecycle — it used to be the only one rendered as plain text,
    sandwiched between two cards."""
    from qqq_alpha.domain import TradeUpdate

    posts, transport = _recorder()
    trade = _trade(shared=True)
    update = TradeUpdate(
        trade_id=trade.trade_id, ts=trade.opened_at + timedelta(minutes=9),
        price=1.55, return_pct=55.0, note="target:T1 reached (+50%)",
    )
    async with httpx.AsyncClient(transport=transport) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_trade_update(trade, update, delayed=False)

    assert posts["photos"] == 1
    assert posts["texts"] == []


@pytest.mark.asyncio
async def test_the_education_series_goes_out_as_a_card():
    posts, transport = _recorder()
    async with httpx.AsyncClient(transport=transport) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_education(date(2026, 3, 3))

    assert posts["photos"] == 1
    assert posts["texts"] == []  # no wall of text beside the card


@pytest.mark.asyncio
async def test_education_falls_back_to_the_full_text_when_the_photo_fails():
    """The lesson is the content; losing its styling is acceptable, losing the
    lesson is not."""
    posts = {"texts": []}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("sendPhoto"):
            return httpx.Response(400, json={"ok": False})
        posts["texts"].append(json.loads(request.content).get("text", ""))
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        publisher = ChannelPublisher("token", "@chan", client=client)
        await publisher.post_education(date(2026, 3, 3))

    assert "سلسلة حماية رأس المال" in posts["texts"][0]
    assert "القاعدة" in posts["texts"][0]


def test_education_card_parses_title_body_and_rule():
    from io import BytesIO

    from PIL import Image

    from qqq_alpha.live.cards import render_education_card

    for lesson in EDUCATION_SERIES:
        png = render_education_card(lesson)
        assert png.startswith(b"\x89PNG")
        # each card sizes itself to its own text rather than clipping it
        assert Image.open(BytesIO(png)).height > 600


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


@pytest.mark.asyncio
async def test_the_daily_report_reaches_the_private_channel_too(tmp_path):
    """Subscribers were getting the live cards but not the scoreboard.

    The daily and weekly reports are the two posts that show the record
    honestly, losses included — the paying room was seeing less accounting
    than the free one.
    """
    engine = _reporting_engine(tmp_path)
    assert engine.private_channel is not None

    seen, transport = _chat_recorder()
    for channel in engine._report_channels:
        channel._notifier._client = httpx.AsyncClient(transport=transport)

    await engine._publish_channel_daily(DAY)

    assert "@public" in seen
    assert "-1009999" in seen
    for channel in engine._report_channels:
        await channel._notifier._client.aclose()


@pytest.mark.asyncio
async def test_the_weekly_scoreboard_reaches_both_rooms_on_friday(tmp_path):
    friday = date(2026, 3, 6)
    engine = _reporting_engine(tmp_path)
    stats_calls: list[str] = []

    for channel in engine._report_channels:
        chat = channel.channel_id

        async def _daily(day, closed, _chat=chat):
            return None

        async def _weekly(stats, rows, _chat=chat):
            stats_calls.append(_chat)

        channel.post_daily_report = _daily  # type: ignore[method-assign]
        channel.post_weekly_report = _weekly  # type: ignore[method-assign]

    await engine._publish_channel_daily(friday)

    assert sorted(stats_calls) == ["-1009999", "@public"]


@pytest.mark.asyncio
async def test_the_education_series_stays_public_only(tmp_path):
    """It is written to attract readers, not to inform paying ones."""
    tuesday = date(2026, 3, 3)
    engine = _reporting_engine(tmp_path)
    taught: list[str] = []

    for channel in engine._report_channels:
        chat = channel.channel_id

        async def _daily(day, closed, _chat=chat):
            return None

        async def _education(day, _chat=chat):
            taught.append(_chat)

        channel.post_daily_report = _daily  # type: ignore[method-assign]
        channel.post_education = _education  # type: ignore[method-assign]

    await engine._publish_channel_daily(tuesday)

    assert taught == ["@public"]


@pytest.mark.asyncio
async def test_reports_still_publish_with_no_private_channel_configured(tmp_path):
    engine = _reporting_engine(tmp_path, private="")
    assert engine.private_channel is None
    assert [c.channel_id for c in engine._report_channels] == ["@public"]

    posted: list[str] = []

    async def _daily(day, closed):
        posted.append("public")

    engine.channel.post_daily_report = _daily  # type: ignore[method-assign]
    await engine._publish_channel_daily(DAY)

    assert posted == ["public"]
