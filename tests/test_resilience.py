"""Tests for the Anthropic API retry helper.

The point: a transient failure gets retried and, if it does not clear up,
surfaced honestly to the caller — never silently swallowed, never retried
forever.
"""

import pytest

from qqq_alpha.brain.resilience import DEFAULT_RETRY_DELAYS_SEC, call_with_retry


async def test_succeeds_immediately_without_retrying():
    calls = {"n": 0}

    async def _ok():
        calls["n"] += 1
        return "result"

    result = await call_with_retry(_ok, "test", delays=(0, 0))
    assert result == "result"
    assert calls["n"] == 1


async def test_recovers_after_a_transient_failure():
    calls = {"n": 0}

    async def _flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("overloaded")
        return "recovered"

    result = await call_with_retry(_flaky, "test", delays=(0, 0))
    assert result == "recovered"
    assert calls["n"] == 2


async def test_raises_the_last_exception_once_every_attempt_fails():
    calls = {"n": 0}

    async def _always_fails():
        calls["n"] += 1
        raise RuntimeError(f"failure {calls['n']}")

    with pytest.raises(RuntimeError, match="failure 3"):
        await call_with_retry(_always_fails, "test", delays=(0, 0))

    assert calls["n"] == 3  # one initial attempt + two retries


def test_default_delays_are_nonzero():
    """Sanity check on the production default — tests above override it to
    zero deliberately; this guards against that override becoming permanent."""
    assert all(d > 0 for d in DEFAULT_RETRY_DELAYS_SEC)


# ---------------------------------------------------------------- tape watchdog
@pytest.mark.asyncio
async def test_a_dead_feed_is_announced_once_and_recovery_too(tmp_path):
    """A silent engine and a healthy quiet one look identical from a phone.
    The watchdog is what separates them — and it speaks on the edges only."""
    from datetime import UTC, datetime, timedelta

    from qqq_alpha.brain.decider import HeuristicDecider
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.config import MARKET_TZ, Settings
    from qqq_alpha.data.pricing import BlackScholesPricer
    from qqq_alpha.journal import Journal
    from qqq_alpha.live.engine import LiveEngine
    from qqq_alpha.live.notifier import NullNotifier

    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        shadow_symbols_csv="",
    )
    engine = LiveEngine(
        settings=settings,
        decider=HeuristicDecider(settings),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )

    # Tuesday 11:00 ET, mid-session
    now = datetime(2026, 8, 18, 11, 0, tzinfo=MARKET_TZ)

    engine.status.last_bar_at = now.astimezone(UTC) - timedelta(seconds=45)
    await engine._tape_tick(now)
    assert engine.notifier.notes == []  # a healthy feed says nothing

    engine.status.last_bar_at = now.astimezone(UTC) - timedelta(minutes=9)
    await engine._tape_tick(now)
    await engine._tape_tick(now)  # still down: no repeat spam
    outage = [n for n in engine.notifier.notes if "انقطاع في بيانات السوق" in n]
    assert len(outage) == 1
    assert "9 دقيقة" in outage[0]

    engine.status.last_bar_at = now.astimezone(UTC) - timedelta(seconds=30)
    await engine._tape_tick(now)
    assert any("عادت بيانات السوق" in n for n in engine.notifier.notes)

    # outside market hours a quiet tape is normal, not an outage
    engine.notifier.notes.clear()
    evening = datetime(2026, 8, 18, 23, 0, tzinfo=MARKET_TZ)
    engine.status.last_bar_at = evening.astimezone(UTC) - timedelta(hours=6)
    await engine._tape_tick(evening)
    assert engine.notifier.notes == []
