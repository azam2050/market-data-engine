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
