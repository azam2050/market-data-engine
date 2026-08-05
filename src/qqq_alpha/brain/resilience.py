"""Resilience around calls to the Anthropic API.

The SDK already retries connection errors and 5xx responses internally, but a
sustained overload window (HTTP 529, "overloaded_error") can outlast that —
this happened in production: the brain call failed the same way on two
consecutive container restarts. Two failure modes follow from that, and both
matter more than "retry harder":

1. During preflight, a fatal failure here stops the whole engine from
   starting, even though the key and model are fine and the outage is
   temporary. A short extra retry pass absorbs a blip that the SDK's own
   retries did not.
2. During live trading, an unhandled exception from a decision call used to
   propagate out of the engine's bar loop and crash the entire session for
   the rest of the day over one transient overload. That is a worse outcome
   than a single missed decision, so the caller gets an honest failure to
   turn into a safe PASS instead of an unhandled exception.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

log = logging.getLogger(__name__)

# two extra attempts beyond whatever the SDK already retried internally
DEFAULT_RETRY_DELAYS_SEC: tuple[float, ...] = (2.0, 5.0)


async def call_with_retry[T](
    fn: Callable[[], Awaitable[T]],
    label: str,
    delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS_SEC,
) -> T:
    """Run an async call, retrying on any failure after a short delay.

    Raises the last exception once every attempt is exhausted — this function
    never decides what a persistent failure means. That is fatal during
    preflight and a safe PASS during live trading, and only the caller knows
    which situation it is in.
    """
    last_exc: Exception | None = None
    for attempt, delay in enumerate((0.0, *delays)):
        if delay:
            await asyncio.sleep(delay)
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001 - every failure is retried the same way
            last_exc = exc
            log.warning("%s: attempt %d failed: %s", label, attempt + 1, exc)

    assert last_exc is not None  # the loop always runs at least once
    raise last_exc
