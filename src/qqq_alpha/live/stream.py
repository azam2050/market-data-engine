"""Live minute-bar stream.

The provider pushes completed 1-minute aggregates over WebSocket. We consume
those directly rather than assembling bars from raw trades — the same principle
as the historical path, and the reason this pipeline stays predictable.

Everything here is written for a process that must survive a whole trading day
unattended: authentication is verified rather than assumed, reconnects back off
exponentially with jitter, and a silent socket is treated as a failure instead
of as calm.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import websockets

from qqq_alpha.config import Settings
from qqq_alpha.domain import Bar

log = logging.getLogger(__name__)

MAX_BACKOFF_SEC = 60.0
BASE_BACKOFF_SEC = 2.0
# a live feed that says nothing for this long during the session is broken,
# not quiet — QQQ prints every minute
SILENCE_TIMEOUT_SEC = 180


class StreamAuthError(RuntimeError):
    """Authentication was rejected. Retrying will not help — fail loudly."""


class LiveBarStream:
    """Yields completed minute bars for the symbols we track."""

    def __init__(self, settings: Settings, symbols: list[str] | None = None):
        self.settings = settings
        self.symbols = symbols or settings.tracked_symbols
        self.last_message_at: datetime | None = None
        self.connected = False
        self.reconnects = 0

    @property
    def is_delayed(self) -> bool:
        return self.settings.massive_feed_mode != "real_time"

    async def bars(self) -> AsyncIterator[Bar]:
        """Reconnecting stream of minute bars. Runs until cancelled."""
        attempt = 0

        while True:
            try:
                async for bar in self._session():
                    attempt = 0  # a healthy session resets the backoff
                    yield bar
            except StreamAuthError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.reconnects += 1
                # exponential backoff with jitter: a provider outage must not
                # turn every client into a synchronised thundering herd
                delay = min(BASE_BACKOFF_SEC * (2**attempt), MAX_BACKOFF_SEC)
                delay *= 0.5 + random.random()  # noqa: S311 - jitter, not crypto
                attempt += 1
                log.warning(
                    "stream dropped (%s); reconnect #%d in %.1fs", exc, self.reconnects, delay
                )
                await asyncio.sleep(delay)

    async def _session(self) -> AsyncIterator[Bar]:
        if not self.settings.massive_api_key:
            raise StreamAuthError("MASSIVE_API_KEY is not configured")

        url = self.settings.massive_ws_stocks_url
        subscription = ",".join(f"AM.{symbol}" for symbol in self.symbols)

        async with websockets.connect(
            url, ping_interval=20, ping_timeout=20, close_timeout=10, max_queue=512
        ) as socket:
            await self._authenticate(socket)
            await socket.send(json.dumps({"action": "subscribe", "params": subscription}))
            log.info("subscribed to %d symbols (%s feed)", len(self.symbols), self.settings.massive_feed_mode)

            self.connected = True
            self.last_message_at = datetime.now(UTC)

            while True:
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=SILENCE_TIMEOUT_SEC)
                except TimeoutError as exc:
                    raise ConnectionError(
                        f"no data for {SILENCE_TIMEOUT_SEC}s — feed presumed dead"
                    ) from exc

                self.last_message_at = datetime.now(UTC)
                for bar in self._parse(raw):
                    yield bar

    async def _authenticate(self, socket) -> None:
        """Verify auth actually succeeded instead of assuming it did.

        A wrong or expired key otherwise produces an infinite reconnect loop that
        looks like a network problem and can get the key rate-limited.
        """
        await socket.send(
            json.dumps({"action": "auth", "params": self.settings.massive_api_key})
        )

        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            raw = await asyncio.wait_for(socket.recv(), timeout=15)
            try:
                messages = json.loads(raw)
            except json.JSONDecodeError:
                continue

            for message in messages if isinstance(messages, list) else [messages]:
                if message.get("ev") != "status":
                    continue
                status = str(message.get("status", ""))
                detail = str(message.get("message", ""))

                if status == "auth_success":
                    log.info("authenticated: %s", detail)
                    return
                if status in ("auth_failed", "error"):
                    raise StreamAuthError(f"authentication rejected: {detail}")
                log.debug("status during auth: %s %s", status, detail)

        raise StreamAuthError("no auth_success received within 15s")

    def _parse(self, raw: str | bytes) -> list[Bar]:
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("undecodable frame dropped")
            return []

        if not isinstance(events, list):
            events = [events]

        bars: list[Bar] = []
        for event in events:
            if event.get("ev") == "status":
                log.info("feed status: %s", event.get("message"))
                continue
            if event.get("ev") != "AM":
                continue

            symbol = event.get("sym")
            if symbol not in self.symbols:
                continue

            # 's' is the aggregate window start; that is the bar's timestamp
            start_ms = event.get("s") or event.get("e")
            ts = (
                datetime.fromtimestamp(start_ms / 1000, tz=UTC)
                if start_ms
                else datetime.now(UTC)
            )

            try:
                bars.append(
                    Bar(
                        symbol=symbol,
                        ts=ts.replace(second=0, microsecond=0),
                        open=float(event["o"]),
                        high=float(event["h"]),
                        low=float(event["l"]),
                        close=float(event["c"]),
                        volume=int(event.get("v") or 0),
                        vwap=float(event["vw"]) if event.get("vw") is not None else None,
                        transactions=int(event["z"]) if event.get("z") is not None else None,
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                log.warning("malformed bar for %s dropped: %s", symbol, exc)

        return bars

    @property
    def seconds_since_last_message(self) -> float | None:
        if self.last_message_at is None:
            return None
        return (datetime.now(UTC) - self.last_message_at).total_seconds()


async def drain(stream: LiveBarStream, seconds: float) -> list[Bar]:
    """Collect bars for a fixed period. Used by the connection self-test."""
    collected: list[Bar] = []
    deadline = datetime.now(UTC) + timedelta(seconds=seconds)

    async def _collect() -> None:
        async for bar in stream.bars():
            collected.append(bar)
            if datetime.now(UTC) >= deadline:
                return

    with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
        await asyncio.wait_for(_collect(), timeout=seconds)
    return collected
