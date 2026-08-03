"""Telegram delivery.

Built for one recipient first — you, during the shadow period. The subscriber
bots come later and reuse this transport unchanged, which means the message
wording and the delivery path both get months of real use before anyone pays
for them.

Delivery is treated as unreliable by default: Telegram rate-limits, times out,
and occasionally rejects a message outright. A dropped signal during a live
session is a real loss, so sends are retried, and a permanent failure is logged
loudly rather than swallowed.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import logging

import httpx

from qqq_alpha.domain import Trade, TradeUpdate
from qqq_alpha.live.notifier import format_signal, format_update

log = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org"
MAX_MESSAGE_CHARS = 4000  # Telegram's limit is 4096; leave room for markup
MAX_ATTEMPTS = 4
BASE_RETRY_SEC = 1.5


class TelegramNotifier:
    """Sends signals to a Telegram chat."""

    def __init__(
        self,
        token: str,
        chat_id: str,
        client: httpx.AsyncClient | None = None,
        silent_notes: bool = True,
    ):
        if not token or not chat_id:
            raise ValueError("Telegram token and chat id are both required")
        self.token = token
        self.chat_id = chat_id
        self._client = client
        self._owns_client = client is None
        self.silent_notes = silent_notes
        self.failures = 0

    async def _post(self, text: str, silent: bool = False) -> bool:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)

        url = f"{TELEGRAM_API}/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "disable_notification": silent,
        }

        for attempt in range(MAX_ATTEMPTS):
            try:
                response = await self._client.post(url, json=payload)
                if response.status_code == 200:
                    return True

                if response.status_code == 429:
                    # honour Telegram's own backoff instruction
                    retry_after = 5
                    with contextlib.suppress(Exception):
                        retry_after = int(
                            response.json().get("parameters", {}).get("retry_after", 5)
                        )
                    log.warning("telegram rate limited; waiting %ss", retry_after)
                    await asyncio.sleep(retry_after)
                    continue

                log.error(
                    "telegram rejected message: %s %s",
                    response.status_code,
                    response.text[:200],
                )
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                log.warning("telegram send failed (%s), retrying", exc)

            await asyncio.sleep(BASE_RETRY_SEC * (2**attempt))

        self.failures += 1
        log.error("telegram send permanently failed after %d attempts", MAX_ATTEMPTS)
        return False

    @staticmethod
    def _chunks(text: str) -> list[str]:
        """Split on line boundaries so a trade plan is never cut mid-number."""
        if len(text) <= MAX_MESSAGE_CHARS:
            return [text]

        chunks: list[str] = []
        current: list[str] = []
        size = 0
        for line in text.split("\n"):
            if size + len(line) + 1 > MAX_MESSAGE_CHARS and current:
                chunks.append("\n".join(current))
                current, size = [], 0
            current.append(line)
            size += len(line) + 1
        if current:
            chunks.append("\n".join(current))
        return chunks

    async def _send(self, text: str, silent: bool = False) -> bool:
        ok = True
        for chunk in self._chunks(f"<pre>{html.escape(text)}</pre>"):
            ok = await self._post(chunk, silent=silent) and ok
        return ok

    # ------------------------------------------------------------------
    async def signal(self, trade: Trade, delayed: bool) -> None:
        await self._send(format_signal(trade, delayed), silent=False)

    async def update(self, trade: Trade, update: TradeUpdate, delayed: bool) -> None:
        # closes and target hits should buzz; routine heartbeats should not
        noteworthy = update.note.startswith(("closed:", "target:"))
        await self._send(format_update(trade, update, delayed), silent=not noteworthy)

    async def note(self, text: str) -> None:
        await self._send(text, silent=self.silent_notes)

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


class TelegramCommandListener:
    """Long-polls for messages from the operator so lessons can be approved
    from a phone — the engine already established that a terminal is not
    something to assume the operator has.

    Only messages from the configured chat are honoured. That is enough
    authorization here: this is a single-operator bot, not a public one, and
    the chat id was set up by the operator during the Railway walkthrough.
    """

    def __init__(self, token: str, chat_id: str, client: httpx.AsyncClient | None = None):
        self.token = token
        self.chat_id = str(chat_id)
        self._client = client
        self._owns_client = client is None
        self._offset = 0

    async def poll(self, timeout: int = 25) -> list[str]:
        """Block up to ``timeout`` seconds, return any new command texts."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=timeout + 10.0)

        url = f"{TELEGRAM_API}/bot{self.token}/getUpdates"
        try:
            response = await self._client.get(
                url, params={"offset": self._offset, "timeout": timeout}
            )
            response.raise_for_status()
        except (httpx.TransportError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            log.warning("telegram command poll failed (%s)", exc)
            await asyncio.sleep(2.0)  # do not spin on a persistent error
            return []

        commands: list[str] = []
        for update in response.json().get("result", []):
            self._offset = update["update_id"] + 1
            message = update.get("message") or {}
            chat = message.get("chat") or {}
            text = message.get("text")
            if text and str(chat.get("id")) == self.chat_id:
                commands.append(text.strip())
        return commands

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


class FanoutNotifier:
    """Sends to several destinations. One failing channel must not silence the rest."""

    def __init__(self, *notifiers: object):
        self.notifiers = [n for n in notifiers if n is not None]

    async def _fanout(self, method: str, *args: object) -> None:
        for notifier in self.notifiers:
            try:
                await getattr(notifier, method)(*args)
            except Exception:  # noqa: BLE001 - a broken channel must not stop the others
                log.exception("notifier %s failed on %s", type(notifier).__name__, method)

    async def signal(self, trade: Trade, delayed: bool) -> None:
        await self._fanout("signal", trade, delayed)

    async def update(self, trade: Trade, update: TradeUpdate, delayed: bool) -> None:
        await self._fanout("update", trade, update, delayed)

    async def note(self, text: str) -> None:
        await self._fanout("note", text)


async def verify_telegram(token: str, chat_id: str) -> tuple[bool, str]:
    """Confirm the bot can actually reach the chat before a session depends on it."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            me = await client.get(f"{TELEGRAM_API}/bot{token}/getMe")
            if me.status_code != 200:
                return False, f"invalid bot token ({me.status_code})"
            name = me.json().get("result", {}).get("username", "unknown")

            notifier = TelegramNotifier(token, chat_id, client=client)
            sent = await notifier._send("✅ QQQ Alpha متصل بنجاح — هذه رسالة اختبار")
            if not sent:
                return False, f"bot @{name} works, but cannot post to chat {chat_id}"
            return True, f"connected as @{name}"
        except httpx.HTTPError as exc:
            return False, f"network error: {exc}"
