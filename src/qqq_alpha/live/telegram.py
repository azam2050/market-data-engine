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
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx

from qqq_alpha.domain import Trade, TradeUpdate
from qqq_alpha.live.notifier import DISCLAIMER, format_signal, format_update

if TYPE_CHECKING:
    from qqq_alpha.memory import Memory

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

    async def _post(self, text: str, silent: bool = False, chat_id: str | None = None) -> bool:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)

        url = f"{TELEGRAM_API}/bot{self.token}/sendMessage"
        payload = {
            "chat_id": chat_id or self.chat_id,
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

    async def _post_photo(
        self, png: bytes, caption: str = "", silent: bool = False, chat_id: str | None = None
    ) -> bool:
        """One photo message. Best-effort, single retry — if the photo cannot
        be delivered, the caller falls back to the text version of the signal."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)

        url = f"{TELEGRAM_API}/bot{self.token}/sendPhoto"
        for attempt in range(2):
            try:
                response = await self._client.post(
                    url,
                    data={
                        "chat_id": chat_id or self.chat_id,
                        "caption": caption[:1000],
                        "disable_notification": silent,
                    },
                    files={"photo": ("signal.png", png, "image/png")},
                )
                if response.status_code == 200:
                    return True
                log.warning(
                    "telegram rejected photo: %s %s", response.status_code, response.text[:200]
                )
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                log.warning("telegram photo send failed (%s)", exc)
            await asyncio.sleep(BASE_RETRY_SEC * (2**attempt))
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

    async def _send(
        self, text: str, silent: bool = False, chat_id: str | None = None
    ) -> bool:
        ok = True
        for chunk in self._chunks(f"<pre>{html.escape(text)}</pre>"):
            ok = await self._post(chunk, silent=silent, chat_id=chat_id) and ok
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


@dataclass
class InboundMessage:
    """One private message received by the bot, from anyone."""

    chat_id: str
    text: str
    username: str = ""
    first_name: str = ""


class TelegramCommandListener:
    """Long-polls the bot's inbox.

    Two kinds of sender arrive on the same stream: the operator (lesson
    approvals, from the configured chat) and would-be subscribers pressing
    /start. The listener reports both and the engine routes — authorization
    for operator commands is enforced there by chat id, never here.
    """

    def __init__(self, token: str, chat_id: str, client: httpx.AsyncClient | None = None):
        self.token = token
        self.chat_id = str(chat_id)
        self._client = client
        self._owns_client = client is None
        self._offset = 0
        self._webhook_cleared = False

    async def _claim_inbox(self) -> None:
        """Delete any webhook so getUpdates actually receives messages.

        Telegram delivers a bot's inbox to exactly one place: a webhook, or
        getUpdates — never both. If any past experiment or integration ever
        registered a webhook on this token, every getUpdates call 409s
        *forever*, silently: sending still works, receiving never does. This
        showed up in production as /start and operator replies vanishing into
        nowhere. Claiming the inbox at startup is idempotent and instant.
        """
        try:
            info = await self._client.get(f"{TELEGRAM_API}/bot{self.token}/getWebhookInfo")
            result = info.json().get("result")
            webhook_url = result.get("url", "") if isinstance(result, dict) else ""
            if webhook_url:
                log.warning("a webhook was hijacking the bot inbox (%s); removing it", webhook_url)
            await self._client.post(f"{TELEGRAM_API}/bot{self.token}/deleteWebhook")
            self._webhook_cleared = True
        except Exception as exc:  # noqa: BLE001 - claiming must never block polling
            log.warning("could not verify/clear the bot webhook (%s); will retry", exc)

    async def poll(self, timeout: int = 25) -> list[InboundMessage]:
        """Block up to ``timeout`` seconds, return any new inbound messages."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=timeout + 10.0)
        if not self._webhook_cleared:
            await self._claim_inbox()

        url = f"{TELEGRAM_API}/bot{self.token}/getUpdates"
        try:
            response = await self._client.get(
                url, params={"offset": self._offset, "timeout": timeout}
            )
            response.raise_for_status()
        except (httpx.TransportError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 409:
                # a webhook (or a second engine instance) took the inbox back
                log.error("getUpdates conflict (409): another consumer holds the inbox")
                self._webhook_cleared = False  # re-claim on the next pass
            else:
                log.warning("telegram command poll failed (%s)", exc)
            await asyncio.sleep(2.0)  # do not spin on a persistent error
            return []

        messages: list[InboundMessage] = []
        for update in response.json().get("result", []):
            self._offset = update["update_id"] + 1
            message = update.get("message") or {}
            chat = message.get("chat") or {}
            sender = message.get("from") or {}
            text = message.get("text")
            # only private chats: the bot has no business reading groups
            if text and chat.get("id") is not None and chat.get("type", "private") == "private":
                messages.append(
                    InboundMessage(
                        chat_id=str(chat["id"]),
                        text=text.strip(),
                        username=sender.get("username") or "",
                        first_name=sender.get("first_name") or "",
                    )
                )
        return messages

    async def send(self, chat_id: str, text: str) -> bool:
        """A direct reply to one chat — welcomes, trial status, farewells."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        url = f"{TELEGRAM_API}/bot{self.token}/sendMessage"
        try:
            response = await self._client.post(
                url,
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
            )
            if response.status_code != 200:
                # Telegram's refusal reason (blocked bot, bad chat id, …)
                # must land in the logs — a bare False hides the funnel break
                log.warning(
                    "telegram reply to %s rejected (%s): %s",
                    chat_id, response.status_code, response.text[:200]
                )
            return response.status_code == 200
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("telegram reply to %s failed (%s)", chat_id, exc)
            return False

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


class BroadcastNotifier(TelegramNotifier):
    """The operator's chat plus every active trial subscriber.

    Signals and trade updates fan out to the whole list; system notes
    (preflight, errors, daily reviews) stay operator-only — a subscriber pays
    for trades, not for plumbing. One failed recipient never blocks the rest.
    """

    def __init__(
        self,
        token: str,
        admin_chat_id: str,
        memory: Memory,
        client: httpx.AsyncClient | None = None,
        silent_notes: bool = True,
    ):
        super().__init__(token, admin_chat_id, client=client, silent_notes=silent_notes)
        self.memory = memory

    async def _broadcast(self, text: str, silent: bool, card: bytes | None = None) -> None:
        recipients = [
            self.chat_id,  # the operator, always first
            *(
                chat_id
                for chat_id in self.memory.active_subscriber_ids(datetime.now(UTC))
                if chat_id != self.chat_id
            ),
        ]
        for chat_id in recipients:
            try:
                delivered_card = False
                if card is not None:
                    delivered_card = await self._post_photo(card, silent=silent, chat_id=chat_id)
                # subscribers get the card alone — the operator asked for one
                # clean image per event, not image-plus-wall-of-text. The full
                # text still goes to the operator (their audit copy) and to any
                # recipient whose photo failed to deliver: a signal may lose its
                # styling, never its content.
                if chat_id == self.chat_id or not delivered_card:
                    await self._send(text, silent=silent, chat_id=chat_id)
            except Exception:  # noqa: BLE001 - one blocked user must not stop the list
                log.exception("broadcast to %s failed", chat_id)
            await asyncio.sleep(0.05)  # stay under Telegram's ~30 msg/s ceiling

    @staticmethod
    def _render_card(kind: str, trade: Trade, update: TradeUpdate | None, delayed: bool) -> bytes | None:
        """Best-effort card image. Any failure means text-only, never no-signal."""
        try:
            from qqq_alpha.live import cards

            if kind == "entry":
                return cards.render_entry_card(trade, delayed)
            if kind == "scale_out" and update is not None:
                return cards.render_scale_out_card(trade, update)
            if kind == "close" and update is not None:
                return cards.render_close_card(trade, update)
        except Exception:  # noqa: BLE001 - a drawing bug must never cost a signal
            log.exception("card rendering failed; sending text only")
        return None

    async def signal(self, trade: Trade, delayed: bool) -> None:
        card = self._render_card("entry", trade, None, delayed)
        await self._broadcast(format_signal(trade, delayed), silent=False, card=card)

    async def update(self, trade: Trade, update: TradeUpdate, delayed: bool) -> None:
        card: bytes | None = None
        if update.note.startswith("closed:"):
            card = self._render_card("close", trade, update, delayed)
        elif update.note.startswith("scale_out"):
            card = self._render_card("scale_out", trade, update, delayed)
        noteworthy = update.note.startswith(("closed:", "target:", "scale_out"))
        await self._broadcast(
            format_update(trade, update, delayed), silent=not noteworthy, card=card
        )


def welcome_message(trial_days: int) -> str:
    return (
        "أهلاً بك في QQQ Alpha 👋\n\n"
        f"تم تفعيل فترتك التجريبية المجانية لمدة {trial_days} يوماً.\n"
        "ستصلك توصيات الخيارات الحية على QQQ فور صدورها: العقد، الاتجاه، "
        "الأهداف، وقف الخسارة، وسبب الدخول كاملاً — ومتابعة كل صفقة حتى إغلاقها.\n\n"
        f"⚠️ {DISCLAIMER}"
    )


def trial_status_message(days_left: int) -> str:
    return (
        f"فترتك التجريبية فعّالة — المتبقي {max(days_left, 0)} يوماً.\n"
        "التوصيات تصلك تلقائياً فور صدورها، لا يلزمك أي إجراء."
    )


def farewell_message(channel_url: str) -> str:
    lines = [
        "انتهت فترتك التجريبية المجانية في QQQ Alpha — شكراً لمتابعتك معنا 🙏",
    ]
    if channel_url:
        lines.append(f"\nللاستمرار ومتابعة الجديد، انضم إلينا هنا:\n{channel_url}")
    return "\n".join(lines)


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

            # sending is only half the job: a webhook left behind by any past
            # integration silently starves getUpdates (409) so /start and
            # operator replies never arrive. Surface it — the engine clears
            # it at startup, but the operator deserves to know it was there.
            detail = f"connected as @{name}"
            info = await client.get(f"{TELEGRAM_API}/bot{token}/getWebhookInfo")
            if (info.json().get("result") or {}).get("url"):
                detail += " — ⚠️ كان فيه webhook يسرق رسائل البوت الواردة، سيُزال تلقائياً"
            return True, detail
        except httpx.HTTPError as exc:
            return False, f"network error: {exc}"
