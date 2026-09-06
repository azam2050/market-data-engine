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
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

from qqq_alpha.domain import Trade, TradeUpdate
from qqq_alpha.live.notifier import format_signal, format_update

if TYPE_CHECKING:
    pass

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
    ) -> int | None:
        """One photo message. Best-effort, single retry — if the photo cannot
        be delivered, the caller falls back to the text version of the signal.

        Returns the posted message id (or a truthy sentinel when Telegram
        omits it), so callers can later edit the card in place — that is how
        the entry card becomes a live status board. None means not delivered.
        """
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
                    result = {}
                    with contextlib.suppress(Exception):
                        result = response.json().get("result") or {}
                    return int(result.get("message_id") or -1)
                log.warning(
                    "telegram rejected photo: %s %s", response.status_code, response.text[:200]
                )
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                log.warning("telegram photo send failed (%s)", exc)
            await asyncio.sleep(BASE_RETRY_SEC * (2**attempt))
        return None

    # every admin right this desk actually uses, and the feature that breaks
    # without it. Checking only can_post_messages would let the operator fix
    # "posting", see a green tick, and then discover weeks later that the
    # living card never refreshed and expired subscribers were never removed.
    CHANNEL_RIGHTS: tuple[tuple[str, str], ...] = (
        ("can_post_messages", "نشر الرسائل (البطاقات نفسها)"),
        ("can_edit_messages", "تعديل الرسائل (تحديث البطاقة الحية)"),
        ("can_invite_users", "دعوة المستخدمين (روابط الاشتراك وقبول الطلبات)"),
        ("can_restrict_members", "حظر المستخدمين (إخراج المنتهية اشتراكاتهم)"),
    )

    async def check_channel(self, chat_id: str, full_rights: bool = True) -> str:
        """Ask Telegram whether we can actually publish in a channel.

        Cards going to the operator's private chat instead of the subscribers'
        channel is indistinguishable, from the outside, from cards going
        nowhere: both look like "the bot messaged me". The operator had no way
        to tell which was happening. This asks Telegram directly — does the
        chat exist under this id, is the bot an admin, and does it hold every
        right the desk depends on — and returns one Arabic line for their
        phone. ``full_rights`` is False for the public channel, which only
        ever needs to post.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)

        async def call(method: str, payload: dict) -> dict | None:
            try:
                response = await self._client.post(
                    f"{TELEGRAM_API}/bot{self.token}/{method}", json=payload
                )
                if response.status_code == 200:
                    return response.json().get("result") or {}
                log.warning("%s failed: %s %s", method, response.status_code, response.text[:200])
                with contextlib.suppress(Exception):
                    return {"_error": response.json().get("description", "")}
                return None
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                log.warning("%s failed (%s)", method, exc)
                return None

        # a channel id is a negative number beginning -100. Dropping the minus
        # sign when copying it is the single easiest mistake to make, and
        # Telegram's answer for the resulting positive number ("chat not
        # found") says nothing about the cause — so check the shape first and
        # name the typo outright.
        shape = ""
        if not chat_id.startswith("@"):
            if not chat_id.startswith("-"):
                shape = "الشرطة ناقصة في بداية المعرّف — معرّفات القنوات سالبة دائمًا"
            elif not chat_id.startswith("-100"):
                shape = "معرّف القناة يجب أن يبدأ بـ -100"

        chat = await call("getChat", {"chat_id": chat_id})
        if not chat or "_error" in (chat or {}):
            reason = (chat or {}).get("_error", "لا استجابة من تلجرام")
            if shape:
                return f"❌ {chat_id} — {shape} (رد تلجرام: {reason})"
            return f"❌ {chat_id} — القناة غير متاحة للبوت ({reason})"

        title = chat.get("title") or chat.get("username") or chat_id
        me = await call("getMe", {})
        bot_id = (me or {}).get("id")
        if not bot_id:
            return f"⚠️ {title} — القناة موجودة، لكن تعذّر التحقق من صلاحيات البوت"

        member = await call("getChatMember", {"chat_id": chat_id, "user_id": bot_id})
        status = (member or {}).get("status", "")
        if status not in {"administrator", "creator"}:
            if status == "member":
                return (
                    f"⚠️ {title} — البوت عضو وليس مشرفًا. النشر في القنوات يتطلب "
                    "صلاحية مشرف: افتح القناة ← اسم القناة ← المشرفون ← إضافة مشرف "
                    "← اختر البوت"
                )
            return f"❌ {title} — البوت ليس داخل القناة (الحالة: {status or 'غير معروفة'})"

        required = self.CHANNEL_RIGHTS if full_rights else self.CHANNEL_RIGHTS[:1]
        # a creator holds everything implicitly; for an administrator Telegram
        # omits a right it did not grant, so absent is treated as absent — not
        # as a permissive default, which is how the old check passed a bot that
        # could not post
        missing = [
            label
            for key, label in required
            if status == "administrator" and not (member or {}).get(key, False)
        ]
        if missing:
            return (
                f"❌ {title} — البوت مشرف لكن تنقصه صلاحيات:\n"
                + "\n".join(f"      • {label}" for label in missing)
            )
        return f"✅ {title} — البوت مشرف بكل الصلاحيات المطلوبة"

    async def _edit_photo(
        self, chat_id: str, message_id: int, png: bytes, caption: str = ""
    ) -> bool:
        """Replace an already-posted card image in place. Single attempt —
        a missed heartbeat refresh costs nothing; the next one catches up."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        media = {"type": "photo", "media": "attach://photo"}
        if caption:
            media["caption"] = caption[:1000]
        try:
            response = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/editMessageMedia",
                data={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "media": json.dumps(media),
                },
                files={"photo": ("signal.png", png, "image/png")},
            )
            if response.status_code != 200:
                log.warning(
                    "telegram rejected photo edit: %s %s",
                    response.status_code, response.text[:200],
                )
            return response.status_code == 200
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("telegram photo edit failed (%s)", exc)
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

    async def watch(self, png: bytes | None, text: str) -> None:
        """A blue under-watch card — quiet by design: a forming setup is
        information, not an event worth a buzz."""
        delivered = None
        if png is not None:
            delivered = await self._post_photo(png, silent=True)
        if not delivered:
            await self._send(text, silent=True)

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
    # the sender's Telegram app language (IETF tag, e.g. "ar", "en") —
    # recorded silently at /start so the demand for a non-Arabic experience
    # becomes a number instead of a guess
    language: str = ""


@dataclass
class JoinRequest:
    """Someone tapped the private channel's invite link and awaits approval."""

    channel_id: str
    user_id: str
    username: str = ""
    first_name: str = ""


@dataclass
class MembershipChange:
    """Someone joined or left a chat the bot administrates.

    Distinct from ``JoinRequest``, which only ever fires for channels set to
    approve members manually. A single-use invite link admits people with no
    join request at all, so without this event the engine is blind to exactly
    the route its own funnel uses.
    """

    chat_id: str
    user_id: str
    joined: bool
    username: str = ""
    first_name: str = ""


@dataclass
class ButtonPress:
    """An inline-keyboard button was tapped (a callback_query)."""

    callback_id: str
    user_id: str
    chat_id: str
    message_id: int
    data: str
    username: str = ""
    first_name: str = ""


class TelegramCommandListener:
    """Long-polls the bot's inbox.

    Two kinds of sender arrive on the same stream: the operator (lesson
    approvals, from the configured chat) and would-be subscribers pressing
    /start. The listener reports both and the engine routes — authorization
    for operator commands is enforced there by chat id, never here.
    """

    # set by the engine: (chat_id, direction, text) → None. Every reply the
    # listener sends is journaled so the dashboard can show the conversation
    journal: Callable[[str, str, str], None] | None = None

    def _note_out(self, chat_id: str, text: str) -> None:
        if self.journal is not None:
            with contextlib.suppress(Exception):
                self.journal(str(chat_id), "out", text)

    def __init__(self, token: str, chat_id: str, client: httpx.AsyncClient | None = None):
        self.token = token
        self.chat_id = str(chat_id)
        self._client = client
        self._owns_client = client is None
        self._offset = 0
        self._webhook_cleared = False
        # channels the bot was just promoted in — the engine reports these to
        # the operator so they can copy the numeric id into the env config
        self.channel_promotions: list[tuple[str, str]] = []
        # pending join requests for the private subscribers channel
        self.join_requests: list[JoinRequest] = []
        self.membership_changes: list[MembershipChange] = []
        # inline-keyboard taps awaiting routing (consent gate, previews)
        self.button_presses: list[ButtonPress] = []

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
                url,
                params={
                    "offset": self._offset,
                    "timeout": timeout,
                    # explicit, because the funnel depends on all of them: DMs,
                    # admin promotions (channel-id discovery), the private
                    # channel's join requests, consent buttons, and chat_member
                    # — the last one is how anyone who walked in on an invite
                    # link (i.e. everyone the funnel actually invites) is seen
                    "allowed_updates": json.dumps(
                        [
                            "message",
                            "my_chat_member",
                            "chat_member",
                            "chat_join_request",
                            "callback_query",
                        ]
                    ),
                },
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
            # promotion to channel admin arrives as my_chat_member — surfacing
            # it is how the operator learns a private channel's numeric id
            # without any technical digging
            member = update.get("my_chat_member") or {}
            member_chat = member.get("chat") or {}
            if (
                member_chat.get("type") == "channel"
                and (member.get("new_chat_member") or {}).get("status") == "administrator"
            ):
                self.channel_promotions.append(
                    (str(member_chat.get("id")), member_chat.get("title") or "")
                )
            join = update.get("chat_join_request") or {}
            if join:
                sender = join.get("from") or {}
                self.join_requests.append(
                    JoinRequest(
                        channel_id=str((join.get("chat") or {}).get("id")),
                        user_id=str(sender.get("id")),
                        username=sender.get("username") or "",
                        first_name=sender.get("first_name") or "",
                    )
                )
            change = update.get("chat_member") or {}
            if change:
                sender = (change.get("new_chat_member") or {}).get("user") or {}
                was = (change.get("old_chat_member") or {}).get("status") or ""
                now = (change.get("new_chat_member") or {}).get("status") or ""
                inside = {"member", "administrator", "creator", "restricted"}
                if (was in inside) != (now in inside):
                    self.membership_changes.append(
                        MembershipChange(
                            chat_id=str((change.get("chat") or {}).get("id")),
                            user_id=str(sender.get("id")),
                            joined=now in inside,
                            username=sender.get("username") or "",
                            first_name=sender.get("first_name") or "",
                        )
                    )
            callback = update.get("callback_query") or {}
            if callback:
                sender = callback.get("from") or {}
                origin = callback.get("message") or {}
                self.button_presses.append(
                    ButtonPress(
                        callback_id=str(callback.get("id")),
                        user_id=str(sender.get("id")),
                        chat_id=str((origin.get("chat") or {}).get("id") or sender.get("id")),
                        message_id=int(origin.get("message_id") or 0),
                        data=str(callback.get("data") or ""),
                        username=sender.get("username") or "",
                        first_name=sender.get("first_name") or "",
                    )
                )
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
                        language=sender.get("language_code") or "",
                    )
                )
        return messages

    async def send_with_buttons(
        self, chat_id: str, text: str, buttons: list[tuple[str, str]]
    ) -> bool:
        """One message with a single-column inline keyboard.

        ``buttons`` is (label, value) pairs — a value starting with http
        becomes a URL button (tap opens the page), anything else is
        callback_data. The consent gate, the operator previews, and the
        pay button all ride this.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        markup = {
            "inline_keyboard": [
                [
                    {"text": label, "url": value}
                    if value.startswith("http")
                    else {"text": label, "callback_data": value}
                ]
                for label, value in buttons
            ]
        }
        try:
            response = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                    "reply_markup": markup,
                },
            )
            if response.status_code != 200:
                log.warning(
                    "buttoned message to %s rejected (%s): %s",
                    chat_id, response.status_code, response.text[:200],
                )
            else:
                labels = " | ".join(label for label, _ in buttons)
                self._note_out(chat_id, text + (f"\n[أزرار: {labels}]" if labels else ""))
            return response.status_code == 200
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("buttoned message to %s failed (%s)", chat_id, exc)
            return False

    async def answer_button(self, callback_id: str, text: str = "") -> None:
        """Acknowledge a button tap so Telegram stops the loading spinner."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        with contextlib.suppress(httpx.TransportError, httpx.TimeoutException):
            await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/answerCallbackQuery",
                json={"callback_query_id": callback_id, "text": text[:200]},
            )

    async def replace_message(self, chat_id: str, message_id: int, text: str) -> bool:
        """Rewrite a sent message (dropping any buttons) — how the consent
        message flips to '✅ تم الإقرار' once a verdict is pressed."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        try:
            response = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/editMessageText",
                json={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
            )
            return response.status_code == 200
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("message edit in %s failed (%s)", chat_id, exc)
            return False

    async def send_photo(self, chat_id: str, png: bytes, caption: str = "") -> bool:
        """One photo to one chat — the operator's card previews ride this."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        try:
            response = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/sendPhoto",
                data={"chat_id": chat_id, "caption": caption[:1000]},
                files={"photo": ("card.png", png, "image/png")},
            )
            if response.status_code != 200:
                log.warning(
                    "photo to %s rejected (%s): %s",
                    chat_id, response.status_code, response.text[:200],
                )
            return response.status_code == 200
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("photo to %s failed (%s)", chat_id, exc)
            return False

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
            else:
                self._note_out(chat_id, text)
            return response.status_code == 200
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("telegram reply to %s failed (%s)", chat_id, exc)
            return False

    async def _join_request_verdict(
        self, method: str, channel_id: str, user_id: str
    ) -> bool:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        try:
            response = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/{method}",
                json={"chat_id": channel_id, "user_id": int(user_id)},
            )
            if response.status_code != 200:
                log.warning(
                    "%s for %s failed (%s): %s",
                    method, user_id, response.status_code, response.text[:200],
                )
            return response.status_code == 200
        except (httpx.TransportError, httpx.TimeoutException, ValueError) as exc:
            log.warning("%s for %s failed (%s)", method, user_id, exc)
            return False

    async def approve_join_request(self, channel_id: str, user_id: str) -> bool:
        return await self._join_request_verdict("approveChatJoinRequest", channel_id, user_id)

    async def decline_join_request(self, channel_id: str, user_id: str) -> bool:
        return await self._join_request_verdict("declineChatJoinRequest", channel_id, user_id)

    async def create_invite_link(self, channel_id: str, name: str = "") -> str | None:
        """A single-use invite link to the private channel — one link, one
        member, so a forwarded link cannot smuggle in free riders."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        try:
            response = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/createChatInviteLink",
                json={"chat_id": channel_id, "member_limit": 1, "name": name[:32]},
            )
            if response.status_code == 200:
                return (response.json().get("result") or {}).get("invite_link")
            log.warning(
                "invite link creation failed (%s): %s",
                response.status_code, response.text[:200],
            )
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("invite link creation failed (%s)", exc)
        return None

    async def member_count(self, chat_id: str) -> int | None:
        """How many people are actually in the channel right now.

        The Bot API cannot enumerate a channel's members — no endpoint exists,
        by design — so this count is the only way to see the gap between who
        is inside and who the engine has on its books. None means the question
        could not be answered, which must not be shown as zero.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        try:
            response = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/getChatMemberCount",
                json={"chat_id": chat_id},
            )
            if response.status_code == 200:
                return int(response.json().get("result") or 0)
            log.warning("member count failed (%s): %s", response.status_code, response.text[:200])
        except (httpx.TransportError, httpx.TimeoutException, ValueError, TypeError) as exc:
            log.warning("member count failed (%s)", exc)
        return None

    async def is_member(self, chat_id: str, user_id: str) -> bool | None:
        """Is this specific person still inside? None when unanswerable."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        try:
            response = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/getChatMember",
                json={"chat_id": chat_id, "user_id": user_id},
            )
            if response.status_code == 200:
                status = ((response.json().get("result") or {}).get("status")) or ""
                return status in {"member", "administrator", "creator", "restricted"}
            if response.status_code == 400:
                return False  # "user not found" — a definite answer, not a failure
            log.warning("member check failed (%s): %s", response.status_code, response.text[:200])
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            log.warning("member check failed (%s)", exc)
        return None

    async def kick(self, channel_id: str, user_id: str) -> bool:
        """Remove an expired subscriber from the private channel.

        Ban then immediately unban: the ban performs the removal, the unban
        clears the blacklist so a future paid re-join with a fresh link works.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=20.0)
        try:
            banned = await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/banChatMember",
                json={"chat_id": channel_id, "user_id": int(user_id)},
            )
            await self._client.post(
                f"{TELEGRAM_API}/bot{self.token}/unbanChatMember",
                json={"chat_id": channel_id, "user_id": int(user_id), "only_if_banned": True},
            )
            if banned.status_code != 200:
                log.warning(
                    "kick of %s failed (%s): %s",
                    user_id, banned.status_code, banned.text[:200],
                )
            return banned.status_code == 200
        except (httpx.TransportError, httpx.TimeoutException, ValueError) as exc:
            log.warning("kick of %s failed (%s)", user_id, exc)
            return False

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


CONSENT_YES = "consent:yes"
CONSENT_NO = "consent:no"
PREVIEW_YES = "preview:yes"
PREVIEW_NO = "preview:no"

CONSENT_BUTTONS: list[tuple[str, str]] = [
    ("✅ أوافق وأقر", CONSENT_YES),
    ("❌ لا أوافق", CONSENT_NO),
]


def _days_ar(days: int) -> str:
    """Arabic number agreement: 3-10 take أيام, the rest take يوماً."""
    return f"{days} أيام" if 3 <= days <= 10 else f"{days} يوماً"


def welcome_pitch_message(
    trial_days: int, price_sar: int, walkthrough_url: str = "", youtube_url: str = ""
) -> str:
    """Message one of the funnel: what the indicator draws for you, the two
    places to see it working (the live walkthrough and the daily videos),
    and the free week. Deliberately free of the advisory-room register: the
    indicator is an analysis assistant, the decision stays with the reader.
    """
    lines = [
        "أهلاً بك في مِرصاد ٩ 👋",
        "",
        "مؤشر على TradingView يقرأ الشموع عنك ويرسم لك على الشارت مباشرة:",
        "",
        "🟢 الإشارة كول أو بوت مع درجة جودتها من ١٠٠",
        "🟠 خط الدخول البرتقالي: تدخل عند لمسه لا عند الإشارة",
        "🔴 الوقف، و● ● ● ثلاثة أهداف تنوّر واحداً واحداً",
        "½ تنبيه «بع النصف» يؤمّن الصفقة وينقل الوقف إلى الدخول",
        "🎯 الستررايك والانتهاء المناسبان لكل صفقة",
        "📋 شاشة مراقبة لعشر شركات تختارها، كل صفقة بنتيجتها",
        "",
        "يعمل على أي فريم: من الدقيقة إلى اليومي والأسبوعي. مساعدك في التحليل، والقرار لك.",
    ]
    if walkthrough_url:
        lines += ["", f"🎬 شاهد العرض الحي (دقيقتان):\n{walkthrough_url}"]
    if youtube_url:
        lines += ["", f"▶️ مقاطعنا اليومية على الصفقات:\n{youtube_url}"]
    lines += [
        "",
        f"🎁 {_days_ar(trial_days)} مجانية كاملة على حسابك في TradingView، "
        f"بلا بطاقة وبلا التزام. وبعدها {price_sar} ريال شهرياً لمن أحب الاستمرار.",
        "",
        "قبل التفعيل اقرأ الإقرار في الرسالة التالية 👇",
    ]
    return "\n".join(lines)


def consent_terms_message() -> str:
    """The legal gate: shown BEFORE any access is granted.

    The subscriber's explicit button press on this exact text is recorded
    with a timestamp — the platform's proof of informed consent. Static on
    purpose: no numbers that drift with settings, so the text someone agreed
    to last month is the text on record today.
    """
    return (
        "إقرار وإخلاء مسؤولية:\n\n"
        "تعريف الخدمة: مِرصاد ٩ أداة تحليل فني برمجية (مؤشر) تعمل داخل منصة "
        "TradingView، تعرض إشارات ومستويات محسوبة آلياً من بيانات الأسعار "
        "لمساعدة المستخدم على قراءة الشارت.\n\n"
        "١. كل ما يعرضه المؤشر من إشارات ومستويات وأهداف هو حساب آلي لأغراض "
        "تعليمية وتحليلية حصراً، ولا يُعد بأي حال توصية استثمارية أو استشارة "
        "مالية أو دعوة لشراء أو بيع أي أداة مالية.\n\n"
        "٢. تداول الأسهم وعقود الخيارات ينطوي على مخاطر عالية قد تصل إلى خسارة "
        "كامل المبلغ، وقد لا يكون مناسباً لجميع الأشخاص.\n\n"
        "٣. النتائج السابقة للمؤشر، على الشارت أو في التقارير أو المقاطع، لا "
        "تضمن ولا تشير إلى نتائج مستقبلية مماثلة.\n\n"
        "٤. أي قرار يتخذه المستخدم هو قراره الشخصي وعلى مسؤوليته الكاملة وحده، "
        "ولا تتحمل هذه المنصة أي مسؤولية عن قرارات أو نتائج أي مستخدم.\n\n"
        "٥. المنصة لا تقدم أي خدمات وساطة أو تنفيذ، ولا تنفذ صفقات نيابة عن "
        "أحد، ولا تدير أموالاً أو محافظ. والمؤشر لا ينفذ أي أمر في حساب "
        "المستخدم.\n\n"
        "٦. أسعار عقود الخيارات لا تُعرض داخل المؤشر لأن بيانات الخيارات في "
        "TradingView متأخرة بلا وسيط مربوط؛ سعر العقد والتنفيذ من منصة "
        "المستخدم لدى وسيطه المرخص.\n\n"
        "٧. الوصول شخصي ويُمنح لاسم مستخدم واحد في TradingView يقدمه المستخدم "
        "بنفسه. لا نطلب كلمة مرور ولا رمز دخول أبداً، ولا يجوز مشاركة الوصول.\n\n"
        "٨. تنبيه أمني: قنواتنا الرسمية الوحيدة هي القناة والبوت المرسل لهذه "
        "الرسالة. لا نراسل أحداً بشكل خاص أبداً ولا نطلب تحويلات، فاحذر أي جهة "
        "تنتحل اسمنا.\n\n"
        "بالضغط على زر الموافقة أدناه، فأنت تقر بأنك قرأت ما سبق وفهمته "
        "ووافقت عليه:"
    )


def consent_accepted_note(trial_days: int, expires_on: str = "", link: str = "") -> str:
    """Post-consent confirmation: the free window is now running. ``link``
    is a private-channel invite for deployments that still run one; the
    indicator-only product leaves it empty."""
    lines = [
        "✅ تم تسجيل إقرارك، أهلاً بك في مِرصاد ٩ 🎉",
        f"بدأت فترتك المجانية ({_days_ar(trial_days)})"
        + (f"، وتنتهي بتاريخ {expires_on}." if expires_on else "."),
    ]
    if link:
        lines.append("\n🔗 رابط دخولك الشخصي للقناة الخاصة (صالح لشخص واحد):\n" + link)
    return "\n".join(lines)


def consent_declined_note() -> str:
    return (
        "نحترم قرارك، ولم يُسجَّل أي شيء.\n"
        "بابنا مفتوح متى غيّرت رأيك: أرسل /start من جديد وستصلك هذه "
        "الرسالة مرة أخرى."
    )


def cards_guide_message(walkthrough_url: str = "", youtube_url: str = "") -> str:
    """Orientation after the TradingView name is booked: how to find the
    indicator once access lands, which frame to open, and which four alerts
    to switch on. The live walkthrough carries the visual version."""
    lines = [
        "📖 دليل البداية، خطوة بخطوة:",
        "",
        "١. حين تصلك دعوة الوصول: افتح أي شارت في TradingView ← المؤشرات ← "
        "«النصوص البرمجية بدعوة فقط» ← مِرصاد ٩ ← أضفه.",
        "٢. الفريم المقترح للبداية ٣ دقائق للصفقات اليومية. يعمل على ٥ و١٥ "
        "واليومي والأسبوعي بلا تغيير إعدادات.",
        "٣. الإشارة «كول» أو «بوت» مع رقم الجودة. لا تدخل عندها، انتظر الخط "
        "البرتقالي: الدخول عند لمسه.",
        "٤. مع الدخول تُرسم لك الوقف والأهداف الثلاثة وخط بيع النصف. عند "
        "«½ بع النصف» تبيع نصف الكمية والوقف ينتقل إلى الدخول.",
        "٥. فعّل التنبيهات الأربعة من زر التنبيه على الشارت: كول، بوت، تأمين، "
        "خروج، لتصلك على الجوال.",
        "٦. شاشة المراقبة: من الإعدادات ضع شركاتك حتى عشر، وكل صفقة تبقى "
        "بنتيجتها في صفها.",
        "٧. سعر العقد من منصتك لا من الشارت؛ الشارت يعطيك الستررايك والانتهاء.",
    ]
    if walkthrough_url:
        lines += ["", f"🎬 كل هذا مرئياً في العرض الحي:\n{walkthrough_url}"]
    if youtube_url:
        lines += ["", f"▶️ ومقاطع اليوم بيوم على اليوتيوب:\n{youtube_url}"]
    return "\n".join(lines)


def trial_status_message(days_left: int, tv_username: str = "") -> str:
    lines = [f"فترتك المجانية في مِرصاد ٩ فعّالة، المتبقي {_days_ar(max(days_left, 0))}."]
    if tv_username:
        lines.append(f"الوصول على حساب TradingView‏: {tv_username}")
    else:
        lines.append("لم يصلنا اسم المستخدم في TradingView بعد. أرسله هنا كما هو في ملفك.")
    return "\n".join(lines)


PAY_BUTTON = "💳 ادفع الآن"


def plans_offer_message(ind_sar: int, ch_sar: int = 0, vip_sar: int = 0, days: int = 30) -> str:
    """The subscription offer above the pay button. One product now, the
    indicator; the older plan arguments are accepted and ignored so callers
    written for the three-plan era keep working."""
    return (
        "💳 اشتراك مِرصاد ٩\n\n"
        f"📊 {ind_sar} ريال لكل {_days_ar(days)}\n"
        "المؤشر كاملاً على حساب TradingView الخاص بك: الإشارات، خط الدخول، "
        "الوقف والأهداف، تنبيه بيع النصف، الستررايك، وشاشة المراقبة.\n\n"
        "📦 بلا تجديد تلقائي، تدفع حين تريد الاستمرار\n"
        "⚡ التفعيل تلقائي فور الدفع، ويصلك التأكيد هنا خلال لحظات\n"
        "🔒 دفع آمن عبر بوابة مرخصة من البنك المركزي السعودي\n\n"
        "اضغط الزر 👇"
    )


def renewal_reminder_message(expires_on: str, from_sar: int, with_button: bool) -> str:
    """The two-days-left nudge — sent once per trial window. The pay URL
    rides an inline button, never the text."""
    lines = [
        f"⏳ تنبيه ودّي: تنتهي فترتك المجانية في مِرصاد ٩ بتاريخ {expires_on}، "
        "وبعدها يُزال الوصول من TradingView تلقائياً.",
    ]
    if with_button:
        lines.append(
            f"\nللاستمرار بلا انقطاع: {from_sar} ريال شهرياً، والتفعيل فوري بعد "
            "الدفع 👇"
        )
    else:
        lines.append("\nتفاصيل الاستمرار ستصلك قبل انتهاء الفترة.")
    return "\n".join(lines)


def tv_username_prompt() -> str:
    """The one thing the trial needs: their TradingView name."""
    return (
        "🖥️ الخطوة الأخيرة: أرسل هنا اسم المستخدم الخاص بك في TradingView كما "
        "يظهر في ملفك، بالحروف الإنجليزية. مثال:\n\nAhmed_Trader\n\n"
        "تصلك دعوة الوصول داخل TradingView من حسابنا الرسمي OqoodOptions خلال "
        "ساعات. لا نطلب كلمة مرور ولا رمز دخول أبداً."
    )


def tv_username_booked_note(username: str, expires_on: str = "") -> str:
    return (
        f"✅ سُجّل اسمك في TradingView‏: {username}\n"
        "ستصلك دعوة الوصول داخل TradingView خلال ساعات"
        + (f"، وفترتك المجانية حتى {expires_on}." if expires_on else ".")
        + "\nفي انتظارها، اقرأ دليل البداية في الرسالة التالية 👇"
    )


def tv_granted_note(username: str, expires_on: str = "") -> str:
    """The operator confirmed the grant on TradingView: the indicator is live."""
    return (
        f"✅ تم تفعيل مِرصاد ٩ على حسابك في TradingView‏: {username}\n"
        "افتح الشارت ← المؤشرات ← «Invite-only scripts» وأضف مِرصاد ٩.\n"
        + (f"الوصول ساري حتى {expires_on}.\n" if expires_on else "")
        + "دليل البداية في الرسائل السابقة، وإن احتجت شيئاً اكتب لنا هنا."
    )


def tv_revoked_note(username: str) -> str:
    return (
        f"انتهى وصول حسابك {username} إلى مِرصاد ٩ في TradingView.\n"
        "للعودة في أي وقت أرسل /start 🤍"
    )


def payment_activated_note(plan_label: str, expires_on: str) -> str:
    return (
        f"✅ تم تفعيل اشتراكك — {plan_label} — شكرًا لك 🤍\n"
        f"اشتراكك فعال حتى {expires_on}."
    )


def farewell_message(has_buttons: bool) -> str:
    text = (
        "انتهت فترتك المجانية في مِرصاد ٩ وأُزيل الوصول من TradingView. "
        "شكراً لبقائك معنا 🙏"
    )
    if has_buttons:
        text += "\n\nللاستمرار أو لمتابعة مقاطعنا، خياراتك بضغطة زر 👇"
    return text


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

    async def watch(self, png: bytes | None, text: str) -> None:
        await self._fanout("watch", png, text)


async def verify_telegram(token: str, chat_id: str) -> tuple[bool, str]:
    """Confirm the bot can actually reach the chat before a session depends on it."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            me = await client.get(f"{TELEGRAM_API}/bot{token}/getMe")
            if me.status_code != 200:
                return False, f"invalid bot token ({me.status_code})"
            name = me.json().get("result", {}).get("username", "unknown")

            notifier = TelegramNotifier(token, chat_id, client=client)
            sent = await notifier._send("✅ بوت عقود الخيارات متصل بنجاح — هذه رسالة اختبار")
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
