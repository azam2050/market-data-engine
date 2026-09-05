"""The channels — where MIRSAD 9's reports go.

Nothing here is a signal. A channel receives the indicator's after-the-bell
report cards (daily, weekly, monthly) and nothing else; the posting
primitives below are all a report needs: a photo with a caption, or the text
that stands in for it when the photo cannot be delivered.

Publishing is strictly best-effort: the channel is a shop window, and no
broken window is ever allowed to stop the engine inside.
"""

from __future__ import annotations

import logging

import httpx

from qqq_alpha.live.telegram import TelegramNotifier

log = logging.getLogger(__name__)


class ChannelPublisher:
    """Posts to one channel. Every method is best-effort by contract: a
    channel failure is logged and swallowed — the engine never stops for
    the shop window."""

    def __init__(self, token: str, channel_id: str, client: httpx.AsyncClient | None = None):
        self.channel_id = channel_id
        self._notifier = TelegramNotifier(token, channel_id, client=client)

    # ------------------------------------------------------------------
    async def post_text(self, text: str) -> None:
        try:
            await self._notifier._send(text)
        except Exception:  # noqa: BLE001
            log.exception("channel text post failed")

    async def _post_card(self, png: bytes | None, caption: str, fallback: str) -> int | None:
        try:
            delivered = None
            if png is not None:
                delivered = await self._notifier._post_photo(png, caption=caption)
            if not delivered:
                await self._notifier._send(fallback)
                return None
            return delivered
        except Exception:  # noqa: BLE001
            log.exception("channel card post failed")
            return None

    async def aclose(self) -> None:
        await self._notifier.aclose()
