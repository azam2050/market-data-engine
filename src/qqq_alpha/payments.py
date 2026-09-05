"""Moyasar direct: the pay link, the identity signature, and the re-check.

Design constraints, in order of importance:

- The subscriber's browser is hostile territory. The embedded form's amount
  and metadata are just JavaScript, so nothing a webhook says about itself
  is trusted: before any activation, the payment is re-fetched from
  Moyasar's API with the secret key and its status, amount, currency, and
  product tag are checked server-side. A tampered payment buys nothing.
- The Telegram chat id in a pay link is signed (HMAC over the bot token),
  so nobody can craft a link that activates someone else's — or a made-up —
  subscription.
- The shared Moyasar account serves another app too. Our payments carry a
  product tag in metadata; the webhook ignores everything else, and the
  other app never sees ours.
- Keys live ONLY in environment variables. With them unset the whole
  module goes dark: no links are offered and the webhook drops everything.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Any

import httpx

from qqq_alpha.config import Settings

log = logging.getLogger(__name__)

# the metadata marker that separates our payments from the other app's on
# the shared Moyasar account
PRODUCT_TAG = "oqood_channel"

MOYASAR_API = "https://api.moyasar.com/v1"

# the three monthly plans. Codes are wire format — they ride pay links and
# payment metadata, so they never change; labels and prices are display.
PLAN_LABELS: dict[str, str] = {
    "indicator": "📊 مِرصاد ٩ — المؤشر",
    "channel": "⭐️ القناة الخاصة",
    "vip": "👑 VIP — القناة والمؤشر معاً",
}
# the product is the indicator; the other two codes stay valid so links and
# payments issued in the channel era still resolve
DEFAULT_PLAN = "indicator"


def plan_price_sar(settings: Settings, plan: str) -> int:
    return {
        "indicator": settings.price_indicator_sar,
        "channel": settings.price_channel_sar,
        "vip": settings.price_vip_sar,
    }.get(plan, settings.price_vip_sar)


def plan_includes_channel(plan: str) -> bool:
    return plan in ("channel", "vip")


def plan_includes_indicator(plan: str) -> bool:
    return plan in ("indicator", "vip")


def payments_configured(settings: Settings) -> bool:
    return bool(
        settings.moyasar_publishable_key
        and settings.moyasar_secret_key
        and settings.public_base_url
    )


def sign_chat(settings: Settings, chat_id: str) -> str:
    """A short HMAC tying a pay link to one Telegram chat.

    Keyed on the bot token — already secret, already present, and rotating
    it invalidates outstanding links, which is the right failure mode.
    """
    digest = hmac.new(
        settings.telegram_bot_token.encode(),
        f"pay:{chat_id}".encode(),
        hashlib.sha256,
    ).hexdigest()
    return digest[:20]


def verify_chat_signature(settings: Settings, chat_id: str, signature: str) -> bool:
    return bool(chat_id) and hmac.compare_digest(sign_chat(settings, chat_id), signature)


def pay_link(settings: Settings, chat_id: str, plan: str = DEFAULT_PLAN) -> str | None:
    """The personal payment URL for one subscriber and plan, or None while dark."""
    if not payments_configured(settings):
        return None
    base = settings.public_base_url.rstrip("/")
    return f"{base}/pay?u={chat_id}&t={sign_chat(settings, chat_id)}&p={plan}"


def expected_amount_halalas(settings: Settings, plan: str) -> int:
    return plan_price_sar(settings, plan) * 100


async def fetch_payment(
    settings: Settings, payment_id: str, client: httpx.AsyncClient | None = None
) -> dict[str, Any] | None:
    """The payment as Moyasar itself reports it — the only trusted copy."""
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=20.0)
    try:
        response = await client.get(
            f"{MOYASAR_API}/payments/{payment_id}",
            auth=(settings.moyasar_secret_key, ""),
        )
        if response.status_code == 200:
            return response.json()
        log.warning(
            "payment fetch failed (%s): %s", response.status_code, response.text[:200]
        )
    except (httpx.TransportError, httpx.TimeoutException) as exc:
        log.warning("payment fetch failed (%s)", exc)
    finally:
        if owns_client:
            await client.aclose()
    return None


def payment_problems(settings: Settings, payment: dict[str, Any]) -> list[str]:
    """Everything that disqualifies this payment from activating anything.

    Named reasons, not a bool: a rejected payment lands in an operator
    note, and "amount 100 ≠ 19900" is actionable where "invalid" is not.
    """
    problems: list[str] = []
    if payment.get("status") != "paid":
        problems.append(f"الحالة {payment.get('status')!r} وليست paid")
    meta = payment.get("metadata") or {}
    plan = str(meta.get("plan") or "")
    if plan not in PLAN_LABELS:
        problems.append(f"باقة غير معروفة {plan!r}")
    else:
        # the amount must match the CLAIMED plan's price: paying the
        # indicator's price cannot buy the VIP bundle
        expected = expected_amount_halalas(settings, plan)
        if int(payment.get("amount") or 0) != expected:
            problems.append(
                f"المبلغ {payment.get('amount')} هللة ≠ المطلوب {expected} لباقة {plan}"
            )
    if (payment.get("currency") or "").upper() != "SAR":
        problems.append(f"العملة {payment.get('currency')!r} وليست SAR")
    if meta.get("product") != PRODUCT_TAG:
        problems.append("وسم المنتج غير مطابق")
    chat_id = str(meta.get("telegram_id") or "")
    if not verify_chat_signature(settings, chat_id, str(meta.get("sig") or "")):
        problems.append("توقيع معرف تيليجرام غير صحيح")
    return problems
