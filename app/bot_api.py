import asyncio
import contextlib
import io
import json
import logging
import os
import secrets
import time
import urllib.parse
from dataclasses import dataclass
from html import escape as _esc
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from uuid import uuid4

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    URLInputFile,
)
from openpyxl import Workbook

from app.config import get_settings
from app.db import PremiumDB
from app.mongo_db import MongoPremiumDB
from app.mongo_store import MongoTokenStore
from app.store import FileRef, TokenStore

settings = get_settings()
store = (
    MongoTokenStore(settings.redis_url, settings.mongo_uri, settings.mongo_db_name, history_limit=settings.history_limit)
    if settings.mongo_uri
    else TokenStore(settings.redis_url, history_limit=settings.history_limit)
)
db = (
    MongoPremiumDB(settings.db_path, settings.mongo_uri, settings.mongo_db_name)
    if settings.mongo_uri
    else PremiumDB(settings.db_path)
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("stream_bot_api")

bot = Bot(token=settings.bot_token)
dp = Dispatcher(storage=MemoryStorage())

CREDIT_COST = 1
DEFAULT_CREDIT_PRICE_INR = 0.45
DEFAULT_PAY_TEXT = "Price per credit: INR {price}\nTo add credits, contact admin."
ADMIN_CONTACT = "@azmoviedeal"
PREMIUM_MONTHLY_PRICE_INR = 499.0
PREMIUM_MONTHLY_DAYS = 30
MIN_CUSTOM_PAY_INR = 10.0
BUTTON_COOLDOWN_SECONDS = 20
PAY_REQUEST_COOLDOWN_SECONDS = 20
PAY_ACTIVE_STATUSES = ("pending", "processing", "awaiting_screenshot", "under_review", "submitted")
PAY_CLOSED_STATUSES = {"processed", "delivered", "approved", "rejected", "failed", "cancelled", "expired"}
PAYMENT_REQUEST_EXPIRY_SECONDS = 15 * 60
PAYMENT_EXPIRY_SCAN_INTERVAL_SECONDS = 20
ORDER_TIMEOUT_SEC = 15 * 60
XWALLET_PAY_URL = "https://xwalletbot.shop/wallet/getway/pay.php"
XWALLET_CHECK_URL = "https://xwalletbot.shop/wallet/getway/check.php"
XWALLET_SUCCESS_STATUSES = {"TXN_SUCCESS", "SUCCESS", "PAID", "COMPLETED"}
XWALLET_FAILED_STATUSES = {"FAILED", "TXN_FAILED", "EXPIRED", "CANCELLED"}

_payment_expiry_task: asyncio.Task | None = None
_xwallet_poll_tasks: dict[str, asyncio.Task] = {}


# ---------------------------------------------------------------------------
#  FSM States
# ---------------------------------------------------------------------------

class PayState(StatesGroup):
    waiting_amount = State()       # custom amount input
    waiting_utr = State()          # UTR number input
    waiting_screenshot = State()   # screenshot photo


class ThumbState(StatesGroup):
    waiting_photo = State()        # waiting for admin to send thumbnail photo


class TrendState(StatesGroup):
    waiting_bar = State()
    waiting_title = State()
    waiting_media = State()
    waiting_description = State()
    waiting_normal_link = State()
    waiting_premium_link = State()


@dataclass(frozen=True)
class PaymentPlan:
    code: str
    label: str
    amount: float
    auth_seconds: int


PAYMENT_PLANS: dict[str, PaymentPlan] = {
    "daily": PaymentPlan("daily", "Basic (24 Hours)", 10.0, 86400),
    "weekly": PaymentPlan("weekly", "Standard (7 Days)", 35.0, 7 * 86400),
    "monthly": PaymentPlan("monthly", "Premium (30 Days)", 125.0, 30 * 86400),
}


# ---------------------------------------------------------------------------
#  Format helpers
# ---------------------------------------------------------------------------

def esc(text: str) -> str:
    return _esc(str(text), quote=False)

def code(text: str) -> str:
    return f"<code>{esc(str(text))}</code>"

def link(text: str, url: str) -> str:
    return f'<a href="{esc(url)}">{esc(text)}</a>'

def bold(text: str) -> str:
    return f"<b>{esc(str(text))}</b>"

def bullet(items: list[str]) -> str:
    return "\n".join(f"• {item}" for item in items)

def _format_money(v: float) -> str:
    return f"{v:.2f}"


def _format_expiry(expires_at: int | None, now_ts: int | None = None) -> str:
    if expires_at is None:
        return "lifetime ♾️"
    now = int(now_ts or time.time())
    exp = int(expires_at)
    stamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(exp))
    if exp <= now:
        return f"expired on {stamp}"
    remaining = exp - now
    days = remaining // 86400
    hours = (remaining % 86400) // 3600
    mins = (remaining % 3600) // 60
    if days > 0:
        left = f"{days}d {hours}h left"
    elif hours > 0:
        left = f"{hours}h {mins}m left"
    else:
        left = f"{max(1, mins)}m left"
    return f"{stamp} ({left})"

def format_msg(title, sections=None, tip=None, status=None) -> str:
    parts: list[str] = []
    if status:
        parts.append(status)
    parts.append(f"<b>{esc(title)}</b>")
    parts.append("")
    if sections:
        for label, value in sections:
            if label:
                parts.append(f"<b>{esc(label)}:</b> {value}")
            else:
                parts.append(value)
    if tip:
        parts.append("")
        parts.append(f"💡 <i>{esc(tip)}</i>")
    return "\n".join(parts)


def _admin_targets() -> set[int]:
    targets = {int(x) for x in settings.admin_ids if int(x) > 0}
    owner_raw = os.getenv("OWNER_ID", "").strip()
    if owner_raw:
        try:
            owner_id = int(owner_raw)
            if owner_id > 0:
                targets.add(owner_id)
        except Exception:
            pass
    return targets


def _action_from_status(status: str) -> str:
    s = str(status or "").strip().lower()
    if s in {"processed", "delivered", "approved"}:
        return "approved"
    if s == "rejected":
        return "rejected"
    return ""


# ---------------------------------------------------------------------------
#  Core helpers
# ---------------------------------------------------------------------------

BOT_COMMANDS = [
    BotCommand(command="start", description="Bot overview and usage"),
    BotCommand(command="buy", description="Buy premium access"),
    BotCommand(command="credit", description="Check credits and plan"),
    BotCommand(command="pay", description="Buy credits"),
    BotCommand(command="premium", description="Premium plan"),
    BotCommand(command="health", description="Health check"),
    BotCommand(command="showsections", description="Show sections (admin)"),
    BotCommand(command="addsection", description="Set upload section (admin)"),
    BotCommand(command="publishsection", description="Show section on website (admin)"),
    BotCommand(command="unpublishsection", description="Hide section from website (admin)"),
    BotCommand(command="addtrending", description="Add trending content (admin)"),
    BotCommand(command="trendinglist", description="List trending content (admin)"),
    BotCommand(command="deltrending", description="Delete trending content (admin)"),
    BotCommand(command="credit_add", description="Add credits (admin)"),
    BotCommand(command="credit_remove", description="Remove credits (admin)"),
    BotCommand(command="add", description="Add premium user (admin)"),
    BotCommand(command="addpremium", description="Grant premium plan (admin)"),
    BotCommand(command="payments", description="List payments (admin)"),
    BotCommand(command="approve", description="Approve payment (admin)"),
    BotCommand(command="reject", description="Reject payment (admin)"),
    BotCommand(command="broadcast", description="Broadcast message (admin)"),
    BotCommand(command="paydb", description="Export payments sheet (admin)"),
    BotCommand(command="paysettings", description="Show payment settings (admin)"),
    BotCommand(command="setgateway", description="Set payment gateway (admin)"),
    BotCommand(command="setxwalletkey", description="Set XWallet API key (admin)"),
    BotCommand(command="settutorial", description="Set tutorial video (admin)"),
    BotCommand(command="setautodelete", description="Set file auto-delete time (admin)"),
    BotCommand(command="setthumbnail", description="Set delivery thumbnail (admin)"),
    BotCommand(command="delthumbnail", description="Remove thumbnail (admin)"),
    BotCommand(command="thumbnail", description="Toggle thumbnail on/off (admin)"),
    BotCommand(command="setupi", description="Set UPI ID (admin)"),
]


def is_admin(user_id: int | None) -> bool:
    return bool(user_id and user_id in settings.admin_ids)

def build_link(token: str) -> str:
    return f"{settings.base_url}/player/{token}"


def _is_http_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(str(url or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _section_link_value(section_id: str) -> str:
    section_url = f"{settings.base_url}/section/{section_id}"
    if _is_http_url(section_url):
        return link("Open Section", section_url)
    return code(section_url or "BASE_URL not configured")


def _norm_lookup(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


async def _resolve_section(query: str) -> tuple[str, str] | None:
    raw = str(query or "").strip()
    if not raw:
        return None
    raw_norm = _norm_lookup(raw)
    rows = await store.list_sections()
    for name, section_id in rows:
        if raw == section_id or raw_norm == _norm_lookup(section_id) or raw_norm == _norm_lookup(name):
            return section_id, name
    return None


def _trending_page_url() -> str:
    return f"{settings.base_url}/trending"


async def _send_trending_prompt(message: Message, state: FSMContext, title: str, body: str) -> None:
    sent = await message.reply(format_msg(title, sections=[("", body)]), parse_mode="HTML")
    data = await state.get_data()
    prompt_ids = list(data.get("prompt_ids") or [])
    prompt_ids.append(sent.message_id)
    await state.update_data(prompt_ids=prompt_ids, prompt_chat_id=sent.chat.id)


async def _delete_trending_prompts(state: FSMContext) -> None:
    data = await state.get_data()
    chat_id = int(data.get("prompt_chat_id") or 0)
    prompt_ids = list(data.get("prompt_ids") or [])
    if not chat_id:
        return
    for message_id in prompt_ids:
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id=chat_id, message_id=int(message_id))


def parse_send_all_payload(payload: str) -> tuple[str, str] | None:
    raw = str(payload or "").strip()
    if not raw.startswith("sa_"):
        return None
    rest = raw[3:]
    section_id, sep, access = rest.rpartition("_")
    if not sep:
        return None
    access = access.strip().lower()
    if access not in {"normal", "premium"}:
        return None
    section_id = section_id.strip()
    if not section_id:
        return None
    return section_id, access

def parse_period(value: str) -> int | None:
    value = value.strip().lower()
    if value in {"life", "lifetime", "permanent", "perm"}:
        return None
    return int(value)


def _payment_order_id() -> str:
    return f"P{int(time.time())}{uuid4().hex[:6].upper()}"


def _subscription_plan_type(plan_code: str) -> str:
    return f"premium_{str(plan_code or '').strip().lower()}"


def _subscription_plan_from_type(plan_type: str) -> PaymentPlan | None:
    normalized = str(plan_type or '').strip().lower()
    if normalized == 'premium_30d':
        return PaymentPlan('monthly', 'Premium (30 Days)', PREMIUM_MONTHLY_PRICE_INR, PREMIUM_MONTHLY_DAYS * 86400)
    if normalized.startswith('premium_'):
        code = normalized.split('_', 1)[1]
        return PAYMENT_PLANS.get(code)
    return None


def _payment_plan_label(plan_type: str, credits: int = 0) -> str:
    plan = _subscription_plan_from_type(plan_type)
    if plan is not None:
        return plan.label
    if str(plan_type or '').strip().lower() == 'premium_30d':
        return f'Premium {PREMIUM_MONTHLY_DAYS} days'
    return f"{int(credits or 0)} credits"


def _format_expiry_ts(expires_at: int | None) -> str:
    if expires_at is None:
        return 'lifetime'
    return time.strftime('%d-%m-%Y %I:%M %p UTC', time.gmtime(int(expires_at)))


def _resolve_gateway_name(raw: str) -> str:
    gateway = str(raw or 'manual').strip().lower()
    return gateway if gateway in {'manual', 'xwallet'} else 'manual'


def _buy_plan_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"{plan.label} | INR {plan.amount:.2f}", callback_data=f"buyplan:{plan.code}")
    ] for plan in PAYMENT_PLANS.values()])


def _manual_buy_action_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="I've Paid", callback_data=f"buypaid:{order_id}")],
        [InlineKeyboardButton(text="Cancel", callback_data=f"buycancel:{order_id}")],
    ])


def _xwallet_buy_action_kb(order_id: str, payment_link: str) -> InlineKeyboardMarkup:
    rows = []
    if payment_link:
        rows.append([InlineKeyboardButton(text="Pay Now", url=payment_link)])
    rows.append([InlineKeyboardButton(text="Cancel", callback_data=f"buycancel:{order_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _get_payment_settings() -> dict:
    return await store.get_payment_settings()


async def _resolve_payment_gateway() -> str:
    settings_doc = await _get_payment_settings()
    return _resolve_gateway_name(settings_doc.get("payment_gateway", "manual"))


def _http_json(url: str, timeout: int = 15) -> dict:
    with urlopen(url, timeout=timeout) as response:
        payload = response.read().decode("utf-8", errors="replace")
    data = json.loads(payload or "{}")
    return data if isinstance(data, dict) else {}


async def _xwallet_create_payment(api_key: str, amount: float) -> tuple[str, str]:
    query = urllib.parse.urlencode({"key": str(api_key or "").strip(), "amount": f"{float(amount or 0.0):.2f}"})
    url = f"{XWALLET_PAY_URL}?{query}"

    def _call() -> tuple[str, str]:
        data = _http_json(url)
        qr_code_id = str(
            data.get("qr_code_id")
            or data.get("qrCodeId")
            or data.get("code")
            or data.get("qr_id")
            or ""
        ).strip()
        payment_link = str(
            data.get("payment_link")
            or data.get("paymentLink")
            or data.get("link")
            or data.get("url")
            or ""
        ).strip()
        return qr_code_id, payment_link

    try:
        return await asyncio.to_thread(_call)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        logger.warning("xwallet create payment failed: %s", exc)
        return "", ""


async def _xwallet_check_payment(qr_code_id: str) -> tuple[str, str]:
    code_id = str(qr_code_id or "").strip()
    if not code_id:
        return "", ""
    query = urllib.parse.urlencode({"code": code_id})
    url = f"{XWALLET_CHECK_URL}?{query}"

    def _call() -> tuple[str, str]:
        data = _http_json(url)
        status = str(data.get("status") or data.get("payment_status") or "").strip().upper()
        txn_id = str(data.get("txn_id") or data.get("transaction_id") or data.get("txnId") or "").strip()
        return status, txn_id

    try:
        return await asyncio.to_thread(_call)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        logger.warning("xwallet check payment failed: %s", exc)
        return "", ""


async def _set_user_auth(user_id: int, auth_seconds: int, plan_name: str, *, extend_existing: bool) -> int:
    now = int(time.time())
    current_exp = await db.get_expiry(user_id)
    if extend_existing and current_exp is not None and int(current_exp) > now:
        expires_at = int(current_exp) + int(auth_seconds)
    else:
        expires_at = now + int(auth_seconds)
    await db.set_expiry(user_id, expires_at)
    return expires_at


async def _send_tutorial_video(user_id: int) -> bool:
    settings_doc = await _get_payment_settings()
    from_chat_id = int(settings_doc.get("tutorial_chat_id", 0) or 0)
    message_id = int(settings_doc.get("tutorial_message_id", 0) or 0)
    if from_chat_id == 0 or message_id <= 0:
        return False
    try:
        await bot.copy_message(chat_id=int(user_id), from_chat_id=from_chat_id, message_id=message_id)
        return True
    except Exception as exc:
        logger.warning("tutorial copy failed for %s: %s", user_id, exc)
        return False


async def _finalize_premium_request(req: dict, approver: str, *, txn_id: str = "", extend_existing: bool = False, grant_type: str = "", send_tutorial: bool = True) -> tuple[bool, str, int]:
    plan = _subscription_plan_from_type(str(req.get("plan_type", "")))
    if plan is None:
        return False, "", 0
    user_id = int(req.get("user_id", 0) or 0)
    request_id = str(req.get("id", "")).strip()
    if not request_id or user_id <= 0:
        return False, "", 0

    if approver.startswith("admin:"):
        admin_id = int(approver.split(":", 1)[1] or 0)
    else:
        admin_id = 0

    target_status = "processed"
    req_after, changed = await store.transition_payment_request_status(
        request_id,
        ("pending", "processing", "awaiting_screenshot", "under_review", "submitted"),
        target_status,
        note=txn_id or "verified",
        admin_id=admin_id,
        extra_updates={
            "txn_id": str(txn_id or ""),
            "approved_by": approver,
            "grant_type": grant_type or str(req.get("grant_type", "") or ""),
        },
    )
    if not req_after or not changed:
        return False, "", 0

    expires_at = await _set_user_auth(user_id, plan.auth_seconds, plan.label, extend_existing=extend_existing)
    await store.update_payment_request(
        request_id,
        {
            "status": target_status,
            "expires_at": expires_at,
            "approved_by": approver,
            "txn_id": str(txn_id or ""),
            "grant_type": grant_type or str(req.get("grant_type", "") or ""),
        },
    )
    if float(req.get("amount_inr", 0) or 0) > 0:
        await store.add_total_earnings(float(req.get("amount_inr", 0) or 0))
    user_text = format_msg(
        "✅ Payment verified, premium activated",
        sections=[
            ("Plan", esc(plan.label)),
            ("Valid Till", esc(_format_expiry_ts(expires_at))),
        ],
        tip="Enjoy premium access.",
    )
    try:
        await bot.send_message(user_id, user_text, parse_mode="HTML")
    except Exception:
        pass
    if send_tutorial:
        await _send_tutorial_video(user_id)
    await _clear_user_payment_prompt(request_id, user_id)
    return True, plan.label, expires_at


async def _finalize_credit_request(req: dict, approver: str, *, txn_id: str = "", grant_type: str = "") -> tuple[bool, int, int]:
    user_id = int(req.get("user_id", 0) or 0)
    request_id = str(req.get("id", "")).strip()
    credits = int(req.get("credits", 0) or 0)
    if not request_id or user_id <= 0 or credits <= 0:
        return False, 0, 0

    admin_id = int(approver.split(":", 1)[1] or 0) if approver.startswith("admin:") else 0
    req_after, changed = await store.transition_payment_request_status(
        request_id,
        ("pending", "processing", "awaiting_screenshot", "under_review", "submitted"),
        "processed",
        note=txn_id or "verified",
        admin_id=admin_id,
        extra_updates={
            "txn_id": str(txn_id or ""),
            "approved_by": approver,
            "grant_type": grant_type or str(req.get("grant_type", "") or ""),
        },
    )
    if not req_after or not changed:
        return False, 0, 0

    balance = await store.add_credits(user_id, credits)
    note = f"{credits} credits added. New balance: {balance}"
    await store.update_payment_request(
        request_id,
        {
            "status": "processed",
            "note": note,
            "approved_by": approver,
            "txn_id": str(txn_id or ""),
            "grant_type": grant_type or str(req.get("grant_type", "") or ""),
        },
    )
    amount = float(req.get("amount_inr", 0) or 0)
    if amount > 0:
        await store.add_total_earnings(amount)
    try:
        await bot.send_message(
            user_id,
            format_msg(
                "✅ Payment verified",
                sections=[("Credits Added", code(credits)), ("New Balance", code(balance))],
                tip="Your credits are ready to use.",
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass
    await _clear_user_payment_prompt(request_id, user_id)
    return True, credits, balance


async def _poll_xwallet_order(request_id: str) -> None:
    try:
        while True:
            req = await store.get_payment_request(request_id)
            if not req:
                return
            status = str(req.get("status", "")).strip().lower()
            if status in PAY_CLOSED_STATUSES:
                return
            expires_at = int(req.get("expires_at", 0) or 0)
            if expires_at > 0 and int(time.time()) >= expires_at:
                await store.transition_payment_request_status(request_id, ("pending", "processing"), "expired", note="xwallet timeout", admin_id=0)
                user_id = int(req.get("user_id", 0) or 0)
                if user_id > 0:
                    with contextlib.suppress(Exception):
                        await bot.send_message(user_id, format_msg("⌛ Payment Expired", sections=[("Request ID", code(request_id))], tip="Start /pay again if needed."), parse_mode="HTML")
                return

            status_str, txn_id = await _xwallet_check_payment(str(req.get("qr_code_id", "") or ""))
            if status_str in XWALLET_SUCCESS_STATUSES:
                plan_type = str(req.get("plan_type", "credits") or "credits").strip().lower()
                if _subscription_plan_from_type(plan_type) or plan_type == "premium_30d":
                    ok, plan_label, expires_at = await _finalize_premium_request(req, "xwallet:auto", txn_id=txn_id, extend_existing=False, grant_type="xwallet", send_tutorial=True)
                    if ok:
                        await _broadcast_payment_resolution(
                            req_id=request_id,
                            user_id=int(req.get("user_id", 0) or 0),
                            amount=float(req.get("amount_inr", 0) or 0),
                            credits=int(req.get("credits", 0) or 0),
                            plan_type=plan_type,
                            action="approved",
                            actor_admin_id=0,
                            note=f"{plan_label} until {_format_expiry_ts(expires_at)}",
                        )
                    return
                ok, credits_added, balance = await _finalize_credit_request(req, "xwallet:auto", txn_id=txn_id, grant_type="xwallet")
                if ok:
                    await _broadcast_payment_resolution(
                        req_id=request_id,
                        user_id=int(req.get("user_id", 0) or 0),
                        amount=float(req.get("amount_inr", 0) or 0),
                        credits=int(req.get("credits", 0) or 0),
                        plan_type=plan_type,
                        action="approved",
                        actor_admin_id=0,
                        note=f"{credits_added} credits added. New balance: {balance}",
                    )
                return
            if status_str in XWALLET_FAILED_STATUSES:
                await store.transition_payment_request_status(request_id, ("pending", "processing"), "failed", note=f"xwallet:{status_str}", admin_id=0, extra_updates={"txn_id": txn_id})
                user_id = int(req.get("user_id", 0) or 0)
                if user_id > 0:
                    with contextlib.suppress(Exception):
                        await bot.send_message(user_id, format_msg("❌ Payment Failed", sections=[("Request ID", code(request_id)), ("Gateway", "XWallet"), ("Status", esc(status_str or "FAILED"))], tip="Use /pay to retry."), parse_mode="HTML")
                return
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("xwallet poll failed for %s: %s", request_id, exc)
    finally:
        _xwallet_poll_tasks.pop(str(request_id), None)


def _spawn_xwallet_poll(request_id: str) -> None:
    req_id = str(request_id or "").strip()
    if not req_id:
        return
    task = _xwallet_poll_tasks.get(req_id)
    if task and not task.done():
        return
    _xwallet_poll_tasks[req_id] = asyncio.create_task(_poll_xwallet_order(req_id))


def _plan_kb(*, include_premium: bool = True) -> InlineKeyboardMarkup:
    """Quick plan selection keyboard."""
    rows = [
        [
            InlineKeyboardButton(text="₹10 Credits", callback_data="pay:10"),
            InlineKeyboardButton(text="₹50 Credits", callback_data="pay:50"),
            InlineKeyboardButton(text="₹100 Credits", callback_data="pay:100"),
        ],
    ]
    second_row = [InlineKeyboardButton(text="✏️ Custom Amount", callback_data="pay:custom")]
    if include_premium:
        second_row.append(InlineKeyboardButton(text=f"✨ Premium ₹{PREMIUM_MONTHLY_PRICE_INR:.0f}/30d", callback_data="pay:premium"))
    rows.append(second_row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _payment_action_kb(req_id: str) -> InlineKeyboardMarkup:
    """After plan selected — screenshot or cancel buttons."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📸 Send Screenshot", callback_data=f"sc:{req_id}"),
        ],
        [
            InlineKeyboardButton(text="❌ Cancel Request", callback_data=f"cxl:{req_id}"),
        ],
    ])


def _admin_action_kb(req_id: str) -> InlineKeyboardMarkup:
    """Inline approve/reject for admin."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Approve", callback_data=f"apv:{req_id}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"rjt:{req_id}"),
        ],
    ])


async def _send_payment_instructions(
    chat_id: int,
    req_id: str,
    amount: float,
    credits: int,
    plan_type: str,
) -> None:
    """Send QR + instructions + action buttons after plan is chosen."""
    upi_id = await store.get_upi_id() or "example@upi"
    upi_url = f"upi://pay?pa={urllib.parse.quote(upi_id)}&pn=AZ+Stream&am={amount:.2f}&cu=INR"
    qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={urllib.parse.quote(upi_url)}"
    plan_label = _payment_plan_label(plan_type, credits)

    caption = format_msg(
        "Complete Your Payment",
        sections=[
            ("Request ID", code(req_id)),
            ("Plan", esc(plan_label)),
            ("Amount", f"INR {_format_money(amount)}"),
            ("UPI ID", code(upi_id)),
            ("", ""),
            ("", "1. Scan the QR or copy UPI ID\n2. Pay the exact amount\n3. Send payment screenshot below"),
        ],
        tip="Use the exact amount shown. Wrong amounts delay verification.",
    )
    sent_msg = None
    try:
        sent_msg = await bot.send_photo(
            chat_id,
            photo=URLInputFile(qr_url, filename="qr.png"),
            caption=caption,
            parse_mode="HTML",
            reply_markup=_payment_action_kb(req_id),
        )
    except Exception:
        sent_msg = await bot.send_message(
            chat_id,
            caption,
            parse_mode="HTML",
            reply_markup=_payment_action_kb(req_id),
        )
    if sent_msg is not None:
        await store.set_payment_prompt(req_id, chat_id, sent_msg.message_id)


async def _notify_admin_payment(req_id: str, user_id: int, amount: float, credits: int, plan_type: str, proof: str) -> None:
    plan_label = _payment_plan_label(plan_type, credits)
    text = format_msg(
        "🧾 New Payment - Action Required",
        sections=[
            ("Request ID", code(req_id)),
            ("User ID", code(user_id)),
            ("Plan", esc(plan_label)),
            ("Amount", f"INR {_format_money(amount)}"),
            ("Proof", esc(proof)),
        ],
    )
    for admin_id in _admin_targets():
        try:
            sent = await bot.send_message(admin_id, text, parse_mode="HTML", reply_markup=_admin_action_kb(req_id))
            await _track_payment_message(req_id, sent)
        except Exception:
            pass


async def _broadcast_payment_resolution(
    *,
    req_id: str,
    user_id: int,
    amount: float,
    credits: int,
    plan_type: str,
    action: str,
    actor_admin_id: int,
    note: str = "",
) -> None:
    action_norm = str(action or "").strip().lower()
    status_title = "Approved" if action_norm == "approved" else "Rejected"
    status_emoji = "✅" if action_norm == "approved" else "❌"
    plan_label = _payment_plan_label(plan_type, int(credits or 0))
    extra_note = str(note or "").strip()

    text = format_msg(
        f"{status_emoji} Payment {status_title}",
        sections=[
            ("Request ID", code(req_id)),
            ("User ID", code(user_id)),
            ("Plan", esc(plan_label)),
            ("Amount", f"INR {_format_money(amount)}"),
            ("Action By", code(actor_admin_id) if actor_admin_id else esc("system")),
            ("Status", esc(status_title)),
            ("Note", esc(extra_note or "-")),
        ],
    )

    targets = set(settings.admin_ids)
    if user_id > 0:
        targets.add(int(user_id))
    for target_id in targets:
        try:
            await bot.send_message(int(target_id), text, parse_mode="HTML")
        except Exception:
            pass


async def _update_admin_payment_messages(
    *,
    req_id: str,
    action: str,
    actor_admin_id: int,
    note: str = "",
    skip_message: tuple[int, int] | None = None,
) -> None:
    action_norm = str(action or "").strip().lower()
    if action_norm not in {"approved", "rejected"}:
        return
    status_title = "Approved" if action_norm == "approved" else "Rejected"
    status_emoji = "✅" if action_norm == "approved" else "❌"
    req = await store.get_payment_request(req_id)
    user_id = int(req.get("user_id", 0) or 0) if req else 0
    amount = float(req.get("amount_inr", 0) or 0) if req else 0.0
    credits = int(req.get("credits", 0) or 0) if req else 0
    plan_type = str(req.get("plan_type", "credits")).strip().lower() if req else "credits"
    plan_label = _payment_plan_label(plan_type, credits)
    status_block = format_msg(
        f"{status_emoji} Payment {status_title}",
        sections=[
            ("Request ID", code(req_id)),
            ("User ID", code(user_id)),
            ("Plan", esc(plan_label)),
            ("Amount", f"INR {_format_money(amount)}"),
            ("Status", esc(status_title)),
            ("Action By", code(actor_admin_id) if actor_admin_id else esc("system")),
            ("Note", esc(note or "-")),
        ],
    )

    try:
        refs = await store.list_payment_messages(req_id)
    except Exception:
        refs = []

    admin_targets = _admin_targets()
    seen: set[tuple[int, int]] = set()
    for chat_id, message_id in refs:
        key = (int(chat_id), int(message_id))
        if key in seen:
            continue
        seen.add(key)
        if int(chat_id) not in admin_targets:
            continue
        if skip_message and key == (int(skip_message[0]), int(skip_message[1])):
            continue
        try:
            await bot.edit_message_caption(chat_id=int(chat_id), message_id=int(message_id), caption=status_block, parse_mode="HTML", reply_markup=None)
            continue
        except Exception:
            pass
        try:
            await bot.edit_message_text(text=status_block, chat_id=int(chat_id), message_id=int(message_id), parse_mode="HTML", reply_markup=None)
        except Exception:
            with contextlib.suppress(Exception):
                await bot.edit_message_reply_markup(chat_id=int(chat_id), message_id=int(message_id), reply_markup=None)


async def _notify_restart() -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    text = format_msg("🟢 Bot Online", sections=[("Restarted at", esc(stamp))])
    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception as exc:
            logger.warning("Restart notify failed for %s: %s", admin_id, exc)


def _active_payment_msg(req: dict) -> str:
    req_id = str(req.get("id", "-"))
    amount = float(req.get("amount_inr", 0) or 0)
    status = str(req.get("status", "pending")).strip().lower()
    plan_type = str(req.get("plan_type", "credits")).strip().lower()
    plan_line = _payment_plan_label(plan_type, int(req.get("credits", 0) or 0))
    return format_msg(
        "⚠️ Active Payment Request Found",
        sections=[
            ("Request ID", code(req_id)),
            ("Status", esc(status.title())),
            ("Plan", esc(plan_line)),
            ("Amount", f"INR {_format_money(amount)}"),
        ],
        tip="Complete this request first or cancel it from the request card.",
    )


async def _enforce_callback_cooldown(callback: CallbackQuery) -> bool:
    user_id = callback.from_user.id if callback.from_user else 0
    if user_id <= 0:
        await callback.answer("Invalid user.", show_alert=True)
        return False
    allowed = await store.acquire_action_lock(f"btn:global:{user_id}", BUTTON_COOLDOWN_SECONDS)
    if not allowed:
        await callback.answer(
            f"Please wait {BUTTON_COOLDOWN_SECONDS}s before pressing buttons again.",
            show_alert=True,
        )
        return False
    return True


async def _track_payment_message(req_id: str, msg: Message | None) -> None:
    if not msg:
        return
    try:
        await store.add_payment_message(req_id, msg.chat.id, msg.message_id)
    except Exception:
        pass


async def _delete_payment_messages_for_request(req_id: str, user_chat_id: int) -> None:
    try:
        refs = await store.list_payment_messages(req_id)
    except Exception:
        refs = []

    seen: set[tuple[int, int]] = set()
    for chat_id, message_id in refs:
        key = (int(chat_id), int(message_id))
        if key in seen:
            continue
        seen.add(key)
        if int(chat_id) != int(user_chat_id):
            continue
        try:
            await bot.delete_message(chat_id=int(chat_id), message_id=int(message_id))
        except Exception:
            pass
    try:
        await store.clear_payment_messages(req_id)
    except Exception:
        pass


async def _clear_user_payment_prompt(req_id: str, user_chat_id: int) -> None:
    await _delete_payment_messages_for_request(req_id, user_chat_id)
    with contextlib.suppress(Exception):
        await store.clear_payment_prompt(req_id)


async def _expire_pending_payment_requests_loop() -> None:
    while True:
        try:
            now = int(time.time())
            rows = await store.list_payment_requests(status="all", limit=1000)
            for req in rows:
                req_id = str(req.get("id", "")).strip()
                if not req_id:
                    continue
                status = str(req.get("status", "")).strip().lower()
                if status not in {"pending", "processing", "awaiting_screenshot", "under_review"}:
                    continue
                expires_at = int(req.get("expires_at", 0) or 0)
                created_at = int(req.get("created_at", 0) or 0)
                if expires_at > 0:
                    if now < expires_at:
                        continue
                elif created_at <= 0 or now - created_at < PAYMENT_REQUEST_EXPIRY_SECONDS:
                    continue

                latest = await store.get_payment_request(req_id)
                if not latest:
                    continue
                latest_status = str(latest.get("status", "")).strip().lower()
                if latest_status not in {"pending", "processing", "awaiting_screenshot", "under_review"}:
                    continue

                user_id = int(latest.get("user_id", 0) or 0)
                await store.set_payment_request_status(
                    req_id,
                    "expired" if latest_status == "processing" else "cancelled",
                    note="auto-expired: payment request timed out",
                    admin_id=0,
                )
                if user_id > 0:
                    await store.clear_pending_utr(user_id)
                    await _delete_payment_messages_for_request(req_id, user_id)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("pending-payment expiry loop error: %s", exc)
        await asyncio.sleep(PAYMENT_EXPIRY_SCAN_INTERVAL_SECONDS)


async def _submit_utr(message: Message, state: FSMContext, req_id: str, utr: str) -> None:
    user_id = message.from_user.id if message.from_user else 0
    if user_id <= 0:
        await message.reply(format_msg("❌ Error", sections=[("", "Could not identify your user account.")]), parse_mode="HTML")
        return

    req = await store.get_payment_request(req_id)
    if not req:
        await state.clear()
        await store.clear_pending_utr(user_id)
        await message.reply(
            format_msg("❌ Request Not Found", sections=[("", "This payment request no longer exists. Use /pay to create a new one.")]),
            parse_mode="HTML",
        )
        return

    owner_id = int(req.get("user_id", 0) or 0)
    if owner_id != user_id and not is_admin(user_id):
        await state.clear()
        await store.clear_pending_utr(user_id)
        await message.reply(format_msg("❌ Not Allowed", sections=[("", "This request does not belong to you.")]), parse_mode="HTML")
        return

    current_status = str(req.get("status", "pending")).strip().lower()
    if current_status in PAY_CLOSED_STATUSES:
        await state.clear()
        await store.clear_pending_utr(user_id)
        await message.reply(format_msg("⚠️ Request Closed", sections=[("Status", esc(current_status.title()))]), parse_mode="HTML")
        return

    await store.set_payment_request_status(req_id, "submitted", note=f"UTR:{utr}", admin_id=0)
    await state.clear()
    await store.clear_pending_utr(user_id)

    prompt = await store.get_payment_prompt(req_id)
    if prompt:
        pending_caption = format_msg(
            "🕐 Verification In Progress",
            sections=[
                ("Request ID", code(req_id)),
                ("UTR", code(esc(utr))),
                ("", ""),
                ("", "✅ UTR admin ko bhej diya gaya hai."),
                ("", "Waiting for manual approval by admin."),
            ],
            tip="You will be notified once your payment is approved.",
        )
        try:
            await bot.edit_message_caption(
                chat_id=prompt[0], message_id=prompt[1],
                caption=pending_caption, parse_mode="HTML", reply_markup=None,
            )
        except Exception:
            try:
                await bot.edit_message_text(
                    pending_caption, chat_id=prompt[0], message_id=prompt[1],
                    parse_mode="HTML", reply_markup=None,
                )
            except Exception:
                pass
    else:
        await message.reply(
            format_msg("✅ UTR Submitted", sections=[
                ("Request ID", code(req_id)),
                ("UTR", code(esc(utr))),
                ("Status", "UTR admin ko bhej diya gaya hai"),
            ], tip="You will be notified once verified."),
            parse_mode="HTML",
        )

    amount = float(req.get("amount_inr", 0) or 0)
    credits = int(req.get("credits", 0) or 0)
    plan_type = req.get("plan_type", "credits")
    await _notify_admin_payment(req_id, owner_id, amount, credits, plan_type, f"UTR: {utr}")


async def _auto_delete_task(chat_id: int, message_id: int, delay: int, notice_msg_id: int | None = None) -> None:
    """Wait `delay` seconds then delete the delivered message (and notice if any)."""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass
    if notice_msg_id:
        try:
            await bot.delete_message(chat_id, notice_msg_id)
        except Exception:
            pass


async def _deliver_token(message: Message, token: str) -> bool:
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        await message.reply(format_msg("❌ Not Found", sections=[("", "This link is invalid or has expired.")]), parse_mode="HTML")
        return False
    user_id = message.from_user.id if message.from_user else 0
    if user_id <= 0:
        await message.reply(format_msg("❌ Error", sections=[("", "Could not identify your user account.")]), parse_mode="HTML")
        return False
    premium = await db.is_premium(user_id)
    if ref.access == "premium" and not premium:
        ok, bal = await store.charge_credits(user_id, CREDIT_COST)
        if not ok:
            await message.reply(
                format_msg("✨ Premium / Credits Required", sections=[
                    ("Balance", f"{code(bal)} credits"),
                    ("", bullet(["/pay — buy credits", "/premium — upgrade plan"])),
                ]),
                parse_mode="HTML",
            )
            return False
        await message.reply(format_msg("💳 Credit Used", sections=[("Deducted", "1 credit"), ("Remaining", code(bal))]), parse_mode="HTML")
    try:
        # Check if thumbnail should be used
        thumb_fid: str | None = None
        if await store.get_thumbnail_enabled():
            t = await store.get_thumbnail()
            if t:
                thumb_fid = t

        protect = (ref.access != "premium")
        _THUMB_TYPES = {"video", "document", "audio", "animation"}

        if thumb_fid and ref.media_type in _THUMB_TYPES:
            _send_map = {
                "video": bot.send_video,
                "document": bot.send_document,
                "audio": bot.send_audio,
                "animation": bot.send_animation,
            }
            sent = await _send_map[ref.media_type](
                message.chat.id,
                ref.file_id,
                thumbnail=thumb_fid,
                protect_content=protect,
            )
        else:
            sent = await bot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=ref.chat_id,
                message_id=ref.message_id,
                protect_content=protect,
            )
    except Exception as exc:
        logger.exception("copy_message failed: %s", exc)
        await message.reply(format_msg("❌ Delivery Failed", sections=[("", "Could not send the file. Please try again.")]), parse_mode="HTML")
        return False

    # Auto-delete: read from store first, fallback to env
    delay = await store.get_auto_delete(default=settings.auto_delete_seconds)
    if delay and delay > 0:
        mins = delay // 60
        secs = delay % 60
        duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"
        notice = await message.reply(
            format_msg(
                "⏳ Auto-Delete Enabled",
                sections=[("", f"This file will be deleted in <b>{esc(duration_str)}</b>.")],
            ),
            parse_mode="HTML",
        )
        asyncio.create_task(
            _auto_delete_task(message.chat.id, sent.message_id, delay, notice.message_id)
        )
    return True




# ---------------------------------------------------------------------------
#  User Commands
# ---------------------------------------------------------------------------

@dp.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext) -> None:
    await state.clear()
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) > 1 and parts[1].startswith("dl_"):
        await _deliver_token(message, parts[1][3:])
        return
    if len(parts) > 1:
        send_all = parse_send_all_payload(parts[1])
        if send_all:
            section_id, access_filter = send_all
            tokens = await store.list_section(section_id, settings.history_limit)
            if not tokens:
                await message.reply(format_msg("📭 No Files", sections=[("", "No files found in this section.")]), parse_mode="HTML")
                return

            selected_tokens: list[str] = []
            for token in tokens:
                ref = await store.get(token, settings.token_ttl_seconds)
                if not ref:
                    continue
                if (ref.access or "normal").strip().lower() == access_filter:
                    selected_tokens.append(token)

            if not selected_tokens:
                await message.reply(
                    format_msg("📭 No Matching Files", sections=[("", f"No {access_filter} files found in this section.")]),
                    parse_mode="HTML",
                )
                return

            await message.reply(
                format_msg(
                    "📦 Sending Files",
                    sections=[
                        ("Section", code(section_id)),
                        ("Access", esc(access_filter.title())),
                        ("Count", code(len(selected_tokens))),
                    ],
                ),
                parse_mode="HTML",
            )

            sent_count = 0
            skipped_count = 0
            for token in selected_tokens:
                ok = await _deliver_token(message, token)
                if ok:
                    sent_count += 1
                else:
                    skipped_count += 1

            tip = "Normal files are play-only." if access_filter == "normal" else "Premium files may deduct credits if needed."
            await message.reply(
                format_msg(
                    "✅ Send All Complete",
                    sections=[
                        ("Section", code(section_id)),
                        ("Sent", code(sent_count)),
                        ("Skipped", code(skipped_count)),
                    ],
                    tip=tip,
                ),
                parse_mode="HTML",
            )
            return
    await message.reply(
        format_msg("👋 Welcome to FileLord", sections=[
            ("", "Use your website link to receive files directly in Telegram."),
            ("", ""),
            ("", "<b>Plans:</b>"),
            ("", bullet(["Normal — stream files (play only)", "✨ Premium / Credits — downloadable files"])),
            ("", ""),
            ("", "<b>Quick Actions:</b>"),
            ("", bullet(["/credit — check balance", "/pay — buy credits", "/premium — view premium plan"])),
        ]),
        parse_mode="HTML",
    )


@dp.message(Command("health"))
async def health_cmd(message: Message) -> None:
    await message.reply(format_msg("🟢 Bot Status", sections=[("", "I am alive and running.")]), parse_mode="HTML")


@dp.message(Command("credit"))
async def credit_cmd(message: Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    if not uid:
        await message.reply(format_msg("❌ Error", sections=[("", "Could not identify your user account.")]), parse_mode="HTML")
        return
    premium = await db.is_premium(uid)
    bal = await store.get_credits(uid)
    if premium:
        await message.reply(format_msg("💳 Your Credits", sections=[("Plan", "✨ Premium"), ("Credits", "Unlimited")], tip="Premium users enjoy unlimited access."), parse_mode="HTML")
    else:
        await message.reply(format_msg("💳 Your Credits", sections=[("Plan", "Free"), ("Credits", code(bal))], tip="Use /pay to top-up or /premium to upgrade."), parse_mode="HTML")


@dp.message(Command("premium"))
async def premium_cmd(message: Message) -> None:
    await message.reply(
        format_msg("✨ Premium Plan", sections=[
            ("Price", f"INR {PREMIUM_MONTHLY_PRICE_INR:.0f}"),
            ("Duration", f"{PREMIUM_MONTHLY_DAYS} days"),
            ("Credits", "Unlimited"),
            ("", f"Tap /pay to subscribe or contact {esc(ADMIN_CONTACT)}."),
        ], tip="Premium users can download files without credit deductions."),
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
#  /pay — Step 1: show plan keyboard
# ---------------------------------------------------------------------------

@dp.message(Command("buy"))
async def buy_cmd(message: Message, state: FSMContext) -> None:
    await state.clear()
    gateway = await _resolve_payment_gateway()
    if gateway == "xwallet":
        await message.reply(
            format_msg(
                "Buy Credits",
                sections=[
                    ("Gateway", esc(gateway.title())),
                    ("Plans", "Choose a credit plan below:"),
                ],
                tip="Automatic confirmation is enabled through XWallet.",
            ),
            parse_mode="HTML",
            reply_markup=_plan_kb(include_premium=False),
        )
        return
    await message.reply(
        format_msg(
            "Buy Premium",
            sections=[
                ("Gateway", esc(gateway.title())),
                ("Plans", "Choose a premium plan below:"),
            ],
            tip="Manual UPI and XWallet both create durable order records.",
        ),
        parse_mode="HTML",
        reply_markup=_buy_plan_kb(),
    )


@dp.callback_query(F.data.startswith("buyplan:"))
async def buy_plan_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    await callback.answer()
    plan_code = callback.data.split(":", 1)[1].strip().lower()
    plan = PAYMENT_PLANS.get(plan_code)
    if plan is None:
        await callback.message.reply(format_msg("⚠️ Invalid Plan", sections=[("Plan", code(plan_code or "-"))]), parse_mode="HTML")
        return
    if await _resolve_payment_gateway() == "xwallet":
        await callback.message.reply(
            format_msg("⚠️ Flow Changed", sections=[("", "XWallet now handles credit plans through /pay only.")]),
            parse_mode="HTML",
        )
        return

    user_id = callback.from_user.id if callback.from_user else 0
    active_req = await store.get_user_active_payment_request(user_id, statuses=PAY_ACTIVE_STATUSES)
    if active_req:
        await callback.message.reply(_active_payment_msg(active_req), parse_mode="HTML")
        return

    create_allowed = await store.acquire_action_lock(f"buy:create:{user_id}", PAY_REQUEST_COOLDOWN_SECONDS)
    if not create_allowed:
        await callback.message.reply(
            format_msg("⏳ Please Wait", sections=[("", f"You can create a new order after {PAY_REQUEST_COOLDOWN_SECONDS} seconds.")]),
            parse_mode="HTML",
        )
        return

    gateway = await _resolve_payment_gateway()
    settings_doc = await _get_payment_settings()
    req_id = _payment_order_id()
    expires_at = int(time.time()) + ORDER_TIMEOUT_SEC
    plan_type = _subscription_plan_type(plan.code)
    await store.create_payment_request(
        req_id,
        user_id,
        plan.amount,
        0,
        plan_type=plan_type,
        gateway=gateway,
        expires_at=expires_at,
    )

    if gateway == "xwallet":
        api_key = str(settings_doc.get("xwallet_api_key", "") or "").strip()
        if not api_key:
            await store.delete_payment_request(req_id)
            await callback.message.reply(
                format_msg("Gateway Not Ready", sections=[("Gateway", "XWallet"), ("", "Admin must set /setxwalletkey first.")]),
                parse_mode="HTML",
            )
            return
        qr_code_id, payment_link = await _xwallet_create_payment(api_key, plan.amount)
        if not qr_code_id or not payment_link:
            await store.delete_payment_request(req_id)
            await callback.message.reply(
                format_msg("❌ Payment Init Failed", sections=[("Gateway", "XWallet"), ("", "Could not create payment link right now.")]),
                parse_mode="HTML",
            )
            return
        await store.update_payment_request(req_id, {"status": "processing", "qr_code_id": qr_code_id, "payment_link": payment_link})
        sent = await callback.message.reply(
            format_msg(
                "Complete Payment",
                sections=[
                    ("Order ID", code(req_id)),
                    ("Plan", esc(plan.label)),
                    ("Amount", f"INR {plan.amount:.2f}"),
                    ("Status", "Processing via XWallet"),
                    ("Expires", esc(_format_expiry_ts(expires_at))),
                ],
                tip="Tap Pay Now and wait for automatic confirmation.",
            ),
            parse_mode="HTML",
            reply_markup=_xwallet_buy_action_kb(req_id, payment_link),
        )
        await store.set_payment_prompt(req_id, sent.chat.id, sent.message_id)
        await _track_payment_message(req_id, sent)
        _spawn_xwallet_poll(req_id)
        return

    upi_id = await store.get_upi_id() or "example@upi"
    sent = await callback.message.reply(
        format_msg(
            "Complete Payment",
            sections=[
                ("Order ID", code(req_id)),
                ("Plan", esc(plan.label)),
                ("Amount", f"INR {plan.amount:.2f}"),
                ("UPI ID", code(upi_id)),
                ("Expires", esc(_format_expiry_ts(expires_at))),
                ("", "1. Pay exact amount\n2. Tap I've Paid\n3. Send screenshot"),
            ],
            tip="Manual review starts after screenshot submission.",
        ),
        parse_mode="HTML",
        reply_markup=_manual_buy_action_kb(req_id),
    )
    await store.set_payment_prompt(req_id, sent.chat.id, sent.message_id)
    await _track_payment_message(req_id, sent)


@dp.callback_query(F.data.startswith("buypaid:"))
async def buy_paid_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    req_id = callback.data.split(":", 1)[1]
    req = await store.get_payment_request(req_id)
    user_id = callback.from_user.id if callback.from_user else 0
    if not req:
        await callback.answer("Order not found.", show_alert=True)
        return
    if int(req.get("user_id", 0) or 0) != int(user_id):
        await callback.answer("This order does not belong to you.", show_alert=True)
        return
    if str(req.get("gateway", "manual")).strip().lower() != "manual":
        await callback.answer("This order uses automatic payment.", show_alert=True)
        return
    await callback.answer()
    await state.set_state(PayState.waiting_screenshot)
    await state.update_data(req_id=req_id)
    await store.update_payment_request(req_id, {"status": "awaiting_screenshot"})
    prompt_msg = await callback.message.reply(
        format_msg("Send Screenshot", sections=[("Order ID", code(req_id)), ("", "Please send your payment screenshot.")]),
        parse_mode="HTML",
    )
    await _track_payment_message(req_id, prompt_msg)


@dp.callback_query(F.data.startswith("buycancel:"))
async def buy_cancel_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    req_id = callback.data.split(":", 1)[1]
    req = await store.get_payment_request(req_id)
    user_id = callback.from_user.id if callback.from_user else 0
    if not req:
        await callback.answer("Order not found.", show_alert=True)
        return
    if int(req.get("user_id", 0) or 0) != int(user_id) and not is_admin(user_id):
        await callback.answer("This order does not belong to you.", show_alert=True)
        return
    await callback.answer("Cancelled")
    await store.update_payment_request(req_id, {"status": "cancelled", "note": "user_cancelled"})
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.reply(
        format_msg("Order Cancelled", sections=[("Order ID", code(req_id))], tip="Use /buy to create a new premium order."),
        parse_mode="HTML",
    )
    await state.clear()


@dp.message(Command("pay"))
async def pay_cmd(message: Message, state: FSMContext) -> None:
    await state.clear()
    gateway = await _resolve_payment_gateway()
    if gateway == "xwallet":
        await message.reply(
            format_msg(
                "Buy Credits",
                sections=[
                    ("Gateway", esc(gateway.title())),
                    ("Plans", "Choose a credit plan below:"),
                ],
                tip="Automatic confirmation is enabled through XWallet.",
            ),
            parse_mode="HTML",
            reply_markup=_plan_kb(include_premium=False),
        )
        return
    price, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    pay_info = template.replace("{price}", _format_money(price))
    await message.reply(
        format_msg("💰 Buy Credits / Premium", sections=[
            ("", esc(pay_info)),
            ("", ""),
            ("", "Choose a plan below:"),
        ]),
        parse_mode="HTML",
        reply_markup=_plan_kb(),
    )


# ---------------------------------------------------------------------------
#  /pay — Step 2: plan selected via inline button
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("pay:"))
async def pay_plan_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    await callback.answer()
    plan = callback.data.split(":", 1)[1]
    gateway = await _resolve_payment_gateway()
    price, _ = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    user_id = callback.from_user.id

    active_req = await store.get_user_active_payment_request(user_id, statuses=PAY_ACTIVE_STATUSES)
    if active_req:
        await callback.message.reply(_active_payment_msg(active_req), parse_mode="HTML")
        return

    if plan == "custom":
        await state.set_state(PayState.waiting_amount)
        await callback.message.reply(
            format_msg("✏️ Custom Amount", sections=[("", f"Enter your desired amount (minimum INR {MIN_CUSTOM_PAY_INR:.0f}):")]),
            parse_mode="HTML",
        )
        return

    if gateway == "xwallet" and plan == "premium":
        await callback.message.reply(
            format_msg("⚠️ Invalid Plan", sections=[("", "XWallet flow currently supports credit plans only. Please choose a credit amount.")]),
            parse_mode="HTML",
        )
        return

    if plan == "premium":
        amount = PREMIUM_MONTHLY_PRICE_INR
        credits = 0
        plan_type = "premium_30d"
    else:
        amount = float(plan)
        credits = max(1, int(amount / max(price, 0.01)))
        plan_type = "credits"

    create_allowed = await store.acquire_action_lock(f"pay:create:{user_id}", PAY_REQUEST_COOLDOWN_SECONDS)
    if not create_allowed:
        await callback.message.reply(
            format_msg(
                "⏳ Please Wait",
                sections=[("", f"You can create a new payment request after {PAY_REQUEST_COOLDOWN_SECONDS} seconds.")],
            ),
            parse_mode="HTML",
        )
        return

    req_id = await store.next_payment_request_id()
    if gateway == "xwallet":
        settings_doc = await _get_payment_settings()
        api_key = str(settings_doc.get("xwallet_api_key", "") or "").strip()
        if not api_key:
            await callback.message.reply(
                format_msg("Gateway Not Ready", sections=[("Gateway", "XWallet"), ("", "Admin must set /setxwalletkey first.")]),
                parse_mode="HTML",
            )
            return
        expires_at = int(time.time()) + ORDER_TIMEOUT_SEC
        await store.create_payment_request(req_id, user_id, amount, credits, plan_type=plan_type, gateway="xwallet", expires_at=expires_at)
        qr_code_id, payment_link = await _xwallet_create_payment(api_key, amount)
        if not qr_code_id or not payment_link:
            await store.delete_payment_request(req_id)
            await callback.message.reply(
                format_msg("❌ Payment Init Failed", sections=[("Gateway", "XWallet"), ("", "Could not create payment link right now.")]),
                parse_mode="HTML",
            )
            return
        await store.update_payment_request(req_id, {"status": "processing", "qr_code_id": qr_code_id, "payment_link": payment_link})
        plan_label = _payment_plan_label(plan_type, credits)
        sent = await callback.message.reply(
            format_msg(
                "Complete Payment",
                sections=[
                    ("Request ID", code(req_id)),
                    ("Plan", esc(plan_label)),
                    ("Amount", f"INR {amount:.2f}"),
                    ("Status", "Processing via XWallet"),
                    ("Expires", esc(_format_expiry_ts(expires_at))),
                ],
                tip="Tap Pay Now and wait for automatic credit confirmation.",
            ),
            parse_mode="HTML",
            reply_markup=_xwallet_buy_action_kb(req_id, payment_link),
        )
        await store.set_payment_prompt(req_id, sent.chat.id, sent.message_id)
        await _track_payment_message(req_id, sent)
        _spawn_xwallet_poll(req_id)
        return
    await store.create_payment_request(req_id, user_id, amount, credits, plan_type=plan_type)
    await _send_payment_instructions(callback.message.chat.id, req_id, amount, credits, plan_type)


# ---------------------------------------------------------------------------
#  Cancel Request
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("cxl:"))
async def pay_cancel_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    req_id = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id

    req = await store.get_payment_request(req_id)
    if not req:
        await callback.answer("Request not found or already cancelled.", show_alert=True)
        return

    # Only the owner can cancel
    if int(req.get("user_id", 0) or 0) != user_id and not is_admin(user_id):
        await callback.answer("❌ This request does not belong to you.", show_alert=True)
        return
    await callback.answer("Cancelling...")

    # Delete the record
    try:
        await store.delete_payment_request(req_id)
    except Exception:
        # Fallback: mark as cancelled if delete not available
        await store.set_payment_request_status(req_id, "cancelled", note="user_cancelled", admin_id=0)
    await store.clear_pending_utr(int(req.get("user_id", 0) or 0))

    # Edit message to show cancelled state (remove buttons)
    cancelled_text = format_msg(
        "🚫 Request Cancelled",
        sections=[
            ("Request ID", code(req_id)),
            ("", "Your payment request has been cancelled and removed."),
        ],
        tip="Use /pay anytime to start a new request.",
    )
    try:
        await callback.message.edit_caption(caption=cancelled_text, parse_mode="HTML", reply_markup=None)
    except Exception:
        try:
            await callback.message.edit_text(cancelled_text, parse_mode="HTML", reply_markup=None)
        except Exception:
            await callback.message.reply(cancelled_text, parse_mode="HTML")

    await state.clear()


# ---------------------------------------------------------------------------
#  /pay — Step 2b: custom amount input
# ---------------------------------------------------------------------------

@dp.message(StateFilter(PayState.waiting_amount))
async def pay_custom_amount_handler(message: Message, state: FSMContext) -> None:
    gateway = await _resolve_payment_gateway()
    try:
        amount = float((message.text or "").strip())
    except Exception:
        await message.reply(format_msg("⚠️ Invalid", sections=[("", "Please enter a valid number, e.g. 75")]), parse_mode="HTML")
        return
    if amount < MIN_CUSTOM_PAY_INR:
        await message.reply(format_msg("⚠️ Too Low", sections=[("Minimum", f"INR {MIN_CUSTOM_PAY_INR:.0f}")]), parse_mode="HTML")
        return

    price, _ = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    credits = max(1, int(amount / max(price, 0.01)))
    user_id = message.from_user.id

    active_req = await store.get_user_active_payment_request(user_id, statuses=PAY_ACTIVE_STATUSES)
    if active_req:
        await state.clear()
        await message.reply(_active_payment_msg(active_req), parse_mode="HTML")
        return

    create_allowed = await store.acquire_action_lock(f"pay:create:{user_id}", PAY_REQUEST_COOLDOWN_SECONDS)
    if not create_allowed:
        await message.reply(
            format_msg(
                "⏳ Please Wait",
                sections=[("", f"You can create a new payment request after {PAY_REQUEST_COOLDOWN_SECONDS} seconds.")],
            ),
            parse_mode="HTML",
        )
        return

    req_id = await store.next_payment_request_id()
    await state.clear()
    if gateway == "xwallet":
        settings_doc = await _get_payment_settings()
        api_key = str(settings_doc.get("xwallet_api_key", "") or "").strip()
        if not api_key:
            await message.reply(
                format_msg("Gateway Not Ready", sections=[("Gateway", "XWallet"), ("", "Admin must set /setxwalletkey first.")]),
                parse_mode="HTML",
            )
            return
        expires_at = int(time.time()) + ORDER_TIMEOUT_SEC
        await store.create_payment_request(req_id, user_id, amount, credits, plan_type="credits", gateway="xwallet", expires_at=expires_at)
        qr_code_id, payment_link = await _xwallet_create_payment(api_key, amount)
        if not qr_code_id or not payment_link:
            await store.delete_payment_request(req_id)
            await message.reply(
                format_msg("❌ Payment Init Failed", sections=[("Gateway", "XWallet"), ("", "Could not create payment link right now.")]),
                parse_mode="HTML",
            )
            return
        await store.update_payment_request(req_id, {"status": "processing", "qr_code_id": qr_code_id, "payment_link": payment_link})
        sent = await message.reply(
            format_msg(
                "Complete Payment",
                sections=[
                    ("Request ID", code(req_id)),
                    ("Plan", esc(_payment_plan_label("credits", credits))),
                    ("Amount", f"INR {amount:.2f}"),
                    ("Status", "Processing via XWallet"),
                    ("Expires", esc(_format_expiry_ts(expires_at))),
                ],
                tip="Tap Pay Now and wait for automatic credit confirmation.",
            ),
            parse_mode="HTML",
            reply_markup=_xwallet_buy_action_kb(req_id, payment_link),
        )
        await store.set_payment_prompt(req_id, sent.chat.id, sent.message_id)
        await _track_payment_message(req_id, sent)
        _spawn_xwallet_poll(req_id)
        return
    await store.create_payment_request(req_id, user_id, amount, credits, plan_type="credits")
    await _send_payment_instructions(message.chat.id, req_id, amount, credits, "credits")


# ---------------------------------------------------------------------------
#  /pay — Step 3a: user clicks "Submit UTR"
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("utr:"))
async def pay_utr_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    req_id = callback.data.split(":", 1)[1]
    req = await store.get_payment_request(req_id)
    user_id = callback.from_user.id
    if not req:
        await callback.answer("Request not found.", show_alert=True)
        return
    if int(req.get("user_id", 0) or 0) != user_id and not is_admin(user_id):
        await callback.answer("This request does not belong to you.", show_alert=True)
        return
    if str(req.get("status", "pending")).lower() in PAY_CLOSED_STATUSES:
        await callback.answer("This request is already closed.", show_alert=True)
        return
    await callback.answer()
    await state.set_state(PayState.waiting_utr)
    await state.update_data(req_id=req_id)
    await store.set_pending_utr(user_id, req_id, ttl_seconds=PAYMENT_REQUEST_EXPIRY_SECONDS)
    prompt_msg = await callback.message.reply(
        format_msg("📋 Submit UTR", sections=[("", f"Request: {code(req_id)}"), ("", "Please type your UTR / Transaction ID:")]),
        parse_mode="HTML",
    )
    await _track_payment_message(req_id, prompt_msg)


@dp.message(StateFilter(PayState.waiting_utr))
async def pay_utr_text_handler(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    req_id = str(data.get("req_id", "")).strip()
    user_id = message.from_user.id if message.from_user else 0
    if not req_id and user_id > 0:
        req_id = await store.get_pending_utr(user_id)
    utr = (message.text or "").strip()
    if not utr:
        await message.reply(format_msg("⚠️ Empty", sections=[("", "Please type a valid UTR number.")]), parse_mode="HTML")
        return
    if not req_id:
        await state.clear()
        await message.reply(
            format_msg("⚠️ No Active Request", sections=[("", "Please tap 'Submit UTR' from your payment card first.")]),
            parse_mode="HTML",
        )
        return

    await _submit_utr(message, state, req_id, utr)


# ---------------------------------------------------------------------------
#  /pay — Step 3b: user clicks "Send Screenshot"
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("sc:"))
async def pay_screenshot_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    req_id = callback.data.split(":", 1)[1]
    req = await store.get_payment_request(req_id)
    user_id = callback.from_user.id
    if not req:
        await callback.answer("Request not found.", show_alert=True)
        return
    if int(req.get("user_id", 0) or 0) != user_id and not is_admin(user_id):
        await callback.answer("This request does not belong to you.", show_alert=True)
        return
    if str(req.get("status", "pending")).lower() in PAY_CLOSED_STATUSES:
        await callback.answer("This request is already closed.", show_alert=True)
        return
    await callback.answer()
    await store.clear_pending_utr(user_id)
    await state.set_state(PayState.waiting_screenshot)
    await state.update_data(req_id=req_id)
    prompt_msg = await callback.message.reply(
        format_msg("📸 Send Screenshot", sections=[("", f"Request: {code(req_id)}"), ("", "Please send your payment screenshot as a photo:")]),
        parse_mode="HTML",
    )
    await _track_payment_message(req_id, prompt_msg)


@dp.message(StateFilter(PayState.waiting_screenshot), F.photo)
async def pay_screenshot_photo_handler(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    req_id = str(data.get("req_id", "")).strip()
    await state.clear()
    user_id = message.from_user.id if message.from_user else 0
    if user_id > 0:
        await store.clear_pending_utr(user_id)

    req = await store.get_payment_request(req_id)
    if not req:
        await message.reply(format_msg("❌ Request Not Found", sections=[("", "Use /pay or /buy to create a fresh request.")]), parse_mode="HTML")
        return

    plan_type = str(req.get("plan_type", "credits") or "credits")
    next_status = "under_review" if _subscription_plan_from_type(plan_type) else "submitted"
    proof_file_id = message.photo[-1].file_id
    await store.update_payment_request(req_id, {"status": next_status, "note": "screenshot", "screenshot_file_id": proof_file_id})

    # Edit the original QR payment message to show pending status
    prompt = await store.get_payment_prompt(req_id)
    if prompt:
        pending_caption = format_msg(
            "🕐 Verification In Progress",
            sections=[
                ("Request ID", code(req_id)),
                ("Proof", "📸 Screenshot received"),
                ("", ""),
                ("", "✅ Screenshot admin ko bhej diya gaya hai."),
                ("", "Waiting for manual approval by admin."),
            ],
            tip="You will be notified once your payment is approved.",
        )
        try:
            await bot.edit_message_caption(
                chat_id=prompt[0], message_id=prompt[1],
                caption=pending_caption, parse_mode="HTML", reply_markup=None,
            )
        except Exception:
            try:
                await bot.edit_message_text(
                    pending_caption, chat_id=prompt[0], message_id=prompt[1],
                    parse_mode="HTML", reply_markup=None,
                )
            except Exception:
                pass
    else:
        await message.reply(
            format_msg(
                "✅ Screenshot Submitted",
                sections=[
                    ("Request ID", code(req_id)),
                    ("Status", "Screenshot admin ko bhej diya gaya hai"),
                ],
                tip="You will be notified once verified.",
            ),
            parse_mode="HTML",
        )

    amount = float(req.get("amount_inr", 0) or 0)
    credits = int(req.get("credits", 0) or 0)
    user_id = message.from_user.id if message.from_user else 0

    submitted_at = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    admin_caption = format_msg("🔔 Payment Screenshot — Action Required", sections=[
        ("Request ID", code(req_id)),
        ("User ID", code(user_id)),
        ("Plan", esc(_payment_plan_label(plan_type, credits))),
        ("Amount", f"INR {_format_money(amount)}"),
        ("Proof", "Screenshot"),
        ("Status", esc(next_status.replace("_", " ").title())),
        ("Submitted At", esc(submitted_at)),
    ])
    for admin_id in _admin_targets():
        try:
            sent = await bot.send_photo(
                admin_id,
                photo=proof_file_id,
                caption=admin_caption,
                parse_mode="HTML",
                reply_markup=_admin_action_kb(req_id),
            )
            await _track_payment_message(req_id, sent)
        except Exception:
            pass


@dp.message(StateFilter(PayState.waiting_screenshot))
async def pay_screenshot_wrong_type(message: Message, state: FSMContext) -> None:
    await message.reply(format_msg("⚠️ Photo Required", sections=[("", "Please send a photo (screenshot), not a file or text.")]), parse_mode="HTML")


# ---------------------------------------------------------------------------
#  Admin Inline: ✅ Approve
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("apv:"))
async def admin_approve_callback(callback: CallbackQuery) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    admin_id = callback.from_user.id
    if not is_admin(admin_id):
        await callback.answer("❌ Not allowed.", show_alert=True)
        return
    await callback.answer("Processing...")

    req_id = callback.data.split(":", 1)[1]
    req = await store.get_payment_request(req_id)
    if not req:
        await callback.message.reply(format_msg("❌ Not Found", sections=[("", f"Request {code(req_id)} not found.")]), parse_mode="HTML")
        return

    plan_type = str(req.get("plan_type", "credits") or "credits")
    user_id = int(req.get("user_id", 0) or 0)
    credits = int(req.get("credits", 0) or 0)
    amount = float(req.get("amount_inr", 0) or 0)
    await store.clear_pending_utr(user_id)

    if _subscription_plan_from_type(plan_type) or plan_type == "premium_30d":
        ok, plan_label, expires_at = await _finalize_premium_request(
            req,
            f"admin:{admin_id}",
            txn_id=str(req.get("txn_id", "") or ""),
            extend_existing=False,
            grant_type="manual_review",
            send_tutorial=True,
        )
        if not ok:
            latest = await store.get_payment_request(req_id)
            handled_status = str((latest or {}).get("status", "")).strip().lower()
            handled_action = _action_from_status(handled_status)
            if handled_action:
                await _update_admin_payment_messages(
                    req_id=req_id,
                    action=handled_action,
                    actor_admin_id=int((latest or {}).get("admin_id", 0) or 0),
                    note=str((latest or {}).get("note", "") or ""),
                )
            await callback.answer("Already handled by another admin.", show_alert=True)
            return
        admin_note = f"{plan_label} until {_format_expiry_ts(expires_at)}"
    else:
        req_after, changed = await store.transition_payment_request_status(
            req_id,
            ("pending", "submitted", "under_review"),
            "processed",
            note="verified",
            admin_id=admin_id,
        )
        if not req_after:
            await callback.message.reply(format_msg("❌ Not Found", sections=[("", f"Request {code(req_id)} not found.")]), parse_mode="HTML")
            return
        if not changed:
            handled_status = str(req_after.get("status", "")).strip().lower()
            handled_action = _action_from_status(handled_status)
            if handled_action:
                await _update_admin_payment_messages(
                    req_id=req_id,
                    action=handled_action,
                    actor_admin_id=int(req_after.get("admin_id", 0) or 0),
                    note=str(req_after.get("note", "") or ""),
                )
            await callback.answer("Already handled by another admin.", show_alert=True)
            return
        balance = await store.add_credits(user_id, credits)
        admin_note = f"{credits} credits added. New balance: {balance}"
        await store.set_payment_request_status(req_id, "processed", note=admin_note, admin_id=admin_id)
        try:
            await bot.send_message(
                user_id,
                format_msg("✅ Payment verified", sections=[
                    ("Request ID", code(req_id)),
                    ("Credits Added", code(credits)),
                    ("New Balance", code(balance)),
                ], tip="Your credits are ready to use."),
                parse_mode="HTML",
            )
        except Exception:
            pass
        await _clear_user_payment_prompt(req_id, user_id)

    # Edit admin message to show approved state
    try:
        await callback.message.edit_caption(
            caption=(callback.message.caption or "") + f"\n\n✅ <b>Approved by admin</b> — {esc(admin_note)}",
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_text(
                (callback.message.text or "") + f"\n\n✅ <b>Approved</b> — {esc(admin_note)}",
                parse_mode="HTML",
                reply_markup=None,
            )
        except Exception:
            pass

    skip_msg = None
    if callback.message:
        skip_msg = (callback.message.chat.id, callback.message.message_id)
    await _update_admin_payment_messages(
        req_id=req_id,
        action="approved",
        actor_admin_id=admin_id,
        note=admin_note,
        skip_message=skip_msg,
    )

    await _broadcast_payment_resolution(
        req_id=req_id,
        user_id=user_id,
        amount=amount,
        credits=credits,
        plan_type=plan_type,
        action="approved",
        actor_admin_id=admin_id,
        note=admin_note,
    )


# ---------------------------------------------------------------------------
#  Admin Inline: ❌ Reject
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("rjt:"))
async def admin_reject_callback(callback: CallbackQuery) -> None:
    if not await _enforce_callback_cooldown(callback):
        return
    admin_id = callback.from_user.id
    if not is_admin(admin_id):
        await callback.answer("❌ Not allowed.", show_alert=True)
        return
    await callback.answer("Rejected.")

    req_id = callback.data.split(":", 1)[1]
    reason = "Rejected by admin"
    req, changed = await store.transition_payment_request_status(
        req_id,
        ("pending", "processing", "awaiting_screenshot", "under_review", "submitted"),
        "rejected",
        note=reason,
        admin_id=admin_id,
    )
    if not req:
        await callback.message.reply(format_msg("❌ Not Found", sections=[("", f"Request {code(req_id)} not found.")]), parse_mode="HTML")
        return
    if not changed:
        handled_status = str(req.get("status", "")).strip().lower()
        handled_action = _action_from_status(handled_status)
        if handled_action:
            await _update_admin_payment_messages(
                req_id=req_id,
                action=handled_action,
                actor_admin_id=int(req.get("admin_id", 0) or 0),
                note=str(req.get("note", "") or ""),
            )
        await callback.answer("Already handled by another admin.", show_alert=True)
        return

    user_id = int(req.get("user_id", 0) or 0)
    await store.clear_pending_utr(user_id)

    try:
        await callback.message.edit_caption(
            caption=(callback.message.caption or "") + f"\n\n❌ <b>Rejected by admin</b>",
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_text(
                (callback.message.text or "") + f"\n\n❌ <b>Rejected</b>",
                parse_mode="HTML",
                reply_markup=None,
            )
        except Exception:
            pass

    try:
        await bot.send_message(
            user_id,
            format_msg("❌ Payment Rejected", sections=[
                ("Request ID", code(req_id)),
                ("Reason", esc(reason)),
            ], tip=f"For help, contact {esc(ADMIN_CONTACT)}."),
            parse_mode="HTML",
        )
    except Exception:
        pass

    skip_msg = None
    if callback.message:
        skip_msg = (callback.message.chat.id, callback.message.message_id)
    await _update_admin_payment_messages(
        req_id=req_id,
        action="rejected",
        actor_admin_id=admin_id,
        note=reason,
        skip_message=skip_msg,
    )

    await _broadcast_payment_resolution(
        req_id=req_id,
        user_id=user_id,
        amount=float(req.get("amount_inr", 0) or 0),
        credits=int(req.get("credits", 0) or 0),
        plan_type=str(req.get("plan_type", "credits")),
        action="rejected",
        actor_admin_id=admin_id,
        note=reason,
    )


# ---------------------------------------------------------------------------
#  Admin Commands
# ---------------------------------------------------------------------------

@dp.message(Command("approve"))
async def approve_cmd(message: Message) -> None:
    admin_id = message.from_user.id if message.from_user else 0
    if not is_admin(admin_id):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/approve <request_id>"))]), parse_mode="HTML")
        return
    req_id = parts[1].strip()
    req = await store.get_payment_request(req_id)
    if not req:
        await message.reply(format_msg("❌ Not Found", sections=[("", f"No request {code(req_id)}.")]), parse_mode="HTML")
        return

    plan_type = str(req.get("plan_type", "credits") or "credits")
    credits = int(req.get("credits", 0) or 0)
    user_id = int(req.get("user_id", 0) or 0)
    await store.clear_pending_utr(user_id)
    if _subscription_plan_from_type(plan_type) or plan_type == "premium_30d":
        ok, plan_label, expires_at = await _finalize_premium_request(
            req,
            f"admin:{admin_id}",
            txn_id=str(req.get("txn_id", "") or ""),
            extend_existing=False,
            grant_type="manual_review",
            send_tutorial=True,
        )
        if not ok:
            latest = await store.get_payment_request(req_id)
            handled_status = str((latest or {}).get("status", "")).strip().lower()
            handled_action = _action_from_status(handled_status)
            if handled_action:
                await _update_admin_payment_messages(
                    req_id=req_id,
                    action=handled_action,
                    actor_admin_id=int((latest or {}).get("admin_id", 0) or 0),
                    note=str((latest or {}).get("note", "") or ""),
                )
            await message.reply(format_msg("ℹ️ Already Handled", sections=[("Request", code(req_id))]), parse_mode="HTML")
            return
        result = f"{plan_label} until {_format_expiry_ts(expires_at)}"
    else:
        req_after, changed = await store.transition_payment_request_status(
            req_id,
            ("pending", "submitted", "under_review"),
            "processed",
            note="verified",
            admin_id=admin_id,
        )
        if not req_after:
            await message.reply(format_msg("❌ Not Found", sections=[("", f"No request {code(req_id)}.")]), parse_mode="HTML")
            return
        if not changed:
            handled_status = str(req_after.get("status", "")).strip().lower()
            handled_action = _action_from_status(handled_status)
            if handled_action:
                await _update_admin_payment_messages(
                    req_id=req_id,
                    action=handled_action,
                    actor_admin_id=int(req_after.get("admin_id", 0) or 0),
                    note=str(req_after.get("note", "") or ""),
                )
            await message.reply(format_msg("ℹ️ Already Handled", sections=[("Request", code(req_id))]), parse_mode="HTML")
            return
        balance = await store.add_credits(user_id, credits)
        result = f"{credits} credits • balance: {balance}"
        await store.set_payment_request_status(req_id, "processed", note=result, admin_id=admin_id)
        try:
            await bot.send_message(
                user_id,
                format_msg("✅ Payment verified", sections=[("Credits Added", code(credits)), ("New Balance", code(balance))], tip="Credits are ready."),
                parse_mode="HTML",
            )
        except Exception:
            pass
        await _clear_user_payment_prompt(req_id, user_id)
    await message.reply(format_msg("✅ Approved", sections=[("Request", code(req_id)), ("Result", esc(result))]), parse_mode="HTML")
    await _update_admin_payment_messages(
        req_id=req_id,
        action="approved",
        actor_admin_id=admin_id,
        note=result,
    )
    await _broadcast_payment_resolution(
        req_id=req_id,
        user_id=user_id,
        amount=float(req.get("amount_inr", 0) or 0),
        credits=credits,
        plan_type=plan_type,
        action="approved",
        actor_admin_id=admin_id,
        note=result,
    )


@dp.message(Command("reject"))
async def reject_cmd(message: Message) -> None:
    admin_id = message.from_user.id if message.from_user else 0
    if not is_admin(admin_id):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/reject <request_id> [reason]"))]), parse_mode="HTML")
        return
    req_id = parts[1].strip()
    reason = parts[2].strip() if len(parts) >= 3 else "No reason provided"
    req, changed = await store.transition_payment_request_status(
        req_id,
        ("pending", "processing", "awaiting_screenshot", "under_review", "submitted"),
        "rejected",
        note=reason,
        admin_id=admin_id,
    )
    if not req:
        await message.reply(format_msg("❌ Not Found", sections=[("", f"No request {code(req_id)}.")]), parse_mode="HTML")
        return
    if not changed:
        handled_status = str(req.get("status", "")).strip().lower()
        handled_action = _action_from_status(handled_status)
        if handled_action:
            await _update_admin_payment_messages(
                req_id=req_id,
                action=handled_action,
                actor_admin_id=int(req.get("admin_id", 0) or 0),
                note=str(req.get("note", "") or ""),
            )
        await message.reply(format_msg("ℹ️ Already Handled", sections=[("Request", code(req_id))]), parse_mode="HTML")
        return

    user_id = int(req.get("user_id", 0) or 0)
    await store.clear_pending_utr(user_id)
    await message.reply(format_msg("❌ Rejected", sections=[("Request", code(req_id)), ("Reason", esc(reason))]), parse_mode="HTML")
    try:
        await bot.send_message(user_id, format_msg("❌ Payment Rejected", sections=[("Request ID", code(req_id)), ("Reason", esc(reason))], tip=f"Contact {esc(ADMIN_CONTACT)} for help."), parse_mode="HTML")
    except Exception:
        pass
    await _update_admin_payment_messages(
        req_id=req_id,
        action="rejected",
        actor_admin_id=admin_id,
        note=reason,
    )
    await _broadcast_payment_resolution(
        req_id=req_id,
        user_id=user_id,
        amount=float(req.get("amount_inr", 0) or 0),
        credits=int(req.get("credits", 0) or 0),
        plan_type=str(req.get("plan_type", "credits")),
        action="rejected",
        actor_admin_id=admin_id,
        note=reason,
    )


@dp.message(Command("payments"))
async def payments_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    status = parts[1].strip().lower() if len(parts) >= 2 else "all"
    if status == "approved":
        status = "processed"
    limit = 20
    if len(parts) >= 3:
        try:
            limit = max(1, min(int(parts[2]), 100))
        except Exception:
            pass
    rows = await store.list_payment_requests(status=status, limit=limit)
    if not rows:
        await message.reply(format_msg("📄 Payments", sections=[("", f"No requests for status: {code(status)}")]), parse_mode="HTML")
        return
    lines = [
        f"• {code(r.get('id'))} — {esc(str(r.get('status')))} | uid:{code(r.get('user_id'))} | {esc(_payment_plan_label(str(r.get('plan_type', 'credits')), int(r.get('credits', 0) or 0)))} | INR {_format_money(float(r.get('amount_inr', 0)))}"
        for r in rows
    ]
    await message.reply(format_msg(f"📄 Payments ({esc(status)})", sections=[("", "\n".join(lines))]), parse_mode="HTML")


@dp.message(Command("paydb"))
async def paydb_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    rows = await store.list_payment_requests(status="all", limit=1000)
    wb = Workbook()
    ws = wb.active
    ws.title = "payments"
    ws.append(["id", "user_id", "amount_inr", "credits", "plan_type", "gateway", "status", "note", "created_at", "updated_at", "expires_at", "admin_id", "approved_by", "grant_type", "txn_id"])
    for r in rows:
        ws.append([r.get("id"), r.get("user_id"), r.get("amount_inr"), r.get("credits"), r.get("plan_type"), r.get("gateway"), r.get("status"), r.get("note"), r.get("created_at"), r.get("updated_at"), r.get("expires_at"), r.get("admin_id"), r.get("approved_by"), r.get("grant_type"), r.get("txn_id")])
    data = io.BytesIO()
    wb.save(data)
    await bot.send_document(message.chat.id, document=BufferedInputFile(data.getvalue(), filename="payments.xlsx"))


@dp.message(Command("resetpaydb"))
async def resetpaydb_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip().lower() != "confirm":
        await message.reply(format_msg("⚠️ Confirm Required", sections=[("Usage", code("/resetpaydb confirm"))]), parse_mode="HTML")
        return
    deleted = await store.reset_payment_requests()
    await message.reply(format_msg("🧹 Payment DB Reset", sections=[("Removed", code(deleted)), ("Next ID", "001")]), parse_mode="HTML")


@dp.message(Command("addsection", "addsections"))
async def addsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/addsection <name>"))]), parse_mode="HTML")
        return
    section_name = parts[1].strip()
    if not section_name:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/addsection <name>"))]), parse_mode="HTML")
        return
    sid = await store.set_section(section_name)
    if not sid:
        await message.reply(format_msg("❌ Failed", sections=[("", "Section already exists or invalid name.")]), parse_mode="HTML")
        return
    section_msg = format_msg(
        "✅ Section Created",
        sections=[
            ("Name", esc(section_name)),
            ("ID", code(sid)),
            ("Link", _section_link_value(sid)),
        ],
        tip="Uploads will now be mapped to this section.",
    )
    try:
        await message.reply(section_msg, parse_mode="HTML")
    except Exception as exc:
        logger.warning("addsection reply failed for %s: %s", sid, exc)
        await message.reply(
            format_msg(
                "✅ Section Created",
                sections=[
                    ("Name", esc(section_name)),
                    ("ID", code(sid)),
                ],
                tip="Uploads will now be mapped to this section.",
            ),
            parse_mode="HTML",
        )


@dp.message(Command("endsection"))
async def endsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    await store.set_section(None)
    await message.reply(format_msg("🛑 Section Ended", sections=[("", "Uploads will not be mapped until /addsection is used again.")]), parse_mode="HTML")


@dp.message(Command("delsection"))
async def delsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/delsection <name>"))]), parse_mode="HTML")
        return
    ok = await store.delete_section(parts[1].strip())
    if ok:
        await message.reply(format_msg("🗑️ Deleted", sections=[("Name", code(esc(parts[1].strip())))]), parse_mode="HTML")
    else:
        await message.reply(format_msg("❌ Not Found", sections=[("", "No section with that name.")]), parse_mode="HTML")


@dp.message(Command("showsections", "showsection", "sections"))
async def showsections_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    rows = await store.list_sections()
    if not rows:
        await message.reply(format_msg("📚 Sections", sections=[("", "No sections yet. Use /addsection.")]), parse_mode="HTML")
        return
    rows.sort(key=lambda x: x[0].lower())
    lines = []
    for name, sid in rows:
        views_total, views_unique = await store.get_section_views(sid)
        label = (
            f"• {link(name, f'{settings.base_url}/section/{sid}')}"
            if _is_http_url(f"{settings.base_url}/section/{sid}")
            else f"• {esc(name)} ({code(sid)})"
        )
        lines.append(f"{label} — visits: {code(views_total)} | unique: {code(views_unique)}")
    await message.reply(format_msg("📚 Sections", sections=[("", "\n".join(lines))]), parse_mode="HTML")


@dp.message(Command("publishsection", "publicsection", "makepublic"))
async def publishsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/publishsection <section name or id>"))]), parse_mode="HTML")
        return
    resolved = await _resolve_section(parts[1])
    if not resolved:
        await message.reply(format_msg("❌ Not Found", sections=[("", "No section matched that name or ID. Use /showsections.")]), parse_mode="HTML")
        return
    section_id, section_name = resolved
    await store.set_public_section(section_id, section_name, True)
    public_url = f"{settings.base_url}/sections"
    await message.reply(
        format_msg(
            "✅ Section Published",
            sections=[
                ("Name", esc(section_name)),
                ("ID", code(section_id)),
                ("Public Page", link("Open Public Sections", public_url) if _is_http_url(public_url) else code(public_url)),
                ("Section", _section_link_value(section_id)),
            ],
            tip="This section is now visible on the website public sections page.",
        ),
        parse_mode="HTML",
    )


@dp.message(Command("unpublishsection", "privatesection", "hidepublic"))
async def unpublishsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/unpublishsection <section name or id>"))]), parse_mode="HTML")
        return
    resolved = await _resolve_section(parts[1])
    if not resolved:
        await message.reply(format_msg("❌ Not Found", sections=[("", "No section matched that name or ID. Use /showsections.")]), parse_mode="HTML")
        return
    section_id, section_name = resolved
    await store.set_public_section(section_id, section_name, False)
    await message.reply(
        format_msg(
            "🙈 Section Hidden",
            sections=[("Name", esc(section_name)), ("ID", code(section_id))],
            tip="This section is no longer visible on the website public sections page.",
        ),
        parse_mode="HTML",
    )


@dp.message(Command("publicsections", "showpublic"))
async def publicsections_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    rows = await store.list_public_sections()
    if not rows:
        await message.reply(format_msg("🌐 Public Sections", sections=[("", "No public sections yet. Use /publishsection.")]), parse_mode="HTML")
        return
    rows.sort(key=lambda x: x[0].lower())
    lines = []
    for name, sid in rows:
        section_url = f"{settings.base_url}/section/{sid}"
        label = link(name, section_url) if _is_http_url(section_url) else f"{esc(name)} ({code(sid)})"
        lines.append(f"• {label}")
    public_url = f"{settings.base_url}/sections"
    await message.reply(
        format_msg(
            "🌐 Public Sections",
            sections=[
                ("Website", link("Open Public Page", public_url) if _is_http_url(public_url) else code(public_url)),
                ("", "\n".join(lines)),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("addtrending"))
async def addtrending_cmd(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    await state.clear()
    await state.set_state(TrendState.waiting_bar)
    await state.update_data(prompt_ids=[])
    await _send_trending_prompt(message, state, "🔥 Add Trending", "Step 1/6: Send the bar/category name, e.g. Latest, Top, VIP.")


@dp.message(StateFilter(TrendState.waiting_bar), F.text)
async def trending_bar_handler(message: Message, state: FSMContext) -> None:
    bar = (message.text or "").strip()
    if not bar or bar.startswith("/"):
        await _send_trending_prompt(message, state, "⚠️ Invalid", "Send a bar/category name, not a command.")
        return
    await state.update_data(bar=bar)
    await state.set_state(TrendState.waiting_title)
    await _send_trending_prompt(message, state, "🔥 Add Trending", "Step 2/6: Send the card title.")


@dp.message(StateFilter(TrendState.waiting_title), F.text)
async def trending_title_handler(message: Message, state: FSMContext) -> None:
    title = (message.text or "").strip()
    if not title or title.startswith("/"):
        await _send_trending_prompt(message, state, "⚠️ Invalid", "Send a title, not a command.")
        return
    await state.update_data(title=title, media=[])
    await state.set_state(TrendState.waiting_media)
    await _send_trending_prompt(message, state, "🔥 Add Trending", "Step 3/6: Send preview photos/videos one by one. Send /done when finished.")


@dp.message(StateFilter(TrendState.waiting_media), F.photo | F.video)
async def trending_media_handler(message: Message, state: FSMContext) -> None:
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        media_file_id = message.video.file_id
        media_type = "video"
    else:
        await _send_trending_prompt(message, state, "⚠️ Invalid", "Send a photo or video preview.")
        return
    data = await state.get_data()
    media = list(data.get("media", []) or [])
    media.append({"file_id": media_file_id, "type": media_type})
    await state.update_data(media=media, media_file_id=media_file_id, media_type=media_type)
    await _send_trending_prompt(
        message,
        state,
        "✅ Preview Added",
        f"{len(media)} preview saved. Send another photo/video, or send /done to continue.",
    )


@dp.message(StateFilter(TrendState.waiting_media), Command("done"))
async def trending_media_done_handler(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    media = list(data.get("media", []) or [])
    if not media:
        await _send_trending_prompt(message, state, "⚠️ Add Preview", "Send at least one photo or video preview before /done.")
        return
    await state.set_state(TrendState.waiting_description)
    await _send_trending_prompt(message, state, "🔥 Add Trending", "Step 4/6: Send the description.")


@dp.message(StateFilter(TrendState.waiting_media))
async def trending_media_wrong_handler(message: Message, state: FSMContext) -> None:
    await _send_trending_prompt(message, state, "⚠️ Invalid", "Send a photo/video preview, or send /done after adding previews.")


@dp.message(StateFilter(TrendState.waiting_description), F.text)
async def trending_description_handler(message: Message, state: FSMContext) -> None:
    description = (message.text or "").strip()
    if not description or description.startswith("/"):
        await _send_trending_prompt(message, state, "⚠️ Invalid", "Send a description, not a command.")
        return
    await state.update_data(description=description)
    await state.set_state(TrendState.waiting_normal_link)
    await _send_trending_prompt(message, state, "🔥 Add Trending", "Step 5/6: Send the normal link.")


@dp.message(StateFilter(TrendState.waiting_normal_link), F.text)
async def trending_normal_link_handler(message: Message, state: FSMContext) -> None:
    normal_link = (message.text or "").strip()
    if not _is_http_url(normal_link):
        await _send_trending_prompt(message, state, "⚠️ Invalid Link", "Send a valid normal link starting with http:// or https://.")
        return
    await state.update_data(normal_link=normal_link)
    await state.set_state(TrendState.waiting_premium_link)
    await _send_trending_prompt(message, state, "🔥 Add Trending", "Step 6/6: Send the premium link.")


@dp.message(StateFilter(TrendState.waiting_premium_link), F.text)
async def trending_premium_link_handler(message: Message, state: FSMContext) -> None:
    premium_link = (message.text or "").strip()
    if not _is_http_url(premium_link):
        await _send_trending_prompt(message, state, "⚠️ Invalid Link", "Send a valid premium link starting with http:// or https://.")
        return
    data = await state.get_data()
    item = await store.add_trending_item({
        "bar": data.get("bar", ""),
        "title": data.get("title", ""),
        "description": data.get("description", ""),
        "media": data.get("media", []),
        "media_file_id": data.get("media_file_id", ""),
        "media_type": data.get("media_type", ""),
        "normal_link": data.get("normal_link", ""),
        "premium_link": premium_link,
        "created_by": message.from_user.id if message.from_user else 0,
    })
    await _delete_trending_prompts(state)
    await state.clear()

    trending_url = _trending_page_url()
    reply_markup = None
    if _is_http_url(trending_url):
        reply_markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Open Trending", url=trending_url)]
        ])
    await message.reply(
        format_msg(
            "✅ Content Added",
            sections=[
                ("ID", code(item.get("id", ""))),
                ("Bar", esc(item.get("bar", ""))),
                ("Title", esc(item.get("title", ""))),
                ("Previews", str(len(item.get("media", []) or []))),
            ],
            tip="Trending card is now live on the website.",
        ),
        parse_mode="HTML",
        reply_markup=reply_markup,
    )


@dp.message(Command("trendinglist", "showtrending"))
async def trendinglist_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    items = await store.list_trending_items(30)
    if not items:
        await message.reply(format_msg("🔥 Trending", sections=[("", "No trending items yet. Use /addtrending.")]), parse_mode="HTML")
        return
    lines = []
    for item in items:
        lines.append(f"• {code(item.get('id', ''))} | {esc(item.get('bar', 'Trending'))} | {esc(item.get('title', '-'))}")
    await message.reply(format_msg("🔥 Trending Items", sections=[("", "\n".join(lines))]), parse_mode="HTML")


@dp.message(Command("deltrending"))
async def deltrending_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/deltrending <id>"))]), parse_mode="HTML")
        return
    item_id = parts[1].strip()
    ok = await store.delete_trending_item(item_id)
    if ok:
        await message.reply(format_msg("🗑️ Trending Deleted", sections=[("ID", code(item_id))]), parse_mode="HTML")
    else:
        await message.reply(format_msg("❌ Not Found", sections=[("", "No trending item found with that ID.")]), parse_mode="HTML")


@dp.message(Command("sethomesection", "home_section", "sethome"))
async def sethomesection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/sethomesection <section name or id>"))]), parse_mode="HTML")
        return
    resolved = await _resolve_section(parts[1])
    if not resolved:
        await message.reply(format_msg("❌ Not Found", sections=[("", "No section matched that name or ID. Use /showsections.")]), parse_mode="HTML")
        return
    section_id, section_name = resolved
    await store.set_home_section(section_id, section_name)
    await message.reply(
        format_msg(
            "✅ Homepage Section Set",
            sections=[
                ("Name", esc(section_name)),
                ("ID", code(section_id)),
                ("Website", link("Open Homepage", settings.base_url) if _is_http_url(settings.base_url) else code(settings.base_url)),
                ("Section", _section_link_value(section_id)),
            ],
            tip="Homepage will show only this selected route.",
        ),
        parse_mode="HTML",
    )


@dp.message(Command("home", "homesection"))
async def homesection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    section_id, section_name = await store.get_home_section()
    if not section_id:
        await message.reply(format_msg("🏠 Homepage Section", sections=[("", "No homepage section selected."), ("Usage", code("/sethomesection <section name or id>"))]), parse_mode="HTML")
        return
    await message.reply(
        format_msg(
            "🏠 Homepage Section",
            sections=[
                ("Name", esc(section_name or section_id)),
                ("ID", code(section_id)),
                ("Link", _section_link_value(section_id)),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("clearhomesection", "unsethome"))
async def clearhomesection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    await store.set_home_section(None)
    await message.reply(format_msg("🧹 Homepage Section Cleared", sections=[("", "Homepage route card is now hidden.")]), parse_mode="HTML")


@dp.message(Command("add"))
async def add_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/add <userid> <period_days|life>"))]), parse_mode="HTML")
        return
    try:
        uid = int(parts[1]); period = parse_period(parts[2])
    except Exception:
        await message.reply(format_msg("❌ Invalid", sections=[("", "User ID must be a number. Period: days or 'life'.")]), parse_mode="HTML")
        return
    await db.add_user(uid, period)
    await message.reply(format_msg("✨ Premium Updated", sections=[("User ID", code(uid)), ("Period", "Lifetime ♾️" if period is None else f"{code(period)} days")]), parse_mode="HTML")


@dp.message(Command("addpremium"))
async def addpremium_cmd(message: Message) -> None:
    admin_id = message.from_user.id if message.from_user else 0
    if not is_admin(admin_id):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return

    parts = (message.text or "").split()
    target_user_id = 0
    plan_code = ""
    if message.reply_to_message and len(parts) >= 2:
        target_user_id = int(message.reply_to_message.from_user.id if message.reply_to_message.from_user else 0)
        plan_code = parts[1].strip().lower()
    elif len(parts) >= 3:
        try:
            target_user_id = int(parts[1])
        except Exception:
            target_user_id = 0
        plan_code = parts[2].strip().lower()
    else:
        await message.reply(format_msg("⚠️ Usage", sections=[("", bullet([code("/addpremium <user_id> <daily|weekly|monthly>"), "Reply to a user: /addpremium <daily|weekly|monthly>"]))]), parse_mode="HTML")
        return

    plan = PAYMENT_PLANS.get(plan_code)
    if target_user_id <= 0 or plan is None:
        await message.reply(format_msg("❌ Invalid", sections=[("", "Valid plan codes: daily, weekly, monthly.")]), parse_mode="HTML")
        return

    order_id = _payment_order_id()
    expires_at = await _set_user_auth(target_user_id, plan.auth_seconds, plan.label, extend_existing=True)
    await store.create_payment_request(
        order_id,
        target_user_id,
        0.0,
        0,
        plan_type=_subscription_plan_type(plan.code),
        gateway="manual",
        expires_at=expires_at,
        approved_by=f"admin:{admin_id}",
        grant_type="manual_admin",
    )
    await store.update_payment_request(
        order_id,
        {
            "status": "delivered",
            "expires_at": expires_at,
            "approved_by": f"admin:{admin_id}",
            "grant_type": "manual_admin",
            "note": f"manual grant: {plan.label}",
        },
    )
    await message.reply(
        format_msg("✅ Premium Granted", sections=[("User ID", code(target_user_id)), ("Plan", esc(plan.label)), ("Order ID", code(order_id)), ("Valid Till", esc(_format_expiry_ts(expires_at)))]),
        parse_mode="HTML",
    )
    try:
        await bot.send_message(
            target_user_id,
            format_msg("✨ Premium Activated", sections=[("Plan", esc(plan.label)), ("Valid Till", esc(_format_expiry_ts(expires_at)))]),
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.message(Command("paysettings"))
async def paysettings_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    settings_doc = await _get_payment_settings()
    await message.reply(
        format_msg("💳 Payment Settings", sections=[
            ("Gateway", esc(str(settings_doc.get("payment_gateway", "manual")).title())),
            ("XWallet Key", "Set" if str(settings_doc.get("xwallet_api_key", "")).strip() else "Missing"),
            ("Tutorial", "Configured" if int(settings_doc.get("tutorial_message_id", 0) or 0) > 0 else "Not set"),
            ("Earnings", f"INR {_format_money(float(settings_doc.get('total_earnings', 0.0) or 0.0))}"),
        ]),
        parse_mode="HTML",
    )


@dp.message(Command("setgateway"))
async def setgateway_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/setgateway <manual|xwallet>"))]), parse_mode="HTML")
        return
    gateway = _resolve_gateway_name(parts[1])
    await store.update_payment_settings({"payment_gateway": gateway})
    await message.reply(format_msg("✅ Gateway Updated", sections=[("Gateway", esc(gateway.title()))]), parse_mode="HTML")


@dp.message(Command("setxwalletkey"))
async def setxwalletkey_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/setxwalletkey <api_key>"))]), parse_mode="HTML")
        return
    await store.update_payment_settings({"xwallet_api_key": parts[1].strip()})
    await message.reply(format_msg("✅ XWallet Key Saved", sections=[("", "Gateway key updated.")]), parse_mode="HTML")


@dp.message(Command("settutorial"))
async def settutorial_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) >= 2 and parts[1].strip().lower() == "clear":
        await store.update_payment_settings({"tutorial_chat_id": 0, "tutorial_message_id": 0})
        await message.reply(format_msg("🧹 Tutorial Cleared", sections=[("", "Tutorial video removed.")]), parse_mode="HTML")
        return
    if not message.reply_to_message:
        await message.reply(format_msg("⚠️ Usage", sections=[("", bullet([code("/settutorial"), "Reply to a tutorial video/document with this command", code("/settutorial clear")]))]), parse_mode="HTML")
        return
    await store.update_payment_settings({"tutorial_chat_id": message.reply_to_message.chat.id, "tutorial_message_id": message.reply_to_message.message_id})
    await message.reply(format_msg("✅ Tutorial Saved", sections=[("Chat ID", code(message.reply_to_message.chat.id)), ("Message ID", code(message.reply_to_message.message_id))]), parse_mode="HTML")


@dp.message(Command("credit_add"))
async def credit_add_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/credit_add <user_id> <amount>"))]), parse_mode="HTML")
        return
    try:
        uid = int(parts[1]); amt = int(parts[2])
    except Exception:
        await message.reply(format_msg("❌ Invalid", sections=[("", "Both values must be integers.")]), parse_mode="HTML")
        return
    bal = await store.add_credits(uid, amt)
    await message.reply(format_msg("➕ Credits Added", sections=[("User ID", code(uid)), ("Added", code(amt)), ("New Balance", code(bal))]), parse_mode="HTML")


@dp.message(Command("credit_remove"))
async def credit_remove_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/credit_remove <user_id> <amount>"))]), parse_mode="HTML")
        return
    try:
        uid = int(parts[1]); amt = int(parts[2])
    except Exception:
        await message.reply(format_msg("❌ Invalid", sections=[("", "Both values must be integers.")]), parse_mode="HTML")
        return
    ok, bal = await store.charge_credits(uid, amt)
    if not ok:
        await message.reply(format_msg("⚠️ Insufficient", sections=[("Requested", code(amt)), ("Balance", code(bal))]), parse_mode="HTML")
        return
    await message.reply(format_msg("➖ Credits Removed", sections=[("User ID", code(uid)), ("Removed", code(amt)), ("New Balance", code(bal))]), parse_mode="HTML")


@dp.message(Command("stats"))
async def stats_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    users = await store.list_known_user_ids(limit=100000)
    admins = await db.list_admins()
    sections = await store.list_sections()
    premium_rows = await db.list_premium_users()
    active_premium = 0
    for row in premium_rows:
        if await db.is_premium(row.user_id):
            active_premium += 1
    await message.reply(
        format_msg("📊 Bot Stats", sections=[
            ("Users", code(len(users))),
            ("Admins", code(len(admins))),
            ("Sections", code(len(sections))),
            ("Active Premium", code(active_premium)),
        ]),
        parse_mode="HTML",
    )


@dp.message(Command("history"))
async def history_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    limit = 20
    if len(parts) >= 2:
        try:
            limit = max(1, min(int(parts[1]), 100))
        except Exception:
            pass
    tokens = await store.list_recent(limit * 2)
    lines = []
    for token in tokens:
        ref = await store.get(token, settings.token_ttl_seconds)
        if not ref:
            continue
        lines.append(f"• [{esc(ref.access)}] {esc(ref.section_name or '-')}: {link('🔗 Open', build_link(token))}")
        if len(lines) >= limit:
            break
    if not lines:
        await message.reply(format_msg("📜 History", sections=[("", "No history yet.")]), parse_mode="HTML")
        return
    await message.reply(format_msg("📜 Recent Uploads", sections=[("", "\n".join(lines))]), parse_mode="HTML")


@dp.message(Command("premiumlist"))
async def premiumlist_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    rows = await db.list_premium_users()
    if not rows:
        await message.reply(format_msg("✨ Premium Users", sections=[("", "No premium users yet.")]), parse_mode="HTML")
        return
    now = int(time.time())
    lines = []
    for row in rows[:100]:
        exp = _format_expiry(row.expires_at, now_ts=now)
        lines.append(f"• {code(row.user_id)} → {esc(exp)}")
    await message.reply(format_msg("✨ Premium Users", sections=[("", "\n".join(lines))]), parse_mode="HTML")


@dp.message(Command("setupi"))
async def setupi_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        current = await store.get_upi_id()
        await message.reply(format_msg("💳 UPI ID", sections=[("Current", code(current or "Not set"))]), parse_mode="HTML")
        return
    upi = await store.set_upi_id(parts[1].strip())
    await message.reply(format_msg("✅ UPI Updated", sections=[("New UPI", code(esc(upi)))]), parse_mode="HTML")


@dp.message(Command("setautodelete"))
async def setautodelete_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        current = await store.get_auto_delete()
        if current:
            mins = current // 60; secs = current % 60
            display = f"{mins}m {secs}s" if mins else f"{secs}s"
            status = f"{code(current)}s ({esc(display)})"
        else:
            status = "Disabled"
        await message.reply(
            format_msg("⏳ Auto-Delete Setting", sections=[
                ("Current", status),
                ("", ""),
                ("Usage", code("/setautodelete <seconds>")),
                ("", bullet(["e.g. /setautodelete 300 -> 5 minutes", "/setautodelete 0 -> disable"])),
            ]),
            parse_mode="HTML",
        )
        return
    try:
        seconds = int(parts[1].strip())
    except Exception:
        await message.reply(format_msg("❌ Invalid", sections=[("", "Provide a number in seconds. e.g. /setautodelete 300")]), parse_mode="HTML")
        return
    saved = await store.set_auto_delete(seconds)
    if saved == 0:
        await message.reply(format_msg("✅ Auto-Delete Disabled", sections=[("", "Delivered files will no longer be auto-deleted.")]), parse_mode="HTML")
    else:
        mins = saved // 60; secs = saved % 60
        display = f"{mins}m {secs}s" if mins else f"{secs}s"
        await message.reply(
            format_msg("✅ Auto-Delete Updated", sections=[
                ("Delay", f"{code(saved)}s ({esc(display)})"),
                ("", "Delivered files will be deleted after this time."),
            ]),
            parse_mode="HTML",
        )

@dp.message(Command("setthumbnail"))
async def setthumbnail_cmd(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    await state.set_state(ThumbState.waiting_photo)
    await message.reply(format_msg("🖼️ Set Thumbnail", sections=[("", "Please send a photo to be used as a thumbnail for delivered media.")]), parse_mode="HTML")

@dp.message(StateFilter(ThumbState.waiting_photo), F.photo)
async def wait_thumb_photo(message: Message, state: FSMContext) -> None:
    fid = message.photo[-1].file_id
    await store.set_thumbnail(fid)
    await store.set_thumbnail_enabled(True)
    await state.clear()
    await message.reply(format_msg("✅ Thumbnail Set", sections=[("", "This photo will now be attached to videos and documents.")]), parse_mode="HTML")

@dp.message(StateFilter(ThumbState.waiting_photo))
async def wait_thumb_not_photo(message: Message, state: FSMContext) -> None:
    await message.reply(format_msg("❌ Invalid", sections=[("", "Please send a photo. /cancel to abort.")]), parse_mode="HTML")

@dp.message(Command("delthumbnail"))
async def delthumbnail_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    await store.del_thumbnail()
    await message.reply(format_msg("🗑️ Thumbnail Removed", sections=[("", "No thumbnail will be sent.")]), parse_mode="HTML")

@dp.message(Command("thumbnail"))
async def thumbnail_toggle_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        enabled = await store.get_thumbnail_enabled()
        has_thumb = bool(await store.get_thumbnail())
        status = "✅ Enabled" if enabled else "⛔ Disabled"
        if enabled and not has_thumb:
            status += " (But no photo set)"
        await message.reply(
            format_msg("🖼️ Thumbnail Status", sections=[
                ("Status", status),
                ("", ""),
                ("Usage", bullet([code("/thumbnail on"), code("/thumbnail off")])),
            ]),
            parse_mode="HTML",
        )
        return
    val = parts[1].strip().lower()
    if val in {"on", "yes", "true", "1"}:
        await store.set_thumbnail_enabled(True)
        await message.reply(format_msg("✅ Settings Updated", sections=[("Thumbnail", "✅ Enabled")]), parse_mode="HTML")
    elif val in {"off", "no", "false", "0"}:
        await store.set_thumbnail_enabled(False)
        await message.reply(format_msg("✅ Settings Updated", sections=[("Thumbnail", "⛔ Disabled")]), parse_mode="HTML")
    else:
        await message.reply(format_msg("❌ Invalid", sections=[("", "Use 'on' or 'off'.")]), parse_mode="HTML")


@dp.message(Command("broadcast"))
async def broadcast_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/broadcast <text>"))]), parse_mode="HTML")
        return
    txt = parts[1].strip()
    targets = set(await store.list_known_user_ids(limit=100000))
    for row in await db.list_premium_users():
        targets.add(int(row.user_id))

    sender_id = message.from_user.id if message.from_user else 0
    if sender_id in targets:
        targets.remove(sender_id)

    if not targets:
        await message.reply(format_msg("📣 Broadcast", sections=[("", "No users yet.")]), parse_mode="HTML")
        return
    sent = failed = 0
    for uid in sorted(targets):
        try:
            await bot.send_message(uid, txt)
            sent += 1
        except Exception as exc:
            retry_after = getattr(exc, "retry_after", None)
            if isinstance(retry_after, (int, float)) and retry_after > 0:
                await asyncio.sleep(float(retry_after))
                try:
                    await bot.send_message(uid, txt)
                    sent += 1
                    continue
                except Exception:
                    pass
            failed += 1
    await message.reply(format_msg("✅ Broadcast Complete", sections=[("Sent", code(sent)), ("Failed", code(failed))]), parse_mode="HTML")


@dp.message(Command("setcreditprice"))
async def setcreditprice_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/setcreditprice <price>"))]), parse_mode="HTML")
        return
    try:
        price = float(parts[1])
    except Exception:
        await message.reply(format_msg("❌ Invalid", sections=[("", "Price must be a number.")]), parse_mode="HTML")
        return
    _, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    await store.set_pay_plan(price, template)
    await message.reply(format_msg("✅ Price Updated", sections=[("New Price", f"INR {_format_money(price)} per credit")]), parse_mode="HTML")


@dp.message(Command("setpay"))
async def setpay_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", bullet([code("/setpay view"), code("/setpay text <msg>")]))]), parse_mode="HTML")
        return
    sub = parts[1].strip().lower()
    price, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    if sub == "view":
        await message.reply(format_msg("💳 Pay Plan", sections=[("Price", f"INR {_format_money(price)}"), ("Template", f"\n{esc(template)}")]), parse_mode="HTML")
        return
    if sub == "text":
        if len(parts) < 3:
            await message.reply(format_msg("⚠️ Usage", sections=[("", code("/setpay text <msg>"))]), parse_mode="HTML")
            return
        await store.set_pay_plan(price, parts[2].strip())
        await message.reply(format_msg("✅ Payment Text Updated", sections=[]), parse_mode="HTML")
        return
    await message.reply(format_msg("❌ Unknown Option", sections=[("", "Valid: view, text")]), parse_mode="HTML")


@dp.message(Command("db"))
async def db_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    rows = await store.list_credit_balances(limit=20)
    if not rows:
        await message.reply(format_msg("📦 Credit DB", sections=[("", "No data.")]), parse_mode="HTML")
        return
    lines = [f"• {code(uid)} -> {code(bal)}" for uid, bal in rows]
    await message.reply(format_msg("📦 Credit DB (Top 20)", sections=[("", "\n".join(lines))]), parse_mode="HTML")


@dp.message(Command("addadmin"))
async def addadmin_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/addadmin <user_id>"))]), parse_mode="HTML")
        return
    try:
        uid = int(parts[1])
    except Exception:
        await message.reply(format_msg("❌ Invalid", sections=[("", "User ID must be numeric.")]), parse_mode="HTML")
        return
    await db.add_admin(uid)
    settings.admin_ids.add(uid)
    await message.reply(format_msg("✅ Admin Added", sections=[("User ID", code(uid))]), parse_mode="HTML")


@dp.message(Command("showadminlist"))
async def showadminlist_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    admins = await db.list_admins()
    if not admins:
        await message.reply(format_msg("👥 Admins", sections=[("", "No admins configured.")]), parse_mode="HTML")
        return
    await message.reply(format_msg("👥 Admin List", sections=[("", "\n".join(f"• {code(x)}" for x in admins))]), parse_mode="HTML")


@dp.message(Command("redeem"))
async def redeem_cmd(message: Message) -> None:
    await message.reply(format_msg("🎟️ Redeem", sections=[("", "Redeem command coming in next update.")]), parse_mode="HTML")


@dp.message(Command("paymentsdb"))
async def alias_paydb(message: Message) -> None:
    await paydb_cmd(message)


@dp.message(Command("bot", "health"))
async def bot_cmd(message: Message) -> None:
    await message.reply(format_msg("🟢 Bot Status", sections=[("", "I am alive and running.")]), parse_mode="HTML")


@dp.message(F.text)
async def utr_text_fallback_handler(message: Message, state: FSMContext) -> None:
    if message.chat.type != "private":
        return
    if (message.text or "").startswith("/"):
        return
    current_state = await state.get_state()
    if current_state == PayState.waiting_utr.state:
        return
    user_id = message.from_user.id if message.from_user else 0
    if user_id <= 0:
        return
    req_id = await store.get_pending_utr(user_id)
    if not req_id:
        return
    utr = (message.text or "").strip()
    if not utr:
        return
    await _submit_utr(message, state, req_id, utr)


# ---------------------------------------------------------------------------
#  Media upload handler
# ---------------------------------------------------------------------------

@dp.message()
async def private_media_handler(message: Message, state: FSMContext) -> None:
    if message.chat.type not in {"private", "channel"}:
        return

    media = (
        message.document or message.video or message.audio or message.animation
        or message.voice or message.video_note
        or (message.photo[-1] if message.photo else None)
    )

    if not media:
        if (message.text or "").startswith("/"):
            await message.reply(format_msg("❌ Unknown Command", sections=[("", "Use /start to see available commands.")]), parse_mode="HTML")
            return
        if message.chat.type == "private" and is_admin(message.from_user.id if message.from_user else None):
            await message.reply(format_msg("⚠️ Unsupported", sections=[("", "Send a document, video, audio, animation, voice, video_note, or photo.")]), parse_mode="HTML")
        return

    if message.chat.type == "private" and not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Only admins can upload files.")]), parse_mode="HTML")
        return

    section_id, section_name = await store.get_section()
    if not section_id:
        if message.chat.type == "private":
            await message.reply(format_msg("⚠️ No Active Section", sections=[("", f"Set one first: {code('/addsection <name>')}")]), parse_mode="HTML")
        return

    normal_token = secrets.token_urlsafe(24)
    premium_token = secrets.token_urlsafe(24)
    file_name = getattr(media, "file_name", None)
    mime_type = getattr(media, "mime_type", None)
    file_size = getattr(media, "file_size", None)

    media_type = "document"
    if message.video: media_type = "video"
    elif message.audio: media_type = "audio"
    elif message.animation: media_type = "animation"
    elif message.voice: media_type = "voice"
    elif message.video_note: media_type = "video_note"
    elif message.photo: media_type = "photo"

    base_ref = dict(
        file_id=media.file_id, chat_id=message.chat.id, message_id=message.message_id,
        file_unique_id=media.file_unique_id, file_name=file_name, mime_type=mime_type,
        file_size=file_size, media_type=media_type, created_at=time.time(),
        section_id=section_id, section_name=section_name,
    )
    await store.set(normal_token, FileRef(**base_ref, access="normal"), settings.token_ttl_seconds)
    await store.set(premium_token, FileRef(**base_ref, access="premium"), settings.token_ttl_seconds)

    if message.chat.type == "private":
        await message.reply(
            format_msg("✅ File Uploaded", sections=[
                ("Section", esc(section_name or section_id)),
                ("Normal", link("▶️ Open Stream", build_link(normal_token))),
                ("Premium", link("⬇️ Download Stream", build_link(premium_token))),
            ], tip="Normal = stream only. Premium = downloadable."),
            parse_mode="HTML",
        )


# ---------------------------------------------------------------------------
#  Startup / Shutdown
# ---------------------------------------------------------------------------

async def _startup() -> None:
    global _payment_expiry_task
    await store.connect()
    await db.connect()
    for admin_id in settings.admin_ids:
        await db.add_admin(admin_id)
    admins = await db.list_admins()
    settings.admin_ids.update(admins)
    try:
        await bot.set_my_commands(BOT_COMMANDS)
    except Exception as exc:
        logger.warning("set_my_commands failed: %s", exc)
    if _payment_expiry_task is None or _payment_expiry_task.done():
        _payment_expiry_task = asyncio.create_task(_expire_pending_payment_requests_loop())
    for row in await store.pending_xwallet_orders(limit=200):
        _spawn_xwallet_poll(str(row.get("id", "")))
    await _notify_restart()
    logger.info("Bot started (aiogram)")


async def _shutdown() -> None:
    global _payment_expiry_task
    task = _payment_expiry_task
    _payment_expiry_task = None
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    for poll_task in list(_xwallet_poll_tasks.values()):
        poll_task.cancel()
    for poll_task in list(_xwallet_poll_tasks.values()):
        with contextlib.suppress(asyncio.CancelledError):
            await poll_task
    _xwallet_poll_tasks.clear()
    await store.close()
    await db.close()


async def run() -> None:
    dp.startup.register(_startup)
    dp.shutdown.register(_shutdown)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(run())



