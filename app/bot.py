import asyncio
import logging
import re
import secrets
import time
import tempfile
from pathlib import Path
from urllib.parse import quote

from openpyxl import Workbook
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait

from app.config import get_settings
from app.db import PremiumDB
from app.store import FileRef, TokenStore

settings = get_settings()
store = TokenStore(settings.redis_url, history_limit=settings.history_limit)
db = PremiumDB(settings.db_path)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("stream_bot")

app = Client(
    "stream_bot",
    api_id=settings.api_id,
    api_hash=settings.api_hash,
    bot_token=settings.bot_token,
    sleep_threshold=10000,
)

VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".webm", ".avi", ".mpeg", ".mpg", ".m4v"}
CREDIT_COST = 1
DEFAULT_CREDIT_PRICE_INR = 0.35
DEFAULT_PAY_TEXT = "Price per credit: INR {price}\nTo add credits, contact admin."
ADMIN_CONTACT = "@azmoviedeal"
MIN_CUSTOM_PAY_INR = 10.0
DEFAULT_UPI_PAYEE_NAME = "AZ File Conversion"


def build_link(token: str) -> str:
    return f"{settings.base_url}/player/{token}"


def is_admin(user_id: int | None) -> bool:
    if not user_id:
        return False
    return user_id in settings.admin_ids


def parse_period(value: str) -> int | None:
    value = value.strip().lower()
    if value in {"life", "lifetime", "permanent", "perm"}:
        return None
    return int(value)

def is_video_document(message) -> bool:
    if not message.document:
        return False
    mime = (message.document.mime_type or "").lower()
    name = message.document.file_name or ""
    ext = Path(name).suffix.lower()
    return mime.startswith("video/") or ext in VIDEO_EXTS

async def reupload_video_as_media(client: Client, message, target_chat_id):
    if not message.document:
        return None
    caption = message.caption
    with tempfile.TemporaryDirectory() as tmpdir:
        target_path = Path(tmpdir) / (message.document.file_name or "video.mp4")
        download_path = await message.download(file_name=str(target_path))
        if not download_path:
            return None
        return await client.send_video(
            chat_id=target_chat_id,
            video=download_path,
            caption=caption,
        )

def build_reaction_keyboard(token: str, likes: int, dislikes: int, status: int) -> InlineKeyboardMarkup:
    like_label = f"👍 {likes}" + (" ✅" if status == 1 else "")
    dislike_label = f"👎 {dislikes}" + (" ✅" if status == -1 else "")
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(like_label, callback_data=f"react:{token}:up"),
            InlineKeyboardButton(dislike_label, callback_data=f"react:{token}:down"),
        ]]
    )


async def send_reaction_prompt(client: Client, user_id: int, token: str) -> None:
    likes, dislikes, status = await store.get_reactions(token, user_id)
    await client.send_message(
        chat_id=user_id,
        text="Rate this file:",
        reply_markup=build_reaction_keyboard(token, likes, dislikes, status),
    )


DELETE_AFTER_SECONDS = 30 * 60
PAYMENT_QR_DELETE_SECONDS = 15 * 60


async def schedule_delete(client: Client, chat_id: int, message_id: int, delay: int = DELETE_AFTER_SECONDS) -> None:
    await asyncio.sleep(delay)
    try:
        await client.delete_messages(chat_id=chat_id, message_ids=message_id)
    except Exception:
        pass


async def send_premium_file(client: Client, user_id: int, ref: FileRef, protect: bool) -> None:
    try:
        sent = await client.copy_message(
            chat_id=user_id,
            from_chat_id=ref.chat_id,
            message_id=ref.message_id,
            protect_content=protect,
        )
        if sent:
            asyncio.create_task(schedule_delete(client, user_id, sent.id))
        return
    except Exception:
        pass
    sent = await client.send_cached_media(
        chat_id=user_id,
        file_id=ref.file_id,
        protect_content=protect,
    )
    if sent:
        asyncio.create_task(schedule_delete(client, user_id, sent.id))


def parse_send_all_payload(payload: str) -> tuple[str, str] | None:
    if not payload.startswith("sa_"):
        return None
    rest = payload[3:]
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


def render_pay_text(template: str, price: float) -> str:
    formatted_price = f"{price:.2f}"
    try:
        return template.format(price=formatted_price)
    except Exception:
        return f"{template}\nPrice per credit: INR {formatted_price}"


def normalize_plan_text(raw_text: str) -> str:
    # Allow admins to type escaped newlines in Telegram commands.
    return (
        raw_text.replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "\n")
        .strip()
    )


def parse_amount_value(raw: str) -> float | None:
    value = (raw or "").strip().replace(",", "")
    try:
        amount = float(value)
    except Exception:
        return None
    if amount <= 0:
        return None
    return round(amount, 2)


def credits_for_amount(amount_inr: float, price_per_credit: float) -> int:
    if price_per_credit <= 0:
        return 0
    return int(amount_inr // price_per_credit)


def new_payment_request_id() -> str:
    return f"P{int(time.time())}{secrets.token_hex(2).upper()}"


def format_payment_request_line(req: dict) -> str:
    return (
        f"{req.get('id')} | user {req.get('user_id')} | INR {float(req.get('amount_inr', 0)):.2f} "
        f"| credits {req.get('credits')} | {req.get('status')}"
    )


def validate_upi_id(upi_id: str) -> bool:
    value = (upi_id or "").strip()
    if not value or "@" not in value:
        return False
    return bool(re.fullmatch(r"[a-zA-Z0-9._-]{2,}@[a-zA-Z]{2,}", value))


def build_upi_uri(upi_id: str, amount_inr: float, request_id: str) -> str:
    params = {
        "pa": upi_id,
        "pn": DEFAULT_UPI_PAYEE_NAME,
        "am": f"{amount_inr:.2f}",
        "cu": "INR",
        "tn": f"Credits {request_id}",
    }
    qp = "&".join(f"{k}={quote(v, safe='')}" for k, v in params.items())
    return f"upi://pay?{qp}"


def build_upi_qr_url(upi_uri: str) -> str:
    return f"https://quickchart.io/qr?size=600&text={quote(upi_uri, safe='')}"


def build_payment_request_keyboard(request_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Send UTR", callback_data=f"payreq:utr:{request_id}"),
                InlineKeyboardButton("Cancel", callback_data=f"payreq:cancel:{request_id}"),
            ],
            [InlineKeyboardButton("Contact Admin", url=f"https://t.me/{ADMIN_CONTACT.lstrip('@')}")],
        ]
    )


def build_admin_payment_keyboard(request_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Approve", callback_data=f"payadm:approve:{request_id}"),
            InlineKeyboardButton("Reject", callback_data=f"payadm:reject:{request_id}"),
        ]]
    )


async def send_payment_request_message(client: Client, chat_id: int, req: dict, upi_id: str) -> None:
    amount_inr = float(req.get("amount_inr", 0) or 0)
    request_id = str(req.get("id", "")).strip()
    credits = int(req.get("credits", 0) or 0)
    upi_uri = build_upi_uri(upi_id=upi_id, amount_inr=amount_inr, request_id=request_id)
    caption = (
        "PAYMENT CHECKOUT\n\n"
        f"Request ID: {request_id}\n"
        f"Amount: INR {amount_inr:.2f}\n"
        f"Credits: {credits}\n"
        f"UPI ID: {upi_id}\n\n"
        "Steps:\n"
        "1) Open any UPI app and scan this QR.\n"
        "2) Pay the exact amount shown above.\n"
        "3) Tap 'Send UTR' below.\n"
        "4) Send transaction UTR/Ref number.\n"
        "5) Wait for admin approval.\n\n"
        "Use 'Cancel' if you do not want to continue."
    )
    qr_url = build_upi_qr_url(upi_uri)
    keyboard = build_payment_request_keyboard(request_id)
    sent = None
    try:
        sent = await client.send_photo(chat_id=chat_id, photo=qr_url, caption=caption, reply_markup=keyboard)
    except Exception:
        sent = await client.send_message(
            chat_id=chat_id,
            text=caption + f"\n\nUPI link:\n{upi_uri}",
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
    if sent:
        await store.set_payment_prompt(request_id, chat_id, sent.id)
        asyncio.create_task(schedule_delete(client, chat_id, sent.id, PAYMENT_QR_DELETE_SECONDS))



async def delete_payment_prompt_message(client: Client, request_id: str) -> None:
    prompt = await store.get_payment_prompt(request_id)
    if not prompt:
        return
    chat_id, message_id = prompt
    if not chat_id or not message_id:
        await store.clear_payment_prompt(request_id)
        return
    try:
        await client.delete_messages(chat_id=chat_id, message_ids=message_id)
    except Exception:
        pass
    await store.clear_payment_prompt(request_id)


async def notify_admin_payment_submitted(client: Client, req: dict, user, utr: str) -> None:
    username = f"@{user.username}" if getattr(user, "username", None) else "-"
    full_name = (getattr(user, "first_name", "") or "").strip()
    if getattr(user, "last_name", None):
        full_name = f"{full_name} {user.last_name}".strip()
    amount_inr = float(req.get("amount_inr", 0) or 0)
    request_id = str(req.get("id", "")).strip()
    credits = int(req.get("credits", 0) or 0)
    text_msg = (
        "New payment submitted.\n"
        f"Request ID: {request_id}\n"
        f"User ID: {user.id}\n"
        f"Name: {full_name or '-'}\n"
        f"Username: {username}\n"
        f"Amount: INR {amount_inr:.2f}\n"
        f"Credits: {credits}\n"
        f"UTR: {utr}"
    )
    markup = build_admin_payment_keyboard(request_id)
    for admin_id in settings.admin_ids:
        try:
            await client.send_message(chat_id=admin_id, text=text_msg, reply_markup=markup)
        except Exception:
            continue


async def submit_payment_with_utr(client: Client, user, request_id: str, utr: str) -> tuple[bool, str]:
    req = await store.get_payment_request(request_id)
    if not req:
        return False, "Request not found."
    if int(req.get("user_id", 0) or 0) != int(user.id):
        return False, "This request is not yours."
    status = (req.get("status") or "").strip().lower()
    if status in {"approved", "rejected", "cancelled"}:
        return False, f"Request already {status}."
    clean_utr = (utr or "").strip()
    if len(clean_utr) < 6:
        return False, "UTR looks too short. Send full UTR."
    await store.set_payment_request_status(request_id, "submitted", note=clean_utr, admin_id=0)
    await store.clear_pending_utr(user.id)
    req = await store.get_payment_request(request_id)
    if req:
        await notify_admin_payment_submitted(client, req, user, clean_utr)
    return True, "Payment submitted to admin. You will be notified after review."


async def approve_payment_request(client: Client, request_id: str, admin_id: int, note: str) -> tuple[bool, str]:
    req = await store.get_payment_request(request_id)
    if not req:
        return False, "Request not found."
    status = (req.get("status") or "").strip().lower()
    if status == "approved":
        return False, "Already approved."
    if status in {"rejected", "cancelled"}:
        return False, f"Request is {status}."
    credits = int(req.get("credits", 0) or 0)
    user_id = int(req.get("user_id", 0) or 0)
    if credits <= 0 or user_id <= 0:
        return False, "Invalid request data."
    balance = await store.add_credits(user_id, credits)
    await store.set_payment_request_status(request_id, "approved", note=note, admin_id=admin_id)
    await store.clear_pending_utr(user_id)
    await delete_payment_prompt_message(client, request_id)
    try:
        await client.send_message(user_id, f"Your payment {request_id} is approved. Credits added: {credits}. Balance: {balance}")
    except Exception:
        pass
    return True, f"Approved {request_id}. Added {credits} credits to {user_id}. New balance: {balance}"


async def reject_payment_request(client: Client, request_id: str, admin_id: int, reason: str) -> tuple[bool, str]:
    req = await store.get_payment_request(request_id)
    if not req:
        return False, "Request not found."
    status = (req.get("status") or "").strip().lower()
    if status == "approved":
        return False, "Request already approved. Cannot reject now."
    if status == "rejected":
        return False, "Already rejected."
    if status == "cancelled":
        return False, "Request already cancelled."
    user_id = int(req.get("user_id", 0) or 0)
    await store.set_payment_request_status(request_id, "rejected", note=reason, admin_id=admin_id)
    await store.clear_pending_utr(user_id)
    await delete_payment_prompt_message(client, request_id)
    try:
        await client.send_message(user_id, f"Your payment {request_id} was rejected. Reason: {reason}")
    except Exception:
        pass
    return True, f"Rejected {request_id}."


def build_pay_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("10rs", callback_data="payamt:10"),
                InlineKeyboardButton("50rs", callback_data="payamt:50"),
            ],
            [
                InlineKeyboardButton("100rs", callback_data="payamt:100"),
                InlineKeyboardButton("Custom amount (>10rs)", callback_data="payamt:custom"),
            ],
            [InlineKeyboardButton("Contact Admin", url=f"https://t.me/{ADMIN_CONTACT.lstrip('@')}")],
        ]
    )


async def create_payment_request_for_amount(user_id: int, amount_inr: float) -> tuple[dict | None, str | None]:
    price, _ = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    credits = credits_for_amount(amount_inr, price)
    if credits < 1:
        return None, f"Amount is too low. Minimum amount for 1 credit is INR {price:.2f}."
    req = await store.create_payment_request(
        request_id=new_payment_request_id(),
        user_id=user_id,
        amount_inr=amount_inr,
        credits=credits,
    )
    return req, None

async def deliver_token(client: Client, user_id: int, token: str, include_guidance: bool = True) -> bool:
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        return False
    is_premium = await db.is_premium(user_id)
    if ref.access == "premium":
        if is_premium:
            await send_premium_file(client, user_id, ref, protect=False)
            await send_reaction_prompt(client, user_id, token)
            return True
        ok, balance = await store.charge_credits(user_id, CREDIT_COST)
        if not ok:
            await client.send_message(chat_id=user_id, text=f"Not enough credits. Balance: {balance}. 💳")
            return False
        try:
            await send_premium_file(client, user_id, ref, protect=False)
        except Exception:
            await store.add_credits(user_id, CREDIT_COST)
            raise
        await client.send_message(chat_id=user_id, text=f"✅ 1 credit used. Remaining: {balance}")
        await send_reaction_prompt(client, user_id, token)
        return True

    await send_premium_file(client, user_id, ref, protect=True)
    await send_reaction_prompt(client, user_id, token)
    if include_guidance:
        await client.send_message(
            chat_id=user_id,
            text=(
                "Play-only mode enabled (saving/forwarding is blocked). 🔒\n"
                "Want full download access? Use /pay to buy credits or ask for premium."
            ),
        )
    return True


@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message):
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text(
            "Hey! 👋\n"
            "Open a download link here and I’ll deliver the file.\n"
            "Admins: /add, /addsection, /showsections, /credit_add, /setupi."
        )
        return

    if not message.from_user:
        await message.reply_text("I couldn’t read your user info. Please try again. 🙏")
        return

    user_id = message.from_user.id
    payload = parts[1].strip()
    if payload.startswith("dl_"):
        token = payload[3:]
        ok = await deliver_token(client, user_id, token, include_guidance=True)
        if not ok:
            await message.reply_text("This link is expired, invalid, or inaccessible. ⏳")
        return

    send_all = parse_send_all_payload(payload)
    if not send_all:
        await message.reply_text("That link doesn’t look right. Please open a valid download link. 🙏")
        return

    section_id, access_filter = send_all
    tokens = await store.list_section(section_id, settings.history_limit)
    if not tokens:
        await message.reply_text("No files found in this section. 📭")
        return

    selected_tokens: list[str] = []
    for token in tokens:
        ref = await store.get(token, settings.token_ttl_seconds)
        if not ref:
            continue
        if (ref.access or "normal").strip().lower() == access_filter:
            selected_tokens.append(token)
    if not selected_tokens:
        await message.reply_text("No matching files found for this section access. 📭")
        return

    await message.reply_text(f"Sending {len(selected_tokens)} files from section `{section_id}` ({access_filter}).")
    sent_count = 0
    skipped_count = 0
    for token in selected_tokens:
        try:
            ok = await deliver_token(client, user_id, token, include_guidance=False)
            if ok:
                sent_count += 1
            else:
                skipped_count += 1
        except FloodWait as exc:
            await asyncio.sleep(exc.value)
        except Exception:
            skipped_count += 1

    if access_filter == "normal":
        await message.reply_text(
            f"Completed. Sent: {sent_count}, Skipped: {skipped_count}\n"
            "Normal files are in play-only mode. Use /pay for credits or ask for premium."
        )
    else:
        await message.reply_text(f"Completed. Sent: {sent_count}, Skipped: {skipped_count}")


@app.on_message(filters.command("addsection") & filters.private)
async def add_section(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply_text("Usage: /addsection <name> 📁")
        return

    section = parts[1].strip()
    section_id = await store.set_section(section)
    if not section_id:
        await message.reply_text("Section name already exists. Try another name. 🧭")
        return
    link = f"{settings.base_url}/section/{section_id}"
    await message.reply_text(f"Section set: {section}\nOpen: {link} ✅")


@app.on_message(filters.command("endsection") & filters.private)
async def end_section(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    await store.set_section(None)
    await message.reply_text("Section cleared. ✅")


@app.on_message(filters.command("delsection") & filters.private)
async def delete_section(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply_text("Usage: /delsection <name> 🗑️")
        return

    name = parts[1].strip()
    ok = await store.delete_section(name)
    if not ok:
        await message.reply_text("Section not found. 🧩")
        return
    await message.reply_text(f"Section deleted: {name} ✅")


@app.on_message(filters.command("showsections") & filters.private)
async def show_sections(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    sections = await store.list_sections()
    if not sections:
        await message.reply_text("No sections yet. 📭")
        return

    lines = ["Sections:"]
    for name, section_id in sections:
        lines.append(f"{name} -> {settings.base_url}/section/{section_id}")
    await message.reply_text("\n".join(lines))


@app.on_message(filters.command("addadmin") & filters.private)
async def add_admin(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply_text("Usage: /addadmin <userid> 👤")
        return

    try:
        user_id = int(parts[1])
    except Exception:
        await message.reply_text("Invalid user id. 🧾")
        return

    await db.add_admin(user_id)
    settings.admin_ids.add(user_id)
    await message.reply_text(f"Admin added: {user_id} ✅")


@app.on_message(filters.command("showadminlist") & filters.private)
async def show_admins(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    admins = sorted(settings.admin_ids)
    if not admins:
        await message.reply_text("No admins found. 📭")
        return
    await message.reply_text("Admins:\n" + "\n".join(str(a) for a in admins))

@app.on_message(filters.command("credit") & filters.private)
async def credit_balance(client: Client, message):
    if not message.from_user:
        await message.reply_text("I couldn’t read your user info. Please try again. 🙏")
        return
    user_id = message.from_user.id
    balance = await store.get_credits(user_id)
    await message.reply_text(f"Your credits: {balance} 💳")


@app.on_message(filters.command("pay") & filters.private)
async def pay_info(client: Client, message):
    price, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    upi_id = await store.get_upi_id()
    keyboard = build_pay_keyboard()
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) >= 2:
        amount_inr = parse_amount_value(parts[1])
        if amount_inr is None:
            await message.reply_text("Invalid amount. Example: /pay 75")
            return
        if amount_inr <= MIN_CUSTOM_PAY_INR:
            await message.reply_text(f"Custom amount must be more than INR {MIN_CUSTOM_PAY_INR:.0f}.")
            return
        if not message.from_user:
            await message.reply_text("User not found.")
            return
        if not upi_id:
            await message.reply_text("Payment is not configured yet. Please contact admin.")
            return
        req, err = await create_payment_request_for_amount(message.from_user.id, amount_inr)
        if err:
            await message.reply_text(err)
            return
        await send_payment_request_message(client, message.chat.id, req, upi_id)
        return

    upi_note = f"\nUPI ID: {upi_id}" if upi_id else "\nUPI not configured yet."
    await message.reply_text(
        render_pay_text(template, price)
        + upi_note
        + f"\n\nChoose an amount below, or use /pay <amount> (custom must be > INR {MIN_CUSTOM_PAY_INR:.0f}).",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


@app.on_message(filters.command("editplan") & filters.private)
async def edit_plan(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.reply_text("Usage: /editplan <price> <text with {price}>")
        return
    try:
        price = float(parts[1])
    except Exception:
        await message.reply_text("Invalid price. Example: /editplan 0.35 Price per credit: INR {price}")
        return
    if price <= 0:
        await message.reply_text("Price must be greater than 0.")
        return
    text = normalize_plan_text(parts[2])
    if not text:
        await message.reply_text("Text cannot be empty.")
        return
    new_price, new_text = await store.set_pay_plan(price, text)
    await message.reply_text(
        "Plan updated.\n"
        f"Price: INR {new_price:.2f}\n"
        f"Preview:\n{render_pay_text(new_text, new_price)}"
    )


@app.on_message(filters.command("setcreditprice") & filters.private)
async def set_credit_price(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("Usage: /setcreditprice <price_inr>")
        return
    value = parse_amount_value(parts[1])
    if value is None:
        await message.reply_text("Invalid price. Example: /setcreditprice 0.50")
        return
    _, current_text = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    new_price, _ = await store.set_pay_plan(value, current_text)
    await message.reply_text(f"Credit price updated: INR {new_price:.2f}")


@app.on_message(filters.command("setupi") & filters.private)
async def set_upi(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. ??")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        current = await store.get_upi_id()
        if current:
            await message.reply_text(f"Current UPI ID: {current}\nUsage: /setupi <upi_id>\nClear: /setupi clear")
        else:
            await message.reply_text("Usage: /setupi <upi_id>\nExample: /setupi yourname@upi")
        return

    raw = parts[1].strip()
    if raw.lower() in {"clear", "none", "remove"}:
        await store.set_upi_id("")
        await message.reply_text("UPI ID cleared.")
        return

    if not validate_upi_id(raw):
        await message.reply_text("Invalid UPI ID format. Example: yourname@upi")
        return

    saved = await store.set_upi_id(raw)
    await message.reply_text(f"UPI ID updated: {saved}")


@app.on_message(filters.command("paid") & filters.private)
async def mark_paid(client: Client, message):
    if not message.from_user:
        await message.reply_text("User not found.")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.reply_text("Usage: /paid <request_id> <UTR>")
        return
    request_id = parts[1].strip()
    utr = parts[2].strip()
    ok, msg = await submit_payment_with_utr(client, message.from_user, request_id, utr)
    await message.reply_text(msg)


@app.on_message(filters.private & filters.text & ~filters.command(["start", "pay", "paid", "add", "addsection", "endsection", "delsection", "showsections", "setcreditprice", "setupi", "payments", "paydb", "approve", "reject", "credit", "credit_add", "db", "premium", "premiumlist", "history", "stats", "redeem", "setpay", "editplan"]))
async def collect_pending_utr(client: Client, message):
    if not message.from_user:
        return
    request_id = await store.get_pending_utr(message.from_user.id)
    if not request_id:
        return
    utr = (message.text or "").strip()
    if not utr:
        await message.reply_text("Please send your UTR text.")
        return
    ok, msg = await submit_payment_with_utr(client, message.from_user, request_id, utr)
    await message.reply_text(msg)


@app.on_message(filters.command("payments") & filters.private)
async def list_payments(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return
    parts = (message.text or "").split()
    status = "all"
    limit = 20
    if len(parts) >= 2:
        status = parts[1].strip().lower()
    if len(parts) >= 3:
        try:
            limit = max(1, min(int(parts[2]), 100))
        except Exception:
            limit = 20
    if status not in {"all", "pending", "submitted", "approved", "rejected", "cancelled"}:
        status = "all"
    rows = await store.list_payment_requests(status=status, limit=limit)
    if not rows:
        await message.reply_text("No payment requests found.")
        return
    lines = [f"Payments ({status}) top {len(rows)}:"]
    for req in rows:
        lines.append(format_payment_request_line(req))
    await message.reply_text("\n".join(lines))


@app.on_message(filters.command("paydb") & filters.private)
async def export_payments_db(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. ??")
        return

    parts = (message.text or "").split()
    status = "all"
    if len(parts) >= 2:
        status = parts[1].strip().lower()
    if status not in {"all", "pending", "submitted", "approved", "rejected", "cancelled"}:
        status = "all"

    rows = await store.list_payment_requests(status=status, limit=100000)
    if not rows:
        await message.reply_text("No payment records found.")
        return

    wb = Workbook()
    ws = wb.active
    ws.title = "payments"
    ws.append([
        "request_id",
        "user_id",
        "amount_inr",
        "credits",
        "status",
        "utr_or_note",
        "admin_id",
        "created_at",
        "updated_at",
    ])

    def fmt_ts(value):
        try:
            ts = int(value or 0)
            if ts <= 0:
                return ""
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        except Exception:
            return ""

    for req in rows:
        ws.append([
            str(req.get("id", "")),
            int(req.get("user_id", 0) or 0),
            float(req.get("amount_inr", 0) or 0),
            int(req.get("credits", 0) or 0),
            str(req.get("status", "")),
            str(req.get("note", "")),
            int(req.get("admin_id", 0) or 0),
            fmt_ts(req.get("created_at", 0)),
            fmt_ts(req.get("updated_at", 0)),
        ])

    ts_name = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    out_path = Path(tempfile.gettempdir()) / f"payments-{status}-{ts_name}.xlsx"
    wb.save(out_path)
    await client.send_document(
        chat_id=message.chat.id,
        document=str(out_path),
        caption=f"Payment export ({status}) | records: {len(rows)}",
    )
    try:
        out_path.unlink(missing_ok=True)
    except Exception:
        pass


@app.on_message(filters.command("approve") & filters.private)
async def approve_payment(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. ??")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply_text("Usage: /approve <request_id> [note]")
        return
    request_id = parts[1].strip()
    note = parts[2].strip() if len(parts) >= 3 else "approved"
    ok, msg = await approve_payment_request(
        client,
        request_id=request_id,
        admin_id=(message.from_user.id if message.from_user else 0),
        note=note,
    )
    await message.reply_text(msg)


@app.on_message(filters.command("reject") & filters.private)
async def reject_payment(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. ??")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply_text("Usage: /reject <request_id> [reason]")
        return
    request_id = parts[1].strip()
    reason = parts[2].strip() if len(parts) >= 3 else "rejected"
    ok, msg = await reject_payment_request(
        client,
        request_id=request_id,
        admin_id=(message.from_user.id if message.from_user else 0),
        reason=reason,
    )
    await message.reply_text(msg)


@app.on_message(filters.command("credit_add") & filters.private)
async def credit_add(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply_text("Usage: /credit_add <userid> <amount> 💳")
        return
    try:
        user_id = int(parts[1])
        amount = int(parts[2])
    except Exception:
        await message.reply_text("Invalid format. Example: /credit_add 123456 10 🧾")
        return
    if amount <= 0:
        await message.reply_text("Amount must be > 0. 🔢")
        return
    balance = await store.add_credits(user_id, amount)
    await message.reply_text(f"Added {amount} credits to {user_id}. Balance: {balance} ✅")


@app.on_message(filters.command("db") & filters.private)
async def credit_db(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return
    parts = (message.text or "").split()
    limit = 20
    if len(parts) >= 2:
        try:
            limit = max(1, min(int(parts[1]), 100))
        except Exception:
            limit = 20
    rows = await store.list_credit_balances(limit)
    if not rows:
        await message.reply_text("No credit records found. 📭")
        return
    lines = [f"Credit DB (top {len(rows)}):"]
    for user_id, balance in rows:
        lines.append(f"{user_id} -> {balance}")
    await message.reply_text("\n".join(lines))


@app.on_callback_query(filters.regex("^payamt:"))
async def pay_amount_callback(client: Client, callback):
    if not callback.from_user or not callback.data:
        return
    value = callback.data.split(":", 1)[1].strip().lower()
    if value == "custom":
        await callback.answer(
            f"Use /pay <amount>. Custom amount must be more than INR {MIN_CUSTOM_PAY_INR:.0f}.",
            show_alert=True,
        )
        return
    amount_inr = parse_amount_value(value)
    if amount_inr is None:
        await callback.answer("Invalid amount.", show_alert=True)
        return
    upi_id = await store.get_upi_id()
    if not upi_id:
        await callback.answer("Payment is not configured yet. Contact admin.", show_alert=True)
        return
    req, err = await create_payment_request_for_amount(callback.from_user.id, amount_inr)
    if err:
        await callback.answer(err, show_alert=True)
        return
    if callback.message:
        await send_payment_request_message(client, callback.message.chat.id, req, upi_id)
    await callback.answer("Request created.")


@app.on_callback_query(filters.regex("^payreq:"))
async def payment_request_action(client: Client, callback):
    if not callback.from_user or not callback.data:
        return
    parts = callback.data.split(":", 2)
    if len(parts) != 3:
        await callback.answer("Invalid action.", show_alert=True)
        return
    action = parts[1].strip().lower()
    request_id = parts[2].strip()
    req = await store.get_payment_request(request_id)
    if not req:
        await callback.answer("Request not found.", show_alert=True)
        return
    user_id = int(req.get("user_id", 0) or 0)
    if callback.from_user.id != user_id:
        await callback.answer("This request is not yours.", show_alert=True)
        return
    status = (req.get("status") or "").strip().lower()

    if action == "utr":
        if status in {"approved", "rejected", "cancelled"}:
            await callback.answer(f"Request already {status}.", show_alert=True)
            return
        await store.set_pending_utr(user_id, request_id, ttl_seconds=1800)
        await callback.answer("Now send your UTR as a message.", show_alert=True)
        if callback.message:
            await callback.message.reply_text(f"Send your UTR for request {request_id} now.")
        return

    if action == "cancel":
        if status in {"approved", "rejected", "cancelled"}:
            await callback.answer(f"Request already {status}.", show_alert=True)
            return
        await store.set_payment_request_status(request_id, "cancelled", note="cancelled by user", admin_id=0)
        await store.clear_pending_utr(user_id)
        await store.clear_payment_prompt(request_id)
        await callback.answer("Payment request cancelled.", show_alert=True)
        if callback.message:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await client.send_message(user_id, f"Payment request {request_id} cancelled.")
        return

    await callback.answer("Unknown action.", show_alert=True)


@app.on_callback_query(filters.regex("^payadm:"))
async def admin_payment_action(client: Client, callback):
    if not callback.from_user or not callback.data:
        return
    if not is_admin(callback.from_user.id):
        await callback.answer("Not allowed.", show_alert=True)
        return
    parts = callback.data.split(":", 2)
    if len(parts) != 3:
        await callback.answer("Invalid action.", show_alert=True)
        return
    action = parts[1].strip().lower()
    request_id = parts[2].strip()

    if action == "approve":
        ok, msg = await approve_payment_request(
            client,
            request_id=request_id,
            admin_id=callback.from_user.id,
            note="approved by admin button",
        )
    elif action == "reject":
        ok, msg = await reject_payment_request(
            client,
            request_id=request_id,
            admin_id=callback.from_user.id,
            reason="rejected by admin button",
        )
    else:
        await callback.answer("Unknown action.", show_alert=True)
        return

    await callback.answer(msg if not ok else "Done", show_alert=not ok)
    if callback.message:
        if ok:
            status_line = "Status: APPROVED" if action == "approve" else "Status: REJECTED"
            updated_text = (callback.message.text or "").strip()
            if updated_text:
                updated_text = (
                    f"{updated_text}\n\n"
                    f"{status_line}\n"
                    f"Handled by: {callback.from_user.id}"
                )
                try:
                    await callback.message.edit_text(updated_text, reply_markup=None)
                    return
                except Exception:
                    pass
        try:
            await callback.message.reply_text(msg)
        except Exception:
            pass


@app.on_callback_query(filters.regex("^react:"))
async def reaction_callback(client: Client, callback):
    if not callback.from_user or not callback.data:
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        return
    token = parts[1]
    action = parts[2]
    user_id = callback.from_user.id

    _, _, status = await store.get_reactions(token, user_id)
    if action == "up":
        new_reaction = 0 if status == 1 else 1
    elif action == "down":
        new_reaction = 0 if status == -1 else -1
    else:
        return

    likes, dislikes, status = await store.set_reaction(token, user_id, new_reaction)
    if callback.message:
        await callback.message.edit_reply_markup(
            reply_markup=build_reaction_keyboard(token, likes, dislikes, status)
        )
    await callback.answer("Updated")


@app.on_message(filters.command("premiumlist") & filters.private)
async def premium_list(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    users = await db.list_premium_users()
    if not users:
        await message.reply_text("No premium users. 📭")
        return

    lines = ["Premium users:"]
    now = int(time.time())
    for user in users:
        if user.expires_at is None:
            lines.append(f"{user.user_id} (lifetime)")
        else:
            remaining = user.expires_at - now
            lines.append(f"{user.user_id} (expires in {max(0, remaining)}s)")
    await message.reply_text("\n".join(lines))


@app.on_message(filters.command("history") & filters.private)
async def history_links(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    parts = (message.text or "").split()
    limit = 20
    if len(parts) >= 2:
        try:
            limit = max(1, min(int(parts[1]), 100))
        except Exception:
            limit = 20

    tokens = await store.list_recent(limit * 2)
    if not tokens:
        await message.reply_text("No history yet. 📭")
        return

    lines = ["Recent stream links:"]
    shown = 0
    for token in tokens:
        ref = await store.get(token, settings.token_ttl_seconds)
        if not ref:
            continue
        link = build_link(token)
        name = ref.file_name or ref.file_unique_id or "file"
        access = "Premium" if ref.access == "premium" else "Normal"
        section = ref.section_name or "-"
        lines.append(f"{access} [{section}]: {link}\n{name}")
        shown += 1
        if shown >= limit:
            break

    if shown == 0:
        await message.reply_text("No active links found (expired or missing). ⏳")
        return

    await message.reply_text("\n\n".join(lines))


@app.on_message(filters.command("add") & filters.private)
async def add_premium_user(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply_text("Usage: /add <userid> <period_days|life> 🏷️")
        return

    try:
        user_id = int(parts[1])
        period = parse_period(parts[2])
    except Exception:
        await message.reply_text("Invalid format. Example: /add 123456 30 🧾")
        return

    await db.add_user(user_id, period)
    if period is None:
        await message.reply_text(f"Added {user_id} as lifetime premium. ✅")
    else:
        await message.reply_text(f"Added {user_id} premium for {period} days. ✅")


@app.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_private_media(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed. 🚫")
        return

    media = message.document or message.video or message.audio
    if not media:
        return

    section_id, section_name = await store.get_section()
    if not section_id:
        await message.reply_text("Set a section first using /addsection <name>. 📁")
        return

    normal_token = secrets.token_urlsafe(24)
    premium_token = secrets.token_urlsafe(24)

    base_ref = dict(
        file_id=media.file_id,
        chat_id=message.chat.id,
        message_id=message.id,
        file_unique_id=media.file_unique_id,
        file_name=getattr(media, "file_name", None),
        mime_type=media.mime_type,
        file_size=media.file_size,
        media_type=message.media.value,
        created_at=time.time(),
        section_id=section_id,
        section_name=section_name,
    )

    await store.set(
        normal_token,
        FileRef(**base_ref, access="normal"),
        settings.token_ttl_seconds,
    )
    await store.set(
        premium_token,
        FileRef(**base_ref, access="premium"),
        settings.token_ttl_seconds,
    )

    link = build_link(normal_token)
    premium_link = build_link(premium_token)
    link_text = f"Stream (Normal): {link}\nStream (Premium): {premium_link}\nSection: {section_name}"
    await message.reply_text(link_text + "\n\nReady to stream. ✅")

@app.on_message(filters.channel & (filters.document | filters.video | filters.audio))
async def handle_channel_media(client: Client, message):
    if message.outgoing:
        return
    original_message = message
    media = message.document or message.video or message.audio
    if not media:
        return

    logger.info("Media received chat_id=%s title=%s file_unique_id=%s", message.chat.id, message.chat.title, media.file_unique_id)

    reuploaded = False
    if settings.reupload_video and is_video_document(message) and settings.dump_chat_id:
        try:
            new_message = await reupload_video_as_media(client, message, settings.dump_chat_id)
        except Exception as exc:
            logger.exception("Failed to reupload video: %s", exc)
            new_message = None
        if new_message and new_message.video:
            media = new_message.video
            message = new_message
            reuploaded = True

    normal_token = secrets.token_urlsafe(24)
    premium_token = secrets.token_urlsafe(24)

    section_id, section_name = await store.get_section()

    base_ref = dict(
        file_id=media.file_id,
        chat_id=message.chat.id,
        message_id=message.id,
        file_unique_id=media.file_unique_id,
        file_name=getattr(media, "file_name", None),
        mime_type=media.mime_type,
        file_size=media.file_size,
        media_type=message.media.value,
        created_at=time.time(),
        section_id=section_id,
        section_name=section_name,
    )

    await store.set(
        normal_token,
        FileRef(**base_ref, access="normal"),
        settings.token_ttl_seconds,
    )
    await store.set(
        premium_token,
        FileRef(**base_ref, access="premium"),
        settings.token_ttl_seconds,
    )

    link = build_link(normal_token)
    premium_link = build_link(premium_token)
    caption = (message.caption or "").strip()
    link_text = f"Stream (Normal): {link}\nStream (Premium): {premium_link}"
    if caption:
        new_caption = f"{caption}\n\n{link_text}"
    else:
        new_caption = link_text
    try:
        if settings.send_link_as_message:
            await client.send_message(chat_id=original_message.chat.id, text=link_text)
            return
        if reuploaded:
            try:
                await client.edit_message_caption(
                    chat_id=original_message.chat.id,
                    message_id=original_message.id,
                    caption=new_caption,
                )
            except Exception as exc:
                logger.exception("Caption edit failed, sending message: %s", exc)
                await client.send_message(chat_id=original_message.chat.id, text=link_text)
        else:
            await client.edit_message_caption(
                chat_id=message.chat.id,
                message_id=message.id,
                caption=new_caption,
            )
    except Exception as exc:
        logger.exception("Failed to send link: %s", exc)


async def runner() -> None:
    await store.connect()
    await db.connect()
    for admin_id in settings.admin_ids:
        await db.add_admin(admin_id)
    admins = await db.list_admins()
    settings.admin_ids.update(admins)
    while True:
        try:
            await app.start()
            logger.info("Bot started")
            break
        except FloodWait as exc:
            logger.warning("FloodWait %s seconds", exc.value)
            await asyncio.sleep(exc.value)
    try:
        await asyncio.Event().wait()
    finally:
        await app.stop()
        await store.close()
        await db.close()


if __name__ == "__main__":
    app.run(runner())




