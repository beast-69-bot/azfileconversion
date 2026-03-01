import asyncio
import io
import logging
import secrets
import time
import urllib.parse
from html import escape as _esc

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
from app.store import FileRef, TokenStore

settings = get_settings()
store = TokenStore(settings.redis_url, history_limit=settings.history_limit)
db = PremiumDB(settings.db_path)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("stream_bot_api")

bot = Bot(token=settings.bot_token)
dp = Dispatcher(storage=MemoryStorage())

CREDIT_COST = 1
DEFAULT_CREDIT_PRICE_INR = 0.35
DEFAULT_PAY_TEXT = "Price per credit: INR {price}\nTo add credits, contact admin."
ADMIN_CONTACT = "@azmoviedeal"
PREMIUM_MONTHLY_PRICE_INR = 499.0
PREMIUM_MONTHLY_DAYS = 30
MIN_CUSTOM_PAY_INR = 10.0


# ---------------------------------------------------------------------------
#  FSM States
# ---------------------------------------------------------------------------

class PayState(StatesGroup):
    waiting_amount = State()       # custom amount input
    waiting_utr = State()          # UTR number input
    waiting_screenshot = State()   # screenshot photo


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


# ---------------------------------------------------------------------------
#  Core helpers
# ---------------------------------------------------------------------------

BOT_COMMANDS = [
    BotCommand(command="start", description="Bot overview and usage"),
    BotCommand(command="credit", description="Check credits and plan"),
    BotCommand(command="pay", description="Buy credits"),
    BotCommand(command="premium", description="Premium plan"),
    BotCommand(command="health", description="Health check"),
    BotCommand(command="showsections", description="Show sections (admin)"),
    BotCommand(command="addsection", description="Set upload section (admin)"),
    BotCommand(command="credit_add", description="Add credits (admin)"),
    BotCommand(command="credit_remove", description="Remove credits (admin)"),
    BotCommand(command="add", description="Add premium user (admin)"),
    BotCommand(command="payments", description="List payments (admin)"),
    BotCommand(command="approve", description="Approve payment (admin)"),
    BotCommand(command="reject", description="Reject payment (admin)"),
    BotCommand(command="paydb", description="Export payments sheet (admin)"),
    BotCommand(command="setautodelete", description="Set file auto-delete time (admin)"),
    BotCommand(command="setupi", description="Set UPI ID (admin)"),
]


def is_admin(user_id: int | None) -> bool:
    return bool(user_id and user_id in settings.admin_ids)

def build_link(token: str) -> str:
    return f"{settings.base_url}/player/{token}"

def parse_period(value: str) -> int | None:
    value = value.strip().lower()
    if value in {"life", "lifetime", "permanent", "perm"}:
        return None
    return int(value)


def _plan_kb() -> InlineKeyboardMarkup:
    """Quick plan selection keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="₹10 Credits", callback_data="pay:10"),
            InlineKeyboardButton(text="₹50 Credits", callback_data="pay:50"),
            InlineKeyboardButton(text="₹100 Credits", callback_data="pay:100"),
        ],
        [
            InlineKeyboardButton(text="✏️ Custom Amount", callback_data="pay:custom"),
            InlineKeyboardButton(text=f"✨ Premium ₹{PREMIUM_MONTHLY_PRICE_INR:.0f}/30d", callback_data="pay:premium"),
        ],
    ])


def _payment_action_kb(req_id: str) -> InlineKeyboardMarkup:
    """After plan selected — UTR, screenshot, or cancel buttons."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Submit UTR", callback_data=f"utr:{req_id}"),
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

    plan_label = f"✨ Premium 30 days" if plan_type == "premium_30d" else f"{credits} credits"

    caption = format_msg(
        "💳 Complete Your Payment",
        sections=[
            ("Request ID", code(req_id)),
            ("Plan", plan_label),
            ("Amount", f"INR {_format_money(amount)}"),
            ("UPI ID", code(upi_id)),
            ("", ""),
            ("", "1️⃣ Scan the QR or copy UPI ID\n2️⃣ Pay the exact amount\n3️⃣ Submit UTR or screenshot below"),
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
        # Fallback if QR fails
        sent_msg = await bot.send_message(
            chat_id,
            caption,
            parse_mode="HTML",
            reply_markup=_payment_action_kb(req_id),
        )
    # Store message reference so we can edit it later
    if sent_msg is not None:
        await store.set_payment_prompt(req_id, chat_id, sent_msg.message_id)


async def _notify_admin_payment(req_id: str, user_id: int, amount: float, credits: int, plan_type: str, proof: str) -> None:
    plan_label = "✨ Premium 30d" if plan_type == "premium_30d" else f"{credits} credits"
    text = format_msg(
        "🔔 New Payment — Action Required",
        sections=[
            ("Request ID", code(req_id)),
            ("User ID", code(user_id)),
            ("Plan", plan_label),
            ("Amount", f"INR {_format_money(amount)}"),
            ("Proof", esc(proof)),
        ],
    )
    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML", reply_markup=_admin_action_kb(req_id))
        except Exception:
            pass


async def _notify_restart() -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    text = format_msg("🟢 Bot Online", sections=[("Restarted at", esc(stamp))])
    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception as exc:
            logger.warning("Restart notify failed for %s: %s", admin_id, exc)


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


async def _deliver_token(message: Message, token: str) -> None:
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        await message.reply(format_msg("❌ Not Found", sections=[("", "This link is invalid or has expired.")]), parse_mode="HTML")
        return
    user_id = message.from_user.id if message.from_user else 0
    if user_id <= 0:
        await message.reply(format_msg("❌ Error", sections=[("", "Could not identify your user account.")]), parse_mode="HTML")
        return
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
            return
        await message.reply(format_msg("💳 Credit Used", sections=[("Deducted", "1 credit"), ("Remaining", code(bal))]), parse_mode="HTML")
    try:
        sent = await bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=ref.chat_id,
            message_id=ref.message_id,
            protect_content=(ref.access != "premium"),
        )
    except Exception as exc:
        logger.exception("copy_message failed: %s", exc)
        await message.reply(format_msg("❌ Delivery Failed", sections=[("", "Could not send the file. Please try again.")]), parse_mode="HTML")
        return

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

@dp.message(Command("pay"))
async def pay_cmd(message: Message, state: FSMContext) -> None:
    await state.clear()
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
    await callback.answer()
    plan = callback.data.split(":", 1)[1]
    price, _ = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    user_id = callback.from_user.id

    if plan == "custom":
        await state.set_state(PayState.waiting_amount)
        await callback.message.reply(
            format_msg("✏️ Custom Amount", sections=[("", f"Enter your desired amount (minimum INR {MIN_CUSTOM_PAY_INR:.0f}):")]),
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

    req_id = await store.next_payment_request_id()
    await store.create_payment_request(req_id, user_id, amount, credits, plan_type=plan_type)
    await _send_payment_instructions(callback.message.chat.id, req_id, amount, credits, plan_type)


# ---------------------------------------------------------------------------
#  Cancel Request
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("cxl:"))
async def pay_cancel_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Cancelling...")
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

    # Delete the record
    try:
        await store.delete_payment_request(req_id)
    except Exception:
        # Fallback: mark as cancelled if delete not available
        await store.set_payment_request_status(req_id, "cancelled", note="user_cancelled", admin_id=0)

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
    req_id = await store.next_payment_request_id()
    await store.create_payment_request(req_id, user_id, amount, credits, plan_type="credits")
    await state.clear()
    await _send_payment_instructions(message.chat.id, req_id, amount, credits, "credits")


# ---------------------------------------------------------------------------
#  /pay — Step 3a: user clicks "Submit UTR"
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("utr:"))
async def pay_utr_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    req_id = callback.data.split(":", 1)[1]
    await state.set_state(PayState.waiting_utr)
    await state.update_data(req_id=req_id)
    await callback.message.reply(
        format_msg("📋 Submit UTR", sections=[("", f"Request: {code(req_id)}"), ("", "Please type your UTR / Transaction ID:")]),
        parse_mode="HTML",
    )


@dp.message(StateFilter(PayState.waiting_utr))
async def pay_utr_text_handler(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    req_id = data.get("req_id", "?")
    utr = (message.text or "").strip()
    if not utr:
        await message.reply(format_msg("⚠️ Empty", sections=[("", "Please type a valid UTR number.")]), parse_mode="HTML")
        return

    req = await store.get_payment_request(req_id)
    if req:
        await store.set_payment_request_status(req_id, "submitted", note=f"UTR:{utr}", admin_id=0)

    await state.clear()

    # Edit the original QR payment message to show pending status
    prompt = await store.get_payment_prompt(req_id)
    if prompt:
        pending_caption = format_msg(
            "🕐 Verification In Progress",
            sections=[
                ("Request ID", code(req_id)),
                ("UTR", code(esc(utr))),
                ("", ""),
                ("", "✅ Payment details received.\nWaiting for manual approval by admin."),
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
        # Fallback reply if original message not found
        await message.reply(
            format_msg("✅ UTR Submitted", sections=[
                ("Request ID", code(req_id)),
                ("UTR", code(esc(utr))),
                ("Status", "Pending admin review"),
            ], tip="You will be notified once verified."),
            parse_mode="HTML",
        )

    if req:
        amount = float(req.get("amount_inr", 0))
        credits = int(req.get("credits", 0) or 0)
        plan_type = req.get("plan_type", "credits")
        user_id = message.from_user.id if message.from_user else 0
        await _notify_admin_payment(req_id, user_id, amount, credits, plan_type, f"UTR: {utr}")


# ---------------------------------------------------------------------------
#  /pay — Step 3b: user clicks "Send Screenshot"
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("sc:"))
async def pay_screenshot_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    req_id = callback.data.split(":", 1)[1]
    await state.set_state(PayState.waiting_screenshot)
    await state.update_data(req_id=req_id)
    await callback.message.reply(
        format_msg("📸 Send Screenshot", sections=[("", f"Request: {code(req_id)}"), ("", "Please send your payment screenshot as a photo:")]),
        parse_mode="HTML",
    )


@dp.message(StateFilter(PayState.waiting_screenshot), F.photo)
async def pay_screenshot_photo_handler(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    req_id = data.get("req_id", "?")
    await state.clear()

    req = await store.get_payment_request(req_id)
    if req:
        await store.set_payment_request_status(req_id, "submitted", note="screenshot", admin_id=0)

    # Edit the original QR payment message to show pending status
    prompt = await store.get_payment_prompt(req_id)
    if prompt:
        pending_caption = format_msg(
            "🕐 Verification In Progress",
            sections=[
                ("Request ID", code(req_id)),
                ("Proof", "📸 Screenshot received"),
                ("", ""),
                ("", "✅ Payment details received.\nWaiting for manual approval by admin."),
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

    if req:
        amount = float(req.get("amount_inr", 0))
        credits = int(req.get("credits", 0) or 0)
        plan_type = req.get("plan_type", "credits")
        user_id = message.from_user.id if message.from_user else 0

        # Forward screenshot to admins with approve/reject buttons
        admin_caption = format_msg("🔔 Payment Screenshot — Action Required", sections=[
            ("Request ID", code(req_id)),
            ("User ID", code(user_id)),
            ("Plan", "✨ Premium 30d" if plan_type == "premium_30d" else f"{credits} credits"),
            ("Amount", f"INR {_format_money(amount)}"),
        ])
        for admin_id in settings.admin_ids:
            try:
                await bot.send_photo(
                    admin_id,
                    photo=message.photo[-1].file_id,
                    caption=admin_caption,
                    parse_mode="HTML",
                    reply_markup=_admin_action_kb(req_id),
                )
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

    plan_type = req.get("plan_type", "credits")
    user_id = int(req.get("user_id", 0) or 0)
    credits = int(req.get("credits", 0) or 0)
    amount = float(req.get("amount_inr", 0))

    await store.set_payment_request_status(req_id, "approved", note="approved", admin_id=admin_id)

    if plan_type == "premium_30d":
        await db.add_user(user_id, PREMIUM_MONTHLY_DAYS)
        admin_note = f"✨ Premium activated ({PREMIUM_MONTHLY_DAYS} days)"
        user_msg = format_msg("✅ Payment Approved", sections=[
            ("Request ID", code(req_id)),
            ("Plan", f"✨ Premium {PREMIUM_MONTHLY_DAYS} days activated"),
        ], tip="Enjoy unlimited access!")
    else:
        balance = await store.add_credits(user_id, credits)
        admin_note = f"{credits} credits added. New balance: {balance}"
        user_msg = format_msg("✅ Payment Approved", sections=[
            ("Request ID", code(req_id)),
            ("Credits Added", code(credits)),
            ("New Balance", code(balance)),
        ], tip="Your credits are ready to use.")

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

    # Notify user
    try:
        await bot.send_message(user_id, user_msg, parse_mode="HTML")
    except Exception:
        pass


# ---------------------------------------------------------------------------
#  Admin Inline: ❌ Reject
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("rjt:"))
async def admin_reject_callback(callback: CallbackQuery) -> None:
    admin_id = callback.from_user.id
    if not is_admin(admin_id):
        await callback.answer("❌ Not allowed.", show_alert=True)
        return
    await callback.answer("Rejected.")

    req_id = callback.data.split(":", 1)[1]
    req = await store.get_payment_request(req_id)
    if not req:
        await callback.message.reply(format_msg("❌ Not Found", sections=[("", f"Request {code(req_id)} not found.")]), parse_mode="HTML")
        return

    user_id = int(req.get("user_id", 0) or 0)
    reason = "Rejected by admin"
    await store.set_payment_request_status(req_id, "rejected", note=reason, admin_id=admin_id)

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
            format_msg("⚠️ Payment Rejected", sections=[
                ("Request ID", code(req_id)),
                ("Reason", esc(reason)),
            ], tip=f"For help, contact {esc(ADMIN_CONTACT)}."),
            parse_mode="HTML",
        )
    except Exception:
        pass


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
    plan_type = req.get("plan_type", "credits")
    credits = int(req.get("credits", 0) or 0)
    user_id = int(req.get("user_id", 0) or 0)
    await store.set_payment_request_status(req_id, "approved", note="approved", admin_id=admin_id)
    if plan_type == "premium_30d":
        await db.add_user(user_id, PREMIUM_MONTHLY_DAYS)
        result = f"✨ Premium {PREMIUM_MONTHLY_DAYS}d activated"
        user_msg = format_msg("✅ Payment Approved", sections=[("Plan", f"✨ Premium {PREMIUM_MONTHLY_DAYS} days")], tip="Enjoy unlimited access!")
    else:
        balance = await store.add_credits(user_id, credits)
        result = f"{credits} credits → balance: {balance}"
        user_msg = format_msg("✅ Payment Approved", sections=[("Credits Added", code(credits)), ("New Balance", code(balance))], tip="Credits are ready.")
    await message.reply(format_msg("✅ Approved", sections=[("Request", code(req_id)), ("Result", esc(result))]), parse_mode="HTML")
    try:
        await bot.send_message(user_id, user_msg, parse_mode="HTML")
    except Exception:
        pass


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
    req = await store.get_payment_request(req_id)
    if not req:
        await message.reply(format_msg("❌ Not Found", sections=[("", f"No request {code(req_id)}.")]), parse_mode="HTML")
        return
    user_id = int(req.get("user_id", 0) or 0)
    await store.set_payment_request_status(req_id, "rejected", note=reason, admin_id=admin_id)
    await message.reply(format_msg("⚠️ Rejected", sections=[("Request", code(req_id)), ("Reason", esc(reason))]), parse_mode="HTML")
    try:
        await bot.send_message(user_id, format_msg("⚠️ Payment Rejected", sections=[("Request ID", code(req_id)), ("Reason", esc(reason))], tip=f"Contact {esc(ADMIN_CONTACT)} for help."), parse_mode="HTML")
    except Exception:
        pass


@dp.message(Command("payments"))
async def payments_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split()
    status = parts[1].strip().lower() if len(parts) >= 2 else "all"
    limit = 20
    if len(parts) >= 3:
        try:
            limit = max(1, min(int(parts[2]), 100))
        except Exception:
            pass
    rows = await store.list_payment_requests(status=status, limit=limit)
    if not rows:
        await message.reply(format_msg("ℹ️ Payments", sections=[("", f"No requests for status: {code(status)}")]), parse_mode="HTML")
        return
    lines = [
        f"• {code(r.get('id'))} — {esc(str(r.get('status')))} | uid:{code(r.get('user_id'))} | INR {_format_money(float(r.get('amount_inr', 0)))} | {code(r.get('credits', 0))} cr"
        for r in rows
    ]
    await message.reply(format_msg(f"📋 Payments ({esc(status)})", sections=[("", "\n".join(lines))]), parse_mode="HTML")


@dp.message(Command("paydb"))
async def paydb_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    rows = await store.list_payment_requests(status="all", limit=1000)
    wb = Workbook()
    ws = wb.active
    ws.title = "payments"
    ws.append(["id", "user_id", "amount_inr", "credits", "plan_type", "status", "note", "created_at", "updated_at", "admin_id"])
    for r in rows:
        ws.append([r.get("id"), r.get("user_id"), r.get("amount_inr"), r.get("credits"), r.get("plan_type"), r.get("status"), r.get("note"), r.get("created_at"), r.get("updated_at"), r.get("admin_id")])
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
    await message.reply(format_msg("✅ Payment DB Reset", sections=[("Removed", code(deleted)), ("Next ID", "001")]), parse_mode="HTML")


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
    sid = await store.set_section(section_name)
    if not sid:
        await message.reply(format_msg("⚠️ Failed", sections=[("", "Section already exists or invalid name.")]), parse_mode="HTML")
        return
    section_link = f"{settings.base_url}/section/{sid}"
    await message.reply(format_msg("✅ Section Created", sections=[("Name", esc(section_name)), ("ID", code(sid)), ("Link", link("Open Section", section_link))], tip="Uploads will now be mapped to this section."), parse_mode="HTML")


@dp.message(Command("endsection"))
async def endsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    await store.set_section(None)
    await message.reply(format_msg("✅ Section Ended", sections=[("", "Uploads will not be mapped until /addsection is used again.")]), parse_mode="HTML")


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
        await message.reply(format_msg("✅ Deleted", sections=[("Name", code(esc(parts[1].strip())))]), parse_mode="HTML")
    else:
        await message.reply(format_msg("❌ Not Found", sections=[("", "No section with that name.")]), parse_mode="HTML")


@dp.message(Command("showsections", "showsection", "sections"))
async def showsections_cmd(message: Message) -> None:
    rows = await store.list_sections()
    if not rows:
        await message.reply(format_msg("ℹ️ Sections", sections=[("", "No sections yet. Use /addsection.")]), parse_mode="HTML")
        return
    rows.sort(key=lambda x: x[0].lower())
    lines = [f"• {link(esc(name), f'{settings.base_url}/section/{sid}')}" for name, sid in rows]
    await message.reply(format_msg("📂 Sections", sections=[("", "\n".join(lines))]), parse_mode="HTML")


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
    await message.reply(format_msg("✅ Premium Updated", sections=[("User ID", code(uid)), ("Period", "Lifetime ♾️" if period is None else f"{code(period)} days")]), parse_mode="HTML")


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
    await message.reply(format_msg("✅ Credits Added", sections=[("User ID", code(uid)), ("Added", code(amt)), ("New Balance", code(bal))]), parse_mode="HTML")


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
    await message.reply(format_msg("✅ Credits Removed", sections=[("User ID", code(uid)), ("Removed", code(amt)), ("New Balance", code(bal))]), parse_mode="HTML")


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
        lines.append(f"• [{esc(ref.access)}] {esc(ref.section_name or '-')}: {link('▶️ Open', build_link(token))}")
        if len(lines) >= limit:
            break
    if not lines:
        await message.reply(format_msg("ℹ️ History", sections=[("", "No history yet.")]), parse_mode="HTML")
        return
    await message.reply(format_msg("🕓 Recent Uploads", sections=[("", "\n".join(lines))]), parse_mode="HTML")


@dp.message(Command("premiumlist"))
async def premiumlist_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    rows = await db.list_premium_users()
    if not rows:
        await message.reply(format_msg("ℹ️ Premium Users", sections=[("", "No premium users yet.")]), parse_mode="HTML")
        return
    now = int(time.time())
    lines = []
    for row in rows[:100]:
        exp = "lifetime ♾️" if row.expires_at is None else ("expired" if row.expires_at < now else str(row.expires_at))
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
                ("", bullet(["e.g. /setautodelete 300 → 5 minutes", "/setautodelete 0 → disable"])),
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



@dp.message(Command("broadcast"))
async def broadcast_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(format_msg("❌ Access Denied", sections=[("", "Admins only.")]), parse_mode="HTML")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(format_msg("⚠️ Usage", sections=[("", code("/broadcast <text>"))]), parse_mode="HTML")
        return
    txt = parts[1].strip()
    users = await store.list_known_user_ids(limit=50000)
    if not users:
        await message.reply(format_msg("ℹ️ Broadcast", sections=[("", "No users yet.")]), parse_mode="HTML")
        return
    sent = failed = 0
    for uid in users:
        try:
            await bot.send_message(uid, txt)
            sent += 1
        except Exception:
            failed += 1
    await message.reply(format_msg("📣 Broadcast Complete", sections=[("Sent", code(sent)), ("Failed", code(failed))]), parse_mode="HTML")


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
        await message.reply(format_msg("⚙️ Pay Plan", sections=[("Price", f"INR {_format_money(price)}"), ("Template", f"\n{esc(template)}")]), parse_mode="HTML")
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
        await message.reply(format_msg("ℹ️ Credit DB", sections=[("", "No data.")]), parse_mode="HTML")
        return
    lines = [f"• {code(uid)} → {code(bal)}" for uid, bal in rows]
    await message.reply(format_msg("🗄️ Credit DB (Top 20)", sections=[("", "\n".join(lines))]), parse_mode="HTML")


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
        await message.reply(format_msg("ℹ️ Admins", sections=[("", "No admins configured.")]), parse_mode="HTML")
        return
    await message.reply(format_msg("👑 Admin List", sections=[("", "\n".join(f"• {code(x)}" for x in admins))]), parse_mode="HTML")


@dp.message(Command("redeem"))
async def redeem_cmd(message: Message) -> None:
    await message.reply(format_msg("ℹ️ Redeem", sections=[("", "Redeem command coming in next update.")]), parse_mode="HTML")


@dp.message(Command("paymentsdb"))
async def alias_paydb(message: Message) -> None:
    await paydb_cmd(message)


@dp.message(Command("bot", "health"))
async def bot_cmd(message: Message) -> None:
    await message.reply(format_msg("🟢 Bot Status", sections=[("", "I am alive and running.")]), parse_mode="HTML")


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
    await _notify_restart()
    logger.info("Bot started (aiogram)")


async def _shutdown() -> None:
    await store.close()
    await db.close()


async def run() -> None:
    dp.startup.register(_startup)
    dp.shutdown.register(_shutdown)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(run())
