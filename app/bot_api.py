import asyncio
import io
import logging
import secrets
import time

from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import BotCommand, BufferedInputFile, Message
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
dp = Dispatcher()

CREDIT_COST = 1
DEFAULT_CREDIT_PRICE_INR = 0.35
DEFAULT_PAY_TEXT = "Price per credit: INR {price}\nTo add credits, contact admin."
ADMIN_CONTACT = "@azmoviedeal"
PREMIUM_MONTHLY_PRICE_INR = 499.0
PREMIUM_MONTHLY_DAYS = 30
MIN_CUSTOM_PAY_INR = 10.0


BOT_COMMANDS = [
    BotCommand(command="start", description="Bot overview and usage"),
    BotCommand(command="credit", description="Check credits and plan"),
    BotCommand(command="pay", description="Buy credits"),
    BotCommand(command="premium", description="Premium plan"),
    BotCommand(command="paid", description="Submit payment UTR"),
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
    BotCommand(command="stats", description="Bot stats (admin)"),
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


def _format_money(v: float) -> str:
    return f"{v:.2f}"


async def _notify_restart() -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    text = f"✅ Bot restarted and is online.\n🕒 {stamp}"
    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(admin_id, text)
        except Exception as exc:
            logger.warning("Restart notify failed for %s: %s", admin_id, exc)


async def _deliver_token(message: Message, token: str) -> None:
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        await message.reply("Invalid or expired link.")
        return

    user_id = message.from_user.id if message.from_user else 0
    if user_id <= 0:
        await message.reply("User not found.")
        return

    premium = await db.is_premium(user_id)
    if ref.access == "premium" and not premium:
        ok, bal = await store.charge_credits(user_id, CREDIT_COST)
        if not ok:
            await message.reply(
                f"Premium/Credit required.\nBalance: {bal}\nUse /pay or /premium"
            )
            return
        await message.reply(f"1 credit used. Remaining: {bal}")

    try:
        await bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=ref.chat_id,
            message_id=ref.message_id,
            protect_content=(ref.access != "premium"),
        )
    except Exception as exc:
        logger.exception("copy_message failed: %s", exc)
        await message.reply("Failed to deliver file.")


@dp.message(CommandStart())
async def start_cmd(message: Message) -> None:
    text = message.text or ""
    parts = text.split(maxsplit=1)

    if len(parts) > 1 and parts[1].startswith("dl_"):
        await _deliver_token(message, parts[1][3:])
        return

    await message.reply(
        "👋 Welcome to FileLord\n\n"
        "Use your website link to receive files in Telegram.\n"
        "Normal users: play-only protected files\n"
        "Premium/Credit users: downloadable files\n\n"
        "Commands:\n"
        "/credit\n/pay\n/premium\n/paid <request_id> <utr>"
    )


@dp.message(Command("health"))
async def health_cmd(message: Message) -> None:
    await message.reply("I am alive!")


@dp.message(Command("credit"))
async def credit_cmd(message: Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    if not uid:
        await message.reply("User not found.")
        return
    premium = await db.is_premium(uid)
    bal = await store.get_credits(uid)
    if premium:
        await message.reply("Plan: Premium\nCredits: Unlimited")
    else:
        await message.reply(f"Plan: Free\nCredits: {bal}")


@dp.message(Command("premium"))
async def premium_cmd(message: Message) -> None:
    await message.reply(
        f"Premium Plan\nPrice: INR {PREMIUM_MONTHLY_PRICE_INR:.0f}\n"
        f"Duration: {PREMIUM_MONTHLY_DAYS} days\n"
        "Unlimited credits"
    )


@dp.message(Command("pay"))
async def pay_cmd(message: Message) -> None:
    parts = (message.text or "").split(maxsplit=1)
    price, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)

    if len(parts) == 1:
        await message.reply(
            template.replace("{price}", _format_money(price))
            + f"\nContact: {ADMIN_CONTACT}\n\n"
            + "Create request: /pay <amount_inr>"
        )
        return

    try:
        amount = float(parts[1].strip())
    except Exception:
        await message.reply("Usage: /pay <amount_inr>")
        return

    if amount < MIN_CUSTOM_PAY_INR:
        await message.reply(f"Minimum amount is INR {MIN_CUSTOM_PAY_INR:.0f}")
        return

    credits = max(1, int(amount / max(price, 0.01)))
    req_id = await store.next_payment_request_id()
    user_id = message.from_user.id if message.from_user else 0
    await store.create_payment_request(req_id, user_id, amount, credits, plan_type="credits")

    await message.reply(
        f"Payment request created.\n"
        f"Request ID: {req_id}\n"
        f"Amount: INR {_format_money(amount)}\n"
        f"Credits: {credits}\n"
        f"After payment send: /paid {req_id} <UTR>"
    )


@dp.message(Command("paid"))
async def paid_cmd(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else 0
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.reply("Usage: /paid <request_id> <UTR>")
        return

    req_id = parts[1].strip()
    utr = parts[2].strip()
    req = await store.get_payment_request(req_id)
    if not req:
        await message.reply("Request not found.")
        return
    if req.get("user_id") != user_id and not is_admin(user_id):
        await message.reply("This request does not belong to you.")
        return

    await store.set_payment_request_status(req_id, "submitted", note=f"UTR:{utr}", admin_id=0)
    await message.reply("Payment submitted to admin. You will be notified after review.")

    text = (
        f"New payment submitted\n"
        f"Request ID: {req_id}\n"
        f"User ID: {req.get('user_id')}\n"
        f"Amount: INR {_format_money(float(req.get('amount_inr', 0)))}\n"
        f"Credits: {req.get('credits', 0)}\n"
        f"UTR: {utr}\n"
        f"Use /approve {req_id} or /reject {req_id}"
    )
    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            pass


@dp.message(Command("approve"))
async def approve_cmd(message: Message) -> None:
    admin_id = message.from_user.id if message.from_user else 0
    if not is_admin(admin_id):
        await message.reply("Not allowed.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Usage: /approve <request_id>")
        return

    req_id = parts[1].strip()
    req = await store.get_payment_request(req_id)
    if not req:
        await message.reply("Request not found.")
        return

    credits = int(req.get("credits", 0) or 0)
    user_id = int(req.get("user_id", 0) or 0)
    balance = await store.add_credits(user_id, credits)
    await store.set_payment_request_status(req_id, "approved", note="approved", admin_id=admin_id)

    await message.reply(f"Approved {req_id}. Added {credits} credits to {user_id}. Balance:{balance}")
    try:
        await bot.send_message(user_id, f"Your payment {req_id} is approved. Credits added: {credits}. Balance:{balance}")
    except Exception:
        pass


@dp.message(Command("reject"))
async def reject_cmd(message: Message) -> None:
    admin_id = message.from_user.id if message.from_user else 0
    if not is_admin(admin_id):
        await message.reply("Not allowed.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply("Usage: /reject <request_id> [reason]")
        return

    req_id = parts[1].strip()
    reason = parts[2].strip() if len(parts) >= 3 else "rejected"
    req = await store.get_payment_request(req_id)
    if not req:
        await message.reply("Request not found.")
        return

    await store.set_payment_request_status(req_id, "rejected", note=reason, admin_id=admin_id)
    await message.reply(f"Rejected {req_id}")
    try:
        await bot.send_message(int(req.get("user_id", 0) or 0), f"Your payment {req_id} was rejected. Reason: {reason}")
    except Exception:
        pass


@dp.message(Command("payments"))
async def payments_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
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
        await message.reply("No payment requests.")
        return

    out = [f"Payments ({status}):"]
    for r in rows:
        out.append(
            f"{r.get('id')} | {r.get('status')} | uid:{r.get('user_id')} | INR {_format_money(float(r.get('amount_inr', 0)))} | c:{r.get('credits', 0)}"
        )
    await message.reply("\n".join(out))


@dp.message(Command("paydb"))
async def paydb_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return

    rows = await store.list_payment_requests(status="all", limit=1000)
    wb = Workbook()
    ws = wb.active
    ws.title = "payments"
    ws.append(["id", "user_id", "amount_inr", "credits", "status", "note", "created_at", "updated_at", "admin_id"])
    for r in rows:
        ws.append([
            r.get("id"),
            r.get("user_id"),
            r.get("amount_inr"),
            r.get("credits"),
            r.get("status"),
            r.get("note"),
            r.get("created_at"),
            r.get("updated_at"),
            r.get("admin_id"),
        ])

    data = io.BytesIO()
    wb.save(data)
    payload = BufferedInputFile(data.getvalue(), filename="payments.xlsx")
    await bot.send_document(message.chat.id, document=payload)


@dp.message(Command("resetpaydb"))
async def resetpaydb_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip().lower() != "confirm":
        await message.reply("Usage: /resetpaydb confirm")
        return
    deleted = await store.reset_payment_requests()
    await message.reply(f"Payment DB reset done. Removed keys/entries: {deleted}. Next request ID starts from 001.")


@dp.message(Command("addsection", "addsections"))
async def addsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Usage: /addsection <name>")
        return
    section_name = parts[1].strip()
    sid = await store.set_section(section_name)
    if not sid:
        await message.reply("Section already exists or invalid.")
        return
    section_link = f"{settings.base_url}/section/{sid}"
    await message.reply(
        f"Section set: {section_name} -> {sid}\n"
        f"Section link: {section_link}"
    )


@dp.message(Command("endsection"))
async def endsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    await store.set_section(None)
    await message.reply("Section ended. Uploads will not be mapped until /addsection is set.")


@dp.message(Command("delsection"))
async def delsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Usage: /delsection <name>")
        return
    ok = await store.delete_section(parts[1].strip())
    await message.reply("Section deleted." if ok else "Section not found.")


@dp.message(Command("showsections", "showsection", "sections"))
async def showsections_cmd(message: Message) -> None:
    rows = await store.list_sections()
    if not rows:
        await message.reply("No sections yet.")
        return
    rows.sort(key=lambda x: x[0].lower())
    lines = ["Sections:"]
    for name, section_id in rows:
        lines.append(f"{name} -> {settings.base_url}/section/{section_id}")
    await message.reply("\n".join(lines))


@dp.message(Command("addadmin"))
async def addadmin_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("Usage: /addadmin <user_id>")
        return
    try:
        uid = int(parts[1])
    except Exception:
        await message.reply("Invalid user id")
        return
    await db.add_admin(uid)
    settings.admin_ids.add(uid)
    await message.reply(f"Admin added: {uid}")


@dp.message(Command("showadminlist"))
async def showadminlist_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    admins = await db.list_admins()
    if not admins:
        await message.reply("No admins.")
        return
    await message.reply("Admins:\n" + "\n".join(str(x) for x in admins))


@dp.message(Command("credit_add"))
async def credit_add_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply("Usage: /credit_add <user_id> <amount>")
        return
    try:
        uid = int(parts[1]); amt = int(parts[2])
    except Exception:
        await message.reply("Invalid format")
        return
    bal = await store.add_credits(uid, amt)
    await message.reply(f"Added {amt}. New balance: {bal}")


@dp.message(Command("credit_remove"))
async def credit_remove_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply("Usage: /credit_remove <user_id> <amount>")
        return
    try:
        uid = int(parts[1]); amt = int(parts[2])
    except Exception:
        await message.reply("Invalid format")
        return
    ok, bal = await store.charge_credits(uid, amt)
    if not ok:
        await message.reply(f"Cannot remove {amt}. Current balance: {bal}")
        return
    await message.reply(f"Removed {amt}. New balance: {bal}")


@dp.message(Command("db"))
async def db_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    rows = await store.list_credit_balances(limit=20)
    if not rows:
        await message.reply("No credit data.")
        return
    lines = ["Credit DB (top):"]
    for uid, bal in rows:
        lines.append(f"{uid} -> {bal}")
    await message.reply("\n".join(lines))


@dp.message(Command("stats"))
async def stats_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
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
        "Bot Stats\n"
        f"users:{len(users)}\n"
        f"admins:{len(admins)}\n"
        f"sections:{len(sections)}\n"
        f"premium_active:{active_premium}"
    )


@dp.message(Command("history"))
async def history_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
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
        lines.append(f"{ref.access} [{ref.section_name or '-'}]: {build_link(token)}")
        if len(lines) >= limit:
            break
    if not lines:
        await message.reply("No history yet.")
        return
    await message.reply("\n".join(lines))


@dp.message(Command("premiumlist"))
async def premiumlist_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    rows = await db.list_premium_users()
    if not rows:
        await message.reply("No premium users.")
        return
    now = int(time.time())
    lines = ["Premium users:"]
    for row in rows[:100]:
        if row.expires_at is None:
            exp = "lifetime"
        elif row.expires_at < now:
            exp = "expired"
        else:
            exp = str(row.expires_at)
        lines.append(f"{row.user_id} -> {exp}")
    await message.reply("\n".join(lines))


@dp.message(Command("add"))
async def add_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply("Usage: /add <userid> <period_days|life>")
        return
    try:
        uid = int(parts[1]); period = parse_period(parts[2])
    except Exception:
        await message.reply("Invalid format")
        return
    await db.add_user(uid, period)
    await message.reply("Premium updated")


@dp.message(Command("setcreditprice"))
async def setcreditprice_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("Usage: /setcreditprice <price>")
        return
    try:
        price = float(parts[1])
    except Exception:
        await message.reply("Invalid price")
        return
    _, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    await store.set_pay_plan(price, template)
    await message.reply(f"Credit price set: INR {_format_money(price)}")


@dp.message(Command("setpay"))
async def setpay_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply("Usage: /setpay view | /setpay text <msg>")
        return
    sub = parts[1].strip().lower()
    price, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    if sub == "view":
        await message.reply(f"Price: INR {_format_money(price)}\nTemplate:\n{template}")
        return
    if sub == "text":
        if len(parts) < 3:
            await message.reply("Usage: /setpay text <payment_text>")
            return
        await store.set_pay_plan(price, parts[2].strip())
        await message.reply("Payment text updated.")
        return
    await message.reply("Unknown setpay option.")


@dp.message(Command("editplan"))
async def editplan_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply("Usage: /editplan <price> [template]")
        return
    try:
        price = float(parts[1])
    except Exception:
        await message.reply("Invalid price")
        return
    _, current = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    template = parts[2].strip() if len(parts) >= 3 else current
    await store.set_pay_plan(price, template)
    await message.reply(f"Plan updated. Price: INR {_format_money(price)}")


@dp.message(Command("setupi"))
async def setupi_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        current = await store.get_upi_id()
        await message.reply(f"Current UPI: {current or '-'}")
        return
    upi = await store.set_upi_id(parts[1].strip())
    await message.reply(f"UPI updated: {upi}")


@dp.message(Command("broadcast"))
async def broadcast_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Usage: /broadcast <text>")
        return
    txt = parts[1].strip()
    users = await store.list_known_user_ids(limit=50000)
    if not users:
        await message.reply("No users to broadcast.")
        return
    sent = 0
    failed = 0
    for uid in users:
        try:
            await bot.send_message(uid, txt)
            sent += 1
        except Exception:
            failed += 1
    await message.reply(f"Broadcast done. Sent:{sent} Failed:{failed}")


@dp.message(Command("redeem"))
async def redeem_cmd(message: Message) -> None:
    await message.reply("Redeem command will be added in next patch.")


@dp.message(Command("paymentsdb"))
async def alias_paydb(message: Message) -> None:
    await paydb_cmd(message)


@dp.message(Command("bot"))
async def bot_cmd(message: Message) -> None:
    await message.reply("I am alive!")


@dp.message()
async def private_media_handler(message: Message) -> None:
    if message.chat.type not in {"private", "channel"}:
        return

    media = (
        message.document
        or message.video
        or message.audio
        or message.animation
        or message.voice
        or message.video_note
        or (message.photo[-1] if message.photo else None)
    )
    if not media:
        if (message.text or "").startswith("/"):
            await message.reply("Unknown command. Use /start")
            return
        if message.chat.type == "private" and is_admin(message.from_user.id if message.from_user else None):
            await message.reply("Unsupported media type. Send document/video/audio/animation/voice/video_note/photo.")
        return

    if message.chat.type == "private" and not is_admin(message.from_user.id if message.from_user else None):
        await message.reply("Not allowed.")
        return

    section_id, section_name = await store.get_section()
    if not section_id:
        if message.chat.type == "private":
            await message.reply("Set a section first using /addsection <name>.")
        return

    normal_token = secrets.token_urlsafe(24)
    premium_token = secrets.token_urlsafe(24)
    file_name = getattr(media, "file_name", None)
    mime_type = getattr(media, "mime_type", None)
    file_size = getattr(media, "file_size", None)

    media_type = "document"
    if message.video:
        media_type = "video"
    elif message.audio:
        media_type = "audio"
    elif message.animation:
        media_type = "animation"
    elif message.voice:
        media_type = "voice"
    elif message.video_note:
        media_type = "video_note"
    elif message.photo:
        media_type = "photo"

    base_ref = dict(
        file_id=media.file_id,
        chat_id=message.chat.id,
        message_id=message.message_id,
        file_unique_id=media.file_unique_id,
        file_name=file_name,
        mime_type=mime_type,
        file_size=file_size,
        media_type=media_type,
        created_at=time.time(),
        section_id=section_id,
        section_name=section_name,
    )

    await store.set(normal_token, FileRef(**base_ref, access="normal"), settings.token_ttl_seconds)
    await store.set(premium_token, FileRef(**base_ref, access="premium"), settings.token_ttl_seconds)

    if message.chat.type == "private":
        await message.reply(
            f"Stream (Normal): {build_link(normal_token)}\n"
            f"Stream (Premium): {build_link(premium_token)}\n"
            f"Section: {section_name}"
        )


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
