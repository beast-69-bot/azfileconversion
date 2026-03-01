import asyncio
import io
import logging
import secrets
import time
from html import escape as _esc

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


# ---------------------------------------------------------------------------
#  Format helpers
# ---------------------------------------------------------------------------

def esc(text: str) -> str:
    """Escape user-supplied text for safe HTML embedding."""
    return _esc(str(text), quote=False)


def code(text: str) -> str:
    """Wrap text in <code> tags."""
    return f"<code>{esc(str(text))}</code>"


def link(text: str, url: str) -> str:
    """Create a safe HTML hyperlink."""
    return f'<a href="{esc(url)}">{esc(text)}</a>'


def bold(text: str) -> str:
    return f"<b>{esc(str(text))}</b>"


def format_msg(
    title: str,
    sections: list[tuple[str, str]] | None = None,
    tip: str | None = None,
    status: str | None = None,
) -> str:
    """
    Build a consistently styled HTML message.

    title:    emoji + title text, e.g. "💳 Credits"
    sections: list of (label, value) pairs; label="" means plain line
    tip:      Optional tip line shown at bottom
    status:   Optional line prepended as ✅/⚠️/❌ etc.
    """
    parts: list[str] = []

    if status:
        parts.append(status)

    parts.append(f"<b>{esc(title)}</b>")
    parts.append("")  # blank line

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


def bullet(items: list[str]) -> str:
    return "\n".join(f"• {item}" for item in items)


def _format_money(v: float) -> str:
    return f"{v:.2f}"


# ---------------------------------------------------------------------------
#  Core helpers
# ---------------------------------------------------------------------------

def is_admin(user_id: int | None) -> bool:
    return bool(user_id and user_id in settings.admin_ids)


def build_link(token: str) -> str:
    return f"{settings.base_url}/player/{token}"


def parse_period(value: str) -> int | None:
    value = value.strip().lower()
    if value in {"life", "lifetime", "permanent", "perm"}:
        return None
    return int(value)


async def _notify_restart() -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    text = format_msg(
        "🟢 Bot Online",
        sections=[("Restarted at", esc(stamp))],
    )
    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception as exc:
            logger.warning("Restart notify failed for %s: %s", admin_id, exc)


async def _deliver_token(message: Message, token: str) -> None:
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        await message.reply(
            format_msg("❌ Not Found", sections=[("", "This link is invalid or has expired.")]),
            parse_mode="HTML",
        )
        return

    user_id = message.from_user.id if message.from_user else 0
    if user_id <= 0:
        await message.reply(
            format_msg("❌ Error", sections=[("", "Could not identify your user account.")]),
            parse_mode="HTML",
        )
        return

    premium = await db.is_premium(user_id)
    if ref.access == "premium" and not premium:
        ok, bal = await store.charge_credits(user_id, CREDIT_COST)
        if not ok:
            await message.reply(
                format_msg(
                    "✨ Premium / Credits Required",
                    sections=[
                        ("Balance", f"{code(bal)} credits"),
                        ("", ""),
                        ("", bullet([
                            "Use /pay to buy credits",
                            "Use /premium to upgrade your plan",
                        ])),
                    ],
                ),
                parse_mode="HTML",
            )
            return
        await message.reply(
            format_msg(
                "💳 Credit Used",
                sections=[
                    ("Deducted", "1 credit"),
                    ("Remaining", f"{code(bal)} credits"),
                ],
            ),
            parse_mode="HTML",
        )

    try:
        await bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=ref.chat_id,
            message_id=ref.message_id,
            protect_content=(ref.access != "premium"),
        )
    except Exception as exc:
        logger.exception("copy_message failed: %s", exc)
        await message.reply(
            format_msg("❌ Delivery Failed", sections=[("", "Could not send the file. Please try again.")]),
            parse_mode="HTML",
        )


# ---------------------------------------------------------------------------
#  Commands — User
# ---------------------------------------------------------------------------

@dp.message(CommandStart())
async def start_cmd(message: Message) -> None:
    text = message.text or ""
    parts = text.split(maxsplit=1)

    if len(parts) > 1 and parts[1].startswith("dl_"):
        await _deliver_token(message, parts[1][3:])
        return

    await message.reply(
        format_msg(
            "👋 Welcome to FileLord",
            sections=[
                ("", "Use your website link to receive files directly in Telegram."),
                ("", ""),
                ("", "<b>Plans:</b>"),
                ("", bullet([
                    "Normal — stream files (play only)",
                    "✨ Premium / Credits — downloadable files",
                ])),
                ("", ""),
                ("", "<b>Quick Actions:</b>"),
                ("", bullet([
                    "/credit — check your balance",
                    "/pay — buy credits (contact admin)",
                    "/premium — view premium plan",
                ])),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("health"))
async def health_cmd(message: Message) -> None:
    await message.reply(
        format_msg("🟢 Bot Status", sections=[("", "I am alive and running.")]),
        parse_mode="HTML",
    )


@dp.message(Command("credit"))
async def credit_cmd(message: Message) -> None:
    uid = message.from_user.id if message.from_user else 0
    if not uid:
        await message.reply(
            format_msg("❌ Error", sections=[("", "Could not identify your user account.")]),
            parse_mode="HTML",
        )
        return
    premium = await db.is_premium(uid)
    bal = await store.get_credits(uid)
    if premium:
        await message.reply(
            format_msg(
                "💳 Your Credits",
                sections=[
                    ("Plan", "✨ Premium"),
                    ("Credits", "Unlimited"),
                ],
                tip="Premium users enjoy unlimited access.",
            ),
            parse_mode="HTML",
        )
    else:
        await message.reply(
            format_msg(
                "💳 Your Credits",
                sections=[
                    ("Plan", "Free"),
                    ("Credits", code(bal)),
                ],
                tip="Use /pay to top-up or /premium to upgrade.",
            ),
            parse_mode="HTML",
        )


@dp.message(Command("premium"))
async def premium_cmd(message: Message) -> None:
    await message.reply(
        format_msg(
            "✨ Premium Plan",
            sections=[
                ("Price", f"INR {PREMIUM_MONTHLY_PRICE_INR:.0f}"),
                ("Duration", f"{PREMIUM_MONTHLY_DAYS} days"),
                ("Credits", "Unlimited"),
                ("", ""),
                ("", f"Contact {esc(ADMIN_CONTACT)} to subscribe."),
            ],
            tip="Premium users can directly download files without credit deductions.",
        ),
        parse_mode="HTML",
    )


@dp.message(Command("pay"))
async def pay_cmd(message: Message) -> None:
    price, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    pay_info = template.replace("{price}", _format_money(price))
    await message.reply(
        format_msg(
            "💰 Buy Credits",
            sections=[
                ("", esc(pay_info)),
                ("", ""),
                ("Contact Admin", esc(ADMIN_CONTACT)),
            ],
            tip="Message the admin with your payment screenshot to get credits added.",
        ),
        parse_mode="HTML",
    )




@dp.message(Command("approve"))
async def approve_cmd(message: Message) -> None:
    admin_id = message.from_user.id if message.from_user else 0
    if not is_admin(admin_id):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/approve <request_id>"))]),
            parse_mode="HTML",
        )
        return

    req_id = parts[1].strip()
    req = await store.get_payment_request(req_id)
    if not req:
        await message.reply(
            format_msg("❌ Not Found", sections=[("", f"No request with ID {code(req_id)}.")]),
            parse_mode="HTML",
        )
        return

    credits = int(req.get("credits", 0) or 0)
    user_id = int(req.get("user_id", 0) or 0)
    balance = await store.add_credits(user_id, credits)
    await store.set_payment_request_status(req_id, "approved", note="approved", admin_id=admin_id)

    await message.reply(
        format_msg(
            "✅ Payment Approved",
            sections=[
                ("Request ID", code(req_id)),
                ("User ID", code(user_id)),
                ("Credits Added", code(credits)),
                ("New Balance", code(balance)),
            ],
        ),
        parse_mode="HTML",
    )
    try:
        await bot.send_message(
            user_id,
            format_msg(
                "✅ Payment Approved",
                sections=[
                    ("Request ID", code(req_id)),
                    ("Credits Added", code(credits)),
                    ("New Balance", code(balance)),
                ],
                tip="Your credits are now available. Use them via your stream link.",
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.message(Command("reject"))
async def reject_cmd(message: Message) -> None:
    admin_id = message.from_user.id if message.from_user else 0
    if not is_admin(admin_id):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/reject <request_id> [reason]"))]),
            parse_mode="HTML",
        )
        return

    req_id = parts[1].strip()
    reason = parts[2].strip() if len(parts) >= 3 else "No reason provided"
    req = await store.get_payment_request(req_id)
    if not req:
        await message.reply(
            format_msg("❌ Not Found", sections=[("", f"No request with ID {code(req_id)}.")]),
            parse_mode="HTML",
        )
        return

    await store.set_payment_request_status(req_id, "rejected", note=reason, admin_id=admin_id)
    await message.reply(
        format_msg(
            "⚠️ Payment Rejected",
            sections=[
                ("Request ID", code(req_id)),
                ("Reason", esc(reason)),
            ],
        ),
        parse_mode="HTML",
    )
    try:
        await bot.send_message(
            int(req.get("user_id", 0) or 0),
            format_msg(
                "⚠️ Payment Rejected",
                sections=[
                    ("Request ID", code(req_id)),
                    ("Reason", esc(reason)),
                ],
                tip="For help, contact the admin.",
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.message(Command("payments"))
async def payments_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
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
        await message.reply(
            format_msg("ℹ️ Payments", sections=[("", f"No payment requests found for status: {code(status)}")]),
            parse_mode="HTML",
        )
        return

    lines = []
    for r in rows:
        lines.append(
            f"• {code(r.get('id'))} — {esc(str(r.get('status')))} "
            f"| uid:{code(r.get('user_id'))} "
            f"| INR {_format_money(float(r.get('amount_inr', 0)))} "
            f"| {code(r.get('credits', 0))} cr"
        )

    await message.reply(
        format_msg(
            f"📋 Payments ({esc(status)})",
            sections=[("", "\n".join(lines))],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("paydb"))
async def paydb_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
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
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip().lower() != "confirm":
        await message.reply(
            format_msg("⚠️ Confirm Required", sections=[("Usage", code("/resetpaydb confirm"))]),
            parse_mode="HTML",
        )
        return
    deleted = await store.reset_payment_requests()
    await message.reply(
        format_msg(
            "✅ Payment DB Reset",
            sections=[
                ("Removed entries", code(deleted)),
                ("Next request ID", "001"),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("addsection", "addsections"))
async def addsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/addsection <name>"))]),
            parse_mode="HTML",
        )
        return
    section_name = parts[1].strip()
    sid = await store.set_section(section_name)
    if not sid:
        await message.reply(
            format_msg("⚠️ Failed", sections=[("", "Section already exists or the name is invalid.")]),
            parse_mode="HTML",
        )
        return
    section_link = f"{settings.base_url}/section/{sid}"
    await message.reply(
        format_msg(
            "✅ Section Created",
            sections=[
                ("Name", esc(section_name)),
                ("ID", code(sid)),
                ("Link", link("Open Section", section_link)),
            ],
            tip="Uploads will now be mapped to this section.",
        ),
        parse_mode="HTML",
    )


@dp.message(Command("endsection"))
async def endsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    await store.set_section(None)
    await message.reply(
        format_msg(
            "✅ Section Ended",
            sections=[("", "Uploads will not be mapped to any section until you use /addsection again.")],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("delsection"))
async def delsection_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/delsection <name>"))]),
            parse_mode="HTML",
        )
        return
    ok = await store.delete_section(parts[1].strip())
    if ok:
        await message.reply(
            format_msg("✅ Section Deleted", sections=[("Name", code(esc(parts[1].strip())))]),
            parse_mode="HTML",
        )
    else:
        await message.reply(
            format_msg("❌ Not Found", sections=[("", "No section with that name exists.")]),
            parse_mode="HTML",
        )


@dp.message(Command("showsections", "showsection", "sections"))
async def showsections_cmd(message: Message) -> None:
    rows = await store.list_sections()
    if not rows:
        await message.reply(
            format_msg("ℹ️ Sections", sections=[("", "No sections exist yet. Use /addsection to create one.")]),
            parse_mode="HTML",
        )
        return
    rows.sort(key=lambda x: x[0].lower())
    lines = []
    for name, section_id in rows:
        section_link = f"{settings.base_url}/section/{section_id}"
        lines.append(f"• {link(esc(name), section_link)}")
    await message.reply(
        format_msg("📂 Sections", sections=[("", "\n".join(lines))]),
        parse_mode="HTML",
    )


@dp.message(Command("addadmin"))
async def addadmin_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/addadmin <user_id>"))]),
            parse_mode="HTML",
        )
        return
    try:
        uid = int(parts[1])
    except Exception:
        await message.reply(
            format_msg("❌ Invalid", sections=[("", "Please provide a valid numeric user ID.")]),
            parse_mode="HTML",
        )
        return
    await db.add_admin(uid)
    settings.admin_ids.add(uid)
    await message.reply(
        format_msg("✅ Admin Added", sections=[("User ID", code(uid))]),
        parse_mode="HTML",
    )


@dp.message(Command("showadminlist"))
async def showadminlist_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    admins = await db.list_admins()
    if not admins:
        await message.reply(
            format_msg("ℹ️ Admins", sections=[("", "No admins configured.")]),
            parse_mode="HTML",
        )
        return
    lines = [f"• {code(x)}" for x in admins]
    await message.reply(
        format_msg("👑 Admin List", sections=[("", "\n".join(lines))]),
        parse_mode="HTML",
    )


@dp.message(Command("credit_add"))
async def credit_add_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/credit_add <user_id> <amount>"))]),
            parse_mode="HTML",
        )
        return
    try:
        uid = int(parts[1]); amt = int(parts[2])
    except Exception:
        await message.reply(
            format_msg("❌ Invalid", sections=[("", "User ID and amount must be valid integers.")]),
            parse_mode="HTML",
        )
        return
    bal = await store.add_credits(uid, amt)
    await message.reply(
        format_msg(
            "✅ Credits Added",
            sections=[
                ("User ID", code(uid)),
                ("Added", code(amt)),
                ("New Balance", code(bal)),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("credit_remove"))
async def credit_remove_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/credit_remove <user_id> <amount>"))]),
            parse_mode="HTML",
        )
        return
    try:
        uid = int(parts[1]); amt = int(parts[2])
    except Exception:
        await message.reply(
            format_msg("❌ Invalid", sections=[("", "User ID and amount must be valid integers.")]),
            parse_mode="HTML",
        )
        return
    ok, bal = await store.charge_credits(uid, amt)
    if not ok:
        await message.reply(
            format_msg(
                "⚠️ Insufficient Credits",
                sections=[
                    ("Requested", code(amt)),
                    ("Current Balance", code(bal)),
                ],
            ),
            parse_mode="HTML",
        )
        return
    await message.reply(
        format_msg(
            "✅ Credits Removed",
            sections=[
                ("User ID", code(uid)),
                ("Removed", code(amt)),
                ("New Balance", code(bal)),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("db"))
async def db_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    rows = await store.list_credit_balances(limit=20)
    if not rows:
        await message.reply(
            format_msg("ℹ️ Credit DB", sections=[("", "No credit data found.")]),
            parse_mode="HTML",
        )
        return
    lines = [f"• {code(uid)} → {code(bal)}" for uid, bal in rows]
    await message.reply(
        format_msg("🗄️ Credit DB (Top 20)", sections=[("", "\n".join(lines))]),
        parse_mode="HTML",
    )


@dp.message(Command("stats"))
async def stats_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
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
        format_msg(
            "📊 Bot Stats",
            sections=[
                ("Users", code(len(users))),
                ("Admins", code(len(admins))),
                ("Sections", code(len(sections))),
                ("Active Premium", code(active_premium)),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("history"))
async def history_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
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
        section = esc(ref.section_name or "-")
        player_url = build_link(token)
        lines.append(f"• [{esc(ref.access)}] {section}: {link('▶️ Open', player_url)}")
        if len(lines) >= limit:
            break
    if not lines:
        await message.reply(
            format_msg("ℹ️ History", sections=[("", "No upload history yet.")]),
            parse_mode="HTML",
        )
        return
    await message.reply(
        format_msg("🕓 Recent Uploads", sections=[("", "\n".join(lines))]),
        parse_mode="HTML",
    )


@dp.message(Command("premiumlist"))
async def premiumlist_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    rows = await db.list_premium_users()
    if not rows:
        await message.reply(
            format_msg("ℹ️ Premium Users", sections=[("", "No premium users yet.")]),
            parse_mode="HTML",
        )
        return
    now = int(time.time())
    lines = []
    for row in rows[:100]:
        if row.expires_at is None:
            exp = "lifetime ♾️"
        elif row.expires_at < now:
            exp = "expired"
        else:
            exp = str(row.expires_at)
        lines.append(f"• {code(row.user_id)} → {esc(exp)}")
    await message.reply(
        format_msg("✨ Premium Users", sections=[("", "\n".join(lines))]),
        parse_mode="HTML",
    )


@dp.message(Command("add"))
async def add_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/add <userid> <period_days|life>"))]),
            parse_mode="HTML",
        )
        return
    try:
        uid = int(parts[1]); period = parse_period(parts[2])
    except Exception:
        await message.reply(
            format_msg("❌ Invalid", sections=[("", "User ID must be a number. Period can be days or 'life'.")]),
            parse_mode="HTML",
        )
        return
    await db.add_user(uid, period)
    await message.reply(
        format_msg(
            "✅ Premium Updated",
            sections=[
                ("User ID", code(uid)),
                ("Period", "Lifetime ♾️" if period is None else f"{code(period)} days"),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("setcreditprice"))
async def setcreditprice_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/setcreditprice <price>"))]),
            parse_mode="HTML",
        )
        return
    try:
        price = float(parts[1])
    except Exception:
        await message.reply(
            format_msg("❌ Invalid", sections=[("", "Price must be a valid number.")]),
            parse_mode="HTML",
        )
        return
    _, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    await store.set_pay_plan(price, template)
    await message.reply(
        format_msg(
            "✅ Credit Price Updated",
            sections=[("New Price", f"INR {_format_money(price)} per credit")],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("setpay"))
async def setpay_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply(
            format_msg(
                "⚠️ Usage",
                sections=[("", bullet([
                    code("/setpay view"),
                    code("/setpay text <msg>"),
                ]))],
            ),
            parse_mode="HTML",
        )
        return
    sub = parts[1].strip().lower()
    price, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    if sub == "view":
        await message.reply(
            format_msg(
                "⚙️ Pay Plan",
                sections=[
                    ("Price", f"INR {_format_money(price)} per credit"),
                    ("Template", f"\n{esc(template)}"),
                ],
            ),
            parse_mode="HTML",
        )
        return
    if sub == "text":
        if len(parts) < 3:
            await message.reply(
                format_msg("⚠️ Usage", sections=[("", code("/setpay text <payment_text>"))]),
                parse_mode="HTML",
            )
            return
        await store.set_pay_plan(price, parts[2].strip())
        await message.reply(
            format_msg("✅ Payment Text Updated", sections=[]),
            parse_mode="HTML",
        )
        return
    await message.reply(
        format_msg("❌ Unknown Option", sections=[("", "Valid options: view, text")]),
        parse_mode="HTML",
    )


@dp.message(Command("editplan"))
async def editplan_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/editplan <price> [template]"))]),
            parse_mode="HTML",
        )
        return
    try:
        price = float(parts[1])
    except Exception:
        await message.reply(
            format_msg("❌ Invalid", sections=[("", "Price must be a valid number.")]),
            parse_mode="HTML",
        )
        return
    _, current = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    template = parts[2].strip() if len(parts) >= 3 else current
    await store.set_pay_plan(price, template)
    await message.reply(
        format_msg(
            "✅ Plan Updated",
            sections=[("New Price", f"INR {_format_money(price)} per credit")],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("setupi"))
async def setupi_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        current = await store.get_upi_id()
        await message.reply(
            format_msg("💳 UPI ID", sections=[("Current", code(current or "Not set"))]),
            parse_mode="HTML",
        )
        return
    upi = await store.set_upi_id(parts[1].strip())
    await message.reply(
        format_msg("✅ UPI Updated", sections=[("New UPI", code(esc(upi)))]),
        parse_mode="HTML",
    )


@dp.message(Command("broadcast"))
async def broadcast_cmd(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "This command is for admins only.")]),
            parse_mode="HTML",
        )
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            format_msg("⚠️ Usage", sections=[("", code("/broadcast <text>"))]),
            parse_mode="HTML",
        )
        return
    txt = parts[1].strip()
    users = await store.list_known_user_ids(limit=50000)
    if not users:
        await message.reply(
            format_msg("ℹ️ Broadcast", sections=[("", "No users to broadcast to yet.")]),
            parse_mode="HTML",
        )
        return
    sent = 0
    failed = 0
    for uid in users:
        try:
            await bot.send_message(uid, txt)
            sent += 1
        except Exception:
            failed += 1
    await message.reply(
        format_msg(
            "📣 Broadcast Complete",
            sections=[
                ("Sent", code(sent)),
                ("Failed", code(failed)),
            ],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("redeem"))
async def redeem_cmd(message: Message) -> None:
    await message.reply(
        format_msg(
            "ℹ️ Redeem",
            sections=[("", "Redeem command will be available in the next update.")],
        ),
        parse_mode="HTML",
    )


@dp.message(Command("paymentsdb"))
async def alias_paydb(message: Message) -> None:
    await paydb_cmd(message)


@dp.message(Command("bot"))
async def bot_cmd(message: Message) -> None:
    await message.reply(
        format_msg("🟢 Bot Status", sections=[("", "I am alive and running.")]),
        parse_mode="HTML",
    )


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
            await message.reply(
                format_msg("❌ Unknown Command", sections=[("", "Use /start to see available commands.")]),
                parse_mode="HTML",
            )
            return
        if message.chat.type == "private" and is_admin(message.from_user.id if message.from_user else None):
            await message.reply(
                format_msg(
                    "⚠️ Unsupported Media",
                    sections=[("", "Please send a document, video, audio, animation, voice, video note, or photo.")],
                ),
                parse_mode="HTML",
            )
        return

    if message.chat.type == "private" and not is_admin(message.from_user.id if message.from_user else None):
        await message.reply(
            format_msg("❌ Access Denied", sections=[("", "Only admins can upload files.")]),
            parse_mode="HTML",
        )
        return

    section_id, section_name = await store.get_section()
    if not section_id:
        if message.chat.type == "private":
            await message.reply(
                format_msg(
                    "⚠️ No Active Section",
                    sections=[("", f"Set a section first: {code('/addsection <name>')}.")],
                ),
                parse_mode="HTML",
            )
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
            format_msg(
                "✅ File Uploaded",
                sections=[
                    ("Section", esc(section_name or section_id)),
                    ("Normal", link("▶️ Open Stream", build_link(normal_token))),
                    ("Premium", link("⬇️ Download Stream", build_link(premium_token))),
                ],
                tip="Normal link = stream only. Premium link = downloadable.",
            ),
            parse_mode="HTML",
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
