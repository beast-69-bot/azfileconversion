import asyncio
import logging
import time

from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from app.config import get_settings
from app.db import PremiumDB
from app.store import TokenStore

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


async def _notify_restart() -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(admin_id, f"Bot restarted and is online.\nTime: {stamp}")
        except Exception:
            pass


async def _deliver_token(message: Message, token: str) -> None:
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        await message.reply("Invalid/expired link.")
        return

    user_id = message.from_user.id if message.from_user else 0
    premium = await db.is_premium(user_id)

    if ref.access == "premium" and not premium:
        ok, bal = await store.charge_credits(user_id, CREDIT_COST)
        if not ok:
            await message.reply(f"Premium/Credit required. Balance: {bal}. Use /pay")
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
        "Welcome to FileLord.\n"
        "Commands:\n"
        "/credit\n/pay\n/premium"
    )


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


@dp.message(Command("pay"))
async def pay_cmd(message: Message) -> None:
    price, template = await store.get_pay_plan(DEFAULT_CREDIT_PRICE_INR, DEFAULT_PAY_TEXT)
    await message.reply(template.replace("{price}", f"{price:.2f}") + f"\nContact: {ADMIN_CONTACT}")


@dp.message(Command("premium"))
async def premium_cmd(message: Message) -> None:
    await message.reply(
        f"Premium Plan\nPrice: INR {PREMIUM_MONTHLY_PRICE_INR:.0f}\n"
        f"Duration: {PREMIUM_MONTHLY_DAYS} days\nUnlimited credits"
    )


@dp.message(Command("health"))
async def health_cmd(message: Message) -> None:
    await message.reply("I am alive!")


@dp.message(Command(["addsection", "showsections", "credit_add", "credit_remove", "add", "stats", "db", "broadcast", "payments", "paydb", "approve", "reject", "resetpaydb", "setpay", "editplan", "setupi", "setcreditprice", "paid", "redeem", "premiumlist", "history"]))
async def migrating_cmds(message: Message) -> None:
    await message.reply("This command is being migrated. Core commands are active.")


async def _startup() -> None:
    await store.connect()
    await db.connect()
    for admin_id in settings.admin_ids:
        await db.add_admin(admin_id)
    admins = await db.list_admins()
    settings.admin_ids.update(admins)
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
