import asyncio
import logging
import secrets
import time

from pyrogram import Client, filters
from pyrogram.errors import FloodWait

from app.config import get_settings
from app.db import PremiumDB
from app.store import FileRef, TokenStore

settings = get_settings()
store = TokenStore(settings.redis_url)
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


@app.on_message(filters.command("add") & filters.private)
async def add_premium_user(client: Client, message):
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.reply_text("Not allowed.")
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply_text("Usage: /add <userid> <period_days|life>")
        return

    try:
        user_id = int(parts[1])
        period = parse_period(parts[2])
    except Exception:
        await message.reply_text("Invalid format. Example: /add 123456 30")
        return

    await db.add_user(user_id, period)
    if period is None:
        await message.reply_text(f"Added {user_id} as lifetime premium.")
    else:
        await message.reply_text(f"Added {user_id} premium for {period} days.")


@app.on_message(filters.channel & (filters.document | filters.video | filters.audio))
async def handle_channel_media(client: Client, message):
    media = message.document or message.video or message.audio
    if not media:
        return

    logger.info("Media received chat_id=%s title=%s file_unique_id=%s", message.chat.id, message.chat.title, media.file_unique_id)

    normal_token = secrets.token_urlsafe(24)
    premium_token = secrets.token_urlsafe(24)

    base_ref = dict(
        chat_id=message.chat.id,
        message_id=message.id,
        file_unique_id=media.file_unique_id,
        file_name=getattr(media, "file_name", None),
        mime_type=media.mime_type,
        file_size=media.file_size,
        media_type=message.media.value,
        created_at=time.time(),
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
    try:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"Stream (Normal): {link}\n"
                f"Stream (Premium): {premium_link}"
            ),
        )
    except Exception as exc:
        logger.exception("Failed to send link: %s", exc)


async def runner() -> None:
    await store.connect()
    await db.connect()
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