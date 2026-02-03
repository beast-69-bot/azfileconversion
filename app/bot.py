import asyncio
import logging
import secrets
import time
import tempfile
from pathlib import Path

from pyrogram import Client, filters
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

async def send_premium_file(client: Client, user_id: int, ref: FileRef) -> None:
    try:
        await client.copy_message(
            chat_id=user_id,
            from_chat_id=ref.chat_id,
            message_id=ref.message_id,
        )
        return
    except Exception:
        pass
    await client.send_cached_media(
        chat_id=user_id,
        file_id=ref.file_id,
    )


@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message):
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text(
            "Hey! 👋\n"
            "Open a download link here and I’ll deliver the file.\n"
            "Admins: /add, /addsection, /showsections, /credit_add."
        )
        return

    payload = parts[1].strip()
    if not payload.startswith("dl_"):
        await message.reply_text("That link doesn’t look right. Please open a valid download link. 🙏")
        return

    if not message.from_user:
        await message.reply_text("I couldn’t read your user info. Please try again. 🙏")
        return

    user_id = message.from_user.id
    token = payload[3:]
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        await message.reply_text("This link is expired or invalid. Please ask for a fresh one. ⏳")
        return
    if ref.access != "premium":
        await message.reply_text("Please use the premium download link for this file. 🔒")
        return

    is_premium = await db.is_premium(user_id)
    if not is_premium:
        ok, balance = await store.charge_credits(user_id, CREDIT_COST)
        if not ok:
            await message.reply_text(f"Not enough credits. Balance: {balance}. 💳")
            return
        try:
            await send_premium_file(client, user_id, ref)
        except Exception:
            await store.add_credits(user_id, CREDIT_COST)
            raise
        await message.reply_text(f"✅ 1 credit used. Remaining: {balance}")
        return

    await send_premium_file(client, user_id, ref)


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


