import asyncio
import secrets
import time

from pyrogram import Client, filters
from pyrogram.errors import FloodWait

from app.config import get_settings
from app.store import FileRef, TokenStore

settings = get_settings()
store = TokenStore(settings.redis_url)

app = Client(
    "stream_bot",
    api_id=settings.api_id,
    api_hash=settings.api_hash,
    bot_token=settings.bot_token,
    in_memory=True,
)


def build_link(token: str) -> str:
    return f"{settings.base_url}/player/{token}"


@app.on_message(filters.channel & (filters.document | filters.video | filters.audio))
async def handle_channel_media(client: Client, message):
    media = message.document or message.video or message.audio
    if not media:
        return

    token = secrets.token_urlsafe(24)
    ref = FileRef(
        file_id=media.file_id,
        file_unique_id=media.file_unique_id,
        file_name=getattr(media, "file_name", None),
        mime_type=media.mime_type,
        file_size=media.file_size,
        media_type=message.media.value,
        created_at=time.time(),
    )
    await store.set(token, ref, settings.token_ttl_seconds)

    link = build_link(token)
    await client.send_message(chat_id=message.chat.id, text=f"Stream: {link}")


async def runner() -> None:
    await store.connect()
    while True:
        try:
            await app.start()
            break
        except FloodWait as exc:
            await asyncio.sleep(exc.value)
    try:
        await asyncio.Event().wait()
    finally:
        await app.stop()
        await store.close()


if __name__ == "__main__":
    app.run(runner())