import asyncio
import os
import time
import tempfile
from collections import deque
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.errors import FloodWait

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

if not API_ID or not API_HASH or not BOT_TOKEN:
    raise SystemExit("Missing API_ID, API_HASH, or BOT_TOKEN in environment.")

VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".webm", ".avi", ".mpeg", ".mpg", ".m4v"}
AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac", ".alac", ".wma"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tiff", ".tif"}


def human_bytes(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} PB"


def classify_document(doc) -> Optional[str]:
    mime = (doc.mime_type or "").lower()
    name = doc.file_name or ""
    ext = Path(name).suffix.lower()

    if mime.startswith("video/") or ext in VIDEO_EXTS:
        return "video"
    if mime.startswith("audio/") or ext in AUDIO_EXTS:
        return "audio"
    if mime.startswith("image/") or ext in IMAGE_EXTS:
        return "photo"

    if mime or ext:
        return "document"
    return None


class RecentCache:
    def __init__(self, maxlen: int = 2000) -> None:
        self.maxlen = maxlen
        self.queue = deque()
        self.keys = set()

    def add(self, key: str) -> bool:
        if key in self.keys:
            return False
        self.queue.append(key)
        self.keys.add(key)
        if len(self.queue) > self.maxlen:
            old = self.queue.popleft()
            self.keys.discard(old)
        return True


recent_cache = RecentCache()


async def progress_callback(current: int, total: int, phase: str, status, start_time: float, state: dict):
    now = time.time()
    if current != total and now - state["last"] < 0.7:
        return
    state["last"] = now

    percent = (current / total) * 100 if total else 0
    speed = current / max(now - start_time, 1e-6)
    text = (
        f"{phase}\n"
        f"{percent:.1f}% ({human_bytes(current)}/{human_bytes(total)})\n"
        f"{human_bytes(speed)}/s"
    )
    try:
        await status.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception:
        pass


app = Client("fileconversionbot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


@app.on_message(filters.document)
async def handle_document(client: Client, message):
    doc = message.document
    if not doc:
        return
    if message.edit_date:
        return

    if not recent_cache.add(doc.file_unique_id):
        return

    caption = message.caption
    status = await message.reply_text("Preparing...")

    kind = classify_document(doc)
    if kind is None:
        await status.edit_text("Unsupported file type.")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        state = {"last": 0.0}
        start_time = time.time()
        download_path = await message.download(
            file_name=tmpdir,
            progress=progress_callback,
            progress_args=("Downloading", status, start_time, state),
        )

        if not download_path:
            await status.edit_text("Download failed.")
            return

        upload_state = {"last": 0.0}
        upload_start = time.time()

        if kind == "video":
            await client.send_video(
                chat_id=message.chat.id,
                video=download_path,
                caption=caption,
                progress=progress_callback,
                progress_args=("Uploading video", status, upload_start, upload_state),
            )
        elif kind == "audio":
            await client.send_audio(
                chat_id=message.chat.id,
                audio=download_path,
                caption=caption,
                progress=progress_callback,
                progress_args=("Uploading audio", status, upload_start, upload_state),
            )
        elif kind == "photo":
            await client.send_photo(
                chat_id=message.chat.id,
                photo=download_path,
                caption=caption,
                progress=progress_callback,
                progress_args=("Uploading photo", status, upload_start, upload_state),
            )
        else:
            await client.send_document(
                chat_id=message.chat.id,
                document=download_path,
                caption=caption,
                file_name=doc.file_name or Path(download_path).name,
                progress=progress_callback,
                progress_args=("Uploading document", status, upload_start, upload_state),
            )

    try:
        await status.delete()
    except Exception:
        pass


app.run()
