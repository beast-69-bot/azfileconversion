import asyncio
from typing import AsyncGenerator, Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pyrogram import Client
from pyrogram.errors import FloodWait

from app.config import get_settings
from app.store import FileRef, TokenStore

settings = get_settings()
store = TokenStore(settings.redis_url)

app = FastAPI()

client = Client(
    "stream_api",
    api_id=settings.api_id,
    api_hash=settings.api_hash,
    bot_token=settings.bot_token,
    in_memory=True,
    no_updates=True,
)

_client_started = False
_client_lock = asyncio.Lock()


@app.on_event("startup")
async def on_startup() -> None:
    await store.connect()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if _client_started:
        await client.stop()
    await store.close()


async def ensure_client_started() -> None:
    global _client_started
    if _client_started:
        return
    async with _client_lock:
        if _client_started:
            return
        try:
            await client.start()
            _client_started = True
        except FloodWait as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Telegram flood-wait. Retry after {exc.value} seconds.",
                headers={"Retry-After": str(exc.value)},
            )


def parse_range(range_header: Optional[str], size: Optional[int]) -> tuple[int, Optional[int]]:
    if not range_header:
        return 0, None
    if not range_header.startswith("bytes="):
        raise HTTPException(status_code=416, detail="Invalid Range")

    range_spec = range_header.replace("bytes=", "", 1)
    start_str, _, end_str = range_spec.partition("-")
    if start_str == "":
        if size is None:
            raise HTTPException(status_code=416, detail="Invalid Range")
        length = int(end_str)
        start = max(size - length, 0)
        return start, size - 1

    start = int(start_str)
    end = int(end_str) if end_str else None
    if size is not None:
        if start >= size:
            raise HTTPException(status_code=416, detail="Range Not Satisfiable")
        if end is None or end >= size:
            end = size - 1
    return start, end


async def telegram_stream(file_id: str, start: int, end: Optional[int]) -> AsyncGenerator[bytes, None]:
    limit = None
    if end is not None:
        limit = end - start + 1
    async for chunk in client.iter_download(
        file_id,
        offset=start,
        limit=limit,
        chunk_size=settings.chunk_size,
    ):
        yield chunk
        await asyncio.sleep(0)


@app.get("/stream/{token}")
async def stream(token: str, range: Optional[str] = Header(None)):
    await ensure_client_started()

    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")

    start, end = parse_range(range, ref.file_size)
    total = ref.file_size

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": ref.mime_type or "application/octet-stream",
    }

    status_code = 200
    if range:
        status_code = 206
        if total is None:
            raise HTTPException(status_code=416, detail="Range Not Supported")
        content_length = (end - start + 1) if end is not None else total - start
        headers["Content-Range"] = f"bytes {start}-{start + content_length - 1}/{total}"
        headers["Content-Length"] = str(content_length)
    elif total is not None:
        headers["Content-Length"] = str(total)

    return StreamingResponse(
        telegram_stream(ref.file_id, start, end),
        status_code=status_code,
        headers=headers,
    )


@app.get("/player/{token}")
async def player(token: str):
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")

    media_tag = "video"
    if ref.media_type == "audio":
        media_tag = "audio"

    html = f"""
<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Stream</title>
    <style>
      body {{ font-family: Arial, sans-serif; background: #0b1020; color: #fff; margin: 0; display: grid; place-items: center; height: 100vh; }}
      .player {{ width: min(960px, 95vw); }}
      {media_tag} {{ width: 100%; height: auto; background: #000; }}
      a {{ color: #8ab4ff; }}
    </style>
  </head>
  <body>
    <div class=\"player\">
      <{media_tag} controls autoplay>
        <source src=\"/stream/{token}\" type=\"{ref.mime_type or 'application/octet-stream'}\" />
      </{media_tag}>
      <p><a href=\"/stream/{token}\">Direct stream link</a></p>
    </div>
  </body>
</html>
"""
    return HTMLResponse(content=html)