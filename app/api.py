import asyncio
import hashlib
import hmac
import mimetypes
from typing import AsyncGenerator, Optional

from fastapi import FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from pyrogram import Client
from pyrogram.errors import FloodWait

from app.config import get_settings
from app.store import FileRef, TokenStore

settings = get_settings()
store = TokenStore(settings.redis_url, history_limit=settings.history_limit)

app = FastAPI()

client = Client(
    "stream_api",
    api_id=settings.api_id,
    api_hash=settings.api_hash,
    bot_token=settings.bot_token,
    no_updates=True,
    sleep_threshold=10000,
)

_client_started = False
_client_lock = asyncio.Lock()


_warm_lock = asyncio.Lock()


async def warm_client() -> None:
    async with _warm_lock:
        if _client_started:
            return
        while True:
            try:
                await client.start()
                globals()['_client_started'] = True
                return
            except FloodWait as exc:
                await asyncio.sleep(exc.value)
            except Exception:
                await asyncio.sleep(2)


@app.on_event("startup")
async def on_startup() -> None:
    await store.connect()
    asyncio.create_task(warm_client())


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



def human_size(num: int | None) -> str:
    if not num:
        return "Unknown size"
    size = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def resolve_mime(ref: FileRef) -> str:
    if ref.mime_type:
        return ref.mime_type
    if ref.file_name:
        guessed, _ = mimetypes.guess_type(ref.file_name)
        if guessed:
            return guessed
    return "application/octet-stream"


def password_enabled() -> bool:
    return bool(settings.stream_password)


def password_cookie_value() -> str:
    seed = f"azfileconversion:{settings.stream_password}".encode()
    return hashlib.sha256(seed).hexdigest()


def is_authed(request: Request) -> bool:
    if not password_enabled():
        return True
    cookie = request.cookies.get("stream_auth", "")
    return hmac.compare_digest(cookie, password_cookie_value())



def supports_iter_download() -> bool:
    return hasattr(client, "iter_download")



async def telegram_stream(message, start: int, end: Optional[int]) -> AsyncGenerator[bytes, None]:
    tg_chunk_size = 1024 * 1024
    chunk_offset = start // tg_chunk_size
    chunk_limit = 0
    if end is not None:
        byte_len = end - start + 1
        chunk_limit = ((byte_len + tg_chunk_size - 1) // tg_chunk_size) + 1

    if supports_iter_download():
        async for chunk in client.iter_download(message, offset=start, length=None if end is None else end - start + 1):
            if settings.chunk_size and settings.chunk_size < len(chunk):
                for i in range(0, len(chunk), settings.chunk_size):
                    yield chunk[i:i + settings.chunk_size]
            else:
                yield chunk
            await asyncio.sleep(0)
        return

    async for chunk in client.stream_media(message, offset=chunk_offset, limit=chunk_limit):
        if start or end is not None:
            if start:
                drop = start % tg_chunk_size
                chunk = chunk[drop:]
                start = 0
            if end is not None:
                remaining = end + 1
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]
                end = remaining - len(chunk) - 1
        if settings.chunk_size and settings.chunk_size < len(chunk):
            for i in range(0, len(chunk), settings.chunk_size):
                yield chunk[i:i + settings.chunk_size]
        else:
            yield chunk
        await asyncio.sleep(0)


async def fetch_message(chat_id: int, message_id: int):
    try:
        return await client.get_messages(chat_id, message_id)
    except Exception:
        try:
            await client.get_chat(chat_id)
            return await client.get_messages(chat_id, message_id)
        except Exception:
            return None


@app.get("/stream/{token}")
async def stream(token: str, request: Request, range: Optional[str] = Header(None)):
    await ensure_client_started()

    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")
    if ref.access == "normal" and not settings.public_stream:
        raise HTTPException(status_code=403, detail="Streaming is premium-only")
    if not is_authed(request):
        raise HTTPException(status_code=401, detail="Password required")

    message = await fetch_message(ref.chat_id, ref.message_id)
    stream_target = message if (message and message.media) else ref.file_id
    if not stream_target:
        raise HTTPException(status_code=404, detail="Message not found")

    start, end = parse_range(range, ref.file_size)
    total = ref.file_size

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": resolve_mime(ref),
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
        telegram_stream(stream_target, start, end),
        status_code=status_code,
        headers=headers,
    )


@app.get("/download/{token}")
async def download(token: str, request: Request, range: Optional[str] = Header(None)):
    await ensure_client_started()

    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")
    if not settings.direct_download:
        raise HTTPException(status_code=403, detail="Download via bot only")
    if ref.access != "premium":
        raise HTTPException(status_code=403, detail="Download is premium-only")
    if not is_authed(request):
        raise HTTPException(status_code=401, detail="Password required")

    message = await fetch_message(ref.chat_id, ref.message_id)
    stream_target = message if (message and message.media) else ref.file_id
    if not stream_target:
        raise HTTPException(status_code=404, detail="Message not found")

    start, end = parse_range(range, ref.file_size)
    total = ref.file_size

    filename = ref.file_name or "file"
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": resolve_mime(ref),
        "Content-Disposition": f'attachment; filename="{filename}"',
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
        telegram_stream(stream_target, start, end),
        status_code=status_code,
        headers=headers,
    )


def password_form_html(token: str, error: str = "") -> str:
    error_block = f"<p class=\"error\">{error}</p>" if error else ""
    return f"""
<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Protected Stream</title>
    <style>
      body { font-family: Arial, sans-serif; background: #0b1020; color: #fff; margin: 0; display: grid; place-items: center; height: 100vh; }
      .card { width: min(420px, 92vw); background: #111b33; padding: 28px; border-radius: 16px; text-align: center; }
      input { width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid #2a3a5f; background: #0f1a33; color: #fff; }
      button { margin-top: 12px; width: 100%; padding: 10px 12px; border: 0; border-radius: 10px; background: #7bdff2; color: #0b0f1a; font-weight: 700; cursor: pointer; }
      .error { color: #ffb3b3; font-size: 13px; margin: 10px 0 0; }
    </style>
  </head>
  <body>
    <div class=\"card\">
      <h2>Enter Password</h2>
      <form method=\"post\" action=\"/player/{token}\">
        <input type=\"password\" name=\"password\" placeholder=\"Password\" required />
        <button type=\"submit\">Unlock Stream</button>
      </form>
      {error_block}
    </div>
  </body>
</html>
"""


@app.get("/player/{token}")
async def player(token: str, request: Request):
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")
    if ref.access == "normal" and not settings.public_stream:
        html = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Premium Required</title>
    <style>
      body { font-family: Arial, sans-serif; background: #0b1020; color: #fff; margin: 0; display: grid; place-items: center; height: 100vh; }
      .card { width: min(640px, 95vw); background: #111b33; padding: 32px; border-radius: 16px; text-align: center; }
      a { color: #8ab4ff; }
    </style>
  </head>
  <body>
    <div class="card">
      <h2>Premium Required</h2>
      <p>This stream is available for premium users only.</p>
      <p>Please contact the admin to upgrade.</p>
    </div>
  </body>
</html>
"""
        return HTMLResponse(content=html, status_code=403)

    if password_enabled() and not is_authed(request):
        return HTMLResponse(content=password_form_html(token), status_code=401)

    media_tag = "video"
    if ref.media_type == "audio":
        media_tag = "audio"

    file_name = ref.file_name or "Unknown file"
    size_text = human_size(ref.file_size)
    download_block = ""
    if ref.access == "premium" and settings.bot_username:
        download_link = f"https://t.me/{settings.bot_username}?start=dl_{token}"
        download_block = f'<p><a href="{download_link}">Download</a></p>'

    html = f"""
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Stream</title>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
      :root {{
        --bg-1: #0b0f1a;
        --bg-2: #111a2b;
        --accent: #7bdff2;
        --accent-2: #f2a07b;
        --card: rgba(15, 22, 36, 0.9);
        --border: rgba(255, 255, 255, 0.08);
        --text: #e9eef8;
        --muted: #9fb0c9;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: 'Space Grotesk', system-ui, sans-serif;
        color: var(--text);
        min-height: 100vh;
        display: grid;
        place-items: center;
        background:
          radial-gradient(1200px 500px at 10% 0%, rgba(123, 223, 242, 0.12), transparent 60%),
          radial-gradient(900px 600px at 90% 10%, rgba(242, 160, 123, 0.14), transparent 60%),
          linear-gradient(135deg, var(--bg-1), var(--bg-2));
        overflow: hidden;
      }}
      body::before {{
        content: '';
        position: fixed;
        inset: 0;
        background: repeating-linear-gradient(
          115deg,
          rgba(255,255,255,0.03) 0px,
          rgba(255,255,255,0.03) 1px,
          transparent 1px,
          transparent 6px
        );
        pointer-events: none;
        opacity: 0.35;
      }}
      .shell {{
        width: min(1024px, 94vw);
        padding: 28px;
        border-radius: 24px;
        background: var(--card);
        border: 1px solid var(--border);
        box-shadow: 0 20px 60px rgba(0,0,0,0.35);
        backdrop-filter: blur(12px);
        animation: float-in 600ms ease-out;
      }}
      .header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 18px;
      }}
      .title {{
        font-size: 20px;
        font-weight: 700;
        letter-spacing: 0.3px;
      }}
      .meta {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-top: 6px;
      }}
      .chip {{
        font-family: 'IBM Plex Mono', ui-monospace, SFMono-Regular, monospace;
        font-size: 12px;
        color: var(--muted);
        padding: 6px 10px;
        border-radius: 999px;
        border: 1px solid var(--border);
        background: rgba(255,255,255,0.03);
      }}
      .player {{
        width: 100%;
        border-radius: 18px;
        overflow: hidden;
        border: 1px solid var(--border);
        background: #000;
      }}
      {media_tag} {{
        width: 100%;
        height: auto;
        display: block;
        background: #000;
      }}
      .actions {{
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-top: 16px;
        align-items: center;
      }}
      .btn {{
        text-decoration: none;
        color: #0b0f1a;
        background: linear-gradient(135deg, var(--accent), #b9f3ff);
        padding: 10px 16px;
        border-radius: 12px;
        font-weight: 600;
        transition: transform 120ms ease, box-shadow 120ms ease;
      }}
      .btn.secondary {{
        background: transparent;
        color: var(--text);
        border: 1px solid var(--border);
      }}
      .btn:hover {{
        transform: translateY(-1px);
        box-shadow: 0 8px 20px rgba(123, 223, 242, 0.2);
      }}
      .hint {{
        font-size: 12px;
        color: var(--muted);
        margin-top: 10px;
      }}
      @keyframes float-in {{
        from {{ opacity: 0; transform: translateY(10px); }}
        to {{ opacity: 1; transform: translateY(0); }}
      }}
      @media (max-width: 600px) {{
        .shell {{ padding: 18px; }}
        .title {{ font-size: 18px; }}
      }}
    </style>
  </head>
  <body>
    <div class="shell">
      <div class="header">
        <div>
          <div class="title">Streaming</div>
          <div class="meta">
            <div class="chip">{file_name}</div>
            <div class="chip">{size_text}</div>
            <div class="chip">{resolve_mime(ref)}</div>
          </div>
        </div>
      </div>
      <div class="player">
        <{media_tag} controls autoplay preload="auto" controlsList="nodownload">
          <source src="/stream/{token}" type="{resolve_mime(ref)}" />
        </{media_tag}>
      </div>
      <div class="actions">
        <a class="btn secondary" href="/stream/{token}">Direct stream</a>
        {download_block}
      </div>
      <div class="hint">If playback stalls, try refreshing once. Some files need a few seconds to start.</div>
    </div>
  </body>
</html>
"""

    return HTMLResponse(content=html)


@app.post("/player/{token}")
async def player_password(token: str, password: str = Form(...)):
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")
    if not password_enabled():
        return RedirectResponse(url=f"/player/{token}", status_code=302)
    if not hmac.compare_digest(password, settings.stream_password):
        return HTMLResponse(content=password_form_html(token, "Invalid password."), status_code=401)
    response = RedirectResponse(url=f"/player/{token}", status_code=302)
    response.set_cookie("stream_auth", password_cookie_value(), httponly=True, max_age=60 * 60 * 12, samesite="lax")
    return response
