import asyncio
import hashlib
import hmac
import math
import mimetypes
from typing import AsyncGenerator, Optional
from urllib.parse import urlencode

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


def viewer_fingerprint(request: Request) -> str:
    client_host = request.client.host if request.client else "unknown"
    agent = request.headers.get("user-agent", "unknown")
    raw = f"{client_host}:{agent}".encode()
    return hashlib.sha256(raw).hexdigest()


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









def section_password_form_html(section_id: str, access_filter: str, error: str = "") -> str:
    error_block = f"<p class=\"error\">{error}</p>" if error else ""
    return f"""
<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Protected Section</title>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
      :root {{
        --bg-1: #0b0f1a;
        --bg-2: #101a2c;
        --accent: #7bdff2;
        --accent-2: #f2a07b;
        --card: rgba(15, 22, 36, 0.92);
        --border: rgba(255, 255, 255, 0.08);
        --text: #e9eef8;
        --muted: #9fb0c9;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: 'Space Grotesk', system-ui, sans-serif;
        color: var(--text);
        display: grid;
        place-items: center;
        min-height: 100vh;
        background:
          radial-gradient(900px 420px at 20% 0%, rgba(123, 223, 242, 0.14), transparent 60%),
          radial-gradient(700px 520px at 90% 20%, rgba(242, 160, 123, 0.18), transparent 60%),
          linear-gradient(135deg, var(--bg-1), var(--bg-2));
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
        opacity: 0.35;
        pointer-events: none;
      }}
      .card {{
        width: min(460px, 92vw);
        background: var(--card);
        padding: 30px;
        border-radius: 20px;
        text-align: center;
        border: 1px solid var(--border);
        box-shadow: 0 20px 60px rgba(0,0,0,0.35);
        backdrop-filter: blur(12px);
      }}
      h2 {{ margin: 0 0 10px; font-size: 22px; }}
      p {{ color: var(--muted); margin: 0 0 16px; font-size: 14px; }}
      input {{
        width: 100%;
        padding: 12px 14px;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: rgba(255,255,255,0.04);
        color: var(--text);
        font-size: 14px;
      }}
      button {{
        margin-top: 14px;
        width: 100%;
        padding: 12px 14px;
        border: 0;
        border-radius: 12px;
        background: linear-gradient(135deg, var(--accent), #b9f3ff);
        color: #0b0f1a;
        font-weight: 700;
        cursor: pointer;
      }}
      .error {{ color: #ffb3b3; font-size: 13px; margin: 12px 0 0; }}
    </style>
  </head>
  <body>
    <div class=\"card\">
      <h2>Section Locked</h2>
      <p>Enter the password to unlock this section.</p>
      <form method=\"post\" action=\"/section/{section_id}/auth?access={access_filter}\">
        <input type=\"password\" name=\"password\" placeholder=\"Password\" required />
        <button type=\"submit\">Unlock Section</button>
      </form>
      {error_block}
    </div>
  </body>
</html>
"""

async def render_section(section_id: str, access_filter: str, request: Request) -> HTMLResponse:
    if password_enabled() and not is_authed(request):
        return HTMLResponse(content=section_password_form_html(section_id, access_filter), status_code=401)

    tokens = await store.list_section(section_id, settings.history_limit)
    if not tokens:
        raise HTTPException(status_code=404, detail="Section not found")

    sort = (request.query_params.get("sort") or "newest").lower()
    try:
        page = int(request.query_params.get("page", "1"))
    except ValueError:
        page = 1
    try:
        per_page = int(request.query_params.get("per_page", "24"))
    except ValueError:
        per_page = 24
    page = max(page, 1)
    per_page = max(6, min(per_page, 60))

    entries = []
    base_url = str(request.base_url).rstrip("/")
    for token in tokens:
        ref = await store.get(token, settings.token_ttl_seconds)
        if not ref:
            continue
        if access_filter == "premium" and ref.access != "premium":
            continue
        if access_filter == "normal" and ref.access != "normal":
            continue
        name = ref.file_name or ref.file_unique_id or "file"
        size_text = human_size(ref.file_size)
        views_total, views_unique = await store.get_views(token)
        entries.append({
            "token": token,
            "name": name,
            "size_text": size_text,
            "mime": resolve_mime(ref),
            "created_at": ref.created_at,
            "file_size": ref.file_size or 0,
            "views_total": views_total,
            "views_unique": views_unique,
            "download_ok": bool(settings.direct_download and ref.access == "premium"),
            "play_link": f"/player/{token}",
            "download_link": f"/download/{token}",
            "copy_link": f"{base_url}/player/{token}",
        })

    if not entries:
        raise HTTPException(status_code=404, detail="Section empty")

    if sort == "name_asc":
        entries.sort(key=lambda item: item["name"].lower())
    elif sort == "name_desc":
        entries.sort(key=lambda item: item["name"].lower(), reverse=True)
    elif sort == "oldest":
        entries.sort(key=lambda item: item["created_at"])
    elif sort == "size":
        entries.sort(key=lambda item: item["file_size"], reverse=True)
    else:
        entries.sort(key=lambda item: item["created_at"], reverse=True)
        sort = "newest"

    total_items = len(entries)
    page_count = max(1, math.ceil(total_items / per_page))
    page = min(page, page_count)
    start = (page - 1) * per_page
    end = start + per_page
    page_entries = entries[start:end]

    max_views = max((item["views_total"] for item in entries), default=0)

    def build_query(**overrides: str) -> str:
        current = dict(request.query_params)
        current.update({k: str(v) for k, v in overrides.items() if v is not None})
        return "?" + urlencode(current)

    def sort_option(value: str, label: str) -> str:
        selected = " selected" if sort == value else ""
        return f"<option value=\"{value}\"{selected}>{label}</option>"

    items = []
    for item in page_entries:
        view_text = f"👁 {item['views_total']}"
        if item["views_unique"]:
            view_text = f"{view_text} · {item['views_unique']} unique"
        badge = ""
        if max_views and item["views_total"] == max_views:
            badge = "<span class=\"badge\">Trending</span>"
        download_button = "<button class=\"btn ghost disabled\" disabled>Download</button>"
        if item["download_ok"]:
            download_button = f"<a class=\"btn ghost\" href=\"{item['download_link']}\">Download</a>"
        items.append(
            "<li class=\"card\">"
            "<div class=\"card-main\">"
            f"<div class=\"file-name\" title=\"{item['name']}\">{item['name']}</div>"
            "<div class=\"file-meta\">"
            f"<span>{item['size_text']}</span>"
            f"<span>{item['mime']}</span>"
            f"<span>{view_text}</span>"
            f"{badge}"
            "</div>"
            "</div>"
            "<div class=\"card-actions\">"
            f"<a class=\"btn\" href=\"{item['play_link']}\">Play</a>"
            f"{download_button}"
            f"<button class=\"btn ghost copy\" data-copy=\"{item['copy_link']}\">Copy Link</button>"
            "</div>"
            "</li>"
        )

    skeleton_items = "".join(["<li class=\"card skeleton\"><div class=\"line w-60\"></div><div class=\"line w-40\"></div><div class=\"line w-30\"></div></li>" for _ in range(min(6, per_page))])

    title = f"Section ({access_filter.title()}): {section_id}"
    breadcrumb = f"<a href=\"{settings.base_url}\">Home</a> <span>→</span> <span>Section</span> <span>→</span> <span>{section_id}</span>"

    prev_link = build_query(page=page - 1) if page > 1 else ""
    next_link = build_query(page=page + 1) if page < page_count else ""

    sort_options = [
        sort_option("name_asc", "Name A-Z"),
        sort_option("name_desc", "Name Z-A"),
        sort_option("newest", "Newest"),
        sort_option("oldest", "Oldest"),
        sort_option("size", "Size"),
    ]

    prev_class = "page disabled" if not prev_link else "page"
    next_class = "page disabled" if not next_link else "page"
    show_start = start + 1
    show_end = min(end, total_items)

    html = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>__TITLE__</title>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
      :root {
        --bg-1: #0b0f1a;
        --bg-2: #111a2b;
        --accent: #7bdff2;
        --accent-2: #f2a07b;
        --card: rgba(15, 22, 36, 0.92);
        --border: rgba(255, 255, 255, 0.08);
        --text: #e9eef8;
        --muted: #9fb0c9;
        --shadow: 0 20px 60px rgba(0,0,0,0.35);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: 'Space Grotesk', system-ui, sans-serif;
        color: var(--text);
        min-height: 100vh;
        background:
          radial-gradient(1100px 540px at 10% 0%, rgba(123, 223, 242, 0.12), transparent 60%),
          radial-gradient(900px 700px at 90% 20%, rgba(242, 160, 123, 0.16), transparent 60%),
          linear-gradient(135deg, var(--bg-1), var(--bg-2));
        padding: 28px 18px 60px;
      }
      body::before {
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
        opacity: 0.35;
        pointer-events: none;
      }
      .shell {
        width: min(1080px, 96vw);
        margin: 0 auto;
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 24px;
        padding: 24px;
        box-shadow: var(--shadow);
        backdrop-filter: blur(12px);
      }
      .breadcrumb {
        font-size: 13px;
        color: var(--muted);
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
        margin-bottom: 16px;
      }
      .breadcrumb a { color: var(--accent); text-decoration: none; }
      .breadcrumb span { color: var(--muted); }
      .header {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 18px;
      }
      .title { font-size: 22px; font-weight: 700; }
      .meta { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
      .chip {
        font-family: 'IBM Plex Mono', ui-monospace, SFMono-Regular, monospace;
        font-size: 12px;
        color: var(--muted);
        padding: 6px 10px;
        border-radius: 999px;
        border: 1px solid var(--border);
        background: rgba(255,255,255,0.03);
      }
      .controls { display: flex; gap: 10px; align-items: center; }
      .controls label { font-size: 12px; color: var(--muted); }
      select {
        background: rgba(255,255,255,0.04);
        color: var(--text);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 8px 12px;
        font-size: 13px;
        font-family: 'IBM Plex Mono', ui-monospace, SFMono-Regular, monospace;
      }
      select:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
      .list { list-style: none; padding: 0; margin: 0; display: grid; gap: 12px; }
      .card {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding: 16px 18px;
        border-radius: 16px;
        border: 1px solid var(--border);
        background: rgba(255,255,255,0.02);
        transition: transform 140ms ease, border-color 140ms ease, box-shadow 140ms ease;
      }
      .card:hover {
        transform: translateY(-1px);
        border-color: rgba(123, 223, 242, 0.35);
        box-shadow: 0 14px 30px rgba(0,0,0,0.25);
      }
      .card:focus-within { border-color: rgba(123, 223, 242, 0.6); }
      .card-main { min-width: 0; }
      .file-name {
        font-size: 16px;
        font-weight: 600;
        color: var(--text);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 560px;
      }
      .file-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        font-size: 12px;
        color: var(--muted);
        margin-top: 6px;
      }
      .badge {
        background: rgba(242, 160, 123, 0.2);
        color: #f2a07b;
        border: 1px solid rgba(242, 160, 123, 0.4);
        padding: 2px 8px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 600;
      }
      .card-actions { display: flex; gap: 8px; flex-wrap: wrap; }
      .btn {
        text-decoration: none;
        color: #0b0f1a;
        background: linear-gradient(135deg, var(--accent), #b9f3ff);
        padding: 8px 12px;
        border-radius: 10px;
        font-weight: 600;
        border: none;
        cursor: pointer;
        transition: transform 120ms ease, box-shadow 120ms ease;
      }
      .btn:hover {
        transform: translateY(-1px);
        box-shadow: 0 8px 18px rgba(123, 223, 242, 0.18);
      }
      .btn.ghost:hover { border-color: rgba(123, 223, 242, 0.4); }
      .btn.disabled {
        opacity: 0.5;
        cursor: not-allowed;
        box-shadow: none;
      }
      .btn.disabled:hover {
        transform: none;
        box-shadow: none;
      }
      .btn.ghost {
        background: transparent;
        color: var(--text);
        border: 1px solid var(--border);
      }
      .btn:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
      a:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
      .pagination {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin-top: 18px;
      }
      .page {
        text-decoration: none;
        color: var(--text);
        border: 1px solid var(--border);
        padding: 8px 12px;
        border-radius: 10px;
      }
      .page:hover { border-color: rgba(123, 223, 242, 0.35); }
      .page.disabled { opacity: 0.45; pointer-events: none; }
      .hint { margin-top: 16px; font-size: 12px; color: var(--muted); }
      .skeleton {
        border-style: dashed;
        background: rgba(255,255,255,0.02);
        position: relative;
        overflow: hidden;
      }
      .skeleton .line {
        height: 10px;
        border-radius: 999px;
        background: rgba(255,255,255,0.12);
        margin: 6px 0;
      }
      .skeleton .w-60 { width: 60%; }
      .skeleton .w-40 { width: 40%; }
      .skeleton .w-30 { width: 30%; }
      .content { opacity: 1; transition: opacity 200ms ease; }
      body.is-loading .content { opacity: 0; height: 0; overflow: hidden; }
      body.is-loading .skeleton-list { display: grid; }
      .skeleton-list { display: none; list-style: none; padding: 0; margin: 0; gap: 12px; }
      @media (max-width: 720px) {
        .shell { padding: 18px; }
        .file-name { max-width: 280px; }
        .card { flex-direction: column; align-items: flex-start; }
        .card-actions { width: 100%; }
        .card-actions .btn { flex: 1; text-align: center; }
        .pagination { flex-direction: column; align-items: stretch; }
      }
    </style>
  </head>
  <body class="is-loading">
    <div class="shell">
      <nav class="breadcrumb" aria-label="Breadcrumb">__BREADCRUMB__</nav>
      <div class="header">
        <div>
          <div class="title">__TITLE__</div>
          <div class="meta">
            <div class="chip">__TOTAL__ files</div>
            <div class="chip">Access: __ACCESS__</div>
            <div class="chip">Page __PAGE__ of __PAGE_COUNT__</div>
          </div>
        </div>
        <div class="controls">
          <label for="sort">Sort</label>
          <select id="sort" name="sort">
            __SORT_OPTIONS__
          </select>
        </div>
      </div>
      <ul class="skeleton-list">__SKELETON__</ul>
      <div class="content">
        <ul class="list">
          __ITEMS__
        </ul>
      </div>
      <div class="pagination">
        <a class="__PREV_CLASS__" href="__PREV_HREF__">Prev</a>
        <div class="chip">Showing __SHOW_START__-__SHOW_END__ of __TOTAL__</div>
        <a class="__NEXT_CLASS__" href="__NEXT_HREF__">Next</a>
      </div>
      <div class="hint">Tip: If a file does not open, refresh once or try again later.</div>
    </div>
    <script>
      const sortSelect = document.getElementById('sort');
      if (sortSelect) {
        sortSelect.addEventListener('change', () => {
          const url = new URL(window.location.href);
          url.searchParams.set('sort', sortSelect.value);
          url.searchParams.set('page', '1');
          window.location.href = url.toString();
        });
      }
      document.querySelectorAll('[data-copy]').forEach((btn) => {
        btn.addEventListener('click', async () => {
          const link = btn.getAttribute('data-copy');
          if (!link) return;
          try {
            await navigator.clipboard.writeText(link);
            const original = btn.textContent;
            btn.textContent = 'Copied';
            setTimeout(() => (btn.textContent = original), 1200);
          } catch (err) {
            window.prompt('Copy link', link);
          }
        });
      });
      document.body.classList.remove('is-loading');
    </script>
  </body>
</html>
"""
    html = (html
            .replace("__TITLE__", title)
            .replace("__BREADCRUMB__", breadcrumb)
            .replace("__TOTAL__", str(total_items))
            .replace("__ACCESS__", access_filter.title())
            .replace("__PAGE__", str(page))
            .replace("__PAGE_COUNT__", str(page_count))
            .replace("__SORT_OPTIONS__", "".join(sort_options))
            .replace("__SKELETON__", skeleton_items)
            .replace("__ITEMS__", "".join(items))
            .replace("__PREV_CLASS__", prev_class)
            .replace("__NEXT_CLASS__", next_class)
            .replace("__PREV_HREF__", prev_link or "#")
            .replace("__NEXT_HREF__", next_link or "#")
            .replace("__SHOW_START__", str(show_start))
            .replace("__SHOW_END__", str(show_end))
    )
    return HTMLResponse(content=html)


@app.get("/section/{section_id}")
async def section_page(section_id: str, request: Request):
    return await render_section(section_id, "normal", request)


@app.get("/section/{section_id}/premium")
async def section_page_premium(section_id: str, request: Request):
    return await render_section(section_id, "premium", request)




@app.post("/section/{section_id}/auth")
async def section_auth(section_id: str, request: Request, password: str = Form(...)):
    access = request.query_params.get("access", "normal")
    if not password_enabled():
        return RedirectResponse(url=f"/section/{section_id}" if access != "premium" else f"/section/{section_id}/premium", status_code=302)
    if not hmac.compare_digest(password, settings.stream_password):
        return HTMLResponse(content=section_password_form_html(section_id, access, "Invalid password."), status_code=401)
    response = RedirectResponse(url=f"/section/{section_id}" if access != "premium" else f"/section/{section_id}/premium", status_code=302)
    response.set_cookie("stream_auth", password_cookie_value(), httponly=True, max_age=60 * 60 * 12, samesite="lax")
    return response


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
      body {{ font-family: Arial, sans-serif; background: #0b1020; color: #fff; margin: 0; display: grid; place-items: center; height: 100vh; }}
      .card {{ width: min(420px, 92vw); background: #111b33; padding: 28px; border-radius: 16px; text-align: center; }}
      input {{ width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid #2a3a5f; background: #0f1a33; color: #fff; }}
      button {{ margin-top: 12px; width: 100%; padding: 10px 12px; border: 0; border-radius: 10px; background: #7bdff2; color: #0b0f1a; font-weight: 700; cursor: pointer; }}
      .error {{ color: #ffb3b3; font-size: 13px; margin: 10px 0 0; }}
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

    viewer_id = viewer_fingerprint(request)
    await store.increment_view(token, viewer_id, settings.token_ttl_seconds)

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


