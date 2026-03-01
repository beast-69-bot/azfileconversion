import asyncio
import hashlib
import hmac
import math
import mimetypes
import secrets
from typing import AsyncGenerator, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pyrogram import Client
from pyrogram.errors import FloodWait

from app.config import get_settings
from app.store import FileRef, TokenStore

settings = get_settings()
store = TokenStore(settings.redis_url, history_limit=settings.history_limit)

app = FastAPI()

# --- Jinja2 Templates + Static Files ---
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

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


@app.get("/")
async def root():
    return {"ok": True, "service": "azfileconversion", "docs": "/docs"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


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
    cookie_id = request.cookies.get("stream_viewer_id")
    if cookie_id:
        return cookie_id
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


# ---------------------------------------------------------------------------
#  Section password form (now uses Jinja2 template)
# ---------------------------------------------------------------------------

def _render_password_template(request: Request, **ctx):
    """Render the shared password.html template."""
    return templates.TemplateResponse("password.html", {"request": request, **ctx})


async def render_section(section_id: str, access_filter: str, request: Request) -> HTMLResponse:
    access_filter = (access_filter or "all").strip().lower()
    if password_enabled() and not is_authed(request):
        return templates.TemplateResponse(
            "password.html",
            {
                "request": request,
                "page_title": "Protected Section",
                "heading": "Section Locked",
                "subtext": "Enter the password to unlock this section.",
                "form_action": f"/section/{section_id}/auth?access={access_filter}",
                "button_text": "Unlock Section",
                "error": "",
            },
            status_code=401,
        )

    if settings.redis_url and getattr(store, "_redis", None) is None:
        await store.connect()

    if request.query_params.get("debug") == "1":
        tokens = await store.list_section(section_id, settings.history_limit)
        present = 0
        for token in tokens:
            ref = await store.get(token, settings.token_ttl_seconds)
            if not ref:
                continue
            present += 1
        return JSONResponse(
            {
                "section_id": section_id,
                "access_filter": access_filter,
                "tokens_total": len(tokens),
                "tokens_present": present,
                "tokens_filtered": present,
            }
        )

    tokens = await store.list_section(section_id, settings.history_limit)
    if not tokens:
        exists = await store.section_id_exists(section_id)
        if not exists:
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

    grouped: dict[str, dict] = {}
    for token in tokens:
        ref = await store.get(token, settings.token_ttl_seconds)
        if ref is None:
            continue
        ref_access = (ref.access or "normal").strip().lower()
        group_key = f"{ref.chat_id}:{ref.message_id}"
        name = ref.file_name or ref.file_unique_id or "file"
        size_text = human_size(ref.file_size)
        views_total, views_unique = await store.get_views(token)
        entry = grouped.setdefault(
            group_key,
            {
                "name": name,
                "size_text": size_text,
                "mime": resolve_mime(ref),
                "created_at": ref.created_at,
                "file_size": ref.file_size or 0,
                "views_total": 0,
                "views_unique": 0,
                "normal_link": "",
                "premium_link": "",
            },
        )
        if entry["created_at"] < ref.created_at:
            entry["created_at"] = ref.created_at
        if not entry["file_size"] and ref.file_size:
            entry["file_size"] = ref.file_size
            entry["size_text"] = size_text
        entry["views_total"] += views_total
        entry["views_unique"] += views_unique
        if settings.bot_username:
            bot_link = f"https://t.me/{settings.bot_username}?start=dl_{token}"
            if ref_access == "premium":
                entry["premium_link"] = bot_link
            else:
                entry["normal_link"] = bot_link

    entries = list(grouped.values())
    empty_section = not entries

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
    page_count = max(1, math.ceil(total_items / per_page)) if total_items else 1
    page = min(page, page_count)
    start = (page - 1) * per_page
    end = start + per_page
    show_start = 0 if total_items == 0 else start + 1
    page_entries = entries[start:end] if total_items else []

    max_views = max((item["views_total"] for item in entries), default=0)

    # Mark trending
    for item in page_entries:
        item["is_trending"] = bool(max_views and item["views_total"] == max_views)

    def build_query(**overrides: str) -> str:
        current = dict(request.query_params)
        current.update({k: str(v) for k, v in overrides.items() if v is not None})
        return "?" + urlencode(current)

    def sort_option(value: str, label: str) -> str:
        selected = " selected" if sort == value else ""
        return f'<option value="{value}"{selected}>{label}</option>'

    if request.query_params.get("debug") == "2":
        # Keep debug JSON response identical
        items = []
        if empty_section:
            items.append(
                '<li class="card empty">No files yet. Upload to this section to see items here.</li>'
            )
        for idx, item in enumerate(page_entries, start=show_start if total_items else 1):
            view_text = f"👁 {item['views_total']}"
            if item["views_unique"]:
                view_text = f"{view_text} · {item['views_unique']} unique"
            badge = ""
            if max_views and item["views_total"] == max_views:
                badge = '<span class="badge">Trending</span>'
            items.append(
                '<li class="card">'
                '<div class="card-main">'
                f'<div class="file-name" title="{item["name"]}">{idx}. {item["name"]}</div>'
                '<div class="file-meta">'
                f'<span>{item["size_text"]}</span>'
                f'<span>{item["mime"]}</span>'
                f'<span>{view_text}</span>'
                f'{badge}'
                '</div>'
                '</div>'
                '<div class="card-actions">'
                f'<a class="btn{"" if item["normal_link"] else " disabled"}" href="{item["normal_link"] or "#"}">Normal Open</a>'
                f'<a class="btn ghost{"" if item["premium_link"] else " disabled"}" href="{item["premium_link"] or "#"}">Premium Open</a>'
                '</div>'
                '</li>'
            )
        return JSONResponse(
            {
                "section_id": section_id,
                "access_filter": access_filter,
                "entries_len": len(entries),
                "page_entries_len": len(page_entries),
                "items_len": len(items),
                "items_sample": items[:2],
            }
        )

    send_all_normal_link = "#"
    send_all_normal_class = "btn btn-secondary btn-disabled"
    send_all_premium_link = "#"
    send_all_premium_class = "btn btn-ghost btn-disabled"
    if settings.bot_username and total_items > 0:
        send_all_normal_link = f"https://t.me/{settings.bot_username}?start=sa_{section_id}_normal"
        send_all_premium_link = f"https://t.me/{settings.bot_username}?start=sa_{section_id}_premium"
        send_all_normal_class = "btn btn-secondary"
        send_all_premium_class = "btn btn-ghost"

    prev_link = build_query(page=page - 1) if page > 1 else ""
    next_link = build_query(page=page + 1) if page < page_count else ""

    sort_options = [
        sort_option("name_asc", "Name A-Z"),
        sort_option("name_desc", "Name Z-A"),
        sort_option("newest", "Newest"),
        sort_option("oldest", "Oldest"),
        sort_option("size", "Size"),
    ]

    show_end = min(end, total_items)

    return templates.TemplateResponse(
        "section.html",
        {
            "request": request,
            "section_id": section_id,
            "base_url": settings.base_url,
            "total_items": total_items,
            "page": page,
            "page_count": page_count,
            "sort_options": sort_options,
            "send_all_normal_link": send_all_normal_link,
            "send_all_normal_class": send_all_normal_class,
            "send_all_premium_link": send_all_premium_link,
            "send_all_premium_class": send_all_premium_class,
            "skeleton_range": range(min(6, per_page)),
            "empty_section": empty_section,
            "page_entries": page_entries,
            "show_start_offset": show_start - 1 if total_items else 0,
            "max_views": max_views,
            "prev_link": prev_link,
            "next_link": next_link,
            "show_start": show_start,
            "show_end": show_end,
        },
    )


@app.get("/debug/section/{section_id}")
async def debug_section(section_id: str, access: str = "premium"):
    if settings.redis_url and getattr(store, "_redis", None) is None:
        await store.connect()
    tokens = await store.list_section(section_id, settings.history_limit)
    entries = []
    for token in tokens:
        ref = await store.get(token, settings.token_ttl_seconds)
        if not ref:
            entries.append({"token": token, "status": "missing"})
            continue
        if access == "premium" and ref.access != "premium":
            continue
        if access == "normal" and ref.access != "normal":
            continue
        entries.append({
            "token": token,
            "access": ref.access,
            "section_id": ref.section_id,
            "file_name": ref.file_name,
        })
    return {
        "tokens_total": len(tokens),
        "entries_filtered": len(entries),
        "entries": entries[:5],
    }

@app.get("/section/{section_id}")
async def section_page(section_id: str, request: Request):
    return await render_section(section_id, "all", request)


@app.get("/section/{section_id}/premium")
async def section_page_premium(section_id: str, request: Request):
    return await render_section(section_id, "all", request)




@app.post("/section/{section_id}/auth")
async def section_auth(section_id: str, request: Request, password: str = Form(...)):
    access = request.query_params.get("access", "normal")
    if not password_enabled():
        return RedirectResponse(url=f"/section/{section_id}" if access != "premium" else f"/section/{section_id}/premium", status_code=302)
    if not hmac.compare_digest(password, settings.stream_password):
        return templates.TemplateResponse(
            "password.html",
            {
                "request": request,
                "page_title": "Protected Section",
                "heading": "Section Locked",
                "subtext": "Enter the password to unlock this section.",
                "form_action": f"/section/{section_id}/auth?access={access}",
                "button_text": "Unlock Section",
                "error": "Invalid password.",
            },
            status_code=401,
        )
    response = RedirectResponse(url=f"/section/{section_id}" if access != "premium" else f"/section/{section_id}/premium", status_code=302)
    response.set_cookie("stream_auth", password_cookie_value(), httponly=True, max_age=60 * 60 * 12, samesite="lax")
    return response


@app.get("/player/{token}")
async def player(token: str, request: Request):
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")
    if ref.access == "normal" and not settings.public_stream:
        return templates.TemplateResponse("premium.html", {"request": request}, status_code=403)

    if password_enabled() and not is_authed(request):
        return templates.TemplateResponse(
            "password.html",
            {
                "request": request,
                "page_title": "Protected Stream",
                "heading": "Enter Password",
                "subtext": "This stream is password protected.",
                "form_action": f"/player/{token}",
                "button_text": "Unlock Stream",
                "error": "",
            },
            status_code=401,
        )

    viewer_cookie = request.cookies.get("stream_viewer_id")
    viewer_id = viewer_cookie or secrets.token_hex(16)
    await store.increment_view(token, viewer_id, settings.token_ttl_seconds)

    media_tag = "video"
    if ref.media_type == "audio":
        media_tag = "audio"

    file_name = ref.file_name or "Unknown file"
    size_text = human_size(ref.file_size)
    views_total, _ = await store.get_views(token)
    likes_total, liked = await store.get_likes(token, viewer_id)

    download_button_url = ""
    if ref.access == "premium" and settings.bot_username:
        download_button_url = f"https://t.me/{settings.bot_username}?start=dl_{token}"

    response = templates.TemplateResponse(
        "player.html",
        {
            "request": request,
            "token": token,
            "file_name": file_name,
            "size_text": size_text,
            "mime_type": resolve_mime(ref),
            "media_tag": media_tag,
            "views_total": views_total,
            "likes_total": likes_total,
            "liked": liked,
            "download_button_url": download_button_url,
        },
    )
    if not viewer_cookie:
        response.set_cookie("stream_viewer_id", viewer_id, httponly=True, max_age=60 * 60 * 24 * 30, samesite="lax")
    return response


@app.get("/player/{token}/download")
async def player_download(token: str, request: Request):
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")
    if ref.access != "premium":
        raise HTTPException(status_code=403, detail="Premium required")
    if settings.bot_username:
        return RedirectResponse(url=f"https://t.me/{settings.bot_username}?start=dl_{token}", status_code=302)
    return HTMLResponse(content="Download unavailable.", status_code=404)


@app.post("/player/{token}/like")
async def player_like(token: str, request: Request):
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    action = str(payload.get("action", "like")).lower()
    liked = action == "like"
    viewer_cookie = request.cookies.get("stream_viewer_id")
    viewer_id = viewer_cookie or secrets.token_hex(16)
    total, user_liked = await store.set_like(token, viewer_id, liked)
    response = JSONResponse({"total": total, "liked": user_liked})
    if not viewer_cookie:
        response.set_cookie("stream_viewer_id", viewer_id, httponly=True, max_age=60 * 60 * 24 * 30, samesite="lax")
    return response


@app.post("/player/{token}")
async def player_password(token: str, request: Request, password: str = Form(...)):
    ref = await store.get(token, settings.token_ttl_seconds)
    if not ref:
        raise HTTPException(status_code=404, detail="Invalid or expired token")
    if not password_enabled():
        return RedirectResponse(url=f"/player/{token}", status_code=302)
    if not hmac.compare_digest(password, settings.stream_password):
        return templates.TemplateResponse(
            "password.html",
            {
                "request": request,
                "page_title": "Protected Stream",
                "heading": "Enter Password",
                "subtext": "This stream is password protected.",
                "form_action": f"/player/{token}",
                "button_text": "Unlock Stream",
                "error": "Invalid password.",
            },
            status_code=401,
        )
    response = RedirectResponse(url=f"/player/{token}", status_code=302)
    response.set_cookie("stream_auth", password_cookie_value(), httponly=True, max_age=60 * 60 * 12, samesite="lax")
    return response
