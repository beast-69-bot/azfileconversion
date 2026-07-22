import os
import asyncio
import aiohttp
import hashlib
import hmac
import math
import mimetypes
import secrets
import time
from typing import AsyncGenerator, Optional
from urllib.parse import urlencode, quote, urljoin
from xml.sax.saxutils import escape

from fastapi import FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pyrogram import Client
from pyrogram.errors import FloodWait
from starlette.middleware.gzip import GZipMiddleware

from app.config import get_settings
from app.mongo_store import MongoTokenStore
from app.store import FileRef, TokenStore

settings = get_settings()
store = (
    MongoTokenStore(settings.redis_url, settings.mongo_uri, settings.mongo_db_name, history_limit=settings.history_limit)
    if settings.mongo_uri
    else TokenStore(settings.redis_url, history_limit=settings.history_limit)
)

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1024)

# --- Jinja2 Templates + Static Files ---
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

client = Client(
    "stream_api",
    api_id=settings.api_id,
    api_hash=settings.api_hash,
    bot_token=settings.bot_token,
    no_updates=True,
    sleep_threshold=10,
)

# ── Prevent Telegram Multi-DC Export Authorization FloodWaits via In-Memory Caching ──
from pyrogram.raw.functions.auth import ExportAuthorization

_exported_auth_cache = {}
_export_auth_lock = asyncio.Lock()
_orig_invoke = client.invoke

async def _locked_invoke(query, *args, **kwargs):
    if isinstance(query, ExportAuthorization):
        dc_id = getattr(query, "dc_id", None)
        if dc_id and dc_id in _exported_auth_cache:
            return _exported_auth_cache[dc_id]
        async with _export_auth_lock:
            if dc_id and dc_id in _exported_auth_cache:
                return _exported_auth_cache[dc_id]
            try:
                res = await _orig_invoke(query, *args, **kwargs)
                if dc_id and res:
                    _exported_auth_cache[dc_id] = res
                return res
            except Exception:
                raise
    return await _orig_invoke(query, *args, **kwargs)

client.invoke = _locked_invoke

_client_started = False
_client_lock = asyncio.Lock()


_warm_lock = asyncio.Lock()


def canonical_url(path: str = "/") -> str:
    clean_path = "/" + path.lstrip("/")
    if clean_path != "/":
        clean_path = clean_path.rstrip("/")
    return f"{settings.base_url}{clean_path}"


def xml_url(location: str, priority: str, changefreq: str = "daily", lastmod: str = "") -> str:
    safe_location = escape(location, {'"': "&quot;"})
    lastmod_tag = f"    <lastmod>{lastmod}</lastmod>\n" if lastmod else ""
    return (
        "  <url>\n"
        f"    <loc>{safe_location}</loc>\n"
        f"{lastmod_tag}"
        f"    <changefreq>{changefreq}</changefreq>\n"
        f"    <priority>{priority}</priority>\n"
        "  </url>"
    )


def make_section_name(section_id: str) -> str:
    """Convert a raw section_id slug to a human-readable title.
    E.g. 'latest-movies-2024' -> 'Latest Movies 2024'
         'batch_123'          -> 'Batch 123'
    """
    import re
    name = re.sub(r'[-_]+', ' ', section_id)
    return name.title()


@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    try:
        response = await call_next(request)
    except Exception:
        return Response(status_code=204)
    if response is not None and hasattr(response, "headers"):
        path = request.url.path
        if path.startswith("/static/"):
            response.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")
        elif path == "/favicon.ico":
            response.headers.setdefault("Cache-Control", "public, max-age=86400")
        elif request.method == "GET" and (response.headers.get("content-type") or "").startswith("text/html"):
            response.headers.setdefault("Cache-Control", "public, max-age=60")
    return response


@app.get("/placeholder.png")
async def placeholder_image():
    # 1x1 transparent PNG byte string to satisfy video poster requests cleanly
    transparent_png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15c4\x00\x00\x00\rIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    return Response(content=transparent_png, media_type="image/png")


async def register_site_visit_safely(visitor_id: str) -> None:
    try:
        await store.register_site_visit(visitor_id)
    except Exception:
        return


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    bot_link = f"https://telegram.me/{settings.bot_username}" if settings.bot_username else "#"
    visitor_cookie = request.cookies.get("site_visitor_id")
    visitor_id = visitor_cookie or secrets.token_hex(16)
    asyncio.create_task(register_site_visit_safely(visitor_id))
    response = templates.TemplateResponse(
        request=request,
        name="home.html",
        context={
            "request": request,
            "bot_link": bot_link,
            "bot_ready": bool(settings.bot_username),
            "site_visits_total": 0,
            "site_visits_text": "0",
            "canonical_url": canonical_url("/"),
        },
    )
    if not visitor_cookie:
        response.set_cookie("site_visitor_id", visitor_id, httponly=True, max_age=60 * 60 * 24 * 365, samesite="lax")
    return response


@app.get("/bots", response_class=HTMLResponse)
async def bots_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="bots.html",
        context={
            "request": request,
            "canonical_url": canonical_url("/bots"),
        },
    )




@app.get("/sections", response_class=HTMLResponse)
async def public_sections(request: Request):
    try:
        rows = await store.list_public_sections()
    except Exception:
        rows = []
    section_cards = [
        {
            "name": name or section_id,
            "id": section_id,
            "href": f"/section/{section_id}",
            "premium_href": f"/section/{section_id}/premium",
        }
        for name, section_id in rows
        if section_id
    ]
    return templates.TemplateResponse(
        request=request,
        name="public_sections.html",
        context={
            "request": request,
            "section_cards": section_cards,
            "canonical_url": canonical_url("/sections"),
        },
    )


@app.get("/trending", response_class=HTMLResponse)
async def trending_page(request: Request):
    try:
        items = await store.list_trending_items(settings.history_limit)
    except Exception:
        items = []

    grouped: dict[str, list[dict]] = {}
    for item in items:
        bar = str(item.get("bar") or "Trending").strip() or "Trending"
        media_items = _trending_media_items(item)
        grouped.setdefault(bar, []).append(
            {
                "id": item.get("id", ""),
                "bar": bar,
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "media_type": media_items[0]["type"] if media_items else "",
                "media_url": media_items[0]["url"] if media_items else "",
                "media_items": media_items,
                "media_count": len(media_items),
                "normal_link": item.get("normal_link", "#"),
                "premium_link": item.get("premium_link", "#"),
            }
        )

    rows = [{"bar": bar, "items": row_items} for bar, row_items in grouped.items()]
    return templates.TemplateResponse(
        request=request,
        name="trending.html",
        context={
            "request": request,
            "trending_rows": rows,
            "total_items": len(items),
            "canonical_url": canonical_url("/trending"),
        },
    )


def _trending_media_items(item: dict) -> list[dict]:
    item_id = str(item.get("id", "") or "").strip()
    media_items: list[dict] = []
    for index, media in enumerate(item.get("media", []) or []):
        file_id = str((media or {}).get("file_id", "") or "").strip()
        media_type = str((media or {}).get("type", "") or "").strip().lower()
        if item_id and file_id and media_type in {"photo", "video"}:
            media_items.append({
                "index": index,
                "type": media_type,
                "url": f"/trending/media/{item_id}/{index}",
            })
    if not media_items:
        file_id = str(item.get("media_file_id", "") or "").strip()
        media_type = str(item.get("media_type", "") or "").strip().lower()
        if item_id and file_id and media_type in {"photo", "video"}:
            media_items.append({"index": 0, "type": media_type, "url": f"/trending/media/{item_id}/0"})
    return media_items


@app.get("/trending/media/{item_id}")
async def trending_media_legacy(item_id: str, range: Optional[str] = Header(default=None)):
    return await trending_media(item_id, 0, range)


@app.get("/trending/media/{item_id}/{media_index}")
async def trending_media(item_id: str, media_index: int, range: Optional[str] = Header(default=None)):
    item = await store.get_trending_item(item_id)
    media_items = []
    if item:
        media_items = [
            media for media in (item.get("media", []) or [])
            if str((media or {}).get("file_id", "") or "").strip()
        ]
        if not media_items and item.get("media_file_id"):
            media_items = [{"file_id": item.get("media_file_id", ""), "type": item.get("media_type", "")}]
    if not item or media_index < 0 or media_index >= len(media_items):
        raise HTTPException(status_code=404, detail="Trending media not found")

    await ensure_client_started()
    selected = media_items[media_index]
    media_type = str(selected.get("type") or "").lower()
    content_type = "video/mp4" if media_type == "video" else "image/jpeg"
    headers = {
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=300",
        "Content-Type": content_type,
    }
    start, end = 0, None
    status_code = 200
    return StreamingResponse(
        telegram_stream(selected["file_id"], start, end),
        status_code=status_code,
        headers=headers,
        media_type=content_type,
    )


@app.get("/health")
async def health():
    return {"status": "healthy"}


# ─────────────────────────────────────────────────────────────
# DARK ARCHIVES ROUTING & VOTE ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.get("/dark-archives", response_class=HTMLResponse)
async def dark_archives_page(request: Request):
    """
    Renders the Premium Dark Archives vault page. Enforces password protection.
    """
    # Fetch configured password
    vault_pass = "dark18"
    if hasattr(store, "get_vault_password"):
        vault_pass = await store.get_vault_password()

    # Generate expected auth cookie hash validation value
    expected_hash = hashlib.sha256(vault_pass.encode()).hexdigest()
    auth_cookie = request.cookies.get("dark_vault_auth")

    # If password protection is active and cookie is missing/incorrect, load login page
    if auth_cookie != expected_hash:
        return templates.TemplateResponse(
            request=request,
            name="dark_login.html",
            context={
                "request": request,
                "error": False,
                "canonical_url": canonical_url("/dark-archives"),
            }
        )

    try:
        posts = []
        if hasattr(store, "list_dark_posts"):
            posts = await store.list_dark_posts()
    except Exception:
        posts = []

    # Inject client IP / Cookie identifier to verify if user has already voted on each card
    visitor_id = request.cookies.get("site_visitor_id") or request.client.host
    user_hash = hashlib.md5(visitor_id.encode()).hexdigest()

    # Tag each post with user's specific past vote to highlight buttons
    for post in posts:
        votes = post.get("votes", {})
        post["user_vote"] = votes.get(user_hash, "none")
        post["seconds_remaining"] = max(0, int(post.get("expires_at", 0) - time.time()))

    return templates.TemplateResponse(
        request=request,
        name="dark_archives.html",
        context={
            "request": request,
            "posts": posts,
            "canonical_url": canonical_url("/dark-archives"),
        },
    )


@app.post("/api/dark-archives/auth", response_class=JSONResponse)
async def api_dark_vault_auth(request: Request):
    """
    Verifies the submitted vault password and issues a cookie on success.
    """
    try:
        body = await request.json()
        password = str(body.get("password", "")).strip()

        vault_pass = "dark18"
        if hasattr(store, "get_vault_password"):
            vault_pass = await store.get_vault_password()

        if password == vault_pass:
            cookie_hash = hashlib.sha256(vault_pass.encode()).hexdigest()
            response = JSONResponse({"success": True})
            # Register auth cookie valid for 1 day (24 hours)
            response.set_cookie(
                "dark_vault_auth",
                cookie_hash,
                httponly=True,
                max_age=60 * 60 * 24 * 1,
                samesite="lax",
                secure=False  # Allow local/heroku routing without https restrictions if testing
            )
            return response
        else:
            return JSONResponse({"success": False, "error": "Incorrect password entered."}, status_code=401)
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@app.post("/api/dark-archives/{post_id}/action", response_class=JSONResponse)
async def api_dark_post_action(post_id: str):
    """
    Increments post view counts on link click actions.
    """
    try:
        if hasattr(store, "increment_dark_post_views"):
            await store.increment_dark_post_views(post_id)
            post = await store.get_dark_post(post_id)
            return JSONResponse({"success": True, "views": post.get("views", 0) if post else 0})
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)
    return JSONResponse({"success": False, "error": "Not supported"}, status_code=400)


@app.post("/api/dark-archives/{post_id}/vote", response_class=JSONResponse)
async def api_dark_post_vote(post_id: str, request: Request):
    """
    Casts a vote (like/dislike) for a dynamic post checking user cookie/IP fingerprint to avoid duplicate votes.
    """
    try:
        body = await request.json()
        vote_type = str(body.get("type", "")).strip().lower()
        if vote_type not in {"like", "dislike"}:
            return JSONResponse({"success": False, "error": "Invalid vote type"}, status_code=400)

        visitor_id = request.cookies.get("site_visitor_id") or request.client.host
        user_hash = hashlib.md5(visitor_id.encode()).hexdigest()

        if hasattr(store, "vote_dark_post"):
            res = await store.vote_dark_post(post_id, user_hash, vote_type)
            return JSONResponse({
                "success": True,
                "likes": res.get("likes", 0),
                "dislikes": res.get("dislikes", 0),
                "user_vote": res.get("user_vote", "none")
            })
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)
    return JSONResponse({"success": False, "error": "Not supported"}, status_code=400)


# ─────────────────────────────────────────────────────────────
# PREMIUM JSON API ENDPOINTS
# Used by Chart.js dashboard, infinite scroll, live search
# ─────────────────────────────────────────────────────────────

@app.get("/api/stats", response_class=JSONResponse)
async def api_stats():
    """
    Returns aggregate site statistics for the analytics dashboard.
    """
    try:
        visit_stats = await store.get_site_visit_stats()
    except Exception:
        visit_stats = {}

    try:
        sections = await store.list_public_sections()
        section_count = len(sections)
    except Exception:
        section_count = 0

    try:
        trending_items = await store.list_trending_items(limit=1000)
        trending_count = len(trending_items)
    except Exception:
        trending_count = 0

    return JSONResponse({
        "total_visits": visit_stats.get("total", 0),
        "unique_visits": visit_stats.get("unique", 0),
        "section_count": section_count,
        "trending_count": trending_count,
        "daily": visit_stats.get("daily", []),
    })


@app.get("/api/trending/latest", response_class=JSONResponse)
async def api_trending_latest(limit: int = 20):
    """
    Returns the latest trending items as JSON.
    Used for live-refresh without a full page reload.
    """
    try:
        limit = min(max(1, limit), 100)
        items = await store.list_trending_items(limit)
    except Exception:
        items = []

    result = []
    for item in items:
        media_items = _trending_media_items(item)
        result.append({
            "id": item.get("id", ""),
            "bar": str(item.get("bar") or "Trending"),
            "title": item.get("title", ""),
            "description": item.get("description", ""),
            "normal_link": item.get("normal_link", "#"),
            "premium_link": item.get("premium_link", "#"),
            "media_count": len(media_items),
            "media_url": media_items[0]["url"] if media_items else None,
        })

    return JSONResponse({"items": result, "total": len(result)})


@app.get("/api/sections/search", response_class=JSONResponse)
async def api_sections_search(q: str = ""):
    """
    Server-side section name search.
    Returns matching public sections as JSON.
    """
    try:
        rows = await store.list_public_sections()
    except Exception:
        rows = []

    query = q.strip().lower()
    results = []
    for name, section_id in rows:
        if not section_id:
            continue
        display_name = name or section_id
        if not query or query in display_name.lower() or query in section_id.lower():
            results.append({
                "name": display_name,
                "id": section_id,
                "href": f"/section/{section_id}",
                "premium_href": f"/section/{section_id}/premium",
            })

    return JSONResponse({"results": results, "total": len(results), "query": q})


@app.get("/api/section/{section_id}/files", response_class=JSONResponse)
async def api_section_files(section_id: str, page: int = 1, sort: str = "default", limit: int = 20):
    """
    Returns a page of files for a given section as JSON.
    Used by infinite scroll in section.html.
    """
    try:
        limit = min(max(1, limit), 100)
        page = max(1, page)

        all_items = await store.list_section_files(section_id, sort=sort)
        total = len(all_items)
        start = (page - 1) * limit
        end = start + limit
        page_items = all_items[start:end]
        page_count = max(1, math.ceil(total / limit))

        def _make_file_entry(item):
            return {
                "name": getattr(item, "name", str(item.get("name", ""))),
                "size_text": getattr(item, "size_text", str(item.get("size_text", ""))),
                "mime": getattr(item, "mime", str(item.get("mime", ""))),
                "views_total": getattr(item, "views_total", item.get("views_total", 0)),
                "normal_link": getattr(item, "normal_link", item.get("normal_link", "#")),
                "premium_link": getattr(item, "premium_link", item.get("premium_link", "#")),
                "is_trending": getattr(item, "is_trending", item.get("is_trending", False)),
            }

        return JSONResponse({
            "files": [_make_file_entry(it) for it in page_items],
            "page": page,
            "page_count": page_count,
            "total": total,
            "has_next": page < page_count,
        })
    except Exception as exc:
        return JSONResponse({"error": str(exc), "files": [], "page": 1, "page_count": 1, "total": 0, "has_next": False}, status_code=200)



@app.get("/robots.txt", include_in_schema=False)
async def robots_txt():
    body = "\n".join(
        [
            "User-agent: *",
            "Allow: /",
            "Disallow: /stream/",
            "Disallow: /download/",
            "Disallow: /player/",
            f"Sitemap: {canonical_url('/sitemap.xml')}",
            "",
        ]
    )
    return Response(content=body, media_type="text/plain; charset=utf-8")


@app.get("/sitemap.xml", include_in_schema=False)
async def sitemap_xml():
    from datetime import date
    today = date.today().isoformat()
    urls = [
        xml_url(canonical_url("/"), "1.0", "daily", today),
        xml_url(canonical_url("/sections"), "0.8", "daily", today),
        xml_url(canonical_url("/trending"), "0.8", "daily", today),
    ]
    try:
        rows = await store.list_public_sections()
    except Exception:
        rows = []
    for _, section_id in rows:
        if section_id:
            urls.append(xml_url(canonical_url(f"/section/{section_id}"), "0.7", "weekly", today))

    body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls)
        + "\n</urlset>\n"
    )
    return Response(content=body, media_type="application/xml; charset=utf-8")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return RedirectResponse(url="/static/favicon.svg", status_code=307)


async def dark_archives_cleanup_loop() -> None:
    while True:
        try:
            if hasattr(store, "delete_expired_dark_posts"):
                await store.delete_expired_dark_posts()
        except Exception:
            pass
        await asyncio.sleep(60)


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
    asyncio.create_task(dark_archives_cleanup_loop())


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


# ── In-memory message cache: avoids repeated Telegram API calls per range request ──
# Key: (chat_id, message_id), Value: (message_object, cached_at_timestamp)
_msg_cache: dict = {}
_MSG_CACHE_TTL = 600  # seconds — 10 minutes

def _msg_cache_get(chat_id: int, message_id: int):
    key = (chat_id, message_id)
    entry = _msg_cache.get(key)
    if entry and (time.time() - entry[1]) < _MSG_CACHE_TTL:
        return entry[0]
    return None

def _msg_cache_set(chat_id: int, message_id: int, message):
    key = (chat_id, message_id)
    _msg_cache[key] = (message, time.time())
    # Evict old entries if cache grows large
    if len(_msg_cache) > 200:
        oldest = sorted(_msg_cache, key=lambda k: _msg_cache[k][1])[:50]
        for k in oldest:
            del _msg_cache[k]

async def telegram_stream(message, start: int, end: Optional[int]) -> AsyncGenerator[bytes, None]:
    tg_chunk_size = 1024 * 1024
    chunk_offset = start // tg_chunk_size
    chunk_limit = 0
    if end is not None:
        byte_len = end - start + 1
        chunk_limit = ((byte_len + tg_chunk_size - 1) // tg_chunk_size) + 1

    if supports_iter_download():
        async for chunk in client.iter_download(message, offset=start, length=None if end is None else end - start + 1):
            yield chunk
            await asyncio.sleep(0)
        return

    try:
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
            yield chunk
            await asyncio.sleep(0)
    except Exception as e:
        logger.error(f"[stream_media_error] Stream exception: {e}")
        return


async def fetch_message(chat_id: int, message_id: int):
    # Check cache first — avoids Telegram API call on every browser range request
    cached = _msg_cache_get(chat_id, message_id)
    if cached is not None:
        return cached
    try:
        msg = await client.get_messages(chat_id, message_id)
        if msg:
            _msg_cache_set(chat_id, message_id, msg)
        return msg
    except Exception:
        try:
            await client.get_chat(chat_id)
            msg = await client.get_messages(chat_id, message_id)
            if msg:
                _msg_cache_set(chat_id, message_id, msg)
            return msg
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
    return templates.TemplateResponse(request=request, name="password.html", context={"request": request, **ctx})


async def render_section(section_id: str, access_filter: str, request: Request) -> HTMLResponse:
    access_filter = (access_filter or "all").strip().lower()
    if password_enabled() and not is_authed(request):
        return templates.TemplateResponse(
            request=request,
            name="password.html",
            context={
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

    viewer_cookie = request.cookies.get("stream_viewer_id")
    viewer_id = viewer_cookie or secrets.token_hex(16)
    section_views_total, section_views_unique = await store.increment_section_view(section_id, viewer_id)

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

    refs_by_token = await store.get_many(tokens, settings.token_ttl_seconds)
    views_by_token = await store.get_views_many(list(refs_by_token))

    grouped: dict[str, dict] = {}
    for token in tokens:
        ref = refs_by_token.get(token)
        if ref is None:
            continue
        ref_access = (ref.access or "normal").strip().lower()
        group_key = f"{ref.chat_id}:{ref.message_id}"
        name = ref.file_name or ref.file_unique_id or "file"
        size_text = human_size(ref.file_size)
        views_total, views_unique = views_by_token.get(token, (0, 0))
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
            bot_link = f"https://telegram.me/{settings.bot_username}?start=dl_{token}"
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
                "section_views_total": section_views_total,
                "section_views_unique": section_views_unique,
                "items_len": len(items),
                "items_sample": items[:2],
            }
        )

    send_all_normal_link = "#"
    send_all_normal_class = "btn btn-secondary btn-disabled"
    send_all_premium_link = "#"
    send_all_premium_class = "btn btn-ghost btn-disabled"
    if settings.bot_username and total_items > 0:
        send_all_normal_link = f"https://telegram.me/{settings.bot_username}?start=sa_{section_id}_normal"
        send_all_premium_link = f"https://telegram.me/{settings.bot_username}?start=sa_{section_id}_premium"
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

    response = templates.TemplateResponse(
        request=request,
        name="section.html",
        context={
            "request": request,
            "section_id": section_id,
            "section_name": make_section_name(section_id),
            "base_url": settings.base_url,
            "canonical_url": canonical_url(f"/section/{section_id}"),
            "total_items": total_items,
            "section_views_total": section_views_total,
            "section_views_unique": section_views_unique,
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
    if not viewer_cookie:
        response.set_cookie("stream_viewer_id", viewer_id, httponly=True, max_age=60 * 60 * 24 * 30, samesite="lax")
    return response


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
    response = await render_section(section_id, "all", request)
    # Tell Google the canonical URL is the non-premium version to avoid duplicate content
    if hasattr(response, 'headers'):
        response.headers["Link"] = f'<{canonical_url(f"/section/{section_id}")}>; rel="canonical"'
    return response




@app.post("/section/{section_id}/auth")
async def section_auth(section_id: str, request: Request, password: str = Form(...)):
    access = request.query_params.get("access", "normal")
    if not password_enabled():
        return RedirectResponse(url=f"/section/{section_id}" if access != "premium" else f"/section/{section_id}/premium", status_code=302)
    if not hmac.compare_digest(password, settings.stream_password):
        return templates.TemplateResponse(
            request=request,
            name="password.html",
            context={
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
        return templates.TemplateResponse(request=request, name="premium.html", context={"request": request}, status_code=403)

    if password_enabled() and not is_authed(request):
        return templates.TemplateResponse(
            request=request,
            name="password.html",
            context={
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
    if settings.bot_username:
        dl_target = getattr(ref, "dl_token", None) or token
        download_button_url = f"{settings.base_url}/player/{dl_target}/download"

    response = templates.TemplateResponse(
        request=request,
        name="player.html",
        context={
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
    if settings.bot_username:
        return RedirectResponse(url=f"https://telegram.me/{settings.bot_username}?start=dl_{token}", status_code=302)
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
            request=request,
            name="password.html",
            context={
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


# ---------------------------------------------------------------------------
#  HLS Proxy & Player
# ---------------------------------------------------------------------------

@app.get("/proxy")
async def hls_proxy(url: str, request: Request):
    if not url:
        raise HTTPException(status_code=400, detail="Error: No URL provided.")
    
    url_lower = url.lower()
    is_terabox = any(domain in url_lower for domain in ["terabox", "1024tera", "terasharefile", "nephobox", "pcs.baidu.com", "baidupcs.com"])

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }

    if is_terabox:
        ndus = os.getenv("NDUS_COOKIE", "Yzdw9XNpeHui_mvplw5zEnlklVr5_nGZ9VutkCij")
        headers["Cookie"] = f"ndus={ndus}"
    else:
        headers["Referer"] = "https://faphouse.com/"
        
    # Forward the Range header from client to support seeking/seeking in HTML5 player
    client_range = request.headers.get("Range")
    if client_range:
        headers["Range"] = client_range
    
    try:
        session = aiohttp.ClientSession()
        resp = await session.get(url, headers=headers)
        
        if resp.status not in (200, 206):
            await resp.release()
            await session.close()
            raise HTTPException(status_code=resp.status, detail="Failed to fetch stream.")
            
        content_type = resp.headers.get("Content-Type", "application/octet-stream")
        
        if ".m3u8" in url or "mpegurl" in content_type.lower():
            text = await resp.text()
            await resp.release()
            await session.close()
            
            new_lines = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                if line.startswith("#"):
                    new_lines.append(line)
                else:
                    absolute_url = urljoin(url, line)
                    new_lines.append(f"/proxy?url={quote(absolute_url, safe='')}")
            
            return Response(
                content="\n".join(new_lines).encode("utf-8"),
                media_type=content_type,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "*"
                }
            )
        else:
            async def stream_generator():
                try:
                    async for chunk in resp.content.iter_chunked(65536):
                        yield chunk
                finally:
                    await resp.release()
                    await session.close()
            
            resp_headers = {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Accept-Ranges": "bytes"
            }
            if "Content-Range" in resp.headers:
                resp_headers["Content-Range"] = resp.headers["Content-Range"]
            if "Content-Length" in resp.headers:
                resp_headers["Content-Length"] = resp.headers["Content-Length"]
                
            return StreamingResponse(
                stream_generator(),
                status_code=resp.status,
                media_type=content_type,
                headers=resp_headers
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Proxy error: {str(e)}")


@app.get("/c29af66ada34d56008c851e7f69be3a7.html")
async def exoclick_verification():
    return HTMLResponse(content="c29af66ada34d56008c851e7f69be3a7")

@app.get("/hlsplayer", response_class=HTMLResponse)
async def hls_player_page(request: Request, url: str):
    if not url:
        return HTMLResponse("Error: No stream URL provided!", status_code=400)
        
    url_lower = url.lower()
    is_terabox = any(domain in url_lower for domain in ["terabox.com", "teraboxapp.com", "1024tera.com", "terasharefile.com", "nephobox.com"])
    
    stream_url = url
    if is_terabox:
        import re
        surl_match = re.search(r'/s/([A-Za-z0-9_-]+)', url)
        if surl_match:
            surl = surl_match.group(1)
            from app.terabox_helper import get_terabox_info
            info = await asyncio.to_thread(get_terabox_info, surl)
            if info and info.get("dlink"):
                stream_url = info["dlink"]
            else:
                return HTMLResponse("❌ **Error:** Failed to extract TeraBox stream link. Please make sure cookies are valid.", status_code=500)
        else:
            return HTMLResponse("❌ **Error:** Invalid TeraBox link format.", status_code=400)
            
    return templates.TemplateResponse(
        request=request,
        name="hls_player.html",
        context={
            "request": request,
            "stream_url": stream_url,
        }
    )
