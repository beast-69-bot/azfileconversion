import re
import os
from curl_cffi import requests as curl_requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36"
    )
}

def get_terabox_info(surl: str) -> dict | None:
    """
    Fetches file metadata from TeraBox using the public share URL ID.
    Uses the working two-step API flow (getting jsToken first, then listing).
    """
    short = surl[1:] if surl.startswith("1") else surl
    ndus_cookie = os.getenv("NDUS_COOKIE", "Yzdw9XNpeHui_mvplw5zEnlklVr5_nGZ9VutkCij")
    session = curl_requests.Session(impersonate="chrome110")
    session.cookies.update({"ndus": ndus_cookie})

    # Step 1: Request sharing/link page to extract jsToken
    first_url = f"https://dm.terabox.app/sharing/link?surl={short}"
    try:
        response = session.get(first_url, headers=HEADERS, timeout=15)
        match = re.search(r'fn%28%22(.*?)%22%29', response.text)
        if not match:
            return None
        jsToken = match.group(1)
    except Exception:
        return None

    # Step 2: Query share/list API using the token
    api_url = "https://dm.terabox.app/share/list"
    params = {
        "app_id": "250528",
        "jsToken": jsToken,
        "site_referer": "https://www.terabox.app/",
        "shorturl": short,
        "root": "1"
    }

    api_headers = {
        "Host": "dm.terabox.app",
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"https://dm.terabox.app/sharing/link?surl={short}&clearCache=1",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://dm.terabox.app"
    }

    try:
        api_response = session.get(api_url, params=params, headers=api_headers, timeout=15)
        data = api_response.json()
        if data.get("errno") != 0 or not data.get("list"):
            return None
        item = data["list"][0]
        return {
            "filename": item.get("server_filename", "video.mp4"),
            "size": int(item.get("size", 0)),
            "dlink": item.get("dlink"),
        }
    except Exception:
        return None
