import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    api_id: int
    api_hash: str
    bot_token: str
    base_url: str
    redis_url: str | None
    token_ttl_seconds: int
    chunk_size: int
    admin_ids: set[int]
    db_path: str
    public_stream: bool
    bot_username: str
    direct_download: bool


def _parse_admin_ids(value: str) -> set[int]:
    ids: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            continue
    return ids


def get_settings() -> Settings:
    api_id = int(os.getenv("API_ID", "0"))
    api_hash = os.getenv("API_HASH", "")
    bot_token = os.getenv("BOT_TOKEN", "")
    base_url = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")
    redis_url = os.getenv("REDIS_URL")
    token_ttl_seconds = int(os.getenv("TOKEN_TTL_SECONDS", "0"))
    chunk_size = int(os.getenv("CHUNK_SIZE", "262144"))
    admin_ids = _parse_admin_ids(os.getenv("ADMIN_IDS", ""))
    db_path = os.getenv("DB_PATH", "data/premium.db")
    public_stream = os.getenv("PUBLIC_STREAM", "true").lower() in {"1", "true", "yes", "y"}
    bot_username = os.getenv("BOT_USERNAME", "").lstrip("@")
    direct_download = os.getenv("DIRECT_DOWNLOAD", "false").lower() in {"1", "true", "yes", "y"}

    if not api_id or not api_hash or not bot_token:
        raise SystemExit("Missing API_ID, API_HASH, or BOT_TOKEN in environment.")

    if chunk_size < 262144 or chunk_size > 524288:
        raise SystemExit("CHUNK_SIZE must be between 262144 and 524288 bytes.")

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        bot_token=bot_token,
        base_url=base_url,
        redis_url=redis_url,
        token_ttl_seconds=token_ttl_seconds,
        chunk_size=chunk_size,
        admin_ids=admin_ids,
        db_path=db_path,
        public_stream=public_stream,
        bot_username=bot_username,
        direct_download=direct_download,
    )
