import os
from dataclasses import dataclass

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


def get_settings() -> Settings:
    api_id = int(os.getenv("API_ID", "0"))
    api_hash = os.getenv("API_HASH", "")
    bot_token = os.getenv("BOT_TOKEN", "")
    base_url = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")
    redis_url = os.getenv("REDIS_URL")
    token_ttl_seconds = int(os.getenv("TOKEN_TTL_SECONDS", "86400"))
    chunk_size = int(os.getenv("CHUNK_SIZE", "524288"))

    if not api_id or not api_hash or not bot_token:
        raise SystemExit("Missing API_ID, API_HASH, or BOT_TOKEN in environment.")

    if chunk_size < 262144 or chunk_size > 1048576:
        raise SystemExit("CHUNK_SIZE must be between 262144 and 1048576 bytes.")

    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        bot_token=bot_token,
        base_url=base_url,
        redis_url=redis_url,
        token_ttl_seconds=token_ttl_seconds,
        chunk_size=chunk_size,
    )