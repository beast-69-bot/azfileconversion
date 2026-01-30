import json
import time
from dataclasses import asdict, dataclass
from typing import Optional

try:
    import redis.asyncio as redis
except Exception:
    redis = None


@dataclass
class FileRef:
    chat_id: int
    message_id: int
    file_unique_id: str
    file_name: Optional[str]
    mime_type: Optional[str]
    file_size: Optional[int]
    media_type: str
    created_at: float


class TokenStore:
    def __init__(self, redis_url: Optional[str] = None) -> None:
        self._redis_url = redis_url
        self._redis = None
        self._memory: dict[str, FileRef] = {}

    async def connect(self) -> None:
        if self._redis_url and redis is not None:
            ssl_cert_reqs = None
            if self._redis_url.startswith("rediss://"):
                ssl_cert_reqs = "none"
            self._redis = redis.from_url(
                self._redis_url,
                decode_responses=True,
                ssl_cert_reqs=ssl_cert_reqs,
            )

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.close()

    async def set(self, token: str, ref: FileRef, ttl_seconds: int) -> None:
        if self._redis is not None:
            await self._redis.setex(token, ttl_seconds, json.dumps(asdict(ref)))
            return
        self._memory[token] = ref

    async def get(self, token: str, ttl_seconds: int) -> Optional[FileRef]:
        if self._redis is not None:
            raw = await self._redis.get(token)
            if not raw:
                return None
            data = json.loads(raw)
            return FileRef(**data)
        ref = self._memory.get(token)
        if not ref:
            return None
        if time.time() - ref.created_at > ttl_seconds:
            self._memory.pop(token, None)
            return None
        return ref