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
    file_id: str
    chat_id: int
    message_id: int
    file_unique_id: str
    file_name: Optional[str]
    mime_type: Optional[str]
    file_size: Optional[int]
    media_type: str
    access: str
    created_at: float


class TokenStore:
    def __init__(self, redis_url: Optional[str] = None, history_limit: int = 200) -> None:
        self._redis_url = redis_url
        self._redis = None
        self._memory: dict[str, FileRef] = {}
        self._history: list[str] = []
        self._history_limit = max(history_limit, 1)
        self._history_key = "history:tokens"

    async def connect(self) -> None:
        if self._redis_url and redis is not None:
            kwargs = {"decode_responses": True}
            if self._redis_url.startswith("rediss://"):
                kwargs["ssl_cert_reqs"] = "none"
            try:
                self._redis = redis.from_url(self._redis_url, **kwargs)
            except TypeError:
                # Older redis clients may not support ssl_cert_reqs
                kwargs.pop("ssl_cert_reqs", None)
                self._redis = redis.from_url(self._redis_url, **kwargs)

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.close()

    async def set(self, token: str, ref: FileRef, ttl_seconds: int) -> None:
        if self._redis is not None:
            payload = json.dumps(asdict(ref))
            if ttl_seconds and ttl_seconds > 0:
                await self._redis.setex(token, ttl_seconds, payload)
            else:
                await self._redis.set(token, payload)
            await self._redis.lpush(self._history_key, token)
            await self._redis.ltrim(self._history_key, 0, self._history_limit - 1)
            return
        self._memory[token] = ref
        self._history.insert(0, token)
        if len(self._history) > self._history_limit:
            self._history = self._history[: self._history_limit]

    async def get(self, token: str, ttl_seconds: int) -> Optional[FileRef]:
        if self._redis is not None:
            raw = await self._redis.get(token)
            if not raw:
                return None
            data = json.loads(raw)
            if "file_id" not in data:
                data["file_id"] = ""
            if "access" not in data:
                data["access"] = "normal"
            return FileRef(**data)
        ref = self._memory.get(token)
        if not ref:
            return None
        if ttl_seconds > 0 and time.time() - ref.created_at > ttl_seconds:
            self._memory.pop(token, None)
            return None
        return ref


    async def list_recent(self, limit: int) -> list[str]:
        limit = max(int(limit), 1)
        if self._redis is not None:
            tokens = await self._redis.lrange(self._history_key, 0, limit - 1)
            return [t for t in tokens if t]
        return self._history[:limit]
