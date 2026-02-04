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
    section_id: Optional[str] = None
    section_name: Optional[str] = None


def _normalize_section(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _slugify(value: str) -> str:
    cleaned = []
    last_dash = False
    for ch in value.strip().lower():
        if ch.isalnum():
            cleaned.append(ch)
            last_dash = False
        else:
            if not last_dash:
                cleaned.append("-")
                last_dash = True
    slug = "".join(cleaned).strip("-")
    return slug or "section"


class TokenStore:
    def __init__(self, redis_url: Optional[str] = None, history_limit: int = 200) -> None:
        self._redis_url = redis_url
        self._redis = None
        self._memory: dict[str, FileRef] = {}
        self._history: list[str] = []
        self._sections: dict[str, list[str]] = {}
        self._section_registry: dict[str, str] = {}
        self._section_registry_id: dict[str, str] = {}
        self._view_counts: dict[str, int] = {}
        self._unique_viewers: dict[str, set[str]] = {}
        self._like_counts: dict[str, int] = {}
        self._like_viewers: dict[str, set[str]] = {}
        self._react_likes: dict[str, set[int]] = {}
        self._react_dislikes: dict[str, set[int]] = {}
        self._credits: dict[int, int] = {}
        self._history_limit = max(history_limit, 1)
        self._history_key = "history:tokens"
        self._section_key = "section:current"
        self._section_name_key = "section:current:name"
        self._section_name_map = "section:registry:name"
        self._section_id_map = "section:registry:id"
        self._current_section: Optional[str] = None
        self._current_section_name: Optional[str] = None

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
            if ref.section_id:
                section_key = f"section:{ref.section_id}"
                await self._redis.lpush(section_key, token)
                await self._redis.ltrim(section_key, 0, self._history_limit - 1)
            return
        self._memory[token] = ref
        self._history.insert(0, token)
        if len(self._history) > self._history_limit:
            self._history = self._history[: self._history_limit]
        if ref.section_id:
            items = self._sections.setdefault(ref.section_id, [])
            items.insert(0, token)
            if len(items) > self._history_limit:
                self._sections[ref.section_id] = items[: self._history_limit]

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
            if "section_id" not in data:
                data["section_id"] = None
            if "section_name" not in data:
                data["section_name"] = None
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

    async def increment_view(self, token: str, viewer_id: Optional[str], ttl_seconds: int) -> tuple[int, int]:
        if self._redis is not None:
            count_key = f"views:count:{token}"
            unique_key = f"views:unique:{token}"
            total = await self._redis.incr(count_key)
            if ttl_seconds and ttl_seconds > 0:
                await self._redis.expire(count_key, ttl_seconds)
            unique = 0
            if viewer_id:
                await self._redis.sadd(unique_key, viewer_id)
                if ttl_seconds and ttl_seconds > 0:
                    await self._redis.expire(unique_key, ttl_seconds)
                unique = await self._redis.scard(unique_key)
            return int(total), int(unique)

        total = self._view_counts.get(token, 0) + 1
        self._view_counts[token] = total
        unique = 0
        if viewer_id:
            viewers = self._unique_viewers.setdefault(token, set())
            viewers.add(viewer_id)
            unique = len(viewers)
        return total, unique

    async def get_views(self, token: str) -> tuple[int, int]:
        if self._redis is not None:
            count_key = f"views:count:{token}"
            unique_key = f"views:unique:{token}"
            total_raw = await self._redis.get(count_key)
            total = int(total_raw or 0)
            unique = await self._redis.scard(unique_key)
            return total, int(unique)

        total = self._view_counts.get(token, 0)
        unique = len(self._unique_viewers.get(token, set()))
        return total, unique

    async def set_like(self, token: str, viewer_id: str, liked: bool) -> tuple[int, bool]:
        if self._redis is not None:
            count_key = f"likes:count:{token}"
            set_key = f"likes:set:{token}"
            if liked:
                added = await self._redis.sadd(set_key, viewer_id)
                if added:
                    await self._redis.incr(count_key)
            else:
                removed = await self._redis.srem(set_key, viewer_id)
                if removed:
                    await self._redis.decr(count_key)
            total_raw = await self._redis.get(count_key)
            total = int(total_raw or 0)
            user_liked = await self._redis.sismember(set_key, viewer_id)
            if total < 0:
                total = 0
                await self._redis.set(count_key, 0)
            return total, bool(user_liked)

        viewers = self._like_viewers.setdefault(token, set())
        if liked:
            viewers.add(viewer_id)
        else:
            viewers.discard(viewer_id)
        total = len(viewers)
        self._like_counts[token] = total
        return total, viewer_id in viewers

    async def get_likes(self, token: str, viewer_id: Optional[str] = None) -> tuple[int, bool]:
        if self._redis is not None:
            count_key = f"likes:count:{token}"
            set_key = f"likes:set:{token}"
            total_raw = await self._redis.get(count_key)
            total = int(total_raw or 0)
            user_liked = False
            if viewer_id:
                user_liked = bool(await self._redis.sismember(set_key, viewer_id))
            return total, user_liked

        total = self._like_counts.get(token, len(self._like_viewers.get(token, set())))
        user_liked = False
        if viewer_id:
            user_liked = viewer_id in self._like_viewers.get(token, set())
        return total, user_liked

    async def get_reactions(self, token: str, user_id: Optional[int] = None) -> tuple[int, int, int]:
        if self._redis is not None:
            like_key = f"react:like:{token}"
            dislike_key = f"react:dislike:{token}"
            likes = await self._redis.scard(like_key)
            dislikes = await self._redis.scard(dislike_key)
            status = 0
            if user_id is not None:
                if await self._redis.sismember(like_key, str(user_id)):
                    status = 1
                elif await self._redis.sismember(dislike_key, str(user_id)):
                    status = -1
            return int(likes), int(dislikes), status

        likes_set = self._react_likes.get(token, set())
        dislikes_set = self._react_dislikes.get(token, set())
        status = 0
        if user_id is not None:
            if user_id in likes_set:
                status = 1
            elif user_id in dislikes_set:
                status = -1
        return len(likes_set), len(dislikes_set), status

    async def set_reaction(self, token: str, user_id: int, reaction: int) -> tuple[int, int, int]:
        # reaction: 1=like, -1=dislike, 0=remove
        if self._redis is not None:
            like_key = f"react:like:{token}"
            dislike_key = f"react:dislike:{token}"
            user = str(user_id)
            if reaction == 1:
                await self._redis.sadd(like_key, user)
                await self._redis.srem(dislike_key, user)
            elif reaction == -1:
                await self._redis.sadd(dislike_key, user)
                await self._redis.srem(like_key, user)
            else:
                await self._redis.srem(like_key, user)
                await self._redis.srem(dislike_key, user)
            likes = await self._redis.scard(like_key)
            dislikes = await self._redis.scard(dislike_key)
            status = 0
            if await self._redis.sismember(like_key, user):
                status = 1
            elif await self._redis.sismember(dislike_key, user):
                status = -1
            return int(likes), int(dislikes), status

        likes_set = self._react_likes.setdefault(token, set())
        dislikes_set = self._react_dislikes.setdefault(token, set())
        if reaction == 1:
            likes_set.add(user_id)
            dislikes_set.discard(user_id)
        elif reaction == -1:
            dislikes_set.add(user_id)
            likes_set.discard(user_id)
        else:
            likes_set.discard(user_id)
            dislikes_set.discard(user_id)
        status = 0
        if user_id in likes_set:
            status = 1
        elif user_id in dislikes_set:
            status = -1
        return len(likes_set), len(dislikes_set), status

    async def get_credits(self, user_id: int) -> int:
        if self._redis is not None:
            raw = await self._redis.get(f"credits:{user_id}")
            return int(raw or 0)
        return int(self._credits.get(user_id, 0))

    async def add_credits(self, user_id: int, amount: int) -> int:
        amount = int(amount)
        if self._redis is not None:
            return int(await self._redis.incrby(f"credits:{user_id}", amount))
        current = int(self._credits.get(user_id, 0)) + amount
        self._credits[user_id] = current
        return current

    async def charge_credits(self, user_id: int, amount: int) -> tuple[bool, int]:
        amount = int(amount)
        if amount <= 0:
            return True, await self.get_credits(user_id)
        if self._redis is not None:
            script = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])
local current = tonumber(redis.call('get', key) or '0')
if current < amount then
  return {-1, current}
end
local newval = redis.call('incrby', key, -amount)
return {1, newval}
"""
            ok, balance = await self._redis.eval(script, 1, f"credits:{user_id}", str(amount))
            return ok == 1, int(balance)
        current = int(self._credits.get(user_id, 0))
        if current < amount:
            return False, current
        current -= amount
        self._credits[user_id] = current
        return True, current


    async def set_section(self, section_name: Optional[str]) -> Optional[str]:
        if not section_name:
            if self._redis is not None:
                await self._redis.delete(self._section_key)
                await self._redis.delete(self._section_name_key)
            self._current_section = None
            self._current_section_name = None
            return None

        normalized = _normalize_section(section_name)
        section_id = _slugify(section_name)
        if await self.section_exists(section_name):
            return None
        if await self.section_id_exists(section_id):
            return None

        if self._redis is not None:
            await self._redis.hset(self._section_name_map, normalized, section_id)
            await self._redis.hset(self._section_id_map, section_id, section_name)
            await self._redis.set(self._section_key, section_id)
            await self._redis.set(self._section_name_key, section_name)
            return section_id

        self._section_registry[normalized] = section_id
        self._section_registry_id[section_id] = section_name
        self._current_section = section_id
        self._current_section_name = section_name
        return section_id

    async def get_section(self) -> tuple[Optional[str], Optional[str]]:
        if self._redis is not None:
            section_id = await self._redis.get(self._section_key)
            section_name = await self._redis.get(self._section_name_key)
            return (section_id or None, section_name or None)
        return (self._current_section, self._current_section_name)

    async def section_exists(self, section_name: str) -> bool:
        normalized = _normalize_section(section_name)
        if self._redis is not None:
            val = await self._redis.hget(self._section_name_map, normalized)
            return val is not None
        return normalized in self._section_registry

    async def section_id_exists(self, section_id: str) -> bool:
        if self._redis is not None:
            val = await self._redis.hget(self._section_id_map, section_id)
            return val is not None
        return section_id in self._section_registry_id

    async def list_sections(self) -> list[tuple[str, str]]:
        if self._redis is not None:
            data = await self._redis.hgetall(self._section_id_map)
            return [(name, section_id) for section_id, name in data.items()]
        return [(name, section_id) for section_id, name in self._section_registry_id.items()]

    async def delete_section(self, section_name: str) -> bool:
        normalized = _normalize_section(section_name)
        if self._redis is not None:
            section_id = await self._redis.hget(self._section_name_map, normalized)
            if not section_id:
                return False
            await self._redis.hdel(self._section_name_map, normalized)
            await self._redis.hdel(self._section_id_map, section_id)
            await self._redis.delete(f"section:{section_id}")
            current = await self._redis.get(self._section_key)
            if current and current == section_id:
                await self._redis.delete(self._section_key)
                await self._redis.delete(self._section_name_key)
            return True

        section_id = self._section_registry.get(normalized)
        if not section_id:
            return False
        self._section_registry.pop(normalized, None)
        self._section_registry_id.pop(section_id, None)
        self._sections.pop(section_id, None)
        if self._current_section == section_id:
            self._current_section = None
            self._current_section_name = None
        return True


    async def list_section(self, section_id: str, limit: int) -> list[str]:
        limit = max(int(limit), 1)
        if self._redis is not None:
            tokens = await self._redis.lrange(f"section:{section_id}", 0, limit - 1)
            return [t for t in tokens if t]
        return self._sections.get(section_id, [])[:limit]
