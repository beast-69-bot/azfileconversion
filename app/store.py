import json
import secrets
import time
from dataclasses import asdict, dataclass
from typing import Optional

try:
    import redis.asyncio as redis
except Exception:
    redis = None

try:
    from redis.exceptions import WatchError
except Exception:
    WatchError = Exception


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
        self._public_sections: dict[str, str] = {}
        self._trending_items: dict[str, dict] = {}
        self._trending_index: list[str] = []
        self._site_visit_count: int = 0
        self._site_visitors: set[str] = set()
        self._section_view_counts: dict[str, int] = {}
        self._section_unique_viewers: dict[str, set[str]] = {}
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
        self._home_section_key = "section:home"
        self._home_section_name_key = "section:home:name"
        self._section_name_map = "section:registry:name"
        self._section_id_map = "section:registry:id"
        self._public_section_map = "section:public"
        self._site_visit_key = "site:visits:total"
        self._site_visitors_key = "site:visitors:unique"
        self._trending_index_key = "trending:index"
        self._trending_item_prefix = "trending:item:"
        self._pay_plan_key = "plan:pay"
        self._pay_req_prefix = "pay:req:"
        self._pay_req_index = "pay:req:index"
        self._pay_req_seq_key = "pay:req:seq"
        self._pay_req_msg_prefix = "pay:reqmsg:"
        self._pay_req_msgs_prefix = "pay:reqmsgs:"
        self._pay_pending_utr_prefix = "pay:pending_utr:"
        self._action_lock_prefix = "act:lock:"
        self._current_section: Optional[str] = None
        self._current_section_name: Optional[str] = None
        self._home_section: Optional[str] = None
        self._home_section_name: Optional[str] = None
        self._pay_price: Optional[float] = None
        self._pay_text: Optional[str] = None
        self._upi_id: Optional[str] = None
        self._payment_settings: dict[str, object] = {
            "payment_gateway": "manual",
            "xwallet_api_key": "",
            "tutorial_chat_id": 0,
            "tutorial_message_id": 0,
            "total_earnings": 0.0,
        }
        self._pay_pending_utr: dict[int, str] = {}
        self._pay_req_messages: dict[str, tuple[int, int]] = {}
        self._pay_req_message_set: dict[str, set[tuple[int, int]]] = {}
        self._pay_req_seq: int = 0
        self._pay_requests: dict[str, dict] = {}
        self._action_locks: dict[str, float] = {}

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

    async def get_many(self, tokens: list[str], ttl_seconds: int) -> dict[str, FileRef]:
        ordered = [str(token) for token in tokens if token]
        if not ordered:
            return {}
        if self._redis is not None:
            pipe = self._redis.pipeline()
            for token in ordered:
                pipe.get(token)
            raw_values = await pipe.execute()
            results: dict[str, FileRef] = {}
            for token, raw in zip(ordered, raw_values):
                if not raw:
                    continue
                data = json.loads(raw)
                if "file_id" not in data:
                    data["file_id"] = ""
                if "access" not in data:
                    data["access"] = "normal"
                if "section_id" not in data:
                    data["section_id"] = None
                if "section_name" not in data:
                    data["section_name"] = None
                results[token] = FileRef(**data)
            return results

        results: dict[str, FileRef] = {}
        for token in ordered:
            ref = await self.get(token, ttl_seconds)
            if ref is not None:
                results[token] = ref
        return results


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

    async def get_views_many(self, tokens: list[str]) -> dict[str, tuple[int, int]]:
        ordered = [str(token) for token in tokens if token]
        if not ordered:
            return {}
        if self._redis is not None:
            pipe = self._redis.pipeline()
            for token in ordered:
                pipe.get(f"views:count:{token}")
                pipe.scard(f"views:unique:{token}")
            values = await pipe.execute()
            results: dict[str, tuple[int, int]] = {}
            for index, token in enumerate(ordered):
                total_raw = values[index * 2]
                unique_raw = values[index * 2 + 1]
                results[token] = (int(total_raw or 0), int(unique_raw or 0))
            return results

        return {
            token: (
                self._view_counts.get(token, 0),
                len(self._unique_viewers.get(token, set())),
            )
            for token in ordered
        }

    async def increment_site_visit(self) -> int:
        if self._redis is not None:
            return int(await self._redis.incr(self._site_visit_key))

        self._site_visit_count += 1
        return self._site_visit_count

    async def get_site_visits(self) -> int:
        if self._redis is not None:
            raw = await self._redis.get(self._site_visit_key)
            return int(raw or 0)

        return int(self._site_visit_count)

    async def register_site_visit(self, visitor_id: str) -> int:
        visitor_id = str(visitor_id or "").strip()
        if not visitor_id:
            return await self.get_site_visits()

        if self._redis is not None:
            await self._redis.sadd(self._site_visitors_key, visitor_id)
            return int(await self._redis.scard(self._site_visitors_key))

        self._site_visitors.add(visitor_id)
        return len(self._site_visitors)

    async def increment_section_view(self, section_id: str, viewer_id: Optional[str]) -> tuple[int, int]:
        if self._redis is not None:
            count_key = f"section:views:count:{section_id}"
            unique_key = f"section:views:unique:{section_id}"
            total = await self._redis.incr(count_key)
            unique = 0
            if viewer_id:
                await self._redis.sadd(unique_key, viewer_id)
                unique = await self._redis.scard(unique_key)
            return int(total), int(unique)

        total = self._section_view_counts.get(section_id, 0) + 1
        self._section_view_counts[section_id] = total
        unique = 0
        if viewer_id:
            viewers = self._section_unique_viewers.setdefault(section_id, set())
            viewers.add(viewer_id)
            unique = len(viewers)
        return total, unique

    async def get_section_views(self, section_id: str) -> tuple[int, int]:
        if self._redis is not None:
            count_key = f"section:views:count:{section_id}"
            unique_key = f"section:views:unique:{section_id}"
            total_raw = await self._redis.get(count_key)
            total = int(total_raw or 0)
            unique = await self._redis.scard(unique_key)
            return total, int(unique)

        total = self._section_view_counts.get(section_id, 0)
        unique = len(self._section_unique_viewers.get(section_id, set()))
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

    async def list_credit_balances(self, limit: int = 20) -> list[tuple[int, int]]:
        limit = max(1, int(limit))
        items: list[tuple[int, int]] = []
        if self._redis is not None:
            async for key in self._redis.scan_iter(match="credits:*", count=500):
                try:
                    user_id = int(str(key).split(":", 1)[1])
                except Exception:
                    continue
                raw = await self._redis.get(key)
                try:
                    balance = int(raw or 0)
                except Exception:
                    balance = 0
                items.append((user_id, balance))
        else:
            items.extend((int(uid), int(balance)) for uid, balance in self._credits.items())

        items.sort(key=lambda pair: pair[1], reverse=True)
        return items[:limit]

    async def get_pay_plan(self, default_price: float, default_text: str) -> tuple[float, str]:
        if self._redis is not None:
            data = await self._redis.hgetall(self._pay_plan_key)
            price_raw = data.get("price")
            text_raw = data.get("text")
            price = float(price_raw) if price_raw else float(default_price)
            text = text_raw or default_text
            return price, text
        price = self._pay_price if self._pay_price is not None else float(default_price)
        text = self._pay_text if self._pay_text else default_text
        return price, text

    async def set_pay_plan(self, price: float, text: str) -> tuple[float, str]:
        price = float(price)
        text = str(text).strip()
        if self._redis is not None:
            await self._redis.hset(self._pay_plan_key, mapping={"price": f"{price:.4f}", "text": text})
            return price, text
        self._pay_price = price
        self._pay_text = text
        return price, text

    async def get_payment_settings(self) -> dict:
        defaults = {
            "payment_gateway": "manual",
            "xwallet_api_key": "",
            "tutorial_chat_id": 0,
            "tutorial_message_id": 0,
            "total_earnings": 0.0,
        }
        if self._redis is not None:
            data = await self._redis.hgetall(self._pay_plan_key)
            return {
                "payment_gateway": str(data.get("payment_gateway", defaults["payment_gateway"]) or defaults["payment_gateway"]).strip().lower(),
                "xwallet_api_key": str(data.get("xwallet_api_key", defaults["xwallet_api_key"]) or ""),
                "tutorial_chat_id": int(data.get("tutorial_chat_id", "0") or 0),
                "tutorial_message_id": int(data.get("tutorial_message_id", "0") or 0),
                "total_earnings": float(data.get("total_earnings", "0") or 0.0),
            }
        settings = dict(defaults)
        settings.update(self._payment_settings)
        settings["payment_gateway"] = str(settings.get("payment_gateway", "manual") or "manual").strip().lower()
        settings["xwallet_api_key"] = str(settings.get("xwallet_api_key", "") or "")
        settings["tutorial_chat_id"] = int(settings.get("tutorial_chat_id", 0) or 0)
        settings["tutorial_message_id"] = int(settings.get("tutorial_message_id", 0) or 0)
        settings["total_earnings"] = float(settings.get("total_earnings", 0.0) or 0.0)
        return settings

    async def update_payment_settings(self, updates: dict) -> dict:
        clean: dict[str, str] = {}
        for key, value in (updates or {}).items():
            if key == "payment_gateway":
                clean[key] = str(value or "manual").strip().lower()
            elif key in {"xwallet_api_key"}:
                clean[key] = str(value or "").strip()
            elif key in {"tutorial_chat_id", "tutorial_message_id"}:
                clean[key] = str(int(value or 0))
            elif key == "total_earnings":
                clean[key] = f"{float(value or 0.0):.2f}"
        if self._redis is not None:
            if clean:
                await self._redis.hset(self._pay_plan_key, mapping=clean)
            return await self.get_payment_settings()
        for key, value in clean.items():
            if key in {"tutorial_chat_id", "tutorial_message_id"}:
                self._payment_settings[key] = int(value or 0)
            elif key == "total_earnings":
                self._payment_settings[key] = float(value or 0.0)
            else:
                self._payment_settings[key] = value
        return await self.get_payment_settings()

    async def add_total_earnings(self, amount: float) -> float:
        amount = float(amount or 0.0)
        if self._redis is not None:
            current = await self.get_payment_settings()
            total = float(current.get("total_earnings", 0.0) or 0.0) + amount
            await self._redis.hset(self._pay_plan_key, mapping={"total_earnings": f"{total:.2f}"})
            return total
        current = float(self._payment_settings.get("total_earnings", 0.0) or 0.0) + amount
        self._payment_settings["total_earnings"] = current
        return current


    async def get_upi_id(self) -> str:
        if self._redis is not None:
            value = await self._redis.hget(self._pay_plan_key, "upi_id")
            return (value or "").strip()
        return (self._upi_id or "").strip()

    async def set_upi_id(self, upi_id: str) -> str:
        clean = str(upi_id or "").strip()
        if self._redis is not None:
            if clean:
                await self._redis.hset(self._pay_plan_key, mapping={"upi_id": clean})
            else:
                await self._redis.hdel(self._pay_plan_key, "upi_id")
            return clean
        self._upi_id = clean or None
        return clean

    async def get_auto_delete(self, default: int = 0) -> int:
        """Return auto-delete seconds (0 = disabled)."""
        if self._redis is not None:
            raw = await self._redis.hget(self._pay_plan_key, "auto_delete_seconds")
            try:
                return max(0, int(raw or 0))
            except Exception:
                return default
        return getattr(self, "_auto_delete_seconds", default)

    async def set_auto_delete(self, seconds: int) -> int:
        """Set auto-delete seconds (0 = disable)."""
        seconds = max(0, int(seconds))
        if self._redis is not None:
            await self._redis.hset(self._pay_plan_key, mapping={"auto_delete_seconds": str(seconds)})
            return seconds
        self._auto_delete_seconds = seconds
        return seconds

    # ------------------------------------------------------------------
    #  Thumbnail
    # ------------------------------------------------------------------

    async def get_thumbnail(self) -> str:
        """Return stored thumbnail file_id or empty string."""
        if self._redis is not None:
            return (await self._redis.hget(self._pay_plan_key, "thumb_file_id") or "").strip()
        return (getattr(self, "_thumb_file_id", None) or "").strip()

    async def set_thumbnail(self, file_id: str) -> str:
        fid = str(file_id or "").strip()
        if self._redis is not None:
            if fid:
                await self._redis.hset(self._pay_plan_key, mapping={"thumb_file_id": fid})
            else:
                await self._redis.hdel(self._pay_plan_key, "thumb_file_id")
            return fid
        self._thumb_file_id = fid or None
        return fid

    async def del_thumbnail(self) -> None:
        await self.set_thumbnail("")

    async def get_thumbnail_enabled(self) -> bool:
        if self._redis is not None:
            raw = await self._redis.hget(self._pay_plan_key, "thumb_enabled")
            return (raw or "1") not in {"0", "false", "off"}
        return bool(getattr(self, "_thumb_enabled", True))

    async def set_thumbnail_enabled(self, enabled: bool) -> None:
        if self._redis is not None:
            await self._redis.hset(self._pay_plan_key, mapping={"thumb_enabled": "1" if enabled else "0"})
            return
        self._thumb_enabled = enabled

    async def set_payment_prompt(self, request_id: str, chat_id: int, message_id: int) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        if self._redis is not None:
            await self._redis.hset(
                f"{self._pay_req_msg_prefix}{req_id}",
                mapping={"chat_id": str(int(chat_id)), "message_id": str(int(message_id))},
            )
        else:
            self._pay_req_messages[req_id] = (int(chat_id), int(message_id))
        await self.add_payment_message(req_id, int(chat_id), int(message_id))

    async def get_payment_prompt(self, request_id: str) -> Optional[tuple[int, int]]:
        req_id = str(request_id).strip()
        if not req_id:
            return None
        if self._redis is not None:
            data = await self._redis.hgetall(f"{self._pay_req_msg_prefix}{req_id}")
            if not data:
                return None
            try:
                return int(data.get("chat_id", "0") or 0), int(data.get("message_id", "0") or 0)
            except Exception:
                return None
        return self._pay_req_messages.get(req_id)

    async def clear_payment_prompt(self, request_id: str) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        if self._redis is not None:
            await self._redis.delete(f"{self._pay_req_msg_prefix}{req_id}")
            return
        self._pay_req_messages.pop(req_id, None)

    async def add_payment_message(self, request_id: str, chat_id: int, message_id: int) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        chat_id = int(chat_id)
        message_id = int(message_id)
        if chat_id == 0 or message_id <= 0:
            return
        if self._redis is not None:
            await self._redis.sadd(f"{self._pay_req_msgs_prefix}{req_id}", f"{chat_id}:{message_id}")
            return
        bucket = self._pay_req_message_set.setdefault(req_id, set())
        bucket.add((chat_id, message_id))

    async def list_payment_messages(self, request_id: str) -> list[tuple[int, int]]:
        req_id = str(request_id).strip()
        if not req_id:
            return []
        results: set[tuple[int, int]] = set()

        if self._redis is not None:
            values = await self._redis.smembers(f"{self._pay_req_msgs_prefix}{req_id}")
            for value in values or []:
                try:
                    chat_raw, msg_raw = str(value).split(":", 1)
                    results.add((int(chat_raw), int(msg_raw)))
                except Exception:
                    continue
        else:
            for item in self._pay_req_message_set.get(req_id, set()):
                try:
                    chat_id, message_id = item
                    results.add((int(chat_id), int(message_id)))
                except Exception:
                    continue

        prompt = await self.get_payment_prompt(req_id)
        if prompt:
            results.add((int(prompt[0]), int(prompt[1])))
        return sorted(results)

    async def clear_payment_messages(self, request_id: str) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        await self.clear_payment_prompt(req_id)
        if self._redis is not None:
            await self._redis.delete(f"{self._pay_req_msgs_prefix}{req_id}")
            return
        self._pay_req_message_set.pop(req_id, None)


    async def set_pending_utr(self, user_id: int, request_id: str, ttl_seconds: int = 900) -> None:
        key = f"{self._pay_pending_utr_prefix}{int(user_id)}"
        value = str(request_id).strip()
        if self._redis is not None:
            if ttl_seconds and ttl_seconds > 0:
                await self._redis.setex(key, int(ttl_seconds), value)
            else:
                await self._redis.set(key, value)
            return
        self._pay_pending_utr[int(user_id)] = value

    async def get_pending_utr(self, user_id: int) -> str:
        key = f"{self._pay_pending_utr_prefix}{int(user_id)}"
        if self._redis is not None:
            value = await self._redis.get(key)
            return (value or "").strip()
        return (self._pay_pending_utr.get(int(user_id)) or "").strip()

    async def clear_pending_utr(self, user_id: int) -> None:
        key = f"{self._pay_pending_utr_prefix}{int(user_id)}"
        if self._redis is not None:
            await self._redis.delete(key)
            return
        self._pay_pending_utr.pop(int(user_id), None)


    async def next_payment_request_id(self) -> str:
        if self._redis is not None:
            seq = int(await self._redis.incr(self._pay_req_seq_key))
            return f"{seq:03d}"
        self._pay_req_seq += 1
        return f"{self._pay_req_seq:03d}"

    async def reset_payment_requests(self) -> int:
        deleted = 0
        if self._redis is not None:
            keys: list[str] = [self._pay_req_index, self._pay_req_seq_key]
            patterns = [
                f"{self._pay_req_prefix}*",
                f"{self._pay_req_msg_prefix}*",
                f"{self._pay_req_msgs_prefix}*",
                f"{self._pay_pending_utr_prefix}*",
            ]
            for pattern in patterns:
                async for key in self._redis.scan_iter(match=pattern, count=1000):
                    keys.append(str(key))
            unique_keys = [k for k in dict.fromkeys(keys) if k]
            if unique_keys:
                deleted = int(await self._redis.delete(*unique_keys))
            return deleted

        deleted = (
            len(self._pay_requests)
            + len(self._pay_req_messages)
            + len(self._pay_req_message_set)
            + len(self._pay_pending_utr)
        )
        self._pay_requests.clear()
        self._pay_req_messages.clear()
        self._pay_req_message_set.clear()
        self._pay_pending_utr.clear()
        self._pay_req_seq = 0
        return deleted

    async def create_payment_request(
        self,
        request_id: str,
        user_id: int,
        amount_inr: float,
        credits: int,
        plan_type: str = "credits",
        *,
        gateway: str = "manual",
        expires_at: int = 0,
        qr_code_id: str = "",
        payment_link: str = "",
        txn_id: str = "",
        approved_by: str = "",
        grant_type: str = "",
        screenshot_file_id: str = "",
    ) -> dict:
        now = int(time.time())
        item = {
            "id": str(request_id),
            "user_id": int(user_id),
            "amount_inr": float(amount_inr),
            "credits": int(credits),
            "plan_type": str(plan_type or "credits").strip().lower(),
            "status": "pending",
            "gateway": str(gateway or "manual").strip().lower(),
            "created_at": now,
            "updated_at": now,
            "expires_at": int(expires_at or 0),
            "note": "",
            "admin_id": 0,
            "qr_code_id": str(qr_code_id or ""),
            "payment_link": str(payment_link or ""),
            "txn_id": str(txn_id or ""),
            "approved_by": str(approved_by or ""),
            "grant_type": str(grant_type or ""),
            "screenshot_file_id": str(screenshot_file_id or ""),
        }
        if self._redis is not None:
            key = f"{self._pay_req_prefix}{request_id}"
            await self._redis.hset(
                key,
                mapping={
                    "id": item["id"],
                    "user_id": str(item["user_id"]),
                    "amount_inr": f"{item['amount_inr']:.2f}",
                    "credits": str(item["credits"]),
                    "plan_type": item["plan_type"],
                    "status": item["status"],
                    "gateway": item["gateway"],
                    "created_at": str(item["created_at"]),
                    "updated_at": str(item["updated_at"]),
                    "expires_at": str(item["expires_at"]),
                    "note": item["note"],
                    "admin_id": str(item["admin_id"]),
                    "qr_code_id": item["qr_code_id"],
                    "payment_link": item["payment_link"],
                    "txn_id": item["txn_id"],
                    "approved_by": item["approved_by"],
                    "grant_type": item["grant_type"],
                    "screenshot_file_id": item["screenshot_file_id"],
                },
            )
            await self._redis.zadd(self._pay_req_index, {item["id"]: float(item["created_at"])})
            return item
        self._pay_requests[item["id"]] = item
        return item

    async def get_payment_request(self, request_id: str) -> Optional[dict]:
        request_id = str(request_id).strip()
        if not request_id:
            return None
        if self._redis is not None:
            data = await self._redis.hgetall(f"{self._pay_req_prefix}{request_id}")
            if not data:
                return None
            return {
                "id": data.get("id", request_id),
                "user_id": int(data.get("user_id", "0") or 0),
                "amount_inr": float(data.get("amount_inr", "0") or 0),
                "credits": int(data.get("credits", "0") or 0),
                "plan_type": data.get("plan_type", "credits"),
                "status": data.get("status", "pending"),
                "gateway": data.get("gateway", "manual"),
                "created_at": int(data.get("created_at", "0") or 0),
                "updated_at": int(data.get("updated_at", "0") or 0),
                "expires_at": int(data.get("expires_at", "0") or 0),
                "note": data.get("note", ""),
                "admin_id": int(data.get("admin_id", "0") or 0),
                "qr_code_id": data.get("qr_code_id", ""),
                "payment_link": data.get("payment_link", ""),
                "txn_id": data.get("txn_id", ""),
                "approved_by": data.get("approved_by", ""),
                "grant_type": data.get("grant_type", ""),
                "screenshot_file_id": data.get("screenshot_file_id", ""),
            }
        req = self._pay_requests.get(request_id)
        if req is not None and "plan_type" not in req:
            req["plan_type"] = "credits"
        if req is not None:
            req.setdefault("gateway", "manual")
            req.setdefault("expires_at", 0)
            req.setdefault("qr_code_id", "")
            req.setdefault("payment_link", "")
            req.setdefault("txn_id", "")
            req.setdefault("approved_by", "")
            req.setdefault("grant_type", "")
            req.setdefault("screenshot_file_id", "")
        return req

    async def update_payment_request(self, request_id: str, updates: dict) -> Optional[dict]:
        req = await self.get_payment_request(request_id)
        if not req:
            return None
        allowed = {
            "status", "note", "admin_id", "updated_at", "gateway", "expires_at",
            "qr_code_id", "payment_link", "txn_id", "approved_by", "grant_type",
            "screenshot_file_id", "credits", "amount_inr", "plan_type",
        }
        clean: dict[str, str] = {}
        for key, value in (updates or {}).items():
            if key not in allowed:
                continue
            if key in {"admin_id", "credits", "updated_at", "expires_at"}:
                req[key] = int(value or 0)
                clean[key] = str(req[key])
            elif key == "amount_inr":
                req[key] = float(value or 0)
                clean[key] = f"{req[key]:.2f}"
            else:
                req[key] = str(value or "").strip().lower() if key in {"status", "gateway", "plan_type"} else str(value or "")
                clean[key] = str(req[key])
        req["updated_at"] = int(time.time())
        clean["updated_at"] = str(req["updated_at"])
        if self._redis is not None:
            await self._redis.hset(f"{self._pay_req_prefix}{req['id']}", mapping=clean)
            return req
        self._pay_requests[req["id"]] = req
        return req

    async def set_payment_request_status(
        self,
        request_id: str,
        status: str,
        note: str = "",
        admin_id: int = 0,
    ) -> Optional[dict]:
        req = await self.get_payment_request(request_id)
        if not req:
            return None
        req["status"] = str(status).strip().lower()
        req["note"] = str(note or "").strip()
        req["admin_id"] = int(admin_id or 0)
        req["updated_at"] = int(time.time())
        if self._redis is not None:
            key = f"{self._pay_req_prefix}{req['id']}"
            await self._redis.hset(
                key,
                mapping={
                    "status": req["status"],
                    "note": req["note"],
                    "admin_id": str(req["admin_id"]),
                    "updated_at": str(req["updated_at"]),
                },
            )
            return req
        self._pay_requests[req["id"]] = req
        return req

    async def transition_payment_request_status(
        self,
        request_id: str,
        from_statuses: tuple[str, ...],
        to_status: str,
        note: str = "",
        admin_id: int = 0,
        extra_updates: Optional[dict] = None,
    ) -> tuple[Optional[dict], bool]:
        req_id = str(request_id).strip()
        if not req_id:
            return None, False

        allowed = {str(s).strip().lower() for s in (from_statuses or ()) if str(s).strip()}
        if not allowed:
            allowed = {"pending", "submitted"}
        target_status = str(to_status or "").strip().lower()
        if not target_status:
            return None, False

        now = int(time.time())
        new_note = str(note or "").strip()
        new_admin_id = int(admin_id or 0)

        if self._redis is not None:
            key = f"{self._pay_req_prefix}{req_id}"
            for _ in range(8):
                pipe = self._redis.pipeline()
                try:
                    await pipe.watch(key)
                    data = await pipe.hgetall(key)
                    if not data:
                        return None, False
                    req = {
                        "id": data.get("id", req_id),
                        "user_id": int(data.get("user_id", "0") or 0),
                        "amount_inr": float(data.get("amount_inr", "0") or 0),
                        "credits": int(data.get("credits", "0") or 0),
                        "plan_type": data.get("plan_type", "credits"),
                        "status": str(data.get("status", "pending") or "pending").strip().lower(),
                        "gateway": data.get("gateway", "manual"),
                        "created_at": int(data.get("created_at", "0") or 0),
                        "updated_at": int(data.get("updated_at", "0") or 0),
                        "expires_at": int(data.get("expires_at", "0") or 0),
                        "note": data.get("note", ""),
                        "admin_id": int(data.get("admin_id", "0") or 0),
                        "qr_code_id": data.get("qr_code_id", ""),
                        "payment_link": data.get("payment_link", ""),
                        "txn_id": data.get("txn_id", ""),
                        "approved_by": data.get("approved_by", ""),
                        "grant_type": data.get("grant_type", ""),
                        "screenshot_file_id": data.get("screenshot_file_id", ""),
                    }
                    if req["status"] not in allowed:
                        return req, False

                    req["status"] = target_status
                    req["note"] = new_note
                    req["admin_id"] = new_admin_id
                    req["updated_at"] = now
                    mapping = {
                        "status": req["status"],
                        "note": req["note"],
                        "admin_id": str(req["admin_id"]),
                        "updated_at": str(req["updated_at"]),
                    }
                    for key, value in (extra_updates or {}).items():
                        if key in {"expires_at"}:
                            req[key] = int(value or 0)
                            mapping[key] = str(req[key])
                        elif key in {"qr_code_id", "payment_link", "txn_id", "approved_by", "grant_type", "screenshot_file_id", "gateway", "plan_type"}:
                            req[key] = str(value or "")
                            mapping[key] = str(req[key])

                    pipe.multi()
                    await pipe.hset(key, mapping=mapping)
                    await pipe.execute()
                    return req, True
                except WatchError:
                    continue
                finally:
                    try:
                        await pipe.reset()
                    except Exception:
                        pass

            latest = await self.get_payment_request(req_id)
            return latest, False

        req = await self.get_payment_request(req_id)
        if not req:
            return None, False
        current_status = str(req.get("status", "pending")).strip().lower()
        if current_status not in allowed:
            return req, False
        req["status"] = target_status
        req["note"] = new_note
        req["admin_id"] = new_admin_id
        req["updated_at"] = now
        for key, value in (extra_updates or {}).items():
            if key == "expires_at":
                req[key] = int(value or 0)
            elif key in {"qr_code_id", "payment_link", "txn_id", "approved_by", "grant_type", "screenshot_file_id", "gateway", "plan_type"}:
                req[key] = str(value or "")
        self._pay_requests[req["id"]] = req
        return req, True

    async def delete_payment_request(self, request_id: str) -> bool:
        request_id = str(request_id).strip()
        if not request_id:
            return False
        if self._redis is not None:
            key = f"{self._pay_req_prefix}{request_id}"
            deleted = await self._redis.delete(key)
            await self._redis.zrem(self._pay_req_index, request_id)
            await self.clear_payment_messages(request_id)
            # Decrement seq only if this was the last issued ID
            try:
                current_seq = int(await self._redis.get(self._pay_req_seq_key) or 0)
                try:
                    req_num = int(request_id)
                except Exception:
                    req_num = -1
                if req_num > 0 and req_num == current_seq:
                    await self._redis.decr(self._pay_req_seq_key)
            except Exception:
                pass
            return deleted > 0
        if request_id in self._pay_requests:
            del self._pay_requests[request_id]
            await self.clear_payment_messages(request_id)
            # Decrement seq only if this was the last issued ID
            try:
                req_num = int(request_id)
                if req_num == self._pay_req_seq:
                    self._pay_req_seq = max(0, self._pay_req_seq - 1)
            except Exception:
                pass
            return True
        return False

    async def list_payment_requests(self, status: str = "all", limit: int = 20) -> list[dict]:
        status = (status or "all").strip().lower()
        limit = max(1, int(limit))
        items: list[dict] = []
        if self._redis is not None:
            request_ids = await self._redis.zrevrange(self._pay_req_index, 0, limit * 5)
            for request_id in request_ids:
                req = await self.get_payment_request(request_id)
                if not req:
                    continue
                if status != "all" and req.get("status") != status:
                    continue
                items.append(req)
                if len(items) >= limit:
                    break
            return items
        for req in sorted(self._pay_requests.values(), key=lambda x: x.get("created_at", 0), reverse=True):
            if status != "all" and req.get("status") != status:
                continue
            items.append(req)
            if len(items) >= limit:
                break
        return items

    async def get_user_active_payment_request(
        self,
        user_id: int,
        statuses: tuple[str, ...] = ("pending", "submitted"),
        scan_limit: int = 1000,
    ) -> Optional[dict]:
        uid = int(user_id or 0)
        if uid <= 0:
            return None
        status_set = {str(s).strip().lower() for s in statuses if str(s).strip()}
        if not status_set:
            status_set = {"pending", "submitted"}
        scan_limit = max(1, int(scan_limit))

        if self._redis is not None:
            request_ids = await self._redis.zrevrange(self._pay_req_index, 0, scan_limit - 1)
            for request_id in request_ids:
                req = await self.get_payment_request(request_id)
                if not req:
                    continue
                if int(req.get("user_id", 0) or 0) != uid:
                    continue
                if str(req.get("status", "")).strip().lower() in status_set:
                    return req
            return None

        for req in sorted(self._pay_requests.values(), key=lambda x: x.get("created_at", 0), reverse=True):
            if int(req.get("user_id", 0) or 0) != uid:
                continue
            if str(req.get("status", "")).strip().lower() in status_set:
                return req
        return None

    async def pending_xwallet_orders(self, limit: int = 1000) -> list[dict]:
        limit = max(1, int(limit))
        now = int(time.time())
        items: list[dict] = []
        if self._redis is not None:
            request_ids = await self._redis.zrevrange(self._pay_req_index, 0, limit * 5)
            for request_id in request_ids:
                req = await self.get_payment_request(request_id)
                if not req:
                    continue
                if str(req.get("gateway", "")).strip().lower() != "xwallet":
                    continue
                if str(req.get("status", "")).strip().lower() not in {"pending", "processing"}:
                    continue
                if int(req.get("expires_at", 0) or 0) <= now:
                    continue
                items.append(req)
                if len(items) >= limit:
                    break
            return items

        for req in sorted(self._pay_requests.values(), key=lambda x: x.get("created_at", 0), reverse=True):
            if str(req.get("gateway", "")).strip().lower() != "xwallet":
                continue
            if str(req.get("status", "")).strip().lower() not in {"pending", "processing"}:
                continue
            if int(req.get("expires_at", 0) or 0) <= now:
                continue
            items.append(req)
            if len(items) >= limit:
                break
        return items

    async def acquire_action_lock(self, key: str, ttl_seconds: int) -> bool:
        lock_key = str(key or "").strip()
        ttl = max(0, int(ttl_seconds or 0))
        if not lock_key or ttl <= 0:
            return True

        if self._redis is not None:
            result = await self._redis.set(f"{self._action_lock_prefix}{lock_key}", "1", ex=ttl, nx=True)
            return bool(result)

        now = time.time()
        expires_at = float(self._action_locks.get(lock_key, 0.0) or 0.0)
        if expires_at > now:
            return False
        self._action_locks[lock_key] = now + ttl

        # Light cleanup for expired locks.
        if len(self._action_locks) > 5000:
            self._action_locks = {k: v for k, v in self._action_locks.items() if v > now}
        return True


    async def list_known_user_ids(self, limit: int = 50000) -> list[int]:
        limit = max(1, int(limit))
        users: set[int] = set()
        if self._redis is not None:
            async for key in self._redis.scan_iter(match="credits:*", count=1000):
                try:
                    users.add(int(str(key).split(":", 1)[1]))
                except Exception:
                    continue
            request_ids = await self._redis.zrevrange(self._pay_req_index, 0, limit * 5)
            for request_id in request_ids:
                req = await self.get_payment_request(request_id)
                if not req:
                    continue
                uid = int(req.get("user_id", 0) or 0)
                if uid > 0:
                    users.add(uid)
                if len(users) >= limit:
                    break
            return sorted(users)

        users.update(int(uid) for uid in self._credits.keys())
        for req in self._pay_requests.values():
            uid = int(req.get("user_id", 0) or 0)
            if uid > 0:
                users.add(uid)
            if len(users) >= limit:
                break
        return sorted(users)


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

    async def set_home_section(self, section_id: Optional[str], section_name: Optional[str] = None) -> None:
        if not section_id:
            if self._redis is not None:
                await self._redis.delete(self._home_section_key)
                await self._redis.delete(self._home_section_name_key)
            self._home_section = None
            self._home_section_name = None
            return

        if self._redis is not None:
            await self._redis.set(self._home_section_key, section_id)
            await self._redis.set(self._home_section_name_key, section_name or section_id)
            return

        self._home_section = section_id
        self._home_section_name = section_name or section_id

    async def get_home_section(self) -> tuple[Optional[str], Optional[str]]:
        if self._redis is not None:
            section_id = await self._redis.get(self._home_section_key)
            section_name = await self._redis.get(self._home_section_name_key)
            return (section_id or None, section_name or None)
        return (self._home_section, self._home_section_name)

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

    async def set_public_section(self, section_id: str, section_name: str, is_public: bool = True) -> None:
        sid = str(section_id or "").strip()
        if not sid:
            return
        if self._redis is not None:
            if is_public:
                await self._redis.hset(self._public_section_map, sid, section_name or sid)
            else:
                await self._redis.hdel(self._public_section_map, sid)
            return

        if is_public:
            self._public_sections[sid] = section_name or sid
        else:
            self._public_sections.pop(sid, None)

    async def list_public_sections(self) -> list[tuple[str, str]]:
        if self._redis is not None:
            data = await self._redis.hgetall(self._public_section_map)
            return [(name, section_id) for section_id, name in data.items()]
        return [(name, section_id) for section_id, name in self._public_sections.items()]

    async def add_trending_item(self, item: dict) -> dict:
        item_id = str(item.get("id") or secrets.token_urlsafe(8)).strip()
        now = float(item.get("created_at") or time.time())
        media_items: list[dict] = []
        for media in item.get("media", []) or []:
            file_id = str((media or {}).get("file_id", "") or "").strip()
            media_type = str((media or {}).get("type", "") or "").strip().lower()
            if file_id and media_type in {"photo", "video"}:
                media_items.append({"file_id": file_id, "type": media_type})
        legacy_file_id = str(item.get("media_file_id", "") or "").strip()
        legacy_media_type = str(item.get("media_type", "") or "").strip().lower()
        if not media_items and legacy_file_id and legacy_media_type in {"photo", "video"}:
            media_items.append({"file_id": legacy_file_id, "type": legacy_media_type})
        first_media = media_items[0] if media_items else {"file_id": "", "type": ""}
        payload = {
            "id": item_id,
            "bar": str(item.get("bar", "") or "").strip(),
            "title": str(item.get("title", "") or "").strip(),
            "description": str(item.get("description", "") or "").strip(),
            "media": media_items,
            "media_file_id": first_media["file_id"],
            "media_type": first_media["type"],
            "normal_link": str(item.get("normal_link", "") or "").strip(),
            "premium_link": str(item.get("premium_link", "") or "").strip(),
            "created_at": now,
            "created_by": int(item.get("created_by", 0) or 0),
        }
        if self._redis is not None:
            key = f"{self._trending_item_prefix}{item_id}"
            await self._redis.set(key, json.dumps(payload))
            await self._redis.lrem(self._trending_index_key, 0, item_id)
            await self._redis.lpush(self._trending_index_key, item_id)
            return payload

        self._trending_items[item_id] = payload
        if item_id in self._trending_index:
            self._trending_index.remove(item_id)
        self._trending_index.insert(0, item_id)
        return payload

    async def get_trending_item(self, item_id: str) -> Optional[dict]:
        item_id = str(item_id or "").strip()
        if not item_id:
            return None
        if self._redis is not None:
            raw = await self._redis.get(f"{self._trending_item_prefix}{item_id}")
            if not raw:
                return None
            try:
                return json.loads(raw)
            except Exception:
                return None
        return self._trending_items.get(item_id)

    async def list_trending_items(self, limit: int = 100) -> list[dict]:
        limit = max(int(limit), 1)
        if self._redis is not None:
            ids = await self._redis.lrange(self._trending_index_key, 0, limit - 1)
            items: list[dict] = []
            for item_id in ids:
                item = await self.get_trending_item(item_id)
                if item:
                    items.append(item)
            return items

        items = []
        for item_id in self._trending_index[:limit]:
            item = self._trending_items.get(item_id)
            if item:
                items.append(item)
        return items

    async def delete_trending_item(self, item_id: str) -> bool:
        item_id = str(item_id or "").strip()
        if not item_id:
            return False
        if self._redis is not None:
            existed = await self._redis.delete(f"{self._trending_item_prefix}{item_id}")
            await self._redis.lrem(self._trending_index_key, 0, item_id)
            return bool(existed)

        existed = item_id in self._trending_items
        self._trending_items.pop(item_id, None)
        if item_id in self._trending_index:
            self._trending_index.remove(item_id)
        return existed

    async def delete_section(self, section_name: str) -> bool:
        normalized = _normalize_section(section_name)
        if self._redis is not None:
            section_id = await self._redis.hget(self._section_name_map, normalized)
            if not section_id:
                return False
            await self._redis.hdel(self._section_name_map, normalized)
            await self._redis.hdel(self._section_id_map, section_id)
            await self._redis.hdel(self._public_section_map, section_id)
            await self._redis.delete(f"section:{section_id}")
            await self._redis.delete(f"section:views:count:{section_id}")
            await self._redis.delete(f"section:views:unique:{section_id}")
            current = await self._redis.get(self._section_key)
            if current and current == section_id:
                await self._redis.delete(self._section_key)
                await self._redis.delete(self._section_name_key)
            home = await self._redis.get(self._home_section_key)
            if home and home == section_id:
                await self._redis.delete(self._home_section_key)
                await self._redis.delete(self._home_section_name_key)
            return True

        section_id = self._section_registry.get(normalized)
        if not section_id:
            return False
        self._section_registry.pop(normalized, None)
        self._section_registry_id.pop(section_id, None)
        self._public_sections.pop(section_id, None)
        self._sections.pop(section_id, None)
        self._section_view_counts.pop(section_id, None)
        self._section_unique_viewers.pop(section_id, None)
        if self._current_section == section_id:
            self._current_section = None
            self._current_section_name = None
        if self._home_section == section_id:
            self._home_section = None
            self._home_section_name = None
        return True


    async def list_section(self, section_id: str, limit: int) -> list[str]:
        limit = max(int(limit), 1)
        if self._redis is not None:
            tokens = await self._redis.lrange(f"section:{section_id}", 0, limit - 1)
            return [t for t in tokens if t]
        return self._sections.get(section_id, [])[:limit]

