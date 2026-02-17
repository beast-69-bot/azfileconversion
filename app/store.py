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
        self._pay_plan_key = "plan:pay"
        self._pay_req_prefix = "pay:req:"
        self._pay_req_index = "pay:req:index"
        self._pay_req_seq_key = "pay:req:seq"
        self._pay_req_msg_prefix = "pay:reqmsg:"
        self._pay_pending_utr_prefix = "pay:pending_utr:"
        self._current_section: Optional[str] = None
        self._current_section_name: Optional[str] = None
        self._pay_price: Optional[float] = None
        self._pay_text: Optional[str] = None
        self._upi_id: Optional[str] = None
        self._pay_pending_utr: dict[int, str] = {}
        self._pay_req_messages: dict[str, tuple[int, int]] = {}
        self._pay_req_seq: int = 0
        self._pay_requests: dict[str, dict] = {}

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


    async def set_payment_prompt(self, request_id: str, chat_id: int, message_id: int) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        if self._redis is not None:
            await self._redis.hset(
                f"{self._pay_req_msg_prefix}{req_id}",
                mapping={"chat_id": str(int(chat_id)), "message_id": str(int(message_id))},
            )
            return
        self._pay_req_messages[req_id] = (int(chat_id), int(message_id))

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
                f"{self._pay_pending_utr_prefix}*",
            ]
            for pattern in patterns:
                async for key in self._redis.scan_iter(match=pattern, count=1000):
                    keys.append(str(key))
            unique_keys = [k for k in dict.fromkeys(keys) if k]
            if unique_keys:
                deleted = int(await self._redis.delete(*unique_keys))
            return deleted

        deleted = len(self._pay_requests) + len(self._pay_req_messages) + len(self._pay_pending_utr)
        self._pay_requests.clear()
        self._pay_req_messages.clear()
        self._pay_pending_utr.clear()
        self._pay_req_seq = 0
        return deleted

    async def create_payment_request(self, request_id: str, user_id: int, amount_inr: float, credits: int, plan_type: str = "credits") -> dict:
        now = int(time.time())
        item = {
            "id": str(request_id),
            "user_id": int(user_id),
            "amount_inr": float(amount_inr),
            "credits": int(credits),
            "plan_type": str(plan_type or "credits").strip().lower(),
            "status": "pending",
            "created_at": now,
            "updated_at": now,
            "note": "",
            "admin_id": 0,
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
                    "created_at": str(item["created_at"]),
                    "updated_at": str(item["updated_at"]),
                    "note": item["note"],
                    "admin_id": str(item["admin_id"]),
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
                "created_at": int(data.get("created_at", "0") or 0),
                "updated_at": int(data.get("updated_at", "0") or 0),
                "note": data.get("note", ""),
                "admin_id": int(data.get("admin_id", "0") or 0),
            }
        req = self._pay_requests.get(request_id)
        if req is not None and "plan_type" not in req:
            req["plan_type"] = "credits"
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

