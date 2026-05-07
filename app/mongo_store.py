import secrets
import time
from dataclasses import asdict
from typing import Optional

from app.store import FileRef, TokenStore, _normalize_section, _slugify

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    from pymongo import ASCENDING, DESCENDING, ReturnDocument
    from pymongo.errors import DuplicateKeyError
except Exception:
    AsyncIOMotorClient = None
    ASCENDING = 1
    DESCENDING = -1
    ReturnDocument = None
    DuplicateKeyError = Exception


class MongoTokenStore(TokenStore):
    def __init__(self, redis_url: Optional[str], mongo_uri: str, mongo_db_name: str, history_limit: int = 200) -> None:
        super().__init__(redis_url, history_limit=history_limit)
        self._mongo_uri = str(mongo_uri or "").strip()
        self._mongo_db_name = str(mongo_db_name or "azfileconversion").strip() or "azfileconversion"
        self._mongo_client = None
        self._mongo = None
        self._tokens = None
        self._token_metrics = None
        self._token_viewers = None
        self._token_likes = None
        self._token_reactions = None
        self._section_metrics = None
        self._section_viewers = None
        self._credits_col = None
        self._config_col = None
        self._payment_requests = None
        self._payment_messages = None
        self._pending_utrs = None
        self._sections_col = None
        self._counters_col = None
        self._site_visitors = None
        self._trending_items = None

    async def connect(self) -> None:
        await super().connect()
        if not self._mongo_uri:
            raise SystemExit("MongoTokenStore requires MONGO_URI.")
        if AsyncIOMotorClient is None:
            raise SystemExit("MONGO_URI is configured but motor/pymongo is not installed.")

        self._mongo_client = AsyncIOMotorClient(self._mongo_uri)
        self._mongo = self._mongo_client[self._mongo_db_name]
        self._tokens = self._mongo["tokens"]
        self._token_metrics = self._mongo["token_metrics"]
        self._token_viewers = self._mongo["token_viewers"]
        self._token_likes = self._mongo["token_likes"]
        self._token_reactions = self._mongo["token_reactions"]
        self._section_metrics = self._mongo["section_metrics"]
        self._section_viewers = self._mongo["section_viewers"]
        self._credits_col = self._mongo["credits"]
        self._config_col = self._mongo["config"]
        self._payment_requests = self._mongo["payment_requests"]
        self._payment_messages = self._mongo["payment_messages"]
        self._pending_utrs = self._mongo["pending_utrs"]
        self._sections_col = self._mongo["sections"]
        self._counters_col = self._mongo["counters"]
        self._site_visitors = self._mongo["site_visitors"]
        self._trending_items = self._mongo["trending_items"]

        await self._tokens.create_index([("created_at", DESCENDING)])
        await self._tokens.create_index([("section_id", ASCENDING), ("created_at", DESCENDING)])
        await self._tokens.create_index([("expires_at", ASCENDING)])
        await self._token_viewers.create_index([("token", ASCENDING), ("viewer_id", ASCENDING)], unique=True)
        await self._token_likes.create_index([("token", ASCENDING), ("viewer_id", ASCENDING)], unique=True)
        await self._token_reactions.create_index([("token", ASCENDING), ("user_id", ASCENDING)], unique=True)
        await self._section_viewers.create_index([("section_id", ASCENDING), ("viewer_id", ASCENDING)], unique=True)
        await self._sections_col.create_index([("normalized", ASCENDING)], unique=True)
        await self._payment_requests.create_index([("created_at", DESCENDING)])
        await self._payment_requests.create_index([("user_id", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING)])
        await self._payment_messages.create_index([("request_id", ASCENDING), ("chat_id", ASCENDING), ("message_id", ASCENDING)], unique=True)
        await self._pending_utrs.create_index([("expires_at", ASCENDING)])
        await self._credits_col.create_index([("balance", DESCENDING)])
        await self._trending_items.create_index([("created_at", DESCENDING)])
        await self._trending_items.create_index([("bar", ASCENDING), ("created_at", DESCENDING)])

    async def close(self) -> None:
        if self._mongo_client is not None:
            self._mongo_client.close()
            self._mongo_client = None
        await super().close()

    def _live_filter(self, extra: Optional[dict] = None) -> dict:
        query = {"$or": [{"expires_at": {"$exists": False}}, {"expires_at": {"$gt": time.time()}}]}
        if extra:
            query.update(extra)
        return query

    def _token_doc_to_ref(self, doc: Optional[dict]) -> Optional[FileRef]:
        if not doc:
            return None
        payload = dict(doc)
        payload.pop("_id", None)
        payload.pop("expires_at", None)
        return FileRef(**payload)

    def _payment_doc_to_dict(self, doc: Optional[dict]) -> Optional[dict]:
        if not doc:
            return None
        return {
            "id": str(doc.get("_id", "")),
            "user_id": int(doc.get("user_id", 0) or 0),
            "amount_inr": float(doc.get("amount_inr", 0) or 0),
            "credits": int(doc.get("credits", 0) or 0),
            "plan_type": str(doc.get("plan_type", "credits") or "credits"),
            "status": str(doc.get("status", "pending") or "pending"),
            "gateway": str(doc.get("gateway", "manual") or "manual"),
            "created_at": int(doc.get("created_at", 0) or 0),
            "updated_at": int(doc.get("updated_at", 0) or 0),
            "expires_at": int(doc.get("expires_at", 0) or 0),
            "note": str(doc.get("note", "") or ""),
            "admin_id": int(doc.get("admin_id", 0) or 0),
            "qr_code_id": str(doc.get("qr_code_id", "") or ""),
            "payment_link": str(doc.get("payment_link", "") or ""),
            "txn_id": str(doc.get("txn_id", "") or ""),
            "approved_by": str(doc.get("approved_by", "") or ""),
            "grant_type": str(doc.get("grant_type", "") or ""),
            "screenshot_file_id": str(doc.get("screenshot_file_id", "") or ""),
        }

    async def set(self, token: str, ref: FileRef, ttl_seconds: int) -> None:
        doc = {"_id": token, **asdict(ref)}
        if ttl_seconds and ttl_seconds > 0:
            doc["expires_at"] = float(ref.created_at) + int(ttl_seconds)
        await self._tokens.replace_one({"_id": token}, doc, upsert=True)

    async def get(self, token: str, ttl_seconds: int) -> Optional[FileRef]:
        doc = await self._tokens.find_one({"_id": token})
        if not doc:
            return None
        expires_at = doc.get("expires_at")
        if expires_at is not None and float(expires_at) <= time.time():
            await self._tokens.delete_one({"_id": token})
            return None
        return self._token_doc_to_ref(doc)

    async def get_many(self, tokens: list[str], ttl_seconds: int) -> dict[str, FileRef]:
        ordered = [str(token) for token in tokens if token]
        if not ordered:
            return {}
        now = time.time()
        results: dict[str, FileRef] = {}
        expired: list[str] = []
        cursor = self._tokens.find({"_id": {"$in": ordered}})
        async for doc in cursor:
            token = str(doc.get("_id", "") or "")
            if not token:
                continue
            expires_at = doc.get("expires_at")
            if expires_at is not None and float(expires_at) <= now:
                expired.append(token)
                continue
            ref = self._token_doc_to_ref(doc)
            if ref is not None:
                results[token] = ref
        if expired:
            await self._tokens.delete_many({"_id": {"$in": expired}})
        return results

    async def list_recent(self, limit: int) -> list[str]:
        limit = max(int(limit), 1)
        cursor = self._tokens.find(self._live_filter(), {"_id": 1}).sort("created_at", DESCENDING).limit(limit)
        return [str(doc["_id"]) async for doc in cursor]

    async def increment_view(self, token: str, viewer_id: Optional[str], ttl_seconds: int) -> tuple[int, int]:
        doc = await self._token_metrics.find_one_and_update(
            {"_id": token},
            {"$inc": {"views_total": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        unique = 0
        if viewer_id:
            try:
                await self._token_viewers.insert_one({"token": token, "viewer_id": viewer_id})
            except DuplicateKeyError:
                pass
            unique = await self._token_viewers.count_documents({"token": token})
        return int(doc.get("views_total", 0) or 0), int(unique)

    async def get_views(self, token: str) -> tuple[int, int]:
        doc = await self._token_metrics.find_one({"_id": token}, {"views_total": 1})
        total = int((doc or {}).get("views_total", 0) or 0)
        unique = await self._token_viewers.count_documents({"token": token})
        return total, int(unique)

    async def get_views_many(self, tokens: list[str]) -> dict[str, tuple[int, int]]:
        ordered = [str(token) for token in tokens if token]
        if not ordered:
            return {}
        token_set = list(dict.fromkeys(ordered))

        totals: dict[str, int] = {}
        cursor = self._token_metrics.find({"_id": {"$in": token_set}}, {"views_total": 1})
        async for doc in cursor:
            totals[str(doc["_id"])] = int(doc.get("views_total", 0) or 0)

        uniques: dict[str, int] = {}
        pipeline = [
            {"$match": {"token": {"$in": token_set}}},
            {"$group": {"_id": "$token", "count": {"$sum": 1}}},
        ]
        async for row in self._token_viewers.aggregate(pipeline):
            uniques[str(row["_id"])] = int(row.get("count", 0) or 0)

        return {token: (totals.get(token, 0), uniques.get(token, 0)) for token in ordered}

    async def increment_site_visit(self) -> int:
        doc = await self._counters_col.find_one_and_update(
            {"_id": "site_visits_total"},
            {"$inc": {"value": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int((doc or {}).get("value", 0) or 0)

    async def get_site_visits(self) -> int:
        doc = await self._counters_col.find_one({"_id": "site_visits_total"}, {"value": 1})
        return int((doc or {}).get("value", 0) or 0)

    async def register_site_visit(self, visitor_id: str) -> int:
        visitor_id = str(visitor_id or "").strip()
        if not visitor_id:
            return await self.get_site_visits()
        try:
            await self._site_visitors.insert_one({"_id": visitor_id, "created_at": time.time()})
        except DuplicateKeyError:
            pass
        return int(await self._site_visitors.count_documents({}))

    async def increment_section_view(self, section_id: str, viewer_id: Optional[str]) -> tuple[int, int]:
        doc = await self._section_metrics.find_one_and_update(
            {"_id": section_id},
            {"$inc": {"views_total": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        unique = 0
        if viewer_id:
            try:
                await self._section_viewers.insert_one({"section_id": section_id, "viewer_id": viewer_id})
            except DuplicateKeyError:
                pass
            unique = await self._section_viewers.count_documents({"section_id": section_id})
        return int(doc.get("views_total", 0) or 0), int(unique)

    async def get_section_views(self, section_id: str) -> tuple[int, int]:
        doc = await self._section_metrics.find_one({"_id": section_id}, {"views_total": 1})
        total = int((doc or {}).get("views_total", 0) or 0)
        unique = await self._section_viewers.count_documents({"section_id": section_id})
        return total, int(unique)

    async def set_like(self, token: str, viewer_id: str, liked: bool) -> tuple[int, bool]:
        if liked:
            await self._token_likes.update_one(
                {"token": token, "viewer_id": viewer_id},
                {"$set": {"token": token, "viewer_id": viewer_id}},
                upsert=True,
            )
        else:
            await self._token_likes.delete_one({"token": token, "viewer_id": viewer_id})
        total = await self._token_likes.count_documents({"token": token})
        user_liked = await self._token_likes.count_documents({"token": token, "viewer_id": viewer_id}) > 0
        return int(total), bool(user_liked)

    async def get_likes(self, token: str, viewer_id: Optional[str] = None) -> tuple[int, bool]:
        total = await self._token_likes.count_documents({"token": token})
        user_liked = False
        if viewer_id:
            user_liked = await self._token_likes.count_documents({"token": token, "viewer_id": viewer_id}) > 0
        return int(total), bool(user_liked)

    async def get_reactions(self, token: str, user_id: Optional[int] = None) -> tuple[int, int, int]:
        likes = await self._token_reactions.count_documents({"token": token, "reaction": 1})
        dislikes = await self._token_reactions.count_documents({"token": token, "reaction": -1})
        status = 0
        if user_id is not None:
            row = await self._token_reactions.find_one({"token": token, "user_id": int(user_id)}, {"reaction": 1})
            if row:
                status = int(row.get("reaction", 0) or 0)
        return int(likes), int(dislikes), status

    async def set_reaction(self, token: str, user_id: int, reaction: int) -> tuple[int, int, int]:
        if reaction in {1, -1}:
            await self._token_reactions.update_one(
                {"token": token, "user_id": int(user_id)},
                {"$set": {"token": token, "user_id": int(user_id), "reaction": int(reaction)}},
                upsert=True,
            )
        else:
            await self._token_reactions.delete_one({"token": token, "user_id": int(user_id)})
        return await self.get_reactions(token, user_id)

    async def get_credits(self, user_id: int) -> int:
        doc = await self._credits_col.find_one({"_id": int(user_id)}, {"balance": 1})
        return int((doc or {}).get("balance", 0) or 0)

    async def add_credits(self, user_id: int, amount: int) -> int:
        doc = await self._credits_col.find_one_and_update(
            {"_id": int(user_id)},
            {"$inc": {"balance": int(amount)}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int(doc.get("balance", 0) or 0)

    async def charge_credits(self, user_id: int, amount: int) -> tuple[bool, int]:
        amount = int(amount)
        if amount <= 0:
            return True, await self.get_credits(user_id)
        doc = await self._credits_col.find_one_and_update(
            {"_id": int(user_id), "balance": {"$gte": amount}},
            {"$inc": {"balance": -amount}},
            return_document=ReturnDocument.AFTER,
        )
        if doc:
            return True, int(doc.get("balance", 0) or 0)
        return False, await self.get_credits(user_id)

    async def list_credit_balances(self, limit: int = 20) -> list[tuple[int, int]]:
        limit = max(1, int(limit))
        rows: list[tuple[int, int]] = []
        cursor = self._credits_col.find({}, {"balance": 1}).sort("balance", DESCENDING).limit(limit)
        async for row in cursor:
            rows.append((int(row["_id"]), int(row.get("balance", 0) or 0)))
        return rows

    async def get_pay_plan(self, default_price: float, default_text: str) -> tuple[float, str]:
        doc = await self._config_col.find_one({"_id": "pay_plan"})
        price = float((doc or {}).get("price", default_price) or default_price)
        text = str((doc or {}).get("text", default_text) or default_text)
        return price, text

    async def set_pay_plan(self, price: float, text: str) -> tuple[float, str]:
        price = float(price)
        text = str(text).strip()
        await self._config_col.update_one(
            {"_id": "pay_plan"},
            {"$set": {"price": price, "text": text}},
            upsert=True,
        )
        return price, text

    async def get_payment_settings(self) -> dict:
        defaults = {
            "payment_gateway": "manual",
            "xwallet_api_key": "",
            "tutorial_chat_id": 0,
            "tutorial_message_id": 0,
            "total_earnings": 0.0,
        }
        doc = await self._config_col.find_one({"_id": "pay_plan"})
        return {
            "payment_gateway": str((doc or {}).get("payment_gateway", defaults["payment_gateway"]) or defaults["payment_gateway"]).strip().lower(),
            "xwallet_api_key": str((doc or {}).get("xwallet_api_key", defaults["xwallet_api_key"]) or ""),
            "tutorial_chat_id": int((doc or {}).get("tutorial_chat_id", defaults["tutorial_chat_id"]) or 0),
            "tutorial_message_id": int((doc or {}).get("tutorial_message_id", defaults["tutorial_message_id"]) or 0),
            "total_earnings": float((doc or {}).get("total_earnings", defaults["total_earnings"]) or 0.0),
        }

    async def update_payment_settings(self, updates: dict) -> dict:
        clean: dict[str, object] = {}
        for key, value in (updates or {}).items():
            if key == "payment_gateway":
                clean[key] = str(value or "manual").strip().lower()
            elif key == "xwallet_api_key":
                clean[key] = str(value or "").strip()
            elif key in {"tutorial_chat_id", "tutorial_message_id"}:
                clean[key] = int(value or 0)
            elif key == "total_earnings":
                clean[key] = float(value or 0.0)
        if clean:
            await self._config_col.update_one({"_id": "pay_plan"}, {"$set": clean}, upsert=True)
        return await self.get_payment_settings()

    async def add_total_earnings(self, amount: float) -> float:
        doc = await self._config_col.find_one_and_update(
            {"_id": "pay_plan"},
            {"$inc": {"total_earnings": float(amount or 0.0)}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return float((doc or {}).get("total_earnings", 0.0) or 0.0)

    async def get_upi_id(self) -> str:
        doc = await self._config_col.find_one({"_id": "pay_plan"}, {"upi_id": 1})
        return str((doc or {}).get("upi_id", "") or "").strip()

    async def set_upi_id(self, upi_id: str) -> str:
        clean = str(upi_id or "").strip()
        if clean:
            await self._config_col.update_one({"_id": "pay_plan"}, {"$set": {"upi_id": clean}}, upsert=True)
        else:
            await self._config_col.update_one({"_id": "pay_plan"}, {"$unset": {"upi_id": ""}}, upsert=True)
        return clean

    async def get_auto_delete(self, default: int = 0) -> int:
        doc = await self._config_col.find_one({"_id": "pay_plan"}, {"auto_delete_seconds": 1})
        try:
            return max(0, int((doc or {}).get("auto_delete_seconds", default) or default))
        except Exception:
            return default

    async def set_auto_delete(self, seconds: int) -> int:
        seconds = max(0, int(seconds))
        await self._config_col.update_one(
            {"_id": "pay_plan"},
            {"$set": {"auto_delete_seconds": seconds}},
            upsert=True,
        )
        return seconds

    async def get_thumbnail(self) -> str:
        doc = await self._config_col.find_one({"_id": "pay_plan"}, {"thumb_file_id": 1})
        return str((doc or {}).get("thumb_file_id", "") or "").strip()

    async def set_thumbnail(self, file_id: str) -> str:
        fid = str(file_id or "").strip()
        if fid:
            await self._config_col.update_one({"_id": "pay_plan"}, {"$set": {"thumb_file_id": fid}}, upsert=True)
        else:
            await self._config_col.update_one({"_id": "pay_plan"}, {"$unset": {"thumb_file_id": ""}}, upsert=True)
        return fid

    async def del_thumbnail(self) -> None:
        await self.set_thumbnail("")

    async def get_thumbnail_enabled(self) -> bool:
        doc = await self._config_col.find_one({"_id": "pay_plan"}, {"thumb_enabled": 1})
        return (str((doc or {}).get("thumb_enabled", "1")) or "1").lower() not in {"0", "false", "off"}

    async def set_thumbnail_enabled(self, enabled: bool) -> None:
        await self._config_col.update_one(
            {"_id": "pay_plan"},
            {"$set": {"thumb_enabled": "1" if enabled else "0"}},
            upsert=True,
        )

    async def set_payment_prompt(self, request_id: str, chat_id: int, message_id: int) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        await self._payment_requests.update_one(
            {"_id": req_id},
            {"$set": {"prompt_chat_id": int(chat_id), "prompt_message_id": int(message_id)}},
            upsert=True,
        )
        await self.add_payment_message(req_id, int(chat_id), int(message_id))

    async def get_payment_prompt(self, request_id: str) -> Optional[tuple[int, int]]:
        req_id = str(request_id).strip()
        if not req_id:
            return None
        doc = await self._payment_requests.find_one({"_id": req_id}, {"prompt_chat_id": 1, "prompt_message_id": 1})
        if not doc:
            return None
        chat_id = int(doc.get("prompt_chat_id", 0) or 0)
        message_id = int(doc.get("prompt_message_id", 0) or 0)
        if chat_id == 0 or message_id <= 0:
            return None
        return chat_id, message_id

    async def clear_payment_prompt(self, request_id: str) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        await self._payment_requests.update_one(
            {"_id": req_id},
            {"$unset": {"prompt_chat_id": "", "prompt_message_id": ""}},
        )

    async def add_payment_message(self, request_id: str, chat_id: int, message_id: int) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        try:
            await self._payment_messages.insert_one(
                {"request_id": req_id, "chat_id": int(chat_id), "message_id": int(message_id)}
            )
        except DuplicateKeyError:
            pass

    async def list_payment_messages(self, request_id: str) -> list[tuple[int, int]]:
        req_id = str(request_id).strip()
        if not req_id:
            return []
        results: set[tuple[int, int]] = set()
        cursor = self._payment_messages.find({"request_id": req_id}, {"chat_id": 1, "message_id": 1})
        async for row in cursor:
            results.add((int(row.get("chat_id", 0) or 0), int(row.get("message_id", 0) or 0)))
        prompt = await self.get_payment_prompt(req_id)
        if prompt:
            results.add((int(prompt[0]), int(prompt[1])))
        return sorted(results)

    async def clear_payment_messages(self, request_id: str) -> None:
        req_id = str(request_id).strip()
        if not req_id:
            return
        await self.clear_payment_prompt(req_id)
        await self._payment_messages.delete_many({"request_id": req_id})

    async def set_pending_utr(self, user_id: int, request_id: str, ttl_seconds: int = 900) -> None:
        expires_at = int(time.time()) + max(0, int(ttl_seconds or 0)) if ttl_seconds and ttl_seconds > 0 else None
        await self._pending_utrs.update_one(
            {"_id": int(user_id)},
            {"$set": {"request_id": str(request_id).strip(), "expires_at": expires_at}},
            upsert=True,
        )

    async def get_pending_utr(self, user_id: int) -> str:
        doc = await self._pending_utrs.find_one({"_id": int(user_id)})
        if not doc:
            return ""
        expires_at = doc.get("expires_at")
        if expires_at is not None and int(expires_at) <= int(time.time()):
            await self._pending_utrs.delete_one({"_id": int(user_id)})
            return ""
        return str(doc.get("request_id", "") or "").strip()

    async def clear_pending_utr(self, user_id: int) -> None:
        await self._pending_utrs.delete_one({"_id": int(user_id)})

    async def next_payment_request_id(self) -> str:
        doc = await self._counters_col.find_one_and_update(
            {"_id": "payment_request_seq"},
            {"$inc": {"value": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return f"{int(doc.get('value', 0) or 0):03d}"

    async def reset_payment_requests(self) -> int:
        deleted = 0
        deleted += int((await self._payment_requests.delete_many({})).deleted_count)
        deleted += int((await self._payment_messages.delete_many({})).deleted_count)
        deleted += int((await self._pending_utrs.delete_many({})).deleted_count)
        await self._counters_col.update_one({"_id": "payment_request_seq"}, {"$set": {"value": 0}}, upsert=True)
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
            "_id": str(request_id),
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
        await self._payment_requests.replace_one({"_id": item["_id"]}, item, upsert=True)
        return self._payment_doc_to_dict(item)

    async def get_payment_request(self, request_id: str) -> Optional[dict]:
        return self._payment_doc_to_dict(await self._payment_requests.find_one({"_id": str(request_id).strip()}))

    async def update_payment_request(self, request_id: str, updates: dict) -> Optional[dict]:
        req_id = str(request_id).strip()
        if not req_id:
            return None
        clean: dict[str, object] = {}
        for key, value in (updates or {}).items():
            if key in {"status", "note", "gateway", "plan_type", "qr_code_id", "payment_link", "txn_id", "approved_by", "grant_type", "screenshot_file_id"}:
                clean[key] = str(value or "").strip().lower() if key in {"status", "gateway", "plan_type"} else str(value or "")
            elif key in {"admin_id", "credits", "updated_at", "expires_at"}:
                clean[key] = int(value or 0)
            elif key == "amount_inr":
                clean[key] = float(value or 0.0)
        clean["updated_at"] = int(time.time())
        doc = await self._payment_requests.find_one_and_update(
            {"_id": req_id},
            {"$set": clean},
            return_document=ReturnDocument.AFTER,
        )
        return self._payment_doc_to_dict(doc)

    async def set_payment_request_status(
        self,
        request_id: str,
        status: str,
        note: str = "",
        admin_id: int = 0,
    ) -> Optional[dict]:
        doc = await self._payment_requests.find_one_and_update(
            {"_id": str(request_id).strip()},
            {
                "$set": {
                    "status": str(status).strip().lower(),
                    "note": str(note or "").strip(),
                    "admin_id": int(admin_id or 0),
                    "updated_at": int(time.time()),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._payment_doc_to_dict(doc)

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
        allowed = [str(s).strip().lower() for s in (from_statuses or ()) if str(s).strip()]
        if not allowed:
            allowed = ["pending", "submitted"]
        payload: dict[str, object] = {
            "status": str(to_status or "").strip().lower(),
            "note": str(note or "").strip(),
            "admin_id": int(admin_id or 0),
            "updated_at": int(time.time()),
        }
        for key, value in (extra_updates or {}).items():
            if key in {"expires_at"}:
                payload[key] = int(value or 0)
            elif key in {"qr_code_id", "payment_link", "txn_id", "approved_by", "grant_type", "screenshot_file_id", "gateway", "plan_type"}:
                payload[key] = str(value or "").strip().lower() if key in {"gateway", "plan_type"} else str(value or "")
        doc = await self._payment_requests.find_one_and_update(
            {"_id": req_id, "status": {"$in": allowed}},
            {"$set": payload},
            return_document=ReturnDocument.AFTER,
        )
        if doc:
            return self._payment_doc_to_dict(doc), True
        latest = await self.get_payment_request(req_id)
        return latest, False

    async def delete_payment_request(self, request_id: str) -> bool:
        req_id = str(request_id).strip()
        result = await self._payment_requests.delete_one({"_id": req_id})
        await self.clear_payment_messages(req_id)
        try:
            current_seq = await self._counters_col.find_one({"_id": "payment_request_seq"}, {"value": 1})
            req_num = int(req_id)
            seq_num = int((current_seq or {}).get("value", 0) or 0)
            if req_num > 0 and req_num == seq_num:
                await self._counters_col.update_one({"_id": "payment_request_seq"}, {"$inc": {"value": -1}})
        except Exception:
            pass
        return result.deleted_count > 0

    async def list_payment_requests(self, status: str = "all", limit: int = 20) -> list[dict]:
        status = (status or "all").strip().lower()
        query = {} if status == "all" else {"status": status}
        rows: list[dict] = []
        cursor = self._payment_requests.find(query).sort("created_at", DESCENDING).limit(max(1, int(limit)))
        async for row in cursor:
            item = self._payment_doc_to_dict(row)
            if item:
                rows.append(item)
        return rows

    async def get_user_active_payment_request(
        self,
        user_id: int,
        statuses: tuple[str, ...] = ("pending", "submitted"),
        scan_limit: int = 1000,
    ) -> Optional[dict]:
        allowed = [str(s).strip().lower() for s in (statuses or ()) if str(s).strip()]
        if not allowed:
            allowed = ["pending", "submitted"]
        row = await self._payment_requests.find_one(
            {"user_id": int(user_id), "status": {"$in": allowed}},
            sort=[("created_at", DESCENDING)],
        )
        return self._payment_doc_to_dict(row)

    async def pending_xwallet_orders(self, limit: int = 1000) -> list[dict]:
        now = int(time.time())
        rows: list[dict] = []
        cursor = (
            self._payment_requests.find(
                {
                    "gateway": "xwallet",
                    "status": {"$in": ["pending", "processing"]},
                    "expires_at": {"$gt": now},
                }
            )
            .sort("created_at", DESCENDING)
            .limit(max(1, int(limit)))
        )
        async for row in cursor:
            item = self._payment_doc_to_dict(row)
            if item:
                rows.append(item)
        return rows

    async def list_known_user_ids(self, limit: int = 50000) -> list[int]:
        limit = max(1, int(limit))
        users: set[int] = set()
        cursor = self._credits_col.find({}, {"_id": 1})
        async for row in cursor:
            users.add(int(row["_id"]))
            if len(users) >= limit:
                return sorted(users)
        for uid in await self._payment_requests.distinct("user_id"):
            try:
                users.add(int(uid))
            except Exception:
                continue
            if len(users) >= limit:
                break
        return sorted(users)

    async def set_section(self, section_name: Optional[str]) -> Optional[str]:
        if not section_name:
            await self._config_col.delete_one({"_id": "current_section"})
            return None

        normalized = _normalize_section(section_name)
        section_id = _slugify(section_name)
        if await self.section_exists(section_name):
            return None
        if await self.section_id_exists(section_id):
            return None

        try:
            await self._sections_col.insert_one(
                {"_id": section_id, "name": section_name, "normalized": normalized, "created_at": int(time.time())}
            )
        except DuplicateKeyError:
            return None
        await self._config_col.update_one(
            {"_id": "current_section"},
            {"$set": {"section_id": section_id, "section_name": section_name}},
            upsert=True,
        )
        return section_id

    async def get_section(self) -> tuple[Optional[str], Optional[str]]:
        doc = await self._config_col.find_one({"_id": "current_section"})
        if not doc:
            return None, None
        return (doc.get("section_id") or None, doc.get("section_name") or None)

    async def set_home_section(self, section_id: Optional[str], section_name: Optional[str] = None) -> None:
        if not section_id:
            await self._config_col.delete_one({"_id": "home_section"})
            return
        await self._config_col.update_one(
            {"_id": "home_section"},
            {"$set": {"section_id": section_id, "section_name": section_name or section_id}},
            upsert=True,
        )

    async def get_home_section(self) -> tuple[Optional[str], Optional[str]]:
        doc = await self._config_col.find_one({"_id": "home_section"})
        if not doc:
            return None, None
        return (doc.get("section_id") or None, doc.get("section_name") or None)

    async def section_exists(self, section_name: str) -> bool:
        normalized = _normalize_section(section_name)
        return await self._sections_col.count_documents({"normalized": normalized}, limit=1) > 0

    async def section_id_exists(self, section_id: str) -> bool:
        return await self._sections_col.count_documents({"_id": section_id}, limit=1) > 0

    async def list_sections(self) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = []
        cursor = self._sections_col.find({}, {"name": 1}).sort("name", ASCENDING)
        async for row in cursor:
            rows.append((str(row.get("name", "") or ""), str(row.get("_id", "") or "")))
        return rows

    async def set_public_section(self, section_id: str, section_name: str, is_public: bool = True) -> None:
        sid = str(section_id or "").strip()
        if not sid:
            return
        await self._sections_col.update_one(
            {"_id": sid},
            {"$set": {"is_public": bool(is_public), "name": section_name or sid}},
        )

    async def list_public_sections(self) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = []
        cursor = self._sections_col.find({"is_public": True}, {"name": 1}).sort("name", ASCENDING)
        async for row in cursor:
            rows.append((str(row.get("name", "") or ""), str(row.get("_id", "") or "")))
        return rows

    async def add_trending_item(self, item: dict) -> dict:
        item_id = str(item.get("id") or secrets.token_urlsafe(8)).strip()
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
            "_id": item_id,
            "bar": str(item.get("bar", "") or "").strip(),
            "title": str(item.get("title", "") or "").strip(),
            "description": str(item.get("description", "") or "").strip(),
            "media": media_items,
            "media_file_id": first_media["file_id"],
            "media_type": first_media["type"],
            "normal_link": str(item.get("normal_link", "") or "").strip(),
            "premium_link": str(item.get("premium_link", "") or "").strip(),
            "created_at": float(item.get("created_at") or time.time()),
            "created_by": int(item.get("created_by", 0) or 0),
        }
        await self._trending_items.replace_one({"_id": item_id}, payload, upsert=True)
        return {"id": item_id, **{k: v for k, v in payload.items() if k != "_id"}}

    async def get_trending_item(self, item_id: str) -> Optional[dict]:
        item_id = str(item_id or "").strip()
        if not item_id:
            return None
        doc = await self._trending_items.find_one({"_id": item_id})
        if not doc:
            return None
        return {"id": str(doc.get("_id", "")), **{k: v for k, v in doc.items() if k != "_id"}}

    async def list_trending_items(self, limit: int = 100) -> list[dict]:
        rows: list[dict] = []
        cursor = self._trending_items.find({}).sort("created_at", DESCENDING).limit(max(int(limit), 1))
        async for doc in cursor:
            rows.append({"id": str(doc.get("_id", "")), **{k: v for k, v in doc.items() if k != "_id"}})
        return rows

    async def delete_trending_item(self, item_id: str) -> bool:
        item_id = str(item_id or "").strip()
        if not item_id:
            return False
        result = await self._trending_items.delete_one({"_id": item_id})
        return bool(result.deleted_count)

    async def delete_section(self, section_name: str) -> bool:
        normalized = _normalize_section(section_name)
        row = await self._sections_col.find_one({"normalized": normalized}, {"_id": 1})
        if not row:
            return False
        section_id = str(row["_id"])
        await self._sections_col.delete_one({"_id": section_id})
        await self._section_metrics.delete_one({"_id": section_id})
        await self._section_viewers.delete_many({"section_id": section_id})
        current = await self.get_section()
        if current and current[0] == section_id:
            await self._config_col.delete_one({"_id": "current_section"})
        home = await self.get_home_section()
        if home and home[0] == section_id:
            await self._config_col.delete_one({"_id": "home_section"})
        return True

    async def list_section(self, section_id: str, limit: int) -> list[str]:
        limit = max(int(limit), 1)
        cursor = self._tokens.find(self._live_filter({"section_id": section_id}), {"_id": 1}).sort("created_at", DESCENDING).limit(limit)
        return [str(row["_id"]) async for row in cursor]
