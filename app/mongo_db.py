import time
from typing import Optional

from app.db import PremiumDB, PremiumUser

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except Exception:
    AsyncIOMotorClient = None


class MongoPremiumDB(PremiumDB):
    def __init__(self, db_path: str, mongo_uri: str, mongo_db_name: str) -> None:
        super().__init__(db_path)
        self._mongo_uri = str(mongo_uri or "").strip()
        self._mongo_db_name = str(mongo_db_name or "azfileconversion").strip() or "azfileconversion"
        self._client = None
        self._db = None
        self._premium = None
        self._admins = None

    async def connect(self) -> None:
        if not self._mongo_uri:
            raise SystemExit("MongoPremiumDB requires MONGO_URI.")
        if AsyncIOMotorClient is None:
            raise SystemExit("MONGO_URI is configured but motor is not installed.")

        self._client = AsyncIOMotorClient(self._mongo_uri)
        self._db = self._client[self._mongo_db_name]
        self._premium = self._db["premium_users"]
        self._admins = self._db["admins"]

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    async def add_user(self, user_id: int, period_days: Optional[int]) -> None:
        expires_at = None
        if period_days is not None:
            expires_at = int(time.time()) + period_days * 86400
        await self._premium.update_one(
            {"_id": int(user_id)},
            {"$set": {"expires_at": expires_at}},
            upsert=True,
        )

    async def is_premium(self, user_id: int) -> bool:
        row = await self._premium.find_one({"_id": int(user_id)}, {"expires_at": 1})
        if not row:
            return False
        expires_at = row.get("expires_at")
        if expires_at is None:
            return True
        return int(time.time()) <= int(expires_at)

    async def list_premium_users(self) -> list[PremiumUser]:
        rows: list[PremiumUser] = []
        cursor = self._premium.find({}, {"expires_at": 1}).sort("_id", 1)
        async for row in cursor:
            rows.append(PremiumUser(user_id=int(row["_id"]), expires_at=row.get("expires_at")))
        return rows

    async def add_admin(self, user_id: int) -> None:
        await self._admins.update_one(
            {"_id": int(user_id)},
            {"$set": {"created_at": int(time.time())}},
            upsert=True,
        )

    async def list_admins(self) -> list[int]:
        admins: list[int] = []
        cursor = self._admins.find({}, {"_id": 1}).sort("_id", 1)
        async for row in cursor:
            admins.append(int(row["_id"]))
        return admins
