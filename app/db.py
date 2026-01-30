import time
from dataclasses import dataclass
from typing import Optional

import aiosqlite


@dataclass
class PremiumUser:
    user_id: int
    expires_at: Optional[int]


class PremiumDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS premium_users (
                user_id INTEGER PRIMARY KEY,
                expires_at INTEGER
            )
            """
        )
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()

    async def add_user(self, user_id: int, period_days: Optional[int]) -> None:
        expires_at = None
        if period_days is not None:
            expires_at = int(time.time()) + period_days * 86400
        await self._conn.execute(
            "INSERT OR REPLACE INTO premium_users (user_id, expires_at) VALUES (?, ?)",
            (user_id, expires_at),
        )
        await self._conn.commit()

    async def is_premium(self, user_id: int) -> bool:
        row = await self._conn.execute_fetchone(
            "SELECT expires_at FROM premium_users WHERE user_id = ?",
            (user_id,),
        )
        if not row:
            return False
        expires_at = row[0]
        if expires_at is None:
            return True
        return int(time.time()) <= int(expires_at)