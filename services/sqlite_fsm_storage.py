from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

from aiogram.fsm.storage.base import BaseStorage, StorageKey


def _state_id(state: Any) -> str | None:
    if state is None:
        return None
    if isinstance(state, str):
        return state
    sid = getattr(state, "state", None)
    return str(sid) if sid is not None else str(state)


class SQLiteFSMStorage(BaseStorage):
    def __init__(self, path: str):
        self._path = Path(path)
        self._lock = asyncio.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self._path))

    def _init_db(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fsm_state (
                skey TEXT PRIMARY KEY,
                state TEXT,
                data_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _key(key: StorageKey) -> str:
        return "|".join(
            [
                str(key.bot_id),
                str(key.chat_id),
                str(key.user_id),
                str(getattr(key, "thread_id", "") or ""),
                str(getattr(key, "business_connection_id", "") or ""),
                str(getattr(key, "destiny", "") or ""),
            ]
        )

    async def set_state(self, key: StorageKey, state: Any = None) -> None:
        skey = self._key(key)
        state_str = _state_id(state)
        async with self._lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO fsm_state (skey, state, data_json, updated_at)
                VALUES (?, ?, COALESCE((SELECT data_json FROM fsm_state WHERE skey = ?), '{}'), CURRENT_TIMESTAMP)
                ON CONFLICT(skey) DO UPDATE SET state = excluded.state, updated_at = CURRENT_TIMESTAMP
                """,
                (skey, state_str, skey),
            )
            if state_str is None:
                cur.execute("DELETE FROM fsm_state WHERE skey = ? AND COALESCE(data_json, '{}') = '{}'", (skey,))
            conn.commit()
            conn.close()

    async def get_state(self, key: StorageKey) -> str | None:
        skey = self._key(key)
        async with self._lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute("SELECT state FROM fsm_state WHERE skey = ?", (skey,))
            row = cur.fetchone()
            conn.close()
            return row[0] if row else None

    async def set_data(self, key: StorageKey, data: dict[str, Any]) -> None:
        skey = self._key(key)
        payload = json.dumps(data or {}, ensure_ascii=False)
        async with self._lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO fsm_state (skey, state, data_json, updated_at)
                VALUES (?, COALESCE((SELECT state FROM fsm_state WHERE skey = ?), NULL), ?, CURRENT_TIMESTAMP)
                ON CONFLICT(skey) DO UPDATE SET data_json = excluded.data_json, updated_at = CURRENT_TIMESTAMP
                """,
                (skey, skey, payload),
            )
            conn.commit()
            conn.close()

    async def get_data(self, key: StorageKey) -> dict[str, Any]:
        skey = self._key(key)
        async with self._lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute("SELECT data_json FROM fsm_state WHERE skey = ?", (skey,))
            row = cur.fetchone()
            conn.close()
            if not row or not row[0]:
                return {}
            try:
                return json.loads(row[0])
            except Exception:
                return {}

    async def update_data(self, key: StorageKey, data: dict[str, Any]) -> dict[str, Any]:
        current = await self.get_data(key)
        current.update(data or {})
        await self.set_data(key, current)
        return current

    async def close(self) -> None:
        return None
