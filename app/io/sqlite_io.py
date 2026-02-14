"""
io/sqlite_io.py  –  SQLite の薄いラッパー

- open / execute / query_iter / transaction
- PRAGMA 設定 (journal_mode 等)
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Optional

from app.core.types import SqlError


class SqliteIO:
    """SQLite I/O マネージャ"""

    def __init__(self, db_path: str | Path):
        self._path = str(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    # ──────── lifecycle ────────
    def open(self) -> "SqliteIO":
        """DB 接続を開く"""
        parent = Path(self._path).parent
        parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path)
        # 性能 PRAGMA
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA cache_size=-64000;")  # 64 MB
        self._conn.execute("PRAGMA temp_store=MEMORY;")
        return self

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "SqliteIO":
        return self.open()

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise SqlError("SqliteIO is not open. Call open() first.")
        return self._conn

    # ──────── execute ────────
    def execute(self, sql: str, params: list[Any] | None = None) -> sqlite3.Cursor:
        """単文 SQL 実行"""
        try:
            return self.conn.execute(sql, params or [])
        except sqlite3.Error as e:
            raise SqlError(f"SQL実行エラー: {e}\nSQL: {sql[:500]}") from e

    def executemany(self, sql: str, seq_of_params: list[list[Any]]) -> None:
        """バッチ INSERT 等"""
        try:
            self.conn.executemany(sql, seq_of_params)
        except sqlite3.Error as e:
            raise SqlError(f"SQL executemany エラー: {e}\nSQL: {sql[:500]}") from e

    def executescript(self, script: str) -> None:
        try:
            self.conn.executescript(script)
        except sqlite3.Error as e:
            raise SqlError(f"SQL script エラー: {e}") from e

    # ──────── query ────────
    def query_one(self, sql: str, params: list[Any] | None = None) -> Optional[tuple]:
        cur = self.execute(sql, params)
        return cur.fetchone()

    def query_all(self, sql: str, params: list[Any] | None = None) -> list[tuple]:
        cur = self.execute(sql, params)
        return cur.fetchall()

    def query_iter(
        self, sql: str, params: list[Any] | None = None, chunk_size: int = 5000
    ) -> Generator[list[tuple], None, None]:
        """逐次読み出しジェネレータ（メモリ常駐回避）"""
        cur = self.execute(sql, params)
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            yield rows

    def query_columns(self, sql: str, params: list[Any] | None = None) -> list[str]:
        """SELECT のカラム名リストを返す"""
        cur = self.execute(sql, params)
        return [desc[0] for desc in cur.description] if cur.description else []

    # ──────── transaction ────────
    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """明示的トランザクション"""
        self.conn.execute("BEGIN;")
        try:
            yield
            self.conn.execute("COMMIT;")
        except Exception:
            self.conn.execute("ROLLBACK;")
            raise

    def commit(self) -> None:
        self.conn.commit()
