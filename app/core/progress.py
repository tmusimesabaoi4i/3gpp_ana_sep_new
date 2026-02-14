"""
core/progress.py  –  ASCII ベースの進捗表示

normalization 時: rows / invalid_date / invalid_bool 等
flow 時: step 名 / 出力ファイル名 / 件数
"""
from __future__ import annotations

import sys
import time
from typing import Optional


class AsciiProgress:
    """シンプルな ASCII 進捗表示"""

    def __init__(self, enabled: bool = True):
        self._enabled = enabled
        self._phase: str = ""
        self._t0: float = 0.0
        self._last_print: float = 0.0

    # ──────── phase 管理 ────────
    def start(self, phase: str) -> None:
        self._phase = phase
        self._t0 = time.time()
        self._last_print = 0.0
        self._print(f"[{phase}] 開始...")

    def finish(self, message: str = "") -> None:
        elapsed = time.time() - self._t0
        msg = message or "完了"
        self._print(f"[{self._phase}] {msg}  ({elapsed:.1f}s)")

    # ──────── step (flow 用) ────────
    def step(self, name: str, detail: str = "") -> None:
        msg = f"  → {name}"
        if detail:
            msg += f"  {detail}"
        self._print(msg)

    # ──────── update (normalization 用) ────────
    def update(self, rows: int, **kwargs: int) -> None:
        """throttle 付き進捗更新 (0.5 秒間隔)"""
        now = time.time()
        if now - self._last_print < 0.5:
            return
        self._last_print = now
        parts = [f"rows={rows:,}"]
        for k, v in kwargs.items():
            if v > 0:
                parts.append(f"{k}={v:,}")
        self._print(f"  ... {', '.join(parts)}", end="\r")

    def update_final(self, rows: int, **kwargs: int) -> None:
        """最終行 (改行付き)"""
        parts = [f"rows={rows:,}"]
        for k, v in kwargs.items():
            parts.append(f"{k}={v:,}")
        self._print(f"  ... {', '.join(parts)}")

    # ──────── internal ────────
    def _print(self, msg: str, end: str = "\n") -> None:
        if self._enabled:
            sys.stderr.write(msg + end)
            sys.stderr.flush()
