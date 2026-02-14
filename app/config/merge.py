"""
config/merge.py  –  deep_merge ユーティリティ

- dict 同士は再帰マージ
- list は override で置換
- None (null) は上書きとして扱う
"""
from __future__ import annotations

from typing import Any


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    base に override をディープマージした新 dict を返す。
    base は変更しない（コピー）。

    ルール:
      - dict 同士 → 再帰マージ
      - list → override 側で完全置換
      - None → 上書き (明示的 null)
      - それ以外 → override 側で置換
    """
    result = dict(base)  # shallow copy of base

    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = deep_merge(result[key], val)
        else:
            result[key] = val

    return result
