"""
config/loader.py  –  config.json の読み込み
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.core.types import ConfigError


class ConfigLoader:
    """JSON 設定ファイルを読み込む"""

    @staticmethod
    def load(path: str | Path) -> dict[str, Any]:
        """
        config.json を読んで dict を返す。

        Raises
        ------
        ConfigError
            ファイル不在 / JSON パースエラー
        """
        p = Path(path)
        if not p.exists():
            raise ConfigError(f"設定ファイルが見つかりません: {p}", path=str(p))

        text = p.read_text(encoding="utf-8")
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise ConfigError(
                f"JSON パースエラー (line {e.lineno}, col {e.colno}): {e.msg}",
                path=str(p),
            ) from e

        if not isinstance(data, dict):
            raise ConfigError("トップレベルは object でなければなりません", path=str(p))

        return data
