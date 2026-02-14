"""
funcs/base.py  –  Func の基底クラスと FuncSignature
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class FuncSignature:
    """Func の型情報"""
    name: str
    required_args: list[str] = field(default_factory=list)
    optional_args: list[str] = field(default_factory=list)
    produces: str = "temp"        # "temp" | "select"
    columns: list[str] = field(default_factory=list)  # select の場合の出力列
    description: str = ""


@dataclass
class FuncResult:
    """Func の実行結果"""
    sql: str
    params: list[Any] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    description: str = ""
    row_count: Optional[int] = None


class ExecutionContext:
    """Func 実行時のコンテキスト"""

    def __init__(self, run_id: str, job_id: str):
        self.run_id = run_id
        self.job_id = job_id
        # 論理名 → 物理名マッピング
        self._temp_map: dict[str, str] = {}
        self._step_counter: int = 0

    def allocate_temp(self, logical_name: str) -> str:
        """論理名に対する物理 TEMP テーブル名を払い出す"""
        self._step_counter += 1
        physical = f"tmp__{self.run_id}__{self.job_id}__{self._step_counter:02d}__{logical_name}"
        self._temp_map[logical_name] = physical
        return physical

    def resolve_temp(self, logical_name: str) -> str:
        """論理名から物理名を解決"""
        if logical_name == "isld_pure":
            return "isld_pure"
        phy = self._temp_map.get(logical_name)
        if phy is None:
            raise KeyError(f"TEMP 論理名 '{logical_name}' は未割当です")
        return phy

    def all_temps(self) -> list[str]:
        """作成済み TEMP 物理名の一覧"""
        return list(self._temp_map.values())


class BaseFunc(ABC):
    """Func の基底"""

    @abstractmethod
    def signature(self) -> FuncSignature:
        ...

    @abstractmethod
    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        """SQL を構築して返す"""
        ...
