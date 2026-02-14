"""
funcs/library.py  –  FuncLibrary (Func レジストリ)
"""
from __future__ import annotations

from typing import Optional

from app.funcs.base import BaseFunc


class FuncLibrary:
    """Func 名 → BaseFunc インスタンスのレジストリ"""

    def __init__(self) -> None:
        self._funcs: dict[str, BaseFunc] = {}

    def register(self, func: BaseFunc) -> None:
        sig = func.signature()
        self._funcs[sig.name] = func

    def get(self, name: str) -> BaseFunc:
        f = self._funcs.get(name)
        if f is None:
            raise KeyError(f"Func '{name}' は未登録です")
        return f

    def has(self, name: str) -> bool:
        return name in self._funcs

    def names(self) -> list[str]:
        return list(self._funcs.keys())


def create_default_library() -> FuncLibrary:
    """標準 Func 群を登録済みの FuncLibrary を返す (A〜E 分析用)"""
    from app.funcs.f01_scope import ScopeFunc
    from app.funcs.f02_unique import UniqueFunc
    from app.funcs.f03_enrich import EnrichFunc
    from app.funcs.f10_selects import (
        SelFilingCountTs,
        SelLagStats,
        SelTopSpecsTs,
        SelCompanyRank,
        SelSpecCompanyHeat,
    )
    from app.funcs.f99_cleanup import CleanupFunc

    lib = FuncLibrary()
    lib.register(ScopeFunc())
    lib.register(UniqueFunc())
    lib.register(EnrichFunc())
    lib.register(SelFilingCountTs())      # A: 出願数時系列
    lib.register(SelLagStats())           # B: lag分布サマリ
    lib.register(SelTopSpecsTs())         # C: TopSpec時系列
    lib.register(SelCompanyRank())        # D: 企業別ランキング
    lib.register(SelSpecCompanyHeat())    # E: Spec×会社ヒートマップ
    lib.register(CleanupFunc())
    return lib
