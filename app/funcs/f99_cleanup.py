"""
funcs/f99_cleanup.py  –  CleanupFunc

TEMP テーブルの削除 (best-effort)
"""
from __future__ import annotations

from typing import Any

from app.funcs.base import BaseFunc, ExecutionContext, FuncResult, FuncSignature


class CleanupFunc(BaseFunc):
    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="cleanup",
            required_args=[],
            optional_args=[],
            produces="temp",
            description="TEMP 削除 (best-effort)",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        temps = ctx.all_temps()
        stmts = [f"DROP TABLE IF EXISTS [{t}];" for t in temps]
        sql = "\n".join(stmts) if stmts else "SELECT 1;"  # no-op
        return FuncResult(sql=sql, params=[], description=f"cleanup: {len(temps)} temps")
