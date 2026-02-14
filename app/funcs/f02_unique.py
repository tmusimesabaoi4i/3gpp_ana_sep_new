"""
funcs/f02_unique.py  –  UniqueFunc

ROW_NUMBER 戦略で一意化: tmp_scope → tmp_uq
"""
from __future__ import annotations

from typing import Any

from app.funcs.base import BaseFunc, ExecutionContext, FuncResult, FuncSignature
from app.core.types import UniqueSpec

# unit → パーティションキー列マップ
UNIT_KEY_MAP: dict[str, str] = {
    "publ": "PUBL_NUMBER",
    "app": "PATT_APPLICATION_NUMBER",
    "family": "DIPG_PATF_ID",
    "dipg": "DIPG_ID",
}


class UniqueFunc(BaseFunc):
    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="unique",
            required_args=["unique_spec", "source"],
            optional_args=[],
            produces="temp",
            description="一意化 (ROW_NUMBER) → TEMP",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        spec: UniqueSpec = args["unique_spec"]
        source = args["source"]
        source_table = ctx.resolve_temp(source)
        out_table = ctx.allocate_temp("uq")

        # unit=none → スキップ (コピー)
        if spec.unit == "none":
            sql = f"CREATE TEMP TABLE [{out_table}] AS SELECT * FROM [{source_table}];"
            return FuncResult(sql=sql, params=[], description=f"unique(none) → copy")

        key_col = UNIT_KEY_MAP.get(spec.unit)
        if not key_col:
            raise ValueError(f"Unknown unique unit: {spec.unit}")

        # PARTITION BY
        partition_cols = [key_col] + spec.partition_extra
        partition_expr = ", ".join(partition_cols)

        # ORDER BY
        order_parts: list[str] = []
        for ob in spec.keep.order_by:
            order_parts.append(f"{ob.col} {ob.dir}")
        # tie-break: __src_rownum ASC (固定)
        if "__src_rownum" not in [ob.col for ob in spec.keep.order_by]:
            order_parts.append("__src_rownum ASC")
        order_expr = ", ".join(order_parts)

        sql = f"""CREATE TEMP TABLE [{out_table}] AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY {partition_expr} ORDER BY {order_expr}) AS __rn
    FROM [{source_table}]
    WHERE {key_col} IS NOT NULL
)
WHERE __rn = 1;"""

        return FuncResult(sql=sql, params=[], description=f"unique({spec.unit}) → {out_table}")
