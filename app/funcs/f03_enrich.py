"""
funcs/f03_enrich.py  –  EnrichFunc

解釈派生列を TEMP で生成:
  - decl_date (COALESCE 戦略)
  - lag_days (JULIANDAY 差)
  - release_num (仮ルール)
  - time_bucket (month/quarter/year/fiscal_year)
"""
from __future__ import annotations

from typing import Any

from app.funcs.base import BaseFunc, ExecutionContext, FuncResult, FuncSignature
from app.core.types import PolicySpec, TimeBucketSpec


class EnrichFunc(BaseFunc):
    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="enrich",
            required_args=["source", "policies"],
            optional_args=["enable_lag", "enable_release", "enable_time_bucket", "time_bucket_spec"],
            produces="temp",
            description="解釈派生列 → TEMP",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        source = args["source"]
        source_table = ctx.resolve_temp(source)
        out_table = ctx.allocate_temp("enriched")

        policies: PolicySpec = args["policies"]
        enable_lag = args.get("enable_lag", True)
        enable_release = args.get("enable_release", False)
        enable_time_bucket = args.get("enable_time_bucket", False)
        tb_spec: TimeBucketSpec = args.get("time_bucket_spec", TimeBucketSpec())

        extra_cols: list[str] = []

        # ─── decl_date ───
        # sentinel date ('1900-01-01') を NULL として扱う
        sig_safe = "NULLIF(IPRD_SIGNATURE_DATE, '1900-01-01')"
        ref_safe = "NULLIF(Reflected_Date, '1900-01-01')"
        if policies.decl_date_policy == "reflected_first":
            decl_expr = f"COALESCE({ref_safe}, {sig_safe})"
        else:
            decl_expr = f"COALESCE({sig_safe}, {ref_safe})"
        extra_cols.append(f"{decl_expr} AS decl_date")

        # ─── lag_days ───
        if enable_lag:
            lag_raw = f"JULIANDAY({decl_expr}) - JULIANDAY(PBPA_APP_DATE)"
            if policies.negative_lag_policy == "zero":
                lag_expr = f"MAX(0, {lag_raw})"
            elif policies.negative_lag_policy == "null":
                lag_expr = f"CASE WHEN ({lag_raw}) < 0 THEN NULL ELSE ({lag_raw}) END"
            elif policies.negative_lag_policy == "drop":
                # drop は後段で WHERE lag_days >= 0 する想定
                lag_expr = f"({lag_raw})"
            else:  # "keep"
                lag_expr = f"({lag_raw})"
            extra_cols.append(f"{lag_expr} AS lag_days")

        # ─── release_num ───
        if enable_release:
            # 仮ルール: TGPV_VERSION の先頭数値部分を取る
            # e.g., "16.0.0" → 16, "Rel-16" → 16
            release_expr = """CAST(
                CASE
                    WHEN TGPV_VERSION GLOB '[0-9]*'
                        THEN SUBSTR(TGPV_VERSION, 1,
                             CASE
                                 WHEN INSTR(TGPV_VERSION, '.') > 0
                                 THEN INSTR(TGPV_VERSION, '.') - 1
                                 ELSE LENGTH(TGPV_VERSION)
                             END)
                    WHEN UPPER(TGPV_VERSION) LIKE 'REL-%'
                        THEN SUBSTR(TGPV_VERSION, 5,
                             CASE
                                 WHEN INSTR(SUBSTR(TGPV_VERSION, 5), '.') > 0
                                 THEN INSTR(SUBSTR(TGPV_VERSION, 5), '.') - 1
                                 ELSE LENGTH(SUBSTR(TGPV_VERSION, 5))
                             END)
                    ELSE NULL
                END AS INTEGER)"""
            extra_cols.append(f"{release_expr} AS release_num")

        # ─── time_bucket ───
        if enable_time_bucket:
            period = tb_spec.period
            if period == "month":
                tb_expr = f"STRFTIME('%Y-%m', {decl_expr})"
            elif period == "quarter":
                tb_expr = (
                    f"STRFTIME('%Y', {decl_expr}) || '-Q' || "
                    f"CAST((CAST(STRFTIME('%m', {decl_expr}) AS INTEGER) + 2) / 3 AS TEXT)"
                )
            elif period == "fiscal_year":
                # 会計年度: 4月始まり想定
                tb_expr = (
                    f"CASE WHEN CAST(STRFTIME('%m', {decl_expr}) AS INTEGER) >= 4 "
                    f"THEN STRFTIME('%Y', {decl_expr}) "
                    f"ELSE CAST(CAST(STRFTIME('%Y', {decl_expr}) AS INTEGER) - 1 AS TEXT) "
                    f"END || '-FY'"
                )
            else:  # year
                tb_expr = f"STRFTIME('%Y', {decl_expr})"
            extra_cols.append(f"{tb_expr} AS time_bucket")

        extra_select = ", ".join(extra_cols)
        sql = f"CREATE TEMP TABLE [{out_table}] AS SELECT *, {extra_select} FROM [{source_table}];"

        return FuncResult(sql=sql, params=[], description=f"enrich → {out_table}")
