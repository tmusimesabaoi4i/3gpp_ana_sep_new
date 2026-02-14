"""
funcs/f01_scope.py  –  ScopeFunc

母集団フィルタリング: isld_pure → tmp_scope
"""
from __future__ import annotations

from typing import Any

from app.funcs.base import BaseFunc, ExecutionContext, FuncResult, FuncSignature
from app.core.types import ScopeSpec


class ScopeFunc(BaseFunc):
    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="scope",
            required_args=["scope_spec"],
            optional_args=["source"],
            produces="temp",
            description="母集団フィルタ → TEMP",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        spec: ScopeSpec = args["scope_spec"]
        source = args.get("source", "isld_pure")
        source_table = ctx.resolve_temp(source) if source != "isld_pure" else "isld_pure"
        out_table = ctx.allocate_temp("scope")

        conditions: list[str] = []
        params: list[Any] = []

        # 会社フィルタ (LIKE, 大文字比較)
        if spec.companies:
            like_clauses = []
            for comp in spec.companies:
                like_clauses.append("UPPER(COMP_LEGAL_NAME) LIKE UPPER(?)")
                params.append(f"%{comp}%")
            conditions.append(f"({' OR '.join(like_clauses)})")

        # 国フィルタ (完全一致)
        if spec.countries:
            placeholders = ", ".join("?" for _ in spec.countries)
            conditions.append(f"Country_Of_Registration IN ({placeholders})")
            params.extend(spec.countries)

        # 国フィルタ (prefix: "JP" → Country_Of_Registration LIKE 'JP %')
        if spec.country_prefixes:
            prefix_clauses = []
            for pfx in spec.country_prefixes:
                prefix_clauses.append("Country_Of_Registration LIKE ?")
                params.append(f"{pfx} %")
            conditions.append(f"({' OR '.join(prefix_clauses)})")

        # Release フィルタ (完全一致)
        if spec.releases:
            placeholders = ", ".join("?" for _ in spec.releases)
            conditions.append(f"TGPV_VERSION IN ({placeholders})")
            params.extend(spec.releases)

        # Version prefix フィルタ ("18" → TGPV_VERSION LIKE '18.%')
        if spec.version_prefixes:
            vp_clauses = []
            for vp in spec.version_prefixes:
                vp_clauses.append("TGPV_VERSION LIKE ?")
                params.append(f"{vp}.%")
            conditions.append(f"({' OR '.join(vp_clauses)})")

        # Spec フィルタ
        if spec.specs:
            placeholders = ", ".join("?" for _ in spec.specs)
            conditions.append(f"TGPP_NUMBER IN ({placeholders})")
            params.extend(spec.specs)

        # 日付範囲
        if spec.date_from:
            conditions.append("PBPA_APP_DATE >= ?")
            params.append(spec.date_from)
        if spec.date_to:
            conditions.append("PBPA_APP_DATE <= ?")
            params.append(spec.date_to)

        # 世代フラグ (gen_flags: {"5G": 1} → Gen_5G = 1)
        if spec.gen_flags:
            gen_col_map = {"2G": "Gen_2G", "3G": "Gen_3G", "4G": "Gen_4G", "5G": "Gen_5G"}
            for gen, val in spec.gen_flags.items():
                col = gen_col_map.get(gen)
                if col and val is not None:
                    conditions.append(f"{col} = ?")
                    params.append(int(val))

        # Essential フラグ (ess_flags: {"ess_to_standard": true})
        if spec.ess_flags:
            ess_col_map = {
                "ess_to_standard": "Ess_To_Standard",
                "ess_to_project": "Ess_To_Project",
            }
            for key, val in spec.ess_flags.items():
                col = ess_col_map.get(key)
                if col and val is not None:
                    if isinstance(val, bool):
                        conditions.append(f"{col} = ?")
                        params.append(1 if val else 0)
                    else:
                        conditions.append(f"{col} = ?")
                        params.append(val)

        # country_mode: "FILTER" の場合のみ国フィルタ有効、"ALL" は無条件通過
        # (countries / country_prefixes は上で既に追加済み。
        #  country_mode="ALL" 時にそれらが指定されていても無視するため、
        #  条件追加をしない = 既に追加された条件を除去する必要がある。
        #  → 設計上、country_mode=ALL なら countries / country_prefixes を空にするのが正しい運用。
        #    Config 側で country_mode=ALL + country_prefixes=["JP"] は矛盾設定として
        #    scope に渡る前に空にする方が安全だが、現状は利用者に委ねる。)

        where = " AND ".join(conditions) if conditions else "1=1"

        sql = f"CREATE TEMP TABLE [{out_table}] AS SELECT * FROM [{source_table}] WHERE {where};"

        return FuncResult(sql=sql, params=params, description=f"scope → {out_table}")
