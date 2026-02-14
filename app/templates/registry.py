"""
templates/registry.py  –  TemplateRegistry
"""
from __future__ import annotations

from app.templates.base import TemplateBuilder


class TemplateRegistry:
    """template 名 → TemplateBuilder マッピング"""

    def __init__(self) -> None:
        self._builders: dict[str, TemplateBuilder] = {}

    def register(self, builder: TemplateBuilder) -> None:
        self._builders[builder.name()] = builder

    def get(self, name: str) -> TemplateBuilder:
        b = self._builders.get(name)
        if b is None:
            raise KeyError(f"Template '{name}' は未登録です。登録済み: {list(self._builders.keys())}")
        return b

    def names(self) -> list[str]:
        return list(self._builders.keys())


def create_default_registry() -> TemplateRegistry:
    """標準テンプレート (A〜E) を登録済みの TemplateRegistry を返す"""
    from app.templates.ts_filing_count import TsFilingCountBuilder
    from app.templates.ts_lag_stats import TsLagStatsBuilder
    from app.templates.ts_top_specs import TsTopSpecsBuilder
    from app.templates.rank_company_counts import RankCompanyCountsBuilder
    from app.templates.heat_spec_company import HeatSpecCompanyBuilder

    reg = TemplateRegistry()
    reg.register(TsFilingCountBuilder())       # A: 出願数時系列
    reg.register(TsLagStatsBuilder())          # B: lag分布サマリ
    reg.register(TsTopSpecsBuilder())          # C: TopSpec時系列
    reg.register(RankCompanyCountsBuilder())   # D: 企業別ランキング
    reg.register(HeatSpecCompanyBuilder())     # E: Spec×会社ヒートマップ
    return reg
