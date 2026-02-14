"""
templates/rank_company_counts.py  –  ANA-D: 企業別ランキング

フロー: scope → sel_company_rank → export → cleanup

出力: country, unique_unit, company, cnt, rank
unique_unit は job.unique.unit で切替（app/family/publ/dipg）
"""
from __future__ import annotations

from app.core.plan import Plan
from app.core.types import JobSpec, OutputSpec
from app.templates.base import TemplateBuilder

# unit → COUNT(DISTINCT column) mapping
_UNIT_COL_MAP = {
    "app": "PATT_APPLICATION_NUMBER",
    "family": "DIPG_PATF_ID",
    "publ": "PUBL_NUMBER",
    "dipg": "DIPG_ID",
}


class RankCompanyCountsBuilder(TemplateBuilder):
    def name(self) -> str:
        return "rank_company_counts"

    def build(self, job: JobSpec) -> tuple[Plan, list[OutputSpec]]:
        plan = Plan(job_id=job.job_id)
        extra = job.extra or {}
        countries = extra.get("analysis_countries", ["JP", "US", "CN", "EP", "KR"])
        include_all = extra.get("include_all", True)
        unit = job.unique.unit if job.unique.unit != "none" else "app"
        unit_col = _UNIT_COL_MAP.get(unit, "PATT_APPLICATION_NUMBER")

        # 1. scope
        plan.add("scope", save_as="scope", scope_spec=job.scope)

        # 2. sel_company_rank
        plan.add(
            "sel_company_rank", save_as="sel__D",
            source="scope",
            countries=countries, include_all=include_all,
            unit_col=unit_col, unit_name=unit,
        )

        # 3. cleanup
        plan.add("cleanup")

        out_csv = extra.get("out_csv") or f"{job.job_id}.csv"
        outputs = [OutputSpec(select_ref="sel__D", format="csv", filename=out_csv)]
        return plan, outputs
