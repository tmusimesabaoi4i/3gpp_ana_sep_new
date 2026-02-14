"""
templates/heat_spec_company.py  –  ANA-E: Spec×会社 ヒートマップ

フロー: scope → sel_spec_company_heat → export → cleanup

出力: country, TGPP_NUMBER, company, cnt  (縦持ち)
上位 top_k Spec に限定（デフォルト 20）
"""
from __future__ import annotations

from app.core.plan import Plan
from app.core.types import JobSpec, OutputSpec
from app.templates.base import TemplateBuilder


class HeatSpecCompanyBuilder(TemplateBuilder):
    def name(self) -> str:
        return "heat_spec_company"

    def build(self, job: JobSpec) -> tuple[Plan, list[OutputSpec]]:
        plan = Plan(job_id=job.job_id)
        extra = job.extra or {}
        countries = extra.get("analysis_countries", ["JP", "US", "CN", "EP", "KR"])
        include_all = extra.get("include_all", True)
        top_k = extra.get("top_k", 20)

        # 1. scope
        plan.add("scope", save_as="scope", scope_spec=job.scope)

        # 2. sel_spec_company_heat
        plan.add(
            "sel_spec_company_heat", save_as="sel__E",
            source="scope",
            countries=countries, include_all=include_all,
            top_k=top_k,
        )

        # 3. cleanup
        plan.add("cleanup")

        out_csv = extra.get("out_csv") or f"{job.job_id}.csv"
        outputs = [OutputSpec(select_ref="sel__E", format="csv", filename=out_csv)]
        return plan, outputs
