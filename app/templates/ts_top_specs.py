"""
templates/ts_top_specs.py  –  ANA-C: TGPP_NUMBER TopK 時系列

フロー: scope → sel_top_specs_ts → export → cleanup

出力: country, company, bucket, TGPP_NUMBER, cnt, rank
"""
from __future__ import annotations

from app.core.plan import Plan
from app.core.types import JobSpec, OutputSpec
from app.templates.base import TemplateBuilder


class TsTopSpecsBuilder(TemplateBuilder):
    def name(self) -> str:
        return "ts_top_specs"

    def build(self, job: JobSpec) -> tuple[Plan, list[OutputSpec]]:
        plan = Plan(job_id=job.job_id)
        extra = job.extra or {}
        countries = extra.get("analysis_countries", ["JP", "US", "CN", "EP", "KR"])
        include_all = extra.get("include_all", True)
        top_k = extra.get("top_k", 10)
        period = job.time_bucket.period if job.time_bucket.period in ("month", "year") else "month"

        # 1. scope
        plan.add("scope", save_as="scope", scope_spec=job.scope)

        # 2. sel_top_specs_ts
        plan.add(
            "sel_top_specs_ts", save_as="sel__C",
            source="scope", period=period,
            countries=countries, include_all=include_all,
            top_k=top_k,
        )

        # 3. cleanup
        plan.add("cleanup")

        out_csv = extra.get("out_csv") or f"{job.job_id}.csv"
        outputs = [OutputSpec(select_ref="sel__C", format="csv", filename=out_csv)]
        return plan, outputs
