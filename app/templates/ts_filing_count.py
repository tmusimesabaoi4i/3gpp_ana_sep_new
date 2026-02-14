"""
templates/ts_filing_count.py  –  ANA-A: 出願数時系列 (Filing Count TS)

フロー: scope → sel_filing_count_ts → export → cleanup

出力: country, company, bucket, filing_count
unique 不要（COUNT(DISTINCT PATT_APPLICATION_NUMBER) で二重カウント防止）
"""
from __future__ import annotations

from app.core.plan import Plan
from app.core.types import JobSpec, OutputSpec
from app.templates.base import TemplateBuilder


class TsFilingCountBuilder(TemplateBuilder):
    def name(self) -> str:
        return "ts_filing_count"

    def build(self, job: JobSpec) -> tuple[Plan, list[OutputSpec]]:
        plan = Plan(job_id=job.job_id)
        extra = job.extra or {}
        countries = extra.get("analysis_countries", ["JP", "US", "CN", "EP", "KR"])
        include_all = extra.get("include_all", True)
        period = job.time_bucket.period if job.time_bucket.period in ("month", "year") else "month"

        # 1. scope (企業フィルタ)
        plan.add("scope", save_as="scope", scope_spec=job.scope)

        # 2. sel_filing_count_ts
        plan.add(
            "sel_filing_count_ts", save_as="sel__A",
            source="scope", period=period,
            countries=countries, include_all=include_all,
        )

        # 3. cleanup
        plan.add("cleanup")

        out_csv = extra.get("out_csv") or f"{job.job_id}.csv"
        outputs = [OutputSpec(select_ref="sel__A", format="csv", filename=out_csv)]
        return plan, outputs
