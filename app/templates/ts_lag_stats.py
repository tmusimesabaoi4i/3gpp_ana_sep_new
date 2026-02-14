"""
templates/ts_lag_stats.py  –  ANA-B: lag分布サマリ時系列

フロー: scope → enrich(lag_days) → sel_lag_stats → export → cleanup

出力: country, company, bucket, n, min/q1/median/q3/max lag_days
"""
from __future__ import annotations

from app.core.plan import Plan
from app.core.types import JobSpec, OutputSpec
from app.templates.base import TemplateBuilder


class TsLagStatsBuilder(TemplateBuilder):
    def name(self) -> str:
        return "ts_lag_stats"

    def build(self, job: JobSpec) -> tuple[Plan, list[OutputSpec]]:
        plan = Plan(job_id=job.job_id)
        extra = job.extra or {}
        countries = extra.get("analysis_countries", ["JP", "US", "CN", "EP", "KR"])
        include_all = extra.get("include_all", True)
        period = job.time_bucket.period if job.time_bucket.period in ("month", "year") else "month"

        # 1. scope
        plan.add("scope", save_as="scope", scope_spec=job.scope)

        # 2. enrich (lag_days 生成)
        plan.add(
            "enrich", save_as="enriched",
            source="scope", policies=job.policies,
            enable_lag=True, enable_release=False, enable_time_bucket=False,
        )

        # 3. sel_lag_stats
        plan.add(
            "sel_lag_stats", save_as="sel__B",
            source="enriched", period=period,
            countries=countries, include_all=include_all,
        )

        # 4. cleanup
        plan.add("cleanup")

        out_csv = extra.get("out_csv") or f"{job.job_id}.csv"
        outputs = [OutputSpec(select_ref="sel__B", format="csv", filename=out_csv)]
        return plan, outputs
