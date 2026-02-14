"""
config/compile.py  –  JobCompiler

defaults + job.override を deep_merge して JobSpec に落とす。
unique 位置は出さない（規約固定）。
"""
from __future__ import annotations

from typing import Any

from app.core.types import (
    BucketEdge,
    BucketSetSpec,
    EnvSpec,
    ExtractSpec,
    JobSpec,
    NullPolicySpec,
    OrderByItem,
    OutputSpec,
    PolicySpec,
    ScopeSpec,
    SelectSpec,
    SeriesSpec,
    TimeBucketSpec,
    TopNConfigSpec,
    TopNSpec,
    UniqueKeepSpec,
    UniqueSpec,
)
from app.config.merge import deep_merge


class JobCompiler:
    """raw config → list[JobSpec]"""

    @staticmethod
    def compile(raw: dict[str, Any]) -> list[JobSpec]:
        env_raw = raw.get("env", {})
        defaults = raw.get("defaults", {})
        jobs_raw = raw.get("jobs", [])

        env = _build_env(env_raw)
        result: list[JobSpec] = []

        for job_raw in jobs_raw:
            merged = deep_merge(defaults, job_raw.get("override", {}))
            js = _build_job_spec(job_raw, merged, env)
            result.append(js)

        return result


# ──────────────────────────────────────────────
# Internal builders
# ──────────────────────────────────────────────
def _build_env(d: dict) -> EnvSpec:
    return EnvSpec(
        sqlite_path=d.get("sqlite_path", "work.sqlite"),
        isld_csv_path=d.get("isld_csv_path", ""),
        out_dir=d.get("out_dir", "out"),
    )


def _build_scope(d: dict | None) -> ScopeSpec:
    if not d:
        return ScopeSpec()
    return ScopeSpec(
        companies=d.get("companies", []),
        countries=d.get("countries", []),
        country_prefixes=d.get("country_prefixes", []),
        releases=d.get("releases", []),
        version_prefixes=d.get("version_prefixes", []),
        specs=d.get("specs", []),
        date_from=d.get("date_from"),
        date_to=d.get("date_to"),
        gen_flags=d.get("gen_flags"),
        ess_flags=d.get("ess_flags"),
        country_mode=d.get("country_mode", "ALL"),
    )


def _build_unique(d: dict | None) -> UniqueSpec:
    if not d:
        return UniqueSpec()
    keep_raw = d.get("keep", {})
    order_by = [
        OrderByItem(col=item.get("col", "__src_rownum"), dir=item.get("dir", "ASC"))
        for item in keep_raw.get("order_by", [{"col": "__src_rownum", "dir": "ASC"}])
    ]
    return UniqueSpec(
        unit=d.get("unit", "publ"),
        keep=UniqueKeepSpec(order_by=order_by),
        partition_extra=d.get("partition_extra", []),
    )


def _build_policies(d: dict | None) -> PolicySpec:
    if not d:
        return PolicySpec()
    return PolicySpec(
        decl_date_policy=d.get("decl_date_policy", "signature_first"),
        negative_lag_policy=d.get("negative_lag_policy", "keep"),
    )


def _build_top_n(d: dict | None) -> TopNSpec:
    if not d:
        return TopNSpec()
    return TopNSpec(
        n=d.get("n", 20),
        metric=d.get("metric", "count"),
        order=d.get("order", "DESC"),
    )


def _build_bucket_set(d: dict | None) -> BucketSetSpec:
    if not d:
        return BucketSetSpec()
    bins_raw = d.get("bins", [])
    bins = [
        BucketEdge(
            label=b.get("label", ""),
            min_val=b.get("min_val"),
            max_val=b.get("max_val"),
        )
        for b in bins_raw
    ]
    return BucketSetSpec(column=d.get("column", "lag_days"), bins=bins)


def _build_time_bucket(d: dict | None) -> TimeBucketSpec:
    if not d:
        return TimeBucketSpec()
    return TimeBucketSpec(period=d.get("period", "quarter"))


def _build_series(d: dict | None) -> SeriesSpec:
    if not d:
        return SeriesSpec()
    return SeriesSpec(
        date_col=d.get("date_col", "PBPA_APP_DATE"),
        out_csv=d.get("out_csv"),
    )


def _build_topn_config(d: dict | None) -> TopNConfigSpec:
    if not d:
        return TopNConfigSpec()
    order_by = [
        OrderByItem(col=item.get("col", "cnt"), dir=item.get("dir", "DESC"))
        for item in d.get("order_by", [{"col": "cnt", "dir": "DESC"}])
    ]
    return TopNConfigSpec(
        group_cols=d.get("group_cols", []),
        order_by=order_by,
        limit=d.get("limit", 100),
        out_csv=d.get("out_csv"),
    )


def _build_extract(d: dict | None) -> ExtractSpec:
    if not d:
        return ExtractSpec()
    order_by = [
        OrderByItem(col=item.get("col", ""), dir=item.get("dir", "ASC"))
        for item in d.get("order_by", [])
    ]
    return ExtractSpec(
        cols=d.get("cols", []),
        distinct=d.get("distinct", True),
        limit=d.get("limit"),
        order_by=order_by,
        out_csv=d.get("out_csv"),
    )


def _build_job_spec(job_raw: dict, merged: dict, env: EnvSpec) -> JobSpec:
    return JobSpec(
        job_id=job_raw["job_id"],
        template=job_raw["template"],
        env=env,
        scope=_build_scope(merged.get("scope")),
        unique=_build_unique(merged.get("unique")),
        policies=_build_policies(merged.get("policies")),
        top_n=_build_top_n(merged.get("top_n")),
        bucket_set=_build_bucket_set(merged.get("bucket_set")),
        time_bucket=_build_time_bucket(merged.get("timeseries") or merged.get("time_bucket")),
        series=_build_series(merged.get("series")),
        topn_config=_build_topn_config(merged.get("topn")),
        extract=_build_extract(merged.get("extract")),
        extra=merged.get("extra", {}),
        job_description=job_raw.get("job_description", ""),
        filters_explain=job_raw.get("filters_explain", []),
    )
