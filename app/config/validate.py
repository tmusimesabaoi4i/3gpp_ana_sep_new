"""
config/validate.py  –  config.json のバリデーション
"""
from __future__ import annotations

from typing import Any

from app.core.types import ConfigError

# ──────── 許可値 ────────
_ALLOWED_TEMPLATES = {
    "ts_filing_count", "ts_lag_stats", "ts_top_specs",
    "rank_company_counts", "heat_spec_company",
}
_ALLOWED_UNIQUE_UNITS = {"publ", "app", "family", "dipg", "none"}
_ALLOWED_DECL_DATE_POLICIES = {"signature_first", "reflected_first"}
_ALLOWED_NEG_LAG_POLICIES = {"keep", "zero", "null", "drop"}
_ALLOWED_PERIODS = {"month", "quarter", "year", "fiscal_year"}
_ALLOWED_ORDER_DIRS = {"ASC", "DESC", "asc", "desc"}

# order_by で許可する列名ホワイトリスト
_ALLOWED_ORDER_COLUMNS = {
    "__src_rownum",
    "IPRD_ID", "DIPG_ID", "DIPG_PATF_ID", "PUBL_NUMBER", "PATT_APPLICATION_NUMBER",
    "IPRD_SIGNATURE_DATE", "Reflected_Date", "PBPA_APP_DATE",
    "TGPP_NUMBER", "TGPV_VERSION",
    "COMP_LEGAL_NAME", "Country_Of_Registration",
    "Patent_Type", "Standard",
    "Gen_2G", "Gen_3G", "Gen_4G", "Gen_5G",
    # enrich 由来 (TEMP)
    "decl_date", "lag_days", "release_num", "time_bucket",
}


class ConfigValidator:
    """config dict のバリデーション"""

    @staticmethod
    def validate(raw: dict[str, Any]) -> None:
        """
        Raises ConfigError on first violation.
        """
        _check_env(raw)
        _check_defaults(raw.get("defaults", {}))
        _check_jobs(raw.get("jobs", []))


# ──────── internal ────────
def _check_env(raw: dict) -> None:
    env = raw.get("env")
    if not isinstance(env, dict):
        raise ConfigError("'env' が必要です", path="env")
    for key in ("sqlite_path", "out_dir"):
        if key not in env or not env[key]:
            raise ConfigError(f"'{key}' は必須です", path=f"env.{key}")


def _check_defaults(defaults: dict) -> None:
    if not isinstance(defaults, dict):
        return

    unique = defaults.get("unique", {})
    if isinstance(unique, dict):
        _check_unique(unique, "defaults.unique")

    policies = defaults.get("policies", {})
    if isinstance(policies, dict):
        _check_policies(policies, "defaults.policies")


def _check_unique(unique: dict, path: str) -> None:
    unit = unique.get("unit", "publ")
    if unit not in _ALLOWED_UNIQUE_UNITS:
        raise ConfigError(
            f"unit '{unit}' は不正です。許可値: {_ALLOWED_UNIQUE_UNITS}",
            path=f"{path}.unit",
        )
    keep = unique.get("keep", {})
    if isinstance(keep, dict):
        order_by = keep.get("order_by", [])
        if isinstance(order_by, list):
            for i, item in enumerate(order_by):
                if isinstance(item, dict):
                    col = item.get("col", "")
                    if col and col not in _ALLOWED_ORDER_COLUMNS:
                        raise ConfigError(
                            f"order_by 列 '{col}' はホワイトリストにありません",
                            path=f"{path}.keep.order_by[{i}].col",
                        )
                    d = item.get("dir", "ASC")
                    if d not in _ALLOWED_ORDER_DIRS:
                        raise ConfigError(
                            f"dir '{d}' は不正です",
                            path=f"{path}.keep.order_by[{i}].dir",
                        )


def _check_policies(policies: dict, path: str) -> None:
    ddp = policies.get("decl_date_policy")
    if ddp is not None and ddp not in _ALLOWED_DECL_DATE_POLICIES:
        raise ConfigError(
            f"decl_date_policy '{ddp}' は不正です",
            path=f"{path}.decl_date_policy",
        )
    nlp = policies.get("negative_lag_policy")
    if nlp is not None and nlp not in _ALLOWED_NEG_LAG_POLICIES:
        raise ConfigError(
            f"negative_lag_policy '{nlp}' は不正です",
            path=f"{path}.negative_lag_policy",
        )


def _check_jobs(jobs: Any) -> None:
    if not isinstance(jobs, list):
        raise ConfigError("'jobs' は配列でなければなりません", path="jobs")
    if not jobs:
        raise ConfigError("'jobs' に最低 1 つのジョブが必要です", path="jobs")

    seen_ids: set[str] = set()
    for i, job in enumerate(jobs):
        if not isinstance(job, dict):
            raise ConfigError(f"jobs[{i}] は object でなければなりません", path=f"jobs[{i}]")

        jid = job.get("job_id")
        if not jid:
            raise ConfigError("'job_id' は必須です", path=f"jobs[{i}].job_id")
        if jid in seen_ids:
            raise ConfigError(f"job_id '{jid}' が重複しています", path=f"jobs[{i}].job_id")
        seen_ids.add(jid)

        template = job.get("template")
        if not template:
            raise ConfigError("'template' は必須です", path=f"jobs[{i}].template")
        if template not in _ALLOWED_TEMPLATES:
            raise ConfigError(
                f"template '{template}' は不正です。許可値: {_ALLOWED_TEMPLATES}",
                path=f"jobs[{i}].template",
            )

        override = job.get("override", {})
        if isinstance(override, dict):
            ov_unique = override.get("unique", {})
            if isinstance(ov_unique, dict) and ov_unique:
                _check_unique(ov_unique, f"jobs[{i}].override.unique")

            ov_policies = override.get("policies", {})
            if isinstance(ov_policies, dict) and ov_policies:
                _check_policies(ov_policies, f"jobs[{i}].override.policies")

            # topN
            top_n = override.get("top_n", {})
            if isinstance(top_n, dict) and "n" in top_n:
                n = top_n["n"]
                if not isinstance(n, int) or n <= 0:
                    raise ConfigError(
                        f"top_n.n は正の整数でなければなりません (got {n})",
                        path=f"jobs[{i}].override.top_n.n",
                    )

            # bins 整合
            bucket_set = override.get("bucket_set", {})
            if isinstance(bucket_set, dict):
                bins = bucket_set.get("bins", [])
                if isinstance(bins, list) and bins:
                    _check_bins(bins, f"jobs[{i}].override.bucket_set.bins")


def _check_bins(bins: list, path: str) -> None:
    """bins の整合チェック: min<max, 昇順, 重複なし, 最後だけ max=null 許可"""
    prev_max = None
    for i, b in enumerate(bins):
        if not isinstance(b, dict):
            raise ConfigError(f"bins[{i}] は object でなければなりません", path=f"{path}[{i}]")
        mn = b.get("min_val")
        mx = b.get("max_val")
        if mn is not None and mx is not None:
            if mn >= mx:
                raise ConfigError(
                    f"bins[{i}]: min_val({mn}) >= max_val({mx})",
                    path=f"{path}[{i}]",
                )
        if prev_max is not None and mn is not None:
            if mn < prev_max:
                raise ConfigError(
                    f"bins[{i}]: 昇順ではありません (前の max={prev_max}, 今の min={mn})",
                    path=f"{path}[{i}]",
                )
        if mx is None and i < len(bins) - 1:
            raise ConfigError(
                f"bins[{i}]: max_val=null は最後の bin のみ許可",
                path=f"{path}[{i}]",
            )
        prev_max = mx
