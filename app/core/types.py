"""
core/types.py  –  ISLD Pipeline の全 Spec 型と例外定義
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


# ──────────────────────────────────────────────
# 例外
# ──────────────────────────────────────────────
class ConfigError(Exception):
    """config.json のバリデーション / パースエラー"""

    def __init__(self, message: str, path: str = ""):
        self.path = path
        super().__init__(f"[ConfigError] {path}: {message}" if path else f"[ConfigError] {message}")


class PlanError(Exception):
    """Plan 構築 / 検証エラー"""


class SqlError(Exception):
    """SQL 構築 / 実行エラー"""


# ──────────────────────────────────────────────
# Spec 型
# ──────────────────────────────────────────────
@dataclass
class EnvSpec:
    sqlite_path: str
    isld_csv_path: str = ""
    out_dir: str = "out"


@dataclass
class OrderByItem:
    col: str
    dir: str = "ASC"  # ASC | DESC


@dataclass
class ScopeSpec:
    """母集団フィルタ"""
    companies: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    country_prefixes: list[str] = field(default_factory=list)  # "JP" → LIKE 'JP %'
    releases: list[str] = field(default_factory=list)
    version_prefixes: list[str] = field(default_factory=list)  # "18" → LIKE '18.%'
    specs: list[str] = field(default_factory=list)
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    gen_flags: Optional[dict[str, Any]] = None     # {"5G": 1} → Gen_5G = 1
    ess_flags: Optional[dict[str, Any]] = None     # {"ess_to_standard": true}
    country_mode: str = "ALL"                      # ALL | FILTER


@dataclass
class UniqueKeepSpec:
    order_by: list[OrderByItem] = field(default_factory=lambda: [OrderByItem(col="__src_rownum", dir="ASC")])


@dataclass
class UniqueSpec:
    unit: str = "publ"  # publ | app | family | dipg | none
    keep: UniqueKeepSpec = field(default_factory=UniqueKeepSpec)
    partition_extra: list[str] = field(default_factory=list)


@dataclass
class PolicySpec:
    decl_date_policy: str = "signature_first"  # signature_first | reflected_first
    negative_lag_policy: str = "keep"           # keep | zero | null | drop


@dataclass
class BucketEdge:
    label: str
    min_val: Optional[float] = None
    max_val: Optional[float] = None


@dataclass
class BucketSetSpec:
    column: str = "lag_days"
    bins: list[BucketEdge] = field(default_factory=list)


@dataclass
class TopNSpec:
    n: int = 20
    metric: str = "count"  # count | density
    order: str = "DESC"


@dataclass
class TimeBucketSpec:
    period: str = "quarter"  # month | quarter | year | fiscal_year


@dataclass
class NullPolicySpec:
    """出力時の NULL 置換ポリシー"""
    int_null: Any = None       # -1 など
    text_null: Any = None      # "" など
    date_null: Any = None


@dataclass
class OutputSpec:
    select_ref: str
    format: str = "csv"  # csv | excel
    filename: Optional[str] = None
    null_policy: NullPolicySpec = field(default_factory=NullPolicySpec)


@dataclass
class SelectSpec:
    ref_name: str
    sql: str
    params: list[Any] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    description: str = ""


@dataclass
class SeriesSpec:
    """時系列集計パラメータ"""
    date_col: str = "PBPA_APP_DATE"
    out_csv: Optional[str] = None


@dataclass
class TopNConfigSpec:
    """汎用 TopN / GROUP BY ランキングパラメータ"""
    group_cols: list[str] = field(default_factory=list)
    order_by: list[OrderByItem] = field(default_factory=lambda: [OrderByItem(col="cnt", dir="DESC")])
    limit: int = 100
    out_csv: Optional[str] = None


@dataclass
class ExtractSpec:
    """列抽出 CSV パラメータ"""
    cols: list[str] = field(default_factory=list)
    distinct: bool = True
    limit: Optional[int] = None
    order_by: list[OrderByItem] = field(default_factory=list)
    out_csv: Optional[str] = None


@dataclass
class ExcelOutputSpec:
    """Excel 出力設定（ALL_*/CO_* シート分割 + META）"""
    enabled: bool = False
    path: str = "out/analysis_results.xlsx"
    companies: dict[str, str] = field(default_factory=dict)  # {display_key: LIKE pattern}
    meta_sheet: bool = True


@dataclass
class JobSpec:
    job_id: str
    template: str
    env: EnvSpec = field(default_factory=EnvSpec)
    scope: ScopeSpec = field(default_factory=ScopeSpec)
    unique: UniqueSpec = field(default_factory=UniqueSpec)
    policies: PolicySpec = field(default_factory=PolicySpec)
    top_n: TopNSpec = field(default_factory=TopNSpec)
    bucket_set: BucketSetSpec = field(default_factory=BucketSetSpec)
    time_bucket: TimeBucketSpec = field(default_factory=TimeBucketSpec)
    series: SeriesSpec = field(default_factory=SeriesSpec)
    topn_config: TopNConfigSpec = field(default_factory=TopNConfigSpec)
    extract: ExtractSpec = field(default_factory=ExtractSpec)
    outputs: list[OutputSpec] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)
    job_description: str = ""                                  # 人間向け説明
    filters_explain: list[str] = field(default_factory=list)   # フィルタ説明


# ──────────────────────────────────────────────
# Plan 用
# ──────────────────────────────────────────────
@dataclass
class FuncRef:
    """Plan の 1 ステップ"""
    func_name: str
    args: dict[str, Any] = field(default_factory=dict)
    save_as: str = ""  # 論理名 (tmp_scope 等)
