"""
funcs/f10_selects.py  –  SELECT 系 Func

各 SELECT は SelectRegistry への登録のみ (DB に保存しない)。
produces = "select"
"""
from __future__ import annotations

from typing import Any

from app.funcs.base import BaseFunc, ExecutionContext, FuncResult, FuncSignature


# ═══════════════════════════════════════════════
# Helpers: multi-country analysis
# ═══════════════════════════════════════════════

_DEFAULT_COUNTRIES = ["JP", "US", "CN", "EP", "KR"]


def _country_case(countries: list[str] | None = None) -> str:
    """Country_Of_Registration → 2-letter code CASE expression"""
    cs = countries or _DEFAULT_COUNTRIES
    parts = [f"WHEN Country_Of_Registration LIKE '{c} %' THEN '{c}'" for c in cs]
    return "CASE " + " ".join(parts) + " ELSE 'OTHER' END"


def _bucket_expr(period: str, date_col: str = "PBPA_APP_DATE") -> str:
    if period == "year":
        return f"SUBSTR({date_col}, 1, 4) || '-01-01'"
    return f"SUBSTR({date_col}, 1, 7) || '-01'"


def _country_in_list(countries: list[str] | None = None) -> str:
    cs = countries or _DEFAULT_COUNTRIES
    return ", ".join(f"'{c}'" for c in cs)



# ═══════════════════════════════════════════════
# ANA-A: 国×企業×月次/年次 出願数推移 (filing count)
# ═══════════════════════════════════════════════

class SelFilingCountTs(BaseFunc):
    """Filing count time series: country × company × bucket"""

    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="sel_filing_count_ts",
            required_args=["source"],
            optional_args=["period", "countries", "include_all"],
            produces="select",
            columns=["country", "company", "bucket", "filing_count"],
            description="出願数時系列 (国×企業×bucket)",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        source = ctx.resolve_temp(args["source"])
        period = args.get("period", "month")
        countries = args.get("countries", _DEFAULT_COUNTRIES)
        include_all = args.get("include_all", True)
        bkt = _bucket_expr(period)
        case = _country_case(countries)
        c_in = _country_in_list(countries)

        sql = f"""
WITH base AS (
  SELECT {case} AS __ctry,
         COMP_LEGAL_NAME AS company,
         {bkt} AS bucket,
         PATT_APPLICATION_NUMBER
  FROM [{source}]
  WHERE PATT_APPLICATION_NUMBER IS NOT NULL
    AND PBPA_APP_DATE IS NOT NULL
)
SELECT __ctry AS country, company, bucket,
       COUNT(DISTINCT PATT_APPLICATION_NUMBER) AS filing_count
FROM base
WHERE __ctry IN ({c_in})
GROUP BY __ctry, company, bucket
"""
        if include_all:
            sql += f"""UNION ALL
SELECT 'ALL' AS country, company, bucket,
       COUNT(DISTINCT PATT_APPLICATION_NUMBER) AS filing_count
FROM base
GROUP BY company, bucket
"""
        sql += "ORDER BY country, company, bucket;"

        return FuncResult(
            sql=sql,
            columns=["country", "company", "bucket", "filing_count"],
            description=f"ANA-A: filing count ts ({period})",
        )


# ═══════════════════════════════════════════════
# ANA-B: 国×企業×月次/年次 lag分布サマリ
# ═══════════════════════════════════════════════

class SelLagStats(BaseFunc):
    """Lag statistics summary: country × company × bucket → boxplot data"""

    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="sel_lag_stats",
            required_args=["source"],
            optional_args=["period", "countries", "include_all"],
            produces="select",
            columns=["country", "company", "bucket", "n",
                      "min_lag_days", "q1_lag_days", "median_lag_days",
                      "q3_lag_days", "max_lag_days"],
            description="lag分布サマリ (国×企業×bucket)",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        source = ctx.resolve_temp(args["source"])
        period = args.get("period", "month")
        countries = args.get("countries", _DEFAULT_COUNTRIES)
        include_all = args.get("include_all", True)
        bkt = _bucket_expr(period)
        case = _country_case(countries)
        c_in = _country_in_list(countries)

        # CTE approach with NTILE(4) for quartiles
        sql = f"""
WITH base AS (
  SELECT {case} AS __ctry,
         COMP_LEGAL_NAME AS company,
         {bkt} AS bucket,
         lag_days
  FROM [{source}]
  WHERE lag_days IS NOT NULL
    AND PBPA_APP_DATE IS NOT NULL
    AND COMP_LEGAL_NAME IS NOT NULL
),
expanded AS (
  SELECT __ctry AS country, company, bucket, lag_days
  FROM base WHERE __ctry IN ({c_in})
"""
        if include_all:
            sql += """  UNION ALL
  SELECT 'ALL', company, bucket, lag_days FROM base
"""
        sql += f"""),
quartiled AS (
  SELECT country, company, bucket, lag_days,
         NTILE(4) OVER (PARTITION BY country, company, bucket ORDER BY lag_days) AS q
  FROM expanded
)
SELECT country, company, bucket,
  COUNT(*) AS n,
  CAST(MIN(lag_days) AS INTEGER) AS min_lag_days,
  CAST(MAX(CASE WHEN q = 1 THEN lag_days END) AS INTEGER) AS q1_lag_days,
  CAST(MAX(CASE WHEN q = 2 THEN lag_days END) AS INTEGER) AS median_lag_days,
  CAST(MAX(CASE WHEN q = 3 THEN lag_days END) AS INTEGER) AS q3_lag_days,
  CAST(MAX(lag_days) AS INTEGER) AS max_lag_days
FROM quartiled
GROUP BY country, company, bucket
ORDER BY country, company, bucket;"""

        return FuncResult(
            sql=sql,
            columns=["country", "company", "bucket", "n",
                      "min_lag_days", "q1_lag_days", "median_lag_days",
                      "q3_lag_days", "max_lag_days"],
            description=f"ANA-B: lag stats ({period})",
        )


# ═══════════════════════════════════════════════
# ANA-C: 国×企業×月次/年次 TGPP_NUMBER TopK
# ═══════════════════════════════════════════════

class SelTopSpecsTs(BaseFunc):
    """Top Specs time series: country × company × bucket × TGPP TopK"""

    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="sel_top_specs_ts",
            required_args=["source"],
            optional_args=["period", "countries", "include_all", "top_k"],
            produces="select",
            columns=["country", "company", "bucket", "TGPP_NUMBER", "cnt", "rank"],
            description="TopSpec時系列 (国×企業×bucket×TGPP TopK)",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        source = ctx.resolve_temp(args["source"])
        period = args.get("period", "month")
        countries = args.get("countries", _DEFAULT_COUNTRIES)
        include_all = args.get("include_all", True)
        top_k = args.get("top_k", 10)
        bkt = _bucket_expr(period)
        case = _country_case(countries)
        c_in = _country_in_list(countries)

        sql = f"""
WITH base AS (
  SELECT {case} AS __ctry,
         COMP_LEGAL_NAME AS company,
         {bkt} AS bucket,
         TGPP_NUMBER
  FROM [{source}]
  WHERE TGPP_NUMBER IS NOT NULL
    AND PBPA_APP_DATE IS NOT NULL
    AND COMP_LEGAL_NAME IS NOT NULL
),
expanded AS (
  SELECT __ctry AS country, company, bucket, TGPP_NUMBER
  FROM base WHERE __ctry IN ({c_in})
"""
        if include_all:
            sql += """  UNION ALL
  SELECT 'ALL', company, bucket, TGPP_NUMBER FROM base
"""
        sql += f"""),
counted AS (
  SELECT country, company, bucket, TGPP_NUMBER, COUNT(*) AS cnt
  FROM expanded
  GROUP BY country, company, bucket, TGPP_NUMBER
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY country, company, bucket ORDER BY cnt DESC
  ) AS rank
  FROM counted
)
SELECT country, company, bucket, TGPP_NUMBER, cnt, rank
FROM ranked
WHERE rank <= {top_k}
ORDER BY country, company, bucket, rank;"""

        return FuncResult(
            sql=sql,
            columns=["country", "company", "bucket", "TGPP_NUMBER", "cnt", "rank"],
            description=f"ANA-C: top {top_k} specs ts ({period})",
        )


# ═══════════════════════════════════════════════
# ANA-D: 国固定 企業別ランキング
# ═══════════════════════════════════════════════

class SelCompanyRank(BaseFunc):
    """Company ranking per country (count by configurable unit)"""

    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="sel_company_rank",
            required_args=["source"],
            optional_args=["countries", "include_all", "unit_col", "unit_name"],
            produces="select",
            columns=["country", "unique_unit", "company", "cnt", "rank"],
            description="企業別ランキング (国×unit)",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        source = ctx.resolve_temp(args["source"])
        countries = args.get("countries", _DEFAULT_COUNTRIES)
        include_all = args.get("include_all", True)
        unit_col = args.get("unit_col", "PATT_APPLICATION_NUMBER")
        unit_name = args.get("unit_name", "app")
        case = _country_case(countries)
        c_in = _country_in_list(countries)

        sql = f"""
WITH base AS (
  SELECT {case} AS __ctry,
         COMP_LEGAL_NAME AS company,
         [{unit_col}]
  FROM [{source}]
  WHERE [{unit_col}] IS NOT NULL
    AND COMP_LEGAL_NAME IS NOT NULL
),
expanded AS (
  SELECT __ctry AS country, company, [{unit_col}]
  FROM base WHERE __ctry IN ({c_in})
"""
        if include_all:
            sql += f"""  UNION ALL
  SELECT 'ALL', company, [{unit_col}] FROM base
"""
        sql += f"""),
counted AS (
  SELECT country, company,
         COUNT(DISTINCT [{unit_col}]) AS cnt
  FROM expanded
  GROUP BY country, company
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY cnt DESC) AS rank
  FROM counted
)
SELECT country, '{unit_name}' AS unique_unit, company, cnt, rank
FROM ranked
ORDER BY country, rank;"""

        return FuncResult(
            sql=sql,
            columns=["country", "unique_unit", "company", "cnt", "rank"],
            description=f"ANA-D: company rank by {unit_name}",
        )


# ═══════════════════════════════════════════════
# ANA-E: Spec×会社 ヒートマップ (縦持ち)
# ═══════════════════════════════════════════════

class SelSpecCompanyHeat(BaseFunc):
    """Spec × Company heatmap (long format)"""

    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="sel_spec_company_heat",
            required_args=["source"],
            optional_args=["countries", "include_all", "top_k"],
            produces="select",
            columns=["country", "TGPP_NUMBER", "company", "cnt"],
            description="Spec×会社ヒートマップ (縦持ち)",
        )

    def build_sql(self, ctx: ExecutionContext, args: dict[str, Any]) -> FuncResult:
        source = ctx.resolve_temp(args["source"])
        countries = args.get("countries", _DEFAULT_COUNTRIES)
        include_all = args.get("include_all", True)
        top_k = args.get("top_k", 20)
        case = _country_case(countries)
        c_in = _country_in_list(countries)

        # Use top_k specs globally (most frequent)
        sql = f"""
WITH base AS (
  SELECT {case} AS __ctry,
         COMP_LEGAL_NAME AS company,
         TGPP_NUMBER
  FROM [{source}]
  WHERE TGPP_NUMBER IS NOT NULL
    AND COMP_LEGAL_NAME IS NOT NULL
),
top_specs AS (
  SELECT TGPP_NUMBER FROM base
  GROUP BY TGPP_NUMBER
  ORDER BY COUNT(*) DESC
  LIMIT {top_k}
),
expanded AS (
  SELECT __ctry AS country, company, b.TGPP_NUMBER
  FROM base b INNER JOIN top_specs t ON b.TGPP_NUMBER = t.TGPP_NUMBER
  WHERE __ctry IN ({c_in})
"""
        if include_all:
            sql += """  UNION ALL
  SELECT 'ALL', company, b.TGPP_NUMBER
  FROM base b INNER JOIN top_specs t ON b.TGPP_NUMBER = t.TGPP_NUMBER
"""
        sql += """)
SELECT country, TGPP_NUMBER, company, COUNT(*) AS cnt
FROM expanded
GROUP BY country, TGPP_NUMBER, company
ORDER BY country, TGPP_NUMBER, cnt DESC;"""

        return FuncResult(
            sql=sql,
            columns=["country", "TGPP_NUMBER", "company", "cnt"],
            description=f"ANA-E: spec×company heatmap (top {top_k})",
        )
