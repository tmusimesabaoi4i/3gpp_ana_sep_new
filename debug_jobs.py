#!/usr/bin/env python3
"""
debug_jobs.py — scope/unique 適用後のデータをサンプル出力するデバッグツール

モード: raw / unique / target / ts（--mode で指定）

例:
  python debug_jobs.py --mode raw --config example_ana/config.json --limit 100 --out out.csv
  python debug_jobs.py --mode unique --config example_ana/config.json --limit 100 --out out.csv
  python debug_jobs.py --mode target --target-col DIPG_ID --target-val 43483 --out out.csv
  python debug_jobs.py --mode ts --company Ericsson --date 1997-10 --country JP --out out/ericsson_jp_1997-10.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Sequence


# -----------------------------------------------------------------------------
# 企業名正規化（空白・記号・大文字化のみ。綴り違いは alias で吸収）
# -----------------------------------------------------------------------------

def _normalize_company(s: str) -> str:
    """企業名の表記ゆれを吸収する正規化（検索用）。"""
    if not s or not isinstance(s, str):
        return ""
    t = re.sub(r"\s+", " ", s.strip())
    t = re.sub(r"[.,]\s*$", "", t)
    return t.upper()


# -----------------------------------------------------------------------------
# 内蔵 alias（config に無いときのフォールバック。最低限 Ericsson/Ericson）
# -----------------------------------------------------------------------------

_BUILTIN_COMPANY_ALIASES: dict[str, list[str]] = {
    "Ericsson": ["ERICSSON", "ERICSON"],
    "Fujitsu": ["FUJITSU"],
    "Huawei": ["HUAWEI", "HUAWEI TECHNOLOGIES"],
    "Kyocera": ["KYOCERA"],
    "LG_Electronics": ["LG ELECTRONICS", "LGE", "LG ELECTRONICS INC"],
    "NEC": ["NEC", "NEC CORPORATION"],
    "Nokia": ["NOKIA", "NOKIA CORPORATION"],
    "NTT_Docomo": ["DOCOMO", "NTT DOCOMO"],
    "Panasonic": ["PANASONIC", "PANASONIC CORPORATION"],
    "Qualcomm": ["QUALCOMM", "QUALCOMM INC", "QUALCOMM INCORPORATED"],
    "Samsung": ["SAMSUNG", "SAMSUNG ELECTRONICS"],
    "Sharp": ["SHARP", "SHARP CORPORATION"],
    "Toyota": ["TOYOTA", "TOYOTA MOTOR"],
    "Xiaomi": ["XIAOMI"],
    "ZTE": ["ZTE", "ZTE CORPORATION"],
}


def get_company_aliases(cfg: dict | None) -> dict[str, list[str]]:
    """config から company_aliases を取得。無ければ excel_output.companies を流用、どちらも無ければ内蔵 alias。"""
    if not cfg:
        return _BUILTIN_COMPANY_ALIASES.copy()
    aliases = cfg.get("company_aliases")
    if isinstance(aliases, dict):
        out = {}
        for k, v in aliases.items():
            if isinstance(v, list):
                out[k] = [str(x) for x in v]
            elif isinstance(v, str):
                out[k] = [v]
        if out:
            return out
    companies = cfg.get("excel_output", {}).get("companies")
    if isinstance(companies, dict):
        return {k: [v] for k, v in companies.items() if v}
    return _BUILTIN_COMPANY_ALIASES.copy()


def resolve_company_patterns(input_company: str, aliases: dict[str, list[str]]) -> list[str]:
    """alias キー一致 → その配列を LIKE 用に正規化して返す。キー不一致 → normalize(input) を1本だけ返す。"""
    key = input_company.strip()
    patterns: list[str] = []
    key_upper = key.upper()
    for k, vals in aliases.items():
        if k.upper() == key_upper:
            patterns = [_normalize_company(p) for p in vals if p]
            break
    if not patterns:
        patterns = [_normalize_company(key)] if key else []
    return patterns


# -----------------------------------------------------------------------------
# 共通: 月範囲パース / CSV 出力 / SQL 実行
# -----------------------------------------------------------------------------

def parse_month_arg(s: str) -> tuple[str, str]:
    """YYYY-MM または YYYY-MM-DD → (month_start, month_end)。month_end は翌月1日（未満で使う）。"""
    s = (s or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})(?:-\d{2})?$", s)
    if not m:
        raise ValueError(f"--date は YYYY-MM or YYYY-MM-DD で指定してください: {s}")
    y, mon = m.group(1), m.group(2)
    month_start = f"{y}-{mon}-01"
    mon_int = int(mon)
    if mon_int == 12:
        month_end = f"{int(y) + 1}-01-01"
    else:
        month_end = f"{y}-{mon_int + 1:02d}-01"
    return month_start, month_end


def write_csv(
    rows: Sequence[Any],
    out_path: Path,
    *,
    exclude_cols: set[str] | None = None,
) -> None:
    """行リストを CSV に書き出す。親ディレクトリは自動作成。"""
    if not rows:
        return
    exclude_cols = exclude_cols or set()
    col_names = [c for c in rows[0].keys() if c not in exclude_cols]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        for row in rows:
            writer.writerow([row[c] for c in col_names])


def fetch_rows(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list:
    """SQL 実行して全行取得。"""
    return list(conn.execute(sql, params or []))


# -----------------------------------------------------------------------------
# scope WHERE と scope SQL 構築
# -----------------------------------------------------------------------------

def _company_where_from_patterns(patterns: list[str]) -> tuple[str, list[str]]:
    """LIKE パターンリストから (SQL fragment, params) を返す。"""
    if not patterns:
        return "1=0", []
    placeholders = " OR ".join(["norm_company(COMP_LEGAL_NAME) LIKE ?"] * len(patterns))
    params = [f"%{p}%" for p in patterns]
    return placeholders, params


def _build_where(scope: dict, aliases: dict[str, list[str]] | None = None) -> tuple[list[str], list]:
    """scope dict → WHERE 条件リスト + params。companies は alias 展開。"""
    conditions = []
    params = []

    if scope.get("companies"):
        clauses = []
        for comp in scope["companies"]:
            a = aliases or _BUILTIN_COMPANY_ALIASES.copy()
            pats = resolve_company_patterns(comp, a)
            frag, ps = _company_where_from_patterns(pats)
            if ps:
                clauses.append(f"({frag})")
                params.extend(ps)
            else:
                clauses.append("norm_company(COMP_LEGAL_NAME) LIKE ?")
                params.append(f"%{_normalize_company(comp)}%")
        if clauses:
            conditions.append(f"({' OR '.join(clauses)})")

    cm = scope.get("country_mode", "ALL")
    if cm == "FILTER" and scope.get("country_prefixes"):
        clauses = []
        for pfx in scope["country_prefixes"]:
            clauses.append("Country_Of_Registration LIKE ?")
            params.append(f"{pfx} %")
        conditions.append(f"({' OR '.join(clauses)})")

    if scope.get("gen_flags"):
        gen_map = {"2G": "Gen_2G", "3G": "Gen_3G", "4G": "Gen_4G", "5G": "Gen_5G"}
        for gen, val in scope["gen_flags"].items():
            col = gen_map.get(gen)
            if col and val is not None:
                conditions.append(f"{col} = ?")
                params.append(int(val))

    if scope.get("ess_flags"):
        ess_map = {"ess_to_standard": "Ess_To_Standard", "ess_to_project": "Ess_To_Project"}
        for key, val in scope["ess_flags"].items():
            col = ess_map.get(key)
            if col and val is not None:
                conditions.append(f"{col} = ?")
                params.append(1 if (isinstance(val, bool) and val) else (0 if isinstance(val, bool) else val))

    if scope.get("date_from"):
        conditions.append("PBPA_APP_DATE >= ?")
        params.append(scope["date_from"])
    if scope.get("date_to"):
        conditions.append("PBPA_APP_DATE <= ?")
        params.append(scope["date_to"])

    if scope.get("version_prefixes"):
        clauses = []
        for vp in scope["version_prefixes"]:
            clauses.append("TGPV_VERSION LIKE ?")
            params.append(f"{vp}.%")
        conditions.append(f"({' OR '.join(clauses)})")

    return conditions, params


def build_scope_sql(
    scope: dict,
    mode: str,
    unique_unit: str,
    limit: int,
    aliases: dict[str, list[str]] | None = None,
) -> tuple[str, list]:
    """raw または unique 用の SQL と params を返す。"""
    conditions, params = _build_where(scope, aliases)
    where = " AND ".join(conditions) if conditions else "1=1"

    if mode == "raw":
        sql = f"SELECT * FROM isld_pure WHERE {where} LIMIT ?"
        params.append(limit)
        return sql, params

    unit_key = {"app": "PATT_APPLICATION_NUMBER", "publ": "PUBL_NUMBER",
                "family": "DIPG_PATF_ID", "dipg": "DIPG_ID"}.get(unique_unit)
    if unit_key and unique_unit != "none":
        sql = f"""
            SELECT * FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY {unit_key} ORDER BY __src_rownum ASC) AS __rn
                FROM isld_pure
                WHERE {where} AND {unit_key} IS NOT NULL
            ) WHERE __rn = 1
            LIMIT ?
        """
    else:
        sql = f"SELECT * FROM isld_pure WHERE {where} LIMIT ?"
    params.append(limit)
    return sql, params


# -----------------------------------------------------------------------------
# 各モード実装
# -----------------------------------------------------------------------------

def _run_raw(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: {config_path} が見つかりません", file=sys.stderr)
        sys.exit(1)
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    scope = cfg.get("defaults", {}).get("scope", {})
    unique_unit = cfg.get("defaults", {}).get("unique", {}).get("unit", "app")
    aliases = get_company_aliases(cfg)

    sql, params = build_scope_sql(scope, "raw", unique_unit, args.limit, aliases)
    if args.show_sql:
        print("[SQL]", sql, file=sys.stderr)
        print("[PARAMS]", params, file=sys.stderr)
    rows = fetch_rows(conn, sql, params)
    if rows:
        out_path = Path(args.out) if args.out else Path("debug_raw_sample.csv")
        write_csv(rows, out_path, exclude_cols={"__rn"})
        print(f"出力: {out_path} ({len(rows)} 行)", file=sys.stderr)
    else:
        print("結果: 0 件", file=sys.stderr)


def _run_unique(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: {config_path} が見つかりません", file=sys.stderr)
        sys.exit(1)
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    scope = cfg.get("defaults", {}).get("scope", {})
    unique_unit = cfg.get("defaults", {}).get("unique", {}).get("unit", "app")
    aliases = get_company_aliases(cfg)

    sql, params = build_scope_sql(scope, "unique", unique_unit, args.limit, aliases)
    if args.show_sql:
        print("[SQL]", sql, file=sys.stderr)
        print("[PARAMS]", params, file=sys.stderr)
    rows = fetch_rows(conn, sql, params)
    if rows:
        out_path = Path(args.out) if args.out else Path("debug_unique_sample.csv")
        write_csv(rows, out_path, exclude_cols={"__rn"})
        print(f"出力: {out_path} ({len(rows)} 行)", file=sys.stderr)
    else:
        print("結果: 0 件", file=sys.stderr)


def _run_target(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    col, val = args.target_col, args.target_val
    try:
        val_num = int(val)
        params: list[Any] = [val_num, args.limit]
    except ValueError:
        params = [val, args.limit]
    sql = f"SELECT * FROM isld_pure WHERE [{col}] = ? LIMIT ?"
    if args.show_sql:
        print("[SQL]", sql, file=sys.stderr)
        print("[PARAMS]", params, file=sys.stderr)
    rows = fetch_rows(conn, sql, params)
    if rows:
        out_path = Path(args.out) if args.out else Path(f"debug_target_{col}_{val}.csv")
        write_csv(rows, out_path)
        print(f"出力: {out_path} ({len(rows)} 行)", file=sys.stderr)
    else:
        print(f"結果: 0 件 ({col} = {val})", file=sys.stderr)


def _run_ts(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    try:
        month_start, month_end = parse_month_arg(args.date)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    country = args.country.strip().upper()[:2]
    country_prefix = f"{country} %"

    # alias 取得: --config があればその cfg、無ければ内蔵のみ
    cfg: dict | None = None
    if getattr(args, "config", None):
        config_path = Path(args.config)
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                cfg = json.load(f)
    aliases = get_company_aliases(cfg)
    company_patterns = resolve_company_patterns(args.company, aliases)
    company_frag, company_params = _company_where_from_patterns(company_patterns)

    if args.show_sql:
        print("[company_patterns (alias展開後)]", [f"%{p}%" for p in company_patterns], file=sys.stderr)
        print("[country prefix]", country_prefix, file=sys.stderr)
        print("[month_start]", month_start, "[month_end]", month_end, file=sys.stderr)

    sql = f"""
    SELECT * FROM isld_pure
    WHERE {company_frag}
      AND Country_Of_Registration LIKE ?
      AND PBPA_APP_DATE >= ?
      AND PBPA_APP_DATE < ?
    ORDER BY PBPA_APP_DATE, PATT_APPLICATION_NUMBER
    """
    params = company_params + [country_prefix, month_start, month_end]
    if args.show_sql:
        print("[SQL]", sql.strip(), file=sys.stderr)
        print("[PARAMS]", params, file=sys.stderr)

    rows = fetch_rows(conn, sql, params)

    if not rows:
        print("結果: 0 件", file=sys.stderr)
        print("  company_patterns (alias展開後のLIKE一覧):", [f"%{p}%" for p in company_patterns], file=sys.stderr)
        print("  country prefix:", country_prefix, file=sys.stderr)
        print("  month_start:", month_start, "month_end:", month_end, file=sys.stderr)
        return

    out_path = Path(args.out) if args.out else Path(f"ts_{args.company[:20].replace(' ', '_')}_{month_start}_{country}.csv")
    write_csv(rows, out_path)
    print(f"出力: {out_path} ({len(rows)} 行)", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="デバッグ用サンプル抽出")
    parser.add_argument("--mode", choices=["raw", "unique", "target", "ts"], default="raw",
                        help="raw / unique / target / ts")
    parser.add_argument("--config", default="example_ana/config.json", help="config.json パス (raw/unique で使用)")
    parser.add_argument("--db", default="work.sqlite", help="SQLite DB パス")
    parser.add_argument("--limit", type=int, default=100, help="出力行数上限 (raw/unique/target)")
    parser.add_argument("--out", default=None, help="出力 CSV パス（親ディレクトリは自動作成）")
    parser.add_argument("--target-col", default=None, help="ターゲット列名 (mode=target)")
    parser.add_argument("--target-val", default=None, help="ターゲット値 (mode=target)")
    parser.add_argument("--company", default=None, help="企業名 (mode=ts)。alias キーまたは生文字列")
    parser.add_argument("--date", default=None, help="対象月 YYYY-MM or YYYY-MM-DD (mode=ts)")
    parser.add_argument("--country", default=None, help="国コード 2文字 (mode=ts)")
    parser.add_argument("--show-sql", action="store_true", help="SQL と params を stderr に表示")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: {db_path} が見つかりません", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.create_function("norm_company", 1, _normalize_company)

    if args.mode == "raw":
        _run_raw(conn, args)
    elif args.mode == "unique":
        _run_unique(conn, args)
    elif args.mode == "target":
        if not args.target_col or not args.target_val:
            print("ERROR: --target-col と --target-val を指定してください", file=sys.stderr)
            sys.exit(1)
        _run_target(conn, args)
    elif args.mode == "ts":
        if not args.company or not args.date or not args.country:
            print("ERROR: --company, --date, --country を指定してください (mode=ts)", file=sys.stderr)
            sys.exit(1)
        _run_ts(conn, args)

    conn.close()


if __name__ == "__main__":
    main()
