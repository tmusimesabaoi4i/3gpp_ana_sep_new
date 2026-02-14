#!/usr/bin/env python3
"""
null_rate_report.py — 企業×列指定で null率を出すレポートツール

使い方:
  # 全列の null率 (全企業)
  python null_rate_report.py

  # 特定企業・特定列
  python null_rate_report.py --companies NTT HUAWEI --columns PBPA_APP_DATE TGPV_VERSION Gen_5G

  # 国・期間指定
  python null_rate_report.py --country JP --date-from 2020-01-01

  # CSV出力
  python null_rate_report.py --out null_report.csv
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path


COMPANY_PATTERNS = {
    "Ericsson": "%ERICSSON%",
    "Fujitsu": "%FUJITSU%",
    "Huawei": "%HUAWEI%",
    "Kyocera": "%KYOCERA%",
    "LG_Electronics": "%LG ELECTRONICS%",
    "NEC": "%NEC %",
    "Nokia": "%NOKIA%",
    "NTT_Docomo": "%DOCOMO%",
    "Panasonic": "%PANASONIC%",
    "Qualcomm": "%QUALCOMM%",
    "Samsung": "%SAMSUNG%",
    "Sharp": "%SHARP%",
    "Toyota": "%TOYOTA%",
    "Xiaomi": "%XIAOMI%",
    "ZTE": "%ZTE%",
}


def main():
    parser = argparse.ArgumentParser(description="null率レポート")
    parser.add_argument("--db", default="work.sqlite")
    parser.add_argument("--companies", nargs="*", default=None,
                        help="企業キーワード (例: NTT HUAWEI)。未指定=全企業")
    parser.add_argument("--columns", nargs="*", default=None,
                        help="対象列名 (例: PBPA_APP_DATE Gen_5G)。未指定=全列")
    parser.add_argument("--country", default=None, help="国コード prefix (例: JP)")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--out", default=None, help="CSV出力先")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA cache_size=-128000;")

    all_cols = [r[1] for r in conn.execute("PRAGMA table_info(isld_pure)").fetchall()]
    target_cols = args.columns if args.columns else all_cols

    # WHERE 構築
    base_where = "1=1"
    base_params = []
    if args.country:
        base_where += " AND country_key = ?"
        base_params.append(args.country.upper())
    if args.date_from:
        base_where += " AND PBPA_APP_DATE >= ?"
        base_params.append(args.date_from)
    if args.date_to:
        base_where += " AND PBPA_APP_DATE <= ?"
        base_params.append(args.date_to)

    # 企業リスト
    if args.companies:
        company_filters = {}
        for c in args.companies:
            # 既知パターンマッチ
            matched = False
            for name, pat in COMPANY_PATTERNS.items():
                if c.upper() in name.upper() or c.upper() in pat.upper():
                    company_filters[name] = pat
                    matched = True
                    break
            if not matched:
                company_filters[c] = f"%{c}%"
    else:
        company_filters = {"ALL": None}
        company_filters.update(COMPANY_PATTERNS)

    results = []

    for comp_name, comp_pat in company_filters.items():
        where = base_where
        params = list(base_params)
        if comp_pat:
            where += " AND UPPER(COMP_LEGAL_NAME) LIKE UPPER(?)"
            params.append(comp_pat)

        # 1クエリで全列のNULL数を取得
        null_exprs = ", ".join(
            [f"COUNT(*)"] +
            [f"SUM(CASE WHEN [{c}] IS NULL THEN 1 ELSE 0 END)" for c in target_cols]
        )
        row = conn.execute(f"SELECT {null_exprs} FROM isld_pure WHERE {where}", params).fetchone()
        total = row[0]

        for i, col in enumerate(target_cols):
            null_count = row[i + 1]
            pct = null_count / total * 100 if total > 0 else 0
            results.append({
                "company": comp_name,
                "total_rows": total,
                "column": col,
                "null_count": null_count,
                "null_pct": round(pct, 2),
            })

    # 出力
    if args.out:
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["company", "total_rows", "column", "null_count", "null_pct"])
            w.writeheader()
            w.writerows(results)
        print(f"出力: {args.out} ({len(results)} 行)", file=sys.stderr)
    else:
        current_comp = None
        for r in results:
            if r["company"] != current_comp:
                current_comp = r["company"]
                print(f"\n{'='*60}", flush=True)
                print(f"  {current_comp}  (n={r['total_rows']:,})", flush=True)
                print(f"{'='*60}", flush=True)
            print(f"  {r['column']:35s}  NULL={r['null_count']:>10,}  ({r['null_pct']:5.1f}%)", flush=True)

    conn.close()


if __name__ == "__main__":
    main()
