#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
null_report_pivot.py

null_report.csv（縦持ち: company, total_rows, column, null_count, null_pct）
を、横持ちテーブル（company 行 × column 列）に変換して出力します。

実行例:
  # null_pct を表にする（デフォルト）
  python null_report_pivot.py --input null_report.csv --output null_report_table.csv

  # null_count を表にする
  python null_report_pivot.py --input null_report.csv --output null_report_table.csv --value null_count
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Set


def pivot_null_report(
    input_path: Path,
    output_path: Path,
    value_field: str = "null_pct",
    include_total_rows: bool = False,
) -> None:
    """
    company を行、"column" を列、value_field をセル値にしてCSV出力。
    """
    if value_field not in ("null_pct", "null_count"):
        raise ValueError('value_field must be "null_pct" or "null_count"')

    # company -> {colname -> value}
    table: Dict[str, Dict[str, str]] = {}
    # company -> total_rows (文字列のまま保持)
    total_rows_map: Dict[str, str] = {}

    # 列名（"column"フィールド）の出現順を保つ
    col_order: List[str] = []
    col_seen: Set[str] = set()

    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"company", "total_rows", "column", "null_count", "null_pct"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError(f"input CSV must have columns: {sorted(required)}")

        for row in reader:
            company = (row.get("company") or "").strip()
            colname = (row.get("column") or "").strip()
            val = (row.get(value_field) or "").strip()
            tr = (row.get("total_rows") or "").strip()

            if company == "" or colname == "":
                # 変な行はスキップ（必要ならここでraiseに変更可）
                continue

            if colname not in col_seen:
                col_seen.add(colname)
                col_order.append(colname)

            table.setdefault(company, {})[colname] = val
            if company not in total_rows_map and tr != "":
                total_rows_map[company] = tr

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # ヘッダ
    header = ["company"]
    if include_total_rows:
        header.append("total_rows")
    header.extend(col_order)

    # company順は入力出現順にしたいなら別途保持も可能だが、
    # ここでは読み込んだdict順(=挿入順)で出力する
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)

        for company, rowmap in table.items():
            out_row = [company]
            if include_total_rows:
                out_row.append(total_rows_map.get(company, ""))
            for colname in col_order:
                out_row.append(rowmap.get(colname, ""))  # 欠損は空欄
            writer.writerow(out_row)


def main() -> int:
    ap = argparse.ArgumentParser(description="null_report.csv を company×column の横持ちテーブルに変換します。")
    ap.add_argument("--input", default="null_report.csv", help="入力CSV (default: null_report.csv)")
    ap.add_argument("--output", default="null_report_table.csv", help="出力CSV (default: null_report_table.csv)")
    ap.add_argument(
        "--value",
        default="null_pct",
        choices=["null_pct", "null_count"],
        help='セルに入れる値 (default: null_pct). "null_count" も可',
    )
    ap.add_argument(
        "--include-total-rows",
        action="store_true",
        help="companyごとの total_rows 列も出力に含める",
    )

    args = ap.parse_args()
    pivot_null_report(
        input_path=Path(args.input),
        output_path=Path(args.output),
        value_field=args.value,
        include_total_rows=args.include_total_rows,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
