#!/usr/bin/env python3
"""
debug_jobs.py — scope/unique 適用後のデータをサンプル出力するデバッグツール

使い方:
  # scope のみ適用 → 100件CSV出力
  python debug_jobs.py --mode raw --config example_ana/config.json

  # scope + unique 適用 → 100件CSV出力
  python debug_jobs.py --mode unique --config example_ana/config.json

  # ターゲット指定抽出 (DIPG_ID 等)
  python debug_jobs.py --mode target --target-col DIPG_ID --target-val 43483
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


def main():
    parser = argparse.ArgumentParser(description="デバッグ用サンプル抽出")
    parser.add_argument("--mode", choices=["raw", "unique", "target"], default="raw",
                        help="raw=scope後, unique=scope+unique後, target=ターゲット抽出")
    parser.add_argument("--config", default="example_ana/config.json", help="config.json パス")
    parser.add_argument("--db", default="work.sqlite", help="SQLite DB パス")
    parser.add_argument("--limit", type=int, default=100, help="出力行数上限")
    parser.add_argument("--out", default=None, help="出力ファイル (デフォルト: stdout)")
    parser.add_argument("--target-col", default=None, help="ターゲット列名 (mode=target)")
    parser.add_argument("--target-val", default=None, help="ターゲット値 (mode=target)")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: {db_path} が見つかりません", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    if args.mode == "target":
        _target_mode(conn, args)
    else:
        _scope_mode(conn, args)

    conn.close()


def _scope_mode(conn: sqlite3.Connection, args):
    """scope (+ unique) 適用後のサンプルを出力"""
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: {config_path} が見つかりません", file=sys.stderr)
        sys.exit(1)

    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)

    scope = cfg.get("defaults", {}).get("scope", {})
    unique_unit = cfg.get("defaults", {}).get("unique", {}).get("unit", "app")

    # WHERE 構築
    conditions, params = _build_where(scope)
    where = " AND ".join(conditions) if conditions else "1=1"

    if args.mode == "raw":
        sql = f"SELECT * FROM isld_pure WHERE {where} LIMIT ?"
        params.append(args.limit)
        out_name = "debug_raw_sample.csv"
    else:
        # unique
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
        params.append(args.limit)
        out_name = "debug_unique_sample.csv"

    print(f"SQL: {sql[:200]}...", file=sys.stderr, flush=True)
    print(f"Params: {params}", file=sys.stderr, flush=True)

    rows = conn.execute(sql, params).fetchall()
    if not rows:
        print("結果: 0 件", file=sys.stderr)
        return

    col_names = rows[0].keys()
    # __rn を除外
    col_names = [c for c in col_names if c != "__rn"]

    out_path = Path(args.out) if args.out else Path(out_name)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        for row in rows:
            writer.writerow([row[c] for c in col_names])

    print(f"出力: {out_path} ({len(rows)} 行)", file=sys.stderr, flush=True)


def _target_mode(conn: sqlite3.Connection, args):
    """ターゲット指定抽出"""
    if not args.target_col or not args.target_val:
        print("ERROR: --target-col と --target-val を指定してください", file=sys.stderr)
        sys.exit(1)

    col = args.target_col
    val = args.target_val

    # 数値っぽければ数値比較
    try:
        val_num = int(val)
        sql = f"SELECT * FROM isld_pure WHERE [{col}] = ? LIMIT ?"
        params = [val_num, args.limit]
    except ValueError:
        sql = f"SELECT * FROM isld_pure WHERE [{col}] = ? LIMIT ?"
        params = [val, args.limit]

    rows = conn.execute(sql, params).fetchall()
    if not rows:
        print(f"結果: 0 件 ({col} = {val})", file=sys.stderr)
        return

    col_names = rows[0].keys()
    out_path = Path(args.out) if args.out else Path(f"debug_target_{col}_{val}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        for row in rows:
            writer.writerow([row[c] for c in col_names])

    print(f"出力: {out_path} ({len(rows)} 行)", file=sys.stderr, flush=True)


def _build_where(scope: dict) -> tuple[list[str], list]:
    """scope dict → WHERE条件 + params"""
    conditions = []
    params = []

    if scope.get("companies"):
        clauses = []
        for comp in scope["companies"]:
            clauses.append("UPPER(COMP_LEGAL_NAME) LIKE UPPER(?)")
            params.append(f"%{comp}%")
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


if __name__ == "__main__":
    main()
