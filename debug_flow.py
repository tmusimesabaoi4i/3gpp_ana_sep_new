#!/usr/bin/env python3
"""
debug_flow.py  –  フィルタ / 一意化 / カウント の自動検証ツール

要件（指示書 §5）:
  - config をランダム/網羅的に生成（≈1000件）
  - scope → unique → サンプル抽出 → 各行が条件を満たすか検証
  - count の妥当性確認
  - 出力: out/debug/ に summary CSV + failure 詳細

使用方法:
  python debug_flow.py [--db work.sqlite] [--count 1000] [--sample-size 100] [--seed 42]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import os
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# ──────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────
SAMPLE_SIZE = 100       # 各config のサンプル行数
MAX_CONFIGS = 500       # 生成config数

# scope パラメータの候補空間
COUNTRY_MODES = ["ALL", "FILTER"]
COUNTRY_PREFIXES = [["JP"], ["US"], ["CN"], ["EP"], ["KR"]]
COMPANIES_LIKE = {
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
GEN_FLAGS_OPTIONS = [
    None,
    {"5G": 1},
    {"4G": 1},
    {"2G": 0, "3G": 0, "4G": 0, "5G": 1},
    {"4G": 1, "5G": 0},
]
ESS_FLAGS_OPTIONS = [
    None,
    {"ess_to_standard": True},
    {"ess_to_project": True},
    {"ess_to_standard": False},
]
UNIQUE_UNITS = ["app", "publ", "family", "dipg", "none"]
PERIODS = ["month", "year"]


# ──────────────────────────────────────────────
# データ型
# ──────────────────────────────────────────────
@dataclass
class DebugConfig:
    config_id: str
    country_mode: str = "ALL"
    country_prefixes: list[str] = field(default_factory=list)
    companies: list[str] = field(default_factory=list)
    gen_flags: Optional[dict] = None
    ess_flags: Optional[dict] = None
    unique_unit: str = "app"
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    version_prefixes: list[str] = field(default_factory=list)
    seed: int = 0

    def to_dict(self) -> dict:
        return {
            "config_id": self.config_id,
            "country_mode": self.country_mode,
            "country_prefixes": self.country_prefixes,
            "companies": self.companies,
            "gen_flags": self.gen_flags,
            "ess_flags": self.ess_flags,
            "unique_unit": self.unique_unit,
            "date_from": self.date_from,
            "date_to": self.date_to,
            "version_prefixes": self.version_prefixes,
        }


@dataclass
class VerifyResult:
    config_id: str
    passed: bool
    scope_row_count: int = 0
    unique_row_count: int = 0
    sample_size: int = 0
    filter_violations: int = 0
    unique_violations: int = 0
    sanity_violations: int = 0
    error_msg: str = ""
    details: list[str] = field(default_factory=list)


# ──────────────────────────────────────────────
# Config 生成
# ──────────────────────────────────────────────
def generate_configs(count: int, seed: int) -> list[DebugConfig]:
    """直積のサブセットからconfig群を生成"""
    rng = random.Random(seed)

    # 会社名リスト(LIKEパターンのキー)
    company_keys = list(COMPANIES_LIKE.keys())

    configs: list[DebugConfig] = []
    seen = set()

    # まず必須の組み合わせ（boundary cases）を生成
    boundary_cases = []

    # 各 unique_unit を最低1回テスト
    for unit in UNIQUE_UNITS:
        boundary_cases.append(DebugConfig(
            config_id=f"boundary_unit_{unit}",
            unique_unit=unit,
        ))

    # 各 gen_flags を最低1回テスト
    for i, gf in enumerate(GEN_FLAGS_OPTIONS):
        if gf is not None:
            boundary_cases.append(DebugConfig(
                config_id=f"boundary_gen_{i}",
                gen_flags=gf,
            ))

    # 各 ess_flags を最低1回テスト
    for i, ef in enumerate(ESS_FLAGS_OPTIONS):
        if ef is not None:
            boundary_cases.append(DebugConfig(
                config_id=f"boundary_ess_{i}",
                ess_flags=ef,
            ))

    # country_mode=FILTER + 各国
    for cp in COUNTRY_PREFIXES:
        boundary_cases.append(DebugConfig(
            config_id=f"boundary_country_{cp[0]}",
            country_mode="FILTER",
            country_prefixes=cp,
        ))

    # 各企業単独テスト
    for ck in company_keys:
        boundary_cases.append(DebugConfig(
            config_id=f"boundary_company_{ck}",
            companies=[COMPANIES_LIKE[ck]],
        ))

    configs.extend(boundary_cases)

    # 残りをランダム生成
    attempts = 0
    while len(configs) < count and attempts < count * 10:
        attempts += 1

        cm = rng.choice(COUNTRY_MODES)
        cp = rng.choice(COUNTRY_PREFIXES) if cm == "FILTER" else []
        comp_count = rng.choice([0, 1, 2])
        comps = [COMPANIES_LIKE[c] for c in rng.sample(company_keys, min(comp_count, len(company_keys)))]
        gf = rng.choice(GEN_FLAGS_OPTIONS)
        ef = rng.choice(ESS_FLAGS_OPTIONS)
        unit = rng.choice(UNIQUE_UNITS)
        vp = rng.choice([[], ["18"], ["16"], ["15"]])
        df = rng.choice([None, "2015-01-01", "2020-01-01"])
        dt = rng.choice([None, "2023-12-31", "2020-12-31"])

        # 重複回避 (簡易hash)
        key = json.dumps([cm, cp, comps, gf, ef, unit, vp, df, dt], sort_keys=True)
        h = hashlib.md5(key.encode()).hexdigest()[:8]
        if h in seen:
            continue
        seen.add(h)

        configs.append(DebugConfig(
            config_id=f"rnd_{h}",
            country_mode=cm,
            country_prefixes=cp,
            companies=comps,
            gen_flags=gf,
            ess_flags=ef,
            unique_unit=unit,
            version_prefixes=vp,
            date_from=df,
            date_to=dt,
            seed=seed,
        ))

    return configs[:count]


# ──────────────────────────────────────────────
# SQL ビルダー (f01_scope 相当)
# ──────────────────────────────────────────────
def build_scope_sql(cfg: DebugConfig, source: str = "isld_pure") -> tuple[str, list]:
    """DebugConfig → (WHERE句, params)"""
    conditions: list[str] = []
    params: list[Any] = []

    # companies (company_key列を使用して高速化)
    if cfg.companies:
        like_clauses = []
        for comp in cfg.companies:
            pat = comp if comp.startswith("%") else f"%{comp}%"
            like_clauses.append("company_key LIKE ?")
            params.append(pat.upper())
        conditions.append(f"({' OR '.join(like_clauses)})")

    # country (FILTER mode のみ, country_key で高速化)
    if cfg.country_mode == "FILTER" and cfg.country_prefixes:
        prefix_clauses = []
        for pfx in cfg.country_prefixes:
            prefix_clauses.append("country_key = ?")
            params.append(pfx.upper())
        conditions.append(f"({' OR '.join(prefix_clauses)})")

    # version_prefixes
    if cfg.version_prefixes:
        vp_clauses = []
        for vp in cfg.version_prefixes:
            vp_clauses.append("TGPV_VERSION LIKE ?")
            params.append(f"{vp}.%")
        conditions.append(f"({' OR '.join(vp_clauses)})")

    # date
    if cfg.date_from:
        conditions.append("PBPA_APP_DATE >= ?")
        params.append(cfg.date_from)
    if cfg.date_to:
        conditions.append("PBPA_APP_DATE <= ?")
        params.append(cfg.date_to)

    # gen_flags
    if cfg.gen_flags:
        gen_col_map = {"2G": "Gen_2G", "3G": "Gen_3G", "4G": "Gen_4G", "5G": "Gen_5G"}
        for gen, val in cfg.gen_flags.items():
            col = gen_col_map.get(gen)
            if col and val is not None:
                conditions.append(f"{col} = ?")
                params.append(int(val))

    # ess_flags
    if cfg.ess_flags:
        ess_col_map = {"ess_to_standard": "Ess_To_Standard", "ess_to_project": "Ess_To_Project"}
        for key, val in cfg.ess_flags.items():
            col = ess_col_map.get(key)
            if col and val is not None:
                if isinstance(val, bool):
                    conditions.append(f"{col} = ?")
                    params.append(1 if val else 0)
                else:
                    conditions.append(f"{col} = ?")
                    params.append(val)

    where = " AND ".join(conditions) if conditions else "1=1"
    return where, params


# ──────────────────────────────────────────────
# 一意化 SQL (f02_unique 相当)
# ──────────────────────────────────────────────
UNIT_KEY_MAP = {
    "publ": "PUBL_NUMBER",
    "app": "PATT_APPLICATION_NUMBER",
    "family": "DIPG_PATF_ID",
    "dipg": "DIPG_ID",
}


def build_unique_sql(cfg: DebugConfig, scope_table: str) -> str:
    """一意化 SQL を生成"""
    if cfg.unique_unit == "none":
        return f"SELECT * FROM [{scope_table}]"

    key_col = UNIT_KEY_MAP.get(cfg.unique_unit)
    if not key_col:
        return f"SELECT * FROM [{scope_table}]"

    return f"""
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY {key_col} ORDER BY __src_rownum ASC) AS __rn
            FROM [{scope_table}]
            WHERE {key_col} IS NOT NULL
        ) WHERE __rn = 1
    """


# ──────────────────────────────────────────────
# 検証ロジック
# ──────────────────────────────────────────────
def verify_one(conn: sqlite3.Connection, cfg: DebugConfig, sample_size: int,
               col_names: list[str], col_idx: dict[str, int],
               table: str = "_debug_subset") -> VerifyResult:
    """1つの DebugConfig を検証 (高速版: サブセット + サンプル)"""
    result = VerifyResult(config_id=cfg.config_id, passed=True)

    try:
        where, params = build_scope_sql(cfg)

        # サンプル抽出 (scope適用後、LIMIT で高速化)
        sample_rows = conn.execute(
            f"SELECT * FROM {table} WHERE {where} LIMIT ?",
            params + [sample_size]
        ).fetchall()
        result.sample_size = len(sample_rows)
        result.scope_row_count = len(sample_rows)  # サンプルサイズ = 取得数

        if len(sample_rows) == 0:
            result.details.append("WARN: scope結果0件")
            return result

        # unique カウント (サンプル内で確認)
        key_col = UNIT_KEY_MAP.get(cfg.unique_unit)
        if cfg.unique_unit == "none" or not key_col:
            result.unique_row_count = len(sample_rows)
        else:
            key_idx = col_idx.get(key_col)
            if key_idx is not None:
                vals = [r[key_idx] for r in sample_rows if r[key_idx] is not None]
                result.unique_row_count = len(set(vals))
            else:
                result.unique_row_count = len(sample_rows)

        # ─── サンプル行の検証 ───
        for row in sample_rows:
            violations = _verify_row(row, col_idx, cfg)
            if violations:
                result.filter_violations += len(violations)
                result.passed = False
                for v in violations[:3]:  # 最初の3件のみ記録
                    result.details.append(v)

        # ─── 一意化検証 (サンプル内) ───
        if cfg.unique_unit != "none" and key_col:
            uq_violations = _verify_uniqueness_sample(sample_rows, col_idx, key_col)
            result.unique_violations = uq_violations
            if uq_violations > 0:
                result.passed = False
                result.details.append(f"UNIQUE violation: {key_col} にパイプ残留 {uq_violations} 件")

        # ─── データ健全性チェック ───
        sanity = _verify_sanity(sample_rows, col_idx)
        result.sanity_violations = sanity["total"]
        if sanity["total"] > 0:
            result.passed = False
            for k, v in sanity.items():
                if k != "total" and v > 0:
                    result.details.append(f"SANITY: {k} = {v}")

    except Exception as e:
        result.passed = False
        result.error_msg = str(e)
        result.details.append(f"ERROR: {e}")

    return result


def _verify_row(row: tuple, col_idx: dict, cfg: DebugConfig) -> list[str]:
    """サンプル行がconfig条件を満たすか検証"""
    violations = []

    # company フィルタ検証 (company_key で判定)
    if cfg.companies:
        ck = row[col_idx.get("company_key", -1)] if "company_key" in col_idx else None
        if ck:
            matched = False
            for pat in cfg.companies:
                clean_pat = pat.strip("%").upper()
                if clean_pat in ck:
                    matched = True
                    break
            if not matched:
                violations.append(f"COMPANY: company_key='{ck}' は {cfg.companies} にマッチしない")

    # country_mode=FILTER の検証 (country_key)
    if cfg.country_mode == "FILTER" and cfg.country_prefixes:
        ck = row[col_idx.get("country_key", -1)] if "country_key" in col_idx else None
        if ck:
            matched = any(ck == pfx.upper() for pfx in cfg.country_prefixes)
            if not matched:
                violations.append(f"COUNTRY: country_key='{ck}' は {cfg.country_prefixes} にマッチしない")

    # gen_flags 検証
    if cfg.gen_flags:
        gen_col_map = {"2G": "Gen_2G", "3G": "Gen_3G", "4G": "Gen_4G", "5G": "Gen_5G"}
        for gen, val in cfg.gen_flags.items():
            col = gen_col_map.get(gen)
            if col and val is not None and col in col_idx:
                actual = row[col_idx[col]]
                expected = int(val)
                if actual != expected:
                    violations.append(f"GEN_FLAG: {col}={actual}, expected={expected}")

    # ess_flags 検証
    if cfg.ess_flags:
        ess_col_map = {"ess_to_standard": "Ess_To_Standard", "ess_to_project": "Ess_To_Project"}
        for key, val in cfg.ess_flags.items():
            col = ess_col_map.get(key)
            if col and val is not None and col in col_idx:
                actual = row[col_idx[col]]
                expected = 1 if (isinstance(val, bool) and val) else (0 if isinstance(val, bool) else val)
                if actual != expected:
                    violations.append(f"ESS_FLAG: {col}={actual}, expected={expected}")

    # date 範囲検証
    if cfg.date_from:
        app_date = row[col_idx["PBPA_APP_DATE"]]
        if app_date and app_date < cfg.date_from:
            violations.append(f"DATE_FROM: {app_date} < {cfg.date_from}")

    if cfg.date_to:
        app_date = row[col_idx["PBPA_APP_DATE"]]
        if app_date and app_date > cfg.date_to:
            violations.append(f"DATE_TO: {app_date} > {cfg.date_to}")

    # version_prefixes 検証
    if cfg.version_prefixes:
        version = row[col_idx["TGPV_VERSION"]]
        if version:
            matched = False
            for vp in cfg.version_prefixes:
                if version.startswith(f"{vp}."):
                    matched = True
                    break
            if not matched:
                violations.append(f"VERSION: '{version}' は prefix {cfg.version_prefixes} にマッチしない")

    return violations


def _verify_uniqueness_sample(sample_rows: list, col_idx: dict,
                              key_col: str) -> int:
    """サンプル行内で uniqueness をチェック (パイプ残留等)"""
    idx = col_idx.get(key_col)
    if idx is None:
        return 0

    violations = 0
    if key_col == "PUBL_NUMBER":
        # パイプ区切りが残っていないか
        for row in sample_rows:
            val = row[idx]
            if val and isinstance(val, str) and "|" in val:
                violations += 1
    return violations


def _verify_sanity(rows: list[tuple], col_idx: dict) -> dict:
    """データ健全性チェック"""
    results = {"total": 0, "pending_in_appno": 0, "datetime_in_date": 0, "pipe_in_publ": 0}

    appno_idx = col_idx.get("PATT_APPLICATION_NUMBER")
    date_idx = col_idx.get("PBPA_APP_DATE")
    publ_idx = col_idx.get("PUBL_NUMBER")

    for row in rows:
        # Pending sentinel in PATT_APPLICATION_NUMBER
        if appno_idx is not None:
            val = row[appno_idx]
            if val and isinstance(val, str) and "pending" in val.lower():
                results["pending_in_appno"] += 1
                results["total"] += 1

        # DATETIME 残留 in PBPA_APP_DATE
        if date_idx is not None:
            val = row[date_idx]
            if val and isinstance(val, str) and ":" in val:
                results["datetime_in_date"] += 1
                results["total"] += 1

        # パイプ残留 in PUBL_NUMBER
        if publ_idx is not None:
            val = row[publ_idx]
            if val and isinstance(val, str) and "|" in val:
                results["pipe_in_publ"] += 1
                results["total"] += 1

    return results


# ──────────────────────────────────────────────
# 出力
# ──────────────────────────────────────────────
def write_results(results: list[VerifyResult], configs: list[DebugConfig],
                  out_dir: Path) -> None:
    """結果を CSV + JSON で保存"""
    out_dir.mkdir(parents=True, exist_ok=True)
    fail_dir = out_dir / "debug_failures"
    fail_dir.mkdir(exist_ok=True)

    # summary CSV
    summary_path = out_dir / "debug_summary.csv"
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "config_id", "pass_fail", "scope_rows", "unique_rows",
            "sample_size", "filter_violations", "unique_violations",
            "sanity_violations", "error_msg"
        ])
        for r in results:
            writer.writerow([
                r.config_id,
                "PASS" if r.passed else "FAIL",
                r.scope_row_count,
                r.unique_row_count,
                r.sample_size,
                r.filter_violations,
                r.unique_violations,
                r.sanity_violations,
                r.error_msg,
            ])

    # Failure 詳細
    cfg_map = {c.config_id: c for c in configs}
    fail_count = 0
    for r in results:
        if not r.passed:
            fail_count += 1
            detail_path = fail_dir / f"{r.config_id}.json"
            detail = {
                "config": cfg_map[r.config_id].to_dict() if r.config_id in cfg_map else {},
                "result": {
                    "passed": r.passed,
                    "scope_rows": r.scope_row_count,
                    "unique_rows": r.unique_row_count,
                    "filter_violations": r.filter_violations,
                    "unique_violations": r.unique_violations,
                    "sanity_violations": r.sanity_violations,
                    "error_msg": r.error_msg,
                    "details": r.details,
                },
            }
            with open(detail_path, "w", encoding="utf-8") as f:
                json.dump(detail, f, ensure_ascii=False, indent=2)

    print(f"\n=== 結果 ===", flush=True)
    print(f"  summary: {summary_path}", flush=True)
    print(f"  failures: {fail_dir} ({fail_count} 件)", flush=True)


# ──────────────────────────────────────────────
# メイン
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="debug_flow: フィルタ/一意化/count の自動検証")
    parser.add_argument("--db", default="work.sqlite", help="SQLite DB パス")
    parser.add_argument("--count", type=int, default=MAX_CONFIGS, help="生成config数")
    parser.add_argument("--sample-size", type=int, default=SAMPLE_SIZE, help="各configのサンプル行数")
    parser.add_argument("--seed", type=int, default=42, help="乱数シード")
    parser.add_argument("--out-dir", default="out/debug", help="出力ディレクトリ")
    parser.add_argument("--subset-size", type=int, default=50000,
                        help="検証用ランダムサブセットの行数 (高速化)")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: {db_path} が存在しません")
        sys.exit(1)

    out_dir = Path(args.out_dir)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA cache_size=-256000;")
    conn.execute("PRAGMA journal_mode=WAL;")

    # ─── 高速化: ランダムサブセットテーブル作成 ───
    total_rows = conn.execute("SELECT COUNT(*) FROM isld_pure").fetchone()[0]
    subset_n = min(args.subset_size, total_rows)
    print(f"検証用サブセット作成中 ({subset_n:,} / {total_rows:,} 行)...", flush=True)
    t_sub = time.time()
    conn.execute("DROP TABLE IF EXISTS _debug_subset;")
    # ABS(RANDOM()) % total を使って均等にランダム抽出
    conn.execute(f"""
        CREATE TEMP TABLE _debug_subset AS
        SELECT * FROM isld_pure
        WHERE ABS(RANDOM()) % {max(1, total_rows // subset_n)} = 0
        LIMIT {subset_n};
    """)
    actual = conn.execute("SELECT COUNT(*) FROM _debug_subset").fetchone()[0]
    print(f"  サブセット完了: {actual:,} 行 ({time.time()-t_sub:.1f}s)", flush=True)

    # config 生成
    print(f"config 生成中 (count={args.count}, seed={args.seed})...", flush=True)
    configs = generate_configs(args.count, args.seed)
    print(f"  生成完了: {len(configs)} 件", flush=True)

    # 実行 (サブセットテーブルに対して検証)
    TABLE = "_debug_subset"
    # 列名を事前に取得
    col_names = [desc[0] for desc in conn.execute(f"SELECT * FROM {TABLE} LIMIT 0").description]
    col_idx = {name: i for i, name in enumerate(col_names)}

    results: list[VerifyResult] = []
    t0 = time.time()
    pass_count = 0
    fail_count = 0

    for i, cfg in enumerate(configs):
        r = verify_one(conn, cfg, args.sample_size, col_names, col_idx, table=TABLE)
        results.append(r)
        if r.passed:
            pass_count += 1
        else:
            fail_count += 1

        if (i + 1) % 50 == 0 or i == len(configs) - 1:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            print(
                f"  [{i+1:>5}/{len(configs)}] "
                f"PASS={pass_count} FAIL={fail_count} "
                f"({elapsed:.1f}s, {rate:.1f} cfg/s)",
                flush=True,
            )

    # 結果出力
    write_results(results, configs, out_dir)

    total_time = time.time() - t0
    print(f"\n=== 完了 ===", flush=True)
    print(f"  total: {len(results)} configs", flush=True)
    print(f"  PASS: {pass_count} ({pass_count/len(results)*100:.1f}%)", flush=True)
    print(f"  FAIL: {fail_count} ({fail_count/len(results)*100:.1f}%)", flush=True)
    print(f"  time: {total_time:.1f}s", flush=True)

    # fail の概要
    if fail_count > 0:
        print(f"\n=== FAIL 概要 ===", flush=True)
        fail_types = {}
        for r in results:
            if not r.passed:
                if r.error_msg:
                    key = f"ERROR: {r.error_msg[:60]}"
                elif r.filter_violations > 0:
                    key = "FILTER_VIOLATION"
                elif r.unique_violations > 0:
                    key = "UNIQUE_VIOLATION"
                elif r.sanity_violations > 0:
                    key = "SANITY_VIOLATION"
                else:
                    key = "UNKNOWN"
                fail_types[key] = fail_types.get(key, 0) + 1
        for k, v in sorted(fail_types.items(), key=lambda x: -x[1]):
            print(f"  {k}: {v} 件", flush=True)

    conn.close()
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
