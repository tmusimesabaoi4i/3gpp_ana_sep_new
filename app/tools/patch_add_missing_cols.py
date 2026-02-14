"""
patch_add_missing_cols.py  –  既存 work.sqlite に不足列を追加するパッチスクリプト

追加する列:
  CSV由来: DECL_IS_PROP_FLAG, LICD_REC_CONDI_FLAG, PBPA_PRIORITY_NUMBERS,
           Illustrative_Part, Explicitely_Disclosed
  派生:    company_key (←COMP_LEGAL_NAME), country_key (←Country_Of_Registration)

高速化のため一時テーブル方式で UPDATE する。
"""
from __future__ import annotations

import csv
import re
import sqlite3
import sys
import time
from pathlib import Path

# ──────────────────────────────────────────────
# パス設定
# ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = BASE_DIR / "work.sqlite"
CSV_PATH = BASE_DIR / "ISLD-export" / "ISLD-export.csv"

# CSV field_size_limit 拡張
csv.field_size_limit(sys.maxsize)

# ──────────────────────────────────────────────
# 正規化関数 (normalizer.py と同等)
# ──────────────────────────────────────────────
_MULTI_WS = re.compile(r"\s+")
_COMPANY_STRIP = re.compile(r"[,.\-'\"()\[\]]")


def norm_text(s: str | None) -> str | None:
    if s is None:
        return None
    s = _MULTI_WS.sub(" ", str(s).strip())
    return s if s else None


def norm_company_key(s: str | None) -> str | None:
    if s is None:
        return None
    s = str(s).strip().upper()
    if not s:
        return None
    s = _COMPANY_STRIP.sub(" ", s)
    s = _MULTI_WS.sub(" ", s).strip()
    return s if s else None


def norm_country_key(s: str | None) -> str | None:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    parts = s.split()
    if parts:
        code = parts[0].upper()
        if len(code) == 2 and code.isalpha():
            return code
    if len(s) >= 2 and s[:2].isalpha():
        return s[:2].upper()
    return s.upper()


# ──────────────────────────────────────────────
# メイン
# ──────────────────────────────────────────────
def main():
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} が存在しません")
        sys.exit(1)
    if not CSV_PATH.exists():
        print(f"ERROR: {CSV_PATH} が存在しません")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=-512000;")  # 512MB

    # ── 1. 既存列チェック ──
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(isld_pure)").fetchall()}
    print(f"既存列数: {len(existing_cols)}")

    new_csv_cols = ["DECL_IS_PROP_FLAG", "LICD_REC_CONDI_FLAG",
                    "PBPA_PRIORITY_NUMBERS", "Illustrative_Part", "Explicitely_Disclosed"]
    derived_cols = ["company_key", "country_key"]
    all_new = new_csv_cols + derived_cols

    cols_to_add = [c for c in all_new if c not in existing_cols]
    if not cols_to_add:
        print("追加すべき列はありません（全て存在済み）")
        conn.close()
        return

    print(f"追加する列: {cols_to_add}")

    # ── 2. ALTER TABLE ──
    for col in cols_to_add:
        try:
            conn.execute(f"ALTER TABLE isld_pure ADD COLUMN [{col}] TEXT;")
            print(f"  ALTER TABLE: {col} 追加")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print(f"  {col}: 既に存在")
            else:
                raise
    conn.commit()

    # ── 3. CSV由来列のバックフィル (一時テーブル方式) ──
    csv_cols_needed = [c for c in new_csv_cols if c in cols_to_add]
    if csv_cols_needed:
        print(f"\nCSV由来列バックフィル: {csv_cols_needed}")
        _backfill_csv_cols(conn, csv_cols_needed)

    # ── 4. 派生列の計算 ──
    if "company_key" in cols_to_add:
        print("\ncompany_key を計算中...")
        # SQLだけで近似: UPPER + 句読点をスペースに置換は困難なので Python で
        _compute_derived_col(conn, "company_key", "COMP_LEGAL_NAME", norm_company_key)

    if "country_key" in cols_to_add:
        print("\ncountry_key を計算中...")
        _compute_derived_col(conn, "country_key", "Country_Of_Registration", norm_country_key)

    # ── 5. インデックス追加 ──
    for col in ["company_key", "country_key"]:
        if col in cols_to_add:
            idx_name = f"idx_isld_pure_{col}"
            conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON isld_pure({col});")
            print(f"  INDEX: {idx_name}")
    conn.commit()

    # ── 6. 検証 ──
    final_cols = conn.execute("PRAGMA table_info(isld_pure)").fetchall()
    print(f"\n最終列数: {len(final_cols)}")
    for r in final_cols:
        print(f"  {r[0]:3d} | {r[1]:35s} | {r[2]}")

    conn.close()
    print("\n完了")


def _backfill_csv_cols(conn: sqlite3.Connection, cols: list[str]):
    """CSVから一時テーブルにロードし、UPDATEでisld_pureに反映"""

    # CSVヘッダ読み取り
    encoding = _detect_encoding(CSV_PATH)
    delimiter = _detect_delimiter(CSV_PATH, encoding)
    print(f"  CSV: encoding={encoding}, delimiter='{delimiter}'")

    with open(CSV_PATH, "r", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        csv_headers = [h.strip().strip('"') for h in next(reader)]

    # CSVの列インデックスを特定
    csv_header_lower = {h.lower(): i for i, h in enumerate(csv_headers)}
    col_indices: dict[str, int] = {}
    for col in cols:
        idx = csv_header_lower.get(col.lower())
        if idx is None:
            print(f"  WARNING: CSV に {col} が見つかりません。スキップ。")
        else:
            col_indices[col] = idx
            print(f"  {col} → CSV index {idx}")

    if not col_indices:
        return

    # 一時テーブル作成
    tmp_cols_ddl = ", ".join(f"[{c}] TEXT" for c in col_indices)
    conn.execute(f"CREATE TEMP TABLE _patch (__rownum INTEGER NOT NULL, {tmp_cols_ddl});")

    # CSV バッチ読み込み → 一時テーブルへ INSERT
    insert_cols = ", ".join(["__rownum"] + [f"[{c}]" for c in col_indices])
    placeholders = ", ".join("?" for _ in range(len(col_indices) + 1))
    insert_sql = f"INSERT INTO _patch ({insert_cols}) VALUES ({placeholders});"

    BATCH = 50_000
    t0 = time.time()

    with open(CSV_PATH, "r", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        next(reader)  # skip header
        batch = []
        rownum = 0
        for raw_row in reader:
            rownum += 1
            vals = [rownum]
            for col, idx in col_indices.items():
                raw = raw_row[idx].strip() if idx < len(raw_row) else None
                vals.append(norm_text(raw) if raw else None)
            batch.append(vals)
            if len(batch) >= BATCH:
                conn.executemany(insert_sql, batch)
                conn.commit()
                batch.clear()
                elapsed = time.time() - t0
                print(f"  temp INSERT: {rownum:>10,} 行 ({elapsed:.1f}s)")
        if batch:
            conn.executemany(insert_sql, batch)
            conn.commit()

    print(f"  temp テーブル完了: {rownum:,} 行 ({time.time()-t0:.1f}s)")

    # インデックス
    conn.execute("CREATE INDEX _patch_idx ON _patch(__rownum);")
    conn.commit()

    # UPDATE
    set_clauses = ", ".join(f"[{c}] = _patch.[{c}]" for c in col_indices)
    update_sql = f"""
        UPDATE isld_pure
        SET {set_clauses}
        FROM _patch
        WHERE isld_pure.__src_rownum = _patch.__rownum;
    """
    print(f"  UPDATE 実行中...")
    t1 = time.time()
    conn.execute(update_sql)
    conn.commit()
    print(f"  UPDATE 完了 ({time.time()-t1:.1f}s)")

    conn.execute("DROP TABLE IF EXISTS _patch;")
    conn.commit()


def _compute_derived_col(conn: sqlite3.Connection, target_col: str, source_col: str, func):
    """source列から派生値を計算してtarget列にUPDATE (バッチ方式)"""
    BATCH = 50_000
    t0 = time.time()

    # distinct値を取得して変換テーブルを作る (ユニーク値での変換の方が高速)
    print(f"  distinct({source_col}) 取得中...")
    distinct_rows = conn.execute(
        f"SELECT DISTINCT [{source_col}] FROM isld_pure WHERE [{source_col}] IS NOT NULL"
    ).fetchall()
    print(f"  distinct値: {len(distinct_rows):,} 件")

    # 一時テーブルで変換マップ
    conn.execute(f"CREATE TEMP TABLE _derive (src TEXT, dst TEXT);")
    batch = []
    for (src_val,) in distinct_rows:
        dst_val = func(src_val)
        batch.append((src_val, dst_val))
        if len(batch) >= BATCH:
            conn.executemany("INSERT INTO _derive (src, dst) VALUES (?, ?);", batch)
            batch.clear()
    if batch:
        conn.executemany("INSERT INTO _derive (src, dst) VALUES (?, ?);", batch)
    conn.commit()

    conn.execute("CREATE INDEX _derive_idx ON _derive(src);")
    conn.commit()

    # UPDATE
    update_sql = f"""
        UPDATE isld_pure
        SET [{target_col}] = _derive.dst
        FROM _derive
        WHERE isld_pure.[{source_col}] = _derive.src;
    """
    print(f"  UPDATE 実行中...")
    t1 = time.time()
    conn.execute(update_sql)
    conn.commit()
    print(f"  UPDATE 完了 ({time.time()-t1:.1f}s)")

    conn.execute("DROP TABLE IF EXISTS _derive;")
    conn.commit()
    print(f"  {target_col} 完了 (total: {time.time()-t0:.1f}s)")


def _detect_encoding(path: Path) -> str:
    with open(path, "rb") as f:
        head = f.read(4)
    return "utf-8-sig" if head[:3] == b"\xef\xbb\xbf" else "utf-8"


def _detect_delimiter(path: Path, encoding: str) -> str:
    with open(path, "r", encoding=encoding) as f:
        first_line = f.readline()
    n_semi = first_line.count(";")
    n_comma = first_line.count(",")
    n_tab = first_line.count("\t")
    if n_semi > n_comma and n_semi > n_tab:
        return ";"
    if n_tab > n_comma:
        return "\t"
    return ","


if __name__ == "__main__":
    main()
