"""
preprocess/isld_csv_stream_loader.py  –  GB 級 CSV → isld_pure ストリームローダー

load_if_needed() で呼ばれ、isld_pure が存在しなければ CSV を読み込む。
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Optional

from app.core.types import ConfigError, EnvSpec
from app.core.progress import AsciiProgress
from app.io.sqlite_io import SqliteIO
from app.schema import isld_pure_schema
from app.schema.isld_column_specs import COLUMN_SPECS, CSV_COLUMN_SPECS
from app.preprocess.header_resolver import resolve_headers
from app.preprocess.row_normalizer import RowNormalizer

# バッチサイズ
BATCH_SIZE = 10_000

# CSV field_size_limit を拡張 (GB 級対応)
csv.field_size_limit(sys.maxsize)


def load_if_needed(
    env: EnvSpec,
    sio: SqliteIO,
    progress: AsciiProgress,
) -> bool:
    """
    isld_pure が存在しなければ CSV からロードする。

    Returns
    -------
    bool
        True = ロード実行、False = 既に存在
    """
    # ── 1. 存在確認 ──
    if isld_pure_schema.table_exists(sio):
        progress.step("isld_pure", "既存テーブルを使用")
        return False

    csv_path = Path(env.isld_csv_path)
    if not csv_path.exists():
        raise ConfigError(f"CSV ファイルが見つかりません: {csv_path}", path="env.isld_csv_path")

    progress.start("CSV → isld_pure ロード")

    # ── 2. テーブル作成 ──
    isld_pure_schema.create_table(sio)
    sio.commit()

    # ── 3. CSV 読み込み ──
    encoding = _detect_encoding(csv_path)
    delimiter = _detect_delimiter(csv_path, encoding)
    insert_sql = isld_pure_schema.insert_sql()
    progress.step(f"encoding={encoding}, delimiter='{delimiter}'")

    with open(csv_path, "r", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)

        # ── 4. ヘッダ解決 ──
        csv_headers = next(reader)
        progress.step(f"CSV列数={len(csv_headers)}")
        mapping = resolve_headers(csv_headers, CSV_COLUMN_SPECS)
        normalizer = RowNormalizer(mapping, COLUMN_SPECS)

        # ── 5. バッチ INSERT ──
        batch: list[list] = []
        rownum = 0

        for raw_row in reader:
            rownum += 1
            values = normalizer.normalize_row(raw_row, rownum)
            batch.append(values)

            if len(batch) >= BATCH_SIZE:
                _flush_batch(sio, insert_sql, batch)
                batch.clear()
                progress.update(
                    rownum,
                    invalid_date=normalizer.stats.invalid_date,
                    invalid_int=normalizer.stats.invalid_int,
                    null_count=normalizer.stats.null_count,
                )

        # 残り
        if batch:
            _flush_batch(sio, insert_sql, batch)

    # ── 6. インデックス作成 ──
    progress.step("インデックス作成中...")
    isld_pure_schema.create_indexes(sio)
    sio.commit()

    # ── 7. 完了 ──
    progress.update_final(
        rownum,
        invalid_date=normalizer.stats.invalid_date,
        invalid_int=normalizer.stats.invalid_int,
        invalid_bool=normalizer.stats.invalid_bool,
        null_count=normalizer.stats.null_count,
    )
    progress.finish(f"{rownum:,} 行ロード完了")
    return True


def _flush_batch(sio: SqliteIO, insert_sql: str, batch: list[list]) -> None:
    """バッチを 1 トランザクションで INSERT"""
    with sio.transaction():
        sio.executemany(insert_sql, batch)


def _detect_encoding(path: Path) -> str:
    """BOM の有無で encoding を判定"""
    with open(path, "rb") as f:
        head = f.read(4)
    if head[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    return "utf-8"


def _detect_delimiter(path: Path, encoding: str) -> str:
    """ヘッダ行からデリミタを自動検出 (; or , or \\t)"""
    with open(path, "r", encoding=encoding) as f:
        first_line = f.readline()
    # セミコロンがカンマより多ければ ; 区切りと判定
    n_semi = first_line.count(";")
    n_comma = first_line.count(",")
    n_tab = first_line.count("\t")
    if n_semi > n_comma and n_semi > n_tab:
        return ";"
    if n_tab > n_comma:
        return "\t"
    return ","
