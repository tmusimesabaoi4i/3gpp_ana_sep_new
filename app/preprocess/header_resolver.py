"""
preprocess/header_resolver.py  –  CSV ヘッダ → ColumnSpec 列割当
"""
from __future__ import annotations

import re
from typing import Optional

from app.core.types import ConfigError
from app.schema.isld_column_specs import ColumnSpec, CSV_COLUMN_SPECS

_MULTI_WS = re.compile(r"\s+")


def _normalize_header(h: str) -> str:
    """比較用ヘッダ正規化: trim + 連続空白圧縮 + 小文字"""
    return _MULTI_WS.sub(" ", h.strip()).lower()


def resolve_headers(
    csv_headers: list[str],
    column_specs: list[ColumnSpec] | None = None,
) -> dict[str, int]:
    """
    CSV ヘッダ配列と ColumnSpec 群を照合し、
    ``{name_sql: csv_index}`` の割当辞書を返す。

    Parameters
    ----------
    csv_headers : list[str]
        CSV ファイルの 1 行目から取得したヘッダ配列
    column_specs : list[ColumnSpec] | None
        照合対象 (None なら CSV_COLUMN_SPECS を使用)

    Returns
    -------
    dict[str, int]
        name_sql → csv_headers のインデックス

    Raises
    ------
    ConfigError
        必須列 (nullable=False かつ source_headers あり) が見つからない場合
    """
    if column_specs is None:
        column_specs = CSV_COLUMN_SPECS

    # csv_headers の正規化版を用意
    norm_csv = [_normalize_header(h) for h in csv_headers]

    mapping: dict[str, int] = {}

    for spec in column_specs:
        if not spec.source_headers:
            continue  # __src_rownum 等の自動列

        found = False
        for candidate in spec.source_headers:
            norm_cand = _normalize_header(candidate)
            for idx, nh in enumerate(norm_csv):
                if nh == norm_cand:
                    mapping[spec.name_sql] = idx
                    found = True
                    break
            if found:
                break

        if not found and not spec.nullable:
            raise ConfigError(
                f"必須列 '{spec.name_sql}' に対応する CSV ヘッダが見つかりません。"
                f" 候補: {spec.source_headers}",
                path="csv_header",
            )

    return mapping
