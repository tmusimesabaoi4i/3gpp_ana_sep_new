"""
preprocess/row_normalizer.py  –  CSV 行 → SQLite 挿入用 values 配列
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from app.schema.isld_column_specs import COLUMN_SPECS, ColumnSpec
from app.preprocess.normalizer import NORMALIZER_MAP


@dataclass
class NormStats:
    """正規化の統計カウンタ"""
    total_rows: int = 0
    invalid_date: int = 0
    invalid_int: int = 0
    invalid_bool: int = 0
    null_count: int = 0


class RowNormalizer:
    """
    コンストラクタで ColumnSpec → csv_index を確定し、
    行処理では index 直参照のみで高速に変換する。
    """

    def __init__(
        self,
        header_mapping: dict[str, int],
        column_specs: list[ColumnSpec] | None = None,
    ):
        """
        Parameters
        ----------
        header_mapping : dict[str, int]
            name_sql → csv row index (HeaderResolver の出力)
        column_specs : list[ColumnSpec] | None
            使用する列仕様 (None なら COLUMN_SPECS)
        """
        self._specs = column_specs or COLUMN_SPECS
        self.stats = NormStats()

        # 各 ColumnSpec について (csv_index, normalizer_func) のペアを構築
        self._plan: list[tuple[Optional[int], Optional[callable], str]] = []
        for spec in self._specs:
            csv_idx = header_mapping.get(spec.name_sql)
            norm_fn = NORMALIZER_MAP.get(spec.normalizer) if spec.normalizer else None
            self._plan.append((csv_idx, norm_fn, spec.col_type))

    def normalize_row(self, raw_row: list[str], rownum: int) -> list[Any]:
        """
        CSV 行を SQLite 挿入用 values リストに変換する。

        Parameters
        ----------
        raw_row : list[str]
            CSV の 1 行 (文字列配列)
        rownum : int
            CSV 読み込み順行番号 (1-based)

        Returns
        -------
        list[Any]
            ColumnSpec 順の values リスト
        """
        self.stats.total_rows += 1
        values: list[Any] = []

        for csv_idx, norm_fn, col_type in self._plan:
            if csv_idx is None:
                # 自動列 (__src_rownum)
                values.append(rownum)
                continue

            # CSV から raw 値を取得
            raw_val = raw_row[csv_idx] if csv_idx < len(raw_row) else None

            if raw_val is not None and isinstance(raw_val, str):
                raw_val = raw_val.strip()
                if not raw_val:
                    raw_val = None

            if raw_val is None:
                values.append(None)
                self.stats.null_count += 1
                continue

            if norm_fn is not None:
                result = norm_fn(raw_val)
                if result is None:
                    # 正規化失敗 → stats カウント
                    if col_type == "DATE" or col_type == "DATETIME":
                        self.stats.invalid_date += 1
                    elif col_type == "INT":
                        self.stats.invalid_int += 1
                    elif col_type == "BOOL":
                        self.stats.invalid_bool += 1
                values.append(result)
            else:
                values.append(raw_val)

        return values
