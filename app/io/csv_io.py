"""
io/csv_io.py  –  SELECT → CSV 出力

NULL 置換は出力時にここで実施（DB 側は NULL 保持）。
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional

from app.core.types import NullPolicySpec, SelectSpec, SqlError


class CsvIO:
    """CSV 出力マネージャ"""

    @staticmethod
    def export_select(
        sio: "SqliteIO",
        select: SelectSpec,
        out_path: str | Path,
        null_policy: NullPolicySpec | None = None,
        chunk_size: int = 5000,
    ) -> int:
        """
        SelectSpec の SQL を実行し、逐次 CSV に書き出す。

        Returns
        -------
        int
            出力行数
        """
        from app.io.sqlite_io import SqliteIO

        out = Path(out_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        total = 0
        with open(out, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            # ヘッダ
            writer.writerow(select.columns)

            for chunk in sio.query_iter(select.sql, select.params, chunk_size):
                rows = [
                    _apply_null_policy(row, null_policy) for row in chunk
                ]
                writer.writerows(rows)
                total += len(rows)

        return total


def _apply_null_policy(row: tuple, policy: NullPolicySpec | None) -> list[Any]:
    """NULL → sentinel 置換"""
    if policy is None:
        return list(row)
    result = []
    for v in row:
        if v is None:
            # 型判定できないので統一で text_null を使う (簡易実装)
            result.append(policy.text_null if policy.text_null is not None else "")
        else:
            result.append(v)
    return result
