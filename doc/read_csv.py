#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
read_csv.py

指定したCSVから「先頭N行（ヘッダ含む）」を取り出して、
- --output CUI のとき：標準出力（コマンドプロンプト）へ表示
- --output パス のとき：そのパスにCSVとして保存

実行例:
  # コマンドプロンプトに表示（ヘッダ含めて100行）
  python read_csv.py --input ../out/---.csv --output CUI

  # 取り出した100行をファイル保存
  python read_csv.py --input ../out/---.csv --output ./out/get.csv

  # 取り出す行数を変更（ヘッダ含めて200行）
  python read_csv.py --input ../out/---.csv --output CUI --n 200

  # 文字コードを明示（例: cp932）
  python read_csv.py --input ../out/---.csv --output CUI --encoding cp932
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from pathlib import Path
from typing import Optional, Tuple


def _read_sample_bytes(path: Path, size: int = 4096) -> bytes:
    with path.open("rb") as f:
        return f.read(size)


def _guess_encoding(path: Path) -> str:
    """
    よくある順に試す（日本語環境・Excel出力を想定）
    - utf-8-sig
    - utf-8
    - cp932
    - shift_jis
    """
    candidates = ["utf-8-sig", "utf-8", "cp932", "shift_jis"]
    data = _read_sample_bytes(path, 8192)

    for enc in candidates:
        try:
            data.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue

    # 最後の手段（壊れた文字は置換）
    return "utf-8"


def _sniff_dialect(sample_text: str) -> csv.Dialect:
    """
    区切り文字などを推定。失敗したらexcel（カンマ区切り）にフォールバック。
    """
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample_text, delimiters=[",", "\t", ";", "|"])
        return dialect
    except Exception:
        return csv.excel


def _open_text_for_sniff(path: Path, encoding: str) -> str:
    sample_bytes = _read_sample_bytes(path, 8192)
    # 置換でとにかく読める状態にする（sniff用途）
    return sample_bytes.decode(encoding, errors="replace")


def _prepare_stdout_utf8() -> None:
    """
    Windowsのコンソールでも崩れにくいようにstdoutをutf-8に寄せる。
    （環境によっては chcp 65001 が必要な場合あり）
    """
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", newline="")
    except Exception:
        pass


def _read_first_rows(
    input_path: Path,
    n_total_rows: int,
    encoding: str,
    dialect: csv.Dialect,
) -> list[list[str]]:
    """
    CSVとして先頭n_total_rows行を返す（ヘッダ含めてn行）。
    """
    rows: list[list[str]] = []
    with input_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, dialect=dialect)
        for i, row in enumerate(reader, start=1):
            rows.append(row)
            if i >= n_total_rows:
                break
    return rows


def _write_rows_to_stdout(rows: list[list[str]], dialect: csv.Dialect) -> None:
    _prepare_stdout_utf8()
    writer = csv.writer(sys.stdout, dialect=dialect, lineterminator=os.linesep)
    for row in rows:
        writer.writerow(row)


def _write_rows_to_file(
    output_path: Path,
    rows: list[list[str]],
    dialect: csv.Dialect,
    out_encoding: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=out_encoding, newline="") as f:
        writer = csv.writer(f, dialect=dialect, lineterminator="\n")
        for row in rows:
            writer.writerow(row)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="CSVの先頭N行（ヘッダ含む）を抽出して表示/保存します。")
    p.add_argument("--input", required=True, help="入力CSVパス")
    p.add_argument("--output", required=True, help='出力先: "CUI" または 出力ファイルパス')
    p.add_argument("--n", type=int, default=100, help="取り出す行数（ヘッダ含む）。既定: 100")
    p.add_argument(
        "--encoding",
        default="auto",
        help='入力CSVの文字コード。auto, utf-8-sig, utf-8, cp932, shift_jis など（既定: auto）',
    )
    p.add_argument(
        "--out-encoding",
        default="utf-8-sig",
        help="ファイル出力時の文字コード（既定: utf-8-sig / Excelで開きやすい）",
    )

    args = p.parse_args(argv)

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] input not found: {input_path}", file=sys.stderr)
        return 2

    n_total = int(args.n)
    if n_total <= 0:
        print("[ERROR] --n must be >= 1", file=sys.stderr)
        return 2

    if args.encoding == "auto":
        encoding = _guess_encoding(input_path)
    else:
        encoding = args.encoding

    # dialect 推定（delimiterなど）
    sample_text = _open_text_for_sniff(input_path, encoding)
    dialect = _sniff_dialect(sample_text)

    try:
        rows = _read_first_rows(input_path, n_total, encoding, dialect)
    except UnicodeDecodeError as e:
        print(f"[ERROR] decode failed with encoding={encoding}: {e}", file=sys.stderr)
        print("        Try --encoding cp932 or --encoding utf-8-sig", file=sys.stderr)
        return 2
    except csv.Error as e:
        print(f"[ERROR] csv parse failed: {e}", file=sys.stderr)
        return 2

    out = str(args.output)
    if out.strip().upper() == "CUI":
        _write_rows_to_stdout(rows, dialect)
    else:
        output_path = Path(out)
        _write_rows_to_file(output_path, rows, dialect, args.out_encoding)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
