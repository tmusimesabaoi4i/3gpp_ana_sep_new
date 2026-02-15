# sample_ntt/ — NTT Docomo 固定ターゲット再現セット

## 目的
特定の識別子を使ってデータを再現・追跡できるサンプルセット。

## ファイル一覧

| ファイル | 内容 |
|---------|------|
| `config_ntt.json` | NTT Docomo × JP の実行用 config |
| `targets.json` | 固定ターゲット識別子（DIPG_ID / ETPR_ID / DIPG_PATF_ID / IPRD_REFERENCE） |
| `target_dipg_43483.csv` | DIPG_ID = 43483 の全行抽出 |
| `target_patf_20438.csv` | DIPG_PATF_ID = 20438 の全行抽出 |
| `target_iprd_2381.csv` | IPRD_ID = 2381 の全行抽出（100行上限） |

## ターゲット値

| キー | 値 | 説明 |
|------|-----|------|
| TARGET_DIPG_ID | 43483 | 特許グループ ID |
| TARGET_ETPR_ID | 876 | 企業 ID（DB列に直接は無い） |
| TARGET_DIPG_PATF_ID | 20438 | Patent Family ID |
| TARGET_IPRD_REFERENCE | ISLD-201608-010 | 宣言リファレンス（DB列に直接は無い） |

## 再現コマンド

```bash
# ターゲット抽出
python debug_jobs.py --mode target --target-col DIPG_ID --target-val 43483 --out sample_ntt/target_dipg_43483.csv

# NTT config 実行
python -m app.main --config sample_ntt/config_ntt.json
```

## 出願数時系列の CSV 出力 (mode=ts)

`debug_jobs.py` の **ts モード**で、特定企業・特定月・特定国の出願数（ts_filing_count 相当）を CSV で取得できます。

| オプション | 説明 | 例 |
|-----------|------|-----|
| `--company` | 企業名（LIKE 検索） | `"NTT DOCOMO INC."` |
| `--date` | 対象月の任意の日（YYYY-MM-DD） | `2010-11-01` |
| `--country` | 国コード（2文字） | `JP` |
| `--out` | 出力 CSV パス（任意） | `sample_ntt/out/ntt_jp_2010-11.csv` |
| `--db` | SQLite DB（省略時: work.sqlite） | `work.sqlite` |

**例: NTT DOCOMO INC. の 2010年11月・JP の出願数を CSV 化**

```bash
python debug_jobs.py --mode ts --company "NTT DOCOMO INC." --date 2010-11-01 --country JP --out sample_ntt/out/ntt_jp_2010-11.csv
```

出力 CSV の列: `country`, `company`, `bucket`, `filing_count`（指定月の 1 日が `bucket`、その月内の出願件数が `filing_count`）。
