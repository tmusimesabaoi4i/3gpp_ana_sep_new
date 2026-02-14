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
