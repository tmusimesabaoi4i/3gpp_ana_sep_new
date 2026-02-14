# ISLD Pipeline — 5分析テンプレート (A〜E) + 多シート Excel 出力

GB 級の ISLD-export.csv を一度だけ SQLite に正規化し、以後は SQLite のみを入力として  
**5 つの分析テンプレート (A〜E)** を **Config だけで** 実行し、  
**ALL(全体) + 企業別** に分割された多シート Excel を自動生成するパイプラインです。

> **エンジニア向け詳細仕様 → [README_detail.md](README_detail.md)**

---

## 前提条件

| 項目 | 要件 |
|------|------|
| Python | 3.10 以上 |
| 入力 CSV | `./ISLD-export/ISLD-export.csv`（セミコロン区切り） |
| ディスク | CSV の約 2 倍（SQLite 生成用） |
| OS | Windows / macOS / Linux |

## セットアップ

```bash
pip install -r requirements.txt

mkdir ISLD-export
# ISLD-export.csv を ISLD-export/ にコピー
```

## クイックスタート

```bash
# 1. 実行（初回は CSV→SQLite 変換で数分かかります）
python -m app.main --config config.json --excel

# 2. 出力先: out/
#   A_filing_ts.csv          ← A: 国×企業×月次 出願数推移
#   B_lag_stats.csv          ← B: 国×企業×月次 lag分布サマリ
#   C_top_specs.csv          ← C: 国×企業×月次 TGPP TopK
#   D_company_rank.csv       ← D: 国別企業ランキング
#   E_spec_company_heat.csv  ← E: Spec×会社ヒートマップ
#   analysis_results.xlsx    ← ALL_*/CO_* 多シート Excel + META
#   plan_summary.txt         ← 実行計画サマリ
```

### 2回目以降

```bash
# CSV は読み込まれません（SQLite を再利用）
python -m app.main --config config.json --excel
# → "isld_pure 既存テーブルを使用" と表示されて高速に完了
```

### 限定分析（例：Release 18 × NTT × JP）

```bash
python -m app.main --config config_r18_ntt_jp.json --excel
# → out/R18_NTT_JP/ に出力
```

---

## 5分析テンプレート一覧

| テンプレ名 | シート記号 | 出力列 | 説明 |
|-----------|-----------|--------|------|
| `ts_filing_count` | **A** | country, company, bucket, filing_count | 出願数時系列 (COUNT DISTINCT appno) |
| `ts_lag_stats` | **B** | country, company, bucket, n, min/q1/median/q3/max | lag分布サマリ (箱ひげ用) |
| `ts_top_specs` | **C** | country, company, bucket, TGPP_NUMBER, cnt, rank | TGPP_NUMBER TopK 時系列 |
| `rank_company_counts` | **D** | country, unique_unit, company, cnt, rank | 企業別ランキング (unit可変) |
| `heat_spec_company` | **E** | country, TGPP_NUMBER, company, cnt | Spec×会社ヒートマップ (縦持ち) |

---

## Excel 出力 (ALL_*/CO_* + META)

`--excel` フラグ付きで実行すると、`config.json` の `excel_output` セクションに基づいて、
1枚の Excel ファイルに以下のシートが自動生成されます。

| シート種別 | 命名規則 | 内容 |
|-----------|---------|------|
| **META** | `META` | ジョブ一覧 + シート一覧 + 生成日時 |
| **ALL** | `ALL_A_filing_ts` | 全企業の集計結果 |
| **CO_企業** | `CO_NTT_DOCOMO_A_filing_ts` | 企業ごとにフィルタされた結果 |

### excel_output 設定

```jsonc
"excel_output": {
  "enabled": true,
  "path": "out/analysis_results.xlsx",
  "companies": {              // display_key: LIKEパターン (15社)
    "Ericsson": "ERICSSON",
    "Fujitsu": "FUJITSU",
    "Huawei": "HUAWEI",
    "Kyocera": "KYOCERA",
    "LG_Electronics": "LG ELECTRONICS",
    "NEC": "NEC ",
    "Nokia": "NOKIA",
    "NTT_Docomo": "DOCOMO",
    "Panasonic": "PANASONIC",
    "Qualcomm": "QUALCOMM",
    "Samsung": "SAMSUNG",
    "Sharp": "SHARP",
    "Toyota": "TOYOTA",
    "Xiaomi": "XIAOMI",
    "ZTE": "ZTE"
  },
  "meta_sheet": true
}
```

---

## config.json の主要設定

```jsonc
{
  "env": {
    "sqlite_path": "work.sqlite",
    "isld_csv_path": "./ISLD-export/ISLD-export.csv",
    "out_dir": "out"
  },
  "defaults": {
    "scope": {
      "companies": ["NTT"],           // LIKE '%NTT%' で絞り込み
      "country_prefixes": ["JP"],     // Country LIKE 'JP %'
      "country_mode": "ALL",          // ALL | FILTER
      "gen_flags": {"5G": 1},         // 世代フラグ (Gen_5G = 1)
      "ess_flags": {"ess_to_standard": true}  // Essential フラグ
    },
    "unique": {"unit": "app"},        // publ / app / family / dipg / none
    "timeseries": {"period": "month"} // month | year
  },
  "jobs": [
    {
      "job_id": "A_filing_ts",
      "template": "ts_filing_count",
      "job_description": "出願数月次推移",       // 人間向け説明
      "filters_explain": ["unique_unit=app"],   // フィルタ説明（処理には使わない）
      "override": {}
    }
  ]
}
```

### scope で指定可能なフィルタ

| パラメータ | 型 | 説明 |
|----------|-----|------|
| `companies` | `string[]` | 企業名 LIKE フィルタ |
| `country_prefixes` | `string[]` | 国コード prefix (LIKE 'JP %') |
| `country_mode` | `"ALL"/"FILTER"` | ALL=国フィルタなし, FILTER=指定国のみ |
| `gen_flags` | `{"2G":0,"5G":1}` | 世代フラグ (0/1/null) |
| `ess_flags` | `{"ess_to_standard":true}` | Essential フラグ |
| `version_prefixes` | `string[]` | Release版 prefix (LIKE '18.%') |
| `date_from` / `date_to` | `string` | 日付範囲 |

### period（時系列粒度）

Config の `timeseries.period` で月次/年次を切り替え可能。

| 値 | bucket 形式 | 例 |
|-----|------------|-----|
| `month` | `YYYY-MM-01` | `2020-03-01` |
| `year` | `YYYY-01-01` | `2020-01-01` |

---

## デバッグオプション

```bash
# CSV→SQLite のロードだけ実行
python -m app.main --config config.json --only-load

# TEMP 生成まで（export しない）
python -m app.main --config config.json --dry-run

# 実行計画を plan_summary.txt に出力
python -m app.main --config config.json --print-plan

# 指定 func で停止
python -m app.main --config config.json --stop-after enrich
```

## よくあるエラー

| エラー | 原因 | 対処 |
|--------|------|------|
| `ConfigError: 'sqlite_path' は必須です` | config.json の env が不正 | env セクションを確認 |
| `Template 'xxx' は未登録です` | テンプレート名のタイプミス | `ts_filing_count` / `ts_lag_stats` / `ts_top_specs` / `rank_company_counts` / `heat_spec_company` のいずれか |
| CSV ロードで列崩壊 | delimiter 不一致 | 自動検出済み。CSV 先頭行を目視確認 |
| 出力が 0 件 | scope 条件が厳しすぎる | scope を空 `{}` にして全件で試す |

---

> **注意**: `.gitignore` により `*.sqlite`、`out*/`、`logs/`、`ISLD-export/` はコミットされません。

詳細な仕様・設計情報は **[README_detail.md](README_detail.md)** を参照してください。
