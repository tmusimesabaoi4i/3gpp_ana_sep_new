# ISLD Pipeline — エンジニア向け詳細仕様書

> **ユーザ向けクイックスタート → [README.md](README.md)**

---

## 目次

1. [全体アーキテクチャ](#1-全体アーキテクチャ)
2. [データ設計：isld_pure](#2-データ設計isld_pure)
3. [Normalization（CSV→SQLite）](#3-normalizationcsvsqlite)
4. [パイプライン設計](#4-パイプライン設計)
5. [Config 仕様](#5-config-仕様)
6. [Func 一覧と契約](#6-func-一覧と契約)
7. [5分析テンプレート（A〜E）仕様](#7-5分析テンプレートae仕様)
8. [Excel 多シート出力（ALL_*/CO_*/META）](#8-excel-多シート出力)
9. [世代フラグ・Essential フラグの意味と指定方法](#9-世代フラグessentialフラグ)
10. [PUBL_NUMBER 複数値問題と DIPG_PATF_ID 代替キー](#10-publ_number-複数値問題)
11. [本番データの不整合と吸収策](#11-本番データの不整合と吸収策)
12. [新しい Flow を追加する方法](#12-新しい-flow-を追加する方法)
13. [検証・監査手順](#13-検証監査手順)
14. [運用ルール](#14-運用ルール)

---

## 1. 全体アーキテクチャ

```
ISLD-export.csv (GB級, セミコロン区切り)
       │
       │  初回のみ (load_if_needed)
       ▼
┌──────────────┐
│  isld_pure   │  ← 唯一の永続テーブル (SQLite)
│  (正規化済)  │     pure-safe: trim, 日付ISO, bool 0/1, 特許番号正規化
└──────┬───────┘
       │
       │  ジョブ実行ごと (TEMP + SELECT ref)
       ▼
  scope (TEMP) → [enrich (TEMP)] → SELECT ref → CSV export
       │                                           │
       │           ┌──────────────────┐             │
       │           │  --excel 指定時  │             │
       │           │  CSV → 多シート  │             │
       │           │  Excel 統合     │             │
       │           └──────────────────┘             │
       └──────── cleanup (DROP TEMP) ◄──────────────┘
```

### 原則
- **永続は `isld_pure` のみ** — 派生テーブルは全て TEMP
- **CSV は一度だけ読む** — 2 回目以降は SQLite を直接利用
- **SELECT ref は DB に保存しない** — メモリ上の SelectRegistry で管理
- **NULL 置換は出力時のみ** — DB 内は SQL NULL を保持
- **5テンプレート (A〜E) に統一** — 旧 dash_A/B/C, ts_count, topn_extract は削除済み

### リポジトリ構成

```
app/
├── main.py                  # エントリポイント (--excel, --print-plan 対応)
├── config/                  # Config ロード・検証・コンパイル
│   ├── loader.py            # JSON → dict
│   ├── validate.py          # バリデーション (ホワイトリスト)
│   ├── merge.py             # deep_merge (defaults + override)
│   └── compile.py           # dict → JobSpec[]
├── core/                    # パイプラインコア
│   ├── types.py             # 全 Spec 型 + 例外 (ScopeSpec, ExcelOutputSpec 等)
│   ├── plan.py              # Plan (FuncRef の配列)
│   ├── executor.py          # Plan 実行エンジン
│   └── progress.py          # ASCII プログレス表示
├── schema/                  # isld_pure スキーマ定義
│   ├── isld_column_specs.py # ColumnSpec (列名, CSV候補, 型, normalizer)
│   └── isld_pure_schema.py  # DDL + インデックス
├── preprocess/              # CSV→SQLite ロード
│   ├── normalizer.py        # 純関数: norm_text, norm_date, norm_bool, ...
│   ├── header_resolver.py   # CSV ヘッダ → SQL 列名マッピング
│   ├── row_normalizer.py    # 行単位の正規化 (index直参照で高速)
│   └── isld_csv_stream_loader.py  # バッチ INSERT + delimiter 自動検出
├── funcs/                   # パイプライン関数 (A〜E 分析用)
│   ├── base.py              # BaseFunc / FuncSignature / FuncResult
│   ├── library.py           # FuncLibrary (関数レジストリ)
│   ├── f01_scope.py         # 母集団フィルタ (gen_flags, ess_flags, country_mode 対応)
│   ├── f02_unique.py        # ROW_NUMBER 一意化
│   ├── f03_enrich.py        # 派生列 (decl_date, lag_days, release_num, time_bucket)
│   ├── f10_selects.py       # SELECT SQL (A〜E の5分析)
│   └── f99_cleanup.py       # TEMP DROP
├── io/                      # I/O
│   ├── sqlite_io.py         # SQLite 接続 / execute / query_iter
│   ├── csv_io.py            # CSV export (逐次書き込み, NULL 置換)
│   └── excel_io.py          # Excel export + 多シート統合 (ALL_*/CO_*/META)
└── templates/               # 5分析テンプレート (A〜E)
    ├── registry.py          # テンプレート名 → Builder マッピング
    ├── base.py              # TemplateBuilder 基底
    ├── __init__.py
    ├── ts_filing_count.py   # A: 出願数時系列
    ├── ts_lag_stats.py      # B: lag分布サマリ
    ├── ts_top_specs.py      # C: TopSpec時系列
    ├── rank_company_counts.py # D: 企業別ランキング
    └── heat_spec_company.py # E: Spec×会社ヒートマップ
```

---

## 2. データ設計：isld_pure

### 設計原則
- 原本に近いが、検索・結合・集計が壊れない程度に整形済み
- 欠損は **SQL NULL** で保持（`"NULL"` 文字列や `-1` は使わない）
- sentinel 置換は出力時（`null_policy`）で実施

### 追加カラム
| カラム | 型 | 説明 |
|--------|----|------|
| `__src_rownum` | INTEGER NOT NULL | CSV 読み込み順番号。unique の tie-break に使用 |

### インデックス
| 用途 | 対象列 |
|------|--------|
| unique 候補 | `PUBL_NUMBER`, `PATT_APPLICATION_NUMBER`, `DIPG_ID`, `DIPG_PATF_ID` |
| scope 頻出 | `Country_Of_Registration`, `PBPA_APP_DATE`, `TGPP_NUMBER`, `TGPV_VERSION` |
| 世代 | `Gen_4G`, `Gen_5G` |

### 主要列と NULL 率（本番 4,901,074 行）

| 列 | 型 | NULL 率 | 備考 |
|----|----|---------|------|
| IPRD_ID | INTEGER | 0.0% | 常に存在 |
| DIPG_ID | INTEGER | 0.0% | 常に存在 |
| DIPG_PATF_ID | INTEGER | 3.7% | **推奨 unique キー** |
| PUBL_NUMBER | TEXT | 11.8% | パイプ区切り複数値あり → 先頭のみ抽出 |
| PATT_APPLICATION_NUMBER | TEXT | 0.8% | "Pending" は NULL 化済み |
| COMP_LEGAL_NAME | TEXT | 0.0% | |
| Country_Of_Registration | TEXT | 0.0% | `"JP JAPAN"` 形式 |
| IPRD_SIGNATURE_DATE | TEXT | 0.0% | `1900-01-01` は sentinel → enrich で NULLIF |
| Reflected_Date | TEXT | 0.0% | |
| PBPA_APP_DATE | TEXT | 4.9% | YYYY-MM-DD 形式（DATETIME は正規化済み） |
| TGPP_NUMBER | TEXT | — | 3GPP 仕様番号 |
| TGPV_VERSION | TEXT | **73.7%** | データ特性。release 推定は non-null 分のみ |
| Standard | TEXT | — | 標準名称 |
| Patent_Type | TEXT | — | 宣言種別 |
| Gen_2G〜Gen_5G | INTEGER | 14.5% | 0/1/NULL（世代フラグ） |
| **Ess_To_Standard** | INTEGER | **3.9%** | 0/1/NULL（Standard への essential 宣言） |
| **Ess_To_Project** | INTEGER | **2.0%** | 0/1/NULL（Project への essential 宣言） |
| PBPA_TITLEEN | TEXT | — | 英文特許タイトル |
| Normalized_Patent | TEXT | — | 正規化済み特許番号 |
| __src_rownum | INTEGER | 0.0% | CSV 読み込み順番号（NOT NULL） |

---

## 3. Normalization（CSV→SQLite）

### ColumnSpec

`schema/isld_column_specs.py` で各列を定義：

```python
ColumnSpec(
    name_sql="PUBL_NUMBER",
    source_headers=["PUBL_NUMBER", "Publication Number"],
    type="TEXT",
    nullable=True,
    normalizer="norm_patent_no",
    db_affinity="TEXT",
    is_key_candidate=True,
)
```

### Normalizer 関数 (`preprocess/normalizer.py`)

| 関数 | 入力例 | 出力 | 失敗時 |
|------|--------|------|--------|
| `norm_text` | `"  Hello  World  "` | `"Hello World"` | `None` |
| `norm_int` | `"1,234"` | `1234` | `None` |
| `norm_real` | `"1,234.5"` | `1234.5` | `None` |
| `norm_bool` | `"TRUE"` / `"1"` / `"yes"` | `1` | `None` |
| `norm_date` | `"2024-01-15 12:00:00"` | `"2024-01-15"` | `None` |
| `norm_datetime` | `"2024-01-15"` | `"2024-01-15 00:00:00"` | `None` |
| `norm_patent_no` | `"US123 \| EP456"` | `"US123"` | `None` |
| `norm_company_name` | `"  Foo Corp.  "` | `"Foo Corp."` | `None` |

**重要**: すべて純関数。例外は投げず、失敗は `None` を返す。

**PENDING センチネル対応**: `norm_patent_no` は完全一致（"pending", "unknown", ""）に加え、部分一致（"PENDING1", "USPATENTAPPLICATIONPENDING" 等）もNULL化する。

### CSV ローダー (`preprocess/isld_csv_stream_loader.py`)

1. `isld_pure` が存在 → スキップ（CSV は読まない）
2. delimiter 自動検出（先頭行のカンマ / セミコロン / タブ出現数で判定）
3. HeaderResolver でヘッダ → SQL 列名マッピング
4. 10,000 行バッチで `executemany` INSERT
5. 全行完了後にインデックス作成

---

## 4. パイプライン設計

### 固定実行順（全テンプレート共通）

```
1. scope     → TEMP (isld_pure の WHERE フィルタ)
2. [enrich]  → TEMP (lag_days 等。B のみ必要)
3. select    → SelectRegistry にSQL登録 (DBには保存しない)
4. export    → CSV 書き出し
5. cleanup   → DROP TEMP (best-effort, export 完了後)
6. [excel]   → CSV → 多シート Excel 統合 (--excel フラグ時)
```

### Plan と FuncRef

```python
Plan = [
    FuncRef(func_name="scope",              args={...}, save_as="scope"),
    FuncRef(func_name="sel_filing_count_ts", args={...}, save_as="sel__A"),
    FuncRef(func_name="cleanup",            args={...}, save_as=None),
]
```

### TEMP 名の衝突回避
- Plan は**論理名**のみ: `scope`, `enriched`
- Executor が**物理名**を生成: `tmp__{run_id}__{job_id}__{step_no}__{logical}`
- run_id は実行時タイムスタンプ → 衝突ゼロ

---

## 5. Config 仕様

### 構造

```jsonc
{
  "env": {
    "sqlite_path": "work.sqlite",       // SQLite ファイルパス (必須)
    "isld_csv_path": "path/to/csv",     // 入力 CSV パス
    "out_dir": "out"                     // 出力先ディレクトリ (必須)
  },
  "defaults": {
    "scope": {
      "companies": [],                   // LIKE '%xxx%' (大文字比較)
      "countries": [],                   // 完全一致 IN
      "country_prefixes": [],            // LIKE 'JP %' (prefix)
      "country_mode": "ALL",             // ALL | FILTER
      "version_prefixes": [],            // LIKE '18.%'
      "gen_flags": null,                 // {"5G": 1} → Gen_5G = 1
      "ess_flags": null,                 // {"ess_to_standard": true}
      "date_from": null,
      "date_to": null
    },
    "unique": {"unit": "app"},           // publ|app|family|dipg|none
    "policies": {
      "decl_date_policy": "signature_first",
      "negative_lag_policy": "keep"
    },
    "timeseries": {"period": "month"},   // month | year
    "extra": {                           // テンプレ共通パラメータ
      "analysis_countries": ["JP","US","CN","EP","KR"],
      "include_all": true
    }
  },
  "excel_output": {                      // Excel 多シート出力設定
    "enabled": true,
    "path": "out/analysis_results.xlsx",
    "companies": {                       // display_key: LIKEパターン
      "NTT_DOCOMO": "DOCOMO",
      "HUAWEI": "HUAWEI"
    },
    "meta_sheet": true
  },
  "jobs": [
    {
      "job_id": "A_filing_ts",
      "template": "ts_filing_count",
      "job_description": "出願数月次推移",      // 人間向け説明（処理には使わない）
      "filters_explain": ["unique_unit=app"],  // フィルタ説明（処理には使わない）
      "override": {}
    }
  ]
}
```

### バリデーションルール

| 項目 | ルール |
|------|--------|
| `env.sqlite_path` | 必須 |
| `env.out_dir` | 必須 |
| `template` | `ts_filing_count` / `ts_lag_stats` / `ts_top_specs` / `rank_company_counts` / `heat_spec_company` |
| `unique.unit` | `publ` / `app` / `family` / `dipg` / `none` |
| `gen_flags` 値 | `0` / `1` / `null` |
| `country_mode` | `"ALL"` / `"FILTER"` |
| `ess_flags` 値 | `true` / `false` / `null` |

### job_description / filters_explain

Config に `job_description` と `filters_explain` を記述することで、Config を見ただけで「何をフィルタして何を出すか」が分かるようになります。

```json
{
  "job_id": "A_company_docomo_jp_5g",
  "template": "ts_filing_count",
  "job_description": "DOCOMOのJP向け5G SEP関連出願数の月次推移",
  "filters_explain": [
    "COMP_LEGAL_NAME LIKE '%DOCOMO%'",
    "Country_Of_Registration LIKE 'JP %'",
    "Gen_5G = 1",
    "unique_unit = app"
  ],
  "override": { ... }
}
```

**注意**: `job_description` と `filters_explain` は処理には一切影響しません。人間向けのドキュメントです。`--print-plan` でも出力されます。

---

## 6. Func 一覧と契約

### f01_scope — 母集団フィルタ

| 項目 | 値 |
|------|----|
| produces | TEMP |
| 入力 | `isld_pure` |
| 出力 | `scope` (フィルタ済み全列) |

フィルタ条件:
- `companies` → `UPPER(COMP_LEGAL_NAME) LIKE UPPER(?)`
- `countries` → `Country_Of_Registration IN (?)`
- `country_prefixes` → `Country_Of_Registration LIKE 'JP %'`
- `version_prefixes` → `TGPV_VERSION LIKE '18.%'`
- `date_from/date_to` → `PBPA_APP_DATE >= ? / <= ?`
- `gen_flags` → `Gen_5G = ?`
- `ess_flags` → `Ess_To_Standard = ?`
- `country_mode` → `"ALL"` (フィルタなし) / `"FILTER"` (country_prefixes が効く)

**全条件はパラメータバインド**（SQL インジェクション防止）。

### f02_unique — 一意化

| unit | キー列 |
|------|--------|
| `publ` | `PUBL_NUMBER` |
| `app` | `PATT_APPLICATION_NUMBER` |
| `family` | `DIPG_PATF_ID` |
| `dipg` | `DIPG_ID` |
| `none` | スキップ（コピー） |

### f03_enrich — 派生列生成

| 列 | ロジック |
|----|---------|
| `decl_date` | `COALESCE(NULLIF(sig, '1900-01-01'), NULLIF(ref, '1900-01-01'))` |
| `lag_days` | `JULIANDAY(decl_date) - JULIANDAY(PBPA_APP_DATE)` |
| `release_num` | TGPV_VERSION 先頭数値 |
| `time_bucket` | `STRFTIME` による month/quarter/year |

### f10_selects — SELECT 定義 (A〜E)

| ref | Func名 | 用途 |
|-----|--------|------|
| sel\_\_A | `sel_filing_count_ts` | 国×企業×bucket 出願数 |
| sel\_\_B | `sel_lag_stats` | 国×企業×bucket lag分布 |
| sel\_\_C | `sel_top_specs_ts` | 国×企業×bucket TopSpec |
| sel\_\_D | `sel_company_rank` | 国×企業ランキング |
| sel\_\_E | `sel_spec_company_heat` | Spec×会社ヒートマップ |

### f99_cleanup — TEMP 削除

`DROP TABLE IF EXISTS` を best-effort で実行。**export 完了後に実行**。

---

## 7. 5分析テンプレート（A〜E）仕様

| テンプレ名 | SELECT Func | Plan フロー | 出力列 |
|-----------|------------|------------|--------|
| `ts_filing_count` | `sel_filing_count_ts` | scope → select → cleanup | country, company, bucket, filing_count |
| `ts_lag_stats` | `sel_lag_stats` | scope → enrich → select → cleanup | country, company, bucket, n, min/q1/median/q3/max lag |
| `ts_top_specs` | `sel_top_specs_ts` | scope → select → cleanup | country, company, bucket, TGPP_NUMBER, cnt, rank |
| `rank_company_counts` | `sel_company_rank` | scope → select → cleanup | country, unique_unit, company, cnt, rank |
| `heat_spec_company` | `sel_spec_company_heat` | scope → select → cleanup | country, TGPP_NUMBER, company, cnt |

### 共通設計方針

- **国分類**: `Country_Of_Registration` の prefix（JP/US/CN/EP/KR）で CASE WHEN 分類。`ALL` は全国
- **企業フィルタ**: `scope.companies` で LIKE '%キー%'
- **二重カウント防止**: A (filing count) は `COUNT(DISTINCT PATT_APPLICATION_NUMBER)` を使用
- **unique_unit**: D (ランキング) のみ `job.unique.unit` で集計キーを切替可能
- **Config パラメータ**: `extra.analysis_countries`, `extra.include_all`, `extra.top_k`, `timeseries.period`

### A) `ts_filing_count` — 出願数時系列

**SQL 概要**: CTE で国分類 → UNION ALL（国別 + ALL）→ `COUNT(DISTINCT PATT_APPLICATION_NUMBER)` で集計

**必須要件**:
- `PATT_APPLICATION_NUMBER IS NOT NULL` かつ `PBPA_APP_DATE IS NOT NULL`
- bucket = `SUBSTR(PBPA_APP_DATE, 1, 7) || '-01'`（月次）

### B) `ts_lag_stats` — lag分布サマリ

**SQL 概要**: enrich で `lag_days` を計算 → CTE で国分類 + NTILE(4) → 四分位数近似

### C) `ts_top_specs` — TopSpec時系列

**SQL 概要**: CTE で国分類 → GROUP BY → ROW_NUMBER で rank → `rank <= top_k`

### D) `rank_company_counts` — 企業別ランキング

**SQL 概要**: CTE で国分類 → `COUNT(DISTINCT {unit_col})` → ROW_NUMBER で rank

**unit 切替**: `unique.unit=app` → PATT_APPLICATION_NUMBER、`family` → DIPG_PATF_ID

### E) `heat_spec_company` — Spec×会社ヒートマップ

**SQL 概要**: グローバル Top K Spec を特定 → INNER JOIN → 国分類 + UNION ALL → GROUP BY

---

## 8. Excel 多シート出力

### 概要

`--excel` フラグ付き実行で、CSV 出力を自動的に多シート Excel に統合します。

```bash
python -m app.main --config config.json --excel
```

### シート構成

| シート種別 | 命名規則 | 内容 |
|-----------|---------|------|
| **META** | `META` | ジョブ一覧 + 全シート一覧 + 生成日時 |
| **ALL** | `ALL_{job_id}` | 全企業の集計結果（フィルタなし） |
| **CO_企業** | `CO_{display_key}_{job_id}` | 企業ごとにフィルタした結果 |

### 企業 Key の正規化

`excel_output.companies` で `{display_key: LIKE_pattern}` を定義します。

```json
"companies": {
  "NTT_DOCOMO": "DOCOMO",
  "HUAWEI": "HUAWEI",
  "SHARP": "SHARP"
}
```

- `display_key`: シート名に使用（例: `CO_NTT_DOCOMO_A_filing_ts`）
- `LIKE_pattern`: CSV の `company` 列を部分一致フィルタ

### META シート

META シートには以下が含まれます:

1. **生成日時**
2. **ジョブ一覧**: job_id, template, job_description, scope_summary, unique_unit, period
3. **シート一覧**: シート名, 分析キー, フィルタ条件

### 設計上の注意

- Excel シート名は最大 31 文字（自動切り詰め）
- 無効文字 (`\/*?[]:`) は `_` に置換
- `company` 列が CSV に存在しない場合、CO_* シートは作成されない

---

## 9. 世代フラグ・Essential フラグの意味と指定方法

### 世代フラグ（Gen_2G / Gen_3G / Gen_4G / Gen_5G）

| 列名 | 値 | 意味 |
|------|----|------|
| `Gen_2G` | `1` | 当該特許が 2G (GSM等) に紐づく 3GPP 技術を含む |
| `Gen_2G` | `0` | 含まない |
| `Gen_2G` | `NULL` | 不明（情報なし） |
| `Gen_3G` | 同上 | 3G (UMTS/WCDMA等) |
| `Gen_4G` | 同上 | 4G (LTE等) |
| `Gen_5G` | 同上 | 5G (NR等) |

**注意**: 1つの特許が複数世代にまたがることがある（例: Gen_4G=1 かつ Gen_5G=1）。

#### Config での指定

```json
"scope": {
  "gen_flags": {
    "2G": 0,
    "3G": 0,
    "4G": 0,
    "5G": 1
  }
}
```

上記は `Gen_5G = 1 AND Gen_2G = 0 AND Gen_3G = 0 AND Gen_4G = 0`（5G のみ）を意味します。

`null` を指定すると、そのフラグは条件に含めません（任意の値を許容）:

```json
"gen_flags": {"5G": 1}   // 5G=1 のみ。他の世代は問わない
```

### Essential フラグ（Ess_To_Standard / Ess_To_Project）

これらの列は **isld_pure に実在** します（CSV の `Ess_To_Standard` / `Ess_To_Project` 列から `norm_bool` でロード）。

| 列名 | 値 | 意味 |
|------|----|------|
| `Ess_To_Standard` | `1` (true) | 指定 Standard/Work item に essential として宣言されている |
| `Ess_To_Standard` | `0` (false) | admin により Non-essential に変更された |
| `Ess_To_Standard` | `NULL` | 情報なし（宣言自体がない） |
| `Ess_To_Project` | `1` (true) | 特定 Project への essential 宣言あり |
| `Ess_To_Project` | `0` (false) | Non-essential として登録 |
| `Ess_To_Project` | `NULL` | Project がそもそも宣言されていない可能性あり |

#### 本番データの分布（4,901,074 行）

| 列 | NULL | 0 (false) | 1 (true) |
|----|-----:|----------:|---------:|
| Ess_To_Standard | 191,849 (3.9%) | 5,503 (0.1%) | 4,703,722 (96.0%) |
| Ess_To_Project | 97,204 (2.0%) | 5,477 (0.1%) | 4,798,393 (97.9%) |

**注意**:
- 大部分の宣言は Essential=1 です（96-98%）
- `Ess_To_Standard` は admin による Non-essential 変更（0）があり得ますが、ごく少数（0.1%）
- `Ess_To_Project` の NULL は Project 宣言自体がないケースです
- **Config の `ess_flags` で `false` を指定すると、値が `0` の行のみにマッチします（NULL はマッチしません）。NULL を含めたい場合は、`ess_flags` ではなく SQL レベルのカスタマイズが必要です。**

#### Config での指定

```json
"scope": {
  "ess_flags": {
    "ess_to_standard": true
  }
}
```

上記は `Ess_To_Standard = 1`（essential のみ）を意味します。

```json
"ess_flags": {
  "ess_to_standard": true,
  "ess_to_project": false
}
```

上記は `Ess_To_Standard = 1 AND Ess_To_Project = 0` を意味します。

### country_mode

| 値 | 意味 |
|-----|------|
| `"ALL"` | 国フィルタなし（`countries`/`country_prefixes` は無視される設計意図） |
| `"FILTER"` | `countries`/`country_prefixes` の条件を適用する |

```json
"scope": {
  "country_mode": "FILTER",
  "country_prefixes": ["JP"]
}
```

---

## 10. PUBL_NUMBER 複数値問題と DIPG_PATF_ID 代替キー

### 問題

本番データでは `PUBL_NUMBER` が `US123456 | EP789012` のようにパイプ区切りで複数値を持つ。

### 対応

1. **Normalization**: パイプの先頭値のみ抽出して保存（`norm_patent_no`）
2. **代替キー**: `DIPG_PATF_ID`（patent family 識別子）を推奨

### 検証結果（本番 4,901,074 行）

| 指標 | PUBL_NUMBER | DIPG_PATF_ID |
|------|-------------|--------------|
| NULL 率 | 11.8% | **3.7%** |
| unique 後件数 | 461,425 | 132,570 |
| Top10 会社一致 | — | **10/10 共通** |

**結論**: `DIPG_PATF_ID（family）` を推奨デフォルトとして採用。

---

## 11. 本番データの不整合と吸収策

| # | 不整合 | 吸収策 |
|---|--------|--------|
| 1 | CSV delimiter が `;` | `_detect_delimiter` で自動検出 |
| 2 | `PUBL_NUMBER` がパイプ区切り複数値 | 先頭値のみ抽出 |
| 3 | `PATT_APPLICATION_NUMBER` に `"Pending"` 部分一致含む | NULL 化（部分一致対応） |
| 4 | DATE 列が `YYYY-MM-DD HH:MM:SS` | `norm_date` が日付部分のみ抽出 |
| 5 | `IPRD_SIGNATURE_DATE = 1900-01-01` | enrich で `NULLIF` |
| 6 | 世代が `Gen_2G`〜`Gen_5G` 独立列 | 0/1/NULL として取り込み |
| 7 | Country が `"US UNITED STATES"` 型 | `country_prefixes` で LIKE フィルタ |
| 8 | `TGPP_TITLE` 等の想定列が不在 | 必須列にしない |
| 9 | `TGPV_VERSION` の 73.7% が NULL | データ特性として許容 |

---

## 12. 新しい Flow を追加する方法

### Step 1: SELECT Func を追加

`app/funcs/f10_selects.py` にクラスを追加:

```python
class SelMyAnalysis(BaseFunc):
    def signature(self) -> FuncSignature:
        return FuncSignature(
            name="sel_my_analysis",
            required_args=["source"],
            produces="select",
            description="カスタム分析",
        )

    def build_sql(self, ctx, args) -> FuncResult:
        source = ctx.resolve_temp(args["source"])
        sql = f"SELECT ... FROM [{source}] ..."
        return FuncResult(sql=sql, columns=[...])
```

### Step 2: Library に登録

`app/funcs/library.py`:

```python
from app.funcs.f10_selects import SelMyAnalysis
lib.register(SelMyAnalysis())
```

### Step 3: Template を作成

`app/templates/my_analysis.py`:

```python
class MyAnalysisBuilder(TemplateBuilder):
    def name(self) -> str:
        return "my_analysis"

    def build(self, job):
        plan = Plan(job_id=job.job_id)
        plan.add("scope", save_as="scope", scope_spec=job.scope)
        plan.add("sel_my_analysis", save_as="sel__F", source="scope")
        plan.add("cleanup")
        outputs = [OutputSpec(select_ref="sel__F", format="csv", filename=f"{job.job_id}.csv")]
        return plan, outputs
```

### Step 4: Registry + Validator に登録

```python
# registry.py
reg.register(MyAnalysisBuilder())

# validate.py
_ALLOWED_TEMPLATES = {..., "my_analysis"}
```

---

## 13. 検証・監査手順

### 再実行確認

```bash
# A〜E 全ジョブ + Excel 統合
python -m app.main --config config.json --excel --print-plan
```

### 実行計画確認

```bash
# plan_summary.txt に各ジョブの scope/period/filters を出力
python -m app.main --config config.json --print-plan --dry-run
```

### 再現性チェック

同一 config で 2 回実行 → 出力 CSV の SHA256 一致を確認。

---

## 14. 運用ルール

### 厳守事項

1. **永続テーブルは `isld_pure` のみ** — 派生永続テーブルを増やさない
2. **TEMP 名は run_id 付き** — 衝突ゼロ
3. **SELECT ref は DB に保存しない** — SelectRegistry のみ
4. **NULL 置換は CsvIO 側で実施** — DB を汚さない
5. **cleanup は export 完了後** — deferred cleanup パターン
6. **テンプレートは A〜E の5つ** — 旧 dash_*/ts_count/topn_extract は廃止

### デバッグモード

| フラグ | 効果 |
|--------|------|
| `--only-load` | CSV→isld_pure のみ（ジョブスキップ） |
| `--dry-run` | TEMP 生成まで（export しない） |
| `--stop-after enrich` | 指定 func で停止 |
| `--excel` | ジョブ後に多シート Excel 統合 |
| `--print-plan` | 実行計画を plan_summary.txt に出力 |
