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
12. [会社名揺らぎ吸収の仕様](#12-会社名揺らぎ吸収の仕様)
13. [データの通り道（全体フロー詳細）](#13-データの通り道全体フロー詳細)
14. [Config指定可能キーと実在列の対応表](#14-config指定可能キーと実在列の対応表)
15. [自動検証ツール debug_flow.py](#15-自動検証ツール-debug_flowpy)
16. [新しい Flow を追加する方法](#16-新しい-flow-を追加する方法)
17. [検証・監査手順](#17-検証監査手順)
18. [運用ルール](#18-運用ルール)

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

### 列数: 30列（CSV由来 27 + 派生 2 + メタ 1）

### インデックス
| 用途 | 対象列 |
|------|--------|
| unique 候補 | `PUBL_NUMBER`, `PATT_APPLICATION_NUMBER`, `DIPG_ID`, `DIPG_PATF_ID` |
| scope 頻出 | `Country_Of_Registration`, `PBPA_APP_DATE`, `TGPP_NUMBER`, `TGPV_VERSION` |
| 世代 | `Gen_4G`, `Gen_5G` |
| 派生キー | `company_key`, `country_key` |

### 全列一覧と NULL 率（本番 4,901,074 行）

| # | DB列名 | 型 | 元CSV列名 | Normalizer | NULL率 | 備考 |
|---|--------|----|-----------|------------|--------|------|
| 0 | IPRD_ID | INTEGER | IPRD_ID | norm_int | 0.0% | 宣言ID。常に存在 |
| 1 | DECL_IS_PROP_FLAG | TEXT | DECL_IS_PROP_FLAG | norm_text | 0.0% | 所有権宣言フラグ ("0"/"1") |
| 2 | LICD_REC_CONDI_FLAG | TEXT | LICD_REC_CONDI_FLAG | norm_text | 23.6% | ライセンス条件フラグ ("0"/"1") |
| 3 | DIPG_ID | INTEGER | DIPG_ID | norm_int | 0.0% | 宣言グループID。常に存在 |
| 4 | DIPG_PATF_ID | INTEGER | DIPG_PATF_ID | norm_int | 3.7% | **推奨 unique キー** (patent family) |
| 5 | PUBL_NUMBER | TEXT | PUBL_NUMBER | norm_patent_no | 11.8% | パイプ区切り複数値 → **先頭のみ抽出** |
| 6 | PATT_APPLICATION_NUMBER | TEXT | PATT_APPLICATION_NUMBER | norm_patent_no | 0.8% | "Pending" 等 → **NULL化済** |
| 7 | COMP_LEGAL_NAME | TEXT | COMP_LEGAL_NAME | norm_company_name | 0.0% | 会社法的名称 |
| 8 | **company_key** | TEXT | ← COMP_LEGAL_NAME | **norm_company_key** | 0.0% | **派生: UPPER + 句読点除去** |
| 9 | Country_Of_Registration | TEXT | Country_Of_Registration | norm_text | 0.0% | `"JP JAPAN"` 形式 |
| 10 | **country_key** | TEXT | ← Country_Of_Registration | **norm_country_key** | 0.0% | **派生: ISO2コード ("JP")** |
| 11 | IPRD_SIGNATURE_DATE | TEXT | IPRD_SIGNATURE_DATE | norm_date | 0.0% | `1900-01-01` は sentinel |
| 12 | Reflected_Date | TEXT | Reflected_Date | norm_date | 0.0% | |
| 13 | PBPA_APP_DATE | TEXT | PBPA_APP_DATE | norm_date | 4.9% | YYYY-MM-DD（DATETIME正規化済） |
| 14 | TGPP_NUMBER | TEXT | TGPP_NUMBER | norm_text | 13.9% | 3GPP 仕様番号 |
| 15 | TGPV_VERSION | TEXT | TGPV_VERSION | norm_text | **73.7%** | release推定は non-null分のみ |
| 16 | Standard | TEXT | Standard | norm_text | 3.9% | 標準名称 |
| 17 | Patent_Type | TEXT | Patent_Type | norm_text | 0.0% | 宣言種別 |
| 18 | Gen_2G | INTEGER | 2G | norm_bool | 14.5% | 世代フラグ (0/1/NULL) |
| 19 | Gen_3G | INTEGER | 3G | norm_bool | 14.5% | |
| 20 | Gen_4G | INTEGER | 4G | norm_bool | 14.5% | |
| 21 | Gen_5G | INTEGER | 5G | norm_bool | 14.5% | |
| 22 | **Ess_To_Standard** | INTEGER | Ess_To_Standard | norm_bool | **3.9%** | Essential宣言 (0/1/NULL) |
| 23 | **Ess_To_Project** | INTEGER | Ess_To_Project | norm_bool | **2.0%** | Project Essential (0/1/NULL) |
| 24 | PBPA_TITLEEN | TEXT | PBPA_TITLEEN | norm_text | 25.8% | 英文特許タイトル |
| 25 | PBPA_PRIORITY_NUMBERS | TEXT | PBPA_PRIORITY_NUMBERS | norm_text | 4.9% | 優先権番号（パイプ区切り複数値） |
| 26 | Illustrative_Part | TEXT | Illustrative_Part | norm_text | **83.7%** | 例示箇所 |
| 27 | Explicitely_Disclosed | TEXT | Explicitely_Disclosed | norm_text | 0.0% | 明示開示フラグ ("0"/"1") |
| 28 | Normalized_Patent | TEXT | Normalized_Patent | norm_text | 0.0% | 正規化済み特許番号 |
| 29 | __src_rownum | INTEGER | (自動付番) | — | 0.0% | CSV行番号 (NOT NULL) |

### 重要企業別 件数・主要列NULL率

| 企業 | 件数 | APPNO NULL | PUBL NULL | DATE NULL | VERSION NULL | Gen_5G NULL | Ess_Std NULL | Ess_Proj NULL |
|------|-----:|-----------:|----------:|----------:|-------------:|------------:|-------------:|--------------:|
| Ericsson | 84,944 | 3.9% | 19.0% | 13.1% | 53.2% | 34.7% | 31.7% | 0.0% |
| Fujitsu | 15,418 | 0.7% | 24.3% | 8.3% | 0.8% | 0.8% | 0.8% | 0.0% |
| Huawei | 247,581 | 0.1% | 12.3% | 8.7% | **99.8%** | 2.3% | 0.1% | 0.3% |
| Kyocera | 8,420 | 0.0% | 5.8% | 5.1% | 85.0% | 0.0% | 0.0% | 0.0% |
| LG Electronics | 236,521 | 0.1% | 10.3% | 6.9% | 89.4% | 12.1% | 0.1% | 0.1% |
| NEC | 38,650 | 0.2% | 5.8% | 4.2% | 16.6% | 5.4% | 0.0% | 0.0% |
| Nokia | 207,944 | 1.2% | 15.2% | 2.6% | 53.9% | 17.9% | 8.8% | 0.1% |
| NTT Docomo | 45,742 | 1.1% | 6.7% | 2.0% | 1.0% | 0.8% | 0.5% | 2.6% |
| Panasonic | 42,668 | 0.0% | 9.8% | 4.7% | 25.9% | 0.0% | 0.0% | 0.2% |
| Qualcomm | 887,871 | 0.7% | 16.5% | 5.8% | 79.4% | 37.0% | 13.3% | 1.2% |
| Samsung | 173,352 | 0.2% | 10.2% | 1.9% | 70.7% | 10.9% | 0.0% | 0.1% |
| Sharp | 182,311 | 0.0% | 23.0% | 14.5% | 76.8% | 0.1% | 0.0% | 1.9% |
| Toyota | 1,232 | 0.0% | 3.7% | 0.0% | 0.2% | 0.0% | 0.0% | 1.0% |
| Xiaomi | 27,734 | 0.0% | 8.1% | 6.4% | **100.0%** | 0.0% | 0.0% | 0.0% |
| ZTE | 109,452 | 0.0% | 5.3% | 0.3% | 89.0% | 1.6% | 0.8% | 1.7% |

> **注意**: Huawei / Xiaomi の `TGPV_VERSION` NULL率が ≈100%。release別分析はこれらの企業で機能しない。

### company_key 上位20

| company_key | 件数 |
|-------------|-----:|
| QUALCOMM INCORPORATED | 859,640 |
| INTERDIGITAL TECHNOLOGY CORP | 756,705 |
| GUANGDONG OPPO MOBILE TELECOMMUNICATIONS CORP LTD | 338,873 |
| HUAWEI TECHNOLOGIES CO LTD | 247,581 |
| LG ELECTRONICS INC | 236,521 |
| SHARP CORPORATION | 182,311 |
| SAMSUNG ELECTRONICS CO LTD | 173,352 |
| APPLE INC | 165,131 |
| MOTOROLA INC | 162,989 |
| INTEL CORPORATION | 156,977 |

### country_key 上位10

| country_key | 件数 |
|-------------|-----:|
| US | 841,875 |
| CN | 678,953 |
| WO | 541,037 |
| JP | 464,678 |
| EP | 463,637 |
| KR | 315,670 |
| DE | 164,760 |
| AU | 148,657 |
| ES | 130,779 |
| CA | 120,890 |

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
| **`norm_company_key`** | `"NTT DOCOMO, INC."` | `"NTT DOCOMO INC"` | `None` |
| **`norm_country_key`** | `"JP JAPAN"` | `"JP"` | `None` |

**重要**: すべて純関数。例外は投げず、失敗は `None` を返す。

### 派生列の生成規則

| 列名 | 元列 | 変換ロジック |
|------|------|-------------|
| `company_key` | `COMP_LEGAL_NAME` | UPPER → 句読点(`,.-'"()[]`)をスペースに → 空白圧縮 |
| `country_key` | `Country_Of_Registration` | 先頭語を抽出し、2文字アルファベットならISO2コードとして返す |

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

`excel_output.companies` で `{display_key: LIKE_pattern}` を定義します（15社）。

```json
"companies": {
  "Ericsson": "ERICSSON",
  "Fujitsu": "FUJITSU",
  "Huawei": "HUAWEI",
  "NTT_Docomo": "DOCOMO",
  "Qualcomm": "QUALCOMM"
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

## 12. 会社名揺らぎ吸収の仕様

### 課題

同一企業が異なる法的名称で宣言している（例: "NOKIA CORPORATION" / "NOKIA TECHNOLOGIES OY"）。
Excel の CO_* シートや scope.companies のフィルタで、揺らぎを正しく吸収する必要がある。

### 仕組み

#### 1. `company_key` 列（isld_pure に永続化）

`COMP_LEGAL_NAME` から派生。`UPPER` + 句読点除去 + 空白圧縮。
- `"NTT DOCOMO, INC."` → `"NTT DOCOMO INC"`
- `"Nokia Corporation"` → `"NOKIA CORPORATION"`

#### 2. `scope.companies`（ScopeFunc / f01_scope.py）

`UPPER(COMP_LEGAL_NAME) LIKE UPPER(?)` で部分一致フィルタ。
- `"DOCOMO"` → NTT DOCOMO, INC. にマッチ
- `"NOKIA"` → NOKIA CORPORATION / NOKIA TECHNOLOGIES OY の両方にマッチ

#### 3. `excel_output.companies`（excel_io.py）

CSV出力の `company` 列に対してLIKE部分一致フィルタ。
- `{"Nokia": "NOKIA"}` → company列に "NOKIA" を含む行を CO_Nokia_* シートに出力

### 整合性

- `scope.companies` と `excel_output.companies` は独立。
  - scope: isld_pure 全体からのフィルタ（WHERE句に影響）
  - excel: CSV出力後のシート分割（WHERE句に影響しない）
- 二重フィルタにはならない（scope は母集団絞り込み、excel は出力の分類）。
- `--print-plan` で scope の WHERE 句を確認可能。

### 15社の LIKEパターン一覧

| 表示名 | LIKEパターン | マッチする company_key 例 |
|--------|-------------|--------------------------|
| Ericsson | ERICSSON | TELEFONAKTIEBOLAGET LM ERICSSON |
| Fujitsu | FUJITSU | FUJITSU LIMITED |
| Huawei | HUAWEI | HUAWEI TECHNOLOGIES CO LTD |
| Kyocera | KYOCERA | KYOCERA CORPORATION |
| LG Electronics | LG ELECTRONICS | LG ELECTRONICS INC |
| NEC | NEC  (末尾スペース) | NEC CORPORATION |
| Nokia | NOKIA | NOKIA CORPORATION, NOKIA TECHNOLOGIES OY |
| NTT Docomo | DOCOMO | NTT DOCOMO INC |
| Panasonic | PANASONIC | PANASONIC INTELLECTUAL PROPERTY CORPORATION OF AMERICA |
| Qualcomm | QUALCOMM | QUALCOMM INCORPORATED |
| Samsung | SAMSUNG | SAMSUNG ELECTRONICS CO LTD |
| Sharp | SHARP | SHARP CORPORATION |
| Toyota | TOYOTA | TOYOTA JIDOSHA KABUSHIKI KAISHA |
| Xiaomi | XIAOMI | BEIJING XIAOMI MOBILE SOFTWARE CO LTD |
| ZTE | ZTE | ZTE CORPORATION |

---

## 13. データの通り道（全体フロー詳細）

### Preprocess (CSV → SQLite)

```
ISLD-export.csv (GB級, セミコロン区切り, 40列)
    │
    ├─ _detect_delimiter()   → ";" 自動検出
    ├─ _detect_encoding()    → "utf-8" or "utf-8-sig"
    │
    ├─ HeaderResolver
    │   ├─ CSV ヘッダを小文字化 + 空白圧縮
    │   ├─ ColumnSpec.source_headers と照合
    │   └─ {name_sql: csv_index} マッピング生成
    │
    ├─ RowNormalizer
    │   ├─ 各行: csv_index → raw_val → normalizer_func → normalized_val
    │   ├─ __src_rownum = 1-based CSV行番号 (自動付番)
    │   └─ company_key, country_key も同時に生成 (同じCSV列を2回参照)
    │
    └─ バッチ INSERT (10,000行/batch)
        └─ isld_pure (30列) + インデックス作成
```

### Core Pipeline (Job実行)

```
config.json
    │
    ├─ loader.py     → JSON dict
    ├─ validate.py   → ホワイトリスト検証
    ├─ merge.py      → defaults + job.override マージ
    └─ compile.py    → JobSpec[] 生成
         │
         ├─ TemplateBuilder.build(job)
         │   └─ Plan (FuncRef の配列) + OutputSpec[] 生成
         │
         └─ Executor.execute(plan)
              │
              ├─ f01_scope (TEMP)
              │   └─ isld_pure → WHERE {conditions} → tmp_scope
              │
              ├─ [f02_unique (TEMP)] ← テンプレートが必要な場合のみ
              │   └─ ROW_NUMBER OVER (PARTITION BY {key}) → tmp_uq
              │
              ├─ [f03_enrich (TEMP)] ← B (lag_stats) のみ
              │   └─ decl_date, lag_days, time_bucket 等を SELECT で付与
              │
              ├─ f10_selects (SELECT ref)
              │   └─ 分析SQL実行 → SelectRegistry に登録
              │
              ├─ csv_io.export()
              │   └─ SELECT ref → CSV ファイル書き出し
              │
              └─ f99_cleanup
                  └─ DROP TABLE IF EXISTS (TEMP テーブル全削除)
```

### Excel 統合 (--excel 時)

```
out/*.csv (A〜E の5ファイル)
    │
    └─ excel_io.build_analysis_excel()
         ├─ META シート: job一覧 + シート一覧 + 生成日時
         ├─ ALL_{job_id} シート × 5
         └─ CO_{company}_{job_id} シート × (15社 × 5分析 = 75枚)
              └─ company列の部分一致でフィルタ
```

### Normalization 仕様一覧

| 対象 | 変換 | 関数 |
|------|------|------|
| DATETIME → DATE | `"2024-01-15 12:00:00"` → `"2024-01-15"` | `norm_date` |
| Pending sentinel → NULL | `"PENDING1"`, `"USPATENTAPPLICATIONPENDING"` → `None` | `norm_patent_no` |
| パイプ区切り → 先頭値 | `"US123 \| EP456"` → `"US123"` | `norm_patent_no` |
| 空白圧縮 | `"  Hello  World  "` → `"Hello World"` | `norm_text` |
| Bool → 0/1 | `"TRUE"` / `"yes"` / `"1"` → `1` | `norm_bool` |
| 会社名キー化 | `"NTT DOCOMO, INC."` → `"NTT DOCOMO INC"` | `norm_company_key` |
| 国名コード化 | `"JP JAPAN"` → `"JP"` | `norm_country_key` |

---

## 14. Config指定可能キーと実在列の対応表

| Config パス | 参照先 | isld_pure列 | 生成元 |
|------------|--------|------------|--------|
| scope.companies | COMP_LEGAL_NAME | 存在 | CSV |
| scope.countries | Country_Of_Registration | 存在 | CSV |
| scope.country_prefixes | Country_Of_Registration | 存在 | CSV |
| scope.country_mode | (ロジック制御) | — | — |
| scope.releases | TGPV_VERSION | 存在 | CSV |
| scope.version_prefixes | TGPV_VERSION | 存在 | CSV |
| scope.specs | TGPP_NUMBER | 存在 | CSV |
| scope.date_from/date_to | PBPA_APP_DATE | 存在 | CSV |
| scope.gen_flags.2G/3G/4G/5G | Gen_2G/3G/4G/5G | 存在 | CSV |
| scope.ess_flags.ess_to_standard | Ess_To_Standard | 存在 | CSV |
| scope.ess_flags.ess_to_project | Ess_To_Project | 存在 | CSV |
| unique.unit=publ | PUBL_NUMBER | 存在 | CSV |
| unique.unit=app | PATT_APPLICATION_NUMBER | 存在 | CSV |
| unique.unit=family | DIPG_PATF_ID | 存在 | CSV |
| unique.unit=dipg | DIPG_ID | 存在 | CSV |
| policies.decl_date_policy | IPRD_SIGNATURE_DATE, Reflected_Date | 存在 | CSV |
| extra.analysis_countries | country_key (暗黙) | 存在 | 派生 |
| extra.top_k | (SELECT内ロジック) | — | — |

**幽霊フィルタ: なし** — 全ての Config キーが実在する列またはロジック制御に対応。

---

## 15. 自動検証ツール debug_flow.py

### 概要

`debug_flow.py` は、scope フィルタ・一意化・データ正規化の正当性を網羅的に検証する自動テストツール。

### 使用方法

```bash
# 基本実行 (500 config, 100行サンプル)
python debug_flow.py --count 500 --sample-size 100 --seed 42

# 大規模検証 (1000 config, 10万行サブセット)
python debug_flow.py --count 1000 --subset-size 100000
```

### 検証内容

1. **Config ランダム生成** (直積のサブセット)
   - country_mode: ALL / FILTER
   - country_prefixes: JP/US/CN/EP/KR
   - companies: 15社のLIKEパターン
   - gen_flags: null/5G=1/4G=1/5Gのみ等
   - ess_flags: null/ess_to_standard=true/false
   - unique.unit: app/publ/family/dipg/none
   - version_prefixes: 18/16/15 等
   - date_from/date_to

2. **各 Config に対する検証** (サンプル行ベース)
   - フィルタ検証: サンプル行が WHERE 条件を満たすか
   - 一意化検証: PUBL_NUMBER にパイプ残留がないか
   - データ健全性: Pending残留/DATETIME残留/パイプ残留

3. **高速化**: isld_pure からランダムサブセット (5万〜10万行) を作成し、全テストをサブセット上で実行

### 出力

| ファイル | 内容 |
|---------|------|
| `out/debug/debug_summary.csv` | config_id, pass/fail, row_count, violation種別 |
| `out/debug/debug_failures/` | fail時の config JSON + 詳細 |

### 最新検証結果

- 1000 configs / 100,000行サブセット / seed=123
- **結果: 1000/1000 PASS (100%)**
- 実行時間: 105秒

---

## 16. 新しい Flow を追加する方法

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

## 17. 検証・監査手順

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

## 18. 運用ルール

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
