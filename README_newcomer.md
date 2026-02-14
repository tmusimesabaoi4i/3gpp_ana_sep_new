# ISLD Pipeline — はじめてのユーザガイド

> **5分で動かす、10分で理解する** を目指したドキュメントです。

---

## 1. 前提条件

| 項目 | 要件 |
|------|------|
| Python | 3.10 以上 |
| 入力 CSV | `ISLD-export/ISLD-export.csv`（ETSI から取得） |
| ディスク | CSV の約 2 倍（SQLite 生成用、約 4GB） |
| 追加ライブラリ | `openpyxl`（Excel出力用）、`matplotlib`（可視化用） |

## 2. セットアップ

```bash
# 1. 依存インストール
pip install -r requirements.txt

# 2. データ配置
mkdir ISLD-export
# ISLD-export.csv を ISLD-export/ にコピー
```

## 3. まず動かしてみる（最短コマンド）

```bash
# example_ana のサンプル config で実行（初回は CSV→SQLite 変換で5-10分）
python -m app.main --config example_ana/config.json --excel
```

### 出力される成果物

```
example_ana/out/
├── A_filing_ts.csv           ← 国×企業×月次 出願数推移
├── B_lag_stats.csv           ← lag分布サマリ（箱ひげ用）
├── C_top_specs.csv           ← Top Spec 時系列
├── D_company_rank.csv        ← 企業ランキング
├── E_spec_company_heat.csv   ← Spec×会社ヒートマップ
├── analysis_results.xlsx     ← 上記を ALL/企業別にまとめた多シート Excel
└── plan_summary.txt          ← 実行計画サマリ
```

### 2回目以降

```bash
# CSV は読み込まない（SQLite を再利用するため数秒で完了）
python -m app.main --config example_ana/config.json --excel
```

## 4. Config の最小編集ポイント

`example_ana/config.json` をコピーして自分用に編集するのが最速です。

### 変えたいこと別ガイド

| やりたいこと | 編集箇所 | 例 |
|-------------|---------|-----|
| 特定企業だけ分析 | `defaults.scope.companies` | `["NTT", "HUAWEI"]` |
| 特定国だけ分析 | `defaults.scope.country_mode` + `country_prefixes` | `"FILTER"` + `["JP"]` |
| 5G だけに絞る | `defaults.scope.gen_flags` | `{"5G": 1}` |
| Essential のみ | `defaults.scope.ess_flags` | `{"ess_to_standard": true}` |
| 年次集計に変更 | `defaults.timeseries.period` | `"year"` |
| 分析対象国を変更 | `defaults.extra.analysis_countries` | `["JP", "US"]` |

### config の最小例

```json
{
  "env": {
    "sqlite_path": "work.sqlite",
    "isld_csv_path": "./ISLD-export/ISLD-export.csv",
    "out_dir": "my_out"
  },
  "defaults": {
    "scope": {
      "companies": ["NTT"],
      "country_mode": "FILTER",
      "country_prefixes": ["JP"],
      "gen_flags": {"5G": 1}
    },
    "unique": {"unit": "app"},
    "timeseries": {"period": "month"},
    "extra": {
      "analysis_countries": ["JP"],
      "include_all": true
    }
  },
  "excel_output": {
    "enabled": true,
    "path": "my_out/results.xlsx",
    "companies": {"NTT_Docomo": "DOCOMO"},
    "meta_sheet": true
  },
  "jobs": [
    {"job_id": "A_filing", "template": "ts_filing_count"}
  ]
}
```

## 5. DB（isld_pure）の概要

パイプラインは CSV を1回だけ読み込んで SQLite の `isld_pure` テーブルに格納します（30列）。

### 主要な列

| 列名 | 内容 | NULL率 |
|------|------|--------|
| `PATT_APPLICATION_NUMBER` | 出願番号 | 0.8% |
| `PUBL_NUMBER` | 公開番号 | 11.8% |
| `COMP_LEGAL_NAME` | 企業名（法的名称） | 0.0% |
| `company_key` | 企業名キー（UPPER+正規化） | 0.0% |
| `Country_Of_Registration` | 出願国（"JP JAPAN" 形式） | 0.0% |
| `country_key` | 国コード（"JP"） | 0.0% |
| `PBPA_APP_DATE` | 出願日（YYYY-MM-DD） | 4.9% |
| `TGPP_NUMBER` | 3GPP 仕様番号 | 13.9% |
| `TGPV_VERSION` | 3GPP バージョン | 73.7% |
| `Gen_5G` | 5G フラグ（0/1/NULL） | 14.5% |
| `Ess_To_Standard` | Essential 宣言（0/1/NULL） | 3.9% |
| `DIPG_PATF_ID` | Patent Family ID | 3.7% |

> 全30列の詳細は [README_detail.md](README_detail.md) §2 を参照。

## 6. 各ジョブ（A〜E）の出力

### A: ts_filing_count（出願数時系列）

**何が出るか**: 国 × 企業 × 月（or 年）ごとの出願数

| 列 | 意味 |
|-----|------|
| country | 国コード (JP/US/CN/EP/KR/ALL) |
| company | 企業名 |
| bucket | 期間 (2020-03-01 = 2020年3月) |
| filing_count | 出願件数（DISTINCT 出願番号ベース） |

**データサンプル**:
```
country,company,bucket,filing_count
JP,NTT DOCOMO INC,2020-01-01,45
JP,NTT DOCOMO INC,2020-02-01,38
US,QUALCOMM INCORPORATED,2020-01-01,312
ALL,QUALCOMM INCORPORATED,2020-01-01,1205
```

### B: ts_lag_stats（lag分布サマリ）

**何が出るか**: 宣言日−出願日の時間差（lag）の統計値

| 列 | 意味 |
|-----|------|
| country | 国コード |
| company | 企業名 |
| bucket | 期間 |
| n | データ件数 |
| min_lag_days〜max_lag_days | 最小/Q1/中央値/Q3/最大 |

### C: ts_top_specs（TopSpec時系列）

**何が出るか**: 各期間で件数上位の 3GPP 仕様番号

### D: rank_company_counts（企業ランキング）

**何が出るか**: 国ごとの企業出願件数ランキング

### E: heat_spec_company（Spec×会社ヒートマップ）

**何が出るか**: 上位 Spec × 企業の件数マトリクス（縦持ち形式）

## 7. デバッグ・確認方法

### データを目視確認

```bash
# フィルタ適用後のデータを100件確認
python debug_jobs.py --mode raw --config example_ana/config.json

# unique適用後も確認
python debug_jobs.py --mode unique --config example_ana/config.json
```

### 実行計画のみ表示

```bash
python -m app.main --config example_ana/config.json --print-plan --dry-run
```

## 8. 可視化

`for_visual/` にサンプルの matplotlib スクリプトがあります。

```bash
python for_visual/plot_A_filing_ts.py
python for_visual/plot_D_company_rank.py
# → for_visual/png/ に PNG が生成されます
```

> グラフのサンプルは [sample_visual.md](sample_visual.md) を参照。

## 9. 次にやることチェックリスト

- [ ] `example_ana/config.json` で実行し、出力を確認
- [ ] `for_visual/` の可視化スクリプトを実行してグラフを確認
- [ ] 自分の分析用 config を作成（上記§4参照）
- [ ] scope（企業/国/世代/期間）を変えて再実行
- [ ] Excel の META シートで全体像を把握
- [ ] 必要に応じて [README_detail.md](README_detail.md) で詳細仕様を確認

---

> **詳細仕様**: [README_detail.md](README_detail.md)  
> **可視化サンプル**: [sample_visual.md](sample_visual.md)  
> **ルートREADME**: [README.md](README.md)
