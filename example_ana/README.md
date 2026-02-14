# example_ana — サンプル分析結果

> **注意**: この出力は `config.json` (または `example_ana/config_examples/example_ana_config_used.json`) で再現可能です。

## 再現手順

```bash
# A〜E 全分析を実行 + 多シート Excel 出力
python -m app.main --config config.json --excel
```

## 出力ファイル

| ファイル | 分析 | 行数目安 | 内容 |
|---------|------|---------|------|
| `A_filing_ts.csv` | A: 出願数時系列 | ~78,000 | country × company × bucket × filing_count |
| `B_lag_stats.csv` | B: lag分布サマリ | ~78,000 | country × company × bucket × n/min/q1/median/q3/max |
| `C_top_specs.csv` | C: TopSpec時系列 | ~404,000 | country × company × bucket × TGPP_NUMBER × cnt × rank |
| `D_company_rank.csv` | D: 企業ランキング | ~1,600 | country × unique_unit × company × cnt × rank |
| `E_spec_company_heat.csv` | E: Spec×会社 | ~9,500 | country × TGPP_NUMBER × company × cnt |
| `analysis_results.xlsx` | 統合Excel | — | ALL_*/CO_* × A〜E + META (31シート) |

## Excel シート構成

### シート命名規則

| シート種別 | 命名規則 | 例 |
|-----------|---------|-----|
| META | `META` | ジョブ一覧 + シート一覧 |
| ALL | `ALL_{analysis_key}` | `ALL_A_filing_ts` |
| CO_企業 | `CO_{display_key}_{analysis_key}` | `CO_NTT_DOCOMO_A_filing_ts` |

### 企業一覧（デフォルト config）

| display_key | LIKE パターン |
|------------|--------------|
| NTT_DOCOMO | DOCOMO |
| HUAWEI | HUAWEI |
| SHARP | SHARP |
| OPPO | OPPO |
| QUALCOMM | QUALCOMM |

## 各分析の列定義

### A: ts_filing_count（出願数時系列）

| 列 | 型 | 説明 |
|----|----|------|
| country | TEXT | 国コード (JP/US/CN/EP/KR/ALL) |
| company | TEXT | 企業名 |
| bucket | TEXT | 月次: YYYY-MM-01, 年次: YYYY-01-01 |
| filing_count | INTEGER | COUNT(DISTINCT PATT_APPLICATION_NUMBER) |

### B: ts_lag_stats（lag分布サマリ）

| 列 | 型 | 説明 |
|----|----|------|
| country | TEXT | 国コード |
| company | TEXT | 企業名 |
| bucket | TEXT | 時間バケット |
| n | INTEGER | データ件数 |
| min_lag_days | INTEGER | 最小 lag (日) |
| q1_lag_days | INTEGER | 第1四分位数 |
| median_lag_days | INTEGER | 中央値 |
| q3_lag_days | INTEGER | 第3四分位数 |
| max_lag_days | INTEGER | 最大 lag (日) |

### C: ts_top_specs（TopSpec時系列）

| 列 | 型 | 説明 |
|----|----|------|
| country | TEXT | 国コード |
| company | TEXT | 企業名 |
| bucket | TEXT | 時間バケット |
| TGPP_NUMBER | TEXT | 3GPP 仕様番号 |
| cnt | INTEGER | 件数 |
| rank | INTEGER | 順位 (1=最多) |

### D: rank_company_counts（企業ランキング）

| 列 | 型 | 説明 |
|----|----|------|
| country | TEXT | 国コード |
| unique_unit | TEXT | 集計単位 (app/family/publ/dipg) |
| company | TEXT | 企業名 |
| cnt | INTEGER | COUNT(DISTINCT unit_col) |
| rank | INTEGER | 順位 (1=最多) |

### E: heat_spec_company（Spec×会社ヒートマップ）

| 列 | 型 | 説明 |
|----|----|------|
| country | TEXT | 国コード |
| TGPP_NUMBER | TEXT | 3GPP 仕様番号（グローバル Top K） |
| company | TEXT | 企業名 |
| cnt | INTEGER | 件数 |

## Config パラメータ

| パラメータ | 影響範囲 | 説明 |
|----------|---------|------|
| `timeseries.period` | A, B, C | month / year |
| `extra.analysis_countries` | A〜E | 分析対象国コードリスト |
| `extra.include_all` | A〜E | ALL（全国集計）を含めるか |
| `extra.top_k` | C, E | TopK の K 値 |
| `unique.unit` | D | 集計単位 (app/family/publ/dipg) |
| `scope.companies` | A〜E | 企業 LIKE フィルタ |
| `scope.gen_flags` | A〜E | 世代フラグ {"5G": 1} |
| `scope.ess_flags` | A〜E | Essential フラグ |
