# doc/Demo_Excel_AppCout

Excel 出力と月次テーブル整形・Release 重ね合わせ・プロット用のデモセット。

## ファイル一覧

| ファイル | 内容 |
|---------|------|
| `configuration.json` | 本デモ用の app 実行設定 |
| `A_filing_ts.csv` | 出願数時系列のサンプル CSV（参考） |
| `normalize_monthly_table.py` | 企業×月次の生データを「年月×企業」テーブルに正規化 |
| `add_release_overlay.py` | 月次テーブルに 3GPP Release 期間の列を追加 |
| `plot_monthly_with_release.py` | 月次折れ線 + Release timeline を 1 枚の PNG に描画 |

---

## 各 Python プログラムの説明

### normalize_monthly_table.py

**用途**  
会社ごとに「出願人名 / 月次日付 / 件数」の 3 列セットが横に並んだ Excel を読み、**1990-01-01 から 1 か月刻み**でラベルを統一した「年月 × 企業」の集計テーブル（欠損月は 0）を Excel に出力する。

- **入力イメージ**: 1 行目に企業名、以降は `[name, date, count]` が繰り返し（例: Ericsson \| date \| count \| Huawei \| date \| count \| ...）
- **出力イメージ**: ヘッダ `年月, Ericsson, Huawei, NEC, ...`、各行は `YYYY-MM-01` と各企業の件数

**主なオプション**

| オプション | 説明 |
|-----------|------|
| `--input` | 入力 Excel (.xlsx)【必須】 |
| `--output` | 出力 Excel (.xlsx)【必須】 |
| `--sheet` | 入力シート名（省略時は先頭シート） |
| `--start` | 開始年月日（既定: 1990-01-01） |
| `--csv-out` | 同じ内容を CSV で出力する場合のパス |
| `--rename-ericson` | ヘッダの "Ericson" を "Ericsson" に修正 |

**実行例**

```bash
python normalize_monthly_table.py --input in.xlsx --output out.xlsx
python normalize_monthly_table.py --input in.xlsx --output out.xlsx --start 1990-01-01 --sheet Sheet1
```

---

### add_release_overlay.py

**用途**  
「年月 × 企業」の月次テーブルに、**3GPP Release の期間（Start〜End/Closure）を月次ラベルに丸めて重ねた列**を追加した Excel を作る。

- **Release の扱い**: Start は必須。End があれば End、なければ Closure。End/Closure が両方空の場合は「終端なし」として入力テーブルの最終月まで有効とする。
- **日付**: `"2027-06-18 (SA#116)"` のような注釈付きでも先頭の YYYY-MM-DD を抽出して処理。
- **出力**: シート `monthly_table`（元の月次）、`monthly_with_release`（Active_Releases / Start_Releases / End_Releases および各 Release の `*_ACTIVE` / `*_START` / `*_END` 列付き）、`release_master`（Release マスタ一覧）。

**主なオプション**

| オプション | 説明 |
|-----------|------|
| `--input` | 入力 Excel (.xlsx)【必須】 |
| `--output` | 出力 Excel (.xlsx)【必須】 |
| `--sheet` | 入力シート名（省略時は先頭シート） |

**実行例**

```bash
python add_release_overlay.py --input monthly.xlsx --output monthly_with_release.xlsx
python add_release_overlay.py --input monthly.xlsx --output monthly_with_release.xlsx --sheet monthly_table
```

---

### plot_monthly_with_release.py

**用途**  
`monthly_with_release` シートを読み、**上段に企業別の月次件数折れ線（y 軸のみ表示）、下段に Release の Start/End を stem とラベルで示すタイムライン**を描き、1 枚の PNG で保存する。

- **入力**: `Rel-xx_ACTIVE` 列から各 Release の start/end（月）を推定。start/end のペアや途切れを監査し、問題があれば WARNING を表示。
- **依存**: `pandas`, `openpyxl`, `matplotlib`, `numpy`

**主なオプション**

| オプション | 説明 |
|-----------|------|
| `--input` | 入力 Excel (.xlsx)【必須】 |
| `--sheet` | シート名（既定: monthly_with_release） |
| `--output` | 出力 PNG パス（既定: lines_with_release_stems.png） |
| `--companies` | 企業列を明示（例: Ericsson,Huawei,NEC）。省略時は自動検出 |
| `--label-rotation` | Release ラベルの回転角（既定: 65） |
| `--month-interval` | x 軸の月表示間隔（既定: 6） |
| `--no-end` | End（下向き stem）を描かない |
| `--audit-csv` | Release の start/end 監査表を CSV で出力するパス |

**実行例**

```bash
python plot_monthly_with_release.py --input monthly_with_release.xlsx --sheet monthly_with_release --output monthly_with_release.png
python plot_monthly_with_release.py --input monthly_with_release.xlsx --output out.png --companies Ericsson,Huawei,NEC,NTT_Docomo,Toyota
python plot_monthly_with_release.py --input monthly_with_release.xlsx --output out.png --audit-csv audit_release_pairs.csv
```

---

## 実行の流れ

### 1. 元データの作成

プロジェクトルートで app を実行し、Excel を生成する。

```bash
cd ../../
python -m app.main --config ./doc/Demo_Excel_AppCout/configuration.json --excel
```

### 2. データの整形

生成された Excel（例: COMP.xlsx）を「年月×企業」に正規化し、Release 列を追加する。

```bash
python normalize_monthly_table.py --input COMP.xlsx --output COMP_out.xlsx
python add_release_overlay.py --input COMP_out.xlsx --output COMP_out_with_release.xlsx
```

### 3. プロット

Release 付き月次テーブルから PNG を出力する。

```bash
python plot_monthly_with_release.py --input COMP_out_with_release.xlsx --sheet monthly_with_release --output monthly_with_release.png
```
