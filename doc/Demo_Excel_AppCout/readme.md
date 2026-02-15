
## 元データの作成
```
cd ../../
```

```
python -m app.main --config ./doc/Demo_Excel_AppCout/configuration.json --excel
```

## データの整形
```
python normalize_monthly_table.py --input COMP.xlsx --output COMP_out.xlsx
```

```
python add_release_overlay.py --input COMP_out.xlsx --output COMP_out_with_release.xlsx
```

```
python plot_monthly_with_release.py --input COMP_out_with_release.xlsx --sheet monthly_with_release --output monthly_with_release.png
```