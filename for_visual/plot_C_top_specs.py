"""C: TopSpec時系列 — 棒グラフ"""
import csv, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from collections import defaultdict

CSV = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("example_ana/out/C_top_specs.csv")
OUT = Path("for_visual/png")
OUT.mkdir(parents=True, exist_ok=True)

rows = []
with open(CSV, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

# ALL 国、全期間合算で TGPP_NUMBER 別件数
spec_total = defaultdict(int)
for r in rows:
    if r["country"] == "ALL":
        spec_total[r["TGPP_NUMBER"]] += int(r["cnt"])

top10 = sorted(spec_total, key=lambda s: -spec_total[s])[:10]

# --- 1) Top10 Spec 棒グラフ ---
fig, ax = plt.subplots(figsize=(12, 6))
ax.barh(list(reversed(top10)), [spec_total[s] for s in reversed(top10)], color="#55A868")
ax.set_title("Top 10 3GPP Specs by Total Count (C: ts_top_specs)", fontsize=13)
ax.set_xlabel("Total Count")
plt.tight_layout()
plt.savefig(OUT / "C_top10_specs_bar.png", dpi=150)
plt.close()
print(f"  saved: C_top10_specs_bar.png")

# --- 2) Top5 Spec の年次推移 ---
top5 = top10[:5]
spec_yearly = defaultdict(lambda: defaultdict(int))
for r in rows:
    if r["country"] == "ALL" and r["TGPP_NUMBER"] in top5:
        year = r["bucket"][:4]
        if year >= "2005":
            spec_yearly[r["TGPP_NUMBER"]][year] += int(r["cnt"])

years = sorted(set(y for s in spec_yearly.values() for y in s.keys()))
fig, ax = plt.subplots(figsize=(14, 6))
for spec in top5:
    vals = [spec_yearly[spec].get(y, 0) for y in years]
    ax.plot(years, vals, "o-", label=spec, linewidth=1.5)
ax.set_title("Top 5 Specs Yearly Trend (C: ts_top_specs)", fontsize=13)
ax.set_xlabel("Year")
ax.set_ylabel("Count")
ax.legend(fontsize=9)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(OUT / "C_top5_specs_trend.png", dpi=150)
plt.close()
print(f"  saved: C_top5_specs_trend.png")

print("C plots done.")
