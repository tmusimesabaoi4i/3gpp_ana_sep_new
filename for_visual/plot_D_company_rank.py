"""D: 企業ランキング — 横棒グラフ + 国別比較"""
import csv, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from collections import defaultdict

CSV = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("example_ana/out/D_company_rank.csv")
OUT = Path("for_visual/png")
OUT.mkdir(parents=True, exist_ok=True)

rows = []
with open(CSV, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

# --- 1) ALL 国 企業 Top20 ---
all_rows = [r for r in rows if r["country"] == "ALL"]
all_rows.sort(key=lambda r: -int(r["cnt"]))
top20 = all_rows[:20]

fig, ax = plt.subplots(figsize=(12, 8))
names = [r["company"][:30] for r in reversed(top20)]
vals = [int(r["cnt"]) for r in reversed(top20)]
ax.barh(names, vals, color="#DD8452")
ax.set_title("Top 20 Companies by Filing Count (D: rank_company_counts)", fontsize=13)
ax.set_xlabel("Distinct Application Count")
plt.tight_layout()
plt.savefig(OUT / "D_top20_companies.png", dpi=150)
plt.close()
print(f"  saved: D_top20_companies.png")

# --- 2) 国別 Top5 比較 ---
countries = ["JP", "US", "CN", "EP", "KR"]
fig, axes = plt.subplots(1, len(countries), figsize=(20, 6), sharey=False)
for i, ctry in enumerate(countries):
    ctry_rows = [r for r in rows if r["country"] == ctry]
    ctry_rows.sort(key=lambda r: -int(r["cnt"]))
    top5 = ctry_rows[:5]
    names = [r["company"][:20] for r in reversed(top5)]
    vals = [int(r["cnt"]) for r in reversed(top5)]
    axes[i].barh(names, vals, color="#4C72B0")
    axes[i].set_title(f"{ctry}", fontsize=12)
    axes[i].tick_params(axis="y", labelsize=8)
plt.suptitle("Top 5 Companies by Country (D)", fontsize=14)
plt.tight_layout()
plt.savefig(OUT / "D_top5_by_country.png", dpi=150)
plt.close()
print(f"  saved: D_top5_by_country.png")

print("D plots done.")
