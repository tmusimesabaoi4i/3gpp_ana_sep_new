"""E: Spec×会社ヒートマップ"""
import csv, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

CSV = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("example_ana/out/E_spec_company_heat.csv")
OUT = Path("for_visual/png")
OUT.mkdir(parents=True, exist_ok=True)

rows = []
with open(CSV, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

# ALL 国のみ
all_rows = [r for r in rows if r["country"] == "ALL"]

# Spec × Company マトリクス
spec_company = defaultdict(lambda: defaultdict(int))
spec_total = defaultdict(int)
company_total = defaultdict(int)
for r in all_rows:
    spec = r["TGPP_NUMBER"]
    comp = r["company"]
    cnt = int(r["cnt"])
    spec_company[spec][comp] += cnt
    spec_total[spec] += cnt
    company_total[comp] += cnt

# Top10 Spec × Top10 Company
top_specs = sorted(spec_total, key=lambda s: -spec_total[s])[:10]
top_comps = sorted(company_total, key=lambda c: -company_total[c])[:10]

# --- 1) ヒートマップ ---
matrix = []
for spec in top_specs:
    row = [spec_company[spec].get(comp, 0) for comp in top_comps]
    matrix.append(row)
matrix = np.array(matrix, dtype=float)

fig, ax = plt.subplots(figsize=(14, 8))
im = ax.imshow(matrix, cmap="YlOrRd", aspect="auto")
ax.set_xticks(range(len(top_comps)))
ax.set_xticklabels([c[:20] for c in top_comps], rotation=45, ha="right", fontsize=8)
ax.set_yticks(range(len(top_specs)))
ax.set_yticklabels(top_specs, fontsize=9)
ax.set_title("Spec x Company Heatmap (E: heat_spec_company, ALL)", fontsize=13)
plt.colorbar(im, ax=ax, label="Count")

# セル内に数値表示
for i in range(len(top_specs)):
    for j in range(len(top_comps)):
        v = int(matrix[i, j])
        if v > 0:
            color = "white" if v > matrix.max() * 0.6 else "black"
            ax.text(j, i, f"{v:,}", ha="center", va="center", fontsize=7, color=color)

plt.tight_layout()
plt.savefig(OUT / "E_heatmap.png", dpi=150)
plt.close()
print(f"  saved: E_heatmap.png")

# --- 2) Top10 Spec 積み上げ棒 ---
fig, ax = plt.subplots(figsize=(14, 7))
bottom = np.zeros(len(top_specs))
colors = plt.cm.tab10(np.linspace(0, 1, len(top_comps)))
for j, comp in enumerate(top_comps):
    vals = [spec_company[spec].get(comp, 0) for spec in top_specs]
    ax.bar(range(len(top_specs)), vals, bottom=bottom, label=comp[:20], color=colors[j])
    bottom += np.array(vals)
ax.set_xticks(range(len(top_specs)))
ax.set_xticklabels(top_specs, rotation=45, ha="right", fontsize=9)
ax.set_title("Top 10 Specs - Stacked by Company (E)", fontsize=13)
ax.set_ylabel("Count")
ax.legend(fontsize=7, loc="upper right")
plt.tight_layout()
plt.savefig(OUT / "E_stacked_bar.png", dpi=150)
plt.close()
print(f"  saved: E_stacked_bar.png")

print("E plots done.")
