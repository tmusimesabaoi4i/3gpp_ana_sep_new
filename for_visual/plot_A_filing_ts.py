"""A: 出願数時系列 — 折れ線グラフ (国×月次)"""
import csv, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from collections import defaultdict

CSV = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("example_ana/out/A_filing_ts.csv")
OUT = Path("for_visual/png")
OUT.mkdir(parents=True, exist_ok=True)

rows = []
with open(CSV, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

# --- 1) 国別 月次出願数推移 (ALL企業合算) ---
country_ts = defaultdict(lambda: defaultdict(int))
for r in rows:
    country_ts[r["country"]][r["bucket"]] += int(r["filing_count"])

fig, ax = plt.subplots(figsize=(14, 6))
for country in ["JP", "US", "CN", "EP", "KR"]:
    if country not in country_ts:
        continue
    buckets = sorted(country_ts[country].keys())
    dates = [datetime.strptime(b, "%Y-%m-%d") for b in buckets if b >= "2000-01-01"]
    vals = [country_ts[country][b] for b in buckets if b >= "2000-01-01"]
    ax.plot(dates, vals, label=country, linewidth=1)
ax.set_title("Monthly Filing Count by Country (A: ts_filing_count)", fontsize=13)
ax.set_xlabel("Date")
ax.set_ylabel("Filing Count")
ax.legend()
ax.xaxis.set_major_locator(mdates.YearLocator(2))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
plt.tight_layout()
plt.savefig(OUT / "A_country_monthly.png", dpi=150)
plt.close()
print(f"  saved: A_country_monthly.png")

# --- 2) ALL の年次合算 (棒グラフ) ---
all_yearly = defaultdict(int)
for r in rows:
    if r["country"] == "ALL":
        year = r["bucket"][:4]
        all_yearly[year] += int(r["filing_count"])

years = sorted(k for k in all_yearly if k >= "2000")
fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(years, [all_yearly[y] for y in years], color="#4C72B0")
ax.set_title("Yearly Total Filing Count (ALL countries)", fontsize=13)
ax.set_xlabel("Year")
ax.set_ylabel("Filing Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(OUT / "A_yearly_bar.png", dpi=150)
plt.close()
print(f"  saved: A_yearly_bar.png")

# --- 3) 国別比率 (円グラフ、最新5年) ---
country_total = defaultdict(int)
for r in rows:
    c = r["country"]
    if c != "ALL" and r["bucket"] >= "2019-01-01":
        country_total[c] += int(r["filing_count"])
labels = sorted(country_total, key=lambda x: -country_total[x])[:6]
vals = [country_total[c] for c in labels]
fig, ax = plt.subplots(figsize=(7, 7))
ax.pie(vals, labels=labels, autopct="%1.1f%%", startangle=90)
ax.set_title("Filing Share by Country (2019-)", fontsize=13)
plt.tight_layout()
plt.savefig(OUT / "A_country_pie.png", dpi=150)
plt.close()
print(f"  saved: A_country_pie.png")

print("A plots done.")
