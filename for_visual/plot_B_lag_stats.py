"""B: lag分布サマリ — 箱ひげ風チャート"""
import csv, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime
from collections import defaultdict

CSV = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("example_ana/out/B_lag_stats.csv")
OUT = Path("for_visual/png")
OUT.mkdir(parents=True, exist_ok=True)

rows = []
with open(CSV, encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

# ALL 国のみ、月次 median lag 推移
all_rows = [r for r in rows if r["country"] == "ALL" and r["bucket"] >= "2005-01-01"]

# 企業でグループ化、上位5社のみ
company_data = defaultdict(list)
for r in all_rows:
    try:
        company_data[r["company"]].append({
            "bucket": r["bucket"],
            "median": float(r["median_lag_days"]) if r["median_lag_days"] else None,
            "q1": float(r["q1_lag_days"]) if r["q1_lag_days"] else None,
            "q3": float(r["q3_lag_days"]) if r["q3_lag_days"] else None,
        })
    except (ValueError, KeyError):
        pass

# 各社の行数で上位5社を選出
top_companies = sorted(company_data, key=lambda c: -len(company_data[c]))[:5]

# --- 1) Median Lag推移 (上位5社) ---
fig, ax = plt.subplots(figsize=(14, 6))
for comp in top_companies:
    data = sorted(company_data[comp], key=lambda d: d["bucket"])
    dates = []
    medians = []
    for d in data:
        if d["median"] is not None and 0 <= d["median"] <= 10000:
            dates.append(datetime.strptime(d["bucket"], "%Y-%m-%d"))
            medians.append(d["median"])
    if dates:
        ax.plot(dates, medians, label=comp[:25], linewidth=1, alpha=0.8)
ax.set_title("Monthly Median Lag Days - Top 5 Companies (B: ts_lag_stats)", fontsize=13)
ax.set_xlabel("Date")
ax.set_ylabel("Median Lag (days)")
ax.legend(fontsize=8)
ax.set_ylim(bottom=0, top=5000)
plt.tight_layout()
plt.savefig(OUT / "B_median_lag_trend.png", dpi=150)
plt.close()
print(f"  saved: B_median_lag_trend.png")

# --- 2) 年次の箱ひげ風 (Q1-Q3 range) ---
yearly_stats = defaultdict(lambda: {"q1": [], "median": [], "q3": []})
for r in rows:
    if r["country"] == "ALL":
        year = r["bucket"][:4]
        if year >= "2005":
            try:
                if r["median_lag_days"]:
                    yearly_stats[year]["median"].append(float(r["median_lag_days"]))
                if r["q1_lag_days"]:
                    yearly_stats[year]["q1"].append(float(r["q1_lag_days"]))
                if r["q3_lag_days"]:
                    yearly_stats[year]["q3"].append(float(r["q3_lag_days"]))
            except ValueError:
                pass

years = sorted(yearly_stats.keys())
avg_medians = []
avg_q1 = []
avg_q3 = []
for y in years:
    s = yearly_stats[y]
    avg_medians.append(sum(s["median"])/len(s["median"]) if s["median"] else 0)
    avg_q1.append(sum(s["q1"])/len(s["q1"]) if s["q1"] else 0)
    avg_q3.append(sum(s["q3"])/len(s["q3"]) if s["q3"] else 0)

fig, ax = plt.subplots(figsize=(12, 5))
x = range(len(years))
ax.fill_between(x, avg_q1, avg_q3, alpha=0.3, label="Q1-Q3 range")
ax.plot(x, avg_medians, "o-", label="Median", linewidth=2)
ax.set_xticks(list(x))
ax.set_xticklabels(years, rotation=45)
ax.set_title("Yearly Average Lag Stats (ALL, B: ts_lag_stats)", fontsize=13)
ax.set_ylabel("Lag Days")
ax.legend()
plt.tight_layout()
plt.savefig(OUT / "B_yearly_lag_box.png", dpi=150)
plt.close()
print(f"  saved: B_yearly_lag_box.png")

print("B plots done.")
