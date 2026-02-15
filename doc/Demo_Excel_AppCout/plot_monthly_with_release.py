#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
plot_lines_with_release_stems_sharedx.py

上: 企業別の月次件数 折れ線（y軸だけ表示、x軸表示なし）
下: Release timeline（Start/End を stem + ラベル、x軸はここだけ表示）

- monthly_with_release シート想定
- Rel-xx_ACTIVE 列から各Releaseの start/end（月）を推定
- start/end が「必ずペアで出ているか」監査して WARNING を出す

実行例:
  python plot_lines_with_release_stems_sharedx.py --input monthly_with_release.xlsx --sheet monthly_with_release --output out.png
  python plot_lines_with_release_stems_sharedx.py --input monthly_with_release.xlsx --output out.png --label-rotation 70
  python plot_lines_with_release_stems_sharedx.py --input monthly_with_release.xlsx --output out.png --companies Ericsson,Huawei,NEC,NTT_Docomo,Toyota
  python plot_lines_with_release_stems_sharedx.py --input monthly_with_release.xlsx --output out.png --audit-csv audit_release_pairs.csv

依存:
  pip install pandas openpyxl matplotlib numpy
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# ----------------------------
# parsing / detection
# ----------------------------

def parse_month_series(s: pd.Series) -> pd.Series:
    """'YYYY-MM-..' / datetime / 'YYYY-MM-DD (SA#..)' を月初 datetime に正規化"""
    dt = pd.to_datetime(s, errors="coerce")
    mask = dt.isna() & s.notna()
    if mask.any():
        xs = s[mask].astype(str).str.strip()
        m = xs.str.extract(r"(\d{4})-(\d{2})", expand=True)
        year = pd.to_numeric(m[0], errors="coerce")
        month = pd.to_numeric(m[1], errors="coerce")
        dt2 = pd.to_datetime(dict(year=year, month=month, day=1), errors="coerce")
        dt.loc[mask] = dt2
    return dt.dt.to_period("M").dt.to_timestamp()


def find_date_col(df: pd.DataFrame) -> str:
    for cand in ["年月", "month", "Month", "date", "Date"]:
        if cand in df.columns:
            return cand
    return df.columns[0]


def detect_release_active_cols(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if isinstance(c, str) and c.endswith("_ACTIVE")]


def detect_company_cols_by_layout(df: pd.DataFrame, date_col: str) -> List[str]:
    cols = list(df.columns)
    i0 = cols.index(date_col)

    stop_names = {
        "Active_Releases", "Active_Release_Names", "Start_Releases", "End_Releases",
        "FF_Releases", "PS_Releases", "Window_Releases",
    }

    stop_idx: Optional[int] = None
    for j in range(i0 + 1, len(cols)):
        c = cols[j]
        if c in stop_names:
            stop_idx = j
            break
        if isinstance(c, str) and re.search(r"_(ACTIVE|START|END)$", c):
            stop_idx = j
            break

    if stop_idx is None:
        # fallback: 数値列っぽいもの（0/1のみは除外）
        out = []
        for c in cols[i0 + 1:]:
            v = pd.to_numeric(df[c], errors="coerce")
            if v.notna().sum() == 0:
                continue
            uniq = set(v.dropna().astype(float).unique().tolist())
            if uniq.issubset({0.0, 1.0}):
                continue
            out.append(c)
        return out

    return [c for c in cols[i0 + 1:stop_idx]]


# ----------------------------
# Release start/end inference + audit
# ----------------------------

def release_rank(code: str) -> int:
    m = re.match(r"Rel-(\d+)$", code or "")
    if m:
        return 10000 + int(m.group(1))
    m = re.match(r"R(\d+)$", code or "")
    if m:
        return 1000 + int(m.group(1))
    return 0


def count_blocks(indices: np.ndarray) -> int:
    """ACTIVE=1 が連続ブロック何個あるか（途切れ検出用）"""
    if indices.size == 0:
        return 0
    gaps = np.where(np.diff(indices) > 1)[0]
    return int(gaps.size + 1)


def infer_release_pairs(
    df: pd.DataFrame,
    months: pd.Series,
    active_cols: List[str],
) -> pd.DataFrame:
    """
    *_ACTIVE 列から releaseごとに start/end（月）を作る。
    ついでに contiguity(途切れ) もチェック。
    """
    rows = []
    for col in active_cols:
        code = col[:-7]  # drop "_ACTIVE"
        v = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int).to_numpy()
        idx = np.where(v == 1)[0]

        if idx.size == 0:
            rows.append({
                "release": code,
                "has_active": False,
                "start_month": "",
                "end_month": "",
                "blocks": 0,
                "note": "ACTIVE has no 1s",
            })
            continue

        b = count_blocks(idx)
        st = months.iloc[int(idx[0])]
        ed = months.iloc[int(idx[-1])]
        rows.append({
            "release": code,
            "has_active": True,
            "start_month": st,
            "end_month": ed,
            "blocks": b,
            "note": "OK" if b == 1 else "ACTIVE has gaps (non-contiguous)",
        })

    out = pd.DataFrame(rows)
    # 見やすく start順→rank順
    def _key(r):
        if not r["has_active"]:
            return (pd.Timestamp.max, 10**9)
        return (r["start_month"], -release_rank(r["release"]))
    out = out.sort_values(by=["has_active"], ascending=[False]).copy()
    out["_sort"] = out.apply(_key, axis=1)
    out = out.sort_values(by="_sort").drop(columns=["_sort"])
    return out


def build_event_levels(dates: List[pd.Timestamp], sign: float, base: float = 1.0, step: float = 0.35) -> List[float]:
    """
    同じ月にイベントが重なるとき、上下方向（sign）に積み上げ。
    """
    counts: Dict[pd.Timestamp, int] = {}
    levels: List[float] = []
    for d in dates:
        k = counts.get(d, 0)
        counts[d] = k + 1
        levels.append(sign * (base + step * k))
    return levels


# ----------------------------
# style helpers
# ----------------------------

def hide_spines(ax, *, left=False, right=True, top=True, bottom=False) -> None:
    ax.spines["left"].set_visible(not left)
    ax.spines["right"].set_visible(not right)
    ax.spines["top"].set_visible(not top)
    ax.spines["bottom"].set_visible(not bottom)


# ----------------------------
# plot
# ----------------------------

def plot(
    months: pd.Series,
    counts: pd.DataFrame,
    companies: List[str],
    pairs: pd.DataFrame,
    *,
    output_png: Path,
    label_rotation: float,
    month_interval: int,
    show_end: bool,
) -> None:
    fig = plt.figure(figsize=(14, 7))
    gs = fig.add_gridspec(2, 1, height_ratios=[3.6, 1.3], hspace=0.02)
    ax_top = fig.add_subplot(gs[0, 0])
    ax_bot = fig.add_subplot(gs[1, 0], sharex=ax_top)

    # --- 上：折れ線（x軸表示なし、y軸表示あり）
    for c in companies:
        ax_top.plot(months, counts[c].to_numpy(dtype=float), linewidth=1.2, label=c)

    ax_top.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0, fontsize=9)

    # 上はx軸の表示を全部消す（ただしsharexは維持）
    ax_top.tick_params(axis="x", which="both", bottom=False, labelbottom=False)
    hide_spines(ax_top, left=False, right=True, top=True, bottom=True)  # 左だけ残す（y軸が読める）

    # --- 下：Release timeline（stem）
    # y軸は“表示する”が bin不要 → ytickは0だけ
    ax_bot.set_yticks([0])
    ax_bot.set_yticklabels(["0"])

    # 枠は left+bottom だけ残す（x軸表示するのでbottomは必要、y軸線も必要ならleft）
    hide_spines(ax_bot, left=False, right=True, top=True, bottom=False)

    # x軸は下だけ表示
    ax_bot.xaxis.set_major_locator(mdates.MonthLocator(interval=max(1, month_interval)))
    ax_bot.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for lab in ax_bot.get_xticklabels():
        lab.set_rotation(45)
        lab.set_ha("right")

    # baseline
    ax_bot.axhline(0, linewidth=1.0)

    # has_active のみプロット対象
    act = pairs[pairs["has_active"] == True].copy()
    if act.empty:
        raise SystemExit("[ERROR] どのReleaseも ACTIVE=1 がありません（timelineを描けません）")

    start_dates = act["start_month"].tolist()
    end_dates = act["end_month"].tolist()

    start_levels = build_event_levels(start_dates, sign=+1.0)
    end_levels = build_event_levels(end_dates, sign=-1.0)

    # stem + marker
    ax_bot.vlines(start_dates, 0, start_levels, linewidth=1.0)
    ax_bot.plot(start_dates, np.zeros(len(start_dates)), marker="^", linestyle="None",
                markersize=4, markerfacecolor="white")

    if show_end:
        ax_bot.vlines(end_dates, 0, end_levels, linewidth=1.0, linestyles="--")
        ax_bot.plot(end_dates, np.zeros(len(end_dates)), marker="v", linestyle="None",
                    markersize=4, markerfacecolor="white")

    # labels（重なり対策：回転＋bbox）
    for (release, st, ed), lvl_s, lvl_e in zip(act[["release", "start_month", "end_month"]].itertuples(index=False, name=None),
                                               start_levels, end_levels):
        # Start label（上）
        ax_bot.annotate(
            f"{release}",
            xy=(st, lvl_s),
            xytext=(2, 3),
            textcoords="offset points",
            ha="left",
            va="bottom",
            rotation=label_rotation,
            fontsize=8,
            bbox=dict(boxstyle="square", pad=0.15, lw=0, fc=(1, 1, 1, 0.75)),
        )
        # End label（下）
        if show_end:
            ax_bot.annotate(
                f"{release}",
                xy=(ed, lvl_e),
                xytext=(2, -3),
                textcoords="offset points",
                ha="left",
                va="top",
                rotation=label_rotation,
                fontsize=8,
                bbox=dict(boxstyle="square", pad=0.15, lw=0, fc=(1, 1, 1, 0.75)),
            )

    # timeline領域のy範囲を固定（見た目の安定）
    ax_bot.set_ylim(-3.0, 3.0)

    output_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_png, dpi=200)
    plt.close(fig)


# ----------------------------
# main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="上：折れ線（y軸のみ表示）/ 下：Release stem（x軸表示）")
    ap.add_argument("--input", required=True, help="入力Excel (.xlsx)")
    ap.add_argument("--sheet", default="monthly_with_release", help="シート名")
    ap.add_argument("--output", default="lines_with_release_stems.png", help="出力PNG")
    ap.add_argument("--companies", default=None, help="企業列を明示（例: Ericsson,Huawei,...）。省略時は自動検出で全部。")
    ap.add_argument("--label-rotation", type=float, default=65.0, help="Releaseラベル回転角（default: 65）")
    ap.add_argument("--month-interval", type=int, default=6, help="x軸（月）の表示間隔（default: 6）")
    ap.add_argument("--no-end", action="store_true", help="end（下向き）を出さない")
    ap.add_argument("--audit-csv", default=None, help="release start/end 監査表をCSVで出力したい場合に指定")
    args = ap.parse_args()

    df = pd.read_excel(Path(args.input), sheet_name=args.sheet, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    date_col = find_date_col(df)
    df["__month"] = parse_month_series(df[date_col])
    df = df[df["__month"].notna()].sort_values("__month").reset_index(drop=True)
    months = df["__month"]

    active_cols = detect_release_active_cols(df)
    if not active_cols:
        raise SystemExit("[ERROR] Rel-xx_ACTIVE 列が見つかりません（timeline生成に必要）")

    pairs = infer_release_pairs(df, months, active_cols)

    # ---- 監査（重要）
    # 1) ACTIVE列はあるのに1が無いRelease
    missing_active = pairs[pairs["has_active"] == False]["release"].tolist()
    if missing_active:
        print("[AUDIT] WARNING: *_ACTIVE column exists but has no 1s for:")
        print("         " + ", ".join(missing_active))

    # 2) 途中で途切れるRelease
    gap = pairs[(pairs["has_active"] == True) & (pairs["blocks"] > 1)]["release"].tolist()
    if gap:
        print("[AUDIT] WARNING: ACTIVE is non-contiguous (has gaps) for:")
        print("         " + ", ".join(gap))

    # 3) start/end が空になってないか（=ペア欠落がないか）
    bad_pair = pairs[(pairs["has_active"] == True) & (pairs["start_month"].astype(str) == "")].shape[0]
    if bad_pair:
        print("[AUDIT] WARNING: some releases have missing start/end (unexpected).")

    # 監査表をCSV出力（任意）
    if args.audit_csv:
        outp = Path(args.audit_csv)
        tmp = pairs.copy()
        # datetimeを見やすく
        tmp["start_month"] = tmp["start_month"].apply(lambda x: x.strftime("%Y-%m") if isinstance(x, pd.Timestamp) else x)
        tmp["end_month"] = tmp["end_month"].apply(lambda x: x.strftime("%Y-%m") if isinstance(x, pd.Timestamp) else x)
        tmp.to_csv(outp, index=False, encoding="utf-8")
        print(f"[AUDIT] wrote: {outp.resolve()}")

    # companies
    if args.companies:
        companies = [c.strip() for c in args.companies.split(",") if c.strip()]
    else:
        companies = detect_company_cols_by_layout(df, date_col=date_col)
    if not companies:
        raise SystemExit("[ERROR] 企業列が検出できません。--companies で明示してください。")

    counts = df[companies].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    plot(
        months=months,
        counts=counts,
        companies=companies,
        pairs=pairs,
        output_png=Path(args.output),
        label_rotation=args.label_rotation,
        month_interval=args.month_interval,
        show_end=(not args.no_end),
    )

    print(f"[OK] saved: {Path(args.output).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
