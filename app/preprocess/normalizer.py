"""
preprocess/normalizer.py  –  pure-safe 正規化関数群

純関数として提供。例外は投げず、失敗は None を返す。
"""
from __future__ import annotations

import calendar
import re
from typing import Optional

# ──────────────────────────────────────────────
# 内部ユーティリティ
# ──────────────────────────────────────────────
_MULTI_WS = re.compile(r"\s+")
_COMMA_OR_WS = re.compile(r"[,\s]")
_DATE_SEP = re.compile(r"[-/.]")

# 日付受理パターン
_DATE_PATTERNS = [
    # YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD
    re.compile(r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$"),
    # DD-MM-YYYY / DD/MM/YYYY  (日≤12 のとき曖昧だが MD>12 で判定)
    re.compile(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})$"),
    # YYYYMMDD
    re.compile(r"^(\d{4})(\d{2})(\d{2})$"),
]

_BOOL_TRUE = frozenset({"true", "yes", "1", "t", "y"})
_BOOL_FALSE = frozenset({"false", "no", "0", "f", "n"})

_PATENT_STRIP = re.compile(r"[\s\-/,.]")
_PATENT_SENTINELS = frozenset({"pending", "-", "n/a", "na", "none", "unknown", ""})
# 部分一致で sentinel と見なすキーワード (PENDING1, USPATENTAPPLICATIONPENDING 等)
_PATENT_SENTINEL_SUBSTR = ("pending", "unknown")

# datetime 受理
_DT_PATTERN = re.compile(
    r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})"
    r"[T ](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$"
)


# ──────────────────────────────────────────────
# Public 正規化関数
# ──────────────────────────────────────────────
def norm_text(s: str | None) -> str | None:
    """trim + 空白圧縮。空なら None"""
    if s is None:
        return None
    s = _MULTI_WS.sub(" ", str(s).strip())
    return s if s else None


def norm_int(s: str | None) -> int | None:
    """カンマ・空白除去→int"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    try:
        cleaned = _COMMA_OR_WS.sub("", s)
        return int(cleaned)
    except (ValueError, OverflowError):
        # 小数が来た場合は切り捨て
        try:
            return int(float(cleaned))
        except (ValueError, OverflowError):
            return None


def norm_real(s: str | None) -> float | None:
    """カンマ除去→float"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    try:
        cleaned = s.replace(",", "")
        return float(cleaned)
    except (ValueError, OverflowError):
        return None


def norm_bool(s: str | None) -> int | None:
    """true/false 候補判定 → 0|1|None"""
    if s is None:
        return None
    v = str(s).strip().lower()
    if not v:
        return None
    if v in _BOOL_TRUE:
        return 1
    if v in _BOOL_FALSE:
        return 0
    return None


def norm_date(s: str | None) -> str | None:
    """受理形式を限定して YYYY-MM-DD の ISO 文字列を返す"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None

    # datetime が来た場合は日付部分だけ
    if "T" in s or " " in s:
        parts = s.replace("T", " ").split(" ")
        s = parts[0].strip()

    for pat in _DATE_PATTERNS:
        m = pat.match(s)
        if not m:
            continue
        groups = m.groups()
        if len(groups[0]) == 4:
            y, mo, d = int(groups[0]), int(groups[1]), int(groups[2])
        else:
            # DD-MM-YYYY or MM-DD-YYYY
            a, b, y = int(groups[0]), int(groups[1]), int(groups[2])
            if a > 12:
                d, mo = a, b
            elif b > 12:
                mo, d = a, b
            else:
                # 曖昧 → DD-MM-YYYY と仮定 (欧州寄り)
                d, mo = a, b
        if 1 <= mo <= 12 and 1800 <= y <= 2100:
            # 月ごとの最大日数を厳密チェック (閏年考慮)
            max_day = calendar.monthrange(y, mo)[1]
            if 1 <= d <= max_day:
                return f"{y:04d}-{mo:02d}-{d:02d}"

    return None


def norm_datetime(s: str | None) -> str | None:
    """秒補完して YYYY-MM-DD HH:MM:SS"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None

    m = _DT_PATTERN.match(s)
    if not m:
        # 日付だけの場合は 00:00:00 付与
        d = norm_date(s)
        if d:
            return f"{d} 00:00:00"
        return None

    y, mo, d, h, mi = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5))
    sec = int(m.group(6)) if m.group(6) else 0

    if not (1 <= mo <= 12 and 1 <= d <= 31 and 0 <= h <= 23 and 0 <= mi <= 59 and 0 <= sec <= 59):
        return None

    return f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{sec:02d}"


def norm_patent_no(s: str | None) -> str | None:
    """安全な範囲で canonical 化（大文字化・区切り削除）

    - パイプ「|」区切りの複数番号 → 先頭番号のみ採用
    - sentinel 値 (Pending, -, N/A 等) → None
    """
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None

    # パイプ区切りの場合は先頭番号を採用
    if "|" in s:
        s = s.split("|")[0].strip()
        if not s:
            return None

    # sentinel 除去 (完全一致)
    if s.lower() in _PATENT_SENTINELS:
        return None
    # sentinel 部分一致 (PENDING1, GB9402492PENDING, USPATENTAPPLICATIONPENDING 等)
    s_low = s.lower()
    if any(kw in s_low for kw in _PATENT_SENTINEL_SUBSTR):
        return None

    # 大文字化 + 区切り文字除去
    cleaned = _PATENT_STRIP.sub("", s.upper())
    return cleaned if cleaned else None


def norm_company_name(s: str | None) -> str | None:
    """見た目維持の最小整形（大文字化はしない）"""
    return norm_text(s)


_COMPANY_STRIP = re.compile(r"[,.\-'\"()\[\]]")

def norm_company_key(s: str | None) -> str | None:
    """会社名 → 検索キー: UPPER + 句読点除去 + 空白圧縮

    例: "NTT DOCOMO, INC." → "NTT DOCOMO INC"
    """
    if s is None:
        return None
    s = str(s).strip().upper()
    if not s:
        return None
    s = _COMPANY_STRIP.sub(" ", s)
    s = _MULTI_WS.sub(" ", s).strip()
    return s if s else None


def norm_country_key(s: str | None) -> str | None:
    """国名 → ISO2コード抽出

    例: "JP JAPAN" → "JP", "US UNITED STATES" → "US"
    2文字のアルファベット先頭語があればそれを返す。
    """
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    parts = s.split()
    if parts:
        code = parts[0].upper()
        if len(code) == 2 and code.isalpha():
            return code
    # fallback: 先頭2文字がアルファベットなら返す
    if len(s) >= 2 and s[:2].isalpha():
        return s[:2].upper()
    return s.upper()


# ──────────────────────────────────────────────
# 名前→関数 マッピング
# ──────────────────────────────────────────────
NORMALIZER_MAP: dict[str, callable] = {
    "norm_text": norm_text,
    "norm_int": norm_int,
    "norm_real": norm_real,
    "norm_bool": norm_bool,
    "norm_date": norm_date,
    "norm_datetime": norm_datetime,
    "norm_patent_no": norm_patent_no,
    "norm_company_name": norm_company_name,
    "norm_company_key": norm_company_key,
    "norm_country_key": norm_country_key,
}
