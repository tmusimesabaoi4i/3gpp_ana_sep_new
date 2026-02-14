"""テスト用小規模 CSV を生成するスクリプト"""
import csv
import random
from pathlib import Path

random.seed(42)

COMPANIES = [
    "Qualcomm Inc.", "Samsung Electronics", "Huawei Technologies",
    "Nokia Corporation", "Ericsson AB", "LG Electronics",
    "InterDigital Patent Holdings", "Sharp Corporation",
    "ZTE Corporation", "Apple Inc.",
]
COUNTRIES = ["US", "KR", "CN", "FI", "SE", "JP", "FR", "DE"]
SPECS = [
    "TS 36.211", "TS 36.212", "TS 36.213", "TS 36.214",
    "TS 38.211", "TS 38.212", "TS 38.213", "TS 38.214",
    "TS 36.321", "TS 36.331", "TS 38.321", "TS 38.331",
    "TS 36.300", "TS 38.300", "TS 23.501", "TS 23.502",
    "TS 24.501", "TS 24.301", "TS 36.101", "TS 38.101",
]
VERSIONS = ["8.0.0", "9.0.0", "10.0.0", "11.0.0", "12.0.0",
            "13.0.0", "14.0.0", "15.0.0", "16.0.0", "17.0.0"]
TYPES = ["patent", "patent application", "utility model"]
TERMS = ["FRAND", ""]

def rand_date(y_min=2005, y_max=2024):
    y = random.randint(y_min, y_max)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y:04d}-{m:02d}-{d:02d}"

out = Path("ISLD-export-test.csv")
N = 2000

with open(out, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow([
        "DIPG_ID", "DIPG_PATF_ID", "PUBL_NUMBER", "PATT_APPLICATION_NUMBER",
        "COMP_LEGAL_NAME", "Country_Of_Registration",
        "IPRD_SIGNATURE_DATE", "Reflected Date", "PBPA_APP_DATE", "PBPA_PUBL_DATE",
        "TGPP_NUMBER", "TGPV_VERSION", "TGPP_TITLE",
        "IPRD_TYPE", "IPRD_LICENSING_TERMS",
        "PBPA_COUNTRY_CODE", "PBPA_KIND_CODE", "IPRD_IS_BLANKET",
    ])
    for i in range(1, N + 1):
        comp = random.choice(COMPANIES)
        country = random.choice(COUNTRIES)
        app_date = rand_date(2000, 2020)
        sig_date = rand_date(2005, 2024) if random.random() > 0.1 else ""
        ref_date = rand_date(2005, 2024) if random.random() > 0.05 else ""
        pub_date = rand_date(2002, 2022)
        spec = random.choice(SPECS)
        ver = random.choice(VERSIONS)
        publ = f"{random.choice(['US','EP','WO','JP','CN','KR'])}{random.randint(1000000,9999999)}{random.choice(['A1','B2','A','B1'])}"
        app_no = f"{random.choice(['US','EP','JP','CN'])}{random.randint(20000000,20249999)}"

        w.writerow([
            i * 100 + random.randint(0, 99),  # DIPG_ID
            random.randint(1, 50000),           # DIPG_PATF_ID
            publ,
            app_no,
            comp, country,
            sig_date, ref_date, app_date, pub_date,
            spec, ver,
            f"Physical layer procedures ({spec})",
            random.choice(TYPES),
            random.choice(TERMS),
            publ[:2],
            random.choice(["A1", "B2", "A", "B1"]),
            random.choice(["true", "false", ""]),
        ])

print(f"Generated {N} rows → {out}")
