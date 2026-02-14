"""
schema/isld_column_specs.py  –  isld_pure の ColumnSpec 定義

本番 ISLD-export.csv のヘッダに合わせて定義。
不要列は定義しない（余剰列は無視）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class ColumnSpec:
    name_sql: str
    source_headers: list[str]
    col_type: str                          # INT | REAL | TEXT | DATE | DATETIME | BOOL
    nullable: bool = True
    normalizer: Optional[str] = None       # normalizer 関数名 (文字列)
    db_affinity: str = "TEXT"              # INTEGER | REAL | TEXT
    is_key_candidate: bool = False


def build_column_specs() -> list[ColumnSpec]:
    """isld_pure に保存する列の ColumnSpec リストを返す。"""
    return [
        # ─── 宣言識別子 ───
        ColumnSpec(
            name_sql="IPRD_ID",
            source_headers=["IPRD_ID", "iprd_id"],
            col_type="INT", normalizer="norm_int",
            db_affinity="INTEGER",
        ),
        ColumnSpec(
            name_sql="DIPG_ID",
            source_headers=["DIPG_ID", "dipg_id"],
            col_type="INT", normalizer="norm_int",
            db_affinity="INTEGER", is_key_candidate=True,
        ),
        ColumnSpec(
            name_sql="DIPG_PATF_ID",
            source_headers=["DIPG_PATF_ID", "dipg_patf_id"],
            col_type="INT", normalizer="norm_int",
            db_affinity="INTEGER", is_key_candidate=True,
        ),

        # ─── 特許識別子 ───
        ColumnSpec(
            name_sql="PUBL_NUMBER",
            source_headers=["PUBL_NUMBER", "Publ_Number", "publ_number",
                            "Publication Number"],
            col_type="TEXT", normalizer="norm_patent_no",
            db_affinity="TEXT", is_key_candidate=True,
        ),
        ColumnSpec(
            name_sql="PATT_APPLICATION_NUMBER",
            source_headers=["PATT_APPLICATION_NUMBER", "patt_application_number",
                            "Application Number"],
            col_type="TEXT", normalizer="norm_patent_no",
            db_affinity="TEXT", is_key_candidate=True,
        ),

        # ─── 会社情報 ───
        ColumnSpec(
            name_sql="COMP_LEGAL_NAME",
            source_headers=["COMP_LEGAL_NAME", "comp_legal_name",
                            "Legal Name", "Company Legal Name"],
            col_type="TEXT", normalizer="norm_company_name",
            db_affinity="TEXT",
        ),
        ColumnSpec(
            name_sql="Country_Of_Registration",
            source_headers=["Country_Of_Registration", "country_of_registration",
                            "Country of Registration"],
            col_type="TEXT", normalizer="norm_text",
            db_affinity="TEXT",
        ),

        # ─── 日付系 ───
        # 本番CSVは "YYYY-MM-DD HH:MM:SS" 形式。norm_date で日付部分のみ抽出。
        ColumnSpec(
            name_sql="IPRD_SIGNATURE_DATE",
            source_headers=["IPRD_SIGNATURE_DATE", "iprd_signature_date",
                            "Signature Date"],
            col_type="DATE", normalizer="norm_date",
            db_affinity="TEXT",
        ),
        ColumnSpec(
            name_sql="Reflected_Date",
            source_headers=["Reflected_Date", "Reflected Date",
                            "reflected_date", "REFLECTED_DATE"],
            col_type="DATE", normalizer="norm_date",
            db_affinity="TEXT",
        ),
        ColumnSpec(
            name_sql="PBPA_APP_DATE",
            source_headers=["PBPA_APP_DATE", "pbpa_app_date",
                            "Application Date"],
            col_type="DATE", normalizer="norm_date",
            db_affinity="TEXT",
        ),

        # ─── 3GPP / Standard 系 ───
        ColumnSpec(
            name_sql="TGPP_NUMBER",
            source_headers=["TGPP_NUMBER", "tgpp_number",
                            "3GPP Number", "Spec Number"],
            col_type="TEXT", normalizer="norm_text",
            db_affinity="TEXT",
        ),
        ColumnSpec(
            name_sql="TGPV_VERSION",
            source_headers=["TGPV_VERSION", "tgpv_version",
                            "3GPP Version", "Version"],
            col_type="TEXT", normalizer="norm_text",
            db_affinity="TEXT",
        ),
        ColumnSpec(
            name_sql="Standard",
            source_headers=["Standard", "standard"],
            col_type="TEXT", normalizer="norm_text",
            db_affinity="TEXT",
        ),

        # ─── 宣言種別 ───
        ColumnSpec(
            name_sql="Patent_Type",
            source_headers=["Patent_Type", "patent_type",
                            "IPRD_TYPE", "iprd_type", "Declaration Type"],
            col_type="TEXT", normalizer="norm_text",
            db_affinity="TEXT",
        ),

        # ─── 世代フラグ (0/1) ───
        ColumnSpec(
            name_sql="Gen_2G",
            source_headers=["2G"],
            col_type="BOOL", normalizer="norm_bool",
            db_affinity="INTEGER",
        ),
        ColumnSpec(
            name_sql="Gen_3G",
            source_headers=["3G"],
            col_type="BOOL", normalizer="norm_bool",
            db_affinity="INTEGER",
        ),
        ColumnSpec(
            name_sql="Gen_4G",
            source_headers=["4G"],
            col_type="BOOL", normalizer="norm_bool",
            db_affinity="INTEGER",
        ),
        ColumnSpec(
            name_sql="Gen_5G",
            source_headers=["5G"],
            col_type="BOOL", normalizer="norm_bool",
            db_affinity="INTEGER",
        ),

        # ─── 特許追加情報 ───
        ColumnSpec(
            name_sql="PBPA_TITLEEN",
            source_headers=["PBPA_TITLEEN", "pbpa_titleen"],
            col_type="TEXT", normalizer="norm_text",
            db_affinity="TEXT",
        ),
        ColumnSpec(
            name_sql="Normalized_Patent",
            source_headers=["Normalized_Patent", "normalized_patent"],
            col_type="TEXT", normalizer="norm_text",
            db_affinity="TEXT",
        ),

        # ─── メタ (CSV 読込時に自動付与) ───
        ColumnSpec(
            name_sql="__src_rownum",
            source_headers=[],   # CSV から読まない (自動付番)
            col_type="INT",
            nullable=False,
            normalizer=None,
            db_affinity="INTEGER",
        ),
    ]


# 便利参照
COLUMN_SPECS: list[ColumnSpec] = build_column_specs()

# name_sql → ColumnSpec
COLUMN_MAP: dict[str, ColumnSpec] = {c.name_sql: c for c in COLUMN_SPECS}

# CSV 由来列のみ (source_headers あり)
CSV_COLUMN_SPECS: list[ColumnSpec] = [c for c in COLUMN_SPECS if c.source_headers]

# unique_unit 候補列
KEY_CANDIDATE_COLUMNS: list[str] = [c.name_sql for c in COLUMN_SPECS if c.is_key_candidate]
