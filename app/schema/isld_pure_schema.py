"""
schema/isld_pure_schema.py  –  isld_pure テーブルの DDL 生成・管理
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.schema.isld_column_specs import COLUMN_SPECS, ColumnSpec

if TYPE_CHECKING:
    from app.io.sqlite_io import SqliteIO


TABLE_NAME = "isld_pure"


def _col_ddl(c: ColumnSpec) -> str:
    parts = [c.name_sql, c.db_affinity]
    if not c.nullable:
        parts.append("NOT NULL")
    return " ".join(parts)


def create_table_sql() -> str:
    cols = ",\n    ".join(_col_ddl(c) for c in COLUMN_SPECS)
    return f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n    {cols}\n);"


def create_indexes_sql() -> list[str]:
    """最小インデックスセット"""
    indexes: list[str] = []
    # unique_unit 候補
    for col in ("PUBL_NUMBER", "PATT_APPLICATION_NUMBER", "DIPG_ID", "DIPG_PATF_ID"):
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_{col} ON {TABLE_NAME}({col});"
        )
    # scope 頻出
    for col in ("Country_Of_Registration", "PBPA_APP_DATE", "TGPP_NUMBER", "TGPV_VERSION"):
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_{col} ON {TABLE_NAME}({col});"
        )
    # 世代フラグ（scope で頻繁に使用）
    for col in ("Gen_4G", "Gen_5G"):
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_{col} ON {TABLE_NAME}({col});"
        )
    # 派生キー（company_key, country_key）
    for col in ("company_key", "country_key"):
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_{col} ON {TABLE_NAME}({col});"
        )
    return indexes


def table_exists(sio: "SqliteIO") -> bool:
    """isld_pure テーブルが存在するか"""
    row = sio.query_one(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;",
        [TABLE_NAME],
    )
    return row is not None and row[0] > 0


def create_table(sio: "SqliteIO") -> None:
    sio.execute(create_table_sql())


def create_indexes(sio: "SqliteIO") -> None:
    for sql in create_indexes_sql():
        sio.execute(sql)


def insert_sql() -> str:
    """INSERT 文（placeholder 付き）"""
    cols = ", ".join(c.name_sql for c in COLUMN_SPECS)
    placeholders = ", ".join("?" for _ in COLUMN_SPECS)
    return f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({placeholders});"
