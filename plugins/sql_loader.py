from __future__ import annotations

import os
from pathlib import Path

SQL_DIR = Path(os.getenv("HN_SQL_DIR", "/opt/airflow/sql"))


def load_sql(filename: str) -> str:
    path = SQL_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"SQL não encontrado: {path}")
    return path.read_text(encoding="utf-8")
