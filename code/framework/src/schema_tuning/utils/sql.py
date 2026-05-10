from __future__ import annotations

from pathlib import Path
from typing import List


def list_sql_files(sql_dir: str) -> List[str]:
    return [str(p) for p in Path(sql_dir).glob("*.sql")]


def read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def sql_id_from_path(path: str) -> str:
    return Path(path).stem
