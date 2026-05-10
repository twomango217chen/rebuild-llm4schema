from __future__ import annotations

from pathlib import Path
from typing import Iterable, List


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def write_text(path: str, content: str) -> None:
    Path(path).write_text(content, encoding="utf-8")


def list_files(root: str, suffix: str) -> List[str]:
    return [str(p) for p in Path(root).rglob(f"*{suffix}")]
