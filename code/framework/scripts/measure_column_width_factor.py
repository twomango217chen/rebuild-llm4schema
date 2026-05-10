from __future__ import annotations

import argparse
import time
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
SRC = ROOT / "src"
for path in (REPO_ROOT, SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from schema_tuning.config import load_config
from schema_tuning.collectors.metadata import _connect_mysql


def _parse_columns(value: str) -> list[str]:
    return [col.strip() for col in value.split(",") if col.strip()]


def _run_query(conn, table: str, columns: list[str], limit: int | None) -> float:
    cols_sql = ", ".join(columns)
    sql = f"SELECT SQL_NO_CACHE {cols_sql} FROM {table}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.fetchall()
    return time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config YAML")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--cols1", required=True, help="Comma-separated columns for query 1")
    parser.add_argument("--cols2", required=True, help="Comma-separated columns for query 2")
    parser.add_argument("--limit", type=int, default=None, help="Optional LIMIT to cap rows")
    parser.add_argument("--repeat", type=int, default=1, help="Repeat times and average")
    args = parser.parse_args()

    cols1 = _parse_columns(args.cols1)
    cols2 = _parse_columns(args.cols2)
    if not cols1 or not cols2:
        raise ValueError("cols1 and cols2 must be non-empty")

    config = load_config(args.config)
    conn = _connect_mysql(config.get("mysql", {}))
    try:
        t1 = 0.0
        t2 = 0.0
        for _ in range(max(args.repeat, 1)):
            t1 += _run_query(conn, args.table, cols1, args.limit)
            t2 += _run_query(conn, args.table, cols2, args.limit)
        t1 /= max(args.repeat, 1)
        t2 /= max(args.repeat, 1)
    finally:
        conn.close()

    factor = (t1 / t2) * (len(cols2) / len(cols1)) if t2 > 0 else 0.0
    print(f"time1_s={t1:.6f}")
    print(f"time2_s={t2:.6f}")
    print(f"col_num1={len(cols1)}")
    print(f"col_num2={len(cols2)}")
    print(f"factor={factor:.6f}")


if __name__ == "__main__":
    main()
