from __future__ import annotations

from typing import Any, Dict, List, Tuple
import csv
import logging
from pathlib import Path
import re
import time

import pymysql
import sqlparse

from schema_tuning.utils.sql import list_sql_files, read_sql, sql_id_from_path

logger = logging.getLogger(__name__)


def run_workload(sql_list: List[str], config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute SQL list and collect latency metrics."""
    sql_map = {f"q{idx + 1:04d}": sql for idx, sql in enumerate(sql_list)}
    return run_workload_map(sql_map, config)


def run_workload_map(sql_map: Dict[str, str], config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute SQL map and collect per-query latency metrics."""
    workload_cfg = config.get("workload", {})
    repeats = int(workload_cfg.get("run_repetitions", 1))
    warmup_runs = int(workload_cfg.get("warmup_runs", 0))
    use_metrics_freq = bool(workload_cfg.get("use_metrics_freq", False))
    freq_scale = float(workload_cfg.get("freq_scale", 1.0))
    max_reps = workload_cfg.get("max_repetitions")
    max_reps = int(max_reps) if isinstance(max_reps, (int, float, str)) and str(max_reps).strip() else None
    if repeats <= 0:
        raise ValueError("workload.run_repetitions must be >= 1")
    if warmup_runs < 0:
        raise ValueError("workload.warmup_runs must be >= 0")

    mysql_cfg = config.get("mysql", {})
    results: List[Dict[str, Any]] = []
    total_latency_ms = 0.0

    freq_map = _load_metrics_freq(config) if use_metrics_freq else {}

    conn = _connect_mysql(mysql_cfg)
    try:
        for sql_id, sql in sql_map.items():
            sql_text = (sql or "").strip()
            if not sql_text:
                logger.warning("skip empty SQL: %s", sql_id)
                continue

            for _ in range(warmup_runs):
                _execute_sql(conn, sql_text)

            run_count = repeats
            if use_metrics_freq:
                raw_freq = freq_map.get(sql_id)
                if raw_freq is not None:
                    scaled = max(int(round(float(raw_freq) * freq_scale)), 1)
                    if max_reps is not None:
                        run_count = min(scaled, max_reps)
                    else:
                        run_count = scaled

            latencies: List[float] = []
            for _ in range(run_count):
                latency_ms = _timed_execute(conn, sql_text)
                latencies.append(latency_ms)

            avg_latency_ms = sum(latencies) / float(len(latencies))
            total_latency = sum(latencies)
            total_latency_ms += total_latency

            results.append(
                {
                    "sql_id": sql_id,
                    "freq": float(run_count),
                    "avg_latency_ms": avg_latency_ms,
                    "total_latency_ms": total_latency,
                }
            )
            logger.info("sql_id=%s avg_ms=%.3f total_ms=%.3f freq=%.1f", sql_id, avg_latency_ms, total_latency, float(run_count))
    finally:
        conn.close()

    return {"total_latency_ms": total_latency_ms, "metrics": results}


def load_workload_sql(sql_dir: Path) -> Dict[str, str]:
    if not sql_dir.exists():
        raise FileNotFoundError(f"sql dir not found: {sql_dir}")
    workload: Dict[str, str] = {}
    for sql_path in sorted(list_sql_files(str(sql_dir))):
        sql_id = sql_id_from_path(sql_path)
        workload[sql_id] = read_sql(sql_path)
    return workload


def write_metrics_csv(metrics: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["sql_id", "freq", "avg_latency_ms", "total_latency_ms"],
        )
        writer.writeheader()
        for row in metrics:
            writer.writerow(
                {
                    "sql_id": row.get("sql_id"),
                    "freq": row.get("freq"),
                    "avg_latency_ms": row.get("avg_latency_ms"),
                    "total_latency_ms": row.get("total_latency_ms"),
                }
            )


def resolve_workload_paths(config: Dict[str, Any], output_override: str | None = None) -> Tuple[Path, Path]:
    workload_cfg = config.get("workload", {})
    dataset_root = Path(str(workload_cfg.get("dataset_root", "."))).expanduser()
    sql_dir_value = str(workload_cfg.get("sql_dir", "workload/sql")).strip()
    metrics_value = output_override or workload_cfg.get("metrics_csv", "workload/metrics.csv")
    sql_dir = _resolve_path(dataset_root, sql_dir_value, "workload/sql")
    metrics_path = _resolve_path(dataset_root, metrics_value, "workload/metrics.csv")
    return sql_dir, metrics_path


def _load_metrics_freq(config: Dict[str, Any]) -> Dict[str, float]:
    workload_cfg = config.get("workload", {})
    dataset_root = Path(str(workload_cfg.get("dataset_root", "."))).expanduser()
    metrics_value = workload_cfg.get("metrics_csv", "workload/metrics.csv")
    metrics_path = _resolve_path(dataset_root, metrics_value, "workload/metrics.csv")
    if not metrics_path.exists():
        return {}
    freq_map: Dict[str, float] = {}
    with metrics_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            sql_id = (row.get("sql_id") or "").strip()
            if not sql_id:
                continue
            try:
                freq_map[sql_id] = float(row.get("freq") or 0.0)
            except ValueError:
                continue
    return freq_map


def _resolve_path(dataset_root: Path, raw_path: Any, default_name: str) -> Path:
    value = str(raw_path or "").strip()
    if not value:
        return dataset_root / default_name
    path = Path(value)
    if path.is_absolute():
        return path
    return dataset_root / path


def _connect_mysql(mysql_cfg: Dict[str, Any]) -> pymysql.connections.Connection:
    required_keys = ("host", "port", "user", "database")
    missing = [key for key in required_keys if mysql_cfg.get(key) in (None, "")]
    if missing:
        raise ValueError(f"mysql config missing: {', '.join(missing)}")
    return pymysql.connect(
        host=str(mysql_cfg["host"]),
        port=int(mysql_cfg["port"]),
        user=str(mysql_cfg["user"]),
        password=str(mysql_cfg.get("password", "")),
        database=str(mysql_cfg["database"]),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def _strip_leading_comments(statement: str) -> str:
    cleaned = re.sub(r"(?is)^\s*/\*.*?\*/\s*", "", statement)
    lines = []
    for line in cleaned.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines).lstrip()


def _split_sql_statements(sql: str) -> List[str]:
    return [stmt.strip() for stmt in sqlparse.split(sql) if stmt.strip()]


def _select_statement_index(statements: List[str]) -> int | None:
    for idx, stmt in enumerate(statements):
        head = _strip_leading_comments(stmt)
        if re.match(r"(?is)^\s*(select|with)\b", head):
            return idx
    return None


def _execute_sql(conn: pymysql.connections.Connection, sql: str) -> None:
    statements = _split_sql_statements(sql)
    if not statements:
        return
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
            cur.fetchall()


def _timed_execute(conn: pymysql.connections.Connection, sql: str) -> float:
    statements = _split_sql_statements(sql)
    if not statements:
        return 0.0
    if len(statements) == 1:
        start = time.perf_counter()
        _execute_sql(conn, sql)
        end = time.perf_counter()
        return (end - start) * 1000.0

    select_idx = _select_statement_index(statements)
    if select_idx is None:
        start = time.perf_counter()
        _execute_sql(conn, sql)
        end = time.perf_counter()
        return (end - start) * 1000.0

    with conn.cursor() as cur:
        for stmt in statements[:select_idx]:
            cur.execute(stmt)
            cur.fetchall()

    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(statements[select_idx])
        cur.fetchall()
    end = time.perf_counter()

    with conn.cursor() as cur:
        for stmt in statements[select_idx + 1 :]:
            cur.execute(stmt)
            cur.fetchall()

    return (end - start) * 1000.0
