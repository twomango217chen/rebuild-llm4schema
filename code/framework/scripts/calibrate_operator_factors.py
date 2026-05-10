from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
SRC = ROOT / "src"
for path in (REPO_ROOT, SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import yaml
import sqlparse

from schema_tuning.collectors.metadata import _collect_schema_from_information_schema, _connect_mysql
from schema_tuning.config import load_config
from schema_tuning.evaluators.performance import (
    _access_factors,
    _extract_access_modes,
    _extract_aliases,
    _extract_join_pairs,
    _extract_operator_flags,
    _extract_table_column_usage,
    _extract_tables,
    _extract_tables_from_plan,
    _join_selectivity,
    _selectivity_for_join,
    _table_metadata,
)
from schema_tuning.workload.runner import load_workload_sql, resolve_workload_paths, run_workload_map

logger = logging.getLogger(__name__)

OPERATOR_KEYS = [
    "nested_loop",
    "hash_join",
    "merge_join",
    "unknown_join",
    "sort",
    "group",
]


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def _read_schema_sql(schema_path: Path) -> str:
    if not schema_path.exists():
        raise FileNotFoundError(f"schema sql not found: {schema_path}")
    return schema_path.read_text(encoding="utf-8")


def _execute_schema(conn, schema_sql: str) -> None:
    statements = [stmt.strip() for stmt in sqlparse.split(schema_sql) if stmt.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)


def _run_explain_analyze(conn, sql: str) -> str:
    statements = [stmt.strip() for stmt in sqlparse.split(sql) if stmt.strip()]
    if not statements:
        raise ValueError("empty SQL for explain")

    def _explain(statement: str) -> str:
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN ANALYZE {statement}")
            rows = cur.fetchall()
        parts: list[str] = []
        for row in rows:
            if isinstance(row, dict):
                value = next(iter(row.values()), None)
            else:
                value = row[0] if row else None
            if value is None:
                continue
            parts.append(str(value))
        result = "\n".join(parts).strip()
        if not result:
            raise ValueError("EXPLAIN ANALYZE returned empty output")
        return result

    if len(statements) == 1:
        return _explain(statements[0])

    def _strip_leading_comments(statement: str) -> str:
        cleaned = sqlparse.format(statement, strip_comments=True).strip()
        return cleaned

    target_idx = None
    for idx, stmt in enumerate(statements):
        head = _strip_leading_comments(stmt).lstrip()
        if head.lower().startswith("select") or head.lower().startswith("with"):
            target_idx = idx
    if target_idx is None:
        raise ValueError("no SELECT statement found for explain")

    with conn.cursor() as cur:
        for stmt in statements[:target_idx]:
            cur.execute(stmt)

    try:
        result = _explain(statements[target_idx])
    finally:
        with conn.cursor() as cur:
            for stmt in statements[target_idx + 1 :]:
                cur.execute(stmt)

    return result


def _build_context(sql: str, plan_text: str) -> Dict[str, Any]:
    alias_map = _extract_aliases(sql)
    usage = _extract_table_column_usage(sql)
    tables = _extract_tables(sql) | set(usage.keys())
    if not tables:
        tables = _extract_tables_from_plan(plan_text, alias_map)
    columns = {table: set() for table in tables}
    for table, cols in usage.items():
        columns.setdefault(table, set()).update(cols)
    operator_flags = _extract_operator_flags(plan_text)
    return {
        "tables": tables,
        "columns": columns,
        "joins": _extract_join_pairs(sql),
        "access_modes": _extract_access_modes(plan_text, alias_map),
        "join_types": operator_flags.get("join_types", []),
        "has_sort": operator_flags.get("has_sort", False),
        "has_group": operator_flags.get("has_group", False),
        "sql": sql,
    }


def _cost_features(
    context: Dict[str, Any],
    stats: Dict[str, Any],
    access_factors: Dict[str, float],
    selectivity_samples: Dict[str, Any],
    config: Dict[str, Any],
) -> Dict[str, float]:
    features = {"table_term": 0.0}
    for key in OPERATOR_KEYS:
        features[key] = 0.0

    for table in context.get("tables", set()):
        entry = stats.get(table)
        if not entry:
            continue
        mode = context.get("access_modes", {}).get(table, "table_scan")
        factor = access_factors.get(mode, 1.0)
        features["table_term"] += float(entry.get("rows", 0.0)) * float(entry.get("width", 1.0)) * factor

    join_pairs = context.get("joins", [])
    join_types = context.get("join_types", [])
    for idx, pair in enumerate(join_pairs):
        if len(pair) != 4:
            continue
        left_table, left_col, right_table, right_col = pair
        left = stats.get(left_table)
        right = stats.get(right_table)
        if not left or not right:
            continue
        join_key = f"{left_table}.{left_col}={right_table}.{right_col}"
        join_selectivity = _selectivity_for_join(
            selectivity_samples,
            left_table,
            right_table,
            [join_key],
            _join_selectivity(config),
        )
        join_rows = max(float(left.get("rows", 0.0)), float(right.get("rows", 0.0))) * join_selectivity
        join_type = join_types[idx] if idx < len(join_types) else "unknown_join"
        base_term = 0.0
        if join_type == "nested_loop":
            base_term += float(left.get("rows", 0.0)) * float(right.get("rows", 0.0))
        elif join_type in {"hash_join", "merge_join", "unknown_join"}:
            base_term += float(left.get("rows", 0.0)) + float(right.get("rows", 0.0))
        base_term += join_rows
        if join_type not in features:
            join_type = "unknown_join"
        features[join_type] += base_term

    total_rows = 0.0
    for table in context.get("tables", set()):
        entry = stats.get(table)
        if entry:
            total_rows += float(entry.get("rows", 0.0))

    if context.get("has_sort") and total_rows > 0:
        features["sort"] += total_rows * math.log2(max(total_rows, 2.0))

    if context.get("has_group") and total_rows > 0:
        features["group"] += total_rows * math.log2(max(total_rows, 2.0))

    return features


def _gaussian_solve(matrix: List[List[float]], vector: List[float]) -> List[float]:
    size = len(vector)
    for col in range(size):
        pivot = col
        for row in range(col + 1, size):
            if abs(matrix[row][col]) > abs(matrix[pivot][col]):
                pivot = row
        if abs(matrix[pivot][col]) < 1e-12:
            raise ValueError("singular matrix")
        if pivot != col:
            matrix[col], matrix[pivot] = matrix[pivot], matrix[col]
            vector[col], vector[pivot] = vector[pivot], vector[col]
        pivot_val = matrix[col][col]
        for k in range(col, size):
            matrix[col][k] /= pivot_val
        vector[col] /= pivot_val
        for row in range(size):
            if row == col:
                continue
            factor = matrix[row][col]
            if abs(factor) < 1e-12:
                continue
            for k in range(col, size):
                matrix[row][k] -= factor * matrix[col][k]
            vector[row] -= factor * vector[col]
    return vector


def _solve_linear_regression(features: List[List[float]], targets: List[float], ridge: float = 1e-6) -> List[float]:
    if not features:
        raise ValueError("no features for regression")
    cols = len(features[0])
    xtx = [[0.0 for _ in range(cols)] for _ in range(cols)]
    xty = [0.0 for _ in range(cols)]
    for row, target in zip(features, targets):
        for i in range(cols):
            xty[i] += row[i] * target
            for j in range(cols):
                xtx[i][j] += row[i] * row[j]
    for i in range(cols):
        xtx[i][i] += ridge
    return _gaussian_solve(xtx, xty)


def _update_config(path: Path, config: Dict[str, Any], updates: Dict[str, Any], enable: bool | None) -> None:
    perf_cfg = config.setdefault("performance", {})
    calibration = perf_cfg.setdefault("calibration", {})
    calibration["operator_factors"] = updates.get("operator_factors", {})
    calibration["global_factor"] = updates.get("global_factor", calibration.get("global_factor", 1.0))
    if enable is not None:
        calibration["enabled"] = bool(enable)

    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(config, handle, sort_keys=False, allow_unicode=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate operator factors using real DB execution.")
    parser.add_argument("--config", default="./framework/configs/default.yaml", help="Path to config YAML")
    parser.add_argument("--output-config", default="", help="Write calibrated factors to a new config file")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of SQL statements for calibration")
    parser.add_argument("--apply-schema", action="store_true", help="Apply schema SQL before running workload")
    parser.add_argument("--schema-sql", default="", help="Override schema SQL path")
    parser.add_argument("--enable", action="store_true", help="Enable calibration in config")
    parser.add_argument("--dry-run", action="store_true", help="Run calibration without writing config")
    args = parser.parse_args()

    _setup_logging()
    config_path = Path(args.config)
    config = load_config(str(config_path))

    sql_dir, _ = resolve_workload_paths(config)
    workload_sql = load_workload_sql(sql_dir)
    if args.limit and args.limit > 0:
        workload_sql = dict(list(workload_sql.items())[: args.limit])

    mysql_cfg = config.get("mysql", {})
    conn = _connect_mysql(mysql_cfg)
    try:
        if args.apply_schema:
            schema_value = args.schema_sql or str(config.get("metadata", {}).get("schema_sql_path") or "")
            if not schema_value:
                raise ValueError("schema sql path is empty")
            schema_path = Path(schema_value)
            if not schema_path.is_absolute():
                schema_path = (REPO_ROOT / schema_path).resolve()
            logger.info("apply schema: %s", schema_path)
            schema_sql = _read_schema_sql(schema_path)
            _execute_schema(conn, schema_sql)

        logger.info("collect schema from information_schema")
        schema_state, storage_stats = _collect_schema_from_information_schema(conn, str(mysql_cfg.get("database")))

        stats, _, _ = _table_metadata(schema_state, storage_stats, config)
        access_factors = _access_factors(config)
        selectivity_samples: Dict[str, Any] = {}

        logger.info("run EXPLAIN ANALYZE for workload")
        plan_texts: Dict[str, str] = {}
        for sql_id, sql in workload_sql.items():
            plan_texts[sql_id] = _run_explain_analyze(conn, sql)

        logger.info("run workload to collect actual latency")
        workload_result = run_workload_map(workload_sql, config)
        metrics = {row["sql_id"]: row for row in workload_result.get("metrics", [])}
    finally:
        conn.close()

    features_matrix: List[List[float]] = []
    targets: List[float] = []
    used_sql: List[str] = []

    for sql_id, sql in workload_sql.items():
        metric = metrics.get(sql_id)
        if not metric:
            continue
        plan_text = plan_texts.get(sql_id, "")
        context = _build_context(sql, plan_text)
        if not context.get("tables"):
            continue
        feat = _cost_features(context, stats, access_factors, selectivity_samples, config)
        row = [feat["table_term"]] + [feat[key] for key in OPERATOR_KEYS]
        features_matrix.append(row)
        targets.append(float(metric.get("avg_latency_ms", 0.0)))
        used_sql.append(sql_id)

    if not features_matrix:
        raise RuntimeError("no valid SQL for calibration")

    logger.info("calibrate operator factors on %d SQL statements", len(features_matrix))
    if len(features_matrix) < len(OPERATOR_KEYS) + 1:
        logger.warning("samples < features; calibration may be unstable")

    coefficients = _solve_linear_regression(features_matrix, targets)
    table_coeff = coefficients[0] if coefficients[0] > 1e-9 else 1.0
    operator_factors: Dict[str, float] = {}
    for idx, key in enumerate(OPERATOR_KEYS, 1):
        raw = coefficients[idx] / table_coeff
        operator_factors[key] = max(min(raw, 10.0), 0.1)

    result = {
        "used_sql": used_sql,
        "coefficients": {"table_term": coefficients[0], **{k: coefficients[i + 1] for i, k in enumerate(OPERATOR_KEYS)}},
        "operator_factors": operator_factors,
        "global_factor": table_coeff,
    }

    output_dir = Path(str(config.get("project", {}).get("output_dir", "./output")))
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "operator_calibration.json"
    output_path.write_text(json.dumps(result, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    logger.info("write calibration summary: %s", output_path)

    if not args.dry_run:
        updates = {"operator_factors": operator_factors, "global_factor": table_coeff}
        output_config = Path(args.output_config) if args.output_config else config_path
        _update_config(output_config, config, updates, enable=True if args.enable else None)
        logger.info("updated config: %s", output_config)


if __name__ == "__main__":
    main()
