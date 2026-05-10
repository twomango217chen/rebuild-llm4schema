from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
import re
from typing import Any, Dict, Set, Tuple

import pymysql
import sqlparse

from schema_tuning.utils.filesystem import read_text
from schema_tuning.utils.sql import list_sql_files, read_sql, sql_id_from_path

logger = logging.getLogger(__name__)


def _read_json_if_exists(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        return {}
    return json.loads(content)


def _merge_schema_constraints(schema_state: Dict[str, Any], constraints: Dict[str, Any]) -> None:
    if not isinstance(constraints, dict) or not constraints:
        return
    tables = constraints.get("tables")
    if not isinstance(tables, dict):
        tables = {}

    relations = schema_state.setdefault("relations", [])
    existing_rel = {
        (str(item.get("from")), str(item.get("to")))
        for item in relations
        if isinstance(item, dict) and item.get("from") and item.get("to")
    }

    def _ensure_table(name: str) -> Dict[str, Any]:
        return schema_state.setdefault(
            "tables",
            {},
        ).setdefault(
            name,
            {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "unique_constraints": [],
                "check_constraints": [],
                "lineage": {"origin": name, "derived_from": []},
            },
        )

    def _add_fk(from_value: str, to_value: str) -> None:
        if not from_value or not to_value:
            return
        key = (from_value, to_value)
        if key in existing_rel:
            return
        relations.append({"from": from_value, "to": to_value})
        existing_rel.add(key)
        if "." in from_value:
            table, _ = from_value.split(".", 1)
            table_info = _ensure_table(table)
            table_info.setdefault("foreign_keys", []).append({"from": from_value, "to": to_value})

    for table_name, info in tables.items():
        if not isinstance(info, dict):
            continue
        table = str(table_name)
        table_info = _ensure_table(table)
        pk_list = info.get("primary_key") or info.get("primary_keys") or []
        if isinstance(pk_list, list):
            for col in pk_list:
                col_name = str(col)
                if col_name and col_name not in table_info["primary_key"]:
                    table_info["primary_key"].append(col_name)
        fk_list = info.get("foreign_keys") or []
        if isinstance(fk_list, list):
            for fk in fk_list:
                if not isinstance(fk, dict):
                    continue
                from_value = str(fk.get("from") or "")
                to_value = str(fk.get("to") or "")
                if not from_value or not to_value:
                    column = str(fk.get("column") or fk.get("from_column") or "")
                    ref_table = str(
                        fk.get("ref_table") or fk.get("to_table") or fk.get("referenced_table") or ""
                    )
                    ref_column = str(
                        fk.get("ref_column") or fk.get("to_column") or fk.get("referenced_column") or ""
                    )
                    if column and ref_table and ref_column:
                        from_value = f"{table}.{column}"
                        to_value = f"{ref_table}.{ref_column}"
                _add_fk(from_value, to_value)

    for fk in constraints.get("relations") or []:
        if not isinstance(fk, dict):
            continue
        from_value = str(fk.get("from") or "")
        to_value = str(fk.get("to") or "")
        _add_fk(from_value, to_value)


def _load_metrics(metrics_path: Path, required: bool) -> Dict[str, Dict[str, float]]:
    if not metrics_path.exists():
        if required:
            raise FileNotFoundError(f"metrics.csv not found: {metrics_path}")
        return {}

    metrics: Dict[str, Dict[str, float]] = {}
    with metrics_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            sql_id = (row.get("sql_id") or "").strip()
            if not sql_id:
                continue
            freq = float(row.get("freq") or 0.0)
            avg_latency_ms = float(row.get("avg_latency_ms") or 0.0)
            total_latency_ms = row.get("total_latency_ms")
            if total_latency_ms in (None, ""):
                total_latency = avg_latency_ms * freq
            else:
                total_latency = float(total_latency_ms)

            metrics[sql_id] = {
                "freq": freq,
                "avg_latency_ms": avg_latency_ms,
                "total_latency_ms": total_latency,
            }
    return metrics


def _load_plans(explain_dir: Path) -> Dict[str, str]:
    if not explain_dir.exists():
        return {}

    plans: Dict[str, str] = {}
    for plan_path in sorted(explain_dir.glob("*.txt")):
        plans[plan_path.stem] = plan_path.read_text(encoding="utf-8")
    return plans


def _load_workload(sql_dir: Path) -> Dict[str, str]:
    if not sql_dir.exists():
        raise FileNotFoundError(f"sql dir not found: {sql_dir}")

    workload: Dict[str, str] = {}
    for sql_path in sorted(list_sql_files(str(sql_dir))):
        sql_id = sql_id_from_path(sql_path)
        workload[sql_id] = read_sql(sql_path)
    return workload


def _table_sizes_from_schema(schema_state: Dict[str, Any]) -> Dict[str, int]:
    result: Dict[str, int] = {}
    tables = schema_state.get("tables", {})
    if not isinstance(tables, dict):
        return result

    for table_name, table_info in tables.items():
        if not isinstance(table_info, dict):
            continue
        size = table_info.get("size_bytes")
        if isinstance(size, (int, float)):
            result[table_name] = int(size)
    return result


def _table_name_map(schema_state: Dict[str, Any]) -> Dict[str, str]:
    tables = schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}
    if not isinstance(tables, dict):
        return {}
    return {str(name).lower(): str(name) for name in tables.keys()}


def _extract_from_clause(sql: str) -> str:
    match = re.search(r"\bfrom\b", sql, re.IGNORECASE)
    if not match:
        return ""
    tail = sql[match.end():]
    stop_pattern = re.compile(r"\b(where|group\s+by|order\s+by|having|limit|union)\b", re.IGNORECASE)
    stop_match = stop_pattern.search(tail)
    if stop_match:
        tail = tail[:stop_match.start()]
    return tail.strip()


def _extract_tables(sql: str, table_map: Dict[str, str] | None = None) -> set[str]:
    from_clause = _extract_from_clause(sql)
    if not from_clause:
        return set()
    pattern = re.compile(
        r"(?:from|join|,)\s+`?([A-Za-z0-9_]+)`?(?:\s+as)?\s*`?([A-Za-z0-9_]+)?`?",
        re.IGNORECASE,
    )
    tables: set[str] = set()
    for match in pattern.finditer("from " + from_clause):
        table = match.group(1)
        if table_map:
            table = table_map.get(table.lower(), table)
        tables.add(table)
    return tables


def _extract_clause_text(sql: str, clause: str, stop_tokens: list[str]) -> str:
    pattern = re.compile(r"\b" + re.escape(clause) + r"\b", re.IGNORECASE)
    match = pattern.search(sql)
    if not match:
        return ""
    tail = sql[match.end():]
    stop_pattern = re.compile(r"\b(" + "|".join(stop_tokens) + r")\b", re.IGNORECASE)
    stop_match = stop_pattern.search(tail)
    if stop_match:
        tail = tail[:stop_match.start()]
    return tail.strip()


def _extract_clause_columns(sql: str, clause: str, stop_tokens: list[str]) -> list[str]:
    text = _extract_clause_text(sql, clause, stop_tokens)
    if not text:
        return []
    return [part.strip() for part in _split_columns(text) if part.strip()]


def _normalize_column_token(token: str) -> tuple[str | None, str | None]:
    cleaned = re.sub(r"\b(asc|desc)\b", "", token, flags=re.IGNORECASE).strip()
    match = re.search(r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?", cleaned)
    if match:
        return match.group(1), match.group(2)
    simple = re.search(r"`?([A-Za-z_][A-Za-z0-9_]*)`?", cleaned)
    if simple:
        return None, simple.group(1)
    return None, None


def _parse_actual_time_ms(line: str) -> float | None:
    match = re.search(r"actual time=([0-9.eE+-]+)\.\.([0-9.eE+-]+)", line)
    if not match:
        return None
    end = float(match.group(2))
    return max(0.0, end)


def _resolve_table_name(name: str, alias_map: Dict[str, str], table_map: Dict[str, str]) -> str:
    raw = name.strip("`")
    resolved = alias_map.get(raw.lower(), raw)
    return table_map.get(str(resolved).lower(), str(resolved))


def _column_name_map(schema_state: Dict[str, Any], table: str) -> Dict[str, str]:
    tables = schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}
    table_info = tables.get(table, {}) if isinstance(tables, dict) else {}
    columns = table_info.get("columns", {}) if isinstance(table_info, dict) else {}
    if not isinstance(columns, dict):
        return {}
    return {str(name).lower(): str(name) for name in columns.keys()}


def _resolve_column_name(schema_state: Dict[str, Any], table: str, column: str) -> str | None:
    col_map = _column_name_map(schema_state, table)
    if not col_map:
        return None
    return col_map.get(column.lower())


def _resolve_unqualified_column(
    schema_state: Dict[str, Any],
    tables: set[str],
    column: str,
) -> tuple[str | None, str | None]:
    matches: list[tuple[str, str]] = []
    for table in tables:
        resolved = _resolve_column_name(schema_state, table, column)
        if resolved:
            matches.append((table, resolved))
    if len(matches) == 1:
        return matches[0]
    return None, None


def _extract_scan_table_from_explain(line: str) -> str | None:
    pattern = re.compile(
        r"(?:Table scan|Index scan|Index lookup|Single-row index lookup|Range scan|Index range scan|Covering index scan|Covering index lookup|Index skip scan) on\s+`?([^\s]+)`?",
        re.IGNORECASE,
    )
    match = pattern.search(line)
    if not match:
        return None
    table = match.group(1).strip()
    if table.startswith("<") and table.endswith(">"):
        return None
    return table


def _extract_sort_columns_from_explain(line: str) -> list[str]:
    if "Sort:" not in line:
        return []
    text = line.split("Sort:", 1)[1].strip()
    if "limit input" in text:
        text = text.split("limit input", 1)[0].strip().rstrip(",")
    text = text.split("(actual time", 1)[0].strip().rstrip(",")
    return [part.strip() for part in _split_columns(text) if part.strip()]


def _format_schema_summary_text(schema_state: Dict[str, Any], storage_stats: Dict[str, Any]) -> str:
    tables = schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}
    if not isinstance(tables, dict) or not tables:
        return ""

    row_counts = storage_stats.get("row_counts", {}) if isinstance(storage_stats, dict) else {}
    if not isinstance(row_counts, dict):
        row_counts = {}

    lines: list[str] = []
    for table_name, table_info in tables.items():
        if not isinstance(table_info, dict):
            continue
        row_count = row_counts.get(table_name)
        row_label = str(int(row_count)) if isinstance(row_count, (int, float)) else "unknown"
        lines.append(f"\"{table_name}\" (rows={row_label}): {{")

        columns = table_info.get("columns", {})
        if not isinstance(columns, dict):
            columns = {}

        primary_keys = set(table_info.get("primary_key", []) or [])
        fk_map: Dict[str, tuple[str, str]] = {}
        for fk in table_info.get("foreign_keys", []) or []:
            if not isinstance(fk, dict):
                continue
            src = str(fk.get("from", ""))
            dst = str(fk.get("to", ""))
            col = src.split(".", 1)[1] if "." in src else src
            ref_table = dst.split(".", 1)[0] if "." in dst else dst
            ref_col = dst.split(".", 1)[1] if "." in dst else ""
            if col:
                fk_map[col] = (ref_table, ref_col)

        for col_name, col_info in columns.items():
            if not isinstance(col_info, dict):
                continue
            col_type = str(col_info.get("type") or "")
            base_type = col_type.split("(", 1)[0].upper() if col_type else "UNKNOWN"
            length = col_info.get("length")

            parts = [f"\"{base_type}\""]
            if isinstance(length, (int, float)):
                parts.append(f"\"len={int(length)}\"")
            if col_name in primary_keys:
                parts.append("\"PRIMARY KEY\"")
            if col_name in fk_map:
                ref_table, ref_col = fk_map[col_name]
                if ref_table and ref_col:
                    parts.append(f"\"FOREIGN KEY REFERENCES {ref_table}({ref_col})\"")

            parts_text = ", ".join(parts)
            lines.append(f"\t\"{col_name}\": {{{parts_text}}},")

        lines.append("},")

    return "\n".join(lines).strip()


def _format_workload_summary_text(
    metrics: Dict[str, Dict[str, float]],
    workload_sql: Dict[str, str],
    plans: Dict[str, str],
    schema_state: Dict[str, Any],
    storage_stats: Dict[str, Any],
) -> str:
    if not workload_sql:
        return "[]"

    if not metrics:
        metrics = {
            sql_id: {"freq": 1.0, "avg_latency_ms": 0.0, "total_latency_ms": 0.0}
            for sql_id in workload_sql.keys()
        }

    if not plans:
        table_map = _table_name_map(schema_state)
        row_counts = storage_stats.get("row_counts", {}) if isinstance(storage_stats, dict) else {}
        if not isinstance(row_counts, dict):
            row_counts = {}

        table_counts: Dict[str, float] = {}
        join_counts: Dict[str, float] = {}
        for sql_id, sql in workload_sql.items():
            metric = metrics.get(sql_id, {})
            freq = float(metric.get("freq") or 0.0)
            if freq <= 0:
                continue
            for table in _extract_tables(sql, table_map):
                table_counts[table] = table_counts.get(table, 0.0) + freq
            for left_table, right_table in _extract_join_pairs(sql, table_map, schema_state):
                key = f"{left_table}<->{right_table}"
                join_counts[key] = join_counts.get(key, 0.0) + freq

        total_queries = int(round(sum(float(item.get("freq") or 0.0) for item in metrics.values())))
        if total_queries <= 0:
            total_queries = len(workload_sql)

        operator_profile: Dict[str, Any] = {
            "summary": {
                "source": "sql_parse_only",
                "sql_count": len(workload_sql),
                "total_query_executions": total_queries,
            },
            "operators": {
                "scan": {
                    "table_level": [],
                    "column_level": [],
                    "operator_total_latency_ms": 0.0,
                    "operator_avg_latency_ms": 0.0,
                },
                "join": {
                    "table_level": [],
                    "column_level": [],
                    "operator_total_latency_ms": 0.0,
                    "operator_avg_latency_ms": 0.0,
                },
                "orderby": {
                    "table_level": [],
                    "column_level": [],
                    "operator_total_latency_ms": 0.0,
                    "operator_avg_latency_ms": 0.0,
                },
                "groupby": {
                    "table_level": [],
                    "column_level": [],
                    "operator_total_latency_ms": 0.0,
                    "operator_avg_latency_ms": 0.0,
                },
            },
        }

        if table_counts:
            for table_name in sorted(table_counts.keys()):
                freq = float(table_counts[table_name])
                record: Dict[str, Any] = {
                    "table": table_name,
                    "frequency": int(round(freq)),
                    "avg_latency_ms": 0.0,
                    "total_latency_ms": 0.0,
                }
                row_count = row_counts.get(table_name)
                if isinstance(row_count, (int, float)):
                    record["row_count"] = float(row_count)
                operator_profile["operators"]["scan"]["table_level"].append(record)

        if join_counts:
            for pair_key in sorted(join_counts.keys()):
                freq = float(join_counts[pair_key])
                left_table, right_table = pair_key.split("<->", 1)
                operator_profile["operators"]["join"]["table_level"].append(
                    {
                        "left_table": left_table,
                        "right_table": right_table,
                        "frequency": int(round(freq)),
                        "avg_latency_ms": 0.0,
                        "total_latency_ms": 0.0,
                    }
                )

        return json.dumps(operator_profile, ensure_ascii=False, indent=2)

    table_map = _table_name_map(schema_state)
    schema_tables = set(schema_state.get("tables", {}).keys()) if isinstance(schema_state, dict) else set()
    row_counts = storage_stats.get("row_counts", {}) if isinstance(storage_stats, dict) else {}
    if not isinstance(row_counts, dict):
        row_counts = {}

    def _accumulate(store: Dict[str, Dict[str, float]], key: str, duration_ms: float, freq: float, row_count: float | None) -> None:
        item = store.setdefault(key, {"freq": 0.0, "total_latency": 0.0, "row_count": row_count})
        item["freq"] += freq
        item["total_latency"] += duration_ms * freq
        if item.get("row_count") is None and row_count is not None:
            item["row_count"] = row_count

    group_stats: Dict[str, Dict[str, float]] = {}
    order_stats: Dict[str, Dict[str, float]] = {}
    scan_stats: Dict[str, Dict[str, float]] = {}
    join_stats: Dict[str, Dict[str, float]] = {}

    for sql_id, sql in workload_sql.items():
        plan = plans.get(sql_id)
        if not plan:
            continue
        metric = metrics.get(sql_id, {})
        freq = float(metric.get("freq") or 0.0)
        if freq <= 0:
            continue

        alias_map = _extract_aliases(sql, table_map)
        tables = {_resolve_table_name(name, alias_map, table_map) for name in _extract_tables(sql, table_map)}
        table_predicates = _extract_table_predicates(sql, table_map)
        where_text = _extract_clause_text(sql, "where", ["group by", "order by", "having", "limit", "union"])
        if where_text:
            where_text = " ".join(where_text.split())
            where_text = _strip_join_predicates(where_text, alias_map, table_map, schema_state)
        sql_join_pairs = _extract_join_pairs(sql, table_map, schema_state)

        group_tokens = _extract_clause_columns(sql, "group by", ["order by", "having", "limit", "union"])
        group_cols: list[str] = []
        for token in group_tokens:
            table, column = _normalize_column_token(token)
            if table:
                table = _resolve_table_name(table, alias_map, table_map)
                resolved_col = _resolve_column_name(schema_state, table, column) if column else None
            else:
                table, resolved_col = _resolve_unqualified_column(schema_state, tables, column) if column else (None, None)
            if table and resolved_col:
                group_cols.append(f"{table}.{resolved_col}")

        lines = [line for line in plan.splitlines() if line.strip()]
        nodes: list[Dict[str, Any]] = []
        join_pattern = re.compile(r"\b(join|nested\s+loop|hash\s+join|merge\s+join|bka\s+join|semi\s+join|anti\s+join)\b", re.IGNORECASE)
        for line in lines:
            indent = len(line) - len(line.lstrip(" "))
            text = line.strip()
            duration = _parse_actual_time_ms(text)
            scan_table = _extract_scan_table_from_explain(text)
            sort_cols = _extract_sort_columns_from_explain(text)
            nodes.append(
                {
                    "indent": indent,
                    "text": text,
                    "duration": duration,
                    "scan_table": scan_table,
                    "sort_cols": sort_cols,
                    "is_join": bool(join_pattern.search(text)),
                    "is_agg": "aggregate" in text.lower(),
                }
            )

        def _tables_from_join_nodes(start_idx: int) -> list[str]:
            base_indent = nodes[start_idx]["indent"]
            found: list[str] = []
            for j in range(start_idx + 1, len(nodes)):
                if nodes[j]["indent"] <= base_indent:
                    break
                table = nodes[j].get("scan_table")
                if not table:
                    continue
                resolved = _resolve_table_name(table, alias_map, table_map)
                if resolved and resolved in schema_tables and resolved not in found:
                    found.append(resolved)
            return found

        def _pairs_from_tables(table_list: list[str]) -> list[tuple[str, str]]:
            pairs: list[tuple[str, str]] = []
            for i in range(len(table_list)):
                for j in range(i + 1, len(table_list)):
                    pairs.append(tuple(sorted((table_list[i], table_list[j]))))
            return pairs

        for idx, node in enumerate(nodes):
            duration = node.get("duration")
            if duration is None:
                continue

            scan_table = node.get("scan_table")
            if scan_table:
                table = _resolve_table_name(scan_table, alias_map, table_map)
                if table:
                    row_count = row_counts.get(table)
                    predicates = table_predicates.get(table, [])
                    if predicates:
                        pred_text = " AND ".join(sorted(set(predicates)))
                        scan_key = f"{table} WHERE {pred_text}"
                    elif where_text:
                        scan_key = f"{table} WHERE {where_text}"
                    else:
                        scan_key = table
                    _accumulate(
                        scan_stats,
                        scan_key,
                        duration,
                        freq,
                        float(row_count) if isinstance(row_count, (int, float)) else None,
                    )

            sort_cols = node.get("sort_cols") or []
            if sort_cols:
                resolved_cols: list[str] = []
                for token in sort_cols:
                    table, column = _normalize_column_token(token)
                    if table:
                        table = _resolve_table_name(table, alias_map, table_map)
                        resolved_col = _resolve_column_name(schema_state, table, column) if column else None
                    else:
                        table, resolved_col = _resolve_unqualified_column(schema_state, tables, column) if column else (None, None)
                    if table and resolved_col:
                        resolved_cols.append(f"{table}.{resolved_col}")
                if resolved_cols:
                    per_col = duration / float(len(resolved_cols))
                    for col in resolved_cols:
                        table = col.split(".", 1)[0]
                        row_count = row_counts.get(table)
                        _accumulate(order_stats, col, per_col, freq, float(row_count) if isinstance(row_count, (int, float)) else None)

            if node.get("is_agg") and group_cols:
                per_col = duration / float(len(group_cols))
                for col in group_cols:
                    table = col.split(".", 1)[0]
                    row_count = row_counts.get(table)
                    _accumulate(group_stats, col, per_col, freq, float(row_count) if isinstance(row_count, (int, float)) else None)

            if node.get("is_join"):
                tables_under = _tables_from_join_nodes(idx)
                if len(tables_under) < 2:
                    continue
                pair_list = _pairs_from_tables(tables_under)
                if sql_join_pairs:
                    pair_list = [pair for pair in pair_list if pair in sql_join_pairs]
                if not pair_list:
                    continue
                per_pair = duration / float(len(pair_list))
                for left_table, right_table in pair_list:
                    join_obj = f"{left_table}<->{right_table}"
                    _accumulate(join_stats, join_obj, per_pair, freq, None)

    total_queries = int(round(sum(float(item.get("freq") or 0.0) for item in metrics.values())))

    def _op_payload() -> Dict[str, Any]:
        return {
            "table_level": [],
            "column_level": [],
            "operator_total_latency_ms": 0.0,
            "operator_avg_latency_ms": 0.0,
        }

    operator_profile: Dict[str, Any] = {
        "summary": {
            "source": "explain_analyze",
            "sql_count": len(workload_sql),
            "total_query_executions": total_queries,
        },
        "operators": {
            "scan": _op_payload(),
            "join": _op_payload(),
            "orderby": _op_payload(),
            "groupby": _op_payload(),
        },
    }

    def _finalize_operator(operator_name: str) -> None:
        op = operator_profile["operators"][operator_name]
        records = op["table_level"] + op["column_level"]
        total = float(sum(float(item.get("total_latency_ms") or 0.0) for item in records))
        freq = float(sum(float(item.get("frequency") or 0.0) for item in records))
        op["operator_total_latency_ms"] = total
        op["operator_avg_latency_ms"] = total / freq if freq > 0 else 0.0

    for key in sorted(scan_stats.keys()):
        item = scan_stats[key]
        freq = float(item.get("freq") or 0.0)
        total = float(item.get("total_latency") or 0.0)
        avg = total / freq if freq > 0 else 0.0
        table_name = key
        predicate = ""
        if " WHERE " in key:
            table_name, predicate = key.split(" WHERE ", 1)
        record: Dict[str, Any] = {
            "table": table_name,
            "frequency": int(round(freq)),
            "avg_latency_ms": avg,
            "total_latency_ms": total,
        }
        if predicate:
            record["predicate"] = predicate
        row_count = item.get("row_count")
        if isinstance(row_count, (int, float)):
            record["row_count"] = float(row_count)
        operator_profile["operators"]["scan"]["table_level"].append(record)

    for key in sorted(join_stats.keys()):
        item = join_stats[key]
        freq = float(item.get("freq") or 0.0)
        total = float(item.get("total_latency") or 0.0)
        avg = total / freq if freq > 0 else 0.0
        left_table, right_table = key.split("<->", 1)
        operator_profile["operators"]["join"]["table_level"].append(
            {
                "left_table": left_table,
                "right_table": right_table,
                "frequency": int(round(freq)),
                "avg_latency_ms": avg,
                "total_latency_ms": total,
            }
        )

    for key in sorted(order_stats.keys()):
        item = order_stats[key]
        freq = float(item.get("freq") or 0.0)
        total = float(item.get("total_latency") or 0.0)
        avg = total / freq if freq > 0 else 0.0
        table_name = ""
        column_name = key
        if "." in key:
            table_name, column_name = key.split(".", 1)
        record = {
            "table": table_name,
            "column": column_name,
            "frequency": int(round(freq)),
            "avg_latency_ms": avg,
            "total_latency_ms": total,
        }
        row_count = item.get("row_count")
        if isinstance(row_count, (int, float)):
            record["row_count"] = float(row_count)
        operator_profile["operators"]["orderby"]["column_level"].append(record)

    for key in sorted(group_stats.keys()):
        item = group_stats[key]
        freq = float(item.get("freq") or 0.0)
        total = float(item.get("total_latency") or 0.0)
        avg = total / freq if freq > 0 else 0.0
        table_name = ""
        column_name = key
        if "." in key:
            table_name, column_name = key.split(".", 1)
        record = {
            "table": table_name,
            "column": column_name,
            "frequency": int(round(freq)),
            "avg_latency_ms": avg,
            "total_latency_ms": total,
        }
        row_count = item.get("row_count")
        if isinstance(row_count, (int, float)):
            record["row_count"] = float(row_count)
        operator_profile["operators"]["groupby"]["column_level"].append(record)

    _finalize_operator("scan")
    _finalize_operator("join")
    _finalize_operator("orderby")
    _finalize_operator("groupby")

    return json.dumps(operator_profile, ensure_ascii=False, indent=2)


def _format_column_cooccurrence_text(cooccurrence: list[Dict[str, Any]]) -> str:
    if not cooccurrence:
        return ""

    grouped: Dict[str, list[Dict[str, Any]]] = {}
    for item in cooccurrence:
        table = str(item.get("table") or "")
        if not table:
            continue
        grouped.setdefault(table, []).append(item)

    lines: list[str] = []
    for table in sorted(grouped.keys()):
        lines.append(f"{table}:")
        for item in grouped[table]:
            cols = item.get("columns", [])
            count = item.get("count", 0)
            if not isinstance(cols, list) or not cols:
                continue
            cols_text = ", ".join(str(col) for col in cols)
            lines.append(f"[{cols_text}] count :{count}")
        lines.append("")

    return "\n".join(lines).strip()


def _default_prompt_template() -> str:
    return (
        "SYSTEM:\n"
        "You are a schema tuning assistant for MySQL 8.0+.\n\n"
        "Task:\n"
        "- Propose a single action sequence that improves workload latency.\n"
        "- Output ONLY a JSON object with the exact key 'actions'.\n"
        "- The value is a JSON array of action objects using the format below.\n"
        "- No extra text, no code fences.\n\n"
        "Action Format:\n{action_format}\n\n"
        "Rules:\n{action_rules}\n\n"
        "USER:\n"
        "Schema Summary:\n{schema_summary}\n\n"
        "Workload Summary:\n{workload_summary}\n\n"
        "Experience Hints:\n{experience_hints}\n"
    )


def _load_experience_hints_from_reference() -> str:
    candidate_paths = [
        Path.cwd() / "references" / "prompt.md",
        Path(__file__).resolve().parents[4] / "references" / "prompt.md",
    ]
    for path in candidate_paths:
        if not path.exists():
            continue
        content = path.read_text(encoding="utf-8")
        start = content.find("## 经验")
        if start < 0:
            continue
        tail = content[start + len("## 经验") :]
        end = tail.find("## 要求")
        section = tail[:end] if end >= 0 else tail
        normalized = section.strip()
        if normalized:
            return normalized
    return (
        "场景: 高频等值连接且连接键高选择性。\n"
        "操作: TableJoin。\n"
        "效果: 降低高频连接开销。\n\n"
        "场景: 宽表冷热列访问差异明显。\n"
        "操作: TableSplit。\n"
        "效果: 降低热点查询扫描宽度。\n\n"
        "场景: 查询大多包含分区键过滤。\n"
        "操作: HorizontalSplit。\n"
        "效果: 缩小扫描范围。\n\n"
        "场景: 仅为取少量主表属性而频繁连接。\n"
        "操作: RedundantColumnAdd。\n"
        "效果: 消除部分高频连接。"
    )


def _extract_aliases(sql: str, table_map: Dict[str, str] | None = None) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    from_clause = _extract_from_clause(sql)
    if not from_clause:
        return alias_map
    pattern = re.compile(
        r"(?:from|join|,)\s+`?([A-Za-z0-9_]+)`?(?:\s+as)?\s*`?([A-Za-z0-9_]+)?`?",
        re.IGNORECASE,
    )
    for match in pattern.finditer("from " + from_clause):
        table = match.group(1)
        if table_map:
            table = table_map.get(table.lower(), table)
        alias = match.group(2)
        if not alias:
            continue
        if alias.lower() in {"on", "where", "join", "left", "right", "inner", "outer", "group", "order", "limit", "having"}:
            continue
        alias_map[alias.lower()] = table
    return alias_map


def _extract_aliases_all(sql: str, table_map: Dict[str, str] | None = None) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    pattern = re.compile(
        r"\b(?:from|join)\s+`?([A-Za-z0-9_]+)`?(?:\s+as)?\s*`?([A-Za-z0-9_]+)?`?",
        re.IGNORECASE,
    )
    for match in pattern.finditer(sql):
        table = match.group(1)
        alias = match.group(2)
        if not alias:
            continue
        if alias.lower() in {"on", "where", "join", "left", "right", "inner", "outer", "group", "order", "limit", "having"}:
            continue
        if table_map:
            table = table_map.get(table.lower(), table)
        alias_map[alias.lower()] = table
    return alias_map


def _extract_table_column_usage(
    sql: str,
    table_map: Dict[str, str] | None = None,
    schema_state: Dict[str, Any] | None = None,
) -> Dict[str, set[str]]:
    usage: Dict[str, set[str]] = {}
    alias_map = _extract_aliases(sql, table_map)
    pattern = re.compile(r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?")
    for raw_table, raw_column in pattern.findall(sql):
        table = alias_map.get(raw_table.lower(), raw_table)
        if table_map:
            table = table_map.get(str(table).lower(), table)
        usage.setdefault(table, set()).add(raw_column)

    if not schema_state:
        return usage

    tables = _extract_tables(sql, table_map)
    if not tables:
        return usage

    cleaned = re.sub(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"", " ", sql)
    cleaned = cleaned.replace("`", " ")

    tokens = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", cleaned)
    if not tokens:
        return usage

    skip = {
        "select", "from", "where", "join", "left", "right", "inner", "outer", "full", "cross",
        "on", "and", "or", "not", "in", "exists", "as", "distinct", "group", "by", "order",
        "having", "limit", "union", "all", "case", "when", "then", "else", "end", "is", "null",
        "like", "between", "into", "values", "update", "delete", "insert", "create", "table",
        "true", "false", "asc", "desc", "over", "partition", "rows", "range", "window",
        "sum", "count", "avg", "min", "max", "extract", "date", "year", "month", "day",
        "interval", "cast", "convert", "substring", "substr", "coalesce", "if", "ifnull",
    }
    skip.update(alias_map.keys())
    for table in tables:
        skip.add(str(table).lower())

    for token in tokens:
        lowered = token.lower()
        if lowered in skip:
            continue
        table, resolved_col = _resolve_unqualified_column(schema_state, tables, token)
        if table and resolved_col:
            usage.setdefault(table, set()).add(resolved_col)

    return usage


def _collect_column_cooccurrence(workload_sql: Dict[str, str], schema_state: Dict[str, Any]) -> list[Dict[str, Any]]:
    counts: Dict[tuple[str, tuple[str, ...]], int] = {}
    table_map = _table_name_map(schema_state)
    for sql in workload_sql.values():
        usage = _extract_table_column_usage(sql, table_map, schema_state)
        for table, columns in usage.items():
            resolved_cols: list[str] = []
            for column in columns:
                resolved = _resolve_column_name(schema_state, table, column)
                if resolved:
                    resolved_cols.append(resolved)
            sorted_cols = sorted(set(resolved_cols))
            if len(sorted_cols) < 2:
                continue
            key = (table, tuple(sorted_cols))
            counts[key] = counts.get(key, 0) + 1

    results = []
    for (table, columns), count in sorted(counts.items()):
        results.append({"table": table, "columns": list(columns), "count": count})
    return results


def _collect_join_key_history(workload_sql: Dict[str, str], schema_state: Dict[str, Any]) -> list[Dict[str, str]]:
    history: Set[tuple[str, str, str, str]] = set()
    pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.(`?[A-Za-z_][A-Za-z0-9_]*`?)\s*=\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\.(`?[A-Za-z_][A-Za-z0-9_]*`?)",
        re.IGNORECASE,
    )
    table_map = _table_name_map(schema_state)
    for sql in workload_sql.values():
        alias_map = _extract_aliases(sql, table_map)
        for left_table, left_col, right_table, right_col in pattern.findall(sql):
            left_table_name = _resolve_table_name(left_table, alias_map, table_map)
            right_table_name = _resolve_table_name(right_table, alias_map, table_map)
            if left_table_name == right_table_name:
                continue
            history.add(
                (
                    left_table_name,
                    left_col.strip("`"),
                    right_table_name,
                    right_col.strip("`"),
                )
            )
    results = []
    for left_table, left_col, right_table, right_col in sorted(history):
        results.append(
            {
                "left_table": left_table,
                "left_column": left_col,
                "right_table": right_table,
                "right_column": right_col,
            }
        )
    return results


def _extract_join_pairs(
    sql: str,
    table_map: Dict[str, str],
    schema_state: Dict[str, Any],
) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    tables = schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}
    if not isinstance(tables, dict):
        return pairs
    alias_map = _extract_aliases_all(sql, table_map)
    pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?\s*=\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?",
        re.IGNORECASE,
    )
    for left_table, _left_col, right_table, _right_col in pattern.findall(sql):
        left_table_name = _resolve_table_name(left_table, alias_map, table_map)
        right_table_name = _resolve_table_name(right_table, alias_map, table_map)
        if left_table_name == right_table_name:
            continue
        if left_table_name not in tables or right_table_name not in tables:
            continue
        pair = tuple(sorted((left_table_name, right_table_name)))
        pairs.add(pair)
    return pairs


def _table_columns(schema_state: Dict[str, Any]) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {}
    tables = schema_state.get("tables", {})
    if not isinstance(tables, dict):
        return result
    for table, info in tables.items():
        if not isinstance(info, dict):
            continue
        columns = info.get("columns", {})
        if isinstance(columns, dict):
            result[str(table)] = {str(col) for col in columns.keys()}
    return result


def _extract_simple_predicates(sql: str, table_map: Dict[str, str] | None = None) -> list[Tuple[str, str, str]]:
    predicates: list[Tuple[str, str, str]] = []
    alias_map = _extract_aliases(sql, table_map)
    pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|([0-9]+(?:\.[0-9]+)?))",
        re.IGNORECASE,
    )
    for raw_table, raw_column, str_val, str_val2, num_val in pattern.findall(sql):
        table = alias_map.get(raw_table.lower(), raw_table)
        if table_map:
            table = table_map.get(str(table).lower(), table)
        column = raw_column
        value = str_val or str_val2 or num_val
        if value is None:
            continue
        predicates.append((table, column, value))
    return predicates


def _extract_table_predicates(sql: str, table_map: Dict[str, str] | None = None) -> Dict[str, list[str]]:
    alias_map = _extract_aliases_all(sql, table_map)
    pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?\s*(=|>=|<=|>|<)\s*(?:'([^']*)'|\"([^\"]*)\"|([0-9]+(?:\.[0-9]+)?))",
        re.IGNORECASE,
    )
    results: Dict[str, list[str]] = {}
    for raw_table, raw_column, op, str_val, str_val2, num_val in pattern.findall(sql):
        table = alias_map.get(raw_table.lower(), raw_table)
        if table_map:
            table = table_map.get(str(table).lower(), table)
        value = str_val or str_val2 or num_val
        if value is None:
            continue
        if num_val:
            value_text = value
        else:
            value_text = f"'{value}'"
        results.setdefault(str(table), []).append(f"{raw_column} {op} {value_text}")
    return results


def _primary_key_map(schema_state: Dict[str, Any]) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {}
    tables = schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}
    if not isinstance(tables, dict):
        return result
    for table_name, table_info in tables.items():
        if not isinstance(table_info, dict):
            continue
        pk_cols = table_info.get("primary_key", []) or []
        if isinstance(pk_cols, list):
            result[str(table_name).lower()] = {str(col).lower() for col in pk_cols}
    return result


def _foreign_key_pairs(schema_state: Dict[str, Any]) -> Set[Tuple[str, str, str, str]]:
    pairs: Set[Tuple[str, str, str, str]] = set()
    tables = schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}
    if not isinstance(tables, dict):
        return pairs
    for table_name, table_info in tables.items():
        if not isinstance(table_info, dict):
            continue
        for fk in table_info.get("foreign_keys", []) or []:
            if not isinstance(fk, dict):
                continue
            src = str(fk.get("from", ""))
            dst = str(fk.get("to", ""))
            if "." in src:
                src_table, src_col = src.split(".", 1)
            else:
                src_table, src_col = str(table_name), src
            if "." in dst:
                dst_table, dst_col = dst.split(".", 1)
            else:
                dst_table, dst_col = dst, ""
            if src_table and src_col and dst_table and dst_col:
                pairs.add((src_table.lower(), src_col.lower(), dst_table.lower(), dst_col.lower()))
    return pairs


def _strip_join_predicates(
    where_text: str,
    alias_map: Dict[str, str],
    table_map: Dict[str, str],
    schema_state: Dict[str, Any],
) -> str:
    if not where_text:
        return ""
    pk_map = _primary_key_map(schema_state)
    fk_pairs = _foreign_key_pairs(schema_state)
    join_pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?\s*=\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?",
        re.IGNORECASE,
    )

    def _normalize_column(table: str, column: str) -> str:
        resolved = _resolve_column_name(schema_state, table, column.strip("`"))
        return str(resolved or column).lower()

    def _is_join_pair(left_table: str, left_col: str, right_table: str, right_col: str) -> bool:
        left_name = _resolve_table_name(left_table, alias_map, table_map)
        right_name = _resolve_table_name(right_table, alias_map, table_map)
        if not left_name or not right_name or left_name == right_name:
            return False
        left_key = left_name.lower()
        right_key = right_name.lower()
        left_col_key = _normalize_column(left_name, left_col)
        right_col_key = _normalize_column(right_name, right_col)
        left_pk = left_col_key in pk_map.get(left_key, set())
        right_pk = right_col_key in pk_map.get(right_key, set())
        if left_pk and right_pk:
            return True
        if (left_key, left_col_key, right_key, right_col_key) in fk_pairs:
            return True
        if (right_key, right_col_key, left_key, left_col_key) in fk_pairs:
            return True
        return False

    def _replace(match: re.Match[str]) -> str:
        left_table, left_col, right_table, right_col = match.groups()
        if _is_join_pair(left_table, left_col, right_table, right_col):
            return " "
        return match.group(0)

    cleaned = join_pattern.sub(_replace, where_text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"^(and|or)\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+(and|or)$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(and|or)\b\s+\b(and|or)\b", r"\2", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def _to_numeric(value: str) -> Any:
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    return value


def _table_row_count(conn: pymysql.connections.Connection, table: str) -> int:
    rows = _fetchall(conn, f"SELECT COUNT(*) AS cnt FROM `{table}`", ())
    if not rows:
        return 0
    return int(rows[0].get("cnt") or 0)


def _count_distinct(conn: pymysql.connections.Connection, table: str, column: str) -> int:
    rows = _fetchall(conn, f"SELECT COUNT(DISTINCT `{column}`) AS cnt FROM `{table}`", ())
    if not rows:
        return 0
    return int(rows[0].get("cnt") or 0)


def _collect_selectivity_samples(
    conn: pymysql.connections.Connection,
    workload_sql: Dict[str, str],
    schema_state: Dict[str, Any],
    storage_stats: Dict[str, Any],
    join_key_history: list[Dict[str, str]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    metadata_cfg = config.get("metadata", {})
    max_predicates = int(metadata_cfg.get("selectivity_max_predicates", 100))
    max_per_sql = int(metadata_cfg.get("selectivity_max_per_sql", 5))
    table_cols = _table_columns(schema_state)
    table_map = _table_name_map(schema_state)
    row_counts = storage_stats.get("row_counts", {}) if isinstance(storage_stats.get("row_counts"), dict) else {}

    predicate_samples: list[Dict[str, Any]] = []
    seen_predicates: Set[Tuple[str, str, str]] = set()
    seen_predicates_all: Set[Tuple[str, str, str]] = set()

    for sql in workload_sql.values():
        if len(predicate_samples) >= max_predicates:
            break
        predicates = _extract_simple_predicates(sql, table_map)
        per_sql_count = 0
        for table, column, value in predicates:
            table = table_map.get(str(table).lower(), table)
            resolved_column = _resolve_column_name(schema_state, table, column)
            if not resolved_column:
                continue
            column = resolved_column
            seen_predicates_all.add((table, column, value))
            if len(predicate_samples) >= max_predicates or per_sql_count >= max_per_sql:
                break
            key = (table, column, value)
            if key in seen_predicates:
                continue
            if table not in table_cols or column not in table_cols.get(table, set()):
                continue
            total_rows = row_counts.get(table)
            if total_rows is None:
                total_rows = _table_row_count(conn, table)
                row_counts[table] = total_rows
            if total_rows <= 0:
                continue
            count_rows = _fetchall(
                conn,
                f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE `{column}` = %s",
                (_to_numeric(value),),
            )
            if not count_rows:
                continue
            matched = int(count_rows[0].get("cnt") or 0)
            predicate_samples.append(
                {
                    "table": table,
                    "column": column,
                    "value": value,
                    "selectivity": float(matched) / float(total_rows),
                    "row_count": int(total_rows),
                }
            )
            seen_predicates.add(key)
            per_sql_count += 1

    join_samples: list[Dict[str, Any]] = []
    seen_joins: Set[Tuple[str, str, str, str]] = set()
    for item in join_key_history:
        left_table = item.get("left_table")
        left_column = item.get("left_column")
        right_table = item.get("right_table")
        right_column = item.get("right_column")
        if not left_table or not left_column or not right_table or not right_column:
            continue
        key = (left_table, left_column, right_table, right_column)
        if key in seen_joins:
            continue
        if left_table not in table_cols or right_table not in table_cols:
            continue
        if left_column not in table_cols[left_table] or right_column not in table_cols[right_table]:
            continue
        left_distinct = _count_distinct(conn, left_table, left_column)
        right_distinct = _count_distinct(conn, right_table, right_column)
        if left_distinct <= 0 or right_distinct <= 0:
            continue
        max_distinct = max(left_distinct, right_distinct)
        selectivity = 1.0 / float(max_distinct)
        join_samples.append(
            {
                "left_table": left_table,
                "left_column": left_column,
                "right_table": right_table,
                "right_column": right_column,
                "selectivity": selectivity,
                "left_distinct": int(left_distinct),
                "right_distinct": int(right_distinct),
            }
        )
        seen_joins.add(key)

    coverage = {
        "predicates_seen": len(seen_predicates_all),
        "predicates_sampled": len(predicate_samples),
        "join_keys_seen": len(join_key_history),
        "join_keys_sampled": len(join_samples),
    }
    return {"predicate": predicate_samples, "join": join_samples, "coverage": coverage}


def _resolve_output_path(dataset_root: Path, raw_path: Any, default_name: str) -> Path:
    value = str(raw_path or "").strip()
    if not value:
        return dataset_root / default_name
    path = Path(value)
    if not path.is_absolute():
        return dataset_root / path
    return path


def _connect_mysql(mysql_cfg: Dict[str, Any]) -> pymysql.connections.Connection:
    required_keys = ("host", "port", "user", "database")
    missing = [key for key in required_keys if mysql_cfg.get(key) in (None, "")]
    if missing:
        raise ValueError(f"mysql config missing: {', '.join(missing)}")
    return pymysql.connect(
        host=str(mysql_cfg["host"]),
        port=int(mysql_cfg["port"]),
        user=str(mysql_cfg["user"]),
        password=str(mysql_cfg["password"]),
        database=str(mysql_cfg["database"]),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def _fetchall(conn: pymysql.connections.Connection, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            normalized.append({str(key).lower(): value for key, value in row.items()})
        else:
            normalized.append(dict(row))
    return normalized


def _collect_schema_from_information_schema(conn: pymysql.connections.Connection, database: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    tables = _fetchall(
        conn,
        """
        SELECT table_name, table_rows, data_length, index_length
        FROM information_schema.tables
        WHERE table_schema = %s
        """,
        (database,),
    )
    columns = _fetchall(
        conn,
        """
        SELECT table_name, column_name, data_type, column_type, is_nullable,
               column_default, extra, character_maximum_length,
               numeric_precision, numeric_scale, character_octet_length
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
        """,
        (database,),
    )
    primary_keys = _fetchall(
        conn,
        """
        SELECT table_name, column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s AND constraint_name = 'PRIMARY'
        ORDER BY table_name, ordinal_position
        """,
        (database,),
    )
    foreign_keys = _fetchall(
        conn,
        """
        SELECT table_name, column_name, referenced_table_name, referenced_column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s AND referenced_table_name IS NOT NULL
        """,
        (database,),
    )
    unique_constraints = _fetchall(
        conn,
        """
        SELECT tc.table_name, tc.constraint_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_schema = kcu.constraint_schema
           AND tc.table_name = kcu.table_name
           AND tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_schema = %s AND tc.constraint_type = 'UNIQUE'
        ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
        """,
        (database,),
    )
    check_constraints = _fetchall(
        conn,
        """
        SELECT tc.table_name, cc.constraint_name, cc.check_clause
        FROM information_schema.table_constraints tc
        JOIN information_schema.check_constraints cc
            ON tc.constraint_schema = cc.constraint_schema
           AND tc.constraint_name = cc.constraint_name
        WHERE tc.constraint_schema = %s AND tc.constraint_type = 'CHECK'
        ORDER BY tc.table_name, cc.constraint_name
        """,
        (database,),
    )

    schema_state: Dict[str, Any] = {"tables": {}, "relations": []}
    storage_stats: Dict[str, Any] = {"table_sizes": {}, "column_sizes": {}, "row_counts": {}}

    for row in tables:
        table = str(row["table_name"])
        data_length = int(row.get("data_length") or 0)
        index_length = int(row.get("index_length") or 0)
        size_bytes = data_length + index_length
        storage_stats["table_sizes"][table] = size_bytes
        storage_stats["row_counts"][table] = int(row.get("table_rows") or 0)
        schema_state["tables"].setdefault(
            table,
            {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "unique_constraints": [],
                "check_constraints": [],
                "lineage": {"origin": table, "derived_from": []},
            },
        )

    for row in columns:
        table = str(row["table_name"])
        column = str(row["column_name"])
        column_type = str(row.get("column_type") or row.get("data_type") or "")
        length = row.get("character_octet_length")
        if length is None:
            length = row.get("numeric_precision")
        if length is None:
            length = row.get("character_maximum_length")
        length_value = int(length) if isinstance(length, (int, float)) else None
        nullable = str(row.get("is_nullable") or "YES").upper() == "YES"

        schema_state["tables"].setdefault(
            table,
            {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "unique_constraints": [],
                "check_constraints": [],
                "lineage": {"origin": table, "derived_from": []},
            },
        )
        extra = str(row.get("extra") or "")
        schema_state["tables"][table]["columns"][column] = {
            "type": column_type,
            "length": length_value,
            "nullable": nullable,
            "default": row.get("column_default"),
            "auto_increment": "auto_increment" in extra.lower(),
            "extra": extra,
        }
        storage_stats.setdefault("column_sizes", {}).setdefault(table, {})[column] = int(length_value or 0)

    for row in primary_keys:
        table = str(row["table_name"])
        column = str(row["column_name"])
        schema_state["tables"].setdefault(
            table,
            {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "unique_constraints": [],
                "check_constraints": [],
                "lineage": {"origin": table, "derived_from": []},
            },
        )
        schema_state["tables"][table]["primary_key"].append(column)

    for row in foreign_keys:
        table = str(row["table_name"])
        column = str(row["column_name"])
        ref_table = str(row.get("referenced_table_name") or "")
        ref_column = str(row.get("referenced_column_name") or "")
        if not ref_table or not ref_column:
            continue
        schema_state["tables"].setdefault(
            table,
            {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "unique_constraints": [],
                "check_constraints": [],
                "lineage": {"origin": table, "derived_from": []},
            },
        )
        fk_info = {"from": f"{table}.{column}", "to": f"{ref_table}.{ref_column}"}
        schema_state["tables"][table]["foreign_keys"].append(fk_info)
        schema_state["relations"].append(fk_info)

    unique_map: Dict[tuple[str, str], list[str]] = {}
    for row in unique_constraints:
        table = str(row.get("table_name") or "")
        name = str(row.get("constraint_name") or "")
        column = str(row.get("column_name") or "")
        if not table or not name or not column:
            continue
        unique_map.setdefault((table, name), []).append(column)

    for (table, name), cols in unique_map.items():
        schema_state["tables"].setdefault(
            table,
            {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "unique_constraints": [],
                "check_constraints": [],
                "lineage": {"origin": table, "derived_from": []},
            },
        )
        schema_state["tables"][table]["unique_constraints"].append({"name": name, "columns": cols})

    for row in check_constraints:
        table = str(row.get("table_name") or "")
        name = str(row.get("constraint_name") or "")
        clause = str(row.get("check_clause") or "")
        if not table or not name:
            continue
        schema_state["tables"].setdefault(
            table,
            {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "unique_constraints": [],
                "check_constraints": [],
                "lineage": {"origin": table, "derived_from": []},
            },
        )
        schema_state["tables"][table]["check_constraints"].append({"name": name, "clause": clause})

    storage_stats["total_size_before"] = int(sum(int(v) for v in storage_stats["table_sizes"].values()))
    return schema_state, storage_stats


def _extract_parenthesized(text: str) -> str:
    start = text.find("(")
    if start == -1:
        return ""
    depth = 0
    for idx in range(start, len(text)):
        if text[idx] == "(":
            depth += 1
        elif text[idx] == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1 : idx]
    return ""


def _split_columns(definition: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    for idx, ch in enumerate(definition):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(definition[start:idx])
            start = idx + 1
    last = definition[start:].strip()
    if last:
        parts.append(last)
    return parts


def _collect_schema_from_schema_sql(schema_path: Path) -> Dict[str, Any]:
    if not schema_path.exists():
        raise FileNotFoundError(f"schema.sql not found: {schema_path}")

    schema_state: Dict[str, Any] = {"tables": {}, "relations": []}
    for statement in sqlparse.split(schema_path.read_text(encoding="utf-8")):
        stmt = statement.strip()
        if not stmt:
            continue
        if not re.match(r"^CREATE\s+TABLE", stmt, re.IGNORECASE):
            continue
        name_match = re.search(r"CREATE\s+TABLE\s+`?([A-Za-z0-9_]+)`?", stmt, re.IGNORECASE)
        if not name_match:
            continue
        table = name_match.group(1)
        schema_state["tables"].setdefault(
            table,
            {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "unique_constraints": [],
                "check_constraints": [],
                "lineage": {"origin": table, "derived_from": []},
            },
        )
        body = _extract_parenthesized(stmt)
        for item in _split_columns(body):
            line = item.strip()
            if not line:
                continue
            if re.match(r"^(PRIMARY|FOREIGN|UNIQUE|KEY|CONSTRAINT)\b", line, re.IGNORECASE):
                pk_match = re.search(r"PRIMARY\s+KEY\s*\(([^\)]+)\)", line, re.IGNORECASE)
                if pk_match:
                    cols = [c.strip(" `") for c in pk_match.group(1).split(",")]
                    schema_state["tables"][table]["primary_key"].extend([c for c in cols if c])
                unique_match = re.search(r"UNIQUE\s+(?:KEY|INDEX)?\s*`?([A-Za-z0-9_]+)?`?\s*\(([^\)]+)\)", line, re.IGNORECASE)
                if unique_match:
                    name = unique_match.group(1) or "unique"
                    cols = [c.strip(" `") for c in unique_match.group(2).split(",")]
                    schema_state["tables"][table]["unique_constraints"].append({"name": name, "columns": [c for c in cols if c]})
                check_match = re.search(r"CHECK\s*\((.*)\)", line, re.IGNORECASE)
                if check_match:
                    clause = check_match.group(1).strip()
                    schema_state["tables"][table]["check_constraints"].append({"name": "check", "clause": clause})
                fk_match = re.search(
                    r"FOREIGN\s+KEY\s*\(([^\)]+)\)\s+REFERENCES\s+`?([A-Za-z0-9_]+)`?\s*\(([^\)]+)\)",
                    line,
                    re.IGNORECASE,
                )
                if fk_match:
                    cols = [c.strip(" `") for c in fk_match.group(1).split(",")]
                    ref_table = fk_match.group(2)
                    ref_cols = [c.strip(" `") for c in fk_match.group(3).split(",")]
                    for col, ref_col in zip(cols, ref_cols):
                        fk_info = {"from": f"{table}.{col}", "to": f"{ref_table}.{ref_col}"}
                        schema_state["tables"][table]["foreign_keys"].append(fk_info)
                        schema_state["relations"].append(fk_info)
                continue

            col_match = re.match(r"`?([A-Za-z0-9_]+)`?\s+([^\s,]+)", line)
            if not col_match:
                continue
            column = col_match.group(1)
            col_type = col_match.group(2)
            nullable = not bool(re.search(r"NOT\s+NULL", line, re.IGNORECASE))
            default_match = re.search(r"DEFAULT\s+([^\s,]+)", line, re.IGNORECASE)
            default_value = default_match.group(1) if default_match else None
            auto_increment = bool(re.search(r"AUTO_INCREMENT", line, re.IGNORECASE))
            length_match = re.search(r"\((\d+)", col_type)
            length_value = int(length_match.group(1)) if length_match else None
            schema_state["tables"][table]["columns"][column] = {
                "type": col_type,
                "length": length_value,
                "nullable": nullable,
                "default": default_value,
                "auto_increment": auto_increment,
            }
    return schema_state


def _run_explain_analyze(conn: pymysql.connections.Connection, sql: str) -> str:
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

    target_idx = None
    for idx, stmt in enumerate(statements):
        head = _strip_leading_comments(stmt)
        if re.match(r"(?is)^\s*(select|with)\b", head):
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


def _collect_explain(
    conn: pymysql.connections.Connection,
    workload_sql: Dict[str, str],
    explain_dir: Path,
    overwrite: bool,
) -> None:
    explain_dir.mkdir(parents=True, exist_ok=True)
    if not workload_sql:
        raise ValueError("no workload SQL found to explain")
    for sql_id, sql in workload_sql.items():
        if not sql.strip():
            raise ValueError(f"SQL is empty for sql_id={sql_id}")
        output_path = explain_dir / f"{sql_id}.txt"
        if output_path.exists() and not overwrite:
            continue
        text = _run_explain_analyze(conn, sql)
        output_path.write_text(text, encoding="utf-8")


def collect_metadata(config: Dict[str, Any]) -> Dict[str, Any]:
    """Collect metadata from dataset files for the offline MVP pipeline."""
    metadata_cfg = config.get("metadata", {})
    explain_only = bool(metadata_cfg.get("explain_only", False))
    collect_explain = bool(metadata_cfg.get("collect_explain", False))
    collect_selectivity = bool(metadata_cfg.get("collect_selectivity", False))
    overwrite_explain = bool(metadata_cfg.get("overwrite_explain", True))
    schema_source = metadata_cfg.get("schema_source", "information_schema")
    schema_sql_path = metadata_cfg.get("schema_sql_path")
    schema_constraints_path = metadata_cfg.get("schema_constraints_path")
    require_metrics = bool(metadata_cfg.get("require_metrics", not explain_only))

    workload_cfg = config.get("workload", {})
    dataset_root = Path(workload_cfg.get("dataset_root", "")).expanduser()
    if not dataset_root.exists():
        raise FileNotFoundError(f"dataset_root not found: {dataset_root}")
    sql_dir = dataset_root / workload_cfg.get("sql_dir", "workload/sql")
    metrics_csv = dataset_root / workload_cfg.get("metrics_csv", "workload/metrics.csv")
    explain_dir = dataset_root / workload_cfg.get("explain_dir", "workload/explain")

    workload_sql = _load_workload(sql_dir)
    metrics = _load_metrics(metrics_csv, required=require_metrics)
    schema_state: Dict[str, Any] = {}
    storage_stats: Dict[str, Any] = {}

    mysql_cfg = config.get("mysql", {})
    conn = None
    selectivity_samples: Dict[str, Any] = {}
    join_key_history: list[Dict[str, str]] = []
    try:
        if collect_explain or collect_selectivity or schema_source == "information_schema":
            conn = _connect_mysql(mysql_cfg)

        if schema_source == "information_schema":
            if conn is None:
                raise ValueError("mysql connection is required for information_schema collection")
            schema_state, storage_stats = _collect_schema_from_information_schema(conn, str(mysql_cfg.get("database")))
        elif schema_source == "schema_sql":
            schema_path = Path(schema_sql_path) if schema_sql_path else (dataset_root / "schema.sql")
            schema_state = _collect_schema_from_schema_sql(schema_path)
            storage_stats = _read_json_if_exists(dataset_root / "storage_stats.json")
            if not storage_stats:
                raise FileNotFoundError("storage_stats.json is required when schema_source=schema_sql")
        elif schema_source == "schema_json":
            schema_state = _read_json_if_exists(dataset_root / "schema_state.json")
            if not schema_state:
                raise FileNotFoundError("schema_state.json not found or empty")
            storage_stats = _read_json_if_exists(dataset_root / "storage_stats.json")
            if not storage_stats:
                raise FileNotFoundError("storage_stats.json not found or empty")
        else:
            raise ValueError(f"unsupported schema_source: {schema_source}")

        tables = schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}
        if not isinstance(tables, dict) or not tables:
            raise ValueError("schema_state is empty; check schema_source/database configuration")

        if schema_constraints_path:
            path_value = Path(str(schema_constraints_path))
            if not path_value.is_absolute():
                path_value = dataset_root / path_value
            constraints = _read_json_if_exists(path_value)
            _merge_schema_constraints(schema_state, constraints)

        table_sizes = storage_stats.get("table_sizes")
        if not isinstance(table_sizes, dict) or not table_sizes:
            table_sizes = _table_sizes_from_schema(schema_state)
            storage_stats["table_sizes"] = table_sizes
        if "total_size_before" not in storage_stats:
            storage_stats["total_size_before"] = int(sum(int(v) for v in table_sizes.values()))

        join_key_history = _collect_join_key_history(workload_sql, schema_state)

        if collect_explain:
            if conn is None:
                raise ValueError("mysql connection is required for explain collection")
            _collect_explain(conn, workload_sql, explain_dir, overwrite_explain)

        if collect_selectivity:
            if conn is None:
                raise ValueError("mysql connection is required for selectivity sampling")
            selectivity_samples = _collect_selectivity_samples(
                conn,
                workload_sql,
                schema_state,
                storage_stats,
                join_key_history,
                config,
            )
    finally:
        if conn is not None:
            conn.close()

    plans = _load_plans(explain_dir)

    prompt_cfg = config.get("prompt", {})
    template_path = prompt_cfg.get("template_path")
    template_text = ""
    if template_path:
        template_text = read_text(template_path)
    if not template_text:
        template_text = _default_prompt_template()

    schema_state["join_key_history"] = join_key_history

    schema_summary = json.dumps(schema_state, ensure_ascii=True)
    cooccurrence = _collect_column_cooccurrence(workload_sql, schema_state)
    workload_summary_text = _format_workload_summary_text(metrics, workload_sql, plans, schema_state, storage_stats)
    try:
        operator_latency_profile = json.loads(workload_summary_text)
    except json.JSONDecodeError:
        operator_latency_profile = {}
    experience_hints = _load_experience_hints_from_reference()

    workload_summary = json.dumps(
        {
            "sql_count": len(workload_sql),
            "metrics": metrics,
            "column_cooccurrence": cooccurrence,
            "join_key_history": join_key_history,
            "selectivity_samples": selectivity_samples,
            "operator_latency_profile": operator_latency_profile,
        },
        ensure_ascii=True,
    )
    schema_summary_text = _format_schema_summary_text(schema_state, storage_stats)
    column_cooccurrence_text = _format_column_cooccurrence_text(cooccurrence)

    logger.info(
        "metadata: tables=%d schema_summary_len=%d workload_sql=%d metrics=%d plans=%d",
        len(schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}),
        len(schema_summary_text),
        len(workload_sql),
        len(metrics),
        len(plans),
    )

    if collect_selectivity:
        output_path = _resolve_output_path(
            dataset_root,
            metadata_cfg.get("selectivity_output_path"),
            "selectivity_samples.json",
        )
        output_path.write_text(json.dumps(selectivity_samples, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        coverage_path = _resolve_output_path(
            dataset_root,
            metadata_cfg.get("selectivity_coverage_path"),
            "selectivity_coverage.json",
        )
        coverage = selectivity_samples.get("coverage", {})
        coverage_path.write_text(json.dumps(coverage, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    return {
        "schema_state": schema_state,
        "storage_stats": storage_stats,
        "workload_sql": workload_sql,
        "metrics": metrics,
        "plans": plans,
        "column_cooccurrence": cooccurrence,
        "join_key_history": join_key_history,
        "selectivity_samples": selectivity_samples,
        "prompt_template": template_text,
        "prompt_context": {
            "schema_summary": schema_summary,
            "workload_summary": workload_summary,
            "schema_summary_text": schema_summary_text,
            "workload_summary_text": workload_summary_text,
            "column_cooccurrence_text": column_cooccurrence_text,
            "experience_hints": experience_hints,
        },
    }
