from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple
from datetime import date, timedelta
import json
import logging
import math
from pathlib import Path
import re


DEFAULT_ROW_COUNT = 1.0
DEFAULT_COLUMN_SIZE = 1.0
DEFAULT_SELECTIVITY = 0.5
DEFAULT_JOIN_SELECTIVITY = 1.0
DEFAULT_ACCESS_FACTORS = {
    "table_scan": 1.0,
    "index_scan": 0.7,
    "index_lookup": 0.5,
    "range_scan": 0.8,
}
DEFAULT_OPERATOR_FACTORS = {
    "nested_loop": 1.2,
    "hash_join": 1.0,
    "merge_join": 1.1,
    "sort": 1.0,
    "group": 1.0,
    "unknown_join": 1.0,
}

logger = logging.getLogger(__name__)


def _to_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _metric_total(record: Dict[str, Any]) -> float:
    total = record.get("total_latency_ms")
    if total not in (None, ""):
        return _to_float(total)
    return _to_float(record.get("avg_latency_ms")) * _to_float(record.get("freq"), 1.0)


def _normalize_metrics(metrics: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    result: Dict[str, Dict[str, float]] = {}
    if not isinstance(metrics, dict):
        return result

    for sql_id, value in metrics.items():
        if not isinstance(value, dict):
            continue
        result[str(sql_id)] = {
            "freq": _to_float(value.get("freq")),
            "avg_latency_ms": _to_float(value.get("avg_latency_ms")),
            "total_latency_ms": _metric_total(value),
        }
    return result


def _extract_aliases(sql: str) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    cte_map = _extract_cte_map(sql)
    alias_map.update(cte_map)
    alias_map.update(_extract_subquery_aliases(sql, cte_map))
    alias_map.update(_extract_from_clause_aliases(sql, cte_map))
    alias_map.update(_extract_nested_aliases(sql, cte_map))
    pattern = re.compile(
        r"\b(from|join)\s+(?!\()\s*`?([A-Za-z0-9_]+)`?(?:\s+as)?\s+`?([A-Za-z0-9_]+)`?",
        re.IGNORECASE,
    )
    for match in pattern.finditer(sql):
        table = match.group(2)
        alias = match.group(3)
        if alias.lower() in {"on", "where", "join", "left", "right", "inner", "outer", "group", "order", "limit", "having"}:
            continue
        alias_map[alias] = cte_map.get(table, table)
    return alias_map


def _extract_from_clause_aliases(sql: str, cte_map: Dict[str, str]) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    for item in _split_from_clause_items(sql):
        if re.search(r"\bjoin\b", item, re.IGNORECASE):
            continue
        item = item.strip()
        if not item:
            continue
        if item.startswith("("):
            end = _find_matching_paren(item, 0)
            if end == -1:
                continue
            body = item[1:end]
            idx = end + 1
            idx = _skip_ws(item, idx)
            if item[idx: idx + 2].lower() == "as":
                idx += 2
                idx = _skip_ws(item, idx)
            alias, _ = _read_identifier(item, idx)
            if alias:
                base = _extract_single_base_table(body, cte_map)
                if base:
                    aliases[alias] = base
            continue
        table, idx = _read_identifier(item, 0)
        if not table:
            continue
        idx = _skip_ws(item, idx)
        if item[idx: idx + 2].lower() == "as":
            idx += 2
            idx = _skip_ws(item, idx)
        alias, _ = _read_identifier(item, idx)
        if alias and not _is_keyword(alias):
            aliases[alias] = cte_map.get(table, table)
    return aliases


def _split_from_clause_items(sql: str) -> List[str]:
    start = _find_from_start(sql)
    if start == -1:
        return []
    items: List[str] = []
    buffer: List[str] = []
    idx = start
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False
    while idx < len(sql):
        char = sql[idx]
        if char == "'" and not in_double and not in_backtick:
            in_single = not in_single
        elif char == '"' and not in_single and not in_backtick:
            in_double = not in_double
        elif char == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
        elif not in_single and not in_double and not in_backtick:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            if depth == 0 and char == ",":
                item = "".join(buffer).strip()
                if item:
                    items.append(item)
                buffer = []
                idx += 1
                continue
            if depth == 0 and _matches_clause_keyword(sql, idx):
                break
        buffer.append(char)
        idx += 1
    tail = "".join(buffer).strip()
    if tail:
        items.append(tail)
    return items


def _find_from_start(sql: str) -> int:
    idx = 0
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False
    while idx < len(sql):
        char = sql[idx]
        if char == "'" and not in_double and not in_backtick:
            in_single = not in_single
        elif char == '"' and not in_single and not in_backtick:
            in_double = not in_double
        elif char == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
        elif not in_single and not in_double and not in_backtick:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            if depth == 0 and _matches_keyword(sql, idx, "from"):
                return idx + 4
        idx += 1
    return -1


def _matches_clause_keyword(sql: str, idx: int) -> bool:
    for keyword in ("where", "group", "order", "having", "limit", "union", "intersect", "except"):
        if _matches_keyword(sql, idx, keyword):
            return True
    return False


def _matches_keyword(sql: str, idx: int, keyword: str) -> bool:
    end = idx + len(keyword)
    if sql[idx:end].lower() != keyword:
        return False
    if idx > 0 and _is_ident_char(sql[idx - 1]):
        return False
    if end < len(sql) and _is_ident_char(sql[end]):
        return False
    return True


def _is_ident_char(char: str) -> bool:
    return char.isalnum() or char == "_"


def _is_keyword(word: str) -> bool:
    return word.lower() in {
        "on",
        "where",
        "join",
        "left",
        "right",
        "inner",
        "outer",
        "full",
        "cross",
        "group",
        "order",
        "limit",
        "having",
        "union",
        "intersect",
        "except",
    }


def _extract_cte_map(sql: str) -> Dict[str, str]:
    match = re.search(r"\bwith\b", sql, re.IGNORECASE)
    if not match:
        return {}
    cte_map: Dict[str, str] = {}
    idx = match.end()
    remainder = sql[idx:].lstrip()
    if remainder.lower().startswith("recursive"):
        idx += len(sql[idx:]) - len(remainder) + len("recursive")

    while idx < len(sql):
        idx = _skip_ws(sql, idx)
        name, idx = _read_identifier(sql, idx)
        if not name:
            break
        idx = _skip_ws(sql, idx)
        if idx < len(sql) and sql[idx] == "(":
            col_end = _find_matching_paren(sql, idx)
            if col_end == -1:
                break
            idx = col_end + 1
        idx = _skip_ws(sql, idx)
        if not sql[idx: idx + 2].lower() == "as":
            break
        idx += 2
        idx = _skip_ws(sql, idx)
        if idx >= len(sql) or sql[idx] != "(":
            break
        end = _find_matching_paren(sql, idx)
        if end == -1:
            break
        body = sql[idx + 1 : end]
        base = _extract_single_base_table(body, cte_map)
        if base:
            cte_map[name] = base
        idx = end + 1
        idx = _skip_ws(sql, idx)
        if idx < len(sql) and sql[idx] == ",":
            idx += 1
            continue
        break

    return cte_map


def _extract_subquery_aliases(sql: str, cte_map: Dict[str, str]) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    idx = 0
    depth = 0
    while idx < len(sql):
        char = sql[idx]
        if char == "(":
            depth += 1
            idx += 1
            continue
        if char == ")":
            depth = max(depth - 1, 0)
            idx += 1
            continue
        if depth == 0:
            match = re.match(r"\b(from|join)\b", sql[idx:], re.IGNORECASE)
            if match:
                idx += match.end()
                idx = _skip_ws(sql, idx)
                if idx < len(sql) and sql[idx] == "(":
                    end = _find_matching_paren(sql, idx)
                    if end == -1:
                        return aliases
                    body = sql[idx + 1 : end]
                    idx = end + 1
                    idx = _skip_ws(sql, idx)
                    if sql[idx: idx + 2].lower() == "as":
                        idx += 2
                        idx = _skip_ws(sql, idx)
                    alias, idx = _read_identifier(sql, idx)
                    if alias:
                        base = _extract_single_base_table(body, cte_map)
                        if base:
                            aliases[alias] = base
                continue
        idx += 1
    return aliases


def _extract_nested_aliases(sql: str, cte_map: Dict[str, str], depth: int = 0) -> Dict[str, str]:
    if depth > 5:
        return {}
    aliases: Dict[str, str] = {}
    for body in _extract_subquery_bodies(sql):
        aliases.update(_extract_from_clause_aliases(body, cte_map))
        aliases.update(_extract_nested_aliases(body, cte_map, depth + 1))
    return aliases


def _extract_subquery_bodies(sql: str) -> List[str]:
    bodies: List[str] = []
    idx = 0
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False
    while idx < len(sql):
        char = sql[idx]
        if char == "'" and not in_double and not in_backtick:
            in_single = not in_single
        elif char == '"' and not in_single and not in_backtick:
            in_double = not in_double
        elif char == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
        elif not in_single and not in_double and not in_backtick:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            if depth == 0 and (_matches_keyword(sql, idx, "from") or _matches_keyword(sql, idx, "join")):
                idx += 4
                idx = _skip_ws(sql, idx)
                if idx < len(sql) and sql[idx] == "(":
                    end = _find_matching_paren(sql, idx)
                    if end == -1:
                        return bodies
                    bodies.append(sql[idx + 1 : end])
                    idx = end + 1
                    continue
        idx += 1
    return bodies


def _extract_subquery_joins(sql: str) -> List[Tuple[str, str]]:
    joins: List[Tuple[str, str]] = []
    idx = 0
    depth = 0
    while idx < len(sql):
        char = sql[idx]
        if char == "(":
            depth += 1
            idx += 1
            continue
        if char == ")":
            depth = max(depth - 1, 0)
            idx += 1
            continue
        if depth == 0:
            match = re.match(
                r"\b(?:(left|right|inner|outer|full|cross)\s+)?join\b",
                sql[idx:],
                re.IGNORECASE,
            )
            if match:
                join_type = (match.group(1) or "inner").lower()
                idx += match.end()
                idx = _skip_ws(sql, idx)
                if idx < len(sql) and sql[idx] == "(":
                    end = _find_matching_paren(sql, idx)
                    if end == -1:
                        return joins
                    idx = end + 1
                    idx = _skip_ws(sql, idx)
                    if sql[idx: idx + 2].lower() == "as":
                        idx += 2
                        idx = _skip_ws(sql, idx)
                    alias, idx = _read_identifier(sql, idx)
                    if alias:
                        joins.append((join_type, alias))
                continue
        idx += 1
    return joins


def _extract_single_base_table(sql: str, cte_map: Dict[str, str]) -> str:
    tables = _extract_top_level_tables(sql, cte_map)
    unique = list({table for table in tables if table})
    if len(unique) == 1:
        return unique[0]
    return ""


def _extract_top_level_tables(sql: str, cte_map: Dict[str, str]) -> List[str]:
    tables: List[str] = []
    idx = 0
    depth = 0
    while idx < len(sql):
        char = sql[idx]
        if char == "(":
            depth += 1
            idx += 1
            continue
        if char == ")":
            depth = max(depth - 1, 0)
            idx += 1
            continue
        if depth == 0:
            match = re.match(r"\b(from|join)\b", sql[idx:], re.IGNORECASE)
            if match:
                idx += match.end()
                idx = _skip_ws(sql, idx)
                if idx < len(sql) and sql[idx] == "(":
                    end = _find_matching_paren(sql, idx)
                    if end == -1:
                        return tables
                    idx = end + 1
                    continue
                table, idx = _read_identifier(sql, idx)
                if table:
                    tables.append(cte_map.get(table, table))
                continue
        idx += 1
    return tables


def _skip_ws(sql: str, idx: int) -> int:
    while idx < len(sql) and sql[idx].isspace():
        idx += 1
    return idx


def _read_identifier(sql: str, idx: int) -> Tuple[str, int]:
    idx = _skip_ws(sql, idx)
    if idx >= len(sql):
        return "", idx
    if sql[idx] == "`":
        end = sql.find("`", idx + 1)
        if end == -1:
            return "", idx
        return sql[idx + 1 : end], end + 1
    match = re.match(r"[A-Za-z0-9_]+", sql[idx:])
    if not match:
        return "", idx
    return match.group(0), idx + match.end()


def _find_matching_paren(sql: str, start: int) -> int:
    depth = 0
    idx = start
    while idx < len(sql):
        if sql[idx] == "(":
            depth += 1
        elif sql[idx] == ")":
            depth -= 1
            if depth == 0:
                return idx
        idx += 1
    return -1


def _extract_table_column_usage(sql: str) -> Dict[str, Set[str]]:
    usage: Dict[str, Set[str]] = {}
    alias_map = _extract_aliases(sql)
    pattern = re.compile(r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?")
    for raw_table, raw_column in pattern.findall(sql):
        table = alias_map.get(raw_table, raw_table)
        usage.setdefault(table, set()).add(raw_column)
    return usage


def _extract_tables(sql: str) -> Set[str]:
    tables: Set[str] = set()
    pattern = re.compile(r"\b(from|join)\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE)
    for match in pattern.finditer(sql):
        tables.add(match.group(2))
    return tables


def _extract_join_pairs(sql: str) -> List[Tuple[str, str, str, str]]:
    pairs: List[Tuple[str, str, str, str]] = []
    alias_map = _extract_aliases(sql)
    pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.(`?[A-Za-z_][A-Za-z0-9_]*`?)\s*=\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\.(`?[A-Za-z_][A-Za-z0-9_]*`?)",
        re.IGNORECASE,
    )
    for left_table, left_col, right_table, right_col in pattern.findall(sql):
        left_table_name = alias_map.get(left_table, left_table)
        right_table_name = alias_map.get(right_table, right_table)
        pairs.append(
            (
                left_table_name,
                left_col.strip("`"),
                right_table_name,
                right_col.strip("`"),
            )
        )
    return pairs


def _remap_table_name(name: str, alias_map: Dict[str, str]) -> str:
    if not name:
        return name
    return alias_map.get(name, name)


def _extract_tables_from_plan(plan_text: str, alias_map: Dict[str, str]) -> Set[str]:
    tables: Set[str] = set()
    if not plan_text:
        return tables
    pattern = re.compile(
        r"(?:Table scan on|Index lookup on|Index scan on|Range scan on|Index range scan on)\s+`?([A-Za-z0-9_]+)`?",
        re.IGNORECASE,
    )
    for match in pattern.finditer(plan_text):
        tables.add(_remap_table_name(match.group(1), alias_map))
    return tables


def _extract_access_modes(plan_text: str, alias_map: Dict[str, str]) -> Dict[str, str]:
    modes: Dict[str, str] = {}
    if not plan_text:
        return modes
    patterns = [
        (re.compile(r"Table scan on\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "table_scan"),
        (re.compile(r"Index scan on\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "index_scan"),
        (re.compile(r"Index lookup on\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "index_lookup"),
        (re.compile(r"Range scan on\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "range_scan"),
        (re.compile(r"Index range scan on\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "range_scan"),
    ]
    for pattern, mode in patterns:
        for match in pattern.finditer(plan_text):
            table = _remap_table_name(match.group(1), alias_map)
            modes.setdefault(table, mode)
    return modes


def _extract_operator_flags(plan_text: str) -> Dict[str, Any]:
    if not plan_text:
        return {"join_types": [], "has_sort": False, "has_group": False}
    join_types: List[str] = []
    if re.search(r"nested loop", plan_text, re.IGNORECASE):
        join_types.extend(["nested_loop"] * len(re.findall(r"nested loop", plan_text, re.IGNORECASE)))
    if re.search(r"hash join", plan_text, re.IGNORECASE):
        join_types.extend(["hash_join"] * len(re.findall(r"hash join", plan_text, re.IGNORECASE)))
    if re.search(r"merge join", plan_text, re.IGNORECASE):
        join_types.extend(["merge_join"] * len(re.findall(r"merge join", plan_text, re.IGNORECASE)))
    has_sort = bool(re.search(r"\bsort\b|filesort", plan_text, re.IGNORECASE))
    has_group = bool(re.search(r"group aggregate|aggregation|group by", plan_text, re.IGNORECASE))
    if not join_types and re.search(r"\bjoin\b", plan_text, re.IGNORECASE):
        join_types.append("unknown_join")
    return {"join_types": join_types, "has_sort": has_sort, "has_group": has_group}


def _column_width_factor(config: Dict[str, Any]) -> float:
    perf_cfg = config.get("performance", {})
    return _to_float(perf_cfg.get("column_width_factor"), 1.0)


def _row_store_penalty_factor(config: Dict[str, Any]) -> float:
    perf_cfg = config.get("performance", {})
    value = _to_float(perf_cfg.get("row_store_penalty_factor"), 0.02)
    return max(0.0, min(value, 1.0))


def _table_metadata(
    schema_state: Dict[str, Any],
    storage_stats: Dict[str, Any],
    config: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, float]], Dict[str, List[str]]]:
    tables = schema_state.get("tables", {}) if isinstance(schema_state, dict) else {}
    row_counts = storage_stats.get("row_counts", {}) if isinstance(storage_stats, dict) else {}
    column_sizes = storage_stats.get("column_sizes", {}) if isinstance(storage_stats, dict) else {}
    width_factor = _column_width_factor(config)
    scaled_column_sizes: Dict[str, Dict[str, float]] = {}
    if isinstance(column_sizes, dict):
        for table, cols in column_sizes.items():
            if not isinstance(cols, dict):
                continue
            scaled_column_sizes[str(table)] = {
                str(col): _to_float(size, DEFAULT_COLUMN_SIZE) * width_factor
                for col, size in cols.items()
            }
    stats: Dict[str, Any] = {}
    pk_map: Dict[str, List[str]] = {}

    for table_name, info in tables.items():
        if not isinstance(info, dict):
            continue
        columns = info.get("columns", {})
        column_list = [str(col) for col in columns.keys()] if isinstance(columns, dict) else []
        pk_cols = info.get("primary_key", []) if isinstance(info.get("primary_key"), list) else []
        pk_map[str(table_name)] = [str(col) for col in pk_cols]
        size_map = scaled_column_sizes.get(table_name, {}) if scaled_column_sizes else {}
        width = 0.0
        for col in column_list:
            width += _to_float(size_map.get(col), DEFAULT_COLUMN_SIZE)
        if width <= 0:
            width = max(1.0, float(len(column_list) or 1))
        rows = _to_float(row_counts.get(table_name), DEFAULT_ROW_COUNT)
        stats[str(table_name)] = {"rows": rows, "width": width, "columns": set(column_list)}

    return stats, scaled_column_sizes if scaled_column_sizes else {}, pk_map


def _column_size(column_sizes: Dict[str, Dict[str, Any]], table: str, column: str) -> float:
    raw = column_sizes.get(table, {})
    if not isinstance(raw, dict):
        return DEFAULT_COLUMN_SIZE
    return _to_float(raw.get(column), DEFAULT_COLUMN_SIZE)


def _split_ratio(config: Dict[str, Any]) -> float:
    perf_cfg = config.get("performance", {})
    ratio = perf_cfg.get("split_ratio")
    if ratio is None:
        ratio = config.get("storage", {}).get("split_ratio")
    return _to_float(ratio, DEFAULT_SELECTIVITY)


def _default_selectivity(config: Dict[str, Any]) -> float:
    perf_cfg = config.get("performance", {})
    return _to_float(perf_cfg.get("default_selectivity"), DEFAULT_SELECTIVITY)


def _join_selectivity(config: Dict[str, Any]) -> float:
    perf_cfg = config.get("performance", {})
    return _to_float(perf_cfg.get("join_selectivity"), DEFAULT_JOIN_SELECTIVITY)


def _access_factors(config: Dict[str, Any]) -> Dict[str, float]:
    perf_cfg = config.get("performance", {})
    raw = perf_cfg.get("access_factors", {})
    if isinstance(raw, dict) and raw:
        return {str(k): _to_float(v, DEFAULT_ACCESS_FACTORS.get(str(k), 1.0)) for k, v in raw.items()}
    return dict(DEFAULT_ACCESS_FACTORS)


def _operator_factors(config: Dict[str, Any]) -> Dict[str, float]:
    perf_cfg = config.get("performance", {})
    raw = perf_cfg.get("operator_factors", {})
    if isinstance(raw, dict) and raw:
        factors = dict(DEFAULT_OPERATOR_FACTORS)
        for key, value in raw.items():
            factors[str(key)] = _to_float(value, factors.get(str(key), 1.0))
        return factors
    return dict(DEFAULT_OPERATOR_FACTORS)


def _calibration_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    perf_cfg = config.get("performance", {})
    calibration = perf_cfg.get("calibration", {})
    if not isinstance(calibration, dict):
        calibration = {}
    return {
        "enabled": bool(calibration.get("enabled", False)),
        "global_factor": _to_float(calibration.get("global_factor"), 1.0),
        "operator_factors": {
            str(key): _to_float(value, 1.0)
            for key, value in calibration.get("operator_factors", {}).items()
        }
        if isinstance(calibration.get("operator_factors", {}), dict)
        else {},
        "latency_scale": _to_float(calibration.get("latency_scale"), 1.0),
    }


def _selectivity_for_join(
    samples: Dict[str, Any],
    left_table: str,
    right_table: str,
    join_keys: List[Any],
    default_value: float,
) -> float:
    join_samples = samples.get("join", []) if isinstance(samples, dict) else []
    if not isinstance(join_samples, list) or not join_samples:
        return default_value
    values: list[float] = []
    for key in join_keys:
        key_str = str(key)
        if "=" in key_str:
            left_key, right_key = [item.strip() for item in key_str.split("=", 1)]
            left_table_name, left_col = _parse_join_key(left_key)
            right_table_name, right_col = _parse_join_key(right_key)
        else:
            left_table_name, left_col = left_table, key_str
            right_table_name, right_col = right_table, key_str
        for item in join_samples:
            if not isinstance(item, dict):
                continue
            if (
                item.get("left_table") == left_table_name
                and item.get("right_table") == right_table_name
                and item.get("left_column") == left_col
                and item.get("right_column") == right_col
            ):
                values.append(_to_float(item.get("selectivity"), default_value))
            if (
                item.get("left_table") == right_table_name
                and item.get("right_table") == left_table_name
                and item.get("left_column") == right_col
                and item.get("right_column") == left_col
            ):
                values.append(_to_float(item.get("selectivity"), default_value))
    if values:
        return max(min(values), 1e-9)
    return default_value


def _parse_predicate_value(predicate: str) -> Tuple[str, str]:
    pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|([0-9]+(?:\.[0-9]+)?))",
        re.IGNORECASE,
    )
    match = pattern.search(predicate)
    if not match:
        return "", ""
    column = match.group(1)
    value = match.group(2) or match.group(3) or match.group(4) or ""
    return column, value


def _normalize_sql_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip().lower()


def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - timedelta(days=1)).day


def _add_months(base: date, months: int) -> date:
    year = base.year + (base.month - 1 + months) // 12
    month = (base.month - 1 + months) % 12 + 1
    day = min(base.day, _last_day_of_month(year, month))
    return date(year, month, day)


def _canonical_date_bound(base_str: str, interval_raw: str, unit_raw: str) -> str:
    try:
        base = date.fromisoformat(base_str)
    except ValueError:
        return base_str
    if not interval_raw or not unit_raw:
        return base.isoformat()

    amount = int(interval_raw)
    unit = unit_raw.lower()
    if unit == "day":
        result = base + timedelta(days=amount)
    elif unit == "month":
        result = _add_months(base, amount)
    elif unit == "year":
        result = _add_months(base, amount * 12)
    else:
        result = base
    return result.isoformat()


def _extract_date_comparisons(expr: str) -> List[Tuple[str, str, str]]:
    pattern = re.compile(
        r"(?:`?[A-Za-z_][A-Za-z0-9_]*`?\.)?`?([A-Za-z_][A-Za-z0-9_]*)`?\s*"
        r"(>=|<=|>|<)\s*date\s*'(\d{4}-\d{2}-\d{2})'"
        r"(?:\s*\+\s*interval\s*(\d+)\s*(day|month|year)s?)?",
        re.IGNORECASE,
    )
    comps: List[Tuple[str, str, str]] = []
    for col, op, base, amount, unit in pattern.findall(expr or ""):
        bound = _canonical_date_bound(base, amount, unit)
        comps.append((col.upper(), op, bound))
    return comps


def _comparison_bounds(comparisons: List[Tuple[str, str, str]], column: str) -> Tuple[Set[str], Set[str]]:
    lower: Set[str] = set()
    upper: Set[str] = set()
    target = column.upper()
    for col, op, bound in comparisons:
        if col != target:
            continue
        if op in {">", ">="}:
            lower.add(f"{op}{bound}")
        elif op in {"<", "<="}:
            upper.add(f"{op}{bound}")
    return lower, upper


def _horizontal_split_predicate_hit(sql_text: str, predicate: str) -> bool:
    if not predicate or not sql_text:
        return False

    if _normalize_sql_text(predicate) in _normalize_sql_text(sql_text):
        return True

    predicate_comparisons = _extract_date_comparisons(predicate)
    if not predicate_comparisons:
        return False
    target_col = predicate_comparisons[0][0]
    pred_lower, pred_upper = _comparison_bounds(predicate_comparisons, target_col)
    if not pred_lower and not pred_upper:
        return False

    sql_comparisons = _extract_date_comparisons(sql_text)
    sql_lower, sql_upper = _comparison_bounds(sql_comparisons, target_col)
    if not sql_lower and not sql_upper:
        return False

    lower_ok = not pred_lower or bool(pred_lower & sql_lower)
    upper_ok = not pred_upper or bool(pred_upper & sql_upper)
    return lower_ok and upper_ok


def _selectivity_for_predicate(
    samples: Dict[str, Any], table: str, predicate: str, default_value: float
) -> float:
    pred_samples = samples.get("predicate", []) if isinstance(samples, dict) else []
    if not isinstance(pred_samples, list) or not pred_samples:
        return default_value
    column, value = _parse_predicate_value(predicate)
    if not column or value == "":
        return default_value
    for item in pred_samples:
        if not isinstance(item, dict):
            continue
        if item.get("table") == table and item.get("column") == column and str(item.get("value")) == value:
            return _to_float(item.get("selectivity"), default_value)
    return default_value


def _apply_column_split(
    stats: Dict[str, Any],
    action: Dict[str, Any],
    column_sizes: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
) -> None:
    table = str(action.get("table", ""))
    column = str(action.get("column", ""))
    new_columns = action.get("new_columns", [])
    keep_original = bool(action.get("keep_original", True))
    if not table or table not in stats or not isinstance(new_columns, list):
        return
    if column not in stats[table]["columns"]:
        return
    table_sizes = column_sizes.setdefault(table, {})
    if not isinstance(table_sizes, dict):
        table_sizes = {}
        column_sizes[table] = table_sizes
    size = _column_size(column_sizes, table, column)
    ratio = _split_ratio(config)
    left_size = size * ratio
    right_size = max(size - left_size, 0.0)
    if not keep_original:
        stats[table]["columns"].discard(column)
        stats[table]["width"] = max(stats[table]["width"] - size, 1.0)
        table_sizes.pop(column, None)
    for idx, new_col in enumerate(new_columns[:2]):
        if not new_col:
            continue
        stats[table]["columns"].add(str(new_col))
        add_size = left_size if idx == 0 else right_size
        table_sizes[str(new_col)] = add_size
        stats[table]["width"] = max(stats[table]["width"] + add_size, 1.0)
    if not keep_original:
        stats[table]["width"] = max(stats[table]["width"], 1.0)


def _apply_table_split(
    stats: Dict[str, Any],
    action: Dict[str, Any],
    column_sizes: Dict[str, Dict[str, Any]],
    pk_map: Dict[str, List[str]],
    config: Dict[str, Any],
) -> None:
    source = str(action.get("source_table", ""))
    new_tables = action.get("new_tables", [])
    column_map = action.get("column_map", [])
    keep_original = bool(action.get("keep_original", True))
    if not source or source not in stats or not isinstance(new_tables, list):
        return
    if len(new_tables) < 2:
        return
    source_columns = set(stats[source]["columns"])

    def _normalize_column_map(raw: Any, new_tables: List[Any]) -> tuple[set[str], set[str]]:
        if isinstance(raw, list):
            return {str(col) for col in raw}, set()
        if isinstance(raw, dict):
            keys = [str(name) for name in new_tables if str(name) in raw]
            if len(keys) < 2:
                keys = [str(key) for key in raw.keys()]
            if len(keys) >= 2:
                cols_a = raw.get(keys[0], [])
                cols_b = raw.get(keys[1], [])
                if isinstance(cols_a, list) and isinstance(cols_b, list):
                    return {str(col) for col in cols_a}, {str(col) for col in cols_b}
        return set(), set()

    cols_a, cols_b = _normalize_column_map(column_map, new_tables)
    if not cols_b:
        cols_b = source_columns - cols_a
    if not keep_original:
        pk_cols = set(pk_map.get(source, []))
        cols_a |= pk_cols
        cols_b |= pk_cols

    rows = stats[source]["rows"]
    source_sizes = column_sizes.get(source, {})
    if not isinstance(source_sizes, dict):
        source_sizes = {}
    for idx, cols in enumerate([cols_a, cols_b]):
        table_name = str(new_tables[idx])
        width = sum(_column_size(column_sizes, source, col) for col in cols) or 1.0
        stats[table_name] = {"rows": rows, "width": width, "columns": set(cols)}
        column_sizes[table_name] = {
            str(col): _to_float(source_sizes.get(str(col)), _column_size(column_sizes, source, str(col)))
            for col in cols
        }

    if not keep_original:
        stats.pop(source, None)
        column_sizes.pop(source, None)


def _parse_join_key(key: str) -> Tuple[str, str]:
    if "." in key:
        table_name, col_name = key.split(".", 1)
        return table_name, col_name
    return "", key


def _apply_table_join(
    stats: Dict[str, Any],
    action: Dict[str, Any],
    column_sizes: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
    selectivity_samples: Dict[str, Any],
) -> None:
    left = str(action.get("left_table", ""))
    right = str(action.get("right_table", ""))
    new_table = str(action.get("new_table", f"{left}_{right}_join"))
    keep_original = bool(action.get("keep_original", True))
    if not left or not right or left not in stats or right not in stats:
        return

    left_rows = stats[left]["rows"]
    right_rows = stats[right]["rows"]
    join_keys = action.get("join_keys", [])
    if not isinstance(join_keys, list):
        join_keys = []
    join_selectivity = _selectivity_for_join(
        selectivity_samples,
        left,
        right,
        join_keys,
        _join_selectivity(config),
    )
    join_rows = max(left_rows, right_rows) * join_selectivity
    columns = set(stats[left]["columns"]) | set(stats[right]["columns"])
    width = sum(_column_size(column_sizes, left, col) for col in stats[left]["columns"]) + sum(
        _column_size(column_sizes, right, col) for col in stats[right]["columns"]
    )

    if join_keys:
        for key in join_keys:
            key_str = str(key)
            if "=" in key_str:
                left_key, right_key = [item.strip() for item in key_str.split("=", 1)]
                left_table, left_col = _parse_join_key(left_key)
                right_table, right_col = _parse_join_key(right_key)
                if left_table:
                    width -= _column_size(column_sizes, left_table, left_col)
                if right_table:
                    width -= _column_size(column_sizes, right_table, right_col)
            else:
                width -= _column_size(column_sizes, left, key_str)
                width -= _column_size(column_sizes, right, key_str)
    width = max(width, 1.0)

    stats[new_table] = {"rows": max(join_rows, DEFAULT_ROW_COUNT), "width": width, "columns": columns}
    left_sizes = column_sizes.get(left, {})
    right_sizes = column_sizes.get(right, {})
    merged_sizes: Dict[str, float] = {}
    if isinstance(left_sizes, dict):
        for col, size in left_sizes.items():
            merged_sizes[str(col)] = _to_float(size, DEFAULT_COLUMN_SIZE)
    if isinstance(right_sizes, dict):
        for col, size in right_sizes.items():
            merged_sizes.setdefault(str(col), _to_float(size, DEFAULT_COLUMN_SIZE))
    if merged_sizes:
        column_sizes[new_table] = merged_sizes
    if not keep_original:
        stats.pop(left, None)
        stats.pop(right, None)
        column_sizes.pop(left, None)
        column_sizes.pop(right, None)


def _apply_horizontal_split(
    stats: Dict[str, Any],
    action: Dict[str, Any],
    config: Dict[str, Any],
    selectivity_samples: Dict[str, Any],
    column_sizes: Dict[str, Dict[str, Any]],
) -> None:
    source = str(action.get("table", ""))
    if not source or source not in stats:
        return
    table_true = str(action.get("table_true", f"{source}_true"))
    table_false = str(action.get("table_false", f"{source}_false"))
    keep_original = bool(action.get("keep_original", True))
    selectivity = _default_selectivity(config)
    predicate = action.get("predicate")
    if predicate:
        selectivity = _selectivity_for_predicate(selectivity_samples, source, str(predicate), selectivity)
    rows = stats[source]["rows"]
    cols = set(stats[source]["columns"])
    width = stats[source]["width"]
    rows_true = max(rows * selectivity, DEFAULT_ROW_COUNT)
    rows_false = max(rows - rows_true, DEFAULT_ROW_COUNT)
    stats[table_true] = {"rows": rows_true, "width": width, "columns": set(cols)}
    stats[table_false] = {"rows": rows_false, "width": width, "columns": set(cols)}
    source_sizes = column_sizes.get(source, {})
    if isinstance(source_sizes, dict):
        copied = {str(col): _to_float(size, DEFAULT_COLUMN_SIZE) for col, size in source_sizes.items()}
        column_sizes[table_true] = dict(copied)
        column_sizes[table_false] = dict(copied)
    if not keep_original:
        stats.pop(source, None)
        column_sizes.pop(source, None)


def _apply_horizontal_merge(
    stats: Dict[str, Any], action: Dict[str, Any], column_sizes: Dict[str, Dict[str, Any]]
) -> None:
    table_a = str(action.get("table_a", ""))
    table_b = str(action.get("table_b", ""))
    new_table = str(action.get("new_table", f"{table_a}_{table_b}_merged"))
    keep_original = bool(action.get("keep_original", True))
    if not table_a or not table_b or table_a not in stats or table_b not in stats:
        return
    rows = stats[table_a]["rows"] + stats[table_b]["rows"]
    width = max(stats[table_a]["width"], stats[table_b]["width"], 1.0)
    cols = set(stats[table_a]["columns"]) or set(stats[table_b]["columns"])
    stats[new_table] = {"rows": rows, "width": width, "columns": set(cols)}
    size_a = column_sizes.get(table_a, {})
    size_b = column_sizes.get(table_b, {})
    merged_sizes: Dict[str, float] = {}
    if isinstance(size_a, dict):
        for col, size in size_a.items():
            merged_sizes[str(col)] = _to_float(size, DEFAULT_COLUMN_SIZE)
    if isinstance(size_b, dict):
        for col, size in size_b.items():
            merged_sizes.setdefault(str(col), _to_float(size, DEFAULT_COLUMN_SIZE))
    if merged_sizes:
        column_sizes[new_table] = merged_sizes
    if not keep_original:
        stats.pop(table_a, None)
        stats.pop(table_b, None)
        column_sizes.pop(table_a, None)
        column_sizes.pop(table_b, None)


def _apply_redundant_add(
    stats: Dict[str, Any], action: Dict[str, Any], column_sizes: Dict[str, Dict[str, Any]]
) -> None:
    src_table = str(action.get("src_table", ""))
    src_column = str(action.get("src_column", ""))
    dst_table = str(action.get("dst_table", ""))
    dst_column = str(action.get("dst_column", ""))
    if not src_table or not dst_table or dst_table not in stats or src_table not in stats:
        return
    size = _column_size(column_sizes, src_table, src_column)
    stats[dst_table]["columns"].add(dst_column)
    stats[dst_table]["width"] = max(stats[dst_table]["width"] + size, 1.0)
    dst_sizes = column_sizes.setdefault(dst_table, {})
    if isinstance(dst_sizes, dict) and dst_column:
        dst_sizes[dst_column] = size


def _apply_redundant_drop(
    stats: Dict[str, Any], action: Dict[str, Any], column_sizes: Dict[str, Dict[str, Any]]
) -> None:
    table = str(action.get("table", ""))
    column = str(action.get("column", ""))
    if not table or table not in stats:
        return
    size = _column_size(column_sizes, table, column)
    stats[table]["columns"].discard(column)
    stats[table]["width"] = max(stats[table]["width"] - size, 1.0)
    table_sizes = column_sizes.get(table, {})
    if isinstance(table_sizes, dict):
        table_sizes.pop(column, None)


def _apply_action_to_stats(
    stats: Dict[str, Any],
    action: Dict[str, Any],
    column_sizes: Dict[str, Dict[str, Any]],
    pk_map: Dict[str, List[str]],
    config: Dict[str, Any],
    selectivity_samples: Dict[str, Any],
) -> None:
    action_type = action.get("type")
    if action_type == "ColumnSplit":
        _apply_column_split(stats, action, column_sizes, config)
    elif action_type == "TableSplit":
        _apply_table_split(stats, action, column_sizes, pk_map, config)
    elif action_type == "TableJoin":
        _apply_table_join(stats, action, column_sizes, config, selectivity_samples)
    elif action_type == "HorizontalSplit":
        _apply_horizontal_split(stats, action, config, selectivity_samples, column_sizes)
    elif action_type == "HorizontalMerge":
        _apply_horizontal_merge(stats, action, column_sizes)
    elif action_type == "RedundantColumnAdd":
        _apply_redundant_add(stats, action, column_sizes)
    elif action_type == "RedundantColumnDrop":
        _apply_redundant_drop(stats, action, column_sizes)


def _has_join_between(context: Dict[str, Any], left: str, right: str) -> bool:
    for l_table, _, r_table, _ in context.get("joins", []):
        if {l_table, r_table} == {left, right}:
            return True
    return False


def _contains_table(sql: str, table: str) -> bool:
    alias_map = _extract_aliases(sql)
    names = {table}
    names.update(alias for alias, mapped in alias_map.items() if mapped == table)
    for name in names:
        if re.search(rf"\b{re.escape(name)}\b", sql, re.IGNORECASE):
            return True
    return False


def _has_join_predicate_sql(sql: str, left: str, right: str) -> bool:
    if not left or not right or not sql:
        return False
    alias_map = _extract_aliases(sql)
    pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.(`?[A-Za-z_][A-Za-z0-9_]*`?)\s*=\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\.(`?[A-Za-z_][A-Za-z0-9_]*`?)",
        re.IGNORECASE,
    )
    for left_table, _, right_table, _ in pattern.findall(sql):
        mapped_left = alias_map.get(left_table, left_table)
        mapped_right = alias_map.get(right_table, right_table)
        if {mapped_left, mapped_right} == {left, right}:
            return True
    if not (_contains_table(sql, left) and _contains_table(sql, right)):
        return False
    if not re.search(r"\bwhere\b|\bon\b", sql, re.IGNORECASE):
        return False
    loose_pattern = re.compile(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\b\s*=\s*\b([A-Za-z_][A-Za-z0-9_]*)\b",
        re.IGNORECASE,
    )
    for left_col, right_col in loose_pattern.findall(sql):
        if left_col.lower() == right_col.lower():
            continue
        if _is_keyword(left_col) or _is_keyword(right_col):
            continue
        return True
    return False


def _join_type_between_sql(sql: str, left: str, right: str) -> str:
    if not left or not right or not sql:
        return ""
    alias_map = _extract_aliases(sql)
    pattern = re.compile(
        r"\b(?:(left|right|inner|outer|full|cross)\s+)?join\s+`?([A-Za-z0-9_]+)`?(?:\s+as)?\s*`?([A-Za-z0-9_]+)?`?",
        re.IGNORECASE,
    )
    for join_type, table_name, alias in pattern.findall(sql):
        mapped = table_name
        if alias and alias_map.get(alias):
            mapped = alias_map.get(alias, table_name)
        if mapped in {left, right}:
            return (join_type or "inner").lower()
    for join_type, alias in _extract_subquery_joins(sql):
        mapped = alias_map.get(alias, alias)
        if mapped in {left, right}:
            return (join_type or "inner").lower()
    return ""


def _can_rewrite_join(sql: str, left: str, right: str, context: Dict[str, Any]) -> Tuple[bool, str]:
    if not sql:
        return False, ""
    join_type = _join_type_between_sql(sql, left, right)
    join_hit = _has_join_between(context, left, right) or _has_join_predicate_sql(sql, left, right)
    if not join_type and join_hit:
        join_type = "inner"
    if not join_type:
        return False, ""
    if join_type in {"left", "right", "outer", "full"}:
        return False, join_type
    return True, join_type


def _apply_action_to_context(context: Dict[str, Any], action: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
    action_type = action.get("type")
    tables = set(context.get("tables", set()))
    columns = {k: set(v) for k, v in context.get("columns", {}).items()}
    access_modes = dict(context.get("access_modes", {}))
    join_types = list(context.get("join_types", []))
    joins = list(context.get("joins", []))
    sql_text = context.get("sql", "")

    if action_type == "ColumnSplit":
        table = str(action.get("table", ""))
        column = str(action.get("column", ""))
        new_columns = action.get("new_columns", [])
        keep_original = bool(action.get("keep_original", True))
        if table in columns and isinstance(new_columns, list) and column:
            if column in columns[table]:
                if not keep_original:
                    columns[table].discard(column)
                for new_col in new_columns[:2]:
                    if new_col:
                        columns[table].add(str(new_col))

    elif action_type == "TableSplit":
        source = str(action.get("source_table", ""))
        new_tables = action.get("new_tables", [])
        column_map = action.get("column_map", [])
        keep_original = bool(action.get("keep_original", True))
        if source in tables and isinstance(new_tables, list) and len(new_tables) >= 2:
            used = columns.get(source, set())
            if isinstance(column_map, list):
                map_set = {str(col) for col in column_map}
            elif isinstance(column_map, dict):
                keys = [str(name) for name in new_tables if str(name) in column_map]
                if len(keys) < 2:
                    keys = [str(key) for key in column_map.keys()]
                if len(keys) >= 2 and isinstance(column_map.get(keys[0]), list):
                    map_set = {str(col) for col in column_map.get(keys[0], [])}
                else:
                    map_set = set()
            else:
                map_set = set()
            source_cols = set(stats.get(source, {}).get("columns", set()))
            other_set = source_cols - map_set
            source_mode = access_modes.get(source, "table_scan")

            if keep_original:
                if used and used.issubset(map_set):
                    tables.discard(source)
                    columns.pop(source, None)
                    access_modes.pop(source, None)
                    tables.add(str(new_tables[0]))
                    columns[str(new_tables[0])] = set(used)
                    access_modes[str(new_tables[0])] = source_mode
                elif used and used.issubset(other_set):
                    tables.discard(source)
                    columns.pop(source, None)
                    access_modes.pop(source, None)
                    tables.add(str(new_tables[1]))
                    columns[str(new_tables[1])] = set(used)
                    access_modes[str(new_tables[1])] = source_mode
            else:
                tables.discard(source)
                columns.pop(source, None)
                access_modes.pop(source, None)
                if not used:
                    tables.add(str(new_tables[0]))
                    tables.add(str(new_tables[1]))
                    columns[str(new_tables[0])] = set(map_set)
                    columns[str(new_tables[1])] = set(other_set)
                    access_modes[str(new_tables[0])] = source_mode
                    access_modes[str(new_tables[1])] = source_mode
                elif used.issubset(map_set):
                    tables.add(str(new_tables[0]))
                    columns[str(new_tables[0])] = set(used)
                    access_modes[str(new_tables[0])] = source_mode
                elif used.issubset(other_set):
                    tables.add(str(new_tables[1]))
                    columns[str(new_tables[1])] = set(used)
                    access_modes[str(new_tables[1])] = source_mode
                else:
                    tables.add(str(new_tables[0]))
                    tables.add(str(new_tables[1]))
                    columns[str(new_tables[0])] = set(used & map_set)
                    columns[str(new_tables[1])] = set(used - map_set)
                    access_modes[str(new_tables[0])] = source_mode
                    access_modes[str(new_tables[1])] = source_mode

    elif action_type == "TableJoin":
        left = str(action.get("left_table", ""))
        right = str(action.get("right_table", ""))
        new_table = str(action.get("new_table", f"{left}_{right}_join"))
        has_left = left in tables
        has_right = right in tables
        should_replace, _ = _can_rewrite_join(sql_text, left, right, context)
        if has_left and has_right and should_replace:
            combined = set(columns.get(left, set())) | set(columns.get(right, set()))
            tables.discard(left)
            tables.discard(right)
            columns.pop(left, None)
            columns.pop(right, None)
            access_modes.pop(left, None)
            access_modes.pop(right, None)
            tables.add(new_table)
            columns[new_table] = combined
            access_modes[new_table] = "table_scan"

            filtered_joins: List[Tuple[str, str, str, str]] = []
            filtered_join_types: List[str] = []
            for idx, pair in enumerate(joins):
                if len(pair) != 4:
                    continue
                l_table, l_col, r_table, r_col = pair
                if {l_table, r_table} == {left, right}:
                    continue
                if left in {l_table, r_table} or right in {l_table, r_table}:
                    continue
                filtered_joins.append(pair)
                if idx < len(join_types):
                    filtered_join_types.append(join_types[idx])
            joins = filtered_joins
            join_types = filtered_join_types

    elif action_type == "HorizontalSplit":
        source = str(action.get("table", ""))
        table_true = str(action.get("table_true", f"{source}_true"))
        table_false = str(action.get("table_false", f"{source}_false"))
        keep_original = bool(action.get("keep_original", True))
        if source in tables:
            source_mode = access_modes.get(source, "table_scan")
            predicate = str(action.get("predicate", ""))
            predicate_hit = _horizontal_split_predicate_hit(sql_text, predicate)
            if keep_original and not predicate_hit:
                return {
                    "tables": tables,
                    "columns": columns,
                    "joins": joins,
                    "join_types": join_types,
                    "access_modes": access_modes,
                    "has_sort": context.get("has_sort", False),
                    "has_group": context.get("has_group", False),
                    "sql": sql_text,
                }
            tables.discard(source)
            source_cols = columns.pop(source, set())
            access_modes.pop(source, None)
            if predicate_hit:
                tables.add(table_true)
                columns[table_true] = set(source_cols)
                access_modes[table_true] = source_mode
            else:
                tables.add(table_true)
                tables.add(table_false)
                columns[table_true] = set(source_cols)
                columns[table_false] = set(source_cols)
                access_modes[table_true] = source_mode
                access_modes[table_false] = source_mode

    elif action_type == "HorizontalMerge":
        table_a = str(action.get("table_a", ""))
        table_b = str(action.get("table_b", ""))
        new_table = str(action.get("new_table", f"{table_a}_{table_b}_merged"))
        keep_original = bool(action.get("keep_original", True))
        has_a = table_a in tables
        has_b = table_b in tables
        if keep_original and not (has_a and has_b):
            return {
                "tables": tables,
                "columns": columns,
                "joins": joins,
                "join_types": join_types,
                "access_modes": access_modes,
                "has_sort": context.get("has_sort", False),
                "has_group": context.get("has_group", False),
                "sql": sql_text,
            }
        if has_a or has_b:
            combined = set(columns.get(table_a, set())) | set(columns.get(table_b, set()))
            tables.discard(table_a)
            tables.discard(table_b)
            columns.pop(table_a, None)
            columns.pop(table_b, None)
            access_modes.pop(table_a, None)
            access_modes.pop(table_b, None)
            tables.add(new_table)
            columns[new_table] = combined
            access_modes[new_table] = "table_scan"

    elif action_type == "RedundantColumnAdd":
        src_table = str(action.get("src_table", ""))
        dst_table = str(action.get("dst_table", ""))
        src_column = str(action.get("src_column", ""))
        dst_column = str(action.get("dst_column", ""))
        join_keys = action.get("join_keys", []) if isinstance(action.get("join_keys"), list) else []
        if src_table in tables and dst_table in tables and src_column:
            src_join_cols: Set[str] = set()
            for raw in join_keys:
                if not isinstance(raw, str) or "=" not in raw:
                    continue
                left_key, right_key = [item.strip() for item in raw.split("=", 1)]
                left_table, left_col = _parse_join_key(left_key)
                right_table, right_col = _parse_join_key(right_key)
                if not left_col or not right_col:
                    continue
                if left_table.lower() == src_table.lower() and right_table.lower() == dst_table.lower():
                    src_join_cols.add(left_col)
                elif left_table.lower() == dst_table.lower() and right_table.lower() == src_table.lower():
                    src_join_cols.add(right_col)

            used_src_cols = set(columns.get(src_table, set()))
            if src_column not in used_src_cols:
                return {
                    "tables": tables,
                    "columns": columns,
                    "joins": joins,
                    "join_types": join_types,
                    "access_modes": access_modes,
                    "has_sort": context.get("has_sort", False),
                    "has_group": context.get("has_group", False),
                    "sql": sql_text,
                }
            allowed_src_cols = {src_column}
            allowed_src_cols.update(src_join_cols)
            if used_src_cols and not used_src_cols.issubset(allowed_src_cols):
                return {
                    "tables": tables,
                    "columns": columns,
                    "joins": joins,
                    "join_types": join_types,
                    "access_modes": access_modes,
                    "has_sort": context.get("has_sort", False),
                    "has_group": context.get("has_group", False),
                    "sql": sql_text,
                }
            join_hit = _has_join_between(context, src_table, dst_table) or _has_join_predicate_sql(
                sql_text, src_table, dst_table
            )
            if not join_hit:
                return {
                    "tables": tables,
                    "columns": columns,
                    "joins": joins,
                    "join_types": join_types,
                    "access_modes": access_modes,
                    "has_sort": context.get("has_sort", False),
                    "has_group": context.get("has_group", False),
                    "sql": sql_text,
                }
            has_other_joins = False
            for pair in joins:
                if len(pair) != 4:
                    continue
                l_table, _, r_table, _ = pair
                if src_table in {l_table, r_table} and dst_table not in {l_table, r_table}:
                    has_other_joins = True
                    break
            if has_other_joins:
                return {
                    "tables": tables,
                    "columns": columns,
                    "joins": joins,
                    "join_types": join_types,
                    "access_modes": access_modes,
                    "has_sort": context.get("has_sort", False),
                    "has_group": context.get("has_group", False),
                    "sql": sql_text,
                }

            filtered_joins: List[Tuple[str, str, str, str]] = []
            filtered_join_types: List[str] = []
            for idx, pair in enumerate(joins):
                if len(pair) != 4:
                    continue
                l_table, l_col, r_table, r_col = pair
                if {l_table, r_table} == {src_table, dst_table}:
                    continue
                filtered_joins.append(pair)
                if idx < len(join_types):
                    filtered_join_types.append(join_types[idx])
            joins = filtered_joins
            join_types = filtered_join_types

            tables.discard(src_table)
            columns.pop(src_table, None)
            access_modes.pop(src_table, None)
            columns.setdefault(dst_table, set()).add(dst_column or src_column)

    elif action_type == "RedundantColumnDrop":
        table = str(action.get("table", ""))
        column = str(action.get("column", ""))
        origin_table = str(action.get("origin_table", ""))
        if table in tables and column in columns.get(table, set()) and origin_table:
            tables.add(origin_table)
            columns.setdefault(origin_table, set()).add(column)
            access_modes.setdefault(origin_table, "table_scan")
            joins = context.get("joins", [])
            join_types = context.get("join_types", [])

    return {
        "tables": tables,
        "columns": columns,
        "joins": joins,
        "join_types": join_types,
        "access_modes": access_modes,
        "has_sort": context.get("has_sort", False),
        "has_group": context.get("has_group", False),
        "sql": sql_text,
    }


def _effective_scan_width(
    table: str,
    context: Dict[str, Any],
    entry: Dict[str, Any],
    column_sizes: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
) -> float:
    full_width = _to_float(entry.get("width"), DEFAULT_COLUMN_SIZE)
    if full_width <= 0:
        return DEFAULT_COLUMN_SIZE

    context_columns = context.get("columns", {})
    used_cols_raw = context_columns.get(table, set()) if isinstance(context_columns, dict) else set()
    used_cols = {str(col) for col in used_cols_raw} if isinstance(used_cols_raw, set) else set()
    if not used_cols:
        return full_width

    size_map = column_sizes.get(table, {})
    projected_width = 0.0
    if isinstance(size_map, dict) and size_map:
        projected_width = sum(_to_float(size_map.get(col), DEFAULT_COLUMN_SIZE) for col in used_cols)

    if projected_width <= 0:
        all_cols = entry.get("columns", set())
        total_cols = len(all_cols) if isinstance(all_cols, set) else 0
        if total_cols > 0:
            fraction = min(len(used_cols), total_cols) / float(total_cols)
            projected_width = max(full_width * fraction, DEFAULT_COLUMN_SIZE)
        else:
            projected_width = full_width

    penalty = _row_store_penalty_factor(config)
    blended = projected_width + full_width * penalty
    return min(full_width, max(blended, DEFAULT_COLUMN_SIZE))


def _estimate_cost(
    context: Dict[str, Any],
    stats: Dict[str, Any],
    column_sizes: Dict[str, Dict[str, Any]],
    access_factors: Dict[str, float],
    operator_factors: Dict[str, float],
    selectivity_samples: Dict[str, Any],
    config: Dict[str, Any],
) -> float:
    total = 0.0
    for table in context.get("tables", set()):
        entry = stats.get(table)
        if not entry:
            continue
        mode = context.get("access_modes", {}).get(table, "table_scan")
        factor = access_factors.get(mode, 1.0)
        scan_width = _effective_scan_width(table, context, entry, column_sizes, config)
        total += _to_float(entry.get("rows"), DEFAULT_ROW_COUNT) * scan_width * factor

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
        join_rows = max(left.get("rows", DEFAULT_ROW_COUNT), right.get("rows", DEFAULT_ROW_COUNT)) * join_selectivity
        join_type = join_types[idx] if idx < len(join_types) else "unknown_join"
        join_factor = operator_factors.get(join_type, 1.0)
        if join_type == "nested_loop":
            total += left.get("rows", DEFAULT_ROW_COUNT) * right.get("rows", DEFAULT_ROW_COUNT) * join_factor
        elif join_type in {"hash_join", "merge_join", "unknown_join"}:
            total += (left.get("rows", DEFAULT_ROW_COUNT) + right.get("rows", DEFAULT_ROW_COUNT)) * join_factor
        total += join_rows * join_factor

    total_rows = 0.0
    for table in context.get("tables", set()):
        entry = stats.get(table)
        if entry:
            total_rows += _to_float(entry.get("rows"), DEFAULT_ROW_COUNT)

    if context.get("has_sort") and total_rows > 0:
        sort_factor = operator_factors.get("sort", 1.0)
        total += total_rows * math.log2(max(total_rows, 2.0)) * sort_factor

    if context.get("has_group") and total_rows > 0:
        group_factor = operator_factors.get("group", 1.0)
        total += total_rows * math.log2(max(total_rows, 2.0)) * group_factor

    return total


def _resolve_breakdown_path(config: Dict[str, Any]) -> Path:
    perf_cfg = config.get("performance", {})
    output_value = perf_cfg.get("breakdown_output_path", "performance_breakdown.json")
    output_dir = Path(str(config.get("project", {}).get("output_dir", "./output")))
    path = Path(str(output_value))
    if not path.is_absolute():
        path = output_dir / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _table_breakdown(
    context: Dict[str, Any],
    stats: Dict[str, Any],
    access_factors: Dict[str, float],
) -> Dict[str, Any]:
    tables: Dict[str, Any] = {}
    for table in context.get("tables", set()):
        entry = stats.get(table)
        if not entry:
            continue
        mode = context.get("access_modes", {}).get(table, "table_scan")
        tables[table] = {
            "rows": _to_float(entry.get("rows"), DEFAULT_ROW_COUNT),
            "width": _to_float(entry.get("width"), DEFAULT_COLUMN_SIZE),
            "access_mode": mode,
            "access_factor": _to_float(access_factors.get(mode), 1.0),
        }
    return tables


def _join_breakdown(
    context: Dict[str, Any],
    selectivity_samples: Dict[str, Any],
    config: Dict[str, Any],
    operator_factors: Dict[str, float],
) -> List[Dict[str, Any]]:
    joins: List[Dict[str, Any]] = []
    join_pairs = context.get("joins", [])
    join_types = context.get("join_types", [])
    for idx, pair in enumerate(join_pairs):
        if len(pair) != 4:
            continue
        left_table, left_col, right_table, right_col = pair
        join_key = f"{left_table}.{left_col}={right_table}.{right_col}"
        join_selectivity = _selectivity_for_join(
            selectivity_samples,
            left_table,
            right_table,
            [join_key],
            _join_selectivity(config),
        )
        join_type = join_types[idx] if idx < len(join_types) else "unknown_join"
        joins.append(
            {
                "left_table": left_table,
                "left_column": left_col,
                "right_table": right_table,
                "right_column": right_col,
                "join_type": join_type,
                "join_selectivity": join_selectivity,
                "operator_factor": _to_float(operator_factors.get(join_type), 1.0),
            }
        )
    return joins


def _write_breakdown(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def estimate_performance(metadata: Dict[str, Any], action_sequence: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Estimate latency delta using workload SQL and table stats proxy."""
    metrics = metadata.get("metrics", {})
    workload_sql = metadata.get("workload_sql", {})
    plans = metadata.get("plans", {})
    schema_state = metadata.get("schema_state", {})
    storage_stats = metadata.get("storage_stats", {})
    selectivity_samples = metadata.get("selectivity_samples", {})
    actions = action_sequence.get("actions", []) if isinstance(action_sequence, dict) else []
    if not isinstance(actions, list):
        actions = []
    access_factors = _access_factors(config)
    operator_factors = _operator_factors(config)
    calibration_info = _calibration_settings(config)
    if calibration_info["enabled"]:
        for op, factor in calibration_info["operator_factors"].items():
            operator_factors[op] = operator_factors.get(op, 1.0) * factor

    table_count = 0
    if isinstance(schema_state, dict):
        tables = schema_state.get("tables", {})
        if isinstance(tables, dict):
            table_count = len(tables)

    logger.info(
        "estimate_performance: sql=%d metrics=%d plans=%d tables=%d actions=%d",
        len(workload_sql),
        len(metrics),
        len(plans),
        table_count,
        len(actions),
    )
    if table_count == 0:
        logger.warning("estimate_performance: schema_state has no tables; cost estimates may be invalid")

    normalized_metrics = _normalize_metrics(metrics)
    baseline_total = sum(v["total_latency_ms"] for v in normalized_metrics.values())

    stats, column_sizes, pk_map = _table_metadata(schema_state, storage_stats, config)
    base_contexts: Dict[str, Dict[str, Any]] = {}
    skipped_sql: List[Dict[str, Any]] = []
    for sql_id, sql in workload_sql.items():
        alias_map = _extract_aliases(sql)
        usage = _extract_table_column_usage(sql)
        tables = _extract_tables(sql) | set(usage.keys())
        if not tables:
            tables = _extract_tables_from_plan(str(plans.get(sql_id, "")), alias_map)
        if not tables:
            skipped_sql.append({"sql_id": sql_id, "reason": "no_tables"})
            continue
        columns = {table: set() for table in tables}
        for table, cols in usage.items():
            columns.setdefault(table, set()).update(cols)
        plan_text = str(plans.get(sql_id, ""))
        operator_flags = _extract_operator_flags(plan_text)
        base_contexts[sql_id] = {
            "tables": tables,
            "columns": columns,
            "joins": _extract_join_pairs(sql),
            "access_modes": _extract_access_modes(plan_text, alias_map),
            "join_types": operator_flags.get("join_types", []),
            "has_sort": operator_flags.get("has_sort", False),
            "has_group": operator_flags.get("has_group", False),
            "sql": sql,
        }

    base_costs: Dict[str, float] = {
        sql_id: _estimate_cost(ctx, stats, column_sizes, access_factors, operator_factors, selectivity_samples, config)
        for sql_id, ctx in base_contexts.items()
    }
    if not base_contexts:
        logger.warning("estimate_performance: no base contexts; check workload_sql/plans")
    elif base_costs and all(cost <= 0 for cost in base_costs.values()):
        logger.warning("estimate_performance: all base costs are zero; check schema_state and plan parsing")
    base_latency: Dict[str, float] = {sql_id: v["total_latency_ms"] for sql_id, v in normalized_metrics.items()}
    if not base_latency and base_costs:
        base_latency = {sql_id: cost for sql_id, cost in base_costs.items()}
        baseline_total = sum(base_latency.values())
    current_latency = {sql_id: base_latency.get(sql_id, 0.0) for sql_id in base_contexts}
    current_costs = dict(base_costs)

    op_deltas: Dict[str, float] = {}
    current_stats = {k: {"rows": v["rows"], "width": v["width"], "columns": set(v["columns"])} for k, v in stats.items()}
    current_contexts = dict(base_contexts)

    for idx, action in enumerate(actions, 1):
        if not isinstance(action, dict):
            continue
        _apply_action_to_stats(current_stats, action, column_sizes, pk_map, config, selectivity_samples)
        current_contexts = {
            sql_id: _apply_action_to_context(ctx, action, current_stats)
            for sql_id, ctx in current_contexts.items()
        }
        new_costs = {
              sql_id: _estimate_cost(ctx, current_stats, column_sizes, access_factors, operator_factors, selectivity_samples, config)
            for sql_id, ctx in current_contexts.items()
        }
        new_latency = {}
        for sql_id, base in base_latency.items():
            if sql_id not in current_contexts:
                continue
            base_cost = base_costs.get(sql_id, 0.0)
            ratio = new_costs.get(sql_id, base_cost) / base_cost if base_cost > 0 else 1.0
            new_latency[sql_id] = base * ratio
        delta = sum(new_latency.get(sql_id, 0.0) - current_latency.get(sql_id, 0.0) for sql_id in new_latency)
        op_deltas[f"action_{idx}"] = delta
        current_latency.update(new_latency)
        current_costs.update(new_costs)

    sql_deltas = {}
    for sql_id, base in base_latency.items():
        if sql_id not in current_latency:
            continue
        sql_deltas[sql_id] = current_latency[sql_id] - base

    total_delta = sum(sql_deltas.values())

    calibration_factor = 1.0
    latency_scale = 1.0
    if calibration_info["enabled"]:
        calibration_factor = calibration_info["global_factor"]
        latency_scale = calibration_info["latency_scale"]
        sql_deltas = {k: v * calibration_factor * latency_scale for k, v in sql_deltas.items()}
        op_deltas = {k: v * calibration_factor * latency_scale for k, v in op_deltas.items()}
        total_delta *= calibration_factor * latency_scale

    breakdown: Dict[str, Any] = {
        "baseline_total_latency_ms": baseline_total,
        "calibration_factor": calibration_factor,
        "latency_scale": latency_scale,
        "calibration": calibration_info,
        "access_factors": access_factors,
        "operator_factors": operator_factors,
        "defaults": {
            "default_selectivity": _default_selectivity(config),
            "join_selectivity": _join_selectivity(config),
            "split_ratio": _split_ratio(config),
              "row_store_penalty_factor": _row_store_penalty_factor(config),
        },
        "debug": {
            "tables": table_count,
            "workload_sql": len(workload_sql),
            "metrics": len(metrics),
            "plans": len(plans),
            "base_contexts": len(base_contexts),
            "skipped_sql": len(skipped_sql),
        },
        "actions": action_sequence.get("actions", []) if isinstance(action_sequence, dict) else [],
        "per_sql": {},
        "skipped_sql": skipped_sql,
    }
    for sql_id, base_ctx in base_contexts.items():
        base_cost = base_costs.get(sql_id, 0.0)
        final_cost = current_costs.get(sql_id, base_cost)
        base_lat = base_latency.get(sql_id, 0.0)
        final_lat_raw = current_latency.get(sql_id, base_lat)
        raw_delta = final_lat_raw - base_lat
        delta = raw_delta * calibration_factor
        final_lat = base_lat + delta
        ratio = final_cost / base_cost if base_cost > 0 else 1.0
        final_ctx = current_contexts.get(sql_id, base_ctx)
        breakdown["per_sql"][sql_id] = {
            "base_cost": base_cost,
            "final_cost": final_cost,
            "cost_ratio": ratio,
            "base_latency_ms": base_lat,
            "final_latency_ms": final_lat,
            "raw_delta_ms": raw_delta,
            "delta_ms": delta,
            "baseline": {
                "tables": _table_breakdown(base_ctx, stats, access_factors),
                "joins": _join_breakdown(base_ctx, selectivity_samples, config, operator_factors),
                "has_sort": bool(base_ctx.get("has_sort")),
                "has_group": bool(base_ctx.get("has_group")),
            },
            "after": {
                "tables": _table_breakdown(final_ctx, current_stats, access_factors),
                "joins": _join_breakdown(final_ctx, selectivity_samples, config, operator_factors),
                "has_sort": bool(final_ctx.get("has_sort")),
                "has_group": bool(final_ctx.get("has_group")),
            },
        }

    breakdown_path = _resolve_breakdown_path(config)
    _write_breakdown(breakdown_path, breakdown)

    logger.info(
        "estimate_performance: baseline=%.3f total_delta=%.3f skipped_sql=%d",
        baseline_total,
        total_delta,
        len(skipped_sql),
    )

    return {
        "sql_deltas": sql_deltas,
        "op_deltas": op_deltas,
        "total_delta": total_delta,
        "baseline_total_latency_ms": baseline_total,
        "skipped_sql": skipped_sql,
    }
