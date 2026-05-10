from __future__ import annotations

from typing import Any, Dict, List, Tuple
from datetime import date, timedelta
import json
import logging
import re
from pathlib import Path

try:
    from sqlglot import exp, parse_one
except Exception:  # pragma: no cover - fallback path when dependency is unavailable
    exp = None
    parse_one = None

logger = logging.getLogger(__name__)


def _parse_sql_ast(sql: str) -> Any:
    if parse_one is None:
        return None
    try:
        return parse_one(sql, read="mysql")
    except Exception:
        return None


def _ast_to_sql(tree: Any, fallback: str) -> str:
    if tree is None:
        return fallback
    try:
        return tree.sql(dialect="mysql")
    except Exception:
        return fallback


def _normalize_ident(name: str) -> str:
    return str(name or "").strip("`\" ").lower()


def _is_simple_identifier(name: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name or "")))


def _table_alias_map_ast(tree: Any) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if exp is None or tree is None:
        return mapping
    for table in tree.find_all(exp.Table):
        real = _normalize_ident(getattr(table, "name", ""))
        alias = _normalize_ident(getattr(table, "alias_or_name", ""))
        if real:
            mapping[real] = real
        if alias and real:
            mapping[alias] = real
    return mapping


def _table_node_names(table_node: Any) -> set[str]:
    if exp is None or not isinstance(table_node, exp.Table):
        return set()
    names = {
        _normalize_ident(getattr(table_node, "name", "")),
        _normalize_ident(getattr(table_node, "alias_or_name", "")),
    }
    return {name for name in names if name}


def _join_arg_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "name"):
        return _normalize_ident(getattr(value, "name", ""))
    return _normalize_ident(str(value))


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
        comps.append((col.upper(), op, _canonical_date_bound(base, amount, unit)))
    return comps


def _comparison_bounds(comparisons: List[Tuple[str, str, str]], column: str) -> Tuple[set[str], set[str]]:
    lower: set[str] = set()
    upper: set[str] = set()
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
        if alias.lower() in {
            "on",
            "where",
            "join",
            "left",
            "right",
            "inner",
            "outer",
            "group",
            "order",
            "limit",
            "having",
        }:
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
                idx += 4 if _matches_keyword(sql, idx, "from") else 4
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


def _aliases_for_table(alias_map: Dict[str, str], table: str) -> List[str]:
    return [alias for alias, mapped in alias_map.items() if mapped == table]


def _alias_for_table(sql: str, table: str) -> str:
    alias_map = _extract_aliases(sql)
    for alias, mapped in alias_map.items():
        if mapped == table:
            return alias
    return ""


def rewrite_sql(
    sql_list: List[str],
    action_sequence: Dict[str, Any],
    schema_state: Dict[str, Any],
    output_dir: str,
) -> Dict[str, Any]:
    """Rewrite SQL files based on schema evolution."""
    actions = action_sequence.get("actions", []) if isinstance(action_sequence, dict) else []
    if not isinstance(actions, list):
        return {"status": "error", "error": "actions must be list", "sql_list": sql_list}

    out_dir = Path(output_dir)
    if out_dir.name != "sql_rewrite":
        out_dir = out_dir / "sql_rewrite"
    out_dir.mkdir(parents=True, exist_ok=True)

    sql_current = list(sql_list)
    table_map: Dict[str, str] = {}
    column_map: Dict[Tuple[str, str], Tuple[str, str]] = {}
    redundant_map: Dict[Tuple[str, str], Tuple[str, str, List[str]]] = {}
    join_map: Dict[Tuple[str, str], Dict[str, str]] = {}
    split_map: Dict[str, Tuple[str, str, List[str], List[str], bool]] = {}
    steps: List[Dict[str, Any]] = []

    for idx, action in enumerate(actions, 1):
        step_dir = out_dir / f"step_{idx:03d}_{action.get('type', 'unknown')}"
        step_dir.mkdir(parents=True, exist_ok=True)
        sql_current = _rewrite_for_action(
            sql_current,
            action,
            schema_state,
            table_map,
            column_map,
            redundant_map,
            join_map,
            split_map,
        )
        _write_sql_files(step_dir, sql_current)
        steps.append({"step": idx, "type": action.get("type"), "dir": str(step_dir)})

    final_dir = out_dir / "final"
    final_dir.mkdir(parents=True, exist_ok=True)
    _write_sql_files(final_dir, sql_current)

    log = {
        "status": "ok",
        "steps": steps,
        "final_dir": str(final_dir),
        "table_map": dict(table_map),
        "column_map": {f"{k[0]}.{k[1]}": f"{v[0]}.{v[1]}" for k, v in column_map.items()},
        "redundant_map": {f"{k[0]}.{k[1]}": f"{v[0]}.{v[1]}" for k, v in redundant_map.items()},
    }
    log_path = out_dir / "rewrite_log.json"
    log_path.write_text(json.dumps(log, ensure_ascii=True, indent=2), encoding="utf-8")

    return {"status": "ok", "sql_list": sql_current, "log_path": str(log_path)}


def _write_sql_files(step_dir: Path, sql_list: List[str]) -> None:
    for idx, sql in enumerate(sql_list, 1):
        file_path = step_dir / f"sql_{idx:04d}.sql"
        file_path.write_text(sql, encoding="utf-8")


def _rewrite_for_action(
    sql_list: List[str],
    action: Dict[str, Any],
    schema_state: Dict[str, Any],
    table_map: Dict[str, str],
    column_map: Dict[Tuple[str, str], Tuple[str, str]],
    redundant_map: Dict[Tuple[str, str], Tuple[str, str, List[str]]],
    join_map: Dict[Tuple[str, str], Dict[str, str]],
    split_map: Dict[str, Tuple[str, str, List[str], List[str], bool]],
) -> List[str]:
    action_type = action.get("type")
    if action_type == "ColumnSplit":
        return _rewrite_column_split(sql_list, action, table_map, column_map)
    if action_type == "TableSplit":
        return _rewrite_table_split(sql_list, action, schema_state, table_map, split_map)
    if action_type == "TableJoin":
        return _rewrite_table_join(sql_list, action, schema_state, table_map, join_map)
    if action_type == "HorizontalSplit":
        return _rewrite_horizontal_split(sql_list, action, table_map)
    if action_type == "HorizontalMerge":
        return _rewrite_horizontal_merge(sql_list, action, table_map)
    if action_type == "RedundantColumnAdd":
        return _rewrite_redundant_add(sql_list, action, redundant_map)
    if action_type == "RedundantColumnDrop":
        return _rewrite_redundant_drop(sql_list, action, redundant_map)
    logger.warning("sql_rewriter: unsupported action type %s", action_type)
    return sql_list


def _rewrite_column_split(
    sql_list: List[str],
    action: Dict[str, Any],
    table_map: Dict[str, str],
    column_map: Dict[Tuple[str, str], Tuple[str, str]],
) -> List[str]:
    table = str(action.get("table"))
    column = str(action.get("column"))
    delimiter = str(action.get("delimiter"))
    new_columns = action.get("new_columns", [])
    if not table or not column or not isinstance(new_columns, list) or len(new_columns) < 2:
        return sql_list

    col_a, col_b = str(new_columns[0]), str(new_columns[1])
    table_effective = table_map.get(table, table)
    column_map[(table, column)] = (table_effective, col_a)

    rewritten = []
    for sql in sql_list:
        alias_map = _extract_aliases(sql)
        prefixes = [table]
        prefixes.extend(_aliases_for_table(alias_map, table))
        updated = sql
        for prefix in prefixes:
            prefix_effective = table_effective if prefix == table else prefix
            pattern = re.compile(rf"\b{re.escape(prefix)}\.{re.escape(column)}\b", re.IGNORECASE)
            replacement = (
                f"CONCAT({prefix_effective}.{col_a}, '{delimiter}', {prefix_effective}.{col_b})"
            )
            updated = pattern.sub(replacement, updated)
        rewritten.append(updated)
    return rewritten


def _rewrite_table_split(
    sql_list: List[str],
    action: Dict[str, Any],
    schema_state: Dict[str, Any],
    table_map: Dict[str, str],
    split_map: Dict[str, Tuple[str, str, List[str], List[str], bool]],
) -> List[str]:
    source = str(action.get("source_table"))
    new_tables = action.get("new_tables", [])
    column_map = action.get("column_map", [])
    keep_original = bool(action.get("keep_original", True))
    if not source or not isinstance(new_tables, list) or len(new_tables) < 2:
        return sql_list

    cols_a, cols_b = _resolve_column_map(column_map, new_tables)
    if not cols_b:
        cols_b = [
            col
            for col in schema_state.get("tables", {}).get(source, {}).get("columns", {}).keys()
            if col not in cols_a
        ]

    split_map[source] = (str(new_tables[0]), str(new_tables[1]), cols_a, cols_b, keep_original)

    rewritten = []
    for sql in sql_list:
        rewritten.append(_rewrite_table_split_sql(sql, source, split_map, schema_state))
    return rewritten


def _rewrite_table_split_sql(
    sql: str,
    source: str,
    split_map: Dict[str, Tuple[str, str, List[str], List[str], bool]],
    schema_state: Dict[str, Any],
) -> str:
    if source not in split_map:
        return sql
    new_a, new_b, cols_a, cols_b, keep_original = split_map[source]
    cols_used = _columns_used(sql, source)
    if cols_used and all(col in cols_a for col in cols_used) and not any(col in cols_b for col in cols_used):
        return _replace_table(sql, source, new_a)
    if cols_used and all(col in cols_b for col in cols_used) and not any(col in cols_a for col in cols_used):
        return _replace_table(sql, source, new_b)

    if keep_original:
        return sql

    pk_cols = schema_state.get("tables", {}).get(source, {}).get("primary_key", [])
    if not pk_cols:
        return sql
    join = " AND ".join([f"`{new_a}`.`{col}` = `{new_b}`.`{col}`" for col in pk_cols])
    alias = _alias_for_table(sql, source) or source
    join_expr = f"({new_a} JOIN {new_b} ON {join}) AS {alias}"
    return _replace_table(sql, source, join_expr)


def _rewrite_table_join(
    sql_list: List[str],
    action: Dict[str, Any],
    schema_state: Dict[str, Any],
    table_map: Dict[str, str],
    join_map: Dict[Tuple[str, str], Dict[str, str]],
) -> List[str]:
    left = str(action.get("left_table"))
    right = str(action.get("right_table"))
    new_table = str(action.get("new_table"))
    if not left or not right or not new_table:
        return sql_list

    join_pairs = _normalize_join_keys(action.get("join_keys", []), left, right)
    left_cols = [left_col for left_col, _ in join_pairs] if join_pairs else list(
        schema_state.get("tables", {}).get(left, {}).get("columns", {}).keys()
    )
    right_cols = [right_col for _, right_col in join_pairs] if join_pairs else list(
        schema_state.get("tables", {}).get(right, {}).get("columns", {}).keys()
    )
    mapping_right = {col: col for col in right_cols}
    for col in right_cols:
        if col in left_cols:
            mapping_right[col] = f"{right}__{col}"
    join_map[(left, right)] = mapping_right

    rewritten = []
    for sql in sql_list:
        updated = sql
        can_rewrite, _ = _can_rewrite_join(updated, left, right)
        if can_rewrite:
            alias_map = _extract_aliases(updated)
            left_aliases = _aliases_for_table(alias_map, left)
            right_aliases = _aliases_for_table(alias_map, right)
            updated = _replace_table(updated, left, new_table)
            updated = _replace_table(updated, right, new_table)
            updated = _replace_column_prefix(updated, left, new_table, None, left_aliases)
            updated = _replace_column_prefix(updated, right, new_table, mapping_right, right_aliases)
            updated = _drop_join_clause(updated, right)
        rewritten.append(updated)
    return rewritten


def _rewrite_horizontal_split(
    sql_list: List[str],
    action: Dict[str, Any],
    table_map: Dict[str, str],
) -> List[str]:
    table = str(action.get("table"))
    table_true = str(action.get("table_true"))
    table_false = str(action.get("table_false"))
    predicate = str(action.get("predicate"))
    keep_original = bool(action.get("keep_original", True))
    if not table or not table_true or not table_false or not predicate:
        return sql_list

    table_map[table] = table
    rewritten = []
    for sql in sql_list:
        if not _contains_table(sql, table):
            rewritten.append(sql)
            continue
        if _horizontal_split_predicate_hit(sql, predicate):
            rewritten.append(_replace_table(sql, table, table_true))
            continue
        if keep_original:
            rewritten.append(sql)
            continue
        alias = _alias_for_table(sql, table) or table
        union_expr = f"({table_true} UNION ALL {table_false}) AS {alias}"
        rewritten.append(_replace_table(sql, table, union_expr))
    return rewritten


def _rewrite_horizontal_merge(
    sql_list: List[str],
    action: Dict[str, Any],
    table_map: Dict[str, str],
) -> List[str]:
    table_a = str(action.get("table_a"))
    table_b = str(action.get("table_b"))
    new_table = str(action.get("new_table"))
    keep_original = bool(action.get("keep_original", True))
    if not table_a or not table_b or not new_table:
        return sql_list

    rewritten = []
    for sql in sql_list:
        has_a = _contains_table(sql, table_a)
        has_b = _contains_table(sql, table_b)
        if keep_original and not (has_a and has_b):
            rewritten.append(sql)
            continue
        table_map[table_a] = new_table
        table_map[table_b] = new_table
        updated = _replace_table(sql, table_a, new_table)
        updated = _replace_table(updated, table_b, new_table)
        rewritten.append(updated)
    return rewritten


def _rewrite_redundant_add(
    sql_list: List[str],
    action: Dict[str, Any],
    redundant_map: Dict[Tuple[str, str], Tuple[str, str, List[str]]],
) -> List[str]:
    src_table = str(action.get("src_table"))
    dst_table = str(action.get("dst_table"))
    src_column = str(action.get("src_column"))
    dst_column = str(action.get("dst_column"))
    join_keys = action.get("join_keys", []) if isinstance(action.get("join_keys"), list) else []
    if not src_table or not dst_table or not src_column or not dst_column:
        return sql_list

    join_pairs = _normalize_join_keys(join_keys, src_table, dst_table)
    src_join_cols = {src_col for src_col, _ in join_pairs}
    allowed_src_cols = {src_column}
    allowed_src_cols.update(src_join_cols)

    redundant_map[(dst_table, dst_column)] = (src_table, src_column, join_keys)
    rewritten = []
    for sql in sql_list:
        if not (_contains_table(sql, src_table) and _contains_table(sql, dst_table)):
            rewritten.append(sql)
            continue
        used_cols = set(_columns_used(sql, src_table))
        if src_column not in used_cols:
            rewritten.append(sql)
            continue
        if used_cols and not used_cols.issubset(allowed_src_cols):
            rewritten.append(sql)
            continue
        if not _has_join_between_sql(sql, src_table, dst_table):
            rewritten.append(sql)
            continue
        alias_map = _extract_aliases(sql)
        dst_aliases = _aliases_for_table(alias_map, dst_table)
        dst_prefix = dst_aliases[0] if dst_aliases else dst_table
        updated = _replace_column_refs(sql, src_table, src_column, dst_prefix, dst_column)
        for src_join_col, dst_join_col in join_pairs:
            updated = _replace_column_refs(updated, src_table, src_join_col, dst_prefix, dst_join_col)
        updated = _drop_join_clause(updated, src_table)
        rewritten.append(updated)
    return rewritten


def _rewrite_redundant_drop(
    sql_list: List[str],
    action: Dict[str, Any],
    redundant_map: Dict[Tuple[str, str], Tuple[str, str, List[str]]],
) -> List[str]:
    table = str(action.get("table"))
    column = str(action.get("column"))
    mapping = redundant_map.get((table, column))
    if not table or not column or not mapping:
        return sql_list

    src_table, src_column, _ = mapping
    rewritten = []
    for sql in sql_list:
        alias_map = _extract_aliases(sql)
        src_aliases = _aliases_for_table(alias_map, src_table)
        replacement_prefix = src_aliases[0] if src_aliases else src_table
        updated = _replace_column_refs(sql, table, column, replacement_prefix, src_column)
        rewritten.append(updated)
    return rewritten


def _contains_table(sql: str, table: str) -> bool:
    tree = _parse_sql_ast(sql)
    target = _normalize_ident(table)
    if tree is not None and exp is not None and target:
        for node in tree.find_all(exp.Table):
            if target in _table_node_names(node):
                return True
        return False

    alias_map = _extract_aliases(sql)
    names = {table}
    names.update(_aliases_for_table(alias_map, table))
    for name in names:
        if re.search(rf"\b{re.escape(name)}\b", sql, flags=re.IGNORECASE):
            return True
    return False


def _replace_table(sql: str, table: str, replacement: str) -> str:
    if _is_simple_identifier(replacement):
        tree = _parse_sql_ast(sql)
        target = _normalize_ident(table)
        if tree is not None and exp is not None and target:
            def _transform(node: Any) -> Any:
                if isinstance(node, exp.Table) and target in _table_node_names(node):
                    new_table = exp.Table(this=exp.to_identifier(replacement))
                    alias = node.args.get("alias")
                    if alias is not None:
                        new_table.set("alias", alias.copy() if hasattr(alias, "copy") else alias)
                    return new_table
                return node

            updated = tree.transform(_transform)
            return _ast_to_sql(updated, sql)

    return re.sub(rf"\b{re.escape(table)}\b", replacement, sql, flags=re.IGNORECASE)


def _replace_column_prefix(
    sql: str,
    old_table: str,
    new_table: str,
    mapping: Dict[str, str] | None,
    aliases: List[str] | None = None,
) -> str:
    prefixes = [old_table]
    if aliases:
        prefixes.extend([alias for alias in aliases if alias and alias not in prefixes])

    tree = _parse_sql_ast(sql)
    if tree is not None and exp is not None:
        prefix_set = {_normalize_ident(prefix) for prefix in prefixes if prefix}
        mapping_lower = {_normalize_ident(key): str(value) for key, value in (mapping or {}).items()}

        def _transform(node: Any) -> Any:
            if not isinstance(node, exp.Column):
                return node
            qualifier = _normalize_ident(getattr(node, "table", ""))
            if not qualifier or qualifier not in prefix_set:
                return node
            column_name = str(getattr(node, "name", ""))
            if mapping_lower:
                mapped = mapping_lower.get(_normalize_ident(column_name))
                if not mapped:
                    return node
                return exp.column(mapped, table=new_table)
            return exp.column(column_name, table=new_table)

        updated = tree.transform(_transform)
        return _ast_to_sql(updated, sql)

    if mapping:
        for prefix in prefixes:
            for col, mapped in mapping.items():
                sql = re.sub(
                    rf"\b{re.escape(prefix)}\.{re.escape(col)}\b",
                    f"{new_table}.{mapped}",
                    sql,
                    flags=re.IGNORECASE,
                )
        return sql
    for prefix in prefixes:
        sql = re.sub(
            rf"\b{re.escape(prefix)}\.",
            f"{new_table}.",
            sql,
            flags=re.IGNORECASE,
        )
    return sql


def _drop_join_clause(sql: str, table: str) -> str:
    tree = _parse_sql_ast(sql)
    target = _normalize_ident(table)
    if tree is not None and exp is not None and target:
        def _transform(node: Any) -> Any:
            if not isinstance(node, exp.Select):
                return node
            joins = node.args.get("joins") or []
            filtered: List[Any] = []
            for join in joins:
                join_table = join.args.get("this") if hasattr(join, "args") else None
                if isinstance(join_table, exp.Table) and target in _table_node_names(join_table):
                    continue
                filtered.append(join)
            node.set("joins", filtered)
            return node

        updated = tree.transform(_transform)
        return _ast_to_sql(updated, sql)

    join_pattern = re.compile(
        rf"\bJOIN\s+{re.escape(table)}\b[^\n]*?(\bON\b[^\n]*?)?(?=(\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|$))",
        re.IGNORECASE | re.DOTALL,
    )
    return join_pattern.sub(" ", sql)


def _columns_used(sql: str, table: str) -> List[str]:
    tree = _parse_sql_ast(sql)
    target = _normalize_ident(table)
    if tree is not None and exp is not None and target:
        alias_map = _table_alias_map_ast(tree)
        cols = set()
        for col in tree.find_all(exp.Column):
            qualifier = _normalize_ident(getattr(col, "table", ""))
            if not qualifier:
                continue
            mapped = alias_map.get(qualifier, qualifier)
            if mapped == target:
                col_name = str(getattr(col, "name", ""))
                if col_name:
                    cols.add(col_name)
        return list(cols)

    alias_map = _extract_aliases(sql)
    pattern = re.compile(r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?")
    cols = set()
    for raw_table, raw_column in pattern.findall(sql):
        mapped = alias_map.get(raw_table, raw_table)
        if mapped == table:
            cols.add(raw_column)
    return list(cols)


def _replace_column_refs(
    sql: str,
    table: str,
    column: str,
    new_prefix: str,
    new_column: str,
) -> str:
    tree = _parse_sql_ast(sql)
    if tree is not None and exp is not None:
        alias_map = _table_alias_map_ast(tree)
        target = _normalize_ident(table)
        prefixes = {target}
        for alias, mapped in alias_map.items():
            if mapped == target:
                prefixes.add(alias)

        def _transform(node: Any) -> Any:
            if not isinstance(node, exp.Column):
                return node
            qualifier = _normalize_ident(getattr(node, "table", ""))
            col_name = _normalize_ident(getattr(node, "name", ""))
            if qualifier in prefixes and col_name == _normalize_ident(column):
                return exp.column(new_column, table=new_prefix)
            return node

        updated = tree.transform(_transform)
        return _ast_to_sql(updated, sql)

    alias_map = _extract_aliases(sql)
    prefixes = [table]
    prefixes.extend(_aliases_for_table(alias_map, table))
    for prefix in prefixes:
        sql = re.sub(
            rf"\b{re.escape(prefix)}\.{re.escape(column)}\b",
            f"{new_prefix}.{new_column}",
            sql,
            flags=re.IGNORECASE,
        )
    return sql


def _has_join_predicate(sql: str, left: str, right: str) -> bool:
    if not left or not right:
        return False

    tree = _parse_sql_ast(sql)
    if tree is not None and exp is not None:
        alias_map = _table_alias_map_ast(tree)
        target_left = _normalize_ident(left)
        target_right = _normalize_ident(right)
        for eq_expr in tree.find_all(exp.EQ):
            lhs = eq_expr.args.get("this")
            rhs = eq_expr.args.get("expression")
            if not isinstance(lhs, exp.Column) or not isinstance(rhs, exp.Column):
                continue
            left_table = alias_map.get(_normalize_ident(getattr(lhs, "table", "")), _normalize_ident(getattr(lhs, "table", "")))
            right_table = alias_map.get(_normalize_ident(getattr(rhs, "table", "")), _normalize_ident(getattr(rhs, "table", "")))
            if {left_table, right_table} == {target_left, target_right}:
                return True
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


def _join_type_between(sql: str, left: str, right: str) -> str:
    if not left or not right:
        return ""

    tree = _parse_sql_ast(sql)
    if tree is not None and exp is not None:
        alias_map = _table_alias_map_ast(tree)
        targets = {_normalize_ident(left), _normalize_ident(right)}
        for join in tree.find_all(exp.Join):
            join_table = join.args.get("this") if hasattr(join, "args") else None
            if isinstance(join_table, exp.Table):
                names = _table_node_names(join_table)
                resolved = {alias_map.get(name, name) for name in names}
                if not (resolved & targets):
                    continue
                side = _join_arg_text(join.args.get("side"))
                kind = _join_arg_text(join.args.get("kind"))
                method = _join_arg_text(join.args.get("method"))
                if side:
                    return side
                if kind:
                    return kind
                if method:
                    return method
                return "inner"
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


def _has_join_between_sql(sql: str, left: str, right: str) -> bool:
    if not (_contains_table(sql, left) and _contains_table(sql, right)):
        return False
    if _join_type_between(sql, left, right):
        return True
    return _has_join_predicate(sql, left, right)


def _normalize_join_keys(join_keys: Any, left: str, right: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    if not isinstance(join_keys, list):
        return pairs
    for raw in join_keys:
        if not isinstance(raw, str) or "=" not in raw:
            continue
        left_key, right_key = [item.strip() for item in raw.split("=", 1)]
        left_table, left_col = _parse_join_key(left_key)
        right_table, right_col = _parse_join_key(right_key)
        if not left_col or not right_col:
            continue
        if left_table.lower() == left.lower() and right_table.lower() == right.lower():
            pairs.append((left_col, right_col))
        elif left_table.lower() == right.lower() and right_table.lower() == left.lower():
            pairs.append((right_col, left_col))
    return pairs


def _parse_join_key(raw: str) -> Tuple[str, str]:
    if "." in raw:
        table, column = raw.split(".", 1)
        return table.strip("`\" '"), column.strip("`\" '")
    return "", raw.strip("`\" '")


def _can_rewrite_join(sql: str, left: str, right: str) -> Tuple[bool, str]:
    if not (_contains_table(sql, left) and _contains_table(sql, right)):
        return False, ""
    join_type = _join_type_between(sql, left, right)
    if not join_type and _has_join_predicate(sql, left, right):
        join_type = "inner"
    if not join_type:
        return False, ""
    if join_type in {"left", "right", "outer", "full"}:
        return False, join_type
    return True, join_type


def _resolve_column_map(column_map: Any, new_tables: List[Any]) -> Tuple[List[str], List[str]]:
    if isinstance(column_map, list):
        return [str(col) for col in column_map], []
    if isinstance(column_map, dict):
        keys = [str(name) for name in new_tables if str(name) in column_map]
        if len(keys) < 2:
            keys = [str(key) for key in column_map.keys()]
        if len(keys) >= 2:
            cols_a = column_map.get(keys[0], [])
            cols_b = column_map.get(keys[1], [])
            if isinstance(cols_a, list) and isinstance(cols_b, list):
                return [str(col) for col in cols_a], [str(col) for col in cols_b]
    return [], []
