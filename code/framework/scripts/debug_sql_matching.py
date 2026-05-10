#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Set, Tuple
import re
import json

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

from schema_tuning.rewriter import sql_rewriter as rewriter
from schema_tuning.evaluators import performance as perf


def _split_statements(sql_text: str) -> List[str]:
    statements: List[str] = []
    buffer: List[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    escape = False
    depth = 0

    for char in sql_text:
        if escape:
            buffer.append(char)
            escape = False
            continue
        if char == "\\":
            buffer.append(char)
            escape = True
            continue
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
        if char == ";" and not in_single and not in_double and not in_backtick and depth == 0:
            stmt = "".join(buffer).strip()
            if stmt:
                statements.append(stmt)
            buffer = []
            continue
        buffer.append(char)

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def _from_clause_tables(sql: str) -> Set[str]:
    tables: Set[str] = set()
    items = rewriter._split_from_clause_items(sql)
    for item in items:
        item = item.strip()
        if not item:
            continue
        if item.startswith("("):
            continue
        table, _ = rewriter._read_identifier(item, 0)
        if table:
            tables.add(table)
    return tables


def _column_ref_tables(sql: str) -> List[str]:
    pattern = re.compile(r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?")
    return [match[0] for match in pattern.findall(sql)]


def _load_schema_state(config: dict, schema_state_path: str | None) -> dict:
    if schema_state_path:
        path = Path(schema_state_path)
    else:
        path = Path("output/pipeline_summary.json")
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "schema_state" in payload:
        return payload.get("schema_state") or {}
    if isinstance(payload, dict) and "tables" in payload:
        return payload
    return {}


def _column_table_map(schema_state: dict) -> Dict[str, Set[str]]:
    mapping: Dict[str, Set[str]] = {}
    for table, info in (schema_state.get("tables") or {}).items():
        if not isinstance(info, dict):
            continue
        for column in (info.get("columns") or {}).keys():
            mapping.setdefault(str(column).lower(), set()).add(str(table))
    return mapping


def _pk_map(schema_state: dict) -> Dict[str, Set[str]]:
    mapping: Dict[str, Set[str]] = {}
    for table, info in (schema_state.get("tables") or {}).items():
        if not isinstance(info, dict):
            continue
        cols = [str(col).lower() for col in (info.get("primary_key") or [])]
        if cols:
            mapping[str(table)] = set(cols)
    return mapping


def _fk_pairs(schema_state: dict) -> Set[Tuple[str, str, str, str]]:
    pairs: Set[Tuple[str, str, str, str]] = set()
    for table, info in (schema_state.get("tables") or {}).items():
        if not isinstance(info, dict):
            continue
        for fk in (info.get("foreign_keys") or []):
            if not isinstance(fk, dict):
                continue
            from_value = str(fk.get("from") or "")
            to_value = str(fk.get("to") or "")
            if not from_value or not to_value:
                continue
            if "." not in from_value or "." not in to_value:
                continue
            from_table, from_col = from_value.split(".", 1)
            to_table, to_col = to_value.split(".", 1)
            pairs.add((from_table, from_col.lower(), to_table, to_col.lower()))
    return pairs


def _extract_view_column_map(
    statements: List[str],
    column_map: Dict[str, Set[str]],
) -> Dict[str, Dict[str, str]]:
    view_map: Dict[str, Dict[str, str]] = {}
    pattern = re.compile(
        r"\bcreate\s+view\s+([A-Za-z0-9_]+)\s*(\([^)]*\))?\s+as\s+(select\b.*)",
        re.IGNORECASE | re.DOTALL,
    )
    for stmt in statements:
        match = pattern.search(stmt)
        if not match:
            continue
        view_name = match.group(1)
        col_list = match.group(2) or ""
        select_sql = match.group(3)
        columns = _parse_column_list(col_list)
        if not columns:
            columns = _derive_select_output_names(select_sql)
        if not columns:
            continue
        view_map[view_name] = _derive_column_sources(select_sql, columns, column_map)
    return view_map


def _extract_cte_column_map(
    sql: str,
    column_map: Dict[str, Set[str]],
) -> Dict[str, Dict[str, str]]:
    cte_map: Dict[str, Dict[str, str]] = {}
    match = re.search(r"\bwith\b", sql, re.IGNORECASE)
    if not match:
        return cte_map
    idx = match.end()
    while idx < len(sql):
        idx = rewriter._skip_ws(sql, idx)
        name, idx = rewriter._read_identifier(sql, idx)
        if not name:
            break
        idx = rewriter._skip_ws(sql, idx)
        col_end = -1
        cols: List[str] = []
        if idx < len(sql) and sql[idx] == "(":
            col_end = rewriter._find_matching_paren(sql, idx)
            if col_end == -1:
                break
            cols = _parse_column_list(sql[idx : col_end + 1])
            idx = col_end + 1
        idx = rewriter._skip_ws(sql, idx)
        if not sql[idx : idx + 2].lower() == "as":
            break
        idx += 2
        idx = rewriter._skip_ws(sql, idx)
        if idx >= len(sql) or sql[idx] != "(":
            break
        end = rewriter._find_matching_paren(sql, idx)
        if end == -1:
            break
        body = sql[idx + 1 : end]
        if not cols:
            cols = _derive_select_output_names(body)
        if cols:
            cte_map[name] = _derive_column_sources(body, cols, column_map)
        idx = end + 1
        idx = rewriter._skip_ws(sql, idx)
        if idx < len(sql) and sql[idx] == ",":
            idx += 1
            continue
        break
    return cte_map


def _collect_nested_tables(sql: str) -> Set[str]:
    tables: Set[str] = set()
    for body in rewriter._extract_subquery_bodies(sql):
        tables.update(rewriter._extract_top_level_tables(body, {}))
        tables.update(_from_clause_tables(body))
        tables.update(_collect_nested_tables(body))
    return tables


def _parse_column_list(raw: str) -> List[str]:
    if not raw:
        return []
    raw = raw.strip()
    if raw.startswith("(") and raw.endswith(")"):
        raw = raw[1:-1]
    return [item.strip().strip("`") for item in raw.split(",") if item.strip()]


def _derive_select_output_names(select_sql: str) -> List[str]:
    items = _select_list(select_sql)
    output: List[str] = []
    for item in items:
        name = _extract_output_name(item)
        if name:
            output.append(name)
    return output


def _extract_output_name(expr: str) -> str:
    match = re.search(r"\bas\s+`?([A-Za-z0-9_]+)`?\s*$", expr, re.IGNORECASE)
    if match:
        return match.group(1)
    expr = expr.strip()
    if not expr:
        return ""
    tokens = re.split(r"\s+", expr)
    if len(tokens) >= 2:
        tail = tokens[-1]
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", tail):
            return tail
    if "." in expr:
        return expr.split(".")[-1].strip("`")
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", expr):
        return expr
    return ""


def _select_list(sql: str) -> List[str]:
    select_pos = _find_top_level_keyword(sql, "select")
    if select_pos == -1:
        return []
    idx = select_pos + len("select")
    from_pos = _find_top_level_keyword(sql, "from", start=idx)
    if from_pos == -1:
        return []
    segment = sql[idx:from_pos]
    return _split_top_level(segment)


def _split_top_level(segment: str) -> List[str]:
    items: List[str] = []
    buffer: List[str] = []
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False
    for char in segment:
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
                continue
        buffer.append(char)
    tail = "".join(buffer).strip()
    if tail:
        items.append(tail)
    return items


def _find_top_level_keyword(sql: str, keyword: str, start: int = 0) -> int:
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
            if depth == 0 and rewriter._matches_keyword(sql, idx, keyword):
                return idx
        idx += 1
    return -1


def _derive_column_sources(
    select_sql: str,
    columns: List[str],
    column_map: Dict[str, Set[str]],
) -> Dict[str, str]:
    alias_map = rewriter._extract_aliases(select_sql)
    tables_in_query = set(rewriter._extract_top_level_tables(select_sql, {}))
    tables_in_query |= _from_clause_tables(select_sql)
    items = _select_list(select_sql)
    if not items:
        return {}
    result: Dict[str, str] = {}
    for idx, name in enumerate(columns):
        if idx >= len(items):
            break
        expr = items[idx]
        sources = _expression_sources(expr, alias_map, column_map, tables_in_query)
        if len(sources) == 1:
            result[name] = next(iter(sources))
    return result


def _expression_sources(
    expr: str,
    alias_map: Dict[str, str],
    column_map: Dict[str, Set[str]],
    tables_in_query: Set[str],
) -> Set[str]:
    sources: Set[str] = set()
    pattern = re.compile(r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?")
    for table, _ in pattern.findall(expr):
        mapped = alias_map.get(table, table)
        sources.add(mapped)
    if sources:
        return sources
    candidates: Set[str] = set()
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr)
    for token in tokens:
        if token.lower() in {"case", "when", "then", "else", "end", "sum", "avg", "min", "max", "count", "distinct"}:
            continue
        tables = column_map.get(token.lower(), set()) & tables_in_query
        if tables:
            if not candidates:
                candidates = set(tables)
            else:
                candidates &= set(tables)
    return candidates
    return sources


def _qualified_join_pairs(sql: str, alias_map: Dict[str, str]) -> Set[frozenset]:
    pairs: Set[frozenset] = set()
    pattern = re.compile(
        r"`?([A-Za-z_][A-Za-z0-9_]*)`?\.(`?[A-Za-z_][A-Za-z0-9_]*`?)\s*=\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\.(`?[A-Za-z_][A-Za-z0-9_]*`?)",
        re.IGNORECASE,
    )
    for left_table, _, right_table, _ in pattern.findall(sql):
        mapped_left = alias_map.get(left_table, left_table)
        mapped_right = alias_map.get(right_table, right_table)
        if mapped_left and mapped_right:
            pairs.add(frozenset((mapped_left, mapped_right)))
    return pairs


def _unqualified_join_predicates(sql: str) -> List[Tuple[str, str]]:
    pattern = re.compile(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\b\s*=\s*\b([A-Za-z_][A-Za-z0-9_]*)\b",
        re.IGNORECASE,
    )
    results: List[Tuple[str, str]] = []
    for left_col, right_col in pattern.findall(sql):
        if left_col.lower() == right_col.lower():
            continue
        if left_col.lower() in {"and", "or", "on", "where"}:
            continue
        if right_col.lower() in {"and", "or", "on", "where"}:
            continue
        results.append((left_col, right_col))
    return results


def _load_config(path: Path) -> dict:
    if yaml is None:
        raise RuntimeError("PyYAML is required to load config files.")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug SQL alias/join matching.")
    parser.add_argument(
        "--config",
        default="framework/configs/default.yaml",
        help="Path to config yaml.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit statements per file (0 for all).",
    )
    parser.add_argument(
        "--pattern",
        default="*.sql",
        help="Glob pattern under sql_dir to include.",
    )
    parser.add_argument(
        "--files",
        default="",
        help="Comma-separated list of SQL filenames to include.",
    )
    parser.add_argument(
        "--report",
        default="./output/sql_match_report.json",
        help="Path to write JSON summary report.",
    )
    parser.add_argument(
        "--schema-state",
        default="",
        help="Path to schema_state.json or pipeline_summary.json for column inference.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    config = _load_config(config_path)
    sql_dir = Path(str(config.get("workload", {}).get("sql_dir", "")))
    if not sql_dir.exists():
        raise RuntimeError(f"SQL directory not found: {sql_dir}")

    files = sorted(sql_dir.glob(args.pattern))
    if args.files:
        allowed = {name.strip() for name in args.files.split(",") if name.strip()}
        files = [path for path in files if path.name in allowed]
    if not files:
        raise RuntimeError(f"No SQL files in: {sql_dir}")

    schema_state = _load_schema_state(config, args.schema_state or None)
    column_map = _column_table_map(schema_state)
    pk_map = _pk_map(schema_state)
    fk_pairs = _fk_pairs(schema_state)

    report: Dict[str, object] = {
        "files": [],
        "summary": {
            "total_files": 0,
            "total_statements": 0,
            "alias_unresolved": 0,
            "join_miss": 0,
            "join_false_positive": 0,
            "join_ambiguous": 0,
            "join_inferred": 0,
        },
        "examples": {
            "alias_unresolved": [],
            "join_miss": [],
            "join_false_positive": [],
            "join_ambiguous": [],
            "join_inferred": [],
        },
    }
    max_examples = 20

    for path in files:
        sql_text = path.read_text(encoding="utf-8")
        statements = _split_statements(sql_text)
        if args.limit > 0:
            statements = statements[: args.limit]
        view_defs = _extract_view_column_map(statements, column_map)
        file_entry = {
            "file": path.name,
            "statements": len(statements),
            "alias_unresolved": 0,
            "join_miss": 0,
            "join_false_positive": 0,
            "join_ambiguous": 0,
            "join_inferred": 0,
        }
        print(f"\n== {path.name} ({len(statements)} statements) ==")
        for idx, stmt in enumerate(statements, 1):
            alias_map = rewriter._extract_aliases(stmt)
            cte_defs = _extract_cte_column_map(stmt, column_map)
            join_pairs = perf._extract_join_pairs(stmt)
            raw_tables = set(rewriter._extract_top_level_tables(stmt, {}))
            raw_tables |= _from_clause_tables(stmt)
            raw_tables |= _collect_nested_tables(stmt)
            raw_tables |= set(alias_map.values())
            tables = sorted({table.upper() for table in raw_tables if table})
            derived_column_map: Dict[str, Set[str]] = {}
            for view_name, columns in view_defs.items():
                for column in columns.keys():
                    derived_column_map.setdefault(column.lower(), set()).add(view_name.upper())
            for cte_name, columns in cte_defs.items():
                for column in columns.keys():
                    derived_column_map.setdefault(column.lower(), set()).add(cte_name.upper())
            print(f"-- stmt {idx}")
            print(f"tables: {tables}")
            print(f"aliases: {alias_map}")
            print(f"join_pairs: {join_pairs}")

            unresolved = []
            for raw_table in _column_ref_tables(stmt):
                if raw_table in alias_map:
                    continue
                if raw_table.upper() in tables:
                    continue
                unresolved.append(raw_table)
            if unresolved:
                file_entry["alias_unresolved"] += 1
                report["summary"]["alias_unresolved"] += 1
                if len(report["examples"]["alias_unresolved"]) < max_examples:
                    report["examples"]["alias_unresolved"].append(
                        {
                            "file": path.name,
                            "stmt_index": idx,
                            "unresolved": sorted(set(unresolved)),
                            "sql": stmt.strip()[:4000],
                        }
                    )

            join_pairs_set = {
                frozenset((left, right))
                for left, _, right, _ in join_pairs
            }
            qualified_pairs = _qualified_join_pairs(stmt, alias_map)
            miss_pairs = [
                tuple(pair)
                for pair in qualified_pairs
                if pair not in join_pairs_set and len(pair) == 2
            ]
            false_pairs: List[Tuple[str, str]] = []
            for pair in join_pairs_set:
                pair_list = list(pair)
                if len(pair_list) != 2:
                    continue
                left, right = pair_list
                if not rewriter._has_join_predicate(stmt, left, right):
                    false_pairs.append((left, right))

            if miss_pairs:
                file_entry["join_miss"] += 1
                report["summary"]["join_miss"] += 1
                if len(report["examples"]["join_miss"]) < max_examples:
                    report["examples"]["join_miss"].append(
                        {
                            "file": path.name,
                            "stmt_index": idx,
                            "pairs": miss_pairs,
                            "sql": stmt.strip()[:4000],
                        }
                    )
            if false_pairs:
                file_entry["join_false_positive"] += 1
                report["summary"]["join_false_positive"] += 1
                if len(report["examples"]["join_false_positive"]) < max_examples:
                    report["examples"]["join_false_positive"].append(
                        {
                            "file": path.name,
                            "stmt_index": idx,
                            "pairs": false_pairs,
                            "sql": stmt.strip()[:4000],
                        }
                    )

            unqualified = _unqualified_join_predicates(stmt)
            inferred_pairs: Set[Tuple[str, str]] = set()
            unresolved_preds: List[Tuple[str, str]] = []
            if unqualified and column_map:
                tables_in_query = set(tables)
                for left_col, right_col in unqualified:
                    left_tables = (
                        column_map.get(left_col.lower(), set())
                        | derived_column_map.get(left_col.lower(), set())
                    ) & tables_in_query
                    right_tables = (
                        column_map.get(right_col.lower(), set())
                        | derived_column_map.get(right_col.lower(), set())
                    ) & tables_in_query
                    pairs = {
                        (left_table, right_table)
                        for left_table in left_tables
                        for right_table in right_tables
                        if left_table != right_table
                    }
                    if fk_pairs and pairs:
                        fk_filtered = {
                            (left_table, right_table)
                            for left_table, right_table in pairs
                            if (left_table, left_col.lower(), right_table, right_col.lower()) in fk_pairs
                            or (right_table, right_col.lower(), left_table, left_col.lower()) in fk_pairs
                        }
                        if fk_filtered:
                            pairs = fk_filtered
                    if len(pairs) == 1:
                        inferred_pairs.update(pairs)
                    else:
                        unresolved_preds.append((left_col, right_col))
            else:
                unresolved_preds = list(unqualified)

            if inferred_pairs:
                file_entry["join_inferred"] += 1
                report["summary"]["join_inferred"] += 1
                if len(report["examples"]["join_inferred"]) < max_examples:
                    report["examples"]["join_inferred"].append(
                        {
                            "file": path.name,
                            "stmt_index": idx,
                            "pairs": sorted({tuple(pair) for pair in inferred_pairs}),
                            "sql": stmt.strip()[:4000],
                        }
                    )

            if unresolved_preds and not join_pairs_set and not qualified_pairs:
                file_entry["join_ambiguous"] += 1
                report["summary"]["join_ambiguous"] += 1
                if len(report["examples"]["join_ambiguous"]) < max_examples:
                    report["examples"]["join_ambiguous"].append(
                        {
                            "file": path.name,
                            "stmt_index": idx,
                            "predicates": unresolved_preds[:10],
                            "sql": stmt.strip()[:4000],
                        }
                    )

        report["files"].append(file_entry)
        report["summary"]["total_files"] += 1
        report["summary"]["total_statements"] += len(statements)

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(f"\nReport written to: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
