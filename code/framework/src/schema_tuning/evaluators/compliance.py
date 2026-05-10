from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


ALLOWED_ACTION_TYPES = {
    "ColumnSplit",
    "TableSplit",
    "TableJoin",
    "HorizontalSplit",
    "HorizontalMerge",
    "RedundantColumnAdd",
    "RedundantColumnDrop",
}


def _table_exists(state: Dict[str, Set[str]], table_name: str) -> bool:
    return table_name in state


def _column_exists(state: Dict[str, Set[str]], table_name: str, column: str) -> bool:
    return table_name in state and column in state[table_name]


def _snapshot_schema(schema_state: Dict[str, Any]) -> Dict[str, Set[str]]:
    tables = schema_state.get("tables", {})
    if not isinstance(tables, dict):
        return {}
    snapshot: Dict[str, Set[str]] = {}
    for table, info in tables.items():
        if not isinstance(info, dict):
            continue
        columns = info.get("columns", {})
        if isinstance(columns, dict):
            snapshot[table] = set(columns.keys())
        else:
            snapshot[table] = set()
    return snapshot


def _primary_keys(schema_state: Dict[str, Any]) -> Dict[str, List[str]]:
    tables = schema_state.get("tables", {})
    if not isinstance(tables, dict):
        return {}
    result: Dict[str, List[str]] = {}
    for table, info in tables.items():
        if not isinstance(info, dict):
            continue
        pk = info.get("primary_key", [])
        if isinstance(pk, list):
            result[table] = [str(col) for col in pk]
        else:
            result[table] = []
    return result


def _foreign_key_pairs(schema_state: Dict[str, Any]) -> Set[tuple[str, str, str, str]]:
    relations = schema_state.get("relations", [])
    pairs: Set[tuple[str, str, str, str]] = set()
    if not isinstance(relations, list):
        return pairs
    for rel in relations:
        if not isinstance(rel, dict):
            continue
        left = rel.get("from")
        right = rel.get("to")
        if not left or not right:
            continue
        if "." not in left or "." not in right:
            continue
        left_table, left_col = left.split(".", 1)
        right_table, right_col = right.split(".", 1)
        pairs.add((left_table, left_col, right_table, right_col))
        pairs.add((right_table, right_col, left_table, left_col))
    return pairs


def _join_key_history(schema_state: Dict[str, Any]) -> Set[tuple[str, str, str, str]]:
    history = schema_state.get("join_key_history", [])
    pairs: Set[tuple[str, str, str, str]] = set()
    if not isinstance(history, list):
        return pairs
    for item in history:
        if not isinstance(item, dict):
            continue
        left_table = item.get("left_table")
        left_column = item.get("left_column")
        right_table = item.get("right_table")
        right_column = item.get("right_column")
        if not left_table or not left_column or not right_table or not right_column:
            continue
        pairs.add((left_table, left_column, right_table, right_column))
        pairs.add((right_table, right_column, left_table, left_column))
    return pairs


def _join_key_allowed(
    left_table: str,
    right_table: str,
    left_column: str,
    right_column: str,
    fk_pairs: Set[tuple[str, str, str, str]],
    history_pairs: Set[tuple[str, str, str, str]],
) -> bool:
    return (left_table, left_column, right_table, right_column) in fk_pairs or (
        left_table,
        left_column,
        right_table,
        right_column,
    ) in history_pairs


def _table_constraints(schema_state: Dict[str, Any], table: str) -> Dict[str, Any]:
    info = schema_state.get("tables", {}).get(table, {}) if isinstance(schema_state.get("tables", {}), dict) else {}
    unique = info.get("unique_constraints", []) if isinstance(info, dict) else []
    checks = info.get("check_constraints", []) if isinstance(info, dict) else []
    columns = info.get("columns", {}) if isinstance(info, dict) else {}
    defaults = []
    auto_inc = []
    if isinstance(columns, dict):
        for col_name, col_info in columns.items():
            if not isinstance(col_info, dict):
                continue
            if col_info.get("default") not in (None, ""):
                defaults.append(f"{table}.{col_name}")
            if col_info.get("auto_increment"):
                auto_inc.append(f"{table}.{col_name}")
    return {
        "unique": unique,
        "checks": checks,
        "defaults": defaults,
        "auto_increment": auto_inc,
    }


def _warn_constraint_loss(warnings: list[str], table: str, constraints: Dict[str, Any], reason: str) -> None:
    if constraints.get("unique"):
        warnings.append(f"constraint warning: unique constraints on '{table}' may be lost ({reason})")
    if constraints.get("checks"):
        warnings.append(f"constraint warning: check constraints on '{table}' may be lost ({reason})")
    if constraints.get("defaults"):
        warnings.append(f"constraint warning: defaults on '{table}' may be lost ({reason})")
    if constraints.get("auto_increment"):
        warnings.append(f"constraint warning: auto_increment on '{table}' may be lost ({reason})")


def _name_warning(warnings: list[str], name: str, suffixes: set[str], label: str) -> None:
    if not name:
        return
    if not any(str(name).endswith(suf) for suf in suffixes):
        warnings.append(f"naming warning: {label} '{name}' should end with {sorted(suffixes)}")


def _check_required(action: Dict[str, Any], required_keys: List[str], errors: list[str], idx: int) -> None:
    for key in required_keys:
        value = action.get(key)
        if value in (None, ""):
            errors.append(f"action[{idx}] missing required field '{key}'")


def _require_bool(action: Dict[str, Any], key: str, errors: list[str], idx: int) -> None:
    value = action.get(key)
    if not isinstance(value, bool):
        errors.append(f"action[{idx}] '{key}' must be true or false")


def _require_list(action: Dict[str, Any], key: str, errors: list[str], idx: int) -> List[Any]:
    value = action.get(key)
    if not isinstance(value, list) or not value:
        errors.append(f"action[{idx}] '{key}' must be a non-empty list")
        return []
    return value


def _add_hint(hints: list[str], message: str) -> None:
    if message and message not in hints:
        hints.append(message)


def _split_ref(ref: str) -> Tuple[str | None, str]:
    text = str(ref).strip()
    if "." in text:
        table_name, col_name = text.split(".", 1)
        return table_name, col_name
    return None, text


def _resolve_table_for_col(
    table_hint: str | None,
    column: str,
    left_table: str,
    right_table: str,
    state: Dict[str, Set[str]],
) -> str | None:
    if table_hint:
        return table_hint
    left_has = _column_exists(state, left_table, column)
    right_has = _column_exists(state, right_table, column)
    if left_has and not right_has:
        return left_table
    if right_has and not left_has:
        return right_table
    if left_has and right_has:
        return left_table
    return None


def _join_key_suggestions(
    left_table: str,
    right_table: str,
    fk_pairs: Set[tuple[str, str, str, str]],
    history_pairs: Set[tuple[str, str, str, str]],
) -> List[str]:
    suggestions: List[str] = []
    seen: Set[str] = set()
    for lt, lc, rt, rc in sorted(fk_pairs | history_pairs):
        if lt == left_table and rt == right_table:
            key = f"{lt}.{lc}={rt}.{rc}"
            if key not in seen:
                suggestions.append(key)
                seen.add(key)
        if len(suggestions) >= 3:
            break
    return suggestions


def _normalize_column_map(
    action: Dict[str, Any],
    new_tables: List[Any],
    errors: list[str],
    idx: int,
) -> tuple[list[str], list[str]]:
    raw = action.get("column_map")
    if isinstance(raw, list):
        return [str(col) for col in raw], []
    if isinstance(raw, dict):
        keys = []
        if isinstance(new_tables, list):
            keys = [str(name) for name in new_tables if str(name) in raw]
        if len(keys) < 2:
            keys = [str(key) for key in raw.keys()]
        if len(keys) < 2:
            errors.append(f"action[{idx}] column_map must map two tables")
            return [], []
        cols_a = raw.get(keys[0], [])
        cols_b = raw.get(keys[1], [])
        if not isinstance(cols_a, list) or not isinstance(cols_b, list):
            errors.append(f"action[{idx}] column_map entries must be lists")
            return [], []
        return [str(col) for col in cols_a], [str(col) for col in cols_b]
    errors.append(f"action[{idx}] column_map must be a list or object")
    return [], []


def _check_action(
    state: Dict[str, Set[str]],
    action: Dict[str, Any],
    idx: int,
    schema_state: Dict[str, Any],
    errors: list[str],
    warnings: list[str],
    hints: list[str],
    primary_keys: Dict[str, List[str]],
    fk_pairs: Set[tuple[str, str, str, str]],
    history_pairs: Set[tuple[str, str, str, str]],
    redundant_pairs: Set[tuple[str, str, str, str]],
    removed_tables: Dict[str, int],
) -> None:
    action_type = action.get("type")
    if action_type not in ALLOWED_ACTION_TYPES:
        errors.append(f"action[{idx}] has unsupported type '{action_type}'")
        return

    if action_type == "ColumnSplit":
        _check_required(action, ["table", "column", "delimiter", "new_columns", "keep_original"], errors, idx)
        table = action.get("table", "")
        column = action.get("column", "")
        new_columns = _require_list(action, "new_columns", errors, idx)
        _require_bool(action, "keep_original", errors, idx)
        if table and not _table_exists(state, table):
            errors.append(f"action[{idx}] table '{table}' does not exist")
        if column and table and not _column_exists(state, table, column):
            errors.append(f"action[{idx}] column '{table}.{column}' does not exist")
        if new_columns and len(new_columns) != 2:
            errors.append(f"action[{idx}] new_columns must contain exactly two items")
        if table and new_columns:
            for name in new_columns:
                if not isinstance(name, str) or not name:
                    errors.append(f"action[{idx}] new_columns contains invalid name")
                elif _column_exists(state, table, name):
                    errors.append(f"action[{idx}] new column '{table}.{name}' already exists")
                else:
                    _name_warning(warnings, name, {"_part1", "_part2"}, "derived column")
        if table and column:
            constraints = _table_constraints(schema_state, table)
            col_info = schema_state.get("tables", {}).get(table, {}).get("columns", {}).get(column, {})
            if isinstance(col_info, dict) and col_info.get("auto_increment"):
                errors.append(f"action[{idx}] cannot split auto_increment column '{table}.{column}'")
            if constraints.get("unique"):
                for item in constraints.get("unique"):
                    cols = item.get("columns", []) if isinstance(item, dict) else []
                    if column in cols:
                        errors.append(f"action[{idx}] cannot split unique column '{table}.{column}'")
            if constraints.get("checks"):
                warnings.append(f"constraint warning: check constraints on '{table}' may need rebuild for ColumnSplit")

    elif action_type == "TableSplit":
        _check_required(action, ["source_table", "new_tables", "column_map", "keep_original"], errors, idx)
        source_table = action.get("source_table", "")
        new_tables = _require_list(action, "new_tables", errors, idx)
        keep_original = action.get("keep_original")
        _require_bool(action, "keep_original", errors, idx)
        cols_a, cols_b = _normalize_column_map(action, new_tables, errors, idx)
        if source_table and not _table_exists(state, source_table):
            errors.append(f"action[{idx}] source_table '{source_table}' does not exist")
        if new_tables and len(new_tables) != 2:
            errors.append(f"action[{idx}] new_tables must contain exactly two items")
        if new_tables and len(set(new_tables)) != len(new_tables):
            errors.append(f"action[{idx}] new_tables must be unique")
        for name in new_tables:
            if _table_exists(state, str(name)):
                errors.append(f"action[{idx}] new table '{name}' already exists")
        if source_table:
            for column in cols_a + cols_b:
                if column and not _column_exists(state, source_table, column):
                    errors.append(f"action[{idx}] column_map '{source_table}.{column}' does not exist")
        if keep_original is False:
            pk_cols = primary_keys.get(source_table, [])
            if pk_cols:
                if cols_b:
                    if not set(pk_cols).issubset(set(cols_a)) or not set(pk_cols).issubset(set(cols_b)):
                        errors.append(f"action[{idx}] column_map must include all primary key columns")
                else:
                    if not set(pk_cols).issubset(set(cols_a)):
                        errors.append(f"action[{idx}] column_map must include all primary key columns")
            _warn_constraint_loss(warnings, source_table, _table_constraints(schema_state, source_table), "TableSplit")
        if new_tables:
            _name_warning(warnings, str(new_tables[0]), {"_split1", "_part1"}, "derived table")
            _name_warning(warnings, str(new_tables[1]), {"_split2", "_part2"}, "derived table")

    elif action_type == "TableJoin":
        _check_required(action, ["left_table", "right_table", "join_keys", "join_type", "new_table", "select_columns", "keep_original"], errors, idx)
        left_table = action.get("left_table", "")
        right_table = action.get("right_table", "")
        new_table = action.get("new_table", "")
        join_keys = _require_list(action, "join_keys", errors, idx)
        select_columns = _require_list(action, "select_columns", errors, idx)
        join_type = str(action.get("join_type", "")).lower()
        keep_original = action.get("keep_original")
        _require_bool(action, "keep_original", errors, idx)
        for table_name in (left_table, right_table):
            if table_name and not _table_exists(state, table_name):
                errors.append(f"action[{idx}] table '{table_name}' does not exist")
                if table_name in removed_tables:
                    _add_hint(
                        hints,
                        f"table '{table_name}' was removed by action[{removed_tables[table_name]}]; use the new table name or keep_original=true",
                    )
        if new_table and _table_exists(state, new_table):
            errors.append(f"action[{idx}] new_table '{new_table}' already exists")
        if left_table and right_table:
            unqualified: List[str] = []
            qualified_left: List[str] = []
            qualified_right: List[str] = []
            for key in join_keys:
                key_name = str(key)
                if "=" in key_name:
                    left_raw, right_raw = [item.strip() for item in key_name.split("=", 1)]
                    left_hint, left_col = _split_ref(left_raw)
                    right_hint, right_col = _split_ref(right_raw)
                    left_resolved = _resolve_table_for_col(left_hint, left_col, left_table, right_table, state)
                    right_resolved = _resolve_table_for_col(right_hint, right_col, left_table, right_table, state)
                    if left_resolved not in {left_table, right_table} or right_resolved not in {left_table, right_table}:
                        errors.append(f"action[{idx}] join_key '{key_name}' table is invalid")
                        _add_hint(hints, "join_keys should reference left_table/right_table columns; example: LEFT.col=RIGHT.col")
                        continue
                    if left_resolved == right_resolved:
                        errors.append(f"action[{idx}] join_key '{key_name}' must reference both tables")
                        _add_hint(hints, "join_keys should connect left_table and right_table")
                        continue
                    if not _column_exists(state, left_resolved, left_col) or not _column_exists(state, right_resolved, right_col):
                        errors.append(f"action[{idx}] join_key '{key_name}' does not exist")
                        continue
                    if not _join_key_allowed(left_resolved, right_resolved, left_col, right_col, fk_pairs, history_pairs):
                        warnings.append(f"join warning: join_key '{key_name}' is not in FK or workload history")
                        suggestions = _join_key_suggestions(left_table, right_table, fk_pairs, history_pairs)
                        if suggestions:
                            _add_hint(hints, f"suggested join_keys: {', '.join(suggestions)}")
                    continue

                table_hint, col_name = _split_ref(key_name)
                if table_hint:
                    if table_hint not in {left_table, right_table}:
                        errors.append(f"action[{idx}] join_key '{key_name}' table is invalid")
                        _add_hint(hints, "join_keys should reference left_table/right_table columns; example: LEFT.col=RIGHT.col")
                        continue
                    if not _column_exists(state, table_hint, col_name):
                        errors.append(f"action[{idx}] join_key '{key_name}' does not exist")
                        continue
                    if table_hint == left_table:
                        qualified_left.append(col_name)
                    else:
                        qualified_right.append(col_name)
                else:
                    unqualified.append(col_name)

            left_only: List[str] = []
            right_only: List[str] = []
            for col_name in unqualified:
                left_has = _column_exists(state, left_table, col_name)
                right_has = _column_exists(state, right_table, col_name)
                if left_has and right_has:
                    if not _join_key_allowed(left_table, right_table, col_name, col_name, fk_pairs, history_pairs):
                        warnings.append(f"join warning: join_key '{col_name}' is not in FK or workload history")
                        suggestions = _join_key_suggestions(left_table, right_table, fk_pairs, history_pairs)
                        if suggestions:
                            _add_hint(hints, f"suggested join_keys: {', '.join(suggestions)}")
                elif left_has and not right_has:
                    left_only.append(col_name)
                elif right_has and not left_has:
                    right_only.append(col_name)
                else:
                    errors.append(f"action[{idx}] join_key '{col_name}' does not exist")

            if left_only or right_only:
                if len(left_only) == len(right_only) and left_only:
                    for left_col, right_col in zip(left_only, right_only):
                        if not _join_key_allowed(left_table, right_table, left_col, right_col, fk_pairs, history_pairs):
                            warnings.append(
                                f"join warning: join_key '{left_col}={right_col}' is not in FK or workload history"
                            )
                            suggestions = _join_key_suggestions(left_table, right_table, fk_pairs, history_pairs)
                            if suggestions:
                                _add_hint(hints, f"suggested join_keys: {', '.join(suggestions)}")
                else:
                    errors.append(f"action[{idx}] join_keys with different column names must use 'LEFT.col=RIGHT.col'")
                    _add_hint(hints, "use join_keys like 'LEFT.col=RIGHT.col' when names differ")

            if qualified_left or qualified_right:
                if len(qualified_left) != len(qualified_right):
                    errors.append(f"action[{idx}] join_keys must provide same number of left/right columns")
                    _add_hint(hints, "provide matching left/right columns, e.g. LEFT.a=RIGHT.b")
                else:
                    for left_col, right_col in zip(qualified_left, qualified_right):
                        if not _join_key_allowed(left_table, right_table, left_col, right_col, fk_pairs, history_pairs):
                            warnings.append(
                                f"join warning: join_key '{left_col}={right_col}' is not in FK or workload history"
                            )
                            suggestions = _join_key_suggestions(left_table, right_table, fk_pairs, history_pairs)
                            if suggestions:
                                _add_hint(hints, f"suggested join_keys: {', '.join(suggestions)}")
            for col in select_columns:
                if not isinstance(col, str) or "." not in col:
                    errors.append(f"action[{idx}] select_columns must use left.col or right.col")
                    continue
                table_name, column_name = col.split(".", 1)
                if table_name not in {left_table, right_table}:
                    errors.append(f"action[{idx}] select_columns table '{table_name}' is invalid")
                    continue
                if not _column_exists(state, table_name, column_name):
                    errors.append(f"action[{idx}] select column '{col}' does not exist")
        if keep_original is True and join_type not in {"left", "right", "natural"}:
            errors.append(f"action[{idx}] join_type must be left/right/natural when keep_original=true")
        if keep_original is False and join_type != "full":
            errors.append(f"action[{idx}] join_type must be full when keep_original=false")
        _name_warning(warnings, new_table, {"_join"}, "derived table")
        _warn_constraint_loss(warnings, left_table, _table_constraints(schema_state, left_table), "TableJoin")
        _warn_constraint_loss(warnings, right_table, _table_constraints(schema_state, right_table), "TableJoin")

    elif action_type == "HorizontalSplit":
        _check_required(action, ["table", "predicate", "table_true", "table_false", "keep_original"], errors, idx)
        table = action.get("table", "")
        table_true = action.get("table_true", "")
        table_false = action.get("table_false", "")
        _require_bool(action, "keep_original", errors, idx)
        if table and not _table_exists(state, table):
            errors.append(f"action[{idx}] table '{table}' does not exist")
        for name in (table_true, table_false):
            if name and _table_exists(state, name):
                errors.append(f"action[{idx}] table '{name}' already exists")
        _name_warning(warnings, table_true, {"_h1", "_split1"}, "derived table")
        _name_warning(warnings, table_false, {"_h2", "_split2"}, "derived table")
        _warn_constraint_loss(warnings, table, _table_constraints(schema_state, table), "HorizontalSplit")

    elif action_type == "HorizontalMerge":
        _check_required(action, ["table_a", "table_b", "new_table", "keep_original"], errors, idx)
        table_a = action.get("table_a", "")
        table_b = action.get("table_b", "")
        new_table = action.get("new_table", "")
        _require_bool(action, "keep_original", errors, idx)
        for name in (table_a, table_b):
            if name and not _table_exists(state, name):
                errors.append(f"action[{idx}] table '{name}' does not exist")
        if new_table and _table_exists(state, new_table):
            errors.append(f"action[{idx}] new_table '{new_table}' already exists")
        if table_a in state and table_b in state and state[table_a] != state[table_b]:
            errors.append(f"action[{idx}] table_a and table_b must have identical columns")
        _name_warning(warnings, new_table, {"_merge", "_merged"}, "derived table")
        _warn_constraint_loss(warnings, table_a, _table_constraints(schema_state, table_a), "HorizontalMerge")
        _warn_constraint_loss(warnings, table_b, _table_constraints(schema_state, table_b), "HorizontalMerge")

    elif action_type == "RedundantColumnAdd":
        _check_required(action, ["src_table", "src_column", "dst_table", "dst_column", "join_keys"], errors, idx)
        src_table = action.get("src_table", "")
        dst_table = action.get("dst_table", "")
        src_column = action.get("src_column", "")
        dst_column = action.get("dst_column", "")
        join_keys = _require_list(action, "join_keys", errors, idx)
        for name in (src_table, dst_table):
            if name and not _table_exists(state, name):
                errors.append(f"action[{idx}] table '{name}' does not exist")
                if name in removed_tables:
                    _add_hint(
                        hints,
                        f"table '{name}' was removed by action[{removed_tables[name]}]; use the new table name or keep_original=true",
                    )
        if src_table and src_column and not _column_exists(state, src_table, src_column):
            errors.append(f"action[{idx}] src column '{src_table}.{src_column}' does not exist")
        if dst_table and dst_column and _column_exists(state, dst_table, dst_column):
            errors.append(f"action[{idx}] dst column '{dst_table}.{dst_column}' already exists")
        if src_table and dst_table:
            for key in join_keys:
                key_name = str(key)
                if "=" in key_name:
                    left_raw, right_raw = [item.strip() for item in key_name.split("=", 1)]
                    left_hint, left_col = _split_ref(left_raw)
                    right_hint, right_col = _split_ref(right_raw)
                    left_resolved = _resolve_table_for_col(left_hint, left_col, src_table, dst_table, state)
                    right_resolved = _resolve_table_for_col(right_hint, right_col, src_table, dst_table, state)
                    if left_resolved not in {src_table, dst_table} or right_resolved not in {src_table, dst_table}:
                        errors.append(f"action[{idx}] join_key '{key_name}' table is invalid")
                        _add_hint(hints, "join_keys should connect src_table and dst_table, e.g. SRC.col=DST.col")
                        continue
                    if left_resolved == right_resolved:
                        errors.append(f"action[{idx}] join_key '{key_name}' must reference both tables")
                        _add_hint(hints, "join_keys should connect src_table and dst_table")
                        continue
                    if not _column_exists(state, left_resolved, left_col) or not _column_exists(state, right_resolved, right_col):
                        errors.append(f"action[{idx}] join_key '{key_name}' does not exist")
                        continue
                    if not _join_key_allowed(left_resolved, right_resolved, left_col, right_col, fk_pairs, history_pairs):
                        errors.append(f"action[{idx}] join_key '{key_name}' is not in FK or workload history")
                        suggestions = _join_key_suggestions(src_table, dst_table, fk_pairs, history_pairs)
                        if suggestions:
                            _add_hint(hints, f"suggested join_keys: {', '.join(suggestions)}")
                else:
                    if not _column_exists(state, src_table, key_name) or not _column_exists(state, dst_table, key_name):
                        errors.append(f"action[{idx}] join_key '{key_name}' must exist in both tables")
                        _add_hint(hints, "if column names differ, use join_keys like 'SRC.col=DST.col'")
                        suggestions = _join_key_suggestions(src_table, dst_table, fk_pairs, history_pairs)
                        if suggestions:
                            _add_hint(hints, f"suggested join_keys: {', '.join(suggestions)}")
                        continue
                    if not _join_key_allowed(src_table, dst_table, key_name, key_name, fk_pairs, history_pairs):
                        errors.append(f"action[{idx}] join_key '{key_name}' is not in FK or workload history")
                        suggestions = _join_key_suggestions(src_table, dst_table, fk_pairs, history_pairs)
                        if suggestions:
                            _add_hint(hints, f"suggested join_keys: {', '.join(suggestions)}")

    elif action_type == "RedundantColumnDrop":
        _check_required(action, ["table", "column", "origin_table"], errors, idx)
        table = action.get("table", "")
        column = action.get("column", "")
        origin_table = action.get("origin_table", "")
        if table and not _table_exists(state, table):
            errors.append(f"action[{idx}] table '{table}' does not exist")
        if origin_table and not _table_exists(state, origin_table):
            errors.append(f"action[{idx}] origin_table '{origin_table}' does not exist")
        if table and column and not _column_exists(state, table, column):
            errors.append(f"action[{idx}] column '{table}.{column}' does not exist")
        if table and origin_table and column:
            derived_ok = any(
                pair[0] == table and pair[1] == column and pair[2] == origin_table for pair in redundant_pairs
            )
            if not derived_ok:
                errors.append(f"action[{idx}] column '{table}.{column}' is not derived from a prior redundancy")
        if table and column:
            _warn_constraint_loss(warnings, table, _table_constraints(schema_state, table), "RedundantColumnDrop")


def _apply_action(state: Dict[str, Set[str]], action: Dict[str, Any], primary_keys: Dict[str, List[str]]) -> None:
    action_type = action.get("type")
    if action_type == "ColumnSplit":
        table = action["table"]
        column = action["column"]
        new_columns = action["new_columns"]
        keep_original = action["keep_original"]
        state[table].update(new_columns)
        if not keep_original:
            state[table].discard(column)
        return

    if action_type == "TableSplit":
        source_table = action["source_table"]
        new_tables = action["new_tables"]
        keep_original = action["keep_original"]
        src_columns = state.get(source_table, set())
        cols_a, cols_b = _normalize_column_map(action, new_tables, [], -1)
        if cols_b:
            first = set(cols_a)
            second = set(cols_b)
        else:
            first = set(cols_a)
            second = src_columns - first
        if not keep_original:
            pk_cols = primary_keys.get(source_table, [])
            first.update(pk_cols)
            second.update(pk_cols)
        state[new_tables[0]] = set(first)
        state[new_tables[1]] = set(second)
        if not keep_original:
            state.pop(source_table, None)
        return

    if action_type == "TableJoin":
        left_table = action["left_table"]
        right_table = action["right_table"]
        new_table = action["new_table"]
        select_columns = action["select_columns"]
        keep_original = action["keep_original"]
        cols = set(col.split(".", 1)[1] for col in select_columns)
        state[new_table] = cols
        if not keep_original:
            state.pop(left_table, None)
            state.pop(right_table, None)
        return

    if action_type == "HorizontalSplit":
        table = action["table"]
        table_true = action["table_true"]
        table_false = action["table_false"]
        keep_original = action["keep_original"]
        cols = set(state.get(table, set()))
        state[table_true] = set(cols)
        state[table_false] = set(cols)
        if not keep_original:
            state.pop(table, None)
        return

    if action_type == "HorizontalMerge":
        table_a = action["table_a"]
        table_b = action["table_b"]
        new_table = action["new_table"]
        keep_original = action["keep_original"]
        cols = set(state.get(table_a, set()))
        state[new_table] = cols
        if not keep_original:
            state.pop(table_a, None)
            state.pop(table_b, None)
        return

    if action_type == "RedundantColumnAdd":
        dst_table = action["dst_table"]
        dst_column = action["dst_column"]
        state[dst_table].add(dst_column)
        return

    if action_type == "RedundantColumnDrop":
        table = action["table"]
        column = action["column"]
        if table in state:
            state[table].discard(column)


def check_compliance(schema_state: Dict[str, Any], action_sequence: Dict[str, Any]) -> Dict[str, Any]:
    """Validate action sequence against schema state.

    Return is_valid, errors, warnings.
    """
    errors: list[str] = []
    warnings: list[str] = []
    hints: list[str] = []

    if not isinstance(action_sequence, dict):
        return {"is_valid": False, "errors": ["candidate is not a JSON object"], "warnings": [], "hints": []}

    candidate_id = action_sequence.get("candidate_id")
    if candidate_id in (None, ""):
        warnings.append("candidate_id is missing")

    actions = action_sequence.get("actions")
    if not isinstance(actions, list):
        errors.append("'actions' must be a list")
        return {"is_valid": False, "errors": errors, "warnings": warnings, "hints": hints}

    state = _snapshot_schema(schema_state)
    primary_keys = _primary_keys(schema_state)
    fk_pairs = _foreign_key_pairs(schema_state)
    history_pairs = _join_key_history(schema_state)
    redundant_pairs: Set[tuple[str, str, str, str]] = set()
    removed_tables: Dict[str, int] = {}
    for idx, action in enumerate(actions):
        if not isinstance(action, dict):
            errors.append(f"action[{idx}] is not an object")
            continue
        before = len(errors)
        _check_action(
            state,
            action,
            idx,
            schema_state,
            errors,
            warnings,
            hints,
            primary_keys,
            fk_pairs,
            history_pairs,
            redundant_pairs,
            removed_tables,
        )
        if len(errors) == before:
            _apply_action(state, action, primary_keys)
            if action.get("type") == "RedundantColumnAdd":
                redundant_pairs.add(
                    (
                        action.get("dst_table"),
                        action.get("dst_column"),
                        action.get("src_table"),
                        action.get("src_column"),
                    )
                )
                redundant_pairs.add(
                    (
                        action.get("src_table"),
                        action.get("src_column"),
                        action.get("dst_table"),
                        action.get("dst_column"),
                    )
                )
            action_type = action.get("type")
            keep_original = bool(action.get("keep_original", True))
            if action_type == "TableSplit" and not keep_original:
                removed_tables[str(action.get("source_table"))] = idx
            elif action_type == "TableJoin" and not keep_original:
                removed_tables[str(action.get("left_table"))] = idx
                removed_tables[str(action.get("right_table"))] = idx
            elif action_type == "HorizontalSplit" and not keep_original:
                removed_tables[str(action.get("table"))] = idx
            elif action_type == "HorizontalMerge" and not keep_original:
                removed_tables[str(action.get("table_a"))] = idx
                removed_tables[str(action.get("table_b"))] = idx

    return {"is_valid": len(errors) == 0, "errors": errors, "warnings": warnings, "hints": hints}
