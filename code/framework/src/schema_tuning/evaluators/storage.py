from __future__ import annotations

from typing import Any, Dict


def _to_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return int(value)
    return default


def _get_table_sizes(storage_stats: Dict[str, Any]) -> Dict[str, int]:
    raw = storage_stats.get("table_sizes", {})
    if not isinstance(raw, dict):
        return {}
    return {str(k): _to_int(v) for k, v in raw.items()}


def _get_column_sizes(storage_stats: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    raw = storage_stats.get("column_sizes", {})
    if not isinstance(raw, dict):
        return {}

    result: Dict[str, Dict[str, int]] = {}
    for table, columns in raw.items():
        if not isinstance(columns, dict):
            continue
        result[str(table)] = {str(c): _to_int(size) for c, size in columns.items()}
    return result


def _column_size(column_sizes: Dict[str, Dict[str, int]], table: str, column: str, fallback: int = 0) -> int:
    return _to_int(column_sizes.get(table, {}).get(column), fallback)


def _half(size: int) -> int:
    return int(size * 0.5)


def _primary_keys(schema_state: Dict[str, Any]) -> Dict[str, list[str]]:
    tables = schema_state.get("tables", {})
    if not isinstance(tables, dict):
        return {}
    result: Dict[str, list[str]] = {}
    for table, info in tables.items():
        if not isinstance(info, dict):
            continue
        pk = info.get("primary_key", [])
        if isinstance(pk, list):
            result[str(table)] = [str(col) for col in pk]
        else:
            result[str(table)] = []
    return result


def _action_delta(
    action: Dict[str, Any],
    table_sizes: Dict[str, int],
    column_sizes: Dict[str, Dict[str, int]],
    pk_map: Dict[str, list[str]],
) -> Dict[str, int]:
    action_type = action.get("type")
    keep_original = bool(action.get("keep_original", True))
    deltas: Dict[str, int] = {}

    if action_type == "ColumnSplit":
        return deltas

    if action_type == "TableSplit":
        source = str(action.get("source_table", ""))
        source_size = table_sizes.get(source, 0)
        new_tables = action.get("new_tables", [])
        column_map = action.get("column_map", [])
        source_columns = set(column_sizes.get(source, {}).keys())
        pk_cols = [col for col in pk_map.get(source, []) if col]
        pk_size = sum(_column_size(column_sizes, source, col) for col in pk_cols)
        if pk_size <= 0 and source_size > 0:
            pk_size = max(int(source_size * 0.05), 0)

        def _resolve_columns(raw: Any, tables: list[Any]) -> tuple[set[str], set[str]]:
            if isinstance(raw, list):
                return {str(col) for col in raw}, set()
            if isinstance(raw, dict):
                keys = [str(name) for name in tables if str(name) in raw]
                if len(keys) < 2:
                    keys = [str(key) for key in raw.keys()]
                if len(keys) >= 2:
                    cols_a = raw.get(keys[0], [])
                    cols_b = raw.get(keys[1], [])
                    if isinstance(cols_a, list) and isinstance(cols_b, list):
                        return {str(col) for col in cols_a}, {str(col) for col in cols_b}
            return set(), set()

        cols_a, cols_b = _resolve_columns(column_map, new_tables if isinstance(new_tables, list) else [])
        if not cols_b:
            cols_b = source_columns - cols_a if source_columns else set()
        if not keep_original and pk_cols:
            cols_a.update(pk_cols)
            cols_b.update(pk_cols)

        def _table_size_from_cols(cols: set[str]) -> int:
            if cols:
                size = sum(_column_size(column_sizes, source, col) for col in cols)
                if size > 0:
                    return size
            return 0

        size_a = _table_size_from_cols(cols_a)
        size_b = _table_size_from_cols(cols_b)
        if size_a <= 0 or size_b <= 0:
            size_a = _half(source_size)
            size_b = source_size - size_a
            if not keep_original:
                size_a += pk_size
                size_b += pk_size
        if isinstance(new_tables, list) and len(new_tables) >= 2:
            deltas[str(new_tables[0])] = deltas.get(str(new_tables[0]), 0) + size_a
            deltas[str(new_tables[1])] = deltas.get(str(new_tables[1]), 0) + size_b
        elif source:
            deltas[f"{source}_part_a"] = deltas.get(f"{source}_part_a", 0) + size_a
            deltas[f"{source}_part_b"] = deltas.get(f"{source}_part_b", 0) + size_b

        if keep_original:
            deltas[source] = deltas.get(source, 0)
        else:
            deltas[source] = deltas.get(source, 0) - source_size
        return deltas

    if action_type == "TableJoin":
        left = str(action.get("left_table", ""))
        right = str(action.get("right_table", ""))
        left_size = table_sizes.get(left, 0)
        right_size = table_sizes.get(right, 0)
        new_table = str(action.get("new_table", f"{left}_{right}_join"))
        join_keys = action.get("join_keys", [])
        join_key_size = 0
        if isinstance(join_keys, list):
            for key in join_keys:
                key_name = str(key)
                join_key_size += _column_size(column_sizes, left, key_name)
                join_key_size += _column_size(column_sizes, right, key_name)
        joined_size = max(left_size + right_size - join_key_size, 0)
        deltas[new_table] = deltas.get(new_table, 0) + joined_size
        if not keep_original:
            deltas[left] = deltas.get(left, 0) - left_size
            deltas[right] = deltas.get(right, 0) - right_size
        return deltas

    if action_type == "HorizontalSplit":
        source = str(action.get("table", ""))
        source_size = table_sizes.get(source, 0)
        table_true = str(action.get("table_true", f"{source}_true"))
        table_false = str(action.get("table_false", f"{source}_false"))
        deltas[table_true] = deltas.get(table_true, 0) + _half(source_size)
        deltas[table_false] = deltas.get(table_false, 0) + source_size - _half(source_size)
        if not keep_original:
            deltas[source] = deltas.get(source, 0) - source_size
        return deltas

    if action_type == "HorizontalMerge":
        table_a = str(action.get("table_a", ""))
        table_b = str(action.get("table_b", ""))
        size_a = table_sizes.get(table_a, 0)
        size_b = table_sizes.get(table_b, 0)
        new_table = str(action.get("new_table", f"{table_a}_{table_b}_merged"))
        deltas[new_table] = deltas.get(new_table, 0) + size_a + size_b
        if not keep_original:
            deltas[table_a] = deltas.get(table_a, 0) - size_a
            deltas[table_b] = deltas.get(table_b, 0) - size_b
        return deltas

    if action_type == "RedundantColumnAdd":
        src_table = str(action.get("src_table", ""))
        src_column = str(action.get("src_column", ""))
        dst_table = str(action.get("dst_table", ""))
        src_col_size = _column_size(column_sizes, src_table, src_column)
        if src_col_size <= 0:
            src_col_size = int(table_sizes.get(src_table, 0) * 0.1)
        deltas[dst_table] = deltas.get(dst_table, 0) + max(src_col_size, 0)
        return deltas

    if action_type == "RedundantColumnDrop":
        table = str(action.get("table", ""))
        column = str(action.get("column", ""))
        col_size = _column_size(column_sizes, table, column)
        if col_size <= 0:
            col_size = int(table_sizes.get(table, 0) * 0.1)
        deltas[table] = deltas.get(table, 0) - max(col_size, 0)
        return deltas

    return deltas


def estimate_storage(
    storage_stats: Dict[str, Any],
    action_sequence: Dict[str, Any],
    config: Dict[str, Any],
    schema_state: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Estimate storage delta for an action sequence."""
    table_sizes = _get_table_sizes(storage_stats)
    column_sizes = _get_column_sizes(storage_stats)
    total_before = _to_int(storage_stats.get("total_size_before"), sum(table_sizes.values()))
    pk_map = _primary_keys(schema_state or {})

    table_deltas: Dict[str, int] = {}
    actions = action_sequence.get("actions", [])
    if not isinstance(actions, list):
        actions = []

    for action in actions:
        if not isinstance(action, dict):
            continue
        delta_map = _action_delta(action, table_sizes, column_sizes, pk_map)
        for table, delta in delta_map.items():
            table_deltas[table] = table_deltas.get(table, 0) + delta

    total_delta = sum(table_deltas.values())
    total_after = total_before + total_delta
    limit = _to_int(config.get("storage", {}).get("max_total_bytes"), 0)

    return {
        "limit_exceeded": bool(limit > 0 and total_after > limit),
        "total_size_before": total_before,
        "total_size_after": total_after,
        "total_delta": total_delta,
        "table_deltas": table_deltas,
    }
