from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
import datetime as dt
import json
import logging
from pathlib import Path
import re

import pymysql

from schema_tuning.collectors.metadata import _collect_schema_from_information_schema

logger = logging.getLogger(__name__)


def apply_schema(action_sequence: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """Apply schema changes and data migration.

    Returns status, logs, warnings, and updated schema_state snapshot.
    """
    apply_cfg = config.get("schema_apply", {})
    if not apply_cfg.get("enabled", False):
        return {"status": "skipped", "reason": "schema_apply.enabled is false"}

    output_dir = Path(str(apply_cfg.get("output_dir") or config.get("project", {}).get("output_dir", "./output")))
    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_dir = output_dir / "schema_apply" / timestamp
    log_dir.mkdir(parents=True, exist_ok=True)

    dry_run = bool(apply_cfg.get("dry_run", False))
    backup_on_drop = bool(apply_cfg.get("backup_on_drop", True))

    mysql_cfg = config.get("mysql", {})
    conn = _connect_mysql(mysql_cfg)
    try:
        schema_state, _ = _collect_schema_from_information_schema(conn, str(mysql_cfg.get("database")))
    finally:
        conn.close()

    actions = action_sequence.get("actions", []) if isinstance(action_sequence, dict) else []
    if not isinstance(actions, list):
        return {"status": "error", "error": "actions must be list"}

    warnings: List[str] = []
    logs: List[Dict[str, Any]] = []
    updated_state = schema_state

    conn = _connect_mysql(mysql_cfg)
    try:
        for idx, action in enumerate(actions, 1):
            if not isinstance(action, dict):
                logs.append({"action": action, "error": "action is not dict"})
                continue
            entry = _apply_action(conn, action, updated_state, dry_run, backup_on_drop, warnings)
            logs.append({"step": idx, **entry})
    finally:
        conn.close()

    log_path = log_dir / "schema_apply_log.json"
    log_payload = {"status": "ok", "dry_run": dry_run, "warnings": warnings, "steps": logs}
    log_path.write_text(json.dumps(log_payload, ensure_ascii=True, indent=2), encoding="utf-8")

    return {
        "status": "ok",
        "dry_run": dry_run,
        "log_path": str(log_path),
        "warnings": warnings,
        "schema_state": updated_state,
    }


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
        autocommit=False,
    )


def _fetch_scalar(conn: pymysql.connections.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    if not row:
        return 0
    value = next(iter(row.values())) if isinstance(row, dict) else row[0]
    return int(value or 0)


def _execute(conn: pymysql.connections.Connection, sql: str, params: tuple[Any, ...] = ()) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params)


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text.upper() in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "NULL"}:
        return text
    return "'" + text.replace("'", "''") + "'"


def _column_def(table: str, column: str, schema_state: Dict[str, Any]) -> str:
    info = schema_state.get("tables", {}).get(table, {}).get("columns", {}).get(column, {})
    return _column_def_from_info(column, info)


def _column_def_from_info(column: str, info: Dict[str, Any]) -> str:
    col_type = str(info.get("type") or "VARCHAR(255)")
    nullable = bool(info.get("nullable", True))
    default = info.get("default") if isinstance(info, dict) else None
    auto_inc = bool(info.get("auto_increment"))
    parts = [f"`{column}`", col_type, "NULL" if nullable else "NOT NULL"]
    if default is not None:
        parts.append(f"DEFAULT {_sql_literal(default)}")
    if auto_inc:
        parts.append("AUTO_INCREMENT")
    return " ".join(parts)


def _pk_columns(schema_state: Dict[str, Any], table: str) -> List[str]:
    return list(schema_state.get("tables", {}).get(table, {}).get("primary_key", []) or [])


def _unique_constraints(schema_state: Dict[str, Any], table: str) -> List[Dict[str, Any]]:
    return list(schema_state.get("tables", {}).get(table, {}).get("unique_constraints", []) or [])


def _check_constraints(schema_state: Dict[str, Any], table: str) -> List[Dict[str, Any]]:
    return list(schema_state.get("tables", {}).get(table, {}).get("check_constraints", []) or [])


def _foreign_keys(schema_state: Dict[str, Any], table: str) -> List[Dict[str, Any]]:
    return list(schema_state.get("tables", {}).get(table, {}).get("foreign_keys", []) or [])


def _column_in_unique(schema_state: Dict[str, Any], table: str, column: str) -> bool:
    for uniq in _unique_constraints(schema_state, table):
        cols = uniq.get("columns", []) if isinstance(uniq, dict) else []
        if column in cols:
            return True
    return False


def _column_in_check(schema_state: Dict[str, Any], table: str, column: str) -> bool:
    for chk in _check_constraints(schema_state, table):
        clause = chk.get("clause") if isinstance(chk, dict) else None
        if clause and re.search(rf"\b{re.escape(column)}\b", clause, flags=re.IGNORECASE):
            return True
    return False


def _filter_unique_constraints(
    schema_state: Dict[str, Any], table: str, cols: List[str], warnings: List[str]
) -> List[Dict[str, Any]]:
    allowed = []
    col_set = set(cols)
    for uniq in _unique_constraints(schema_state, table):
        ucols = uniq.get("columns", []) if isinstance(uniq, dict) else []
        if ucols and set(ucols).issubset(col_set):
            allowed.append(uniq)
        else:
            if ucols:
                warnings.append(f"constraint warning: unique({','.join(ucols)}) not copied to split table")
    return allowed


def _filter_check_constraints(
    schema_state: Dict[str, Any], table: str, cols: List[str], warnings: List[str]
) -> List[Dict[str, Any]]:
    allowed = []
    col_set = set(cols)
    keywords = _sql_keywords()
    for chk in _check_constraints(schema_state, table):
        clause = chk.get("clause") if isinstance(chk, dict) else None
        if not clause:
            continue
        tokens = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", clause))
        unknown = [token for token in tokens if token not in col_set and token.lower() not in keywords]
        if unknown:
            warnings.append("constraint warning: check constraint not copied to split table")
            continue
        allowed.append(chk)
    return allowed


def _sql_keywords() -> set[str]:
    return {
        "and",
        "or",
        "not",
        "null",
        "is",
        "in",
        "like",
        "between",
        "true",
        "false",
    }


def _build_table_ddl(
    table: str,
    columns: Iterable[str],
    schema_state: Dict[str, Any],
    source_table: str | None = None,
    column_sources: Dict[str, Tuple[str, str]] | None = None,
    primary_key: List[str] | None = None,
    unique_constraints: List[Dict[str, Any]] | None = None,
    check_constraints: List[Dict[str, Any]] | None = None,
    foreign_keys: List[Dict[str, Any]] | None = None,
) -> str:
    defs: List[str] = []
    for column in columns:
        if column_sources and column in column_sources:
            src_table, src_column = column_sources[column]
            info = schema_state.get("tables", {}).get(src_table, {}).get("columns", {}).get(src_column, {})
            defs.append(_column_def_from_info(column, info))
            continue
        if source_table:
            info = schema_state.get("tables", {}).get(source_table, {}).get("columns", {}).get(column, {})
            defs.append(_column_def_from_info(column, info))
            continue
        defs.append(_column_def(table, column, schema_state))
    if primary_key:
        defs.append("PRIMARY KEY (" + ", ".join(f"`{col}`" for col in primary_key) + ")")
    for uniq in unique_constraints or []:
        cols = uniq.get("columns", []) if isinstance(uniq, dict) else []
        if not cols:
            continue
        defs.append("UNIQUE (" + ", ".join(f"`{col}`" for col in cols) + ")")
    for chk in check_constraints or []:
        clause = chk.get("clause") if isinstance(chk, dict) else None
        if clause:
            defs.append(f"CHECK ({clause})")
    for fk in foreign_keys or []:
        left = fk.get("from") if isinstance(fk, dict) else None
        right = fk.get("to") if isinstance(fk, dict) else None
        if not left or not right or "." not in left or "." not in right:
            continue
        _, left_col = left.split(".", 1)
        ref_table, ref_col = right.split(".", 1)
        defs.append(
            f"FOREIGN KEY (`{left_col}`) REFERENCES `{ref_table}`(`{ref_col}`)"
        )
    return f"CREATE TABLE `{table}` (\n  " + ",\n  ".join(defs) + "\n)"


def _apply_action(
    conn: pymysql.connections.Connection,
    action: Dict[str, Any],
    schema_state: Dict[str, Any],
    dry_run: bool,
    backup_on_drop: bool,
    warnings: List[str],
) -> Dict[str, Any]:
    action_type = action.get("type")
    entry: Dict[str, Any] = {"action": action}
    sql_steps: List[str] = []
    row_checks: List[Dict[str, Any]] = []

    if action_type == "ColumnSplit":
        table = str(action.get("table"))
        column = str(action.get("column"))
        delimiter = str(action.get("delimiter"))
        new_columns = action.get("new_columns", [])
        keep_original = bool(action.get("keep_original", True))
        if not table or not column or not isinstance(new_columns, list) or len(new_columns) < 2:
            return {"error": "invalid ColumnSplit action"}

        column_info = schema_state.get("tables", {}).get(table, {}).get("columns", {}).get(column, {})
        if column_info.get("auto_increment"):
            warnings.append(f"constraint warning: ColumnSplit on auto_increment column {table}.{column}")
        if _column_in_unique(schema_state, table, column):
            warnings.append(f"constraint warning: ColumnSplit on unique column {table}.{column}")
        if _column_in_check(schema_state, table, column):
            warnings.append(f"constraint warning: ColumnSplit on check column {table}.{column}")

        col_a, col_b = str(new_columns[0]), str(new_columns[1])
        col_def = _column_def(table, column, schema_state)
        col_type = col_def.split(" ", 2)[1] if " " in col_def else "VARCHAR(255)"
        sql_steps.append(f"ALTER TABLE `{table}` ADD COLUMN `{col_a}` {col_type}")
        sql_steps.append(f"ALTER TABLE `{table}` ADD COLUMN `{col_b}` {col_type}")
        sql_steps.append(
            f"UPDATE `{table}` SET `{col_a}` = SUBSTRING_INDEX(`{column}`, {_sql_literal(delimiter)}, 1), "
            f"`{col_b}` = SUBSTRING_INDEX(`{column}`, {_sql_literal(delimiter)}, -1)"
        )
        if not keep_original:
            sql_steps.append(f"ALTER TABLE `{table}` DROP COLUMN `{column}`")

        _apply_steps(conn, sql_steps, dry_run)
        _update_schema_state_column_split(schema_state, action)

    elif action_type == "TableSplit":
        source = str(action.get("source_table"))
        new_tables = action.get("new_tables", [])
        column_map = action.get("column_map", [])
        keep_original = bool(action.get("keep_original", True))
        if not source or not isinstance(new_tables, list) or len(new_tables) < 2:
            return {"error": "invalid TableSplit action"}

        src_columns = set(schema_state.get("tables", {}).get(source, {}).get("columns", {}).keys())
        cols_a, cols_b = _resolve_column_map(column_map, new_tables)
        if not cols_b:
            cols_b = list(src_columns - set(cols_a))
        pk_cols = _pk_columns(schema_state, source)
        if not keep_original and pk_cols:
            for col in pk_cols:
                if col not in cols_a:
                    cols_a.append(col)
                if col not in cols_b:
                    cols_b.append(col)

        ddl_a = _build_table_ddl(
            str(new_tables[0]),
            cols_a,
            schema_state,
            source_table=source,
            primary_key=pk_cols if pk_cols else None,
            unique_constraints=_filter_unique_constraints(schema_state, source, cols_a, warnings),
            check_constraints=_filter_check_constraints(schema_state, source, cols_a, warnings),
            foreign_keys=_filter_foreign_keys(schema_state, source, cols_a, warnings),
        )
        ddl_b = _build_table_ddl(
            str(new_tables[1]),
            cols_b,
            schema_state,
            source_table=source,
            primary_key=pk_cols if pk_cols else None,
            unique_constraints=_filter_unique_constraints(schema_state, source, cols_b, warnings),
            check_constraints=_filter_check_constraints(schema_state, source, cols_b, warnings),
            foreign_keys=_filter_foreign_keys(schema_state, source, cols_b, warnings),
        )
        sql_steps.append(ddl_a)
        sql_steps.append(f"INSERT INTO `{new_tables[0]}` ({_cols(cols_a)}) SELECT {_cols(cols_a)} FROM `{source}`")
        sql_steps.append(ddl_b)
        sql_steps.append(f"INSERT INTO `{new_tables[1]}` ({_cols(cols_b)}) SELECT {_cols(cols_b)} FROM `{source}`")
        if not keep_original:
            sql_steps.extend(_drop_or_backup_table(source, backup_on_drop))

        _apply_steps(conn, sql_steps, dry_run)
        row_checks.append(_row_check(conn, source, new_tables[0], dry_run))
        row_checks.append(_row_check(conn, source, new_tables[1], dry_run))
        _update_schema_state_table_split(schema_state, action)

    elif action_type == "TableJoin":
        left = str(action.get("left_table"))
        right = str(action.get("right_table"))
        new_table = str(action.get("new_table"))
        join_keys = action.get("join_keys", [])
        keep_original = bool(action.get("keep_original", True))
        join_type = str(action.get("join_type", "left")).lower()
        if not left or not right or not new_table:
            return {"error": "invalid TableJoin action"}

        left_cols = list(schema_state.get("tables", {}).get(left, {}).get("columns", {}).keys())
        right_cols = list(schema_state.get("tables", {}).get(right, {}).get("columns", {}).keys())
        mapping_right = {col: col for col in right_cols}
        for col in right_cols:
            if col in left_cols:
                mapping_right[col] = f"{right}__{col}"

        columns = left_cols + [mapping_right[col] for col in right_cols]
        pk = list({*(_pk_columns(schema_state, left)), *(_pk_columns(schema_state, right)), *_join_key_names(join_keys)})
        column_sources = {col: (left, col) for col in left_cols}
        for col in right_cols:
            column_sources[mapping_right[col]] = (right, col)
        ddl = _build_table_ddl(new_table, columns, schema_state, column_sources=column_sources, primary_key=pk)
        select_cols = [f"`{left}`.`{col}` AS `{col}`" for col in left_cols]
        select_cols += [f"`{right}`.`{col}` AS `{mapping_right[col]}`" for col in right_cols]
        join_condition = _join_condition(left, right, join_keys)
        join_sql = _join_sql(left, right, join_type, join_condition)
        warnings.append(f"constraint warning: TableJoin drops unique/check/foreign constraints for {new_table}")
        sql_steps.append(ddl)
        if not keep_original:
            left_join = f"`{left}` LEFT JOIN `{right}` ON {join_condition}"
            right_join = f"`{right}` LEFT JOIN `{left}` ON {join_condition}"
            sql_steps.append(
                "INSERT INTO `{table}` ".format(table=new_table)
                + f"SELECT {', '.join(select_cols)} FROM {left_join} "
                + f"UNION ALL SELECT {', '.join(select_cols)} FROM {right_join}"
            )
        else:
            sql_steps.append(f"INSERT INTO `{new_table}` SELECT {', '.join(select_cols)} FROM {join_sql}")
        if not keep_original:
            sql_steps.extend(_drop_or_backup_table(left, backup_on_drop))
            sql_steps.extend(_drop_or_backup_table(right, backup_on_drop))

        _apply_steps(conn, sql_steps, dry_run)
        _update_schema_state_table_join(schema_state, action, mapping_right)

    elif action_type == "HorizontalSplit":
        table = str(action.get("table"))
        table_true = str(action.get("table_true"))
        table_false = str(action.get("table_false"))
        predicate = str(action.get("predicate"))
        keep_original = bool(action.get("keep_original", True))
        if not table or not table_true or not table_false or not predicate:
            return {"error": "invalid HorizontalSplit action"}

        cols = list(schema_state.get("tables", {}).get(table, {}).get("columns", {}).keys())
        ddl_true = _build_table_ddl(
            table_true,
            cols,
            schema_state,
            source_table=table,
            primary_key=_pk_columns(schema_state, table),
            unique_constraints=_unique_constraints(schema_state, table),
            check_constraints=_check_constraints(schema_state, table),
            foreign_keys=_foreign_keys(schema_state, table),
        )
        ddl_false = _build_table_ddl(
            table_false,
            cols,
            schema_state,
            source_table=table,
            primary_key=_pk_columns(schema_state, table),
            unique_constraints=_unique_constraints(schema_state, table),
            check_constraints=_check_constraints(schema_state, table),
            foreign_keys=_foreign_keys(schema_state, table),
        )
        sql_steps.append(ddl_true)
        sql_steps.append(f"INSERT INTO `{table_true}` SELECT * FROM `{table}` WHERE {predicate}")
        sql_steps.append(ddl_false)
        sql_steps.append(f"INSERT INTO `{table_false}` SELECT * FROM `{table}` WHERE NOT ({predicate})")
        if not keep_original:
            sql_steps.extend(_drop_or_backup_table(table, backup_on_drop))

        _apply_steps(conn, sql_steps, dry_run)
        row_checks.append(_row_check(conn, table, table_true, dry_run))
        row_checks.append(_row_check(conn, table, table_false, dry_run))
        _update_schema_state_horizontal_split(schema_state, action)

    elif action_type == "HorizontalMerge":
        table_a = str(action.get("table_a"))
        table_b = str(action.get("table_b"))
        new_table = str(action.get("new_table"))
        keep_original = bool(action.get("keep_original", True))
        if not table_a or not table_b or not new_table:
            return {"error": "invalid HorizontalMerge action"}

        cols = list(schema_state.get("tables", {}).get(table_a, {}).get("columns", {}).keys())
        ddl = _build_table_ddl(
            new_table,
            cols,
            schema_state,
            source_table=table_a,
            primary_key=_pk_columns(schema_state, table_a),
            unique_constraints=_unique_constraints(schema_state, table_a),
            check_constraints=_check_constraints(schema_state, table_a),
            foreign_keys=_foreign_keys(schema_state, table_a),
        )
        if _unique_constraints(schema_state, table_b) or _check_constraints(schema_state, table_b):
            warnings.append(f"constraint warning: HorizontalMerge drops unique/check constraints from {table_b}")
        sql_steps.append(ddl)
        sql_steps.append(f"INSERT INTO `{new_table}` SELECT * FROM `{table_a}` UNION ALL SELECT * FROM `{table_b}`")
        if not keep_original:
            sql_steps.extend(_drop_or_backup_table(table_a, backup_on_drop))
            sql_steps.extend(_drop_or_backup_table(table_b, backup_on_drop))

        _apply_steps(conn, sql_steps, dry_run)
        _update_schema_state_horizontal_merge(schema_state, action)

    elif action_type == "RedundantColumnAdd":
        src_table = str(action.get("src_table"))
        dst_table = str(action.get("dst_table"))
        src_column = str(action.get("src_column"))
        dst_column = str(action.get("dst_column"))
        join_keys = action.get("join_keys", [])
        if not src_table or not dst_table or not src_column or not dst_column:
            return {"error": "invalid RedundantColumnAdd action"}

        col_type = _column_def(src_table, src_column, schema_state).split(" ", 2)[1]
        sql_steps.append(f"ALTER TABLE `{dst_table}` ADD COLUMN `{dst_column}` {col_type}")
        join_condition = _join_condition(dst_table, src_table, join_keys)
        sql_steps.append(
            f"UPDATE `{dst_table}` JOIN `{src_table}` ON {join_condition} "
            f"SET `{dst_table}`.`{dst_column}` = `{src_table}`.`{src_column}`"
        )
        _apply_steps(conn, sql_steps, dry_run)
        _update_schema_state_redundant_add(schema_state, action)

    elif action_type == "RedundantColumnDrop":
        table = str(action.get("table"))
        column = str(action.get("column"))
        if not table or not column:
            return {"error": "invalid RedundantColumnDrop action"}
        sql_steps.append(f"ALTER TABLE `{table}` DROP COLUMN `{column}`")
        _apply_steps(conn, sql_steps, dry_run)
        _update_schema_state_redundant_drop(schema_state, action)

    else:
        return {"error": f"unsupported action type '{action_type}'"}

    entry["sql"] = sql_steps
    entry["row_checks"] = row_checks
    return entry


def _apply_steps(conn: pymysql.connections.Connection, sql_steps: List[str], dry_run: bool) -> None:
    if dry_run:
        return
    try:
        _execute(conn, "START TRANSACTION")
        for sql in sql_steps:
            _execute(conn, sql)
        _execute(conn, "COMMIT")
    except Exception:
        _execute(conn, "ROLLBACK")
        raise


def _drop_or_backup_table(table: str, backup_on_drop: bool) -> List[str]:
    if backup_on_drop:
        suffix = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup = f"{table}__backup_{suffix}"
        return [f"RENAME TABLE `{table}` TO `{backup}`"]
    return [f"DROP TABLE `{table}`"]


def _cols(columns: Iterable[str]) -> str:
    return ", ".join(f"`{col}`" for col in columns)


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


def _filter_foreign_keys(schema_state: Dict[str, Any], table: str, cols: List[str], warnings: List[str]) -> List[Dict[str, Any]]:
    fks = _foreign_keys(schema_state, table)
    filtered = []
    for fk in fks:
        left = fk.get("from") if isinstance(fk, dict) else None
        if not left or "." not in left:
            continue
        _, col = left.split(".", 1)
        if col in cols:
            filtered.append(fk)
        else:
            warnings.append(f"constraint warning: FK {left} not copied due to TableSplit")
    return filtered


def _join_key_names(join_keys: Any) -> List[str]:
    keys: List[str] = []
    if not isinstance(join_keys, list):
        return keys
    for key in join_keys:
        key_str = str(key)
        if "=" in key_str:
            left, right = [item.strip() for item in key_str.split("=", 1)]
            keys.append(left.split(".", 1)[-1])
            keys.append(right.split(".", 1)[-1])
        else:
            keys.append(key_str.split(".", 1)[-1])
    return list(dict.fromkeys(keys))


def _join_condition(left: str, right: str, join_keys: Any) -> str:
    if not isinstance(join_keys, list) or not join_keys:
        return "1=1"
    parts = []
    for key in join_keys:
        key_str = str(key)
        if "=" in key_str:
            left_key, right_key = [item.strip() for item in key_str.split("=", 1)]
            parts.append(f"`{left_key.replace('.', '`.`')}` = `{right_key.replace('.', '`.`')}`")
        else:
            parts.append(f"`{left}`.`{key_str}` = `{right}`.`{key_str}`")
    return " AND ".join(parts)


def _join_sql(left: str, right: str, join_type: str, join_condition: str) -> str:
    if join_type == "right":
        return f"`{right}` RIGHT JOIN `{left}` ON {join_condition}"
    if join_type == "natural":
        return f"`{left}` NATURAL JOIN `{right}`"
    return f"`{left}` LEFT JOIN `{right}` ON {join_condition}"


def _row_check(conn: pymysql.connections.Connection, source: str, new_table: str, dry_run: bool) -> Dict[str, Any]:
    if dry_run:
        return {"source": source, "target": new_table, "source_rows": None, "target_rows": None}
    return {
        "source": source,
        "target": new_table,
        "source_rows": _fetch_scalar(conn, f"SELECT COUNT(*) AS cnt FROM `{source}`"),
        "target_rows": _fetch_scalar(conn, f"SELECT COUNT(*) AS cnt FROM `{new_table}`"),
    }


def _update_schema_state_column_split(schema_state: Dict[str, Any], action: Dict[str, Any]) -> None:
    table = str(action.get("table"))
    column = str(action.get("column"))
    new_columns = action.get("new_columns", [])
    keep_original = bool(action.get("keep_original", True))
    table_info = schema_state.get("tables", {}).get(table)
    if not isinstance(table_info, dict):
        return
    columns = table_info.get("columns", {})
    if not isinstance(columns, dict):
        return
    if not keep_original and column in columns:
        columns.pop(column, None)
    for idx, name in enumerate(new_columns[:2]):
        if not name:
            continue
        columns[str(name)] = dict(columns.get(column, {}))
        table_info.setdefault("lineage", {}).setdefault("derived_from", []).append(
            {"column": str(name), "origin": f"{table}.{column}", "rule": f"split_{idx+1}"}
        )


def _update_schema_state_table_split(schema_state: Dict[str, Any], action: Dict[str, Any]) -> None:
    source = str(action.get("source_table"))
    new_tables = action.get("new_tables", [])
    column_map = action.get("column_map", [])
    keep_original = bool(action.get("keep_original", True))
    source_info = schema_state.get("tables", {}).get(source)
    if not isinstance(source_info, dict) or len(new_tables) < 2:
        return
    cols_a, cols_b = _resolve_column_map(column_map, new_tables)
    if not cols_b:
        cols_b = [col for col in source_info.get("columns", {}).keys() if col not in cols_a]

    for idx, cols in enumerate([cols_a, cols_b]):
        table_name = str(new_tables[idx])
        schema_state["tables"][table_name] = {
            "columns": {col: dict(source_info.get("columns", {}).get(col, {})) for col in cols},
            "primary_key": list(source_info.get("primary_key", [])),
            "foreign_keys": list(source_info.get("foreign_keys", [])),
            "unique_constraints": list(source_info.get("unique_constraints", [])),
            "check_constraints": list(source_info.get("check_constraints", [])),
            "lineage": {"origin": table_name, "derived_from": [source]},
        }
    if not keep_original:
        schema_state.get("tables", {}).pop(source, None)


def _update_schema_state_table_join(schema_state: Dict[str, Any], action: Dict[str, Any], right_mapping: Dict[str, str]) -> None:
    left = str(action.get("left_table"))
    right = str(action.get("right_table"))
    new_table = str(action.get("new_table"))
    keep_original = bool(action.get("keep_original", True))
    left_info = schema_state.get("tables", {}).get(left, {})
    right_info = schema_state.get("tables", {}).get(right, {})
    columns = {}
    for col, info in left_info.get("columns", {}).items():
        columns[col] = dict(info)
    for col, info in right_info.get("columns", {}).items():
        columns[right_mapping.get(col, col)] = dict(info)
    schema_state.get("tables", {})[new_table] = {
        "columns": columns,
        "primary_key": list({*left_info.get("primary_key", []), *right_info.get("primary_key", [])}),
        "foreign_keys": [],
        "unique_constraints": [],
        "check_constraints": [],
        "lineage": {"origin": new_table, "derived_from": [left, right]},
    }
    if not keep_original:
        schema_state.get("tables", {}).pop(left, None)
        schema_state.get("tables", {}).pop(right, None)


def _update_schema_state_horizontal_split(schema_state: Dict[str, Any], action: Dict[str, Any]) -> None:
    table = str(action.get("table"))
    table_true = str(action.get("table_true"))
    table_false = str(action.get("table_false"))
    keep_original = bool(action.get("keep_original", True))
    info = schema_state.get("tables", {}).get(table)
    if not isinstance(info, dict):
        return
    for name in (table_true, table_false):
        schema_state.get("tables", {})[name] = {
            "columns": dict(info.get("columns", {})),
            "primary_key": list(info.get("primary_key", [])),
            "foreign_keys": list(info.get("foreign_keys", [])),
            "unique_constraints": list(info.get("unique_constraints", [])),
            "check_constraints": list(info.get("check_constraints", [])),
            "lineage": {"origin": name, "derived_from": [table]},
        }
    if not keep_original:
        schema_state.get("tables", {}).pop(table, None)


def _update_schema_state_horizontal_merge(schema_state: Dict[str, Any], action: Dict[str, Any]) -> None:
    table_a = str(action.get("table_a"))
    table_b = str(action.get("table_b"))
    new_table = str(action.get("new_table"))
    keep_original = bool(action.get("keep_original", True))
    info = schema_state.get("tables", {}).get(table_a)
    if not isinstance(info, dict):
        return
    schema_state.get("tables", {})[new_table] = {
        "columns": dict(info.get("columns", {})),
        "primary_key": list(info.get("primary_key", [])),
        "foreign_keys": list(info.get("foreign_keys", [])),
        "unique_constraints": list(info.get("unique_constraints", [])),
        "check_constraints": list(info.get("check_constraints", [])),
        "lineage": {"origin": new_table, "derived_from": [table_a, table_b]},
    }
    if not keep_original:
        schema_state.get("tables", {}).pop(table_a, None)
        schema_state.get("tables", {}).pop(table_b, None)


def _update_schema_state_redundant_add(schema_state: Dict[str, Any], action: Dict[str, Any]) -> None:
    dst_table = str(action.get("dst_table"))
    dst_column = str(action.get("dst_column"))
    src_table = str(action.get("src_table"))
    src_column = str(action.get("src_column"))
    table_info = schema_state.get("tables", {}).get(dst_table)
    if not isinstance(table_info, dict):
        return
    table_info.setdefault("columns", {})[dst_column] = dict(
        schema_state.get("tables", {}).get(src_table, {}).get("columns", {}).get(src_column, {})
    )
    table_info.setdefault("lineage", {}).setdefault("derived_from", []).append(
        {"column": dst_column, "origin": f"{src_table}.{src_column}", "rule": "redundant_add"}
    )


def _update_schema_state_redundant_drop(schema_state: Dict[str, Any], action: Dict[str, Any]) -> None:
    table = str(action.get("table"))
    column = str(action.get("column"))
    table_info = schema_state.get("tables", {}).get(table)
    if not isinstance(table_info, dict):
        return
    table_info.get("columns", {}).pop(column, None)
