from __future__ import annotations

import asyncio
import copy
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
import sys

from schema_tuning.collectors.metadata import collect_metadata
from schema_tuning.collectors.metadata import _collect_schema_from_information_schema
from schema_tuning.apply.schema_apply import _apply_action
from schema_tuning.apply.schema_apply import _connect_mysql
from schema_tuning.rewriter.sql_rewriter import rewrite_sql
from schema_tuning.evaluators.compliance import check_compliance
from schema_tuning.evaluators.performance import estimate_performance
from schema_tuning.evaluators.storage import estimate_storage
from schema_tuning.llm.client import request_sequence_async
from schema_tuning.logging_utils import prepare_llm_log_dir
from schema_tuning.prompt.builder import build_messages
from schema_tuning.search.mcts_schema_search import run_offline_search


def _evaluate_candidate(metadata: Dict[str, Any], candidate: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    compliance = check_compliance(metadata.get("schema_state", {}), candidate)
    storage = estimate_storage(metadata.get("storage_stats", {}), candidate, config, metadata.get("schema_state", {}))
    performance = estimate_performance(metadata, candidate, config)
    return {
        "candidate_id": candidate.get("candidate_id", ""),
        "candidate": candidate,
        "compliance": compliance,
        "storage": storage,
        "performance": performance,
    }


def _is_acceptable(result: Dict[str, Any]) -> bool:
    if not bool(result.get("compliance", {}).get("is_valid") and not result.get("storage", {}).get("limit_exceeded")):
        return False
    total_delta = result.get("performance", {}).get("total_delta", 0.0)
    try:
        total_delta_value = float(total_delta)
    except (TypeError, ValueError):
        total_delta_value = 0.0
    return total_delta_value <= 0.0


def _pick_best(round_results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    valid_results = [r for r in round_results if _is_acceptable(r)]
    if not valid_results:
        return None

    return min(valid_results, key=lambda item: float(item.get("performance", {}).get("total_delta", 0.0)))


def _split_params(param_text: str) -> List[str]:
    params: List[str] = []
    depth = 0
    quote: Optional[str] = None
    start = 0
    for idx, ch in enumerate(param_text):
        if ch in ("'", '"'):
            if quote is None:
                quote = ch
            elif quote == ch:
                quote = None
        elif quote is None:
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
            elif ch == "," and depth == 0:
                params.append(param_text[start:idx].strip())
                start = idx + 1
    tail = param_text[start:].strip()
    if tail:
        params.append(tail)
    return params


def _parse_value(raw: str) -> Any:
    value = raw.strip()
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        items = [item.strip() for item in _split_params(inner)]
        return [item.strip("\"'") for item in items]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if (value.startswith("\"") and value.endswith("\"")) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    return value


def _parse_action_line(line: str) -> Dict[str, Any]:
    match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\((.*)\)", line)
    if not match:
        raise ValueError(f"invalid action format: {line}")
    action_type = match.group(1)
    param_text = match.group(2).strip()
    params: Dict[str, Any] = {"type": action_type}
    if param_text:
        for part in _split_params(param_text):
            if "=" not in part:
                raise ValueError(f"invalid param: {part}")
            key, raw_value = part.split("=", 1)
            key = key.strip()
            if not key:
                raise ValueError(f"invalid param key: {part}")
            params[key] = _parse_value(raw_value)
    return params


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        parts = stripped.split("```", 2)
        if len(parts) >= 3:
            return parts[1].strip()
    return text


def _parse_action_sequence_json(payload: Any, candidate_id: str) -> Dict[str, Any]:
    if isinstance(payload, dict):
        actions = payload.get("actions")
        if isinstance(actions, list):
            candidate = dict(payload)
            candidate.setdefault("candidate_id", candidate_id)
            return candidate
    if isinstance(payload, list):
        if payload and isinstance(payload[0], dict) and "actions" in payload[0]:
            candidate = dict(payload[0])
            candidate.setdefault("candidate_id", candidate_id)
            return candidate
        if all(isinstance(item, dict) and "type" in item for item in payload):
            return {"candidate_id": candidate_id, "actions": payload}
    raise ValueError("invalid JSON action sequence")


def _parse_action_sequence(text: str, candidate_id: str) -> Dict[str, Any]:
    cleaned = _strip_code_fence(text).strip()
    if not cleaned:
        raise ValueError("empty response")

    if cleaned[0] not in "[{":
        raise ValueError("expected JSON response")

    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON response: {exc}") from exc

    return _parse_action_sequence_json(payload, candidate_id)


def _feedback_instructions() -> List[str]:
    return [
        "当前各个序列预期延迟变化如下：",
        "当前各操作后存储开销变化如下：",
        "接下来请继续修改操作，给出进一步性能优化的完整操作。",
        "必须删除或替换所有负优化动作：只要某个 action 或 SQL 的延迟增量为正(>0)，就不得保留该动作。",
        "最终序列的 total_delta 必须 <= 0，否则视为无效并继续改写。",
    ]


def _format_feedback(result: Dict[str, Any]) -> str:
    compliance = result.get("compliance", {})
    storage = result.get("storage", {})
    performance = result.get("performance", {})
    payload = {
        "candidate_id": result.get("candidate_id", ""),
        "errors": compliance.get("errors", []),
        "warnings": compliance.get("warnings", []),
        "hints": compliance.get("hints", []),
        "limit_exceeded": storage.get("limit_exceeded"),
        "total_delta": performance.get("total_delta"),
        "sequence_latency_deltas": performance.get("op_deltas", {}),
        "sql_latency_deltas": performance.get("sql_deltas", {}),
        "storage_total_delta": storage.get("total_delta"),
        "storage_table_deltas": storage.get("table_deltas", {}),
        "feedback_prompt": _feedback_instructions(),
    }
    return json.dumps(payload, ensure_ascii=False)


def _append_llm_log(log_dir: Path, candidate_id: str, payload: Dict[str, Any]) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{candidate_id}.ndjson"
    log_path.write_text("", encoding="utf-8") if not log_path.exists() else None
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _format_first_prompt(messages: List[Dict[str, str]]) -> str:
    system_text = ""
    user_text = ""
    for message in messages:
        if message.get("role") == "system" and not system_text:
            system_text = message.get("content", "")
        elif message.get("role") == "user" and not user_text:
            user_text = message.get("content", "")
    return "SYSTEM:\n" + system_text + "\n\nUSER:\n" + user_text + "\n"


def _safe_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _prompt_yes_no(question: str) -> bool:
    prompt = question.strip()
    if not prompt.endswith(" "):
        prompt += " "
    if not sys.stdin.isatty():
        print(f"{prompt}(非交互模式，自动跳过)")
        return False
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        return False
    return answer in {"y", "yes", "1", "true", "是"}


def _write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json_file(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _format_sql_steps(sql_steps: List[str]) -> str:
    if not sql_steps:
        return ""
    return ";\n".join(step.rstrip(";") for step in sql_steps) + ";\n"


def _interactive_schema_apply(candidate: Dict[str, Any], config: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
    apply_cfg = config.get("schema_apply", {})
    if not bool(apply_cfg.get("enabled", False)):
        print("schema_apply 未启用，跳过落库执行。")
        return {"status": "skipped", "reason": "schema_apply disabled"}

    actions = candidate.get("actions", []) if isinstance(candidate, dict) else []
    if not isinstance(actions, list) or not actions:
        print("无可执行的模式变更动作，跳过。")
        return {"status": "skipped", "reason": "no actions"}

    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_dir = output_dir / "schema_apply" / f"interactive_{timestamp}"
    log_dir.mkdir(parents=True, exist_ok=True)

    mysql_cfg = config.get("mysql", {})
    conn = _connect_mysql(mysql_cfg)
    try:
        schema_state, _ = _collect_schema_from_information_schema(conn, str(mysql_cfg.get("database")))
    finally:
        conn.close()

    exec_state = schema_state
    warnings: List[str] = []
    logs: List[Dict[str, Any]] = []
    backup_on_drop = bool(apply_cfg.get("backup_on_drop", True))

    conn = _connect_mysql(mysql_cfg)
    try:
        for idx, action in enumerate(actions, 1):
            action_type = str(action.get("type") or "unknown")
            preview_state = copy.deepcopy(exec_state)
            try:
                preview_entry = _apply_action(conn, action, preview_state, True, backup_on_drop, warnings)
            except Exception as exc:
                error_msg = f"预演失败: {exc}"
                print(error_msg)
                logs.append({"step": idx, "action": action, "status": "plan_failed", "error": str(exc)})
                break

            sql_steps = preview_entry.get("sql", []) if isinstance(preview_entry, dict) else []
            sql_text = _format_sql_steps(sql_steps)
            sql_path = log_dir / f"step_{idx:03d}_{action_type}.sql"
            _write_text_file(sql_path, sql_text)

            step_payload = {
                "step": idx,
                "action": action,
                "sql": sql_steps,
                "status": "planned",
            }
            _write_json_file(log_dir / f"step_{idx:03d}_{action_type}.json", step_payload)

            print(f"\n第 {idx} 组DDL ({action_type}):")
            if sql_steps:
                for statement in sql_steps:
                    print(statement.rstrip(";") + ";")
            else:
                print("(无DDL输出)")

            if not _prompt_yes_no("是否执行该组DDL? (y/n):"):
                logs.append({"step": idx, "action": action, "status": "aborted"})
                _write_json_file(log_dir / "interactive_apply_log.json", {"status": "aborted", "warnings": warnings, "steps": logs})
                return {"status": "aborted", "log_dir": str(log_dir), "warnings": warnings}

            try:
                exec_entry = _apply_action(conn, action, exec_state, False, backup_on_drop, warnings)
            except Exception as exc:
                error_msg = f"执行失败: {exc}"
                print(error_msg)
                logs.append({"step": idx, "action": action, "status": "failed", "error": str(exc)})
                _write_json_file(log_dir / "interactive_apply_log.json", {"status": "failed", "warnings": warnings, "steps": logs})
                return {"status": "failed", "log_dir": str(log_dir), "warnings": warnings, "error": str(exc)}

            logs.append({"step": idx, "action": action, "status": "ok", "sql": exec_entry.get("sql", [])})
            print("执行成功。")
    finally:
        conn.close()

    _write_json_file(log_dir / "interactive_apply_log.json", {"status": "ok", "warnings": warnings, "steps": logs})
    return {"status": "ok", "log_dir": str(log_dir), "warnings": warnings}


async def _run_candidate_loop(
    candidate_id: str,
    base_messages: List[Dict[str, str]],
    metadata: Dict[str, Any],
    config: Dict[str, Any],
    log_dir: Path,
) -> Dict[str, Any]:
    messages = [dict(message) for message in base_messages]
    rounds: List[Dict[str, Any]] = []
    max_rounds = config.get("llm", {}).get("max_rounds", 1)
    early_stop_round = max(1, max_rounds - 9)

    for round_id in range(1, max_rounds + 1):
        delta_messages: List[Dict[str, str]] = []
        raw_text = await request_sequence_async(messages, config)
        try:
            candidate = _parse_action_sequence(raw_text, candidate_id)
        except ValueError as exc:
            assistant_message = {"role": "assistant", "content": raw_text}
            messages.append(assistant_message)
            delta_messages.append(assistant_message)
            error_feedback = json.dumps(
                {
                    "candidate_id": candidate_id,
                    "errors": [str(exc)],
                    "warnings": [],
                    "hints": [],
                    "limit_exceeded": None,
                    "total_delta": None,
                    "sequence_latency_deltas": {},
                    "sql_latency_deltas": {},
                    "storage_total_delta": None,
                    "storage_table_deltas": {},
                    "feedback_prompt": _feedback_instructions(),
                },
                ensure_ascii=False,
            )
            user_message = {"role": "user", "content": error_feedback}
            messages.append(user_message)
            delta_messages.append(user_message)
            rounds.append({"round": round_id, "raw": raw_text, "parse_error": str(exc)})
            _append_llm_log(
                log_dir,
                candidate_id,
                {
                    "round": round_id,
                    "raw": raw_text,
                    "parse_error": str(exc),
                    "delta_messages": delta_messages,
                    "message_count": len(messages),
                },
            )
            continue

        result = _evaluate_candidate(metadata, candidate, config)
        rounds.append({"round": round_id, "raw": raw_text, "result": result})
        assistant_message = {"role": "assistant", "content": raw_text}
        messages.append(assistant_message)
        delta_messages.append(assistant_message)
        if _is_acceptable(result) and round_id >= early_stop_round:
            _append_llm_log(
                log_dir,
                candidate_id,
                {
                    "round": round_id,
                    "raw": raw_text,
                    "result": result,
                    "delta_messages": delta_messages,
                    "message_count": len(messages),
                },
            )
            return {"candidate_id": candidate_id, "result": result, "rounds": rounds, "messages": messages}

        feedback = _format_feedback(result)
        user_message = {"role": "user", "content": feedback}
        messages.append(user_message)
        delta_messages.append(user_message)
        _append_llm_log(
            log_dir,
            candidate_id,
            {
                "round": round_id,
                "raw": raw_text,
                "result": result,
                "delta_messages": delta_messages,
                "message_count": len(messages),
            },
        )

    return {"candidate_id": candidate_id, "result": None, "rounds": rounds, "messages": messages}


def run_pipeline(config: Dict[str, Any]) -> Dict[str, Any]:
    metadata = collect_metadata(config)
    output_dir = Path(str(config.get("project", {}).get("output_dir", "./output")))
    mcts_cfg = config.get("mcts", {})
    use_mcts = bool(mcts_cfg.get("enabled", False))

    best_candidate = None
    candidate_logs: List[Dict[str, Any]] = []

    result: Dict[str, Any] = {
        "best_candidate": None,
        "round_logs": candidate_logs,
        "metadata": metadata,
    }

    if use_mcts:
        mcts_result = run_offline_search(metadata, config)
        best_candidate = mcts_result.get("best_candidate")
        result["best_candidate"] = best_candidate
        result["mcts_search"] = mcts_result
    else:
        prompt_template = metadata.get("prompt_template", "")
        prompt_context = metadata.get("prompt_context", {})
        base_messages = build_messages(prompt_template, prompt_context)
        candidate_count = int(config.get("llm", {}).get("parallel", 1))
        log_dir = prepare_llm_log_dir(config)

        first_prompt_path = output_dir / "first_prompt.txt"
        first_prompt_path.write_text(_format_first_prompt(base_messages), encoding="utf-8")

        async def _run_all() -> List[Dict[str, Any]]:
            tasks = []
            for idx in range(1, max(candidate_count, 1) + 1):
                candidate_id = f"c{idx}"
                tasks.append(_run_candidate_loop(candidate_id, base_messages, metadata, config, log_dir))
            return await asyncio.gather(*tasks)

        candidate_logs = asyncio.run(_run_all())
        valid_results = [c["result"] for c in candidate_logs if c.get("result")]
        best_candidate = _pick_best(valid_results) if valid_results else None

        result["best_candidate"] = best_candidate
        result["round_logs"] = candidate_logs
        result["first_prompt_path"] = str(first_prompt_path)

    if best_candidate and best_candidate.get("candidate"):
        candidate = best_candidate.get("candidate", {})
        performance = best_candidate.get("performance", {}) if isinstance(best_candidate, dict) else {}
        total_delta = _safe_float(performance.get("total_delta"), 0.0)
        gain_ms = -total_delta

        print("\n最佳候选动作序列:")
        print(json.dumps(candidate, ensure_ascii=False, indent=2))
        direction = "变好" if total_delta <= 0 else "变差"
        print(f"预期延迟变化(ms): {total_delta:+.3f}（{direction}）")

        schema_apply_result = None
        if _prompt_yes_no("是否落库执行模式变更? (y/n):"):
            schema_apply_result = _interactive_schema_apply(candidate, config, output_dir)
            result["schema_apply"] = schema_apply_result
        else:
            result["schema_apply"] = {"status": "skipped", "reason": "user skipped"}

        if schema_apply_result and schema_apply_result.get("status") == "ok":
            if _prompt_yes_no("是否执行 SQL 重写? (y/n):"):
                sql_rewrite_cfg = config.get("sql_rewrite", {})
                if bool(sql_rewrite_cfg.get("enabled", False)):
                    rewrite_output_dir = str(sql_rewrite_cfg.get("output_dir") or output_dir)
                    sql_list = list(metadata.get("workload_sql", {}).values())
                    result["sql_rewrite"] = rewrite_sql(
                        sql_list, candidate, metadata.get("schema_state", {}), rewrite_output_dir
                    )
                else:
                    result["sql_rewrite"] = {"status": "skipped", "reason": "sql_rewrite disabled"}
            else:
                result["sql_rewrite"] = {"status": "skipped", "reason": "user skipped"}

        summary_path = output_dir / "pipeline_summary.json"
        summary_path.write_text(json.dumps(result, ensure_ascii=True, indent=2), encoding="utf-8")

    return result
