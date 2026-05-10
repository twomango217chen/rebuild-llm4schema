from __future__ import annotations

import copy
import json
import logging
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from schema_tuning.evaluators.compliance import check_compliance
from schema_tuning.evaluators.performance import estimate_performance
from schema_tuning.evaluators.storage import estimate_storage
from schema_tuning.llm.client import request_sequence
from schema_tuning.prompt.builder import build_messages
from schema_tuning.utils.filesystem import read_text
from schema_tuning.apply.schema_apply import (
    _update_schema_state_column_split,
    _update_schema_state_table_split,
    _update_schema_state_table_join,
    _update_schema_state_horizontal_split,
    _update_schema_state_horizontal_merge,
    _update_schema_state_redundant_add,
    _update_schema_state_redundant_drop,
)
from schema_tuning.collectors.metadata import (
    _collect_column_cooccurrence,
    _collect_join_key_history,
    _format_column_cooccurrence_text,
    _format_schema_summary_text,
    _format_workload_summary_text,
    _load_experience_hints_from_reference,
)
from schema_tuning.rewriter.sql_rewriter import _rewrite_for_action

logger = logging.getLogger(__name__)

_NODE_CONTEXT_MARKER = "<<NODE_CONTEXT>>"


@dataclass
class SchemaState:
    schema_id: str
    latency: float
    storage: float
    actions: List[Dict[str, Any]] = field(default_factory=list)
    evaluation: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    schema_state: Dict[str, Any] = field(default_factory=dict)
    workload_sql: Dict[str, str] = field(default_factory=dict)
    sql_ids: List[str] = field(default_factory=list)
    plans: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    storage_stats: Dict[str, Any] = field(default_factory=dict)
    selectivity_samples: Dict[str, Any] = field(default_factory=dict)
    prompt_context: Dict[str, Any] = field(default_factory=dict)
    rewrite_state: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchNode:
    state: SchemaState
    parent: Optional["SearchNode"] = None
    action: Optional[Dict[str, Any]] = None
    children: List["SearchNode"] = field(default_factory=list)
    visits: int = 0
    total_reward: float = 0.0
    best_reward: float = float("-inf")
    incoming_reward: float = 0.0
    blacklisted: bool = False
    expanded_keys: Set[str] = field(default_factory=set)
    nonpositive_streak: int = 0

    def q_value(self) -> float:
        return self.total_reward / self.visits if self.visits > 0 else 0.0

    def __repr__(self) -> str:
        action_type = self.action.get("type") if isinstance(self.action, dict) else None
        return (
            f"SearchNode(state={self.state.schema_id}, action={action_type}, "
            f"visits={self.visits}, q={self.q_value():.3f}, "
            f"best={self.best_reward:.3f}, bl={self.blacklisted})"
        )


def _safe_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _action_key(action: Dict[str, Any]) -> str:
    return json.dumps(action, ensure_ascii=True, sort_keys=True)


def _actions_key(actions: List[Dict[str, Any]]) -> str:
    return json.dumps(actions, ensure_ascii=True, sort_keys=True)


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        parts = stripped.split("```", 2)
        if len(parts) >= 3:
            return parts[1].strip()
    return stripped


def _parse_action_response(text: str) -> Optional[Dict[str, Any]]:
    cleaned = _strip_code_fence(text)
    if not cleaned:
        return None
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return None

    if isinstance(payload, dict):
        actions = payload.get("actions")
        if isinstance(actions, list) and actions:
            action = actions[0]
            return action if isinstance(action, dict) and action.get("type") else None
        if payload.get("type"):
            return payload
        candidate = payload.get("candidate")
        if isinstance(candidate, dict):
            actions = candidate.get("actions")
            if isinstance(actions, list) and actions:
                action = actions[0]
                return action if isinstance(action, dict) and action.get("type") else None
        return None

    if isinstance(payload, list):
        if payload and isinstance(payload[0], dict) and payload[0].get("type"):
            return payload[0]
    return None


def _resolve_template_path(path_value: str) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _rewrite_plan_text(plan_text: str, table_map: Dict[str, str]) -> str:
    if not plan_text or not table_map:
        return plan_text
    pattern = re.compile(
        r"(Table scan on|Index lookup on|Index scan on|Range scan on|Index range scan on)\s+(`?)([A-Za-z0-9_]+)(`?)",
        re.IGNORECASE,
    )

    def _replace(match: re.Match[str]) -> str:
        prefix = match.group(1)
        quote_left = match.group(2)
        table = match.group(3)
        quote_right = match.group(4)
        mapped = table_map.get(table, table)
        return f"{prefix} {quote_left}{mapped}{quote_right}"

    return pattern.sub(_replace, plan_text)


def _default_mcts_prompt_template() -> str:
    return """SYSTEM:
你是数据库性能调优专家，需要在当前节点的基础上提出一个候选动作，供 MCTS 选择扩展。
只输出一个 JSON 对象，不要额外文本。

USER:
数据库当前的 schema、字段类型、约束与行数如下：

{schema_summary_text}

历史负载统计与性能概览如下：

{workload_summary_text}

同表列共现统计如下：

{column_cooccurrence_text}

当前节点上下文（含已有动作与评估结果）：

{node_context}

规则：
{action_rules}

补充要求：
- 输出 JSON 仅包含一个对象。
- 必须包含 candidate_id 与 actions。
- actions 只包含 1 个动作。
- 候选动作必须与 current_actions 不重复，且必须基于当前节点的 schema 状态。
- 后续动作必须基于前序动作产生的新表/新列，不能引用已被删除的对象。
- 若 errors/warnings 提示某类操作无效或负优化，必须规避并给出替代方案。

补充说明：
- 候选动作请按预期收益从高到低排序。
- 候选动作必须与 current_actions 不重复，且必须基于当前节点的 schema 状态。
- 若 errors/warnings 提示某类操作无效或负优化，必须规避并给出替代方案。
"""


def _load_mcts_prompt_template(config: Dict[str, Any]) -> str:
    mcts_cfg = config.get("mcts", {})
    path_value = str(mcts_cfg.get("prompt_template_path") or "./references/mcts_prompt.md")
    if path_value:
        path = _resolve_template_path(path_value)
        if path.exists():
            return read_text(str(path))
    prompt_cfg = config.get("prompt", {})
    fallback_path = prompt_cfg.get("template_path")
    if fallback_path:
        path = _resolve_template_path(str(fallback_path))
        if path.exists():
            return read_text(str(path))
    return _default_mcts_prompt_template()


class MCTSSchemaSearch:
    def __init__(
        self,
        metadata: Dict[str, Any],
        config: Dict[str, Any],
        max_nodes: int = 10,
        max_depth: int = 4,
        alpha: float = 0.5,
        beta: float = 0.6,
        c: float = 1.0,
        seed: Optional[int] = None,
    ) -> None:
        self.metadata = metadata
        self.config = config
        self.max_nodes = max_nodes
        self.max_depth = max_depth
        self.alpha = alpha
        self.beta = beta
        self.c = c
        self.cache: Dict[str, Dict[str, Any]] = {}
        mcts_cfg = config.get("mcts", {})
        self.max_attempts_per_node = max(1, int(mcts_cfg.get("max_attempts_per_node", 3)))
        self.max_nonpositive_streak = max(1, int(mcts_cfg.get("max_nonpositive_streak", 3)))
        reward_cfg = mcts_cfg.get("reward", {}) if isinstance(mcts_cfg.get("reward", {}), dict) else {}
        self.latency_weight = _safe_float(reward_cfg.get("latency_weight"), 1.0)
        self.storage_penalty_weight = _safe_float(reward_cfg.get("storage_penalty_weight"), 1.0)
        self.storage_delta_weight = _safe_float(reward_cfg.get("storage_delta_weight"), -0.5)
        self.reject_positive_delta = bool(mcts_cfg.get("reject_positive_delta", True))
        self.positive_delta_epsilon = _safe_float(mcts_cfg.get("positive_delta_epsilon"), 0.0)
        self.prompt_template = _load_mcts_prompt_template(config)
        self.prompt_context = metadata.get("prompt_context", {})

        root_eval = self._evaluate_actions([], self.metadata)
        root_latency = _safe_float(root_eval.get("latency"), 0.0)
        root_storage = _safe_float(root_eval.get("storage_total"), 0.0)
        root_schema_state = copy.deepcopy(self.metadata.get("schema_state", {}))
        root_workload_sql = dict(self.metadata.get("workload_sql", {}))
        root_metrics = dict(self.metadata.get("metrics", {}))
        root_plans = dict(self.metadata.get("plans", {}))
        root_storage_stats = copy.deepcopy(self.metadata.get("storage_stats", {}))
        root_selectivity = dict(self.metadata.get("selectivity_samples", {}))
        root_prompt_context = dict(self.metadata.get("prompt_context", {}))
        root_state = SchemaState(
            schema_id="root",
            latency=root_latency,
            storage=root_storage,
            actions=[],
            evaluation=root_eval,
            description="root",
            schema_state=root_schema_state,
            workload_sql=root_workload_sql,
            sql_ids=list(root_workload_sql.keys()),
            plans=root_plans,
            metrics=root_metrics,
            storage_stats=root_storage_stats,
            selectivity_samples=root_selectivity,
            prompt_context=root_prompt_context,
            rewrite_state={},
        )

        self.root = SearchNode(state=root_state)
        self.nodes: List[SearchNode] = [self.root]

        self.latency_root = root_latency if root_latency > 0 else 1.0
        self.storage_root = root_storage if root_storage > 0 else 1.0
        self.storage_budget = self._resolve_storage_budget(root_storage)

    def _resolve_storage_budget(self, root_storage: float) -> float:
        limit = _safe_float(self.config.get("storage", {}).get("max_total_bytes"), 0.0)
        if limit > 0:
            return limit
        return max(root_storage * 2.0, root_storage + 1.0, 1.0)

    def _apply_action_to_schema_state(self, schema_state: Dict[str, Any], action: Dict[str, Any]) -> Dict[str, Any]:
        new_state = copy.deepcopy(schema_state)
        action_type = action.get("type")
        if action_type == "ColumnSplit":
            _update_schema_state_column_split(new_state, action)
        elif action_type == "TableSplit":
            _update_schema_state_table_split(new_state, action)
        elif action_type == "TableJoin":
            left = str(action.get("left_table", ""))
            right = str(action.get("right_table", ""))
            left_cols = list(new_state.get("tables", {}).get(left, {}).get("columns", {}).keys())
            right_cols = list(new_state.get("tables", {}).get(right, {}).get("columns", {}).keys())
            mapping_right = {col: col for col in right_cols}
            for col in right_cols:
                if col in left_cols:
                    mapping_right[col] = f"{right}__{col}"
            _update_schema_state_table_join(new_state, action, mapping_right)
        elif action_type == "HorizontalSplit":
            _update_schema_state_horizontal_split(new_state, action)
        elif action_type == "HorizontalMerge":
            _update_schema_state_horizontal_merge(new_state, action)
        elif action_type == "RedundantColumnAdd":
            _update_schema_state_redundant_add(new_state, action)
        elif action_type == "RedundantColumnDrop":
            _update_schema_state_redundant_drop(new_state, action)
        return new_state

    def _apply_storage_stats_delta(self, storage_stats: Dict[str, Any], storage_eval: Dict[str, Any]) -> Dict[str, Any]:
        new_stats = copy.deepcopy(storage_stats)
        table_sizes = new_stats.get("table_sizes")
        if not isinstance(table_sizes, dict):
            table_sizes = {}
        deltas = storage_eval.get("table_deltas") if isinstance(storage_eval, dict) else {}
        if isinstance(deltas, dict):
            for table, delta in deltas.items():
                base = table_sizes.get(table, 0)
                try:
                    base_val = int(base)
                except (TypeError, ValueError):
                    base_val = 0
                try:
                    delta_val = int(delta)
                except (TypeError, ValueError):
                    delta_val = 0
                table_sizes[str(table)] = base_val + delta_val
        new_stats["table_sizes"] = table_sizes
        total_after = storage_eval.get("total_size_after") if isinstance(storage_eval, dict) else None
        if isinstance(total_after, (int, float)):
            new_stats["total_size_before"] = int(total_after)
        else:
            new_stats["total_size_before"] = int(sum(int(v) for v in table_sizes.values())) if table_sizes else 0
        return new_stats

    def _rewrite_workload_sql(self, state: SchemaState, action: Dict[str, Any]) -> tuple[Dict[str, str], Dict[str, Any]]:
        sql_ids = list(state.sql_ids) if state.sql_ids else list(state.workload_sql.keys())
        sql_list = [state.workload_sql.get(sql_id, "") for sql_id in sql_ids]
        rewrite_state = state.rewrite_state or {}
        table_map = copy.deepcopy(rewrite_state.get("table_map", {}))
        column_map = copy.deepcopy(rewrite_state.get("column_map", {}))
        redundant_map = copy.deepcopy(rewrite_state.get("redundant_map", {}))
        join_map = copy.deepcopy(rewrite_state.get("join_map", {}))
        split_map = copy.deepcopy(rewrite_state.get("split_map", {}))
        sql_list = _rewrite_for_action(
            sql_list,
            action,
            state.schema_state,
            table_map,
            column_map,
            redundant_map,
            join_map,
            split_map,
        )
        new_sql = {sql_id: sql for sql_id, sql in zip(sql_ids, sql_list)}
        new_rewrite_state = {
            "table_map": table_map,
            "column_map": column_map,
            "redundant_map": redundant_map,
            "join_map": join_map,
            "split_map": split_map,
        }
        return new_sql, new_rewrite_state

    def _rewrite_plans(self, plans: Dict[str, str], table_map: Dict[str, str]) -> Dict[str, str]:
        if not isinstance(plans, dict) or not plans:
            return {}
        if not table_map:
            return dict(plans)
        return {sql_id: _rewrite_plan_text(text, table_map) for sql_id, text in plans.items()}

    def _create_zero_reward_child(self, node: SearchNode, reason: str) -> SearchNode:
        streak = node.nonpositive_streak + 1
        child_state = SchemaState(
            schema_id=f"{node.state.schema_id}->{len(self.nodes)}",
            latency=node.state.latency,
            storage=node.state.storage,
            actions=list(node.state.actions),
            evaluation=node.state.evaluation,
            description="no_op",
            schema_state=node.state.schema_state,
            workload_sql=node.state.workload_sql,
            sql_ids=list(node.state.sql_ids),
            plans=node.state.plans,
            metrics=node.state.metrics,
            storage_stats=node.state.storage_stats,
            selectivity_samples=node.state.selectivity_samples,
            prompt_context=node.state.prompt_context,
            rewrite_state=node.state.rewrite_state,
        )
        child_node = SearchNode(
            state=child_state,
            parent=node,
            action={"type": "NoOp", "reason": reason},
            incoming_reward=0.0,
            blacklisted=True,
            nonpositive_streak=streak,
        )
        node.children.append(child_node)
        self.nodes.append(child_node)
        self._backpropagate(child_node, 0.0)
        return child_node

    def _build_prompt_context(
        self,
        schema_state: Dict[str, Any],
        storage_stats: Dict[str, Any],
        workload_sql: Dict[str, str],
        metrics: Dict[str, Any],
        plans: Dict[str, str],
        selectivity_samples: Dict[str, Any],
    ) -> Dict[str, Any]:
        schema_summary_text = _format_schema_summary_text(schema_state, storage_stats)
        cooccurrence = _collect_column_cooccurrence(workload_sql, schema_state)
        join_key_history = _collect_join_key_history(workload_sql, schema_state)
        workload_summary = json.dumps(
            {
                "sql_count": len(workload_sql),
                "metrics": metrics,
                "column_cooccurrence": cooccurrence,
                "join_key_history": join_key_history,
                "selectivity_samples": selectivity_samples,
            },
            ensure_ascii=True,
        )
        workload_summary_text = _format_workload_summary_text(metrics, workload_sql, plans, schema_state, storage_stats)
        column_cooccurrence_text = _format_column_cooccurrence_text(cooccurrence)
        schema_summary = json.dumps(schema_state, ensure_ascii=True)
        experience_hints = _load_experience_hints_from_reference()
        return {
            "schema_summary": schema_summary,
            "workload_summary": workload_summary,
            "schema_summary_text": schema_summary_text,
            "workload_summary_text": workload_summary_text,
            "column_cooccurrence_text": column_cooccurrence_text,
            "experience_hints": experience_hints,
        }

    def _evaluate_actions(self, actions: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
        key = _actions_key(actions)
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        candidate = {"candidate_id": key, "actions": actions}
        schema_state = metadata.get("schema_state", {})
        storage_stats = metadata.get("storage_stats", {})
        compliance = check_compliance(schema_state, candidate)
        storage = estimate_storage(
            storage_stats,
            candidate,
            self.config,
            schema_state,
        )
        performance = estimate_performance(metadata, candidate, self.config)

        baseline = _safe_float(performance.get("baseline_total_latency_ms"), 0.0)
        total_delta = _safe_float(performance.get("total_delta"), 0.0)
        latency = baseline + total_delta
        storage_total = _safe_float(storage.get("total_size_after"), _safe_float(storage.get("total_size_before"), 0.0))
        is_valid = bool(compliance.get("is_valid") and not storage.get("limit_exceeded"))
        if self.reject_positive_delta and total_delta > self.positive_delta_epsilon:
            is_valid = False
            warnings = list(compliance.get("warnings", [])) if isinstance(compliance.get("warnings", []), list) else []
            warnings.append(
                f"performance gate: total_delta {total_delta:.6f} > {self.positive_delta_epsilon:.6f}"
            )
            compliance["warnings"] = warnings

        snapshot = {
            "candidate": candidate,
            "compliance": compliance,
            "storage": storage,
            "performance": performance,
            "latency": latency,
            "storage_total": storage_total,
            "is_valid": is_valid,
        }
        self.cache[key] = snapshot
        return snapshot

    def _reward(self, parent: SchemaState, child: SchemaState) -> float:
        latency_gain = (parent.latency - child.latency) / self.latency_root
        storage_penalty = self.alpha * (child.storage / self.storage_budget)
        storage_delta = (child.storage - parent.storage) / self.storage_root
        return (
            self.latency_weight * latency_gain
            - self.storage_penalty_weight * storage_penalty
            + self.storage_delta_weight * storage_delta
        )

    def _score_node(self, node: SearchNode) -> float:
        if node.visits == 0:
            return float("inf")

        parent_visits = node.parent.visits if node.parent else max(1, node.visits)
        exploration = self.c * math.sqrt(math.log(max(1, parent_visits)) / node.visits)
        q = node.q_value()
        v = node.best_reward if node.best_reward != float("-inf") else 0.0
        return self.beta * q + (1.0 - self.beta) * v + exploration

    def _select_node(self) -> SearchNode:
        candidates = [
            node
            for node in self.nodes
            if not node.blacklisted and len(node.state.actions) < self.max_depth
        ]
        if not candidates:
            return self.root
        return max(candidates, key=self._score_node)

    def _build_node_context(self, node: SearchNode) -> str:
        evaluation = node.state.evaluation or {}
        performance = evaluation.get("performance", {})
        storage = evaluation.get("storage", {})
        compliance = evaluation.get("compliance", {})
        payload = {
            "current_actions": node.state.actions,
            "current_latency_ms": node.state.latency,
            "current_storage_bytes": node.state.storage,
            "storage_budget_bytes": self.storage_budget,
            "last_total_delta_ms": performance.get("total_delta"),
            "last_op_deltas_ms": performance.get("op_deltas"),
            "storage_total_delta": storage.get("total_delta"),
            "warnings": compliance.get("warnings", []),
            "errors": compliance.get("errors", []),
        }
        return json.dumps(payload, ensure_ascii=True)

    def _build_messages_for_node(self, node: SearchNode) -> List[Dict[str, str]]:
        node_context = self._build_node_context(node)
        template_text = self.prompt_template
        context = dict(node.state.prompt_context)
        context["node_context"] = node_context
        if _NODE_CONTEXT_MARKER in template_text:
            template_text = template_text.replace(_NODE_CONTEXT_MARKER, node_context)
        return build_messages(template_text, context)

    def _propose_action_llm(self, node: SearchNode) -> Optional[Dict[str, Any]]:
        messages = self._build_messages_for_node(node)
        try:
            response = request_sequence(messages, self.config)
        except Exception as exc:
            logger.warning("LLM request failed: %s", exc)
            return None
        action = _parse_action_response(response)
        if not action:
            logger.warning("LLM response not parsed as action")
            return None
        return action

    def _backpropagate(self, node: SearchNode, reward: float) -> None:
        current = node
        while current is not None:
            current.visits += 1
            current.total_reward += reward
            if reward > current.best_reward:
                current.best_reward = reward
            current = current.parent

    def _expand_node(self, node: SearchNode) -> SearchNode:
        used = {_action_key(action) for action in node.state.actions}
        last_invalid_action: Optional[Dict[str, Any]] = None
        last_invalid_eval: Optional[Dict[str, Any]] = None
        last_reason = "LLM did not return a valid action"

        for _ in range(self.max_attempts_per_node):
            action = self._propose_action_llm(node)
            if not action:
                last_reason = "LLM returned empty action"
                continue
            if not isinstance(action, dict) or not action.get("type"):
                last_reason = "LLM action missing type"
                last_invalid_action = action if isinstance(action, dict) else last_invalid_action
                continue

            key = _action_key(action)
            if key in node.expanded_keys or key in used:
                last_reason = "LLM returned duplicate action"
                last_invalid_action = dict(action)
                continue
            node.expanded_keys.add(key)

            actions = list(node.state.actions) + [dict(action)]
            evaluation = self._evaluate_actions(actions, self.metadata)
            if not evaluation.get("is_valid"):
                last_reason = "action invalid by compliance/storage"
                last_invalid_action = dict(action)
                last_invalid_eval = evaluation
                continue

            new_schema_state = self._apply_action_to_schema_state(node.state.schema_state, action)
            new_workload_sql, new_rewrite_state = self._rewrite_workload_sql(node.state, action)
            new_storage_stats = self._apply_storage_stats_delta(node.state.storage_stats, evaluation.get("storage", {}))
            new_plans = self._rewrite_plans(node.state.plans, new_rewrite_state.get("table_map", {}))
            new_prompt_context = self._build_prompt_context(
                new_schema_state,
                new_storage_stats,
                new_workload_sql,
                node.state.metrics,
                new_plans,
                node.state.selectivity_samples,
            )

            child_state = SchemaState(
                schema_id=f"{node.state.schema_id}->{len(self.nodes)}",
                latency=_safe_float(evaluation.get("latency"), 0.0),
                storage=_safe_float(evaluation.get("storage_total"), 0.0),
                actions=actions,
                evaluation=evaluation,
                description=str(action.get("type", "")),
                schema_state=new_schema_state,
                workload_sql=new_workload_sql,
                sql_ids=list(node.state.sql_ids),
                plans=new_plans,
                metrics=node.state.metrics,
                storage_stats=new_storage_stats,
                selectivity_samples=node.state.selectivity_samples,
                prompt_context=new_prompt_context,
                rewrite_state=new_rewrite_state,
            )

            reward = self._reward(node.state, child_state)
            streak = node.nonpositive_streak + 1 if reward <= 0.0 else 0
            child_node = SearchNode(
                state=child_state,
                parent=node,
                action=action,
                incoming_reward=reward,
                nonpositive_streak=streak,
            )
            if streak >= self.max_nonpositive_streak:
                child_node.blacklisted = True
            node.children.append(child_node)
            self.nodes.append(child_node)
            self._backpropagate(child_node, reward)
            return child_node

        node.blacklisted = True
        invalid_payload: Dict[str, Any] = {"reason": last_reason}
        if last_invalid_action is not None:
            invalid_payload["action"] = last_invalid_action
        if isinstance(last_invalid_eval, dict):
            compliance = last_invalid_eval.get("compliance", {})
            invalid_payload["errors"] = compliance.get("errors", [])
            invalid_payload["warnings"] = compliance.get("warnings", [])
        return self._create_zero_reward_child(node, json.dumps(invalid_payload, ensure_ascii=True))



    def run(self) -> None:
        while len(self.nodes) < self.max_nodes:
            selected = self._select_node()
            expanded = self._expand_node(selected)
            if expanded is selected and selected is self.root and selected.blacklisted:
                break

    def _path_reward(self, node: SearchNode) -> float:
        reward = 0.0
        current = node
        while current is not None:
            reward += current.incoming_reward
            current = current.parent
        return reward

    def best_path(self) -> List[SearchNode]:
        leaf_nodes = [node for node in self.nodes if not node.children]
        if not leaf_nodes:
            leaf_nodes = self.nodes
        best = max(leaf_nodes, key=self._path_reward)
        path: List[SearchNode] = []
        current: Optional[SearchNode] = best
        while current is not None:
            path.append(current)
            current = current.parent
        return list(reversed(path))

    def tree_summary(self) -> str:
        lines: List[str] = []
        for node in self.nodes:
            action_type = node.action.get("type") if isinstance(node.action, dict) else "root"
            line = (
                f"{node.state.schema_id}: action={action_type}, "
                f"latency={node.state.latency:.1f}, storage={node.state.storage:.1f}, "
                f"visits={node.visits}, q={node.q_value():.3f}, best={node.best_reward:.3f}"
            )
            lines.append(line)
        return "\n".join(lines)


def run_offline_search(metadata: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    mcts_cfg = config.get("mcts", {})
    search = MCTSSchemaSearch(
        metadata=metadata,
        config=config,
        max_nodes=int(mcts_cfg.get("max_nodes", 10)),
        max_depth=int(mcts_cfg.get("max_depth", 4)),
        alpha=_safe_float(mcts_cfg.get("alpha", 0.5), 0.5),
        beta=_safe_float(mcts_cfg.get("beta", 0.6), 0.6),
        c=_safe_float(mcts_cfg.get("c", 1.0), 1.0),
        seed=mcts_cfg.get("seed"),
    )
    search.run()

    best_path = search.best_path()
    best_node = best_path[-1] if best_path else search.root
    best_eval = best_node.state.evaluation or {}
    candidate = best_eval.get("candidate")

    best_candidate = None
    if isinstance(candidate, dict) and candidate.get("actions"):
        best_candidate = {
            "candidate_id": candidate.get("candidate_id", "mcts"),
            "candidate": candidate,
            "compliance": best_eval.get("compliance", {}),
            "storage": best_eval.get("storage", {}),
            "performance": best_eval.get("performance", {}),
        }

    path_summary = [
        {
            "schema_id": node.state.schema_id,
            "action": node.action,
            "latency": node.state.latency,
            "storage": node.state.storage,
            "reward": node.incoming_reward,
        }
        for node in best_path
    ]

    return {
        "best_candidate": best_candidate,
        "best_path": path_summary,
        "tree_summary": search.tree_summary(),
        "node_count": len(search.nodes),
    }


def run_demo(config_path: str = "./framework/configs/default.yaml") -> Dict[str, Any]:
    from schema_tuning.collectors.metadata import collect_metadata
    from schema_tuning.config import load_config

    config = load_config(config_path)
    metadata = collect_metadata(config)
    result = run_offline_search(metadata, config)

    print("MCTS offline search demo starting...")
    print("\nTree summary:")
    print(result.get("tree_summary", ""))
    best_candidate = result.get("best_candidate")
    if best_candidate:
        print("\nBest candidate:")
        print(json.dumps(best_candidate.get("candidate", {}), ensure_ascii=False, indent=2))
        performance = best_candidate.get("performance", {})
        print(f"total_delta(ms): {performance.get('total_delta')}")
    else:
        print("\nNo valid candidate found.")
    print("Demo complete.")
    return result
