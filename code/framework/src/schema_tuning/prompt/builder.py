from __future__ import annotations

from typing import Any, Dict, List


ACTION_FORMAT = """JSON 格式:
{
    "candidate_id": "c1",
    "actions": [
        {
            "type": "ColumnSplit",
            "table": "T",
            "column": "col",
            "delimiter": " ",
            "new_columns": ["col_part1", "col_part2"],
            "keep_original": true
        }
    ]
}
"""


ACTION_RULES = """规则:
- 只输出一个 JSON 对象，不要输出额外文本或代码块。
- 键和值中的字符串必须使用双引号。
- 布尔值必须为 true/false。
- 所有表名/列名必须来自 schema 摘要。
- 新建表/列名不能与已有名称冲突。
- TableJoin 的 join_keys 必须同时存在于两表。
- TableJoin 的 select_columns 必须使用 left.col 或 right.col。
- TableJoin 的 join_type 必须符合 keep_original 规则。
- RedundantColumnAdd 的 join_keys 必须来自外键或历史负载连接键。
- 仅使用 MySQL 8.0+ 兼容语法。
"""

_MARKER_SCHEMA = "<<SCHEMA_SUMMARY>>"
_MARKER_WORKLOAD = "<<WORKLOAD_SUMMARY>>"
_MARKER_COOCCURRENCE = "<<COLUMN_COOCCURRENCE>>"
_MARKER_EXPERIENCE = "<<EXPERIENCE_HINTS>>"


def _needs_format(template_text: str) -> bool:
    tokens = (
        "{schema_summary}",
        "{workload_summary}",
        "{action_format}",
        "{action_rules}",
        "{experience_hints}",
    )
    return any(token in template_text for token in tokens)


def _apply_marker_replacements(template_text: str, context: Dict[str, Any]) -> tuple[str, bool]:
    replacements = {
        _MARKER_SCHEMA: context.get("schema_summary_text") or context.get("schema_summary", ""),
        _MARKER_WORKLOAD: context.get("workload_summary_text") or context.get("workload_summary", ""),
        _MARKER_COOCCURRENCE: context.get("column_cooccurrence_text", ""),
        _MARKER_EXPERIENCE: context.get("experience_hints", ""),
    }
    rendered = template_text
    used = False
    for marker, value in replacements.items():
        if marker in rendered:
            rendered = rendered.replace(marker, str(value))
            used = True
    return rendered, used


def build_prompt(template_text: str, context: Dict[str, Any]) -> str:
    """Build a prompt from template and context.

    Use template content and ensure real schema/workload summaries are present.
    """
    merged = dict(context)
    merged["action_format"] = ACTION_FORMAT
    merged["action_rules"] = ACTION_RULES
    merged.setdefault("experience_hints", "")

    rendered, used_markers = _apply_marker_replacements(template_text, merged)
    if not used_markers and _needs_format(template_text):
        rendered = template_text.format(**merged)

    if "SYSTEM:" in rendered and "USER:" in rendered:
        return rendered

    system_content = rendered.strip()

    if used_markers:
        user_content = "OK"
    else:
        user_content = "Schema Summary:\n" + str(merged.get("schema_summary", ""))
        user_content += "\n\nWorkload Summary:\n" + str(merged.get("workload_summary", ""))
        experience_hints = str(merged.get("experience_hints", "")).strip()
        if experience_hints:
            user_content += "\n\nExperience Hints:\n" + experience_hints

    return "SYSTEM:\n" + system_content + "\n\nUSER:\n" + user_content


def build_messages(template_text: str, context: Dict[str, Any]) -> List[Dict[str, str]]:
    rendered = build_prompt(template_text, context)
    if "SYSTEM:" not in rendered or "USER:" not in rendered:
        raise ValueError("prompt template must include SYSTEM: and USER: sections")
    system_part, user_part = rendered.split("USER:", 1)
    _, system_content = system_part.split("SYSTEM:", 1)
    system_content = system_content.strip()
    user_content = user_part.strip()
    experience_hints = str(context.get("experience_hints", "")).strip()
    if experience_hints and "经验" not in user_content and "Experience Hints" not in user_content:
        user_content += "\n\nExperience Hints:\n" + experience_hints
    if not system_content or not user_content:
        raise ValueError("prompt template SYSTEM/USER content is empty")
    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content},
    ]
