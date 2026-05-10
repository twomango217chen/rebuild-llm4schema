from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
import re
from typing import Any, Dict, List

import aiohttp

import references.llm as llm_module
from references.llm import call_llm_api
from schema_tuning.logging_utils import resolve_llm_log_dir

logger = logging.getLogger(__name__)


def _apply_llm_config(config: Dict[str, Any]) -> None:
    llm_cfg = config.get("llm", {})

    def _set_env(name: str, value: Any) -> None:
        if value is None:
            return
        if isinstance(value, bool):
            os.environ[name] = "true" if value else "false"
            return
        if isinstance(value, (int, float, str)):
            text = str(value).strip()
            if text:
                os.environ[name] = text
            return
        if isinstance(value, (dict, list)):
            os.environ[name] = json.dumps(value, ensure_ascii=True)

    def _resolve_value(direct_key: str, env_key: str) -> str | None:
        direct = llm_cfg.get(direct_key)
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        env_name = llm_cfg.get(env_key)
        if isinstance(env_name, str) and env_name.strip():
            env_name = env_name.strip()
            if env_name in os.environ:
                return os.environ[env_name]
            if env_key == "api_key_env" and env_name.startswith("sk-"):
                return env_name
            if env_key in {"api_url_env", "api_base_env"} and env_name.startswith("http"):
                return env_name
        return None

    api_key = _resolve_value("api_key", "api_key_env")
    api_url = _resolve_value("api_url", "api_url_env")
    api_base = _resolve_value("api_base", "api_base_env")
    model = _resolve_value("model", "model_env")
    response_format = llm_cfg.get("response_format")

    if isinstance(api_key, str) and api_key:
        os.environ["LLM_API_KEY"] = api_key
        llm_module.api_key = api_key
    if isinstance(api_url, str) and api_url:
        os.environ["LLM_API_URL"] = api_url
        llm_module.url = api_url
    if isinstance(api_base, str) and api_base:
        os.environ["LLM_API_BASE"] = api_base
    if isinstance(model, str) and model:
        os.environ["LLM_MODEL"] = model
        llm_module.model = model
    if isinstance(response_format, str) and response_format:
        os.environ["LLM_RESPONSE_FORMAT"] = response_format

    # Optional request controls for higher-quality model calls.
    _set_env("LLM_TEMPERATURE", llm_cfg.get("temperature"))
    _set_env("LLM_TOP_P", llm_cfg.get("top_p"))
    _set_env("LLM_MAX_COMPLETION_TOKENS", llm_cfg.get("max_completion_tokens"))
    _set_env("LLM_TIMEOUT_SEC", llm_cfg.get("timeout_sec"))
    _set_env("LLM_MAX_RETRIES", llm_cfg.get("request_retries"))
    _set_env("LLM_EXTRA_BODY_JSON", llm_cfg.get("extra_body"))
    _set_env("LLM_EXTRA_HEADERS_JSON", llm_cfg.get("extra_headers"))


def _extract_content(response: Dict[str, Any]) -> str:
    if not isinstance(response, dict):
        raise ValueError("LLM response is not a dict")

    choices = response.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            message = choice.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()
            content = choice.get("text") or choice.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
            delta = choice.get("delta")
            if isinstance(delta, dict):
                content = delta.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()

    for key in ("content", "result", "output", "answer"):
        value = response.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    keys = ",".join(sorted(response.keys()))
    raise ValueError(f"LLM response missing content (keys={keys})")


def _append_response_log(config: Dict[str, Any], response: Dict[str, Any]) -> None:
    log_dir = resolve_llm_log_dir(config)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "llm_response_errors.ndjson"
    payload = {"response": response}
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


async def _request_live(messages: list[dict], config: Dict[str, Any]) -> str:
    _apply_llm_config(config)
    llm_cfg = config.get("llm", {})
    retries = int(llm_cfg.get("retries", 2))
    delay = float(llm_cfg.get("retry_delay_sec", 1.0))
    last_exc: Exception | None = None
    async with aiohttp.ClientSession() as session:
        for attempt in range(retries + 1):
            response = await call_llm_api(session, messages)
            try:
                return _extract_content(response)
            except ValueError as exc:
                last_exc = exc
                _append_response_log(config, response)
                if attempt < retries:
                    logger.warning("LLM response missing content, retrying (%s/%s)", attempt + 1, retries)
                    await asyncio.sleep(delay)
                    continue
                raise
    raise last_exc or ValueError("LLM response missing content")


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_format_value(item) for item in value) + "]"
    text = str(value)
    if re.fullmatch(r"[A-Za-z0-9_]+", text):
        return text
    escaped = text.replace("\\", "\\\\").replace("\"", "\\\"")
    return f'"{escaped}"'


def _format_candidate_actions(candidate: Dict[str, Any]) -> str:
    actions = candidate.get("actions", []) if isinstance(candidate, dict) else []
    if not isinstance(actions, list) or not actions:
        return "NoOp()"
    lines: List[str] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        action_type = action.get("type")
        if not action_type:
            continue
        params = []
        for key in sorted(k for k in action.keys() if k != "type"):
            params.append(f"{key}={_format_value(action[key])}")
        line = f"{action_type}({', '.join(params)})" if params else f"{action_type}()"
        lines.append(line)
    return "\n".join(lines) if lines else "NoOp()"


def _load_mock_candidates(llm_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    inline = llm_cfg.get("mock_candidates_json", [])
    if isinstance(inline, list) and inline:
        return inline
    single = llm_cfg.get("single_candidate", {})
    if isinstance(single, dict) and single:
        return [single]
    file_path = llm_cfg.get("mock_candidates_file")
    if file_path:
        path = Path(str(file_path)).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        if path.exists():
            content = path.read_text(encoding="utf-8").strip()
            if content:
                payload = json.loads(content)
                if isinstance(payload, list):
                    return [item for item in payload if isinstance(item, dict)]
                if isinstance(payload, dict):
                    return [payload]
    return []


async def _request_mock(config: Dict[str, Any]) -> str:
    llm_cfg = config.get("llm", {})
    candidates = _load_mock_candidates(llm_cfg)
    candidate = candidates[0] if candidates else {}
    if isinstance(candidate, dict) and candidate:
        return json.dumps(candidate, ensure_ascii=True)
    return json.dumps({"candidate_id": "mock", "actions": []}, ensure_ascii=True)


async def request_sequence_async(messages: list[dict], config: Dict[str, Any]) -> str:
    llm_cfg = config.get("llm", {})
    mode = llm_cfg.get("mode")
    if mode == "live":
        return await _request_live(messages, config)
    if mode == "mock":
        return await _request_mock(config)
    raise ValueError("llm.mode must be 'live' or 'mock'")


def request_sequence(messages: list[dict], config: Dict[str, Any]) -> str:
    return asyncio.run(request_sequence_async(messages, config))
