from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
SRC = ROOT / "src"
for path in (REPO_ROOT, SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from schema_tuning.collectors.metadata import collect_metadata
from schema_tuning.config import load_config
from schema_tuning.logging_utils import prepare_llm_log_dir, setup_logging
from schema_tuning.pipeline.orchestrator import _run_candidate_loop
from schema_tuning.prompt.builder import build_messages


def _print_prompt(messages: list[dict]) -> None:
    for message in messages:
        role = message.get("role", "")
        content = message.get("content", "")
        print(f"[{role.upper()}]\n{content}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config YAML")
    parser.add_argument("--candidate-id", default="c1", help="Candidate id for log naming")
    args = parser.parse_args()

    setup_logging()
    config = load_config(args.config)

    metadata = collect_metadata(config)
    prompt_template = metadata.get("prompt_template", "")
    prompt_context = metadata.get("prompt_context", {})
    base_messages = build_messages(prompt_template, prompt_context)

    print("=== Initial Prompt ===")
    _print_prompt(base_messages)

    log_dir = prepare_llm_log_dir(config)

    result = asyncio.run(
        _run_candidate_loop(
            args.candidate_id,
            base_messages,
            metadata,
            config,
            log_dir,
        )
    )

    log_path = log_dir / f"{args.candidate_id}.ndjson"
    print("=== Conversation Log Path ===")
    print(log_path)
    if log_path.exists():
        print("=== Conversation Log ===")
        print(log_path.read_text(encoding="utf-8"))
    if result.get("result"):
        print("=== Final Result ===")
        print(result.get("result"))


if __name__ == "__main__":
    main()
