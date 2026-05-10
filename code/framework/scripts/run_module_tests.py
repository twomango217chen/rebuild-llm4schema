from __future__ import annotations

import argparse
import asyncio
import logging
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
from schema_tuning.evaluators.compliance import check_compliance
from schema_tuning.evaluators.performance import estimate_performance
from schema_tuning.evaluators.storage import estimate_storage
from schema_tuning.llm.client import request_sequence_async
from schema_tuning.logging_utils import setup_logging
from schema_tuning.prompt.builder import build_messages
from schema_tuning.rewriter.sql_rewriter import rewrite_sql
from schema_tuning.apply.schema_apply import apply_schema
from schema_tuning.workload.runner import load_workload_sql, resolve_workload_paths, run_workload_map, write_metrics_csv

logger = logging.getLogger(__name__)


def _module_header(name: str) -> None:
    print("\n" + "=" * 8 + f" {name} " + "=" * 8)


def _sample_candidate() -> dict:
    return {
        "candidate_id": "module_test",
        "actions": [
            {
                "type": "ColumnSplit",
                "table": "CUSTOMER",
                "column": "C_NAME",
                "delimiter": " ",
                "new_columns": ["C_NAME_FIRST", "C_NAME_LAST"],
                "keep_original": True,
            }
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config YAML")
    args = parser.parse_args()

    setup_logging()
    config = load_config(args.config)

    _module_header("metadata.collector")
    metadata = collect_metadata(config)
    schema_state = metadata.get("schema_state", {})
    storage_stats = metadata.get("storage_stats", {})
    workload_sql = metadata.get("workload_sql", {})
    plans = metadata.get("plans", {})
    metrics = metadata.get("metrics", {})
    print("tables:", len(schema_state.get("tables", {})))
    print("workload sql:", len(workload_sql))
    print("plans:", len(plans))
    print("metrics:", len(metrics))

    candidate = _sample_candidate()

    _module_header("evaluator.compliance")
    compliance = check_compliance(schema_state, candidate)
    print("is_valid:", compliance.get("is_valid"))
    print("errors:", compliance.get("errors"))
    print("warnings:", compliance.get("warnings"))

    _module_header("evaluator.storage")
    storage = estimate_storage(storage_stats, candidate, config, schema_state)
    print("limit_exceeded:", storage.get("limit_exceeded"))
    print("total_delta:", storage.get("total_delta"))

    _module_header("evaluator.performance")
    performance = estimate_performance(metadata, candidate, config)
    print("baseline_total_latency_ms:", performance.get("baseline_total_latency_ms"))
    print("total_delta:", performance.get("total_delta"))

    _module_header("workload.runner")
    sql_dir, metrics_path = resolve_workload_paths(config, None)
    sql_map = load_workload_sql(sql_dir)
    workload_result = run_workload_map(sql_map, config)
    output_dir = Path(str(config.get("project", {}).get("output_dir", "./output")))
    metrics_out = output_dir / "module_metrics.csv"
    write_metrics_csv(workload_result.get("metrics", []), metrics_out)
    print("sql_dir:", sql_dir)
    print("metrics_output:", metrics_out)
    print("total_latency_ms:", workload_result.get("total_latency_ms"))

    _module_header("llm.orchestrator")
    messages = build_messages(metadata.get("prompt_template", ""), metadata.get("prompt_context", {}))
    try:
        llm_text = asyncio.run(request_sequence_async(messages, config))
        print("llm_response:", llm_text)
    except Exception as exc:
        print("llm_error:", str(exc))

    _module_header("rewriter.sql_rewriter")
    rewrite_sql(list(workload_sql.values())[:1], candidate, schema_state, str(output_dir / "sql_rewrite"))
    print("status: completed")

    _module_header("apply.schema_apply")
    apply_result = apply_schema(candidate, config)
    print("result:", apply_result)


if __name__ == "__main__":
    main()
