from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from schema_tuning.config import load_config
from schema_tuning.logging_utils import setup_logging
from schema_tuning.workload.runner import (
    load_workload_sql,
    resolve_workload_paths,
    run_workload_map,
    write_metrics_csv,
)

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config YAML")
    parser.add_argument("--output", default=None, help="Override output metrics CSV path")
    args = parser.parse_args()

    setup_logging()
    config = load_config(args.config)

    sql_dir, metrics_path = resolve_workload_paths(config, args.output)
    workload_sql = load_workload_sql(sql_dir)
    if not workload_sql:
        raise ValueError(f"no workload SQL found in {sql_dir}")

    result = run_workload_map(workload_sql, config)
    write_metrics_csv(result.get("metrics", []), metrics_path)

    logger.info("metrics written to %s", metrics_path)
    logger.info("total_latency_ms=%.3f", float(result.get("total_latency_ms", 0.0)))


if __name__ == "__main__":
    main()
