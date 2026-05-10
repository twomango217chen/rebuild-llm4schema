from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from schema_tuning.collectors.metadata import collect_metadata
from schema_tuning.config import load_config
from schema_tuning.search.mcts_schema_search import run_offline_search


def main() -> None:
    parser = argparse.ArgumentParser(description="Run offline MCTS schema search")
    parser.add_argument("--config", default="./framework/configs/default.yaml", help="Config YAML path")
    args = parser.parse_args()

    print("运行 schema search demo...")
    config = load_config(args.config)
    metadata = collect_metadata(config)
    result = run_offline_search(metadata, config)

    print("\nTree summary:")
    print(result.get("tree_summary", ""))
    best_candidate = result.get("best_candidate")
    if best_candidate:
        print("\nBest candidate:")
        print(best_candidate.get("candidate"))
    else:
        print("\nNo valid candidate found.")


if __name__ == "__main__":
    main()
