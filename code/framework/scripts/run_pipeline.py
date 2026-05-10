from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
SRC = ROOT / "src"
for path in (REPO_ROOT, SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from schema_tuning.config import load_config
from schema_tuning.pipeline.orchestrator import run_pipeline
from schema_tuning.logging_utils import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config YAML")
    args = parser.parse_args()

    setup_logging()
    config = load_config(args.config)
    run_pipeline(config)


if __name__ == "__main__":
    main()
