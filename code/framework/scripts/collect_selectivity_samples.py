from __future__ import annotations

import argparse
from pathlib import Path

from schema_tuning.collectors.metadata import _resolve_output_path, collect_metadata
from schema_tuning.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect selectivity samples from live database")
    parser.add_argument("--config", default="./framework/configs/default.yaml", help="Config YAML path")
    parser.add_argument(
        "--output",
        default="",
        help="Output path for selectivity_samples.json (default: metadata.selectivity_output_path)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    metadata_cfg = config.setdefault("metadata", {})
    metadata_cfg["collect_selectivity"] = True
    metadata_cfg["collect_explain"] = False
    metadata_cfg["explain_only"] = False
    if args.output:
        metadata_cfg["selectivity_output_path"] = args.output

    metadata = collect_metadata(config)
    samples = metadata.get("selectivity_samples", {})
    coverage = samples.get("coverage", {}) if isinstance(samples, dict) else {}

    workload_cfg = config.get("workload", {})
    dataset_root = Path(str(workload_cfg.get("dataset_root", ""))).expanduser()
    output_path = _resolve_output_path(
        dataset_root,
        metadata_cfg.get("selectivity_output_path"),
        "selectivity_samples.json",
    )

    print("Selectivity sampling complete.")
    print(f"Output: {output_path}")
    if coverage:
        print("Coverage:")
        for key in sorted(coverage.keys()):
            print(f"- {key}: {coverage.get(key)}")


if __name__ == "__main__":
    main()
