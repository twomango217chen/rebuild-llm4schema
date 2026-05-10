from __future__ import annotations

import argparse
import copy
import itertools
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
RUN_PIPELINE = ROOT / "scripts" / "run_pipeline.py"


def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=False)


def _set_nested(payload: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    current: Dict[str, Any] = payload
    for key in parts[:-1]:
        node = current.get(key)
        if not isinstance(node, dict):
            node = {}
            current[key] = node
        current = node
    current[parts[-1]] = value


def _to_slug(parts: List[str]) -> str:
    text = "__".join(parts).strip()
    if not text:
        return "default"
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_", "."}:
            cleaned.append(ch)
        else:
            cleaned.append("-")
    return "".join(cleaned)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _run_single(config_path: Path, run_dir: Path) -> Dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    command = [sys.executable, str(RUN_PIPELINE), "--config", str(config_path)]
    proc = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        input="n\n",
        text=True,
        capture_output=True,
        check=False,
    )

    (run_dir / "stdout.log").write_text(proc.stdout or "", encoding="utf-8")
    (run_dir / "stderr.log").write_text(proc.stderr or "", encoding="utf-8")

    summary_path = run_dir / "pipeline_summary.json"
    summary_payload: Dict[str, Any] = {}
    if summary_path.exists():
        summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))

    best = summary_payload.get("best_candidate") if isinstance(summary_payload, dict) else {}
    if not isinstance(best, dict):
        best = {}
    perf = best.get("performance") if isinstance(best.get("performance"), dict) else {}
    compliance = best.get("compliance") if isinstance(best.get("compliance"), dict) else {}

    baseline = _safe_float(perf.get("baseline_total_latency_ms"), 0.0)
    total_delta = _safe_float(perf.get("total_delta"), 0.0)
    new_total = baseline + total_delta
    improvement_pct = ((baseline - new_total) / baseline * 100.0) if baseline > 0 else 0.0

    return {
        "exit_code": proc.returncode,
        "run_dir": str(run_dir),
        "summary_found": summary_path.exists(),
        "best_candidate_found": bool(best),
        "compliance_valid": _safe_bool(compliance.get("is_valid")),
        "baseline_ms": baseline,
        "total_delta_ms": total_delta,
        "new_ms": new_total,
        "improvement_pct": improvement_pct,
    }


def _aggregate(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    valid_rows = [r for r in rows if r.get("summary_found") and r.get("best_candidate_found")]
    if not valid_rows:
        return {
            "runs": len(rows),
            "all_completed": all(r.get("exit_code") == 0 for r in rows),
            "all_valid": False,
            "avg_total_delta_ms": 0.0,
            "avg_improvement_pct": 0.0,
            "best_improvement_pct": 0.0,
            "worst_improvement_pct": 0.0,
            "meet_20pct_target_any": False,
        }

    avg_delta = sum(_safe_float(r.get("total_delta_ms")) for r in valid_rows) / len(valid_rows)
    avg_improvement = sum(_safe_float(r.get("improvement_pct")) for r in valid_rows) / len(valid_rows)
    best_improvement = max(_safe_float(r.get("improvement_pct")) for r in valid_rows)
    worst_improvement = min(_safe_float(r.get("improvement_pct")) for r in valid_rows)

    return {
        "runs": len(rows),
        "all_completed": all(r.get("exit_code") == 0 for r in rows),
        "all_valid": all(_safe_bool(r.get("compliance_valid")) for r in valid_rows),
        "avg_total_delta_ms": avg_delta,
        "avg_improvement_pct": avg_improvement,
        "best_improvement_pct": best_improvement,
        "worst_improvement_pct": worst_improvement,
        "meet_20pct_target_any": any(_safe_float(r.get("improvement_pct")) >= 20.0 for r in valid_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch-run hyperparameter/model experiments without changing search framework")
    parser.add_argument("--config", required=True, help="Base YAML config path")
    parser.add_argument("--experiment-name", required=True, help="Experiment folder name under output/experiments")
    parser.add_argument("--runs", type=int, default=3, help="Repeat count per combination")
    parser.add_argument("--models", nargs="*", default=[], help="Model list, e.g. deepseek-chat deepseek-reasoner")
    parser.add_argument("--alphas", nargs="*", type=float, default=[], help="MCTS alpha values")
    parser.add_argument("--betas", nargs="*", type=float, default=[], help="MCTS beta values")
    parser.add_argument("--cs", nargs="*", type=float, default=[], help="MCTS exploration constant c values")
    parser.add_argument("--max-nodes", nargs="*", type=int, default=[], help="MCTS max_nodes values")
    parser.add_argument("--max-depths", nargs="*", type=int, default=[], help="MCTS max_depth values")
    parser.add_argument("--seeds", nargs="*", type=int, default=[], help="MCTS seed values")
    parser.add_argument(
        "--overrides-json",
        default="",
        help="Extra fixed overrides as JSON object, e.g. '{\"llm.temperature\":0.0}'",
    )
    args = parser.parse_args()

    base_config_path = Path(args.config).expanduser().resolve()
    if not base_config_path.exists():
        raise FileNotFoundError(f"Config not found: {base_config_path}")

    base_config = _load_yaml(base_config_path)
    if args.runs < 1:
        raise ValueError("--runs must be >= 1")

    grid: Dict[str, List[Any]] = {}
    if args.models:
        grid["llm.model"] = args.models
    if args.alphas:
        grid["mcts.alpha"] = args.alphas
    if args.betas:
        grid["mcts.beta"] = args.betas
    if args.cs:
        grid["mcts.c"] = args.cs
    if args.max_nodes:
        grid["mcts.max_nodes"] = args.max_nodes
    if args.max_depths:
        grid["mcts.max_depth"] = args.max_depths
    if args.seeds:
        grid["mcts.seed"] = args.seeds

    fixed_overrides: Dict[str, Any] = {}
    if args.overrides_json:
        parsed = json.loads(args.overrides_json)
        if not isinstance(parsed, dict):
            raise ValueError("--overrides-json must be a JSON object")
        fixed_overrides = parsed

    keys = list(grid.keys())
    values = [grid[k] for k in keys]
    combinations = list(itertools.product(*values)) if keys else [tuple()]

    output_root = REPO_ROOT / "output" / "experiments" / args.experiment_name
    output_root.mkdir(parents=True, exist_ok=True)

    combo_summaries: List[Dict[str, Any]] = []
    for combo_index, combo_values in enumerate(combinations, start=1):
        combo_overrides = dict(zip(keys, combo_values))
        combo_overrides.update(fixed_overrides)

        combo_parts = [f"{k}={combo_overrides[k]}" for k in sorted(combo_overrides.keys())]
        combo_slug = _to_slug(combo_parts)
        combo_dir = output_root / f"combo_{combo_index:03d}_{combo_slug}"
        combo_dir.mkdir(parents=True, exist_ok=True)

        rows: List[Dict[str, Any]] = []
        for run_id in range(1, args.runs + 1):
            run_dir = combo_dir / f"run_{run_id}"

            cfg = copy.deepcopy(base_config)
            _set_nested(cfg, "project.output_dir", str(run_dir))
            _set_nested(cfg, "schema_apply.enabled", False)
            _set_nested(cfg, "project.llm_log_overwrite", True)

            for dotted_key, value in combo_overrides.items():
                _set_nested(cfg, dotted_key, value)

            run_config_path = combo_dir / f"config_run_{run_id}.yaml"
            _write_yaml(run_config_path, cfg)

            row = _run_single(run_config_path, run_dir)
            row["run"] = run_id
            rows.append(row)

        agg = _aggregate(rows)
        combo_summary = {
            "combo_index": combo_index,
            "overrides": combo_overrides,
            "aggregate": agg,
            "rows": rows,
        }
        combo_summaries.append(combo_summary)
        (combo_dir / "summary.json").write_text(
            json.dumps(combo_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    ranking = sorted(
        combo_summaries,
        key=lambda item: _safe_float(item.get("aggregate", {}).get("avg_improvement_pct"), -1e18),
        reverse=True,
    )

    final_summary = {
        "experiment_name": args.experiment_name,
        "runs_per_combo": args.runs,
        "combination_count": len(combo_summaries),
        "ranking": [
            {
                "combo_index": item.get("combo_index"),
                "overrides": item.get("overrides"),
                "avg_improvement_pct": item.get("aggregate", {}).get("avg_improvement_pct"),
                "avg_total_delta_ms": item.get("aggregate", {}).get("avg_total_delta_ms"),
                "meet_20pct_target_any": item.get("aggregate", {}).get("meet_20pct_target_any"),
            }
            for item in ranking
        ],
    }

    summary_path = output_root / "summary_all.json"
    summary_path.write_text(json.dumps(final_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(final_summary, ensure_ascii=False, indent=2))
    print(f"\nSummary saved: {summary_path}")


if __name__ == "__main__":
    main()
