from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def resolve_llm_log_dir(config: Dict[str, Any]) -> Path:
    project_cfg = config.get("project", {})
    log_dir = project_cfg.get("llm_log_dir")
    if isinstance(log_dir, str) and log_dir.strip():
        return Path(log_dir)
    output_dir = Path(str(project_cfg.get("output_dir", "./output")))
    return output_dir / "llm_logs"


def prepare_llm_log_dir(config: Dict[str, Any]) -> Path:
    project_cfg = config.setdefault("project", {})
    output_dir = Path(str(project_cfg.get("output_dir", "./output")))
    overwrite = bool(project_cfg.get("llm_log_overwrite", True))
    base_dir = output_dir / "llm_logs"
    if overwrite:
        if base_dir.exists():
            shutil.rmtree(base_dir)
        base_dir.mkdir(parents=True, exist_ok=True)
        log_dir = base_dir
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = output_dir / f"llm_logs_{timestamp}"
        log_dir.mkdir(parents=True, exist_ok=True)
    project_cfg["llm_log_dir"] = str(log_dir)
    return log_dir
