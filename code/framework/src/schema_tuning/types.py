from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Action:
    type: str
    params: Dict[str, Any]


@dataclass
class Candidate:
    candidate_id: str
    actions: List[Action] = field(default_factory=list)


@dataclass
class EvaluationResult:
    candidate_id: str
    compliance: Dict[str, Any]
    storage: Dict[str, Any]
    performance: Dict[str, Any]
