from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class InsightAction:
    type: str
    label: str
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Insight:
    id: str
    title: str
    message: str
    severity: str
    category: str
    entity: dict[str, Any]
    evidence: dict[str, Any]
    actions: list[InsightAction] = field(default_factory=list)
    rank_score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "message": self.message,
            "severity": self.severity,
            "category": self.category,
            "entity": self.entity,
            "evidence": self.evidence,
            "actions": [a.to_dict() for a in self.actions],
            "rank_score": round(float(self.rank_score), 6),
        }
