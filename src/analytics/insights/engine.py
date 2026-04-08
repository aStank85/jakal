from __future__ import annotations

import time
from typing import Any

from .detectors import detector_registry
from .feature_builder import build_insight_features
from .models import Insight

INSIGHTS_VERSION = "insights_v1"


def _dedupe_and_rank(insights: list[Insight], limit: int = 12) -> list[Insight]:
    best: dict[tuple[str, str], Insight] = {}
    for ins in insights:
        ent = ins.entity or {}
        if ent.get("type") == "pair":
            key = (ins.category, f"{ent.get('a')}|{ent.get('b')}")
        elif ent.get("type") == "player_pair":
            key = (ins.category, str(ent.get("teammate") or ""))
        else:
            key = (ins.category, ins.id)
        prev = best.get(key)
        if prev is None or float(ins.rank_score) > float(prev.rank_score):
            best[key] = ins
    ranked = sorted(best.values(), key=lambda x: (-float(x.rank_score), x.id))
    return ranked[: max(1, int(limit))]


def _normalize_insight_evidence(insight: Insight, scope_match_ids: int) -> Insight:
    ev = dict(insight.evidence or {})
    ev.setdefault("sample_size", int(ev.get("rounds", 0) or 0))
    ev.setdefault("delta_pp", float(ev.get("delta_pp_overall", ev.get("delta_attack_win_rate_pp", 0.0)) or 0.0))
    ev.setdefault("prob_above_baseline", float(ev.get("prob_above_baseline_overall", 0.0) or 0.0))
    ev.setdefault("p_value", float(ev.get("p_value", 1.0) or 1.0))
    ev.setdefault("volatility", float(ev.get("volatility", 0.0) or 0.0))
    ev.setdefault("scope_match_ids", int(scope_match_ids))
    return Insight(
        id=insight.id,
        title=insight.title,
        message=insight.message,
        severity=insight.severity,
        category=insight.category,
        entity=insight.entity,
        evidence=ev,
        actions=insight.actions,
        rank_score=insight.rank_score,
    )


def run_insight_engine(
    *,
    cur: Any,
    username: str,
    scope: dict[str, Any],
    team_pairs_overall: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    t0 = time.time()
    features = build_insight_features(
        cur=cur,
        username=username,
        player_id=int(scope.get("player_id") or 0),
        match_ids=[str(m) for m in (scope.get("match_ids") or [])],
        team_pairs_overall=team_pairs_overall,
    )
    emitted: list[Insight] = []
    for fn in detector_registry():
        emitted.extend(fn(features))
    top = _dedupe_and_rank(emitted, limit=12)
    scope_match_ids = int((features.get("scope") or {}).get("match_ids") or 0)
    top = [_normalize_insight_evidence(i, scope_match_ids) for i in top]
    baseline = features.get("baseline", {}) or {}
    out = {
        "meta": {
            "compute_ms": int((time.time() - t0) * 1000),
            "version": INSIGHTS_VERSION,
            "counts": {
                "emitted": len(top),
                "considered_pairs": len(features.get("pairs_overall") or []),
            },
        },
        "baseline": {
            "p0": round(float(baseline.get("p0") or 0.0), 6),
            "wins": int(baseline.get("wins") or 0),
            "rounds": int(baseline.get("rounds") or 0),
        },
        "insights": [i.to_dict() for i in top],
    }
    return out
