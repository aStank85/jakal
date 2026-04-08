from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Callable

from . import copy as cp
from .models import Insight, InsightAction

Detector = Callable[[dict[str, Any]], list[Insight]]


def _pair_key(a: str, b: str) -> tuple[str, str]:
    x = str(a or "").strip().lower()
    y = str(b or "").strip().lower()
    return (x, y) if x <= y else (y, x)


def _insight_id(prefix: str, *parts: str) -> str:
    safe = ["".join(ch for ch in str(p).lower() if ch.isalnum() or ch in {"_", "-"}) for p in parts]
    return f"{prefix}|{'|'.join(safe)}"


def _entity_pair(a: str, b: str) -> dict[str, Any]:
    return {"type": "pair", "a": a, "b": b}


def _severity_boost(severity: str) -> float:
    if severity == "ACTION":
        return 1.25
    if severity == "WATCH":
        return 1.1
    return 1.0


def _base_rank(delta_pp: float, rounds_n: int, prob_above: float, severity: str) -> float:
    base = abs(float(delta_pp)) * math.sqrt(max(1.0, float(rounds_n)))
    certainty = abs(float(prob_above) - 0.5) * 2.0
    return base * (1.0 + certainty) * _severity_boost(severity)


def detector_conditional_edge_collapse(features: dict[str, Any]) -> list[Insight]:
    by_side = defaultdict(dict)
    for r in features.get("pairs_by_side", []):
        key = _pair_key(r.get("a"), r.get("b"))
        by_side[key][str(r.get("side") or "").lower()] = r
    out: list[Insight] = []
    for p in features.get("pairs_overall", []):
        rounds_n = int(p.get("rounds_n") or 0)
        delta = float(p.get("delta_pp") or 0.0)
        if rounds_n < 120 or delta < 3.0:
            continue
        key = _pair_key(p.get("a"), p.get("b"))
        atk = by_side.get(key, {}).get("attack")
        deff = by_side.get(key, {}).get("defense")
        if not atk:
            continue
        atk_delta = float(atk.get("delta_pp") or 0.0)
        atk_prob = float(atk.get("prob_above_baseline") or 0.0)
        if atk_delta > 0.0 and atk_prob >= 0.55:
            continue
        a = str(p.get("a") or "")
        b = str(p.get("b") or "")
        sev = "ACTION"
        evidence = {
            "rounds": rounds_n,
            "matches": int(p.get("matches_n") or 0),
            "delta_pp_overall": round(delta, 3),
            "delta_pp_attack": round(atk_delta, 3),
            "delta_pp_defense": round(float((deff or {}).get("delta_pp") or 0.0), 3),
            "prob_above_baseline_overall": round(float(p.get("prob_above_baseline") or 0.0), 4),
            "volatility": round(float(p.get("volatility") or 0.0), 4),
        }
        out.append(
            Insight(
                id=_insight_id("edge_collapse_attack_defense", a, b),
                title="Synergy is defense-skewed",
                message=cp.defense_skewed_message(a, b, delta, atk_delta),
                severity=sev,
                category="SYNERGY",
                entity=_entity_pair(a, b),
                evidence=evidence,
                actions=[
                    InsightAction(type="apply_filter", label="View Defense only", params={"ws_side": "defense"}),
                    InsightAction(type="open_panel", label="Open Operators", params={"ws_panel": "operators"}),
                ],
                rank_score=_base_rank(delta, rounds_n, float(p.get("prob_above_baseline") or 0.5), sev),
            )
        )
    return out


def detector_map_one_trick(features: dict[str, Any]) -> list[Insight]:
    by_pair_maps: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in features.get("pairs_by_map", []):
        by_pair_maps[_pair_key(r.get("a"), r.get("b"))].append(r)
    out: list[Insight] = []
    for p in features.get("pairs_overall", []):
        rounds_n = int(p.get("rounds_n") or 0)
        delta = float(p.get("delta_pp") or 0.0)
        if rounds_n < 120 or delta < 3.0:
            continue
        key = _pair_key(p.get("a"), p.get("b"))
        maps = sorted(by_pair_maps.get(key, []), key=lambda x: int(x.get("rounds_n") or 0), reverse=True)
        if not maps:
            continue
        top = maps[0]
        share = (int(top.get("rounds_n") or 0) / max(1, rounds_n)) * 100.0
        if share <= 55.0:
            continue
        a = str(p.get("a") or "")
        b = str(p.get("b") or "")
        sev = "WATCH"
        out.append(
            Insight(
                id=_insight_id("map_one_trick", a, b, str(top.get("map_name") or "")),
                title=f"{a}+{b} edge is concentrated on {top.get('map_name')}",
                message=cp.map_concentrated_message(a, b, str(top.get("map_name") or "this map"), share),
                severity=sev,
                category="MAP_POOL",
                entity=_entity_pair(a, b),
                evidence={
                    "rounds": rounds_n,
                    "matches": int(p.get("matches_n") or 0),
                    "delta_pp_overall": round(delta, 3),
                    "top_map": str(top.get("map_name") or "unknown"),
                    "top_map_round_share_pct": round(share, 2),
                    "volatility": round(float(p.get("volatility") or 0.0), 4),
                },
                actions=[
                    InsightAction(type="apply_filter", label=f"Filter to {top.get('map_name')}", params={"ws_map_name": top.get("map_name")}),
                    InsightAction(type="clear_filter", label="Compare all maps", params={"ws_map_name": ""}),
                ],
                rank_score=_base_rank(delta, rounds_n, float(p.get("prob_above_baseline") or 0.5), sev),
            )
        )
    return out


def detector_consistent_risk(features: dict[str, Any]) -> list[Insight]:
    out: list[Insight] = []
    for p in features.get("pairs_overall", []):
        rounds_n = int(p.get("rounds_n") or 0)
        matches_n = int(p.get("matches_n") or 0)
        delta = float(p.get("delta_pp") or 0.0)
        prob = float(p.get("prob_above_baseline") or 0.5)
        p_value = float(p.get("p_value") or 1.0)
        vol = float(p.get("volatility") or 0.0)
        stable = vol <= 0.12 or matches_n >= 10
        significant = prob <= 0.35 or p_value <= 0.05
        if rounds_n < 120 or delta > -3.0 or not (stable and significant):
            continue
        a = str(p.get("a") or "")
        b = str(p.get("b") or "")
        sev = "ACTION"
        out.append(
            Insight(
                id=_insight_id("consistent_risk_pair", a, b),
                title=f"Consistent underperformance with {a}+{b}",
                message=cp.consistent_risk_message(a, b, rounds_n, delta, vol),
                severity=sev,
                category="RISK",
                entity=_entity_pair(a, b),
                evidence={
                    "rounds": rounds_n,
                    "matches": matches_n,
                    "delta_pp_overall": round(delta, 3),
                    "prob_above_baseline_overall": round(prob, 4),
                    "p_value": round(p_value, 6),
                    "volatility": round(vol, 4),
                },
                actions=[
                    InsightAction(type="flag_pair", label="Flag pair risk", params={"pair": f"{a}+{b}"}),
                    InsightAction(type="note", label="Try swapping entry/support roles", params={}),
                ],
                rank_score=_base_rank(delta, rounds_n, prob, sev),
            )
        )
    return out


def detector_volatile_edge(features: dict[str, Any]) -> list[Insight]:
    out: list[Insight] = []
    for p in features.get("pairs_overall", []):
        rounds_n = int(p.get("rounds_n") or 0)
        matches_n = int(p.get("matches_n") or 0)
        delta = float(p.get("delta_pp") or 0.0)
        vol = float(p.get("volatility") or 0.0)
        if delta < 3.0 or matches_n < 6 or vol < 0.20:
            continue
        a = str(p.get("a") or "")
        b = str(p.get("b") or "")
        sev = "WATCH"
        out.append(
            Insight(
                id=_insight_id("volatile_edge_pair", a, b),
                title="Edge is volatile",
                message=cp.volatile_edge_message(a, b),
                severity=sev,
                category="CONSISTENCY",
                entity=_entity_pair(a, b),
                evidence={
                    "rounds": rounds_n,
                    "matches": matches_n,
                    "delta_pp_overall": round(delta, 3),
                    "prob_above_baseline_overall": round(float(p.get("prob_above_baseline") or 0.0), 4),
                    "volatility": round(vol, 4),
                },
                actions=[InsightAction(type="apply_filter", label="Narrow to 30d", params={"ws_days": 30})],
                rank_score=_base_rank(delta, rounds_n, float(p.get("prob_above_baseline") or 0.5), sev),
            )
        )
    return out


def detector_side_imbalance(features: dict[str, Any]) -> list[Insight]:
    p = features.get("player_side_profile", {}) or {}
    atk_rounds = int(p.get("attack_rounds") or 0)
    def_rounds = int(p.get("defense_rounds") or 0)
    atk_wr = float(p.get("attack_win_rate") or 0.0)
    def_wr = float(p.get("defense_win_rate") or 0.0)
    diff = abs(atk_wr - def_wr)
    if atk_rounds < 80 or def_rounds < 80 or diff < 8.0:
        return []
    weak = "Attack" if atk_wr < def_wr else "Defense"
    weak_filter = "attack" if weak == "Attack" else "defense"
    sev = "ACTION"
    ins = Insight(
        id=_insight_id("baseline_side_split", weak_filter),
        title="Side imbalance",
        message=cp.side_imbalance_message(atk_wr, def_wr, weak),
        severity=sev,
        category="TEMPO",
        entity={"type": "player"},
        evidence={
            "attack_rounds": atk_rounds,
            "defense_rounds": def_rounds,
            "attack_win_rate": round(atk_wr, 3),
            "defense_win_rate": round(def_wr, 3),
            "delta_pp": round(atk_wr - def_wr, 3),
            "volatility": 0.0,
        },
        actions=[
            InsightAction(type="apply_filter", label=f"View {weak} only", params={"ws_side": weak_filter}),
            InsightAction(type="open_panel", label="Open Operators", params={"ws_panel": "operators"}),
        ],
        rank_score=_base_rank(diff, atk_rounds + def_rounds, 0.8, sev),
    )
    return [ins]


def detector_pair_reduces_first_deaths(features: dict[str, Any]) -> list[Insight]:
    out: list[Insight] = []
    for r in features.get("pair_entry_effect", []):
        paired_rounds = int(r.get("paired_rounds") or 0)
        fd_delta = float(r.get("delta_first_death_rate_pp") or 0.0)
        if paired_rounds < 120 or fd_delta > -3.0:
            continue
        tm = str(r.get("teammate") or "")
        sev = "ACTION"
        out.append(
            Insight(
                id=_insight_id("pair_reduces_first_deaths", tm),
                title=f"{tm} reduces your first deaths",
                message=cp.first_death_drop_message(tm, fd_delta),
                severity=sev,
                category="ROLE",
                entity={"type": "player_pair", "teammate": tm},
                evidence={
                    "paired_rounds": paired_rounds,
                    "unpaired_rounds": int(r.get("unpaired_rounds") or 0),
                    "paired_first_death_rate": round(float(r.get("paired_first_death_rate") or 0.0), 3),
                    "unpaired_first_death_rate": round(float(r.get("unpaired_first_death_rate") or 0.0), 3),
                    "delta_first_death_rate_pp": round(fd_delta, 3),
                },
                actions=[
                    InsightAction(type="apply_filter", label=f"Focus games with {tm}", params={"ws_search": tm}),
                    InsightAction(type="apply_filter", label="Attack only", params={"ws_side": "attack"}),
                ],
                rank_score=_base_rank(fd_delta, paired_rounds, 0.8, sev),
            )
        )
    return out


def detector_over_aggression(features: dict[str, Any]) -> list[Insight]:
    out: list[Insight] = []
    for r in features.get("pair_entry_effect", []):
        paired_rounds = int(r.get("paired_rounds") or 0)
        entry_delta = float(r.get("delta_entry_rate_pp") or 0.0)
        wr_delta = float(r.get("delta_attack_win_rate_pp") or 0.0)
        if paired_rounds < 120 or entry_delta < 4.0 or wr_delta > -2.0:
            continue
        tm = str(r.get("teammate") or "")
        sev = "WATCH"
        out.append(
            Insight(
                id=_insight_id("over_aggression", tm),
                title="Over-aggression signal",
                message=cp.over_aggression_message(tm, entry_delta, wr_delta),
                severity=sev,
                category="RISK",
                entity={"type": "player_pair", "teammate": tm},
                evidence={
                    "paired_rounds": paired_rounds,
                    "delta_entry_rate_pp": round(entry_delta, 3),
                    "delta_attack_win_rate_pp": round(wr_delta, 3),
                    "paired_first_blood_rate": round(float(r.get("paired_first_blood_rate") or 0.0), 3),
                    "paired_first_death_rate": round(float(r.get("paired_first_death_rate") or 0.0), 3),
                },
                actions=[
                    InsightAction(type="apply_filter", label="Attack only", params={"ws_side": "attack"}),
                    InsightAction(type="note", label="Try role shift (entry/support)", params={}),
                ],
                rank_score=_base_rank(wr_delta, paired_rounds, 0.7, sev),
            )
        )
    return out


def detector_registry() -> list[Detector]:
    return [
        detector_conditional_edge_collapse,
        detector_map_one_trick,
        detector_consistent_risk,
        detector_volatile_edge,
        detector_side_imbalance,
        detector_pair_reduces_first_deaths,
        detector_over_aggression,
    ]
