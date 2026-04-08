from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from .parse_v2 import V2Parsed
from .parse_v1 import V1Parsed

@dataclass(frozen=True)
class CanonicalMatch:
    match: Dict[str, Any]
    match_players: List[Dict[str, Any]]
    rounds: List[Dict[str, Any]]
    player_rounds: List[Dict[str, Any]]
    kill_events: List[Dict[str, Any]]
    v1_used: bool

def _build_team_color_to_int(v2_players: List[Dict[str, Any]], v1_players: List[Dict[str, Any]]) -> Dict[str, int]:
    v2_team_by_uuid = {p["player_uuid"]: p.get("team_id") for p in v2_players if p.get("player_uuid")}
    mapping: Dict[str, int] = {}
    for p in v1_players:
        uuid = p.get("id")
        color = p.get("teamId")
        if uuid in v2_team_by_uuid and color:
            tid = v2_team_by_uuid[uuid]
            if tid is None:
                continue
            if color in mapping and mapping[color] != tid:
                # conflict; keep first, but caller can log if desired
                continue
            mapping[color] = int(tid)
    return mapping

def _first_blood_flags_from_kills(kill_events: List[Dict[str, Any]]) -> Dict[Tuple[int, str], Dict[str, bool]]:
    # returns map: (round_id, player_uuid) -> {"first_blood":bool, "first_death":bool}
    by_round = defaultdict(list)
    for ev in kill_events:
        rid = ev.get("roundId")
        ts = ev.get("timestamp")
        victim = ev.get("victimId")
        if rid is None or ts is None or not victim:
            continue
        by_round[int(rid)].append(ev)

    flags: Dict[Tuple[int, str], Dict[str, bool]] = {}
    for rid, evs in by_round.items():
        evs_sorted = sorted(evs, key=lambda x: (int(x.get("timestamp", 0)), str(x.get("victimId"))))
        first = evs_sorted[0]
        victim = first.get("victimId")
        attacker = first.get("attackerId")
        if victim:
            flags[(rid, victim)] = {"first_death": True}
        if attacker:
            flags[(rid, attacker)] = {**flags.get((rid, attacker), {}), "first_blood": True}
    return flags


def _find_focal_v1_player(v1_players: List[Dict[str, Any]], v1_rounds: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not v1_players or not v1_rounds:
        return None

    target = {
        (
            int(r.get("id")),
            r.get("sideId"),
            r.get("operatorId"),
        )
        for r in v1_rounds
        if r.get("id") is not None
    }
    if not target:
        return None

    best_player: Optional[Dict[str, Any]] = None
    best_score = -1
    for player in v1_players:
        player_rounds = player.get("rounds") or []
        score = sum(
            1
            for r in player_rounds
            if (
                r.get("id") is not None
                and (int(r.get("id")), r.get("sideId"), r.get("operatorId")) in target
            )
        )
        if score > best_score:
            best_score = score
            best_player = player
    return best_player if best_score > 0 else None


def _other_team_color(team_color_to_int: Dict[str, int], focal_team_color: Optional[str]) -> Optional[str]:
    if not focal_team_color:
        return None
    for color in team_color_to_int:
        if color != focal_team_color:
            return color
    return None


def merge_v1_v2(v2: V2Parsed, v1: Optional[V1Parsed]) -> CanonicalMatch:
    v1_used = v1 is not None

    # Start with v2 match + v2 match_players as truth for roster/team ints
    match_players = list(v2.match_players)

    # Build helper maps
    v2_handle_by_uuid = {p["player_uuid"]: p.get("handle") for p in v2.match_players if p.get("player_uuid")}
    v2_team_by_uuid = {p["player_uuid"]: p.get("team_id") for p in v2.match_players if p.get("player_uuid")}

    v2_pr_by_key: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for pr in v2.player_rounds:
        pu = pr.get("player_uuid")
        rid = pr.get("round_id")
        if pu and rid is not None:
            v2_pr_by_key[(pu, int(rid))] = pr

    # Round rows: prefer v1 rounds, fall back to v2 round-overview (partial)
    rounds_rows: List[Dict[str, Any]] = []
    kill_rows: List[Dict[str, Any]] = []
    first_flags: Dict[Tuple[int, str], Dict[str, bool]] = {}

    team_color_to_int: Dict[str, int] = {}
    if v1_used:
        team_color_to_int = _build_team_color_to_int(v2.match_players, v1.players)
        focal_player = _find_focal_v1_player(v1.players, v1.rounds)
        focal_team_color = focal_player.get("teamId") if focal_player else None
        opposing_team_color = _other_team_color(team_color_to_int, focal_team_color)

        has_round_outcome_schema = any(r.get("winnerTeamId") is not None for r in v1.rounds)
        if has_round_outcome_schema:
            for r in v1.rounds:
                rid = int(r.get("id"))
                wt = r.get("winnerTeamId")
                at = r.get("attackingTeamId")
                rounds_rows.append({
                    "round_id": rid,
                    "winner_team_color": wt,
                    "winner_team_id": team_color_to_int.get(wt) if wt else None,
                    "win_condition": r.get("winCondition"),
                    "bomb_site_id": r.get("bombSiteId"),
                    "attacking_team_color": at,
                    "attacking_team_id": team_color_to_int.get(at) if at else None,
                    "v2_round_end_reason_id": None,
                    "v2_round_end_reason_name": None,
                    "v2_winner_side_id": None,
                })
        else:
            for r in v1.rounds:
                rid = int(r.get("id"))
                side_id = r.get("sideId")
                result_id = r.get("resultId")
                outcome_id = r.get("outcomeId")
                attacking_team_color = None
                if side_id == "attacker":
                    attacking_team_color = focal_team_color
                elif side_id == "defender":
                    attacking_team_color = opposing_team_color

                winner_team_color = None
                if result_id == "victory":
                    winner_team_color = focal_team_color
                elif result_id == "defeat":
                    winner_team_color = opposing_team_color

                rounds_rows.append({
                    "round_id": rid,
                    "winner_team_color": winner_team_color,
                    "winner_team_id": team_color_to_int.get(winner_team_color) if winner_team_color else None,
                    "win_condition": outcome_id or result_id,
                    "bomb_site_id": None,
                    "attacking_team_color": attacking_team_color,
                    "attacking_team_id": team_color_to_int.get(attacking_team_color) if attacking_team_color else None,
                    "v2_round_end_reason_id": None,
                    "v2_round_end_reason_name": None,
                    "v2_winner_side_id": None,
                })

        # kills
        for ev in v1.kill_events:
            rid = ev.get("roundId")
            ts = ev.get("timestamp")
            victim = ev.get("victimId")
            if rid is None or ts is None or not victim:
                continue
            kill_rows.append({
                "round_id": int(rid),
                "timestamp_ms": int(ts),
                "attacker_uuid": ev.get("attackerId"),
                "victim_uuid": victim,
            })
        if not kill_rows:
            synthetic_events: List[Dict[str, Any]] = []
            round_counters: Dict[int, int] = defaultdict(int)
            for pr in v2.player_rounds:
                attacker_uuid = pr.get("player_uuid")
                rid = pr.get("round_id")
                if not attacker_uuid or rid is None:
                    continue
                for victim_uuid in pr.get("killed_players") or []:
                    if not victim_uuid:
                        continue
                    round_counters[int(rid)] += 1
                    synthetic_events.append({
                        "roundId": int(rid),
                        "timestamp": round_counters[int(rid)],
                        "attackerId": attacker_uuid,
                        "victimId": victim_uuid,
                    })
            kill_rows.extend({
                "round_id": int(ev["roundId"]),
                "timestamp_ms": int(ev["timestamp"]),
                "attacker_uuid": ev.get("attackerId"),
                "victim_uuid": ev["victimId"],
            } for ev in synthetic_events)
            first_flags = _first_blood_flags_from_kills(synthetic_events)
        else:
            first_flags = _first_blood_flags_from_kills(v1.kill_events)

    # Add v2 round-overview info (fills v2_* fields; does not overwrite v1 winner if present)
    v2_round_by_id = {int(r["round_id"]): r for r in v2.round_overviews if r.get("round_id") is not None}
    existing_round_ids = {r["round_id"] for r in rounds_rows}
    for rid, ro in v2_round_by_id.items():
        if rid in existing_round_ids:
            # patch into existing row
            for rr in rounds_rows:
                if rr["round_id"] == rid:
                    rr["v2_round_end_reason_id"] = ro.get("v2_round_end_reason_id")
                    rr["v2_round_end_reason_name"] = ro.get("v2_round_end_reason_name")
                    rr["v2_winner_side_id"] = ro.get("v2_winner_side_id")
                    break
        else:
            rounds_rows.append({
                "round_id": rid,
                "winner_team_color": None,
                "winner_team_id": None,
                "win_condition": None,
                "bomb_site_id": None,
                "attacking_team_color": None,
                "attacking_team_id": None,
                "v2_round_end_reason_id": ro.get("v2_round_end_reason_id"),
                "v2_round_end_reason_name": ro.get("v2_round_end_reason_name"),
                "v2_winner_side_id": ro.get("v2_winner_side_id"),
            })

    canonical_pr: List[Dict[str, Any]] = []

    if v1_used:
        for p in v1.players:
            pu = p.get("id")
            handle = p.get("nickname")
            team_color = p.get("teamId")
            team_id = team_color_to_int.get(team_color) if team_color else v2_team_by_uuid.get(pu)
            for r in (p.get("rounds") or []):
                rid = int(r.get("id"))
                v1_side = r.get("sideId")
                v1_op = r.get("operatorId")
                v1_stats = r.get("stats") or {}
                v2_seg = v2_pr_by_key.get((pu, rid), {})
                flags = first_flags.get((rid, pu), {})
                # v2 fallback for first blood/death
                fb = bool(flags.get("first_blood")) if "first_blood" in flags else (int(v2_seg.get("first_bloods") or 0) > 0)
                fd = bool(flags.get("first_death")) if "first_death" in flags else (int(v2_seg.get("first_deaths") or 0) > 0)
                canonical_pr.append({
                    "round_id": rid,
                    "player_uuid": pu,
                    "handle": handle or v2_handle_by_uuid.get(pu),
                    "team_id": team_id,
                    "side_id": v1_side or v2_seg.get("side_id"),
                    "operator_id": v1_op or v2_seg.get("operator_id"),
                    "kills": v1_stats.get("kills"),
                    "deaths": v1_stats.get("deaths"),
                    "assists": v1_stats.get("assists"),
                    "headshots": v1_stats.get("headshots"),
                    "score": v1_stats.get("score"),
                    "plants": v1_stats.get("plants"),
                    "trades": v1_stats.get("trades"),
                    "is_disconnected": v2_seg.get("is_disconnected"),
                    "first_blood": fb,
                    "first_death": fd,
                    "clutch_won": int(v2_seg.get("clutches") or 0) > 0 if v2_seg else None,
                    "clutch_lost": int(v2_seg.get("clutches_lost") or 0) > 0 if v2_seg else None,
                    "killed_players": v2_seg.get("killed_players"),
                    "killed_by_player_uuid": v2_seg.get("killed_by_player_uuid"),
                })
    else:
        # No v1: use v2 player-round only (less rich but still useful)
        for (pu, rid), v2_seg in v2_pr_by_key.items():
            canonical_pr.append({
                "round_id": rid,
                "player_uuid": pu,
                "handle": v2_handle_by_uuid.get(pu),
                "team_id": v2_seg.get("team_id"),
                "side_id": v2_seg.get("side_id"),
                "operator_id": v2_seg.get("operator_id"),
                "kills": v2_seg.get("kills"),
                "deaths": v2_seg.get("deaths"),
                "assists": v2_seg.get("assists"),
                "headshots": v2_seg.get("headshots"),
                "score": None,
                "plants": None,
                "trades": None,
                "is_disconnected": v2_seg.get("is_disconnected"),
                "first_blood": int(v2_seg.get("first_bloods") or 0) > 0,
                "first_death": int(v2_seg.get("first_deaths") or 0) > 0,
                "clutch_won": int(v2_seg.get("clutches") or 0) > 0,
                "clutch_lost": int(v2_seg.get("clutches_lost") or 0) > 0,
                "killed_players": v2_seg.get("killed_players"),
                "killed_by_player_uuid": v2_seg.get("killed_by_player_uuid"),
            })

    return CanonicalMatch(
        match=v2.match,
        match_players=match_players,
        rounds=sorted(rounds_rows, key=lambda x: x["round_id"]),
        player_rounds=sorted(canonical_pr, key=lambda x: (x["round_id"], x["player_uuid"])),
        kill_events=kill_rows,
        v1_used=v1_used,
    )
