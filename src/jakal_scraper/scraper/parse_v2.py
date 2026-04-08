from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import re

@dataclass(frozen=True)
class V2Parsed:
    match: Dict[str, Any]
    match_players: List[Dict[str, Any]]
    player_rounds: List[Dict[str, Any]]
    round_overviews: List[Dict[str, Any]]

def parse_v2_match(v2: Dict[str, Any]) -> V2Parsed:
    attrs = v2.get("attributes", {}) or {}
    meta = v2.get("metadata", {}) or {}
    match_id = attrs.get("id")
    match = {
        "match_id": match_id,
        "timestamp": meta.get("timestamp"),
        "duration_ms": meta.get("duration"),
        "datacenter": meta.get("datacenter") or attrs.get("datacenter"),
        "session_type": attrs.get("sessionType"),
        "session_game_mode": attrs.get("sessionGameMode"),
        "session_mode": attrs.get("sessionMode"),
        "gamemode": attrs.get("gamemode"),
        "map_slug": attrs.get("sessionMap"),
        "map_name": meta.get("sessionMapName"),
        "is_surrender": meta.get("isSurrender"),
        "is_forfeit": meta.get("isForfeit"),
        "is_rollback": meta.get("isRollback"),
        "is_cancelled_by_ac": meta.get("isCancelledByAC"),
        "full_match_available": meta.get("fullMatchAvailable"),
        "has_overwolf_roster": meta.get("hasOverwolfRoster"),
        "extended_data_available": meta.get("extendedDataAvailable"),
    }

    segments = v2.get("segments", []) or []
    match_players: List[Dict[str, Any]] = []
    player_rounds: List[Dict[str, Any]] = []
    round_overviews: List[Dict[str, Any]] = []

    for seg in segments:
        stype = seg.get("type")
        a = seg.get("attributes", {}) or {}
        m = seg.get("metadata", {}) or {}
        stats = seg.get("stats", {}) or {}
        if stype == "overview":
            player_uuid = a.get("playerId")

            # Preferred: use Tracker-provided values when present (these can differ from naive formulas).
            kills_v = (stats.get("kills") or {}).get("value")
            deaths_v = (stats.get("deaths") or {}).get("value")
            headshots_v = (stats.get("headshots") or {}).get("value")
            first_bloods_v = (stats.get("firstBloods") or {}).get("value")
            first_deaths_v = (stats.get("firstDeaths") or {}).get("value")

            kd_ratio_v = (stats.get("kdRatio") or {}).get("value")
            hs_pct_v = (stats.get("headshotPct") or {}).get("value")
            esr_v = (stats.get("esr") or {}).get("value")

            # Fallbacks if the API omits these fields.
            if kd_ratio_v is None and kills_v is not None and deaths_v is not None:
                kd_ratio_v = float(kills_v) if deaths_v == 0 else float(kills_v) / float(deaths_v)
            if hs_pct_v is None and headshots_v is not None and kills_v is not None and kills_v != 0:
                hs_pct_v = 100.0 * float(headshots_v) / float(kills_v)
            if esr_v is None and first_bloods_v is not None and first_deaths_v is not None:
                denom = (first_bloods_v + first_deaths_v)
                esr_v = None if denom == 0 else 100.0 * float(first_bloods_v) / float(denom)
            match_players.append({
                "player_uuid": player_uuid,
                "handle": m.get("platformUserHandle"),
                "team_id": a.get("teamId"),
                "result": m.get("result"),
                "has_won": m.get("hasWon"),
                "kills": kills_v,
                "deaths": deaths_v,
                "assists": (stats.get("assists") or {}).get("value"),
                "headshots": headshots_v,
                "team_kills": (stats.get("teamKills") or {}).get("value"),
                "first_bloods": first_bloods_v,
                "first_deaths": first_deaths_v,
                "clutches": (stats.get("clutches") or {}).get("value"),
                "clutches_lost": (stats.get("clutchesLost") or {}).get("value"),
                "rounds_played": (stats.get("roundsPlayed") or {}).get("value"),
                "rounds_won": (stats.get("roundsWon") or {}).get("value"),
                "rounds_lost": (stats.get("roundsLost") or {}).get("value"),
                "rank_points": (stats.get("rankPoints") or {}).get("value"),
                "rank_name": (stats.get("rankPoints") or {}).get("metadata", {}).get("rankName") if isinstance((stats.get("rankPoints") or {}).get("metadata"), dict) else None,
                "rank_points_delta": (stats.get("rankPointsDelta") or {}).get("value"),
                # Promoted convenience metrics (typed columns in DB)
                "kd_ratio": kd_ratio_v,
                "hs_pct": hs_pct_v,
                "esr": esr_v,
                "raw_stats": stats,
            })
        elif stype == "player-round":
            player_rounds.append({
                "player_uuid": a.get("playerId"),
                "round_id": a.get("roundId"),
                "team_id": a.get("teamId"),
                "side_id": a.get("sideId"),
                "operator_id": a.get("operatorId"),
                "result_id": a.get("resultId"),
                "is_disconnected": a.get("isDisconnected"),
                "killed_players": m.get("killedPlayersIds"),
                "killed_by_player_uuid": m.get("killedByPlayerId"),
                "kills": (stats.get("kills") or {}).get("value"),
                "deaths": (stats.get("deaths") or {}).get("value"),
                "assists": (stats.get("assists") or {}).get("value"),
                "headshots": (stats.get("headshots") or {}).get("value"),
                "first_bloods": (stats.get("firstBloods") or {}).get("value"),
                "first_deaths": (stats.get("firstDeaths") or {}).get("value"),
                "clutches": (stats.get("clutches") or {}).get("value"),
                "clutches_lost": (stats.get("clutchesLost") or {}).get("value"),
            })
        elif stype == "round-overview":
            round_overviews.append({
                "round_id": a.get("roundId"),
                "v2_round_end_reason_id": a.get("roundEndReasonId"),
                "v2_winner_side_id": a.get("winnerSideId"),
                "v2_round_end_reason_name": m.get("roundEndReasonName"),
            })

    return V2Parsed(match=match, match_players=match_players, player_rounds=player_rounds, round_overviews=round_overviews)
