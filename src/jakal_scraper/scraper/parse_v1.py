from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass(frozen=True)
class V1Parsed:
    players: List[Dict[str, Any]]
    rounds: List[Dict[str, Any]]
    kill_events: List[Dict[str, Any]]

def parse_v1_ingest(v1: Dict[str, Any]) -> V1Parsed:
    payload = v1
    if isinstance(v1.get("data"), dict):
        candidate = v1["data"]
        if any(key in candidate for key in ("players", "rounds", "killEvents")):
            payload = candidate

    players = payload.get("players", []) or []
    rounds = payload.get("rounds", []) or []
    kill_events = payload.get("killEvents", []) or []
    return V1Parsed(players=players, rounds=rounds, kill_events=kill_events)
