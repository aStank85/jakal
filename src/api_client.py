from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class TrackerAPIClient:
    BASE = "https://api.tracker.gg/api/v2/r6siege/standard"
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://r6.tracker.network",
        "Referer": "https://r6.tracker.network/",
        "x-app-version": "1.0.0",
    }

    END_REASON_MAP = {
        1: "attackers_eliminated",
        2: "defenders_eliminated",
        3: "bomb_exploded",
        4: "time_expired",
        5: "defuser_disabled",
        6: "defuser_planted",
    }

    def __init__(
        self,
        timeout_seconds: int = 20,
        sleep_seconds: float = 0.5,
        detail_sleep_min_seconds: float = 2.5,
        detail_sleep_max_seconds: float = 4.0,
        detail_batch_size: int = 10,
        detail_batch_pause_seconds: float = 15.0,
    ):
        self.timeout_seconds = timeout_seconds
        self.sleep_seconds = sleep_seconds
        self.detail_sleep_min_seconds = detail_sleep_min_seconds
        self.detail_sleep_max_seconds = detail_sleep_max_seconds
        self.detail_batch_size = detail_batch_size
        self.detail_batch_pause_seconds = detail_batch_pause_seconds

    @staticmethod
    def _progress_print(message: str, end: str = "\n") -> None:
        try:
            print(message, end=end, flush=True)
        except UnicodeEncodeError:
            fallback = message.replace("📄", "[PAGE]").replace("⚔️", "[MATCH]")
            print(fallback, end=end, flush=True)

    def _get_json(self, url: str, retry_429: bool = True) -> Dict[str, Any]:
        req = Request(url, headers=self.HEADERS, method="GET")
        try:
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 429 and retry_429:
                time.sleep(10)
                return self._get_json(url, retry_429=False)
            raise
        except URLError:
            raise

    @staticmethod
    def _stat(stats: Dict[str, Any], key: str, default: Any = 0) -> Any:
        node = stats.get(key, {})
        value = node.get("value", default) if isinstance(node, dict) else default
        return default if value is None else value

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _map_side(value: Any) -> str:
        if value in ("attacker", "defender"):
            return str(value)
        if value in (0, "0"):
            return "attacker"
        if value in (1, "1"):
            return "defender"
        return "unknown"

    def _map_end_reason(self, reason: Any) -> str:
        if isinstance(reason, str) and reason:
            return reason.lower().replace(" ", "_")
        return self.END_REASON_MAP.get(self._safe_int(reason, -1), "unknown")

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _normalize_since_date(since_date: Optional[Any]) -> Optional[datetime]:
        if since_date is None:
            return None
        if isinstance(since_date, datetime):
            dt = since_date
        elif isinstance(since_date, str):
            dt = TrackerAPIClient._parse_timestamp(since_date)
            if dt is None:
                return None
        else:
            return None

        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def parse_match_list(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        matches = data.get("matches", []) if isinstance(data, dict) else []
        metadata = data.get("metadata", {}) if isinstance(data, dict) else {}
        out: List[Dict[str, Any]] = []

        for match in matches:
            attrs = match.get("attributes", {})
            meta = match.get("metadata", {})
            segments = match.get("segments", [])
            overview = next((s for s in segments if s.get("type") == "overview"), {})
            oattrs = overview.get("attributes", {})
            ometa = overview.get("metadata", {})
            ostats = overview.get("stats", {})

            out.append(
                {
                    "match_id": attrs.get("id"),
                    "map": meta.get("sessionMapName") or attrs.get("sessionMap"),
                    "timestamp": meta.get("timestamp"),
                    "mode": meta.get("sessionTypeName") or attrs.get("sessionType"),
                    "result": ometa.get("result"),
                    "team_id": oattrs.get("teamId"),
                    "player_id_tracker": oattrs.get("playerId"),
                    "username": ometa.get("platformUserHandle"),
                    "rank_points": self._stat(ostats, "rankPoints", None),
                    "rank_points_delta": self._stat(ostats, "rankPointsDelta", None),
                    "overview_segment": overview,
                }
            )

        return {"matches": out, "next": metadata.get("next")}

    def parse_match_detail_segments(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        segments = data.get("segments", []) if isinstance(data, dict) else []
        grouped = {
            "overview": [],
            "player-operator": [],
            "round-overview": [],
            "player-round": [],
        }
        for segment in segments:
            kind = segment.get("type")
            if kind in grouped:
                grouped[kind].append(segment)
        return grouped

    def parse_player_overview(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        attrs = segment.get("attributes", {})
        meta = segment.get("metadata", {})
        stats = segment.get("stats", {})
        return {
            "player_id_tracker": attrs.get("playerId"),
            "username": meta.get("platformUserHandle") or meta.get("displayName"),
            "team_id": self._safe_int(attrs.get("teamId"), -1),
            "result": meta.get("result"),
            "kills": self._safe_int(self._stat(stats, "kills")),
            "deaths": self._safe_int(self._stat(stats, "deaths")),
            "assists": self._safe_int(self._stat(stats, "assists")),
            "headshots": self._safe_int(self._stat(stats, "headshots")),
            "first_bloods": self._safe_int(self._stat(stats, "firstBloods")),
            "first_deaths": self._safe_int(self._stat(stats, "firstDeaths")),
            "clutches_won": self._safe_int(self._stat(stats, "clutches")),
            "clutches_lost": self._safe_int(self._stat(stats, "clutchesLost")),
            "clutches_1v1": self._safe_int(self._stat(stats, "clutches1v1")),
            "clutches_1v2": self._safe_int(self._stat(stats, "clutches1v2")),
            "clutches_1v3": self._safe_int(self._stat(stats, "clutches1v3")),
            "clutches_1v4": self._safe_int(self._stat(stats, "clutches1v4")),
            "clutches_1v5": self._safe_int(self._stat(stats, "clutches1v5")),
            "kills_1k": self._safe_int(self._stat(stats, "kills1K")),
            "kills_2k": self._safe_int(self._stat(stats, "kills2K")),
            "kills_3k": self._safe_int(self._stat(stats, "kills3K")),
            "kills_4k": self._safe_int(self._stat(stats, "kills4K")),
            "kills_5k": self._safe_int(self._stat(stats, "kills5K")),
            "rounds_won": self._safe_int(self._stat(stats, "roundsWon")),
            "rounds_lost": self._safe_int(self._stat(stats, "roundsLost")),
            "rank_points": self._safe_int(self._stat(stats, "rankPoints", 0)),
            "rank_points_delta": self._safe_int(self._stat(stats, "rankPointsDelta", 0)),
            "rank_points_previous": self._safe_int(self._stat(stats, "rankPointsPrevious", 0)),
            "kd_ratio": self._safe_float(self._stat(stats, "kdRatio")),
            "hs_pct": self._safe_float(self._stat(stats, "headshotPct")),
            "esr": self._safe_float(self._stat(stats, "esr")),
            "kills_per_round": self._safe_float(self._stat(stats, "killsPerRound")),
            "time_played_ms": self._safe_int(self._stat(stats, "timePlayed")),
            "elo": self._safe_int(self._stat(stats, "elo", 0)),
            "elo_delta": self._safe_int(self._stat(stats, "eloDelta", 0)),
        }

    def parse_round_outcome(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        attrs = segment.get("attributes", {})
        meta = segment.get("metadata", {})
        return {
            "round_id": self._safe_int(attrs.get("roundId")),
            "end_reason": self._map_end_reason(attrs.get("roundEndReasonId") or meta.get("endReasonName")),
            "winner_side": self._map_side(attrs.get("winnerSideId") or meta.get("winnerSideName")),
        }

    def parse_player_round(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        attrs = segment.get("attributes", {})
        meta = segment.get("metadata", {})
        stats = segment.get("stats", {})
        return {
            "round_id": self._safe_int(attrs.get("roundId")),
            "player_id_tracker": attrs.get("playerId"),
            "team_id": self._safe_int(attrs.get("teamId"), -1),
            "side": self._map_side(attrs.get("sideId") or meta.get("sideName")),
            "operator": meta.get("operatorName") or attrs.get("operatorId"),
            "result": attrs.get("resultId"),
            "is_disconnected": 1 if attrs.get("isDisconnected") else 0,
            "kills": self._safe_int(self._stat(stats, "kills")),
            "deaths": self._safe_int(self._stat(stats, "deaths")),
            "assists": self._safe_int(self._stat(stats, "assists")),
            "headshots": self._safe_int(self._stat(stats, "headshots")),
            "first_blood": self._safe_int(self._stat(stats, "firstBloods")),
            "first_death": self._safe_int(self._stat(stats, "firstDeaths")),
            "clutch_won": self._safe_int(self._stat(stats, "clutches")),
            "clutch_lost": self._safe_int(self._stat(stats, "clutchesLost")),
            "hs_pct": self._safe_float(self._stat(stats, "headshotPct")),
            "esr": self._safe_float(self._stat(stats, "esr")),
        }

    def get_match_list(self, username: str, next_token: Optional[int] = None) -> Dict[str, Any]:
        base = f"{self.BASE}/matches/ubi/{username}"
        if next_token is None:
            url = base
        else:
            url = f"{base}?{urlencode({'next': next_token})}"
        payload = self._get_json(url)
        return self.parse_match_list(payload)

    def get_match_detail(self, match_id: str) -> Dict[str, Any]:
        payload = self._get_json(f"{self.BASE}/matches/{match_id}")
        grouped = self.parse_match_detail_segments(payload)
        return {
            "match_id": match_id,
            "players": [self.parse_player_overview(s) for s in grouped["overview"]],
            "round_outcomes": [self.parse_round_outcome(s) for s in grouped["round-overview"]],
            "player_rounds": [self.parse_player_round(s) for s in grouped["player-round"]],
            "segment_counts": {k: len(v) for k, v in grouped.items()},
        }

    def get_all_matches(
        self,
        username: str,
        max_pages: Optional[int] = None,
        since_date: Optional[Any] = None,
        show_progress: bool = False,
    ) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []
        next_token: Optional[int] = None
        pages_fetched = 0
        cutoff = self._normalize_since_date(since_date)

        while True:
            if max_pages is not None and pages_fetched >= max_pages:
                break
            page = self.get_match_list(username, next_token=next_token)
            page_matches = page.get("matches", [])
            stop_for_cutoff = False
            if cutoff is not None:
                filtered: List[Dict[str, Any]] = []
                for item in page_matches:
                    ts = self._parse_timestamp(item.get("timestamp"))
                    if ts is None or ts >= cutoff:
                        filtered.append(item)
                    else:
                        stop_for_cutoff = True
                page_matches = filtered

            matches.extend(page_matches)
            next_token = page.get("next")
            pages_fetched += 1
            if show_progress:
                self._progress_print(f"  📄 Page {pages_fetched} ({len(matches)} matches)")

            if stop_for_cutoff:
                break
            if next_token is None:
                break
            time.sleep(self.sleep_seconds)

        return matches

    def scrape_full_match_history(
        self,
        username: str,
        max_matches: Optional[int] = None,
        since_date: Optional[Any] = None,
        skip_match_ids: Optional[set[str]] = None,
        show_progress: bool = False,
    ) -> List[Dict[str, Any]]:
        pages = None
        if max_matches is not None:
            pages = max(1, (max_matches + 19) // 20)
        all_matches = self.get_all_matches(
            username,
            max_pages=pages,
            since_date=since_date,
            show_progress=show_progress,
        )
        selected = all_matches[:max_matches] if max_matches is not None else all_matches
        total = len(selected)
        skip_match_ids = skip_match_ids or set()

        if show_progress:
            self._progress_print(f"✅ Match history ({total} matches found)")

        out: List[Dict[str, Any]] = []
        detail_calls = 0
        for i, match in enumerate(selected, 1):
            match_id = match.get("match_id")
            if not match_id:
                continue
            if match_id in skip_match_ids:
                continue

            map_name = match.get("map") or "Unknown Map"
            if show_progress:
                self._progress_print(
                    f"\r  ⚔️  Match {i}/{total}  {map_name:<20}",
                    end="",
                )

            try:
                detail_calls += 1
                detail = self.get_match_detail(match_id)
            except HTTPError as exc:
                if exc.code == 404:
                    continue
                if exc.code == 429:
                    time.sleep(10)
                    try:
                        detail = self.get_match_detail(match_id)
                    except Exception:
                        continue
                else:
                    continue
            except URLError:
                continue
            except Exception:
                continue

            detail["match_meta"] = match
            out.append(detail)
            time.sleep(random.uniform(self.detail_sleep_min_seconds, self.detail_sleep_max_seconds))
            if detail_calls % self.detail_batch_size == 0:
                time.sleep(self.detail_batch_pause_seconds)

        if show_progress and total > 0:
            self._progress_print("")

        return out
