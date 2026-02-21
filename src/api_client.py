from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
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
    PROFILE_SEASON_FIELDS = (
        "matchesPlayed",
        "matchesWon",
        "matchesLost",
        "matchesAbandoned",
        "kills",
        "deaths",
        "assists",
        "headshots",
        "firstBloods",
        "firstDeaths",
        "clutches",
        "clutchesLost",
        "clutches1v1",
        "clutches1v2",
        "clutches1v3",
        "clutches1v4",
        "clutches1v5",
        "clutchesLost1v1",
        "clutchesLost1v2",
        "clutchesLost1v3",
        "clutchesLost1v4",
        "clutchesLost1v5",
        "kills1K",
        "kills2K",
        "kills3K",
        "kills4K",
        "kills5K",
        "rankPoints",
        "maxRankPoints",
        "rank",
        "maxRank",
        "kdRatio",
        "headshotPct",
        "esr",
        "killsPerRound",
        "deathsPerRound",
        "assistsPerRound",
        "roundsPlayed",
        "roundsWon",
        "roundsLost",
        "winPercentage",
        "elo",
        "timePlayed",
        "roundsDisconnected",
        "teamKills",
        "topRankPosition",
        "score",
        "killsPerGame",
        "headshotsPerRound",
        "roundWinPct",
    )

    def __init__(
        self,
        timeout_seconds: int = 20,
        sleep_seconds: float = 0.5,
        detail_sleep_min_seconds: float = 2.5,
        detail_sleep_max_seconds: float = 4.0,
        detail_batch_size: int = 10,
        detail_batch_pause_seconds: float = 15.0,
        min_request_interval_seconds: float = 1.0,
        max_429_wait_seconds: float = 120.0,
    ):
        self.timeout_seconds = timeout_seconds
        self.sleep_seconds = sleep_seconds
        self.detail_sleep_min_seconds = detail_sleep_min_seconds
        self.detail_sleep_max_seconds = detail_sleep_max_seconds
        self.detail_batch_size = detail_batch_size
        self.detail_batch_pause_seconds = detail_batch_pause_seconds
        self.min_request_interval_seconds = max(0.0, float(min_request_interval_seconds))
        self.max_429_wait_seconds = max(1.0, float(max_429_wait_seconds))
        self._next_request_not_before = 0.0

    @staticmethod
    def _progress_print(message: str, end: str = "\n") -> None:
        try:
            print(message, end=end, flush=True)
        except UnicodeEncodeError:
            fallback = message.replace("📄", "[PAGE]").replace("⚔️", "[MATCH]")
            print(fallback, end=end, flush=True)

    @staticmethod
    def _retry_after_seconds(value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            pass
        try:
            dt = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
        except ValueError:
            return None

    def _respect_global_throttle(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return
        now = time.monotonic()
        if now < self._next_request_not_before:
            time.sleep(self._next_request_not_before - now)
        self._next_request_not_before = time.monotonic() + self.min_request_interval_seconds

    def _get_json(self, url: str, retry_429: bool = True) -> Dict[str, Any]:
        self._respect_global_throttle()
        req = Request(url, headers=self.HEADERS, method="GET")
        try:
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 429 and retry_429:
                retry_after_header = exc.headers.get("Retry-After") if exc.headers else None
                wait_seconds = self._retry_after_seconds(retry_after_header)
                if wait_seconds is None:
                    wait_seconds = 10.0
                wait_seconds = min(wait_seconds, self.max_429_wait_seconds)
                time.sleep(wait_seconds)
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

    @classmethod
    def _stats_raw_map(cls, stats: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(stats, dict):
            return {}
        out: Dict[str, Any] = {}
        for key, node in stats.items():
            if isinstance(node, dict):
                out[key] = node.get("value")
            else:
                out[key] = None
        return out

    @classmethod
    def _find_profile_segment(
        cls,
        segments: List[Dict[str, Any]],
        *,
        kind: str,
        season: Optional[int] = None,
        gamemode: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        for segment in segments:
            if segment.get("type") != kind:
                continue
            attrs = segment.get("attributes", {})
            if season is not None and cls._safe_int(attrs.get("season"), -1) != season:
                continue
            if gamemode is not None and attrs.get("gamemode") != gamemode:
                continue
            return segment
        return None

    def parse_profile(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        platform_info = data.get("platformInfo", {}) if isinstance(data, dict) else {}
        segments = data.get("segments", []) if isinstance(data, dict) else []

        season_segment = self._find_profile_segment(
            segments,
            kind="season",
            season=40,
            gamemode="pvp_ranked",
        )
        if season_segment is None:
            ranked_seasons = [
                s for s in segments if s.get("type") == "season" and (s.get("attributes", {}) or {}).get("gamemode") == "pvp_ranked"
            ]
            ranked_seasons.sort(
                key=lambda s: self._safe_int((s.get("attributes", {}) or {}).get("season"), -1),
                reverse=True,
            )
            season_segment = ranked_seasons[0] if ranked_seasons else {}

        overview_segment = self._find_profile_segment(segments, kind="overview") or {}
        season_stats_raw = self._stats_raw_map((season_segment or {}).get("stats", {}))
        overview_stats_raw = self._stats_raw_map((overview_segment or {}).get("stats", {}))

        season_stats = {
            key: season_stats_raw.get(key)
            for key in self.PROFILE_SEASON_FIELDS
            if key in season_stats_raw
        }

        return {
            "uuid": platform_info.get("platformUserId"),
            "username": platform_info.get("platformUserHandle") or platform_info.get("platformUserIdentifier"),
            "season_stats": season_stats,
            "career_stats": overview_stats_raw,
        }

    def get_profile(self, username: str) -> Dict[str, Any]:
        payload = self._get_json(f"{self.BASE}/profile/ubi/{username}")
        return self.parse_profile(payload)

    def _parse_encounters_payload(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data", [])
        if isinstance(data, dict):
            players = data.get("items") or data.get("players") or []
        else:
            players = data

        encounters: List[Dict[str, Any]] = []
        for player in players if isinstance(players, list) else []:
            if not isinstance(player, dict):
                continue
            name = player.get("name") or player.get("platformUserHandle") or player.get("displayName")
            if not name:
                continue
            encounters.append(
                {
                    "name": name,
                    "profileId": player.get("profileId") or player.get("platformUserId"),
                    "count": self._safe_int(player.get("count"), 1),
                    "latestMatch": player.get("latestMatch", ""),
                    "stats": player.get("stats", {}),
                }
            )
        return encounters

    def _get_encounters_from_recent_matches(self, username: str, max_matches: int = 8) -> List[Dict[str, Any]]:
        match_list = self.get_all_matches(username, max_pages=1, show_progress=False)[:max_matches]
        if not match_list:
            return []

        by_name: Dict[str, Dict[str, Any]] = {}
        owner = username.lower()
        for match in match_list:
            match_id = match.get("match_id")
            if not match_id:
                continue
            try:
                detail = self.get_match_detail(match_id)
            except Exception:
                continue

            timestamp = (match.get("timestamp") or "")
            for player in detail.get("players", []):
                if not isinstance(player, dict):
                    continue
                name = player.get("username")
                if not name or name.lower() == owner:
                    continue
                row = by_name.setdefault(
                    name,
                    {
                        "name": name,
                        "profileId": player.get("player_id_tracker"),
                        "count": 0,
                        "latestMatch": "",
                        "stats": {},
                    },
                )
                row["count"] += 1
                if timestamp and (not row["latestMatch"] or timestamp > row["latestMatch"]):
                    row["latestMatch"] = timestamp
        return sorted(by_name.values(), key=lambda r: int(r.get("count", 0)), reverse=True)

    def get_encounters(self, uuid: Optional[str] = None, username: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get players encountered by a user, with fallback if endpoint is unavailable."""
        candidates: List[str] = []
        if uuid:
            candidates.append(f"{self.BASE}/stats/played-with/ubi/{quote(str(uuid), safe='')}")
        if username:
            candidates.append(f"{self.BASE}/stats/played-with/ubi/{quote(str(username), safe='')}")

        for url in candidates:
            try:
                encounters = self._parse_encounters_payload(self._get_json(url))
                if encounters:
                    return encounters
            except HTTPError as exc:
                if exc.code not in (403, 404):
                    raise
            except URLError:
                raise

        if username:
            return self._get_encounters_from_recent_matches(username)
        return []

    @staticmethod
    def _segments_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            segments = data.get("segments", [])
            if isinstance(segments, list):
                return [item for item in segments if isinstance(item, dict)]
        return []

    @staticmethod
    def _first_value(stats: Dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in stats:
                return stats.get(key)
        return None

    def parse_operator_segments(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for segment in self._segments_from_payload(payload):
            attrs = segment.get("attributes", {})
            meta = segment.get("metadata", {})
            raw = self._stats_raw_map(segment.get("stats", {}))
            out.append(
                {
                    "operator_slug": attrs.get("operator"),
                    "operator_name": meta.get("operatorName") or attrs.get("operator"),
                    "side": meta.get("sideName"),
                    "rounds": self._safe_int(self._first_value(raw, "roundsPlayed", "matchesPlayed")),
                    "win_pct": self._safe_float(raw.get("winPercentage")),
                    "kd": self._safe_float(self._first_value(raw, "kdRatio", "kdratio")),
                    "hs_pct": self._safe_float(self._first_value(raw, "headshotPct", "headshotPercentage")),
                    "kills": self._safe_int(raw.get("kills")),
                    "deaths": self._safe_int(raw.get("deaths")),
                    "wins": self._safe_int(self._first_value(raw, "matchesWon", "roundsWon")),
                    "losses": self._safe_int(self._first_value(raw, "matchesLost", "roundsLost")),
                    "assists": self._safe_int(raw.get("assists")),
                    "aces": self._safe_int(self._first_value(raw, "aces", "kills5K")),
                    "teamkills": self._safe_int(raw.get("teamKills")),
                    "matches": self._safe_int(raw.get("matchesPlayed")),
                    "abandons": self._safe_int(raw.get("matchesAbandoned")),
                    "esr": self._safe_float(raw.get("esr")),
                }
            )
        return out

    def get_operator_stats(self, username: str) -> List[Dict[str, Any]]:
        url = (
            f"{self.BASE}/profile/ubi/{username}/segments/operator?"
            f"{urlencode({'sessionType': 'ranked', 'season': 'all'})}"
        )
        return self.parse_operator_segments(self._get_json(url))

    def parse_map_segments(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for item in self._parse_map_segments_with_side(payload):
            row = dict(item)
            row.pop("side", None)
            out.append(row)
        return out

    def _parse_map_segments_with_side(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for segment in self._segments_from_payload(payload):
            attrs = segment.get("attributes", {})
            meta = segment.get("metadata", {})
            raw = self._stats_raw_map(segment.get("stats", {}))
            out.append(
                {
                    "map_slug": attrs.get("map"),
                    "map_name": meta.get("mapName") or attrs.get("map"),
                    "side": attrs.get("side"),
                    "matches": self._safe_int(raw.get("matchesPlayed")),
                    "win_pct": self._safe_float(raw.get("winPercentage")),
                    "wins": self._safe_int(raw.get("matchesWon")),
                    "losses": self._safe_int(raw.get("matchesLost")),
                    "abandons": self._safe_int(raw.get("matchesAbandoned")),
                    "kd": self._safe_float(self._first_value(raw, "kdRatio", "kdratio")),
                    "atk_win_pct": self._safe_float(
                        self._first_value(raw, "attackerWinPercentage", "attackWinPercentage", "atkWinPercentage")
                    ),
                    "def_win_pct": self._safe_float(
                        self._first_value(raw, "defenderWinPercentage", "defenceWinPercentage", "defWinPercentage")
                    ),
                    "hs_pct": self._safe_float(self._first_value(raw, "headshotPct", "headshotPercentage")),
                    "esr": self._safe_float(raw.get("esr")),
                }
            )
        return out

    def get_map_stats(self, username: str) -> List[Dict[str, Any]]:
        base_url = f"{self.BASE}/profile/ubi/{username}/segments/map"
        base_rows = self._parse_map_segments_with_side(
            self._get_json(
                f"{base_url}?{urlencode({'sessionType': 'ranked', 'season': 'all'})}"
            )
        )
        atk_rows = self._parse_map_segments_with_side(
            self._get_json(
                f"{base_url}?{urlencode({'sessionType': 'ranked', 'season': 'all', 'side': 'attacker'})}"
            )
        )
        def_rows = self._parse_map_segments_with_side(
            self._get_json(
                f"{base_url}?{urlencode({'sessionType': 'ranked', 'season': 'all', 'side': 'defender'})}"
            )
        )

        merged: Dict[str, Dict[str, Any]] = {}

        def _ensure(slug: str, row: Dict[str, Any]) -> Dict[str, Any]:
            if slug not in merged:
                merged[slug] = {
                    "map_slug": slug,
                    "map_name": row.get("map_name") or slug,
                    "matches": row.get("matches", 0),
                    "win_pct": row.get("win_pct", 0.0),
                    "wins": row.get("wins", 0),
                    "losses": row.get("losses", 0),
                    "abandons": row.get("abandons", 0),
                    "kd": row.get("kd", 0.0),
                    "atk_win_pct": 0.0,
                    "def_win_pct": 0.0,
                    "hs_pct": row.get("hs_pct", 0.0),
                    "esr": row.get("esr", 0.0),
                }
            return merged[slug]

        def _apply(rows: List[Dict[str, Any]], forced_side: Optional[str] = None) -> None:
            for row in rows:
                slug = row.get("map_slug")
                if not slug:
                    continue
                item = _ensure(slug, row)
                side = forced_side or row.get("side")

                if forced_side is None and not side:
                    item.update(
                        {
                            "map_name": row.get("map_name") or item.get("map_name"),
                            "matches": row.get("matches", item.get("matches")),
                            "win_pct": row.get("win_pct", item.get("win_pct")),
                            "wins": row.get("wins", item.get("wins")),
                            "losses": row.get("losses", item.get("losses")),
                            "abandons": row.get("abandons", item.get("abandons")),
                            "kd": row.get("kd", item.get("kd")),
                            "hs_pct": row.get("hs_pct", item.get("hs_pct")),
                            "esr": row.get("esr", item.get("esr")),
                        }
                    )

                if side == "attacker":
                    item["atk_win_pct"] = row.get("win_pct", item.get("atk_win_pct"))
                elif side == "defender":
                    item["def_win_pct"] = row.get("win_pct", item.get("def_win_pct"))

        _apply(base_rows)
        _apply(atk_rows, forced_side="attacker")
        _apply(def_rows, forced_side="defender")

        return sorted(merged.values(), key=lambda x: (x.get("map_name") or ""))

    def season_stats_to_snapshot(self, season_stats: Dict[str, Any]) -> Dict[str, Any]:
        matches = self._safe_int(season_stats.get("matchesPlayed"))
        wins = self._safe_int(season_stats.get("matchesWon"))
        losses = self._safe_int(season_stats.get("matchesLost"))
        abandons = self._safe_int(season_stats.get("matchesAbandoned"))
        rounds_played = self._safe_int(season_stats.get("roundsPlayed"))
        rounds_won = self._safe_int(season_stats.get("roundsWon"))
        rounds_lost = self._safe_int(season_stats.get("roundsLost"))
        kills = self._safe_int(season_stats.get("kills"))
        headshots = self._safe_int(season_stats.get("headshots"))
        rank = self._safe_int(season_stats.get("rank"))
        max_rank = self._safe_int(season_stats.get("maxRank"), rank)
        top_rank_position = season_stats.get("topRankPosition")
        top_rank_position = None if top_rank_position is None else self._safe_int(top_rank_position)
        time_played_seconds = self._safe_float(season_stats.get("timePlayed"))

        round_win_pct = self._safe_float(season_stats.get("roundWinPct"))
        if round_win_pct <= 0 and rounds_played > 0:
            round_win_pct = (rounds_won / rounds_played) * 100.0

        headshots_per_round = self._safe_float(season_stats.get("headshotsPerRound"))
        if headshots_per_round <= 0 and rounds_played > 0:
            headshots_per_round = headshots / rounds_played

        kills_per_game = self._safe_float(season_stats.get("killsPerGame"))
        if kills_per_game <= 0 and matches > 0:
            kills_per_game = kills / matches

        return {
            "game": {
                "abandons": abandons,
                "matches": matches,
                "wins": wins,
                "losses": losses,
                "match_win_pct": self._safe_float(season_stats.get("winPercentage")),
                "time_played_hours": time_played_seconds / 3600.0,
                "score": self._safe_int(season_stats.get("score")),
            },
            "rounds": {
                "disconnected": self._safe_int(season_stats.get("roundsDisconnected")),
                "rounds_played": rounds_played,
                "rounds_wins": rounds_won,
                "rounds_losses": rounds_lost,
                "win_pct": round_win_pct,
            },
            "combat": {
                "kills": kills,
                "deaths": self._safe_int(season_stats.get("deaths")),
                "assists": self._safe_int(season_stats.get("assists")),
                "kd": self._safe_float(season_stats.get("kdRatio")),
                "kills_per_round": self._safe_float(season_stats.get("killsPerRound")),
                "deaths_per_round": self._safe_float(season_stats.get("deathsPerRound")),
                "assists_per_round": self._safe_float(season_stats.get("assistsPerRound")),
                "kills_per_game": kills_per_game,
                "headshots": headshots,
                "headshots_per_round": headshots_per_round,
                "hs_pct": self._safe_float(season_stats.get("headshotPct")),
                "first_bloods": self._safe_int(season_stats.get("firstBloods")),
                "first_deaths": self._safe_int(season_stats.get("firstDeaths")),
                "teamkills": self._safe_int(season_stats.get("teamKills")),
                "esr": self._safe_float(season_stats.get("esr")),
            },
            "clutches": {
                "total": self._safe_int(season_stats.get("clutches")),
                "1v1": self._safe_int(season_stats.get("clutches1v1")),
                "1v2": self._safe_int(season_stats.get("clutches1v2")),
                "1v3": self._safe_int(season_stats.get("clutches1v3")),
                "1v4": self._safe_int(season_stats.get("clutches1v4")),
                "1v5": self._safe_int(season_stats.get("clutches1v5")),
                "lost_total": self._safe_int(season_stats.get("clutchesLost")),
                "lost_1v1": self._safe_int(season_stats.get("clutchesLost1v1")),
                "lost_1v2": self._safe_int(season_stats.get("clutchesLost1v2")),
                "lost_1v3": self._safe_int(season_stats.get("clutchesLost1v3")),
                "lost_1v4": self._safe_int(season_stats.get("clutchesLost1v4")),
                "lost_1v5": self._safe_int(season_stats.get("clutchesLost1v5")),
            },
            "multikills": {
                "aces": self._safe_int(season_stats.get("kills5K")),
                "1k": self._safe_int(season_stats.get("kills1K")),
                "2k": self._safe_int(season_stats.get("kills2K")),
                "3k": self._safe_int(season_stats.get("kills3K")),
                "4k": self._safe_int(season_stats.get("kills4K")),
            },
            "ranked": {
                "current_rank": rank,
                "max_rank": max_rank,
                "top_rank_position": top_rank_position,
            },
            "uncategorized": {
                "rank_points": self._safe_int(season_stats.get("rankPoints")),
                "max_rank_points": self._safe_int(season_stats.get("maxRankPoints")),
                "trn_elo": self._safe_int(season_stats.get("elo")),
            },
        }

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
