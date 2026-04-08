from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timezone
from itertools import combinations
import json
import os
import re
import sys
import time
import math
import base64
import hashlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api_client import TrackerAPIClient
from src.database import Database
from src.plugins.v3_round_analysis import RoundAnalysisPlugin
from src.plugins.v3_teammate_chemistry import TeammateChemistryPlugin
from src.plugins.v3_lobby_quality import LobbyQualityPlugin
from src.plugins.v3_trade_analysis import TradeAnalysisPlugin
from src.plugins.v3_team_analysis import TeamAnalysisPlugin
from src.plugins.v3_enemy_operator_threat import EnemyOperatorThreatPlugin
from src.plugins.v2_operator_stats import OperatorStatsPlugin
from src.plugins.v2_map_stats import MapStatsPlugin
from src.db_standardizer import DatabaseStandardizer
from src.analytics.insights.engine import run_insight_engine, INSIGHTS_VERSION
from src.cache import (
    _ensure_workspace_cache_tables,
    _workspace_insights_cache_get,
    _workspace_insights_cache_set,
    _workspace_scope_cache_get,
    _workspace_scope_cache_set,
    _workspace_team_cache_get,
    _workspace_team_cache_set,
    configure_workspace_cache,
)
from src.ws_handlers.match_scrape import configure_match_scrape, register_match_scrape_routes
from src.ws_handlers.network_scan import configure_network_scan, register_network_scan_routes
from src.utils import (
    _is_unknown_operator_name,
    _normalize_asset_key,
    _normalize_mode_key,
    _parse_iso_datetime,
    _pctile_abs_bound,
    _wilson_ci,
)

app = FastAPI()
app.mount("/static", StaticFiles(directory="web/static"), name="static")
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _canonical_queue_key(raw_mode: object) -> str:
    tokens = set(re.findall(r"[a-z0-9]+", str(raw_mode or "").strip().lower()))
    if not tokens:
        return "other"
    if "unranked" in tokens:
        return "standard"
    if "ranked" in tokens:
        return "ranked"
    if "standard" in tokens:
        return "standard"
    if "quickmatch" in tokens or "casual" in tokens or ("quick" in tokens and "match" in tokens):
        return "quickmatch"
    if "event" in tokens or "arcade" in tokens:
        return "event"
    return "other"


map_images_dir = None
for candidate in ("map_images", "map images"):
    candidate_path = os.path.join(project_root, candidate)
    if os.path.isdir(candidate_path):
        map_images_dir = candidate_path
        break

if map_images_dir:
    app.mount("/map-images", StaticFiles(directory=map_images_dir), name="map-images")
    try:
        map_file_count = len(
            [name for name in os.listdir(map_images_dir) if os.path.isfile(os.path.join(map_images_dir, name))]
        )
    except Exception:
        map_file_count = -1
    print(f"[MAP] Serving map images from: {map_images_dir} (files={map_file_count})")
else:
    print(f"[MAP] Warning: no map image folder found under {project_root}")

operator_images_dir = None
operator_image_candidates = (
    "operator_images",
    "operator images",
    "operator-icons",
    "operator icons",
    "operators",
    os.path.join("web", "static", "operator_images"),
    os.path.join("web", "static", "operator-images"),
    os.path.join("web", "static", "operators"),
)
for candidate in operator_image_candidates:
    candidate_path = os.path.join(project_root, candidate)
    if os.path.isdir(candidate_path):
        operator_images_dir = candidate_path
        break

operator_image_file_by_key = {}
if operator_images_dir:
    app.mount("/operator-images", StaticFiles(directory=operator_images_dir), name="operator-images")
    try:
        operator_files = [
            name for name in os.listdir(operator_images_dir) if os.path.isfile(os.path.join(operator_images_dir, name))
        ]
        for filename in operator_files:
            stem = os.path.splitext(filename)[0]
            key = _normalize_asset_key(stem)
            if key and key not in operator_image_file_by_key:
                operator_image_file_by_key[key] = filename
        print(
            f"[OPERATORS] Serving operator images from: {operator_images_dir} "
            f"(files={len(operator_files)}, indexed={len(operator_image_file_by_key)})"
        )
    except Exception:
        operator_image_file_by_key = {}
        print(f"[OPERATORS] Warning: failed to index operator images from {operator_images_dir}")
else:
    print(
        f"[OPERATORS] Warning: no operator image folder found under {project_root}. "
        f"Searched: {', '.join(operator_image_candidates)}"
    )

api_client = TrackerAPIClient()
db = Database(os.environ.get("JAKAL_DB_PATH", "data/jakal_fresh.db"))
print(f"[DB] Using database at: {os.path.abspath(db.db_path)}")
try:
    unpack_stats = db.unpack_pending_scraped_match_cards()
    print(
        "[DB] Auto-unpack complete: "
        f"scanned={unpack_stats.get('scanned', 0)} "
        f"unpacked={unpack_stats.get('unpacked_matches', 0)} "
        f"errors={unpack_stats.get('errors', 0)}"
    )
except Exception as e:
    print(f"[DB] Warning: auto-unpack on startup failed: {e}")

rate_tracker = {
    "calls_made": 0,
    "calls_in_window": [],
    "last_call": None,
    "cooling_until": None,
    "window_seconds": 600,
    "max_calls": 30,
}

# Error tracking for monitoring failures
error_tracker = {
    "consecutive_failures": 0,
    "total_failures": 0,
    "last_error": None,
    "failure_threshold": 5,
}

WORKSPACE_API_VERSION = 1
workspace_cache: dict[str, tuple[float, dict]] = {}
WORKSPACE_CACHE_TTL_SECONDS = 90


def _get_db_cursor():
    conn = getattr(db, "conn", None)
    if conn is None:
        raise RuntimeError("Database connection is not initialized.")
    return conn.cursor()

def _iter_chunks(values: list[str], size: int = 800):
    chunk_size = max(1, int(size))
    for i in range(0, len(values), chunk_size):
        yield values[i : i + chunk_size]


configure_workspace_cache(db, _get_db_cursor)

try:
    _ensure_workspace_cache_tables()
except Exception as e:
    print(f"[DB] Warning: workspace cache table init failed: {e}")


def track_call(endpoint: str) -> None:
    now = time.time()
    rate_tracker["calls_made"] += 1
    rate_tracker["last_call"] = now
    rate_tracker["calls_in_window"].append((now, endpoint))

    cutoff = now - rate_tracker["window_seconds"]
    rate_tracker["calls_in_window"] = [
        (ts, ep) for ts, ep in rate_tracker["calls_in_window"] if ts > cutoff
    ]


def get_rate_status() -> dict:
    now = time.time()
    recent_calls = len(rate_tracker["calls_in_window"])

    if rate_tracker["cooling_until"] and now < rate_tracker["cooling_until"]:
        return {
            "status": "cooling",
            "calls_in_window": recent_calls,
            "calls_made": rate_tracker["calls_made"],
            "max_calls": rate_tracker["max_calls"],
            "cooling_seconds": int(rate_tracker["cooling_until"] - now),
        }

    if recent_calls >= rate_tracker["max_calls"]:
        return {
            "status": "danger",
            "calls_in_window": recent_calls,
            "calls_made": rate_tracker["calls_made"],
            "max_calls": rate_tracker["max_calls"],
        }
    if recent_calls >= rate_tracker["max_calls"] * 0.8:
        return {
            "status": "warning",
            "calls_in_window": recent_calls,
            "calls_made": rate_tracker["calls_made"],
            "max_calls": rate_tracker["max_calls"],
        }
    return {
        "status": "safe",
        "calls_in_window": recent_calls,
        "calls_made": rate_tracker["calls_made"],
        "max_calls": rate_tracker["max_calls"],
    }


@app.get("/")
async def root() -> HTMLResponse:
    with open("web/static/index.html", encoding="utf-8") as f:
        return HTMLResponse(
            content=f.read(),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )


@app.get("/api/rate-status")
async def rate_status() -> dict:
    return get_rate_status()


@app.get("/api/operator-image-index")
async def operator_image_index() -> dict:
    return {
        "enabled": bool(operator_images_dir),
        "count": len(operator_image_file_by_key),
        "files": operator_image_file_by_key,
    }


@app.get("/api/scraped-matches/{username}")
async def scraped_matches(username: str, limit: int = 50) -> dict:
    safe_limit = max(1, min(limit, 10000))
    try:
        matches = db.get_scraped_match_cards(username, safe_limit)
        return {"username": username, "matches": matches, "count": len(matches)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load saved matches: {str(e)}")


@app.post("/api/unpack-scraped-matches/{username}")
async def unpack_scraped_matches(username: str, limit: int = 2000) -> dict:
    safe_limit = max(1, min(limit, 5000))
    try:
        stats = db.unpack_pending_scraped_match_cards(username=username, limit=safe_limit)
        return {"username": username, "limit": safe_limit, "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to unpack scraped matches: {str(e)}")


@app.post("/api/delete-bad-scraped-matches/{username}")
async def delete_bad_scraped_matches(username: str) -> dict:
    try:
        stats = db.delete_bad_scraped_matches(username)
        return {"username": username, "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete bad scraped matches: {str(e)}")


@app.get("/api/round-analysis/{username}")
async def round_analysis(username: str) -> dict:
    try:
        analysis = RoundAnalysisPlugin(db, username).analyze()
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run round analysis: {str(e)}")


@app.get("/api/teammate-chemistry/{username}")
async def teammate_chemistry(username: str) -> dict:
    try:
        analysis = TeammateChemistryPlugin(db, username).analyze()
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run teammate chemistry: {str(e)}")


@app.get("/api/lobby-quality/{username}")
async def lobby_quality(username: str) -> dict:
    try:
        analysis = LobbyQualityPlugin(db, username).analyze()
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run lobby quality: {str(e)}")


@app.get("/api/trade-analysis/{username}")
async def trade_analysis(username: str, window_seconds: float = 5.0) -> dict:
    try:
        analysis = TradeAnalysisPlugin(db, username, window_seconds=window_seconds).analyze()
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run trade analysis: {str(e)}")


@app.get("/api/team-analysis/{username}")
async def team_analysis(username: str) -> dict:
    try:
        analysis = TeamAnalysisPlugin(db, username).analyze()
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run team analysis: {str(e)}")


@app.get("/api/enemy-operator-threat/{username}")
async def enemy_operator_threat(username: str) -> dict:
    try:
        analysis = EnemyOperatorThreatPlugin(db, username).analyze()
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run enemy operator threat analysis: {str(e)}")


@app.get("/api/operator-stats/{username}")
async def operator_stats(username: str) -> dict:
    try:
        analysis = OperatorStatsPlugin(db, username).analyze()
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run operator stats: {str(e)}")


@app.get("/api/map-stats/{username}")
async def map_stats(username: str) -> dict:
    try:
        analysis = MapStatsPlugin(db, username).analyze()
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run map stats: {str(e)}")


@app.get("/api/players/encountered")
async def players_encountered(username: str, match_type: str = "Ranked") -> dict:
    try:
        rows = db.get_encountered_players(username, match_type=match_type)
        return {
            "username": username,
            "match_type": match_type,
            "players": rows,
            "count": len(rows),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load encountered players: {str(e)}")


@app.get("/api/players/friends")
async def players_friends(tag: str = "friend") -> dict:
    try:
        rows = db.get_tagged_players(tag=tag)
        return {"tag": tag, "players": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load tagged players: {str(e)}")


@app.post("/api/players/tag")
async def players_tag(request: Request) -> dict:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    username = str((payload or {}).get("username") or "").strip()
    tag = str((payload or {}).get("tag") or "friend").strip().lower()
    enabled = bool((payload or {}).get("enabled", True))
    if not username:
        raise HTTPException(status_code=400, detail="username is required")
    if not tag:
        raise HTTPException(status_code=400, detail="tag is required")
    try:
        result = db.set_player_tag(username, tag, enabled=enabled)
        return {"ok": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update player tag: {str(e)}")


@app.get("/api/stack/synergy")
async def stack_synergy(players: str, match_type: str = "Ranked") -> dict:
    raw = [p.strip() for p in str(players or "").split(",") if p and p.strip()]
    dedup = []
    seen = set()
    for p in raw:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(p)
    if len(dedup) < 2:
        raise HTTPException(status_code=400, detail="Provide at least 2 players in players query param.")
    try:
        analysis = db.compute_stack_synergy(dedup, match_type=match_type)
        return {"players": dedup, "match_type": match_type, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute stack synergy: {str(e)}")


@app.get("/api/stack/debug")
async def stack_debug(players: str, match_type: str = "Ranked") -> dict:
    raw = [p.strip() for p in str(players or "").split(",") if p and p.strip()]
    dedup = []
    seen = set()
    for p in raw:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(p)
    if len(dedup) < 2:
        raise HTTPException(status_code=400, detail="Provide at least 2 players in players query param.")
    try:
        debug = db.debug_stack_synergy(dedup, match_type=match_type)
        return {"players": dedup, "match_type": match_type, "debug": debug}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to debug stack synergy: {str(e)}")


@app.get("/api/players/list")
async def players_list() -> dict:
    try:
        rows = db.get_all_players()
        names = [str(r.get("username") or "").strip() for r in rows if str(r.get("username") or "").strip()]
        names = sorted(set(names), key=lambda x: x.lower())
        return {"players": names, "count": len(names)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load players list: {str(e)}")


@app.get("/api/operators/map-breakdown")
async def operators_map_breakdown(
    username: str,
    stack: str = "all",
    match_type: str = "Ranked",
    min_rounds: int = 5,
) -> dict:
    clean_username = str(username or "").strip()
    if not clean_username:
        raise HTTPException(status_code=400, detail="username is required")
    stack_key = str(stack or "solo").strip().lower()
    stack_to_friend_count = {
        "all": -1,
        "solo": 0,
        "duo": 1,
        "trio": 2,
        "squad": 3,
        "full": 4,
        "fullstack": 4,
        "full_stack": 4,
    }
    if stack_key not in stack_to_friend_count:
        raise HTTPException(status_code=400, detail="stack must be one of: all, solo, duo, trio, squad, full")
    friend_target = int(stack_to_friend_count[stack_key])
    min_rounds_safe = max(1, min(int(min_rounds), 50))
    mode_key = _canonical_queue_key(match_type)

    try:
        cursor = _get_db_cursor()
        cursor.execute(
            """
            SELECT LOWER(TRIM(p.username)) AS username_key
            FROM player_tags pt
            JOIN players p ON p.player_id = pt.player_id
            WHERE LOWER(TRIM(pt.tag)) = 'friend'
            """
        )
        friend_keys = {str(r["username_key"] or "").strip().lower() for r in cursor.fetchall() if str(r["username_key"] or "").strip()}

        cursor.execute(
            """
            SELECT match_id, team_id
            FROM match_detail_players
            WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))
            """,
            (clean_username,),
        )
        user_rows = cursor.fetchall()
        match_team_rows = []
        for row in user_rows:
            mid = str(row["match_id"] or "").strip()
            if not mid:
                continue
            team_id = row["team_id"]
            cursor.execute(
                """
                SELECT match_type
                     , match_type_key
                FROM match_detail_players
                WHERE match_id = ?
                  AND LOWER(TRIM(username)) = LOWER(TRIM(?))
                LIMIT 1
                """,
                (mid, clean_username),
            )
            mt = cursor.fetchone()
            mk = _canonical_queue_key((mt["match_type_key"] if mt and "match_type_key" in mt.keys() else "") or (mt["match_type"] if mt else ""))
            if mk != mode_key:
                continue
            match_team_rows.append((mid, team_id))

        if not match_team_rows:
            return {"username": clean_username, "stack": stack_key, "match_type": match_type, "maps": [], "low_data_maps": [], "eligible_matches": 0}

        eligible_match_ids = []
        for mid, team_id in match_team_rows:
            cursor.execute(
                """
                SELECT LOWER(TRIM(username)) AS teammate_key
                FROM match_detail_players
                WHERE match_id = ?
                  AND team_id = ?
                  AND LOWER(TRIM(username)) != LOWER(TRIM(?))
                """,
                (mid, team_id, clean_username),
            )
            teammates = {str(r["teammate_key"] or "").strip().lower() for r in cursor.fetchall() if str(r["teammate_key"] or "").strip()}
            friend_count = sum(1 for t in teammates if t in friend_keys)
            if friend_target < 0 or friend_count == friend_target:
                eligible_match_ids.append(mid)

        if not eligible_match_ids:
            return {"username": clean_username, "stack": stack_key, "match_type": match_type, "maps": [], "low_data_maps": [], "eligible_matches": 0}

        placeholders = ",".join("?" for _ in eligible_match_ids)
        params = [clean_username, *eligible_match_ids]

        cursor.execute(
            f"""
            WITH latest_cards AS (
                SELECT smc.match_id, COALESCE(NULLIF(TRIM(smc.map_name), ''), 'Unknown') AS map_name
                FROM scraped_match_cards smc
                JOIN (
                    SELECT match_id, MAX(id) AS max_id
                    FROM scraped_match_cards
                    GROUP BY match_id
                ) last ON last.match_id = smc.match_id AND last.max_id = smc.id
            )
            SELECT
                COALESCE(NULLIF(TRIM(pr.operator_key), ''), 'unknown') AS operator_key,
                MAX(COALESCE(NULLIF(TRIM(pr.operator), ''), 'UNKNOWN')) AS operator,
                LOWER(TRIM(COALESCE(pr.side, 'unknown'))) AS side,
                COALESCE(lc.map_name, 'Unknown') AS map_name,
                COUNT(*) AS rounds,
                AVG(CASE WHEN LOWER(TRIM(COALESCE(pr.result, ''))) IN ('win', 'victory') THEN 1.0 ELSE 0.0 END) AS win_rate,
                AVG(CASE WHEN COALESCE(pr.first_blood, 0) = 1 THEN 1.0 ELSE 0.0 END) AS fk_rate,
                AVG(CASE WHEN COALESCE(pr.first_death, 0) = 1 THEN 1.0 ELSE 0.0 END) AS fd_rate,
                AVG(CAST(pr.kills AS FLOAT) / NULLIF(pr.deaths, 0)) AS kd
            FROM player_rounds pr
            LEFT JOIN latest_cards lc ON lc.match_id = pr.match_id
            WHERE LOWER(TRIM(pr.username)) = LOWER(TRIM(?))
              AND pr.match_id IN ({placeholders})
            GROUP BY operator_key, side, map_name
            ORDER BY map_name, side, win_rate DESC
            """,
            params,
        )
        op_rows = [dict(r) for r in cursor.fetchall()]

        cursor.execute(
            f"""
            WITH latest_cards AS (
                SELECT smc.match_id, COALESCE(NULLIF(TRIM(smc.map_name), ''), 'Unknown') AS map_name
                FROM scraped_match_cards smc
                JOIN (
                    SELECT match_id, MAX(id) AS max_id
                    FROM scraped_match_cards
                    GROUP BY match_id
                ) last ON last.match_id = smc.match_id AND last.max_id = smc.id
            )
            SELECT
                COALESCE(lc.map_name, 'Unknown') AS map_name,
                LOWER(TRIM(COALESCE(pr.side, 'unknown'))) AS side,
                COUNT(*) AS rounds,
                AVG(CASE WHEN LOWER(TRIM(COALESCE(pr.result, ''))) IN ('win', 'victory') THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM player_rounds pr
            LEFT JOIN latest_cards lc ON lc.match_id = pr.match_id
            WHERE LOWER(TRIM(pr.username)) = LOWER(TRIM(?))
              AND pr.match_id IN ({placeholders})
            GROUP BY map_name, side
            """,
            params,
        )
        base_rows = [dict(r) for r in cursor.fetchall()]

        maps = defaultdict(lambda: {
            "map_name": "",
            "total_rounds": 0,
            "atk": [],
            "def": [],
            "baseline_atk": 0.0,
            "baseline_def": 0.0,
        })
        for row in base_rows:
            map_name = str(row.get("map_name") or "Unknown")
            side = str(row.get("side") or "unknown")
            rounds = int(row.get("rounds") or 0)
            wr = float(row.get("win_rate") or 0.0) * 100.0
            bucket = maps[map_name]
            bucket["map_name"] = map_name
            bucket["total_rounds"] += rounds
            if side.startswith("atk") or side == "attacker":
                bucket["baseline_atk"] = round(wr, 2)
            elif side.startswith("def") or side == "defender":
                bucket["baseline_def"] = round(wr, 2)

        for row in op_rows:
            map_name = str(row.get("map_name") or "Unknown")
            side = str(row.get("side") or "unknown")
            rounds = int(row.get("rounds") or 0)
            wr = float(row.get("win_rate") or 0.0) * 100.0
            fk = float(row.get("fk_rate") or 0.0) * 100.0
            fd = float(row.get("fd_rate") or 0.0) * 100.0
            kd = float(row.get("kd") or 0.0)
            bucket = maps[map_name]
            bucket["map_name"] = map_name
            base = bucket["baseline_atk"] if (side.startswith("atk") or side == "attacker") else bucket["baseline_def"]
            item = {
                "operator": str(row.get("operator") or "Unknown"),
                "rounds": rounds,
                "win_rate": round(wr, 2),
                "delta_vs_baseline": round(wr - base, 2),
                "fk_rate": round(fk, 2),
                "fd_rate": round(fd, 2),
                "kd": round(kd, 3),
                "meets_min_rounds": rounds >= min_rounds_safe,
            }
            if side.startswith("atk") or side == "attacker":
                bucket["atk"].append(item)
            elif side.startswith("def") or side == "defender":
                bucket["def"].append(item)

        high_data = []
        low_data = []
        for m in maps.values():
            m["atk"].sort(key=lambda x: (-x["delta_vs_baseline"], -x["rounds"], x["operator"].lower()))
            m["def"].sort(key=lambda x: (-x["delta_vs_baseline"], -x["rounds"], x["operator"].lower()))
            filtered_out = sum(1 for x in m["atk"] if not x["meets_min_rounds"]) + sum(1 for x in m["def"] if not x["meets_min_rounds"])
            m["filtered_out_by_min_rounds"] = filtered_out
            if filtered_out:
                print(
                    "[OPERATORS] min-round filter would hide rows: "
                    f"user={clean_username} map={m.get('map_name')} hidden={filtered_out} min_rounds={min_rounds_safe}"
                )
            if int(m.get("total_rounds", 0)) < 10:
                low_data.append({"map_name": m["map_name"], "total_rounds": int(m.get("total_rounds", 0))})
            else:
                high_data.append(m)

        high_data.sort(key=lambda x: (-int(x.get("total_rounds", 0)), str(x.get("map_name", "")).lower()))
        low_data.sort(key=lambda x: (-int(x.get("total_rounds", 0)), str(x.get("map_name", "")).lower()))
        return {
            "username": clean_username,
            "stack": stack_key,
            "match_type": match_type,
            "min_rounds": min_rounds_safe,
            "eligible_matches": len(set(eligible_match_ids)),
            "maps": high_data,
            "low_data_maps": low_data,
            "queue_key": mode_key,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute operators map breakdown: {str(e)}")


@app.get("/api/dev/operators/diagnostics")
async def dev_operator_diagnostics(
    username: str,
    map_name: str,
    side: str,
    match_type: str = "Ranked",
) -> dict:
    if str(os.getenv("JAKAL_ENABLE_DEV_DIAGNOSTICS", "1")).strip() not in {"1", "true", "TRUE", "yes", "on"}:
        raise HTTPException(status_code=404, detail="Not found")

    clean_username = str(username or "").strip()
    clean_map = str(map_name or "").strip()
    side_key = str(side or "").strip().lower()
    queue_key = _canonical_queue_key(match_type)
    if not clean_username or not clean_map:
        raise HTTPException(status_code=400, detail="username and map_name are required")
    if side_key not in {"attacker", "defender", "atk", "def"}:
        raise HTTPException(status_code=400, detail="side must be attacker/defender/atk/def")
    side_norm = "attacker" if side_key in {"attacker", "atk"} else "defender"

    try:
        cursor = _get_db_cursor()
        cursor.execute(
            """
            SELECT
                COUNT(*) AS baseline_n,
                AVG(CASE WHEN LOWER(TRIM(COALESCE(pr.result, ''))) IN ('win', 'victory') THEN 1.0 ELSE 0.0 END) AS baseline_wr
            FROM player_rounds pr
            LEFT JOIN (
                SELECT smc.match_id, COALESCE(NULLIF(TRIM(smc.map_name), ''), 'Unknown') AS map_name
                FROM scraped_match_cards smc
                JOIN (
                    SELECT match_id, MAX(id) AS max_id
                    FROM scraped_match_cards
                    GROUP BY match_id
                ) last ON last.match_id = smc.match_id AND last.max_id = smc.id
            ) lc ON lc.match_id = pr.match_id
            WHERE LOWER(TRIM(pr.username)) = LOWER(TRIM(?))
              AND LOWER(TRIM(COALESCE(lc.map_name, 'Unknown'))) = LOWER(TRIM(?))
              AND LOWER(TRIM(COALESCE(pr.side, 'unknown'))) IN (?, ?)
              AND COALESCE(NULLIF(TRIM(pr.match_type_key), ''), ?) = ?
            """,
            (clean_username, clean_map, side_norm, "atk" if side_norm == "attacker" else "def", queue_key, queue_key),
        )
        base = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(pr.operator_key), ''), 'unknown') AS operator_key,
                MAX(COALESCE(NULLIF(TRIM(pr.operator), ''), 'UNKNOWN')) AS operator,
                COUNT(*) AS n_rounds,
                AVG(CASE WHEN LOWER(TRIM(COALESCE(pr.result, ''))) IN ('win', 'victory') THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM player_rounds pr
            LEFT JOIN (
                SELECT smc.match_id, COALESCE(NULLIF(TRIM(smc.map_name), ''), 'Unknown') AS map_name
                FROM scraped_match_cards smc
                JOIN (
                    SELECT match_id, MAX(id) AS max_id
                    FROM scraped_match_cards
                    GROUP BY match_id
                ) last ON last.match_id = smc.match_id AND last.max_id = smc.id
            ) lc ON lc.match_id = pr.match_id
            WHERE LOWER(TRIM(pr.username)) = LOWER(TRIM(?))
              AND LOWER(TRIM(COALESCE(lc.map_name, 'Unknown'))) = LOWER(TRIM(?))
              AND LOWER(TRIM(COALESCE(pr.side, 'unknown'))) IN (?, ?)
              AND COALESCE(NULLIF(TRIM(pr.match_type_key), ''), ?) = ?
            GROUP BY operator_key
            ORDER BY n_rounds DESC, operator ASC
            LIMIT 5
            """,
            (clean_username, clean_map, side_norm, "atk" if side_norm == "attacker" else "def", queue_key, queue_key),
        )
        top_ops = [dict(r) for r in cursor.fetchall()]

        cursor.execute(
            """
            SELECT pr.match_id, pr.round_id, pr.operator, pr.result, pr.kills, pr.deaths, pr.assists
            FROM player_rounds pr
            LEFT JOIN (
                SELECT smc.match_id, COALESCE(NULLIF(TRIM(smc.map_name), ''), 'Unknown') AS map_name
                FROM scraped_match_cards smc
                JOIN (
                    SELECT match_id, MAX(id) AS max_id
                    FROM scraped_match_cards
                    GROUP BY match_id
                ) last ON last.match_id = smc.match_id AND last.max_id = smc.id
            ) lc ON lc.match_id = pr.match_id
            WHERE LOWER(TRIM(pr.username)) = LOWER(TRIM(?))
              AND LOWER(TRIM(COALESCE(lc.map_name, 'Unknown'))) = LOWER(TRIM(?))
              AND LOWER(TRIM(COALESCE(pr.side, 'unknown'))) IN (?, ?)
              AND COALESCE(NULLIF(TRIM(pr.match_type_key), ''), ?) = ?
            ORDER BY pr.match_id DESC, pr.round_id DESC, pr.id DESC
            LIMIT 10
            """,
            (clean_username, clean_map, side_norm, "atk" if side_norm == "attacker" else "def", queue_key, queue_key),
        )
        rows = [dict(r) for r in cursor.fetchall()]

        return {
            "username": clean_username,
            "map_name": clean_map,
            "side": side_norm,
            "queue_key": queue_key,
            "baseline_n": int(base["baseline_n"] or 0) if base else 0,
            "baseline_wr": float(base["baseline_wr"] or 0.0) * 100.0 if base else 0.0,
            "top_operators_by_n": [
                {
                    "operator": str(r.get("operator") or "UNKNOWN"),
                    "n_rounds": int(r.get("n_rounds") or 0),
                    "win_rate": float(r.get("win_rate") or 0.0) * 100.0,
                }
                for r in top_ops
            ],
            "sample_rows": rows,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build operator diagnostics: {str(e)}")


@app.post("/api/settings/db-standardize")
async def settings_db_standardize(dry_run: bool = True, verbose: bool = False) -> dict:
    try:
        def _run() -> dict:
            standardizer = DatabaseStandardizer(db.db_path, dry_run=dry_run, verbose=verbose)
            report = standardizer.run()
            return {
                "db_path": report.db_path,
                "run_at": report.run_at,
                "total_matches": report.total_matches,
                "total_player_rounds": report.total_player_rounds,
                "total_round_outcomes": report.total_round_outcomes,
                "null_usernames_found": report.null_usernames_found,
                "null_usernames_fixed": report.null_usernames_fixed,
                "bad_match_types_found": report.bad_match_types_found,
                "bad_match_types_fixed": report.bad_match_types_fixed,
                "summary_kills_missing": report.summary_kills_missing,
                "summary_kills_reconstructed": report.summary_kills_reconstructed,
                "owingest_stats_missing": report.owingest_stats_missing,
                "owingest_stats_fixed": report.owingest_stats_fixed,
                "killed_by_op_missing": report.killed_by_op_missing,
                "killed_by_op_fixed": report.killed_by_op_fixed,
                "data_quality_flags": report.data_quality_flags,
            }

        report_payload = await asyncio.to_thread(_run)
        return {"ok": True, "dry_run": dry_run, "report": report_payload}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to standardize DB: {str(e)}")

def _extract_match_times(card_row: dict) -> tuple[datetime | None, datetime | None]:
    summary = card_row.get("summary_json")
    payload = {}
    if isinstance(summary, str) and summary.strip():
        try:
            payload = json.loads(summary)
        except Exception:
            payload = {}
    elif isinstance(summary, dict):
        payload = summary
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    md = data.get("metadata", {}) if isinstance(data.get("metadata"), dict) else {}
    end_keys = (
        md.get("matchEndTime"),
        md.get("endTime"),
        md.get("timestamp"),
        payload.get("matchEndTime") if isinstance(payload, dict) else None,
        payload.get("endTime") if isinstance(payload, dict) else None,
        card_row.get("match_date"),
    )
    start_keys = (
        md.get("matchStartTime"),
        md.get("startTime"),
        payload.get("matchStartTime") if isinstance(payload, dict) else None,
        payload.get("startTime") if isinstance(payload, dict) else None,
    )
    end_dt = next((v for v in (_parse_iso_datetime(x) for x in end_keys) if v is not None), None)
    start_dt = next((v for v in (_parse_iso_datetime(x) for x in start_keys) if v is not None), None)
    return end_dt, start_dt

def _encode_evidence_cursor(ordering_mode: str, primary_order: float, match_id: str, round_id: int, pr_id: int) -> str:
    payload = {
        "v": 1,
        "ordering_mode": ordering_mode,
        "primary": float(primary_order),
        "match_id": str(match_id),
        "round_id": int(round_id),
        "pr_id": int(pr_id),
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8")


def _decode_evidence_cursor(value: str) -> dict | None:
    if not value:
        return None
    try:
        raw = base64.urlsafe_b64decode(value.encode("utf-8"))
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            return None
        return payload
    except Exception:
        return None


def _db_revision_token() -> str:
    cur = _get_db_cursor()
    cur.execute("SELECT MAX(scraped_at) AS mx FROM scraped_match_cards")
    row = cur.fetchone()
    return str(row["mx"] if row and row["mx"] is not None else "")


def _workspace_cache_get(key: str) -> dict | None:
    item = workspace_cache.get(key)
    if not item:
        return None
    ts, payload = item
    if time.time() - ts > WORKSPACE_CACHE_TTL_SECONDS:
        workspace_cache.pop(key, None)
        return None
    return payload


def _workspace_cache_set(key: str, payload: dict) -> None:
    workspace_cache[key] = (time.time(), payload)


def _load_workspace_rows(
    username: str,
    *,
    days: int = 90,
    queue: str = "all",
    playlist: str = "",
    map_name: str = "",
    stack_only: bool = False,
    stack_id: int | None = None,
    search: str = "",
    legacy_mode: str = "",
    columns_profile: str = "full",
) -> tuple[int, list[dict], dict, list[str]]:
    scope = _build_workspace_scope(
        username=username,
        days=days,
        queue=queue,
        playlist=playlist,
        map_name=map_name,
        stack_only=stack_only,
        stack_id=stack_id,
        search=search,
        legacy_mode=legacy_mode,
    )
    player_id = int(scope.get("player_id") or 0)
    warnings = list(scope.get("warnings") or [])
    match_ids = [str(mid or "").strip() for mid in scope.get("match_ids") or [] if str(mid or "").strip()]
    ctx = {
        "ordering_mode": "ingestion_fallback",
        "stack_context": scope.get("stack_context", {}),
        "scope_key": scope.get("scope_key"),
        "scope_cache_hit": bool(scope.get("cache_hit", False)),
        "scope_build_ms": int(scope.get("compute_ms", 0)),
        "scope_match_ids": len(match_ids),
    }
    if player_id <= 0:
        return 0, [], ctx, warnings
    if not match_ids:
        return player_id, [], ctx, warnings

    t0 = time.time()
    cur = _get_db_cursor()
    profile = str(columns_profile or "full").strip().lower()
    if profile in {"operators", "matchups"}:
        selected_cols = """
            pr.id AS pr_id,
            pr.player_id,
            pr.match_id,
            pr.round_id,
            pr.side,
            pr.operator,
            pr.operator_key,
            pr.username,
            pr.player_id_tracker,
            pr.kills,
            pr.deaths,
            pr.assists,
            pr.headshots,
            pr.first_blood,
            pr.first_death,
            pr.clutch_won,
            pr.clutch_lost,
            pr.match_type,
            pr.match_type_key,
            ro.winner_side,
            lc.map_name,
            lc.mode AS card_mode,
            lc.match_date,
            lc.scraped_at
        """
    else:
        selected_cols = """
            pr.id AS pr_id,
            pr.player_id,
            pr.match_id,
            pr.round_id,
            pr.side,
            pr.operator,
            pr.operator_key,
            pr.username,
            pr.player_id_tracker,
            pr.kills,
            pr.deaths,
            pr.assists,
            pr.headshots,
            pr.first_blood,
            pr.first_death,
            pr.clutch_won,
            pr.clutch_lost,
            pr.match_type,
            pr.match_type_key,
            ro.winner_side,
            lc.map_name,
            lc.mode AS card_mode,
            lc.match_date,
            lc.summary_json,
            lc.scraped_at
        """
    sql_template = """
        WITH latest_card AS (
            SELECT match_id, MAX(scraped_at) AS scraped_at
            FROM scraped_match_cards
            GROUP BY match_id
        ),
        latest_rows AS (
            SELECT
                smc.match_id,
                smc.map_name,
                smc.mode,
                smc.match_date,
                smc.summary_json,
                lc.scraped_at
            FROM latest_card lc
            JOIN scraped_match_cards smc
              ON smc.match_id = lc.match_id
             AND smc.scraped_at = lc.scraped_at
        )
        SELECT
            {selected_cols}
        FROM player_rounds pr
        JOIN latest_rows lc
          ON lc.match_id = pr.match_id
        JOIN round_outcomes ro
          ON ro.player_id = pr.player_id
         AND ro.match_id = pr.match_id
         AND ro.round_id = pr.round_id
        WHERE pr.player_id = ?
          AND pr.match_id IN ({match_id_placeholders})
          AND pr.operator IS NOT NULL
          AND TRIM(pr.operator) != ''
    """
    filtered: list[dict] = []
    for chunk in _iter_chunks(match_ids):
        placeholders = ",".join("?" for _ in chunk)
        sql = sql_template.format(selected_cols=selected_cols, match_id_placeholders=placeholders)
        params: list[object] = [player_id, *chunk]
        cur.execute(sql, tuple(params))
        filtered.extend(dict(r) for r in cur.fetchall())
    search_key = str(search or "").strip().lower()
    if search_key:
        filtered = [
            r for r in filtered
            if search_key in str(r.get("operator") or "").lower() or search_key in str(r.get("username") or "").lower()
        ]
    for r in filtered:
        r["_order_primary"] = _parse_iso_datetime(r.get("scraped_at")).timestamp() if _parse_iso_datetime(r.get("scraped_at")) else 0.0
    filtered.sort(
        key=lambda r: (
            float(r.get("_order_primary", 0.0)),
            str(r.get("match_id") or ""),
            int(r.get("round_id") or 0),
            int(r.get("pr_id") or 0),
        ),
        reverse=True,
    )
    ctx["row_load_ms"] = int((time.time() - t0) * 1000)
    print(
        "[WORKSPACE] row-load "
        f"profile={profile} scope_key={ctx.get('scope_key')} "
        f"match_ids={len(match_ids)} rows={len(filtered)} "
        f"scope_ms={ctx.get('scope_build_ms')} row_ms={ctx.get('row_load_ms')}"
    )
    return player_id, filtered, ctx, warnings


def _parse_workspace_scope_params(
    *,
    days: int = 90,
    queue: str = "all",
    playlist: str = "",
    map_name: str = "",
    stack_only: bool = False,
    stack_id: int | None = None,
    search: str = "",
    legacy_mode: str = "",
) -> tuple[dict, list[str]]:
    warnings: list[str] = []
    safe_days = max(1, min(int(days), 3650))
    queue_key = str(queue or "").strip().lower()
    if legacy_mode and queue_key in {"", "all"}:
        queue_key = str(legacy_mode).strip().lower()
    if legacy_mode:
        warnings.append("Legacy 'mode' parameter is deprecated; use 'queue' instead.")
    if queue_key not in {"all", "ranked", "unranked"}:
        queue_key = "all"
    playlist_key = str(playlist or "").strip().lower()
    if playlist_key in {"", "all"}:
        playlist_key = ""
    if playlist_key not in {"", "standard", "quick", "event", "arcade"}:
        playlist_key = ""
    selected_map = str(map_name or "").strip().lower()
    return (
        {
            "days": safe_days,
            "queue": queue_key,
            "playlist": playlist_key,
            "map_name": selected_map,
            "stack_only": bool(stack_only),
            "stack_id": stack_id,
            "search": str(search or "").strip().lower(),
        },
        warnings,
    )


def _build_workspace_scope(
    *,
    username: str,
    days: int = 90,
    queue: str = "all",
    playlist: str = "",
    map_name: str = "",
    stack_only: bool = False,
    stack_id: int | None = None,
    search: str = "",
    legacy_mode: str = "",
) -> dict:
    t0 = time.time()
    cur = _get_db_cursor()
    cur.execute(
        "SELECT player_id FROM players WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) ORDER BY player_id DESC LIMIT 1",
        (username,),
    )
    row = cur.fetchone()
    if not row:
        return {
            "player_id": 0,
            "match_ids": [],
            "warnings": ["Player not found."],
            "stack_context": {"enabled": bool(stack_only), "applied": False, "stack_id": stack_id, "matched_teammates": [], "reason": ""},
            "cache_hit": False,
            "scope_key": "",
            "filters_applied": {},
            "compute_ms": int((time.time() - t0) * 1000),
        }
    player_id = int(row["player_id"])
    parsed, warnings = _parse_workspace_scope_params(
        days=days,
        queue=queue,
        playlist=playlist,
        map_name=map_name,
        stack_only=stack_only,
        stack_id=stack_id,
        search=search,
        legacy_mode=legacy_mode,
    )
    safe_days = int(parsed["days"])
    queue_key = str(parsed["queue"])
    playlist_key = str(parsed["playlist"])
    selected_map = str(parsed["map_name"])
    stack_only = bool(parsed["stack_only"])
    scope_payload = {
        "username": str(username or "").strip().lower(),
        "player_id": player_id,
        "days": safe_days,
        "queue": queue_key,
        "playlist": playlist_key,
        "map_name": selected_map,
        "stack_only": stack_only,
        "stack_id": stack_id,
    }
    db_rev = _db_revision_token()
    scope_key = _hash_payload(scope_payload | {"db_rev": db_rev})
    cached = _workspace_scope_cache_get(scope_key, db_rev)
    if cached is not None:
        cached_out = dict(cached)
        cached_out["cache_hit"] = True
        cached_out["compute_ms"] = int((time.time() - t0) * 1000)
        print(
            "[WORKSPACE] scope-build "
            f"scope_key={scope_key} cache_hit=True player={username} "
            f"match_ids={len(cached_out.get('match_ids') or [])} ms={cached_out['compute_ms']}"
        )
        return cached_out

    sql = """
        WITH latest_card AS (
            SELECT match_id, MAX(scraped_at) AS scraped_at
            FROM scraped_match_cards
            GROUP BY match_id
        )
        SELECT DISTINCT
            me.match_id,
            me.team_id,
            COALESCE(NULLIF(TRIM(smc.mode_key), ''), 'other') AS mode_key,
            CASE
                WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%unranked%' THEN 'unranked'
                WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%ranked%' THEN 'ranked'
                WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%standard%' THEN 'standard'
                WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%quick%' OR LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%casual%' THEN 'quick'
                WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%arcade%' THEN 'arcade'
                WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%event%' THEN 'event'
                ELSE 'other'
            END AS mode_norm,
            LOWER(TRIM(COALESCE(smc.map_name, ''))) AS map_name_norm,
            lc.scraped_at AS last_scraped_at
        FROM match_detail_players me
        JOIN latest_card lc
          ON lc.match_id = me.match_id
        JOIN scraped_match_cards smc
          ON smc.match_id = lc.match_id
         AND smc.scraped_at = lc.scraped_at
        WHERE LOWER(TRIM(me.username)) = LOWER(TRIM(?))
          AND DATETIME(COALESCE(lc.scraped_at, '1970-01-01 00:00:00')) >= DATETIME('now', ?)
    """
    params: list[object] = [username, f"-{safe_days} days"]
    if queue_key == "ranked":
        sql += " AND LOWER(TRIM(COALESCE(smc.mode_key, ''))) = 'ranked'"
    elif queue_key == "unranked":
        sql += " AND LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%unranked%'"
    if playlist_key:
        sql += " AND (CASE WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%unranked%' THEN 'unranked' WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%ranked%' THEN 'ranked' WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%standard%' THEN 'standard' WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%quick%' OR LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%casual%' THEN 'quick' WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%arcade%' THEN 'arcade' WHEN LOWER(TRIM(COALESCE(smc.mode, ''))) LIKE '%event%' THEN 'event' ELSE 'other' END) = ?"
        params.append(playlist_key)
    if selected_map:
        sql += " AND LOWER(TRIM(COALESCE(smc.map_name, ''))) = ?"
        params.append(selected_map)
    cur.execute(sql, tuple(params))
    match_rows = [dict(r) for r in cur.fetchall()]

    stack_context = {"enabled": stack_only, "applied": False, "stack_id": stack_id, "matched_teammates": [], "reason": ""}
    if stack_only and match_rows:
        teammates: set[str] = set()
        chosen_stack_id = stack_id
        if chosen_stack_id is None:
            cur.execute(
                """
                SELECT s.stack_id
                FROM stacks s
                JOIN stack_members sm ON sm.stack_id = s.stack_id
                JOIN players p ON p.player_id = sm.player_id
                WHERE LOWER(TRIM(p.username)) = LOWER(TRIM(?))
                ORDER BY s.stack_id ASC
                LIMIT 1
                """,
                (username,),
            )
            sid = cur.fetchone()
            chosen_stack_id = int(sid["stack_id"]) if sid else None
        if chosen_stack_id is not None:
            cur.execute(
                """
                SELECT p.username
                FROM stack_members sm
                JOIN players p ON p.player_id = sm.player_id
                WHERE sm.stack_id = ?
                """,
                (chosen_stack_id,),
            )
            teammates = {
                str(x["username"]).strip().lower()
                for x in cur.fetchall()
                if str(x["username"]).strip().lower() != username.strip().lower()
            }
            stack_context["stack_id"] = chosen_stack_id
        if not teammates:
            stack_context["reason"] = "No stack teammates available; stack filter not applied."
        else:
            match_ids = [str(r.get("match_id") or "").strip() for r in match_rows if str(r.get("match_id") or "").strip()]
            by_match_users: dict[str, set[str]] = {}
            if match_ids:
                for chunk in _iter_chunks(match_ids):
                    placeholders = ",".join("?" for _ in chunk)
                    cur.execute(
                        f"""
                        SELECT tm.match_id, LOWER(TRIM(tm.username)) AS teammate_name
                        FROM match_detail_players me
                        JOIN match_detail_players tm
                          ON tm.match_id = me.match_id
                         AND tm.team_id = me.team_id
                         AND LOWER(TRIM(tm.username)) != LOWER(TRIM(me.username))
                        WHERE LOWER(TRIM(me.username)) = LOWER(TRIM(?))
                          AND me.match_id IN ({placeholders})
                        """,
                        (username, *chunk),
                    )
                    for rr in cur.fetchall():
                        mid = str(rr["match_id"] or "").strip()
                        nm = str(rr["teammate_name"] or "").strip().lower()
                        if mid and nm:
                            by_match_users.setdefault(mid, set()).add(nm)
            allowed = {mid for mid, names in by_match_users.items() if names.intersection(teammates)}
            matched = sorted({n for mid, names in by_match_users.items() if mid in allowed for n in names.intersection(teammates)})
            if allowed:
                match_rows = [r for r in match_rows if str(r.get("match_id") or "") in allowed]
                stack_context["applied"] = True
                stack_context["matched_teammates"] = matched
            else:
                stack_context["reason"] = "No matches contained configured stack teammates."
    match_ids = sorted({str(r.get("match_id") or "").strip() for r in match_rows if str(r.get("match_id") or "").strip()})
    scope_result = {
        "player_id": player_id,
        "match_ids": match_ids,
        "filters_applied": parsed,
        "stack_context": stack_context,
        "scope_key": scope_key,
        "warnings": warnings,
        "cache_hit": False,
        "time_min": f"-{safe_days} days",
        "time_max": "now",
        "compute_ms": int((time.time() - t0) * 1000),
    }
    _workspace_scope_cache_set(scope_key, scope_result, db_rev)
    print(
        "[WORKSPACE] scope-build "
        f"scope_key={scope_key} cache_hit=False player={username} "
        f"match_ids={len(match_ids)} days={safe_days} queue={queue_key} playlist={playlist_key or 'all'} "
        f"map={selected_map or 'all'} stack_only={stack_only} ms={scope_result['compute_ms']}"
    )
    return scope_result


def _compute_matchup_block(
    rows: list[dict],
    *,
    normalization: str = "global",
    lift_mode: str = "percent_delta",
    interval_method: str = "wilson",
    min_n: int = 0,
    weighting: str = "rounds",
) -> dict:
    min_n_safe = max(0, min(int(min_n), 5000))
    norm_key = str(normalization or "global").strip().lower()
    if norm_key not in {"global", "attacker"}:
        norm_key = "global"
    lift_key = str(lift_mode or "percent_delta").strip().lower()
    if lift_key not in {"percent_delta", "logit_lift", "log_odds_ratio"}:
        lift_key = "percent_delta"
    interval_key = str(interval_method or "wilson").strip().lower()
    if interval_key not in {"wilson", "wald"}:
        interval_key = "wilson"
    weight_key = "matches" if str(weighting or "").strip().lower() == "matches" else "rounds"

    rounds: dict[tuple[str, int], dict] = {}
    for r in rows:
        key = (str(r["match_id"]), int(r["round_id"]))
        b = rounds.setdefault(key, {"winner_side": str(r.get("winner_side") or "").lower(), "atk_ops": set(), "def_ops": set()})
        op = str(r.get("operator") or "").strip()
        if _is_unknown_operator_name(op):
            continue
        side = str(r.get("side") or "").strip().lower()
        if side == "attacker":
            b["atk_ops"].add(op)
        elif side == "defender":
            b["def_ops"].add(op)

    valid_rounds = [v for v in rounds.values() if v["atk_ops"] and v["def_ops"] and v["winner_side"] in {"attacker", "defender"}]
    if not valid_rounds:
        return {"error": "No valid rounds for matchup analysis.", "cells": [], "attackers": [], "defenders": []}

    if weight_key == "matches":
        by_match: dict[str, dict] = {}
        for (mid, _rid), v in rounds.items():
            if not v["atk_ops"] or not v["def_ops"]:
                continue
            m = by_match.setdefault(mid, {"atk_wins": 0, "def_wins": 0, "pairs": set(), "atk_ops": set(), "def_ops": set()})
            if v["winner_side"] == "attacker":
                m["atk_wins"] += 1
            elif v["winner_side"] == "defender":
                m["def_wins"] += 1
            m["atk_ops"].update(v["atk_ops"])
            m["def_ops"].update(v["def_ops"])
            for a in v["atk_ops"]:
                for d in v["def_ops"]:
                    m["pairs"].add((a, d))
        match_units = []
        for m in by_match.values():
            if m["atk_wins"] == m["def_wins"]:
                continue
            winner = "attacker" if m["atk_wins"] > m["def_wins"] else "defender"
            match_units.append({"winner": winner, "atk_ops": sorted(m["atk_ops"]), "def_ops": sorted(m["def_ops"]), "pairs": m["pairs"]})
        units = match_units
    else:
        units = [{"winner": v["winner_side"], "atk_ops": sorted(v["atk_ops"]), "def_ops": sorted(v["def_ops"])} for v in valid_rounds]

    total_units = len(units)
    atk_unit_wins = sum(1 for u in units if u["winner"] == "attacker")
    global_baseline = (atk_unit_wins / total_units) * 100.0 if total_units else 0.0
    pair_stats: dict[tuple[str, str], dict[str, int]] = {}
    atk_counts: dict[str, int] = {}
    def_counts: dict[str, int] = {}
    atk_wins_by_op: dict[str, int] = {}
    for u in units:
        atk_win = 1 if u["winner"] == "attacker" else 0
        atk_ops = u["atk_ops"]
        def_ops = u["def_ops"]
        for a in atk_ops:
            atk_counts[a] = atk_counts.get(a, 0) + 1
            atk_wins_by_op[a] = atk_wins_by_op.get(a, 0) + atk_win
        for d in def_ops:
            def_counts[d] = def_counts.get(d, 0) + 1
        pairs = u.get("pairs")
        if pairs is None:
            for a in atk_ops:
                for d in def_ops:
                    p = (a, d)
                    cell = pair_stats.setdefault(p, {"n": 0, "atk_wins": 0})
                    cell["n"] += 1
                    cell["atk_wins"] += atk_win
        else:
            for p in pairs:
                cell = pair_stats.setdefault(p, {"n": 0, "atk_wins": 0})
                cell["n"] += 1
                cell["atk_wins"] += atk_win

    attackers = sorted(atk_counts.keys(), key=lambda x: (-atk_counts.get(x, 0), x))
    defenders = sorted(def_counts.keys(), key=lambda x: (-def_counts.get(x, 0), x))
    cells = []
    for a in attackers:
        for d in defenders:
            s = pair_stats.get((a, d))
            if not s:
                continue
            n = int(s["n"])
            wins = int(s["atk_wins"])
            win_pct = (wins / n) * 100.0 if n > 0 else 0.0
            row_baseline = (atk_wins_by_op.get(a, 0) / atk_counts.get(a, 1)) * 100.0 if atk_counts.get(a, 0) else global_baseline
            baseline_used = row_baseline if norm_key == "attacker" else global_baseline
            p = wins / n if n > 0 else 0.0
            if interval_key == "wilson":
                lo_p, hi_p = _wilson_ci(wins, n)
            else:
                z = 1.96
                se = math.sqrt((p * (1.0 - p)) / n) if n > 0 else 0.0
                lo_p, hi_p = max(0.0, p - z * se), min(1.0, p + z * se)
            eps = 1e-6
            baseline_prob = max(eps, min(1.0 - eps, baseline_used / 100.0))
            lo_prob = max(eps, min(1.0 - eps, lo_p))
            hi_prob = max(eps, min(1.0 - eps, hi_p))
            if lift_key == "percent_delta":
                metric = win_pct - baseline_used
                ci_low, ci_high = (lo_p * 100.0) - baseline_used, (hi_p * 100.0) - baseline_used
            elif lift_key == "logit_lift":
                p_smooth = (wins + 0.5) / (n + 1.0)
                p_smooth = max(eps, min(1.0 - eps, p_smooth))
                base_logit = math.log(baseline_prob / (1.0 - baseline_prob))
                metric = math.log(p_smooth / (1.0 - p_smooth)) - base_logit
                ci_low = math.log(lo_prob / (1.0 - lo_prob)) - base_logit
                ci_high = math.log(hi_prob / (1.0 - hi_prob)) - base_logit
            else:
                a_count = float(wins)
                b_count = float(max(0, n - wins))
                if norm_key == "attacker":
                    row_total = float(atk_counts.get(a, 0))
                    row_wins = float(atk_wins_by_op.get(a, 0))
                    c_count = float(max(0.0, row_wins - a_count))
                    d_count = float(max(0.0, (row_total - row_wins) - b_count))
                else:
                    total_wins = float(atk_unit_wins)
                    total_losses = float(max(0, total_units - atk_unit_wins))
                    c_count = float(max(0.0, total_wins - a_count))
                    d_count = float(max(0.0, total_losses - b_count))
                if min(a_count, b_count, c_count, d_count) <= 0:
                    a_count += 0.5
                    b_count += 0.5
                    c_count += 0.5
                    d_count += 0.5
                metric = math.log((a_count * d_count) / (b_count * c_count))
                se = math.sqrt((1.0 / a_count) + (1.0 / b_count) + (1.0 / c_count) + (1.0 / d_count))
                ci_low, ci_high = metric - (1.96 * se), metric + (1.96 * se)
            cells.append(
                {
                    "attacker": a,
                    "defender": d,
                    "n_rounds": n,
                    "atk_wins": wins,
                    "win_pct": round(win_pct, 3),
                    "baseline_wr": round(baseline_used, 3),
                    "win_ci_low": round(lo_p * 100.0, 3),
                    "win_ci_high": round(hi_p * 100.0, 3),
                    "lift": round(metric, 4 if lift_key != "percent_delta" else 3),
                    "ci_low": round(ci_low, 4 if lift_key != "percent_delta" else 3),
                    "ci_high": round(ci_high, 4 if lift_key != "percent_delta" else 3),
                }
            )

    by_def, by_atk = {}, {}
    for c in cells:
        d, a = str(c["defender"]), str(c["attacker"])
        n = int(c["n_rounds"])
        lift = float(c["lift"])
        def_rec = by_def.setdefault(d, {"neg_sum_raw": 0.0, "w_raw": 0, "neg_sum_vis": 0.0, "w_vis": 0, "cells_vis": 0})
        atk_rec = by_atk.setdefault(a, {"neg_sum_raw": 0.0, "w_raw": 0, "neg_sum_vis": 0.0, "w_vis": 0, "cells_vis": 0})
        penalty = max(0.0, -lift)
        def_rec["neg_sum_raw"] += penalty * n
        def_rec["w_raw"] += n
        atk_rec["neg_sum_raw"] += penalty * n
        atk_rec["w_raw"] += n
        if n >= min_n_safe:
            def_rec["neg_sum_vis"] += penalty * n
            def_rec["w_vis"] += n
            def_rec["cells_vis"] += 1
            atk_rec["neg_sum_vis"] += penalty * n
            atk_rec["w_vis"] += n
            atk_rec["cells_vis"] += 1
    total_side_units = max(1, total_units)
    defender_threat = []
    for d in defenders:
        rec = by_def.get(d, {})
        has_visible = int(rec.get("cells_vis", 0)) > 0
        covered_visible = int(def_counts.get(d, 0)) if has_visible else 0
        defender_threat.append(
            {
                "operator": d,
                "index": round((float(rec.get("neg_sum_vis", 0.0)) / float(rec.get("w_vis", 1))) if rec.get("w_vis", 0) else 0.0, 4),
                "n_rounds_total": total_side_units,
                "n_rounds_covered_raw": int(def_counts.get(d, 0)),
                "n_rounds_covered_visible": covered_visible,
                "n_cells_visible": int(rec.get("cells_vis", 0)),
                "coverage_pct_visible": round((covered_visible / total_side_units) * 100.0, 3),
            }
        )
    attacker_vulnerability = []
    for a in attackers:
        rec = by_atk.get(a, {})
        has_visible = int(rec.get("cells_vis", 0)) > 0
        covered_visible = int(atk_counts.get(a, 0)) if has_visible else 0
        attacker_vulnerability.append(
            {
                "operator": a,
                "index": round((float(rec.get("neg_sum_vis", 0.0)) / float(rec.get("w_vis", 1))) if rec.get("w_vis", 0) else 0.0, 4),
                "n_rounds_total": total_side_units,
                "n_rounds_covered_raw": int(atk_counts.get(a, 0)),
                "n_rounds_covered_visible": covered_visible,
                "n_cells_visible": int(rec.get("cells_vis", 0)),
                "coverage_pct_visible": round((covered_visible / total_side_units) * 100.0, 3),
            }
        )

    defender_threat.sort(key=lambda x: (-float(x["index"]), -int(x["n_rounds_covered_visible"]), x["operator"]))
    attacker_vulnerability.sort(key=lambda x: (-float(x["index"]), -int(x["n_rounds_covered_visible"]), x["operator"]))
    return {
        "baseline_atk_win_rate": round(global_baseline, 4),
        "total_rounds": total_units,
        "attackers": attackers,
        "defenders": defenders,
        "cells": cells,
        "normalization": norm_key,
        "lift_mode": lift_key,
        "interval_method": interval_key,
        "weighting": weight_key,
        "filters": {"min_n": min_n_safe},
        "threat_index": {
            "defender_threat": defender_threat,
            "attacker_vulnerability": attacker_vulnerability,
        },
        "clamp_defaults": {"clamp_mode": "percentile", "clamp_p_low": 5, "clamp_p_high": 95, "clamp_abs": 15},
    }


def _compute_operator_scatter(
    rows: list[dict],
    *,
    weighting: str = "rounds",
) -> dict:
    rounds: dict[tuple[str, int], dict] = {}
    for r in rows:
        key = (str(r["match_id"]), int(r["round_id"]))
        b = rounds.setdefault(key, {"winner_side": str(r.get("winner_side") or "").lower(), "atk_ops": set(), "def_ops": set()})
        op = str(r.get("operator") or "").strip()
        if _is_unknown_operator_name(op):
            continue
        side = str(r.get("side") or "").strip().lower()
        if side == "attacker":
            b["atk_ops"].add(op)
        elif side == "defender":
            b["def_ops"].add(op)
    valid_rounds = [(mid, rid, v) for (mid, rid), v in rounds.items() if v["atk_ops"] and v["def_ops"] and v["winner_side"] in {"attacker", "defender"}]
    if not valid_rounds:
        return {"points": [], "baselines": {"attacker": 0.0, "defender": 0.0}, "total_units": 0}
    weight_key = "matches" if str(weighting or "").strip().lower() == "matches" else "rounds"
    if weight_key == "matches":
        by_match: dict[str, dict] = {}
        for mid, _rid, v in valid_rounds:
            m = by_match.setdefault(mid, {"atk_wins": 0, "def_wins": 0, "atk_ops": set(), "def_ops": set()})
            if v["winner_side"] == "attacker":
                m["atk_wins"] += 1
            else:
                m["def_wins"] += 1
            m["atk_ops"].update(v["atk_ops"])
            m["def_ops"].update(v["def_ops"])
        units = []
        for m in by_match.values():
            if m["atk_wins"] == m["def_wins"]:
                continue
            units.append({"winner_side": "attacker" if m["atk_wins"] > m["def_wins"] else "defender", "atk_ops": m["atk_ops"], "def_ops": m["def_ops"]})
        total_matches = len(units)
        if total_matches <= 0:
            return {"points": [], "baselines": {"attacker": 0.0, "defender": 0.0}, "total_units": 0}
        baseline_atk = (sum(1 for u in units if u["winner_side"] == "attacker") / total_matches) * 100.0
        baseline_def = 100.0 - baseline_atk
        points = []
        for side in ("attacker", "defender"):
            side_ops: dict[str, dict[str, int]] = {}
            for u in units:
                ops = u["atk_ops"] if side == "attacker" else u["def_ops"]
                side_win = 1 if u["winner_side"] == side else 0
                for op in ops:
                    rec = side_ops.setdefault(op, {"n": 0, "wins": 0})
                    rec["n"] += 1
                    rec["wins"] += side_win
            for op, rec in side_ops.items():
                n = rec["n"]
                wins = rec["wins"]
                win_pct = (wins / n) * 100.0 if n else 0.0
                baseline = baseline_atk if side == "attacker" else baseline_def
                lo, hi = _wilson_ci(wins, n)
                points.append(
                    {
                        "operator": op,
                        "side": side,
                        "n_rounds": n,
                        "presence_pct": round((n / total_matches) * 100.0, 4),
                        "win_pct": round(win_pct, 4),
                        "baseline_win_pct": round(baseline, 4),
                        "win_delta": round(win_pct - baseline, 4),
                        "ci_low": round((lo * 100.0), 4),
                        "ci_high": round((hi * 100.0), 4),
                    }
                )
        return {"points": points, "baselines": {"attacker": round(baseline_atk, 4), "defender": round(baseline_def, 4)}, "total_units": total_matches}

    total_rounds = len(valid_rounds)
    atk_wins = sum(1 for _m, _r, v in valid_rounds if v["winner_side"] == "attacker")
    baseline_atk = (atk_wins / total_rounds) * 100.0
    baseline_def = 100.0 - baseline_atk
    points = []
    for side in ("attacker", "defender"):
        side_ops: dict[str, dict[str, int]] = {}
        for _mid, _rid, v in valid_rounds:
            ops = v["atk_ops"] if side == "attacker" else v["def_ops"]
            side_win = 1 if v["winner_side"] == side else 0
            for op in ops:
                rec = side_ops.setdefault(op, {"n": 0, "wins": 0})
                rec["n"] += 1
                rec["wins"] += side_win
        for op, rec in side_ops.items():
            n = rec["n"]
            wins = rec["wins"]
            win_pct = (wins / n) * 100.0 if n else 0.0
            baseline = baseline_atk if side == "attacker" else baseline_def
            lo, hi = _wilson_ci(wins, n)
            points.append(
                {
                    "operator": op,
                    "side": side,
                    "n_rounds": n,
                    "presence_pct": round((n / total_rounds) * 100.0, 4),
                    "win_pct": round(win_pct, 4),
                    "baseline_win_pct": round(baseline, 4),
                    "win_delta": round(win_pct - baseline, 4),
                    "ci_low": round((lo * 100.0), 4),
                    "ci_high": round((hi * 100.0), 4),
                }
            )
    return {"points": points, "baselines": {"attacker": round(baseline_atk, 4), "defender": round(baseline_def, 4)}, "total_units": total_rounds}


def _hash_payload(payload: dict) -> str:
    try:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except Exception:
        encoded = str(payload).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _integrity_counters(rows: list[dict]) -> dict:
    rounds: dict[tuple[str, int], dict] = {}
    for r in rows:
        key = (str(r.get("match_id")), int(r.get("round_id") or 0))
        b = rounds.setdefault(key, {"players": 0, "atk": 0, "def": 0, "ops_missing": 0, "winner_side": str(r.get("winner_side") or "").lower()})
        b["players"] += 1
        side = str(r.get("side") or "").lower()
        if side == "attacker":
            b["atk"] += 1
        elif side == "defender":
            b["def"] += 1
        if not str(r.get("operator") or "").strip():
            b["ops_missing"] += 1
    counters = {
        "rounds_total": len(rounds),
        "rounds_missing_players": 0,
        "rounds_not_5v5": 0,
        "rounds_missing_operator_entries": 0,
        "rounds_invalid_winner_side": 0,
    }
    for b in rounds.values():
        if b["players"] < 10:
            counters["rounds_missing_players"] += 1
        if b["atk"] != 5 or b["def"] != 5:
            counters["rounds_not_5v5"] += 1
        if b["ops_missing"] > 0:
            counters["rounds_missing_operator_entries"] += 1
        if b["winner_side"] not in {"attacker", "defender"}:
            counters["rounds_invalid_winner_side"] += 1
    return counters


def _compute_workspace_team_pairs(username: str, scope: dict) -> dict:
    t0 = time.time()
    player_id = int(scope.get("player_id") or 0)
    match_ids = [str(mid or "").strip() for mid in scope.get("match_ids") or [] if str(mid or "").strip()]
    if player_id <= 0:
        return {
            "pairs": [],
            "baseline_win_rate": 0.0,
            "matches_in_scope": 0,
            "is_partial": False,
            "reason": "Player not found.",
            "compute_ms": int((time.time() - t0) * 1000),
        }
    if not match_ids:
        return {
            "pairs": [],
            "baseline_win_rate": 0.0,
            "matches_in_scope": 0,
            "is_partial": False,
            "reason": "No matches in scope.",
            "compute_ms": int((time.time() - t0) * 1000),
        }

    limited_match_ids = match_ids
    is_partial = False
    partial_reason = ""
    TEAM_SCOPE_HARD_CAP = 5000
    if len(limited_match_ids) > TEAM_SCOPE_HARD_CAP:
        limited_match_ids = limited_match_ids[:TEAM_SCOPE_HARD_CAP]
        is_partial = True
        partial_reason = f"Scope reduced to first {TEAM_SCOPE_HARD_CAP} matches for compute safety."

    cur = _get_db_cursor()
    me_rows: list[dict] = []
    for chunk in _iter_chunks(limited_match_ids):
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(
            f"""
            SELECT me.match_id, me.result, me.team_id
            FROM match_detail_players me
            WHERE LOWER(TRIM(me.username)) = LOWER(TRIM(?))
              AND me.match_id IN ({placeholders})
            """,
            (username, *chunk),
        )
        me_rows.extend(dict(r) for r in cur.fetchall())
    if not me_rows:
        return {
            "pairs": [],
            "baseline_win_rate": 0.0,
            "matches_in_scope": len(limited_match_ids),
            "is_partial": is_partial,
            "reason": partial_reason or "No player rows in scope.",
            "compute_ms": int((time.time() - t0) * 1000),
        }
    match_meta = {str(r["match_id"]): {"won": str(r.get("result") or "").lower() == "win", "team_id": r.get("team_id")} for r in me_rows}
    baseline_wr = (sum(1 for r in me_rows if str(r.get("result") or "").lower() == "win") / max(1, len(me_rows))) * 100.0

    by_match_teammates: dict[str, set[str]] = {}
    for chunk in _iter_chunks(limited_match_ids):
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(
            f"""
            SELECT tm.match_id, LOWER(TRIM(tm.username)) AS teammate
            FROM match_detail_players me
            JOIN match_detail_players tm
              ON tm.match_id = me.match_id
             AND tm.team_id = me.team_id
             AND LOWER(TRIM(tm.username)) != LOWER(TRIM(me.username))
            WHERE LOWER(TRIM(me.username)) = LOWER(TRIM(?))
              AND me.match_id IN ({placeholders})
            """,
            (username, *chunk),
        )
        for r in cur.fetchall():
            mid = str(r["match_id"] or "").strip()
            nm = str(r["teammate"] or "").strip().lower()
            if mid and nm:
                by_match_teammates.setdefault(mid, set()).add(nm)

    rounds_by_match: dict[str, int] = {}
    for chunk in _iter_chunks(limited_match_ids):
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(
            f"""
            SELECT match_id, COUNT(DISTINCT round_id) AS rounds_n
            FROM player_rounds
            WHERE player_id = ?
              AND match_id IN ({placeholders})
            GROUP BY match_id
            """,
            (player_id, *chunk),
        )
        for r in cur.fetchall():
            rounds_by_match[str(r["match_id"])] = int(r["rounds_n"] or 0)

    agg: dict[tuple[str, str], dict] = {}
    for mid, teammates in by_match_teammates.items():
        if len(teammates) < 2:
            continue
        won = bool(match_meta.get(mid, {}).get("won", False))
        rounds_n = int(rounds_by_match.get(mid, 0))
        for a, b in combinations(sorted(teammates), 2):
            key = (a, b)
            rec = agg.setdefault(
                key,
                {
                    "pair": f"{a} + {b}",
                    "teammate_a": a,
                    "teammate_b": b,
                    "matches_n": 0,
                    "wins_n": 0,
                    "rounds_n": 0,
                },
            )
            rec["matches_n"] += 1
            rec["wins_n"] += 1 if won else 0
            rec["rounds_n"] += rounds_n

    pairs = []
    for rec in agg.values():
        matches_n = int(rec["matches_n"])
        wins_n = int(rec["wins_n"])
        wr = (wins_n / matches_n) * 100.0 if matches_n > 0 else 0.0
        pairs.append(
            {
                **rec,
                "win_rate": round(wr, 2),
                "delta_vs_user_baseline": round(wr - baseline_wr, 2),
            }
        )
    pairs.sort(key=lambda x: (-int(x["rounds_n"]), -int(x["matches_n"]), x["pair"]))
    return {
        "pairs": pairs,
        "baseline_win_rate": round(baseline_wr, 2),
        "matches_in_scope": len(limited_match_ids),
        "is_partial": is_partial,
        "reason": partial_reason,
        "compute_ms": int((time.time() - t0) * 1000),
    }


@app.get("/api/workspace/team/{username}")
async def workspace_team(
    username: str,
    ws_days: int = 90,
    ws_queue: str = "all",
    ws_playlist: str = "",
    ws_map_name: str = "",
    ws_stack_only: bool = False,
    ws_stack_id: int | None = None,
    ws_search: str = "",
    mode: str = "",
    force_refresh: bool = False,
) -> dict:
    try:
        scope = _build_workspace_scope(
            username=username,
            days=ws_days,
            queue=ws_queue,
            playlist=ws_playlist,
            map_name=ws_map_name,
            stack_only=ws_stack_only,
            stack_id=ws_stack_id,
            search=ws_search,
            legacy_mode=mode,
        )
        db_rev = _db_revision_token()
        scope_key = str(scope.get("scope_key") or "")
        team_key = _hash_payload({"scope_key": scope_key, "view": "pairs_v1", "db_rev": db_rev})
        if not force_refresh:
            cached = _workspace_team_cache_get(team_key, db_rev)
            if cached is not None:
                payload = dict(cached)
                payload["cache_hit"] = True
                return payload
        result = _compute_workspace_team_pairs(username, scope)
        payload = {
            "username": username,
            "scope_key": scope_key,
            "cache_hit": False,
            "compute_ms": int(result.get("compute_ms", 0)),
            "is_partial": bool(result.get("is_partial", False)),
            "reason": str(result.get("reason") or ""),
            "scope": {
                "match_ids": int(result.get("matches_in_scope", 0)),
                "filters_applied": scope.get("filters_applied", {}),
                "stack_context": scope.get("stack_context", {}),
                "scope_cache_hit": bool(scope.get("cache_hit", False)),
                "scope_build_ms": int(scope.get("compute_ms", 0)),
            },
            "baseline_win_rate": float(result.get("baseline_win_rate", 0.0)),
            "pairs": result.get("pairs", []),
            "meta": {
                "api_version": WORKSPACE_API_VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "db_rev": db_rev,
            },
        }
        _workspace_team_cache_set(team_key, payload, db_rev)
        return payload
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute workspace team: {str(e)}")


@app.get("/api/workspace/insights/{username}")
async def workspace_insights(
    username: str,
    ws_days: int = 90,
    ws_queue: str = "all",
    ws_playlist: str = "",
    ws_map_name: str = "",
    ws_map: str = "",
    ws_stack_only: bool = False,
    ws_stack_id: int | None = None,
    ws_search: str = "",
    mode: str = "",
    force_refresh: bool = False,
) -> dict:
    try:
        map_filter = ws_map_name or ws_map
        scope = _build_workspace_scope(
            username=username,
            days=ws_days,
            queue=ws_queue,
            playlist=ws_playlist,
            map_name=map_filter,
            stack_only=ws_stack_only,
            stack_id=ws_stack_id,
            search=ws_search,
            legacy_mode=mode,
        )
        db_rev = _db_revision_token()
        scope_key = str(scope.get("scope_key") or "")
        insights_key = _hash_payload(
            {
                "scope_key": scope_key,
                "view": INSIGHTS_VERSION,
                "db_rev": db_rev,
            }
        )
        if not force_refresh:
            cached = _workspace_insights_cache_get(insights_key, db_rev)
            if cached is not None:
                payload = dict(cached)
                payload["cache_hit"] = True
                return payload

        team_result = _compute_workspace_team_pairs(username, scope)
        cur = _get_db_cursor()
        result = run_insight_engine(
            cur=cur,
            username=username,
            scope=scope,
            team_pairs_overall=team_result.get("pairs", []),
        )
        payload = {
            "username": username,
            "scope_key": scope_key,
            "cache_hit": False,
            "meta": {
                **(result.get("meta") or {}),
                "scope_key": scope_key,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
            "baseline": result.get("baseline", {}),
            "insights": result.get("insights", []),
            "scope": {
                "match_ids": int(len(scope.get("match_ids") or [])),
                "filters_applied": scope.get("filters_applied", {}),
                "stack_context": scope.get("stack_context", {}),
                "scope_cache_hit": bool(scope.get("cache_hit", False)),
                "scope_build_ms": int(scope.get("compute_ms", 0)),
            },
        }
        _workspace_insights_cache_set(insights_key, payload, db_rev)
        return payload
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute workspace insights: {str(e)}")


@app.get("/api/dashboard-workspace/{username}")
async def dashboard_workspace(
    username: str,
    panel: str = "all",
    days: int = 90,
    queue: str = "all",
    playlist: str = "",
    map_name: str = "",
    stack_only: bool = False,
    stack_id: int | None = None,
    search: str = "",
    normalization: str = "global",
    lift_mode: str = "percent_delta",
    interval_method: str = "wilson",
    min_n: int = 0,
    weighting: str = "rounds",
    clamp_mode: str = "percentile",
    clamp_abs: float = 15.0,
    clamp_p_low: float = 5.0,
    clamp_p_high: float = 95.0,
    debug: bool = False,
    mode: str = "",
) -> dict:
    try:
        panel_key = str(panel or "all").strip().lower()
        if panel_key not in {"all", "overview", "operators", "matchups", "team"}:
            panel_key = "all"
        db_rev = _db_revision_token()
        cache_key = _hash_payload(
            {
                "u": username,
                "panel": panel_key,
                "days": days,
                "queue": queue,
                "playlist": playlist,
                "map_name": map_name,
                "stack_only": stack_only,
                "stack_id": stack_id,
                "search": search,
                "normalization": normalization,
                "lift_mode": lift_mode,
                "interval_method": interval_method,
                "min_n": min_n,
                "weighting": weighting,
                "clamp_mode": clamp_mode,
                "clamp_abs": clamp_abs,
                "clamp_p_low": clamp_p_low,
                "clamp_p_high": clamp_p_high,
                "debug": debug,
                "mode": mode,
                "db_rev": db_rev,
            }
        )
        cached = _workspace_cache_get(cache_key)
        if cached is not None:
            return cached

        if panel_key == "team":
            parsed_scope, scope_warnings = _parse_workspace_scope_params(
                days=days,
                queue=queue,
                playlist=playlist,
                map_name=map_name,
                stack_only=stack_only,
                stack_id=stack_id,
                search=search,
                legacy_mode=mode,
            )
            response = {
                "username": username,
                "filters_effective": {
                    "panel": panel_key,
                    "days": parsed_scope["days"],
                    "queue": parsed_scope["queue"],
                    "playlist": parsed_scope["playlist"],
                    "map_name": parsed_scope["map_name"],
                    "stack_only": bool(parsed_scope["stack_only"]),
                    "stack_id": parsed_scope["stack_id"],
                    "search": parsed_scope["search"],
                },
                "meta": {
                    "api_version": WORKSPACE_API_VERSION,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "ordering_mode": "scope_only",
                    "db_rev": db_rev,
                    "warnings": scope_warnings,
                    "panel": panel_key,
                },
                "team": {"message": "Workspace Team now loads from /api/workspace/team/{username}."},
            }
            response["meta"]["hash"] = _hash_payload({"team": response.get("team"), "filters": response.get("filters_effective")})
            _workspace_cache_set(cache_key, response)
            return response

        player_id, rows, ctx, warnings = _load_workspace_rows(
            username,
            days=days,
            queue=queue,
            playlist=playlist,
            map_name=map_name,
            stack_only=stack_only,
            stack_id=stack_id,
            search=search,
            legacy_mode=mode,
            columns_profile=panel_key if panel_key in {"operators", "matchups"} else "full",
        )
        if player_id <= 0:
            return {"username": username, "analysis": {"error": "Player not found."}, "meta": {"api_version": WORKSPACE_API_VERSION}}

        ordering_mode = str(ctx.get("ordering_mode") or "ingestion_fallback")
        stack_context = ctx.get("stack_context", {})
        response: dict = {
            "username": username,
            "filters_effective": {
                "panel": panel_key,
                "days": max(1, min(int(days), 3650)),
                "queue": str(queue or "all").lower(),
                "playlist": str(playlist or "").lower(),
                "map_name": map_name,
                "stack_only": bool(stack_only),
                "stack_id": stack_id,
                "search": search,
                "normalization": normalization,
                "lift_mode": lift_mode,
                "interval_method": interval_method,
                "min_n": max(0, min(int(min_n), 5000)),
                "weighting": "matches" if str(weighting).lower() == "matches" else "rounds",
                "clamp_mode": clamp_mode,
                "clamp_abs": clamp_abs,
                "clamp_p_low": clamp_p_low,
                "clamp_p_high": clamp_p_high,
                "labels_default": "on",
            },
            "meta": {
                "api_version": WORKSPACE_API_VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "ordering_mode": ordering_mode,
                "db_rev": db_rev,
                "warnings": warnings,
                "panel": panel_key,
            },
        }
        if not rows:
            response["analysis"] = {"error": "No rows for current filters."}
            response["meta"]["hash"] = _hash_payload(response.get("analysis", {}))
            _workspace_cache_set(cache_key, response)
            return response

        include_ops = panel_key in {"all", "operators"}
        include_matchups = panel_key in {"all", "matchups"}
        if include_ops:
            op_scatter = _compute_operator_scatter(rows, weighting=weighting)
            response["operators"] = {
                "scatter": op_scatter,
                "stack_context": stack_context,
            }
        if include_matchups:
            matchup = _compute_matchup_block(
                rows,
                normalization=normalization,
                lift_mode=lift_mode,
                interval_method=interval_method,
                min_n=min_n,
                weighting=weighting,
            )
            if clamp_mode == "percentile":
                bound = _pctile_abs_bound([float(c.get("lift", 0.0)) for c in matchup.get("cells", [])], fallback=15.0)
            else:
                bound = abs(float(clamp_abs or 15.0))
            matchup["clamp"] = {
                "mode": "percentile" if clamp_mode == "percentile" else "abs",
                "p_low": float(clamp_p_low),
                "p_high": float(clamp_p_high),
                "abs_bound": round(bound, 4),
            }
            response["matchups"] = matchup
        if panel_key in {"all", "team"}:
            response["team"] = {"message": "Phase 1 team workspace shell ready."}
        if panel_key in {"all", "overview"}:
            side_counts = {"attacker": 0, "defender": 0}
            for r in rows:
                side = str(r.get("side") or "").lower()
                if side in side_counts:
                    side_counts[side] += 1
            response["overview"] = {
                "message": "Phase 1 overview workspace shell ready.",
                "rows_after_filters": len(rows),
                "distinct_matches": len({str(r.get("match_id") or "") for r in rows}),
                "distinct_rounds": len({(str(r.get("match_id") or ""), int(r.get("round_id") or 0)) for r in rows}),
                "side_rows": side_counts,
            }
        if debug:
            response["diagnostics"] = {
                "integrity": _integrity_counters(rows),
                "rows_after_filters": len(rows),
            }
        response["meta"]["hash"] = _hash_payload(
            {
                "operators": response.get("operators"),
                "matchups": response.get("matchups"),
                "team": response.get("team"),
                "overview": response.get("overview"),
                "diagnostics": response.get("diagnostics"),
            }
        )
        _workspace_cache_set(cache_key, response)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute dashboard workspace: {str(e)}")


@app.get("/api/dashboard-workspace/{username}/operator/{operator_name}")
async def dashboard_workspace_operator(
    username: str,
    operator_name: str,
    days: int = 90,
    queue: str = "all",
    playlist: str = "",
    map_name: str = "",
    stack_only: bool = False,
    stack_id: int | None = None,
    search: str = "",
    weighting: str = "rounds",
    side: str = "all",
    recent_n_rounds: int = 300,
    previous_window: bool = True,
    mode: str = "",
) -> dict:
    try:
        _pid, rows, ctx, warnings = _load_workspace_rows(
            username,
            days=days,
            queue=queue,
            playlist=playlist,
            map_name=map_name,
            stack_only=stack_only,
            stack_id=stack_id,
            search=search,
            legacy_mode=mode,
            columns_profile="operators",
        )
        op_name = str(operator_name or "").strip().lower()
        side_key = str(side or "all").strip().lower()
        if side_key not in {"all", "attacker", "defender"}:
            side_key = "all"
        op_rows = [
            r for r in rows
            if str(r.get("operator") or "").strip().lower() == op_name
            and (side_key == "all" or str(r.get("side") or "").strip().lower() == side_key)
        ]
        ordering_mode = str(ctx.get("ordering_mode") or "ingestion_fallback")
        db_rev = _db_revision_token()
        if not op_rows:
            return {
                "username": username,
                "operator": operator_name,
                "summary": {"error": "No rows for selected operator/filters."},
                "meta": {"api_version": WORKSPACE_API_VERSION, "ordering_mode": ordering_mode, "db_rev": db_rev, "warnings": warnings},
            }

        n = len(op_rows)
        wins = sum(1 for r in op_rows if str(r.get("winner_side") or "").lower() == str(r.get("side") or "").lower())
        lo, hi = _wilson_ci(wins, n)
        first_bloods = sum(int(r.get("first_blood") or 0) for r in op_rows)
        first_deaths = sum(int(r.get("first_death") or 0) for r in op_rows)
        survived = sum(1 for r in op_rows if int(r.get("deaths") or 0) == 0)
        clutch_w = sum(int(r.get("clutch_won") or 0) for r in op_rows)
        clutch_l = sum(int(r.get("clutch_lost") or 0) for r in op_rows)
        clutch_attempts = clutch_w + clutch_l
        side_totals = {"attacker": 0, "defender": 0}
        for r in rows:
            rk = str(r.get("side") or "").lower()
            if rk in side_totals:
                side_totals[rk] += 1
        op_sides = {"attacker": 0, "defender": 0}
        for r in op_rows:
            rk = str(r.get("side") or "").lower()
            if rk in op_sides:
                op_sides[rk] += 1
        presence_side = "attacker" if op_sides["attacker"] >= op_sides["defender"] else "defender"
        denom = max(1, side_totals.get(presence_side, n))
        recent_n = max(1, min(int(recent_n_rounds), 5000))
        recent_rows = op_rows[:recent_n]
        prev_rows = op_rows[recent_n: recent_n * 2] if previous_window else []
        def _summary(block_rows: list[dict]) -> dict:
            if not block_rows:
                return {"n": 0, "win_pct": 0.0, "opening_kill_rate": 0.0, "opening_death_rate": 0.0, "survival_rate": 0.0}
            bn = len(block_rows)
            bw = sum(1 for x in block_rows if str(x.get("winner_side") or "").lower() == str(x.get("side") or "").lower())
            return {
                "n": bn,
                "win_pct": round((bw / bn) * 100.0, 4),
                "opening_kill_rate": round((sum(int(x.get("first_blood") or 0) for x in block_rows) / bn) * 100.0, 4),
                "opening_death_rate": round((sum(int(x.get("first_death") or 0) for x in block_rows) / bn) * 100.0, 4),
                "survival_rate": round((sum(1 for x in block_rows if int(x.get("deaths") or 0) == 0) / bn) * 100.0, 4),
            }

        return {
            "username": username,
            "operator": operator_name,
            "summary": {
                "n_rounds": n,
                "wins": wins,
                "win_pct": round((wins / n) * 100.0, 4),
                "win_ci_low": round(lo * 100.0, 4),
                "win_ci_high": round(hi * 100.0, 4),
                "presence_pct": round((op_sides[presence_side] / denom) * 100.0, 4),
                "presence_side": presence_side,
            },
            "impact_metrics": {
                "opening_kill_rate": round((first_bloods / n) * 100.0, 4),
                "opening_death_rate": round((first_deaths / n) * 100.0, 4),
                "survival_rate": round((survived / n) * 100.0, 4),
                "clutch_rate": round((clutch_w / max(1, clutch_attempts)) * 100.0, 4),
                "clutch_attempts": clutch_attempts,
            },
            "splits": {
                "attacker_n": op_sides["attacker"],
                "defender_n": op_sides["defender"],
            },
            "recent_windows": {
                "recent_n_rounds": recent_n,
                "recent": _summary(recent_rows),
                "previous": _summary(prev_rows),
            },
            "meta": {
                "api_version": WORKSPACE_API_VERSION,
                "ordering_mode": ordering_mode,
                "db_rev": db_rev,
                "warnings": warnings,
                "weighting": "matches" if str(weighting).lower() == "matches" else "rounds",
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute operator inspector: {str(e)}")


@app.get("/api/dashboard-workspace/{username}/evidence")
async def dashboard_workspace_evidence(
    username: str,
    days: int = 90,
    queue: str = "all",
    playlist: str = "",
    map_name: str = "",
    stack_only: bool = False,
    stack_id: int | None = None,
    search: str = "",
    selection_version: int = 1,
    selection_type: str = "",
    operator: str = "",
    atk_op: str = "",
    def_op: str = "",
    row_atk_op: str = "",
    col_def_op: str = "",
    evidence_limit: int = 200,
    evidence_cursor: str = "",
    mode: str = "",
) -> dict:
    try:
        _pid, rows, ctx, warnings = _load_workspace_rows(
            username,
            days=days,
            queue=queue,
            playlist=playlist,
            map_name=map_name,
            stack_only=stack_only,
            stack_id=stack_id,
            search=search,
            legacy_mode=mode,
            columns_profile="matchups",
        )
        ordering_mode = str(ctx.get("ordering_mode") or "ingestion_fallback")
        db_rev = _db_revision_token()
        sel_type = str(selection_type or "").strip().lower()
        if sel_type not in {"operator", "matchup_cell", "matchup_row", "matchup_col"}:
            sel_type = ""
        filtered_rows = rows
        round_ops: dict[tuple[str, int], dict] = {}
        if sel_type in {"matchup_cell", "matchup_row", "matchup_col"}:
            for r in rows:
                key = (str(r["match_id"]), int(r["round_id"]))
                b = round_ops.setdefault(key, {"atk": set(), "def": set()})
                side = str(r.get("side") or "").lower()
                op = str(r.get("operator") or "").strip()
                if side == "attacker":
                    b["atk"].add(op.lower())
                elif side == "defender":
                    b["def"].add(op.lower())
        if sel_type == "operator":
            target = str(operator or "").strip().lower()
            filtered_rows = [r for r in rows if str(r.get("operator") or "").strip().lower() == target]
        elif sel_type == "matchup_cell":
            a = str(atk_op or "").strip().lower()
            d = str(def_op or "").strip().lower()
            allowed = {k for k, v in round_ops.items() if a in v["atk"] and d in v["def"]}
            filtered_rows = [r for r in rows if (str(r["match_id"]), int(r["round_id"])) in allowed]
        elif sel_type == "matchup_row":
            a = str(row_atk_op or "").strip().lower()
            allowed = {k for k, v in round_ops.items() if a in v["atk"]}
            filtered_rows = [r for r in rows if (str(r["match_id"]), int(r["round_id"])) in allowed]
        elif sel_type == "matchup_col":
            d = str(col_def_op or "").strip().lower()
            allowed = {k for k, v in round_ops.items() if d in v["def"]}
            filtered_rows = [r for r in rows if (str(r["match_id"]), int(r["round_id"])) in allowed]

        limit = max(1, min(int(evidence_limit), 1000))
        cursor_obj = _decode_evidence_cursor(evidence_cursor) if evidence_cursor else None
        if cursor_obj and str(cursor_obj.get("ordering_mode") or "") != ordering_mode:
            cursor_obj = None
        if cursor_obj and int(cursor_obj.get("v") or 0) != 1:
            cursor_obj = None

        def _row_tuple(r: dict) -> tuple[float, str, int, int]:
            return (
                float(r.get("_order_primary") or 0.0),
                str(r.get("match_id") or ""),
                int(r.get("round_id") or 0),
                int(r.get("pr_id") or 0),
            )

        if cursor_obj:
            cur_tuple = (
                float(cursor_obj.get("primary") or 0.0),
                str(cursor_obj.get("match_id") or ""),
                int(cursor_obj.get("round_id") or 0),
                int(cursor_obj.get("pr_id") or 0),
            )
            filtered_rows = [r for r in filtered_rows if _row_tuple(r) < cur_tuple]

        page = filtered_rows[:limit]
        has_more = len(filtered_rows) > limit
        next_cursor = ""
        if has_more and page:
            last = page[-1]
            next_cursor = _encode_evidence_cursor(
                ordering_mode,
                float(last.get("_order_primary") or 0.0),
                str(last.get("match_id") or ""),
                int(last.get("round_id") or 0),
                int(last.get("pr_id") or 0),
            )
        rows_out = []
        for r in page:
            rows_out.append(
                {
                    "pr_id": int(r.get("pr_id") or 0),
                    "match_id": str(r.get("match_id") or ""),
                    "round_id": int(r.get("round_id") or 0),
                    "map_name": r.get("map_name"),
                    "queue_mode": _normalize_mode_key(r.get("card_mode") or r.get("match_type")),
                    "username": r.get("username"),
                    "side": r.get("side"),
                    "operator": r.get("operator"),
                    "winner_side": r.get("winner_side"),
                    "result": "win" if str(r.get("winner_side") or "").lower() == str(r.get("side") or "").lower() else "loss",
                    "kills": int(r.get("kills") or 0),
                    "deaths": int(r.get("deaths") or 0),
                    "assists": int(r.get("assists") or 0),
                    "order_primary": float(r.get("_order_primary") or 0.0),
                }
            )

        return {
            "username": username,
            "selection": {
                "selection_version": selection_version,
                "selection_type": sel_type or None,
                "operator": operator or None,
                "atk_op": atk_op or None,
                "def_op": def_op or None,
                "row_atk_op": row_atk_op or None,
                "col_def_op": col_def_op or None,
            },
            "rows": rows_out,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "limit": limit,
            "meta": {
                "api_version": WORKSPACE_API_VERSION,
                "ordering_mode": ordering_mode,
                "cursor_version": 1,
                "db_rev": db_rev,
                "warnings": warnings,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute evidence page: {str(e)}")


@app.get("/api/atk-def-heatmap/{username}")
async def atk_def_heatmap(
    username: str,
    mode: str = "ranked",
    map_name: str = "",
    days: int = 90,
    stack_only: bool = False,
    stack_id: int | None = None,
    normalization: str = "global",
    lift_mode: str = "percent_delta",
    interval_method: str = "wilson",
    min_n: int = 0,
    debug: bool = False,
) -> dict:
    try:
        cur = _get_db_cursor()
        cur.execute(
            "SELECT player_id FROM players WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) ORDER BY player_id DESC LIMIT 1",
            (username,),
        )
        player = cur.fetchone()
        if not player:
            return {"username": username, "analysis": {"error": "Player not found."}}
        player_id = int(player["player_id"])

        safe_days = max(1, min(int(days), 3650))
        latest_card_sql = """
            WITH latest_card AS (
                SELECT
                    smc.match_id,
                    smc.map_name,
                    smc.mode,
                    smc.scraped_at,
                    ROW_NUMBER() OVER (PARTITION BY smc.match_id ORDER BY smc.id DESC) AS rn
                FROM scraped_match_cards smc
            )
            SELECT
                pr.match_id,
                pr.round_id,
                pr.side,
                pr.operator,
                pr.username,
                ro.winner_side,
                lc.map_name,
                lc.mode,
                lc.scraped_at
            FROM player_rounds pr
            JOIN latest_card lc ON lc.match_id = pr.match_id AND lc.rn = 1
            JOIN round_outcomes ro
              ON ro.player_id = pr.player_id
             AND ro.match_id = pr.match_id
             AND ro.round_id = pr.round_id
            WHERE pr.player_id = ?
              AND pr.operator IS NOT NULL
              AND TRIM(pr.operator) != ''
              AND DATETIME(COALESCE(lc.scraped_at, '1970-01-01 00:00:00')) >= DATETIME('now', ?)
            ORDER BY pr.match_id, pr.round_id
        """
        cur.execute(latest_card_sql, (player_id, f"-{safe_days} days"))
        rows = [dict(r) for r in cur.fetchall()]
        if not rows:
            return {"username": username, "analysis": {"error": "No round data for selected filters."}}

        mode_raw = str(mode or "").strip().lower()
        all_modes = mode_raw == "all"
        mode_key = _normalize_mode_key(mode_raw)
        selected_map = str(map_name or "").strip().lower()
        filtered_mode = [r for r in rows if all_modes or _normalize_mode_key(r.get("mode")) == mode_key]
        available_maps = sorted({str(r.get("map_name") or "").strip() for r in filtered_mode if str(r.get("map_name") or "").strip()})
        filtered = filtered_mode
        if selected_map:
            filtered = [r for r in filtered if str(r.get("map_name") or "").strip().lower() == selected_map]
        if not filtered:
            return {"username": username, "analysis": {"error": "No rounds after mode/map filters.", "available_maps": available_maps}}

        stack_teammates: set[str] = set()
        stack_context = {
            "enabled": stack_only,
            "applied": False,
            "stack_id": stack_id,
            "matched_teammates": [],
            "reason": "",
        }
        if stack_only:
            chosen_stack_id = stack_id
            if chosen_stack_id is None:
                cur.execute(
                    """
                    SELECT s.stack_id
                    FROM stacks s
                    JOIN stack_members sm ON sm.stack_id = s.stack_id
                    JOIN players p ON p.player_id = sm.player_id
                    WHERE LOWER(TRIM(p.username)) = LOWER(TRIM(?))
                    ORDER BY s.stack_id ASC
                    LIMIT 1
                    """,
                    (username,),
                )
                row = cur.fetchone()
                chosen_stack_id = int(row["stack_id"]) if row else None
            if chosen_stack_id is not None:
                cur.execute(
                    """
                    SELECT p.username
                    FROM stack_members sm
                    JOIN players p ON p.player_id = sm.player_id
                    WHERE sm.stack_id = ?
                    """,
                    (chosen_stack_id,),
                )
                stack_teammates = {
                    str(r["username"]).strip().lower()
                    for r in cur.fetchall()
                    if str(r["username"]).strip().lower() != username.strip().lower()
                }
                stack_context["stack_id"] = chosen_stack_id
            if chosen_stack_id is None:
                stack_context["reason"] = "No stack found for this player; showing all matches."
            elif not stack_teammates:
                stack_context["reason"] = "Stack has no teammates besides the player; showing all matches."
            else:
                by_match_users: dict[str, set[str]] = {}
                for r in filtered:
                    by_match_users.setdefault(str(r["match_id"]), set()).add(str(r.get("username") or "").strip().lower())
                allowed_matches = {
                    mid for mid, names in by_match_users.items()
                    if names.intersection(stack_teammates)
                }
                matched = sorted({
                    n for mid, names in by_match_users.items()
                    if mid in allowed_matches
                    for n in names.intersection(stack_teammates)
                })
                stack_context["matched_teammates"] = matched
                if allowed_matches:
                    filtered = [r for r in filtered if str(r["match_id"]) in allowed_matches]
                    stack_context["applied"] = True
                else:
                    stack_context["reason"] = "No rounds matched stack teammates under current filters; showing all matches."

        min_n_safe = max(0, min(int(min_n), 5000))
        rounds: dict[tuple[str, int], dict] = {}
        for r in filtered:
            key = (str(r["match_id"]), int(r["round_id"]))
            bucket = rounds.setdefault(
                key,
                {
                    "winner_side": str(r.get("winner_side") or ""),
                    "atk_ops": set(),
                    "def_ops": set(),
                },
            )
            op = str(r.get("operator") or "").strip()
            side = str(r.get("side") or "").strip().lower()
            if not op:
                continue
            if side == "attacker":
                bucket["atk_ops"].add(op)
            elif side == "defender":
                bucket["def_ops"].add(op)

        valid_rounds = [v for v in rounds.values() if v["atk_ops"] and v["def_ops"]]
        if not valid_rounds:
            return {"username": username, "analysis": {"error": "No valid rounds with both ATK and DEF operator sets.", "available_maps": available_maps}}

        atk_round_wins = sum(1 for v in valid_rounds if str(v.get("winner_side") or "").lower() == "attacker")
        total_rounds = len(valid_rounds)
        global_baseline = (atk_round_wins / total_rounds) * 100.0 if total_rounds else 0.0

        pair_stats: dict[tuple[str, str], dict[str, int]] = {}
        atk_counts: dict[str, int] = {}
        def_counts: dict[str, int] = {}
        atk_wins_by_op: dict[str, int] = {}
        exposures_total = 0
        exposures_atk_wins = 0

        for v in valid_rounds:
            atk_ops = sorted(v["atk_ops"])
            def_ops = sorted(v["def_ops"])
            atk_win = 1 if str(v.get("winner_side") or "").lower() == "attacker" else 0
            exposures = len(atk_ops) * len(def_ops)
            exposures_total += exposures
            exposures_atk_wins += exposures * atk_win
            for a in atk_ops:
                atk_counts[a] = atk_counts.get(a, 0) + 1
                if atk_win:
                    atk_wins_by_op[a] = atk_wins_by_op.get(a, 0) + 1
                else:
                    atk_wins_by_op.setdefault(a, atk_wins_by_op.get(a, 0))
            for d in def_ops:
                def_counts[d] = def_counts.get(d, 0) + 1
            for a in atk_ops:
                for d in def_ops:
                    key = (a, d)
                    cell = pair_stats.setdefault(key, {"n": 0, "atk_wins": 0})
                    cell["n"] += 1
                    cell["atk_wins"] += atk_win

        attackers = [k for k, _ in sorted(atk_counts.items(), key=lambda x: (-x[1], x[0]))]
        defenders = [k for k, _ in sorted(def_counts.items(), key=lambda x: (-x[1], x[0]))]

        norm_key = str(normalization or "global").strip().lower()
        if norm_key not in {"global", "attacker"}:
            norm_key = "global"
        lift_key = str(lift_mode or "percent_delta").strip().lower()
        if lift_key not in {"percent_delta", "logit_lift", "log_odds_ratio"}:
            lift_key = "percent_delta"
        interval_key = str(interval_method or "wilson").strip().lower()
        if interval_key not in {"wilson", "wald"}:
            interval_key = "wilson"

        cells = []
        continuity_applied_count = 0
        logit_eps_clips_count = 0
        nan_or_inf_cells_count = 0
        for a in attackers:
            for d in defenders:
                key = (a, d)
                if key not in pair_stats:
                    continue
                n = int(pair_stats[key]["n"])
                wins = int(pair_stats[key]["atk_wins"])
                win_pct = (wins / n) * 100.0 if n > 0 else 0.0
                row_baseline = (atk_wins_by_op.get(a, 0) / atk_counts.get(a, 1)) * 100.0 if atk_counts.get(a, 0) else global_baseline
                baseline_used = row_baseline if norm_key == "attacker" else global_baseline
                p = wins / n if n > 0 else 0.0
                z = 1.96
                if interval_key == "wilson":
                    z2 = z * z
                    denom = 1.0 + (z2 / n)
                    center = (p + (z2 / (2.0 * n))) / denom
                    half = (z / denom) * math.sqrt((p * (1.0 - p) / n) + (z2 / (4.0 * n * n)))
                    lo_p = max(0.0, center - half)
                    hi_p = min(1.0, center + half)
                else:
                    se = math.sqrt((p * (1.0 - p)) / n) if n > 0 else 0.0
                    lo_p = max(0.0, p - z * se)
                    hi_p = min(1.0, p + z * se)

                eps = 1e-6
                baseline_prob = max(eps, min(1.0 - eps, baseline_used / 100.0))
                if baseline_prob in {eps, 1.0 - eps}:
                    logit_eps_clips_count += 1
                lo_prob = max(eps, min(1.0 - eps, lo_p))
                if lo_prob in {eps, 1.0 - eps}:
                    logit_eps_clips_count += 1
                hi_prob = max(eps, min(1.0 - eps, hi_p))
                if hi_prob in {eps, 1.0 - eps}:
                    logit_eps_clips_count += 1
                p_prob = max(eps, min(1.0 - eps, p))
                if p_prob in {eps, 1.0 - eps}:
                    logit_eps_clips_count += 1

                if lift_key == "percent_delta":
                    metric = win_pct - baseline_used
                    ci_low = (lo_p * 100.0) - baseline_used
                    ci_high = (hi_p * 100.0) - baseline_used
                elif lift_key == "logit_lift":
                    # Jeffreys-smoothed mean for stable logit when wins==0 or wins==n.
                    p_smooth = (wins + 0.5) / (n + 1.0)
                    p_smooth = max(eps, min(1.0 - eps, p_smooth))
                    base_logit = math.log(baseline_prob / (1.0 - baseline_prob))
                    metric = math.log(p_smooth / (1.0 - p_smooth)) - base_logit
                    ci_low = math.log(lo_prob / (1.0 - lo_prob)) - base_logit
                    ci_high = math.log(hi_prob / (1.0 - hi_prob)) - base_logit
                else:
                    # log_odds_ratio from 2x2 table:
                    # present(pair) vs absent(pair) by ATK win/loss.
                    a_count = float(wins)
                    b_count = float(max(0, n - wins))
                    if norm_key == "attacker":
                        row_total = float(atk_counts.get(a, 0))
                        row_wins = float(atk_wins_by_op.get(a, 0))
                        c_count = float(max(0.0, row_wins - a_count))
                        d_count = float(max(0.0, (row_total - row_wins) - b_count))
                    else:
                        total_wins = float(atk_round_wins)
                        total_losses = float(max(0, total_rounds - atk_round_wins))
                        c_count = float(max(0.0, total_wins - a_count))
                        d_count = float(max(0.0, total_losses - b_count))

                    # Continuity correction for zero cells to avoid infinities.
                    if min(a_count, b_count, c_count, d_count) <= 0.0:
                        continuity_applied_count += 1
                        a_count += 0.5
                        b_count += 0.5
                        c_count += 0.5
                        d_count += 0.5

                    log_or = math.log((a_count * d_count) / (b_count * c_count))
                    se_log_or = math.sqrt((1.0 / a_count) + (1.0 / b_count) + (1.0 / c_count) + (1.0 / d_count))
                    metric = log_or
                    ci_low = log_or - (1.96 * se_log_or)
                    ci_high = log_or + (1.96 * se_log_or)

                if not math.isfinite(metric):
                    metric = 0.0
                    nan_or_inf_cells_count += 1
                if not math.isfinite(ci_low):
                    ci_low = 0.0
                    nan_or_inf_cells_count += 1
                if not math.isfinite(ci_high):
                    ci_high = 0.0
                    nan_or_inf_cells_count += 1
                cells.append(
                    {
                        "attacker": a,
                        "defender": d,
                        "n_rounds": n,
                        "atk_wins": wins,
                        "win_pct": round(win_pct, 1),
                        "baseline_wr": round(baseline_used, 1),
                        "win_ci_low": round(lo_p * 100.0, 1),
                        "win_ci_high": round(hi_p * 100.0, 1),
                        "lift": round(metric, 3 if lift_key != "percent_delta" else 1),
                        "ci_low": round(ci_low, 3 if lift_key != "percent_delta" else 1),
                        "ci_high": round(ci_high, 3 if lift_key != "percent_delta" else 1),
                    }
                )

        analysis = {
            "baseline_atk_win_rate": round(global_baseline, 1),
            "total_rounds": total_rounds,
            "attackers": attackers,
            "defenders": defenders,
            "cells": cells,
            "available_maps": available_maps,
            "normalization": norm_key,
            "lift_mode": lift_key,
            "interval_method": interval_key,
            "filters": {
                "mode": "all" if all_modes else mode_key,
                "map_name": map_name,
                "days": safe_days,
                "stack_only": stack_only,
                "normalization": norm_key,
                "lift_mode": lift_key,
                "interval_method": interval_key,
                "min_n": min_n_safe,
            },
            "stack_context": stack_context,
        }
        if debug:
            total_cell_weight = sum(int(c["n_rounds"]) for c in cells)
            weighted_mean_cell_wr = (
                sum(float(c["win_pct"]) * int(c["n_rounds"]) for c in cells) / total_cell_weight
                if total_cell_weight > 0 else 0.0
            )
            exposure_weighted_baseline = (
                (exposures_atk_wins / exposures_total) * 100.0 if exposures_total > 0 else 0.0
            )
            row_checks = []
            if norm_key == "attacker":
                for atk in attackers:
                    row_cells = [c for c in cells if c["attacker"] == atk]
                    row_w = sum(int(c["n_rounds"]) for c in row_cells)
                    row_lift = (
                        sum(float(c["lift"]) * int(c["n_rounds"]) for c in row_cells) / row_w
                        if row_w > 0 else 0.0
                    )
                    row_checks.append(
                        {
                            "attacker": atk,
                            "weighted_mean_lift": round(row_lift, 4),
                            "max_abs_lift": round(max((abs(float(c["lift"])) for c in row_cells), default=0.0), 4),
                            "cells_hidden": sum(1 for c in row_cells if int(c["n_rounds"]) < min_n_safe),
                            "cells_total": len(row_cells),
                        }
                    )
            analysis["diagnostics"] = {
                "global_checks": {
                    "global_atk_baseline_wr": round(global_baseline, 4),
                    "exposure_weighted_baseline_wr": round(exposure_weighted_baseline, 4),
                    "weighted_mean_cell_wr": round(weighted_mean_cell_wr, 4),
                    "rounds_total": len(rounds),
                    "rounds_valid": total_rounds,
                    "rows_fetched": len(rows),
                    "rows_after_mode_filter": len(filtered_mode),
                    "rows_after_all_filters": len(filtered),
                    "cells_hidden_by_min_n": sum(1 for c in cells if int(c["n_rounds"]) < min_n_safe),
                    "cells_total": len(cells),
                },
                "row_checks": row_checks,
                "pathology_counters": {
                    "or_continuity_applied_count": continuity_applied_count,
                    "logit_eps_clips_count": logit_eps_clips_count,
                    "nan_or_inf_cells_count": nan_or_inf_cells_count,
                },
            }
        return {"username": username, "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute atk-def heatmap: {str(e)}")


configure_network_scan(
    error_tracker_dep=error_tracker,
    track_call_dep=track_call,
    get_rate_status_dep=get_rate_status,
)
register_network_scan_routes(app)

configure_match_scrape(db_dep=db)
register_match_scrape_routes(app)


if __name__ == "__main__":
    import uvicorn

    print("Starting JAKAL Web Server...")
    print("Open http://localhost:5000 in your browser")
    uvicorn.run(app, host="127.0.0.1", port=5000)
