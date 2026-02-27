from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timezone
import json
import os
import random
import re
import sys
import time
import traceback
import math
import base64
import hashlib

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

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

app = FastAPI()
app.mount("/static", StaticFiles(directory="web/static"), name="static")
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _normalize_asset_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _normalize_mode_key(raw_mode: object) -> str:
    text = str(raw_mode or "").strip().lower()
    if not text:
        return "other"
    if "unranked" in text:
        return "unranked"
    if "ranked" in text:
        return "ranked"
    if "standard" in text:
        return "standard"
    if "quick" in text:
        return "quick"
    if "event" in text:
        return "event"
    if "arcade" in text:
        return "arcade"
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
async def operators_map_breakdown(username: str, stack: str = "solo", match_type: str = "Ranked") -> dict:
    clean_username = str(username or "").strip()
    if not clean_username:
        raise HTTPException(status_code=400, detail="username is required")
    stack_key = str(stack or "solo").strip().lower()
    stack_to_friend_count = {
        "solo": 0,
        "duo": 1,
        "trio": 2,
        "squad": 3,
        "full": 4,
        "fullstack": 4,
        "full_stack": 4,
    }
    if stack_key not in stack_to_friend_count:
        raise HTTPException(status_code=400, detail="stack must be one of: solo, duo, trio, squad, full")
    friend_target = int(stack_to_friend_count[stack_key])
    mode_key = str(match_type or "Ranked").strip().lower()
    mode_aliases = {"ranked", "pvp_ranked"} if mode_key in {"ranked", "pvp_ranked"} else {mode_key}

    try:
        cursor = db.conn.cursor()
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
                FROM match_detail_players
                WHERE match_id = ?
                  AND LOWER(TRIM(username)) = LOWER(TRIM(?))
                LIMIT 1
                """,
                (mid, clean_username),
            )
            mt = cursor.fetchone()
            mk = str(mt["match_type"] or "").strip().lower() if mt else ""
            if mk not in mode_aliases:
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
            if friend_count == friend_target:
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
                COALESCE(NULLIF(TRIM(pr.operator), ''), 'Unknown') AS operator,
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
            GROUP BY operator, side, map_name
            HAVING COUNT(*) >= 8
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
            "eligible_matches": len(set(eligible_match_ids)),
            "maps": high_data,
            "low_data_maps": low_data,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute operators map breakdown: {str(e)}")


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


def _parse_iso_datetime(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


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


def _wilson_ci(successes: int, trials: int) -> tuple[float, float]:
    if trials <= 0:
        return 0.0, 0.0
    p = successes / trials
    z = 1.96
    z2 = z * z
    denom = 1.0 + (z2 / trials)
    center = (p + (z2 / (2.0 * trials))) / denom
    half = (z / denom) * math.sqrt((p * (1.0 - p) / trials) + (z2 / (4.0 * trials * trials)))
    return max(0.0, center - half), min(1.0, center + half)


def _pctile_abs_bound(values: list[float], fallback: float = 15.0) -> float:
    vals = sorted(abs(float(v)) for v in values if isinstance(v, (float, int)) and math.isfinite(float(v)))
    if len(vals) < 8:
        return fallback
    lo = vals[int(max(0, math.floor(0.05 * (len(vals) - 1))))]
    hi = vals[int(max(0, math.floor(0.95 * (len(vals) - 1))))]
    hi = max(hi, lo, 0.000001)
    return hi


def _is_unknown_operator_name(value: object) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return True
    bad = {
        "unknown",
        "unk",
        "n/a",
        "na",
        "none",
        "null",
        "-",
        "?",
        "operator",
        "undefined",
    }
    return text in bad


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
    cur = db.conn.cursor()
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
) -> tuple[int, list[dict], dict, list[str]]:
    cur = db.conn.cursor()
    cur.execute(
        "SELECT player_id FROM players WHERE LOWER(TRIM(username)) = LOWER(TRIM(?)) ORDER BY player_id DESC LIMIT 1",
        (username,),
    )
    row = cur.fetchone()
    if not row:
        return 0, [], {}, ["Player not found."]
    player_id = int(row["player_id"])

    safe_days = max(1, min(int(days), 3650))
    warnings: list[str] = []
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
    selected_map = str(map_name or "").strip().lower()
    search_key = str(search or "").strip().lower()

    sql = """
        WITH latest_card AS (
            SELECT
                smc.match_id,
                smc.map_name,
                smc.mode,
                smc.match_date,
                smc.summary_json,
                smc.scraped_at,
                ROW_NUMBER() OVER (PARTITION BY smc.match_id ORDER BY smc.id DESC) AS rn
            FROM scraped_match_cards smc
        )
        SELECT
            pr.id AS pr_id,
            pr.player_id,
            pr.match_id,
            pr.round_id,
            pr.side,
            pr.operator,
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
            ro.winner_side,
            lc.map_name,
            lc.mode AS card_mode,
            lc.match_date,
            lc.summary_json,
            lc.scraped_at
        FROM player_rounds pr
        JOIN latest_card lc
          ON lc.match_id = pr.match_id
         AND lc.rn = 1
        JOIN round_outcomes ro
          ON ro.player_id = pr.player_id
         AND ro.match_id = pr.match_id
         AND ro.round_id = pr.round_id
        WHERE pr.player_id = ?
          AND pr.operator IS NOT NULL
          AND TRIM(pr.operator) != ''
          AND DATETIME(COALESCE(lc.scraped_at, '1970-01-01 00:00:00')) >= DATETIME('now', ?)
    """
    cur.execute(sql, (player_id, f"-{safe_days} days"))
    rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        return player_id, [], {}, warnings

    def _row_mode(r: dict) -> str:
        return _normalize_mode_key(r.get("card_mode") or r.get("match_type"))

    def _queue_ok(r: dict) -> bool:
        if queue_key == "all":
            return True
        return _row_mode(r) == queue_key

    def _playlist_ok(r: dict) -> bool:
        if not playlist_key:
            return True
        return _row_mode(r) == playlist_key

    filtered = [r for r in rows if _queue_ok(r) and _playlist_ok(r)]
    if selected_map:
        filtered = [r for r in filtered if str(r.get("map_name") or "").strip().lower() == selected_map]
    if search_key:
        filtered = [
            r for r in filtered
            if search_key in str(r.get("operator") or "").lower() or search_key in str(r.get("username") or "").lower()
        ]

    stack_context = {"enabled": stack_only, "applied": False, "stack_id": stack_id, "matched_teammates": [], "reason": ""}
    if stack_only and filtered:
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
            by_match_users: dict[str, set[str]] = {}
            for r in filtered:
                by_match_users.setdefault(str(r["match_id"]), set()).add(str(r.get("username") or "").strip().lower())
            allowed = {mid for mid, names in by_match_users.items() if names.intersection(teammates)}
            matched = sorted({n for mid, names in by_match_users.items() if mid in allowed for n in names.intersection(teammates)})
            if allowed:
                filtered = [r for r in filtered if str(r["match_id"]) in allowed]
                stack_context["applied"] = True
                stack_context["matched_teammates"] = matched
            else:
                stack_context["reason"] = "No matches contained configured stack teammates."

    by_match: dict[str, dict] = {}
    for r in filtered:
        mid = str(r["match_id"])
        info = by_match.setdefault(mid, {})
        if "end_time" not in info:
            end_dt, start_dt = _extract_match_times(r)
            info["end_time"] = end_dt
            info["start_time"] = start_dt
            info["scraped_at"] = _parse_iso_datetime(r.get("scraped_at"))
    ordering_mode = "ingestion_fallback"
    if any(v.get("end_time") for v in by_match.values()):
        ordering_mode = "match_end_time"
    elif any(v.get("start_time") for v in by_match.values()):
        ordering_mode = "match_start_time"

    def _primary_epoch(mid: str) -> float:
        info = by_match.get(mid, {})
        if ordering_mode == "match_end_time" and info.get("end_time"):
            return float(info["end_time"].timestamp())
        if ordering_mode == "match_start_time" and info.get("start_time"):
            return float(info["start_time"].timestamp())
        dt = info.get("scraped_at")
        return float(dt.timestamp()) if dt else 0.0

    for r in filtered:
        r["_order_primary"] = _primary_epoch(str(r["match_id"]))
    filtered.sort(
        key=lambda r: (
            float(r.get("_order_primary", 0.0)),
            str(r.get("match_id") or ""),
            int(r.get("round_id") or 0),
            int(r.get("pr_id") or 0),
        ),
        reverse=True,
    )
    return player_id, filtered, {"ordering_mode": ordering_mode, "stack_context": stack_context}, warnings


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
        cur = db.conn.cursor()
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


@app.websocket("/ws/scan")
async def scan_network(websocket: WebSocket) -> None:
    await websocket.accept()

    try:
        data = await websocket.receive_json()
        username = data.get("username")
        max_depth = data.get("max_depth", 2)
        debug_browser = bool(data.get("debug_browser", False))

        await websocket.send_json(
            {
                "type": "scan_started",
                "username": username,
                "max_depth": max_depth,
                "debug_browser": debug_browser,
            }
        )

        await scan_player_network(websocket, username, max_depth, debug_browser)
        await websocket.send_json({"type": "scan_complete"})

    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        await websocket.send_json({"type": "error", "message": str(e)})


async def scan_player_network(websocket: WebSocket, username: str, max_depth: int, debug_browser: bool) -> None:
    """
    Scan network using Playwright browser automation.
    Includes comprehensive error handling and graceful degradation.
    """
    browser = None

    # Reset per-scan error counters
    error_tracker["consecutive_failures"] = 0
    error_tracker["total_failures"] = 0
    error_tracker["last_error"] = None

    try:
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(
                    headless=not debug_browser,
                )
                context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                )
                page = await context.new_page()
            except Exception as e:
                await websocket.send_json(
                    {
                        "type": "error",
                        "message": f"Failed to launch browser: {str(e)}",
                    }
                )
                return

            scanned = set()
            best_distance = {username: 0}
            node_depth_sent = {}
            to_scan = deque([(username, 0)])

            await page.wait_for_timeout(random.randint(500, 1500))

            while to_scan:
                current_username, depth = to_scan.popleft()

                if depth > max_depth:
                    continue
                if depth > best_distance.get(current_username, float("inf")):
                    continue
                if current_username in scanned:
                    continue

                if error_tracker["consecutive_failures"] >= error_tracker["failure_threshold"]:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "message": (
                                f"Stopping scan: {error_tracker['failure_threshold']} consecutive failures. "
                                "Possible rate limit or blocking."
                            ),
                            "total_failures": error_tracker["total_failures"],
                            "last_error": error_tracker["last_error"],
                        }
                    )
                    break

                await websocket.send_json(
                    {
                        "type": "scanning",
                        "username": current_username,
                        "depth": depth,
                        "distance": depth,
                    }
                )

                track_call(f"/profile/{current_username}")
                await websocket.send_json({"type": "rate_status", **get_rate_status()})

                try:
                    encoded_username = current_username.replace(" ", "%20")
                    overview_url = f"https://r6.tracker.network/r6siege/profile/ubi/{encoded_username}/overview"
                    encounters_url = f"https://r6.tracker.network/r6siege/profile/ubi/{encoded_username}/encounters"
                    is_root_profile = depth == 0 and current_username == username
                    url = overview_url if is_root_profile else encounters_url

                    try:
                        response = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                        if response and response.status >= 400:
                            if response.status == 429:
                                await websocket.send_json(
                                    {
                                        "type": "error",
                                        "username": current_username,
                                        "message": "Rate limited (429). Pausing scan.",
                                    }
                                )
                                error_tracker["consecutive_failures"] += 1
                                error_tracker["total_failures"] += 1
                                error_tracker["last_error"] = "429 Rate Limit"
                                scanned.add(current_username)
                                await page.wait_for_timeout(30000)
                                continue
                            if response.status == 403:
                                await websocket.send_json(
                                    {
                                        "type": "warning",
                                        "username": current_username,
                                        "message": "403/challenge detected. Attempting Playwright recovery.",
                                    }
                                )
                                recovered = False
                                max_attempts = 6 if debug_browser else 2
                                for _ in range(max_attempts):
                                    try:
                                        # In debug mode this allows manual challenge completion.
                                        await page.wait_for_selector(".giant-stat", timeout=15000)
                                        recovered = True
                                        break
                                    except PlaywrightTimeout:
                                        try:
                                            await page.reload(wait_until="domcontentloaded", timeout=20000)
                                        except Exception:
                                            pass
                                if not recovered:
                                    await websocket.send_json(
                                        {
                                            "type": "error",
                                            "username": current_username,
                                            "message": "Access still blocked after recovery attempts.",
                                        }
                                    )
                                    error_tracker["consecutive_failures"] += 1
                                    error_tracker["total_failures"] += 1
                                    error_tracker["last_error"] = "403 Forbidden"
                                    scanned.add(current_username)
                                    continue
                            if response.status == 404:
                                await websocket.send_json(
                                    {
                                        "type": "error",
                                        "username": current_username,
                                        "message": "Profile not found (404)",
                                    }
                                )
                                error_tracker["consecutive_failures"] = 0
                                scanned.add(current_username)
                                continue
                    except PlaywrightTimeout:
                        await websocket.send_json(
                            {
                                "type": "error",
                                "username": current_username,
                                "message": "Page load timeout (20s). Possible blocking.",
                            }
                        )
                        error_tracker["consecutive_failures"] += 1
                        error_tracker["total_failures"] += 1
                        error_tracker["last_error"] = "Timeout"
                        scanned.add(current_username)
                        continue

                    # Scroll to load content below the initial viewport.
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(2000)
                    await page.evaluate("window.scrollTo(0, 600)")
                    await page.wait_for_timeout(1500)
                    await page.evaluate("window.scrollTo(0, 1200)")
                    await page.wait_for_timeout(1000)

                    try:
                        page_title = await page.title()
                        if "just a moment" in page_title.lower() or "attention required" in page_title.lower():
                            await websocket.send_json(
                                {
                                    "type": "warning",
                                    "username": current_username,
                                    "message": "Challenge page detected. Waiting for clearance.",
                                }
                            )
                            recovered = False
                            max_attempts = 8 if debug_browser else 2
                            for _ in range(max_attempts):
                                try:
                                    await page.wait_for_selector(".giant-stat", timeout=15000)
                                    recovered = True
                                    break
                                except PlaywrightTimeout:
                                    try:
                                        await page.reload(wait_until="domcontentloaded", timeout=20000)
                                    except Exception:
                                        pass
                            if not recovered:
                                await websocket.send_json(
                                    {
                                        "type": "error",
                                        "username": current_username,
                                        "message": "Challenge not cleared in time.",
                                    }
                                )
                                error_tracker["consecutive_failures"] += 1
                                error_tracker["total_failures"] += 1
                                error_tracker["last_error"] = "Challenge not cleared"
                                scanned.add(current_username)
                                continue
                    except Exception:
                        pass

                    if is_root_profile:
                        # ============================================================
                        # WAIT FOR PAGE LOAD
                        # ============================================================
                        try:
                            await page.wait_for_load_state("domcontentloaded")
                            await page.wait_for_timeout(random.randint(2000, 3000))
                        except Exception:
                            pass

                        # ============================================================
                        # CLICK "VIEW ALL STATS" BUTTON
                        # ============================================================
                        try:
                            view_stats_btn = page.get_by_role("button", name="View All Stats")
                            await view_stats_btn.scroll_into_view_if_needed()
                            await page.wait_for_timeout(random.randint(500, 1000))
                            await view_stats_btn.click()
                            await page.wait_for_timeout(random.randint(1500, 2500))
                            try:
                                await page.wait_for_selector("text=/K\\/D/", timeout=5000)
                            except Exception:
                                pass
                        except Exception as e:
                            await websocket.send_json(
                                {
                                    "type": "error",
                                    "username": current_username,
                                    "message": f"Could not open stats drawer: {str(e)}",
                                }
                            )
                            scanned.add(current_username)
                            continue

                        # ============================================================
                        # CLOSE AD IF IT APPEARS (optional)
                        # ============================================================
                        try:
                            ad_close = page.locator("#closeIconHit")
                            await ad_close.click(timeout=2000)
                            await page.wait_for_timeout(500)
                        except Exception:
                            pass

                        # ============================================================
                        # EXTRACT STATS FROM DRAWER
                        # ============================================================
                        rank_points = 0
                        kd = 0.0
                        win_pct = 0.0

                        try:
                            await page.mouse.wheel(0, 300)
                            await page.wait_for_timeout(random.randint(800, 1500))

                            # Primary approach: parse from full rendered text in drawer.
                            try:
                                body_text = await page.inner_text("body")
                                compact = re.sub(r"\s+", " ", body_text)

                                rp_match = re.search(r"Rank Points\s*([0-9][0-9,]*)", compact, re.IGNORECASE)
                                if rp_match:
                                    rank_points = int(rp_match.group(1).replace(",", ""))

                                win_match = re.search(
                                    r"(?:Win %|Win Rate)\s*([0-9]+(?:\.[0-9]+)?)%?",
                                    compact,
                                    re.IGNORECASE,
                                )
                                if win_match:
                                    win_pct = float(win_match.group(1))
                            except Exception:
                                pass

                            # Fallback selectors if text parsing misses anything.
                            if rank_points == 0:
                                try:
                                    rp_elem = await page.query_selector("text=/Rank Points/")
                                    if rp_elem:
                                        rp_text = await rp_elem.inner_text()
                                        rp_number = rp_text.replace("Rank Points", "").replace(",", "").strip()
                                        if rp_number:
                                            rank_points = int(rp_number)
                                except Exception:
                                    pass

                            # Extract K/D from drawer (format: K/D1.11)
                            try:
                                await page.mouse.wheel(0, 200)
                                await page.wait_for_timeout(500)
                                kd_elem = await page.query_selector("text=/K\\/D[0-9.]/")
                                if kd_elem:
                                    kd_text = await kd_elem.inner_text()
                                    kd_number = kd_text.replace("K/D", "").strip()
                                    if kd_number:
                                        kd = float(kd_number)
                            except Exception:
                                pass

                            if win_pct == 0.0:
                                try:
                                    win_elem = await page.query_selector("text=/Win %/")
                                    if win_elem:
                                        win_text = await win_elem.inner_text()
                                        win_number = win_text.replace("Win %", "").replace("%", "").strip()
                                        if win_number:
                                            win_pct = float(win_number)
                                except Exception:
                                    pass
                        except Exception as e:
                            await websocket.send_json(
                                {
                                    "type": "warning",
                                    "message": f"Error extracting stats: {str(e)}",
                                }
                            )

                        # ============================================================
                        # SEND NODE WITH STATS
                        # ============================================================
                        await websocket.send_json(
                            {
                                "type": "node_discovered",
                                "username": current_username,
                                "depth": depth,
                                "stats": {
                                    "rank_points": rank_points,
                                    "kd": kd,
                                    "win_pct": win_pct,
                                },
                            }
                        )
                        node_depth_sent[current_username] = depth

                        error_tracker["consecutive_failures"] = 0

                        # ============================================================
                        # CLOSE STATS DRAWER
                        # ============================================================
                        try:
                            close_drawer = page.locator(".size-6.cursor-pointer").first
                            await close_drawer.click()
                            await page.wait_for_timeout(random.randint(800, 1500))
                        except Exception as e:
                            await websocket.send_json(
                                {
                                    "type": "warning",
                                    "message": f"Could not close stats drawer: {str(e)}",
                                }
                            )

                    # ============================================================
                    # NAVIGATE DIRECTLY TO ENCOUNTERS PAGE
                    # ============================================================
                    try:
                        if is_root_profile:
                            encounters_response = await page.goto(
                                encounters_url,
                                wait_until="domcontentloaded",
                                timeout=20000,
                            )
                            if encounters_response and encounters_response.status >= 400:
                                raise RuntimeError(f"Encounters page status {encounters_response.status}")
                            await page.wait_for_timeout(random.randint(1800, 3200))
                    except Exception as e:
                        await websocket.send_json(
                            {
                                "type": "error",
                                "username": current_username,
                                "message": f"Could not open Encounters page: {str(e)}",
                            }
                        )
                        scanned.add(current_username)
                        continue

                    # ============================================================
                    # EXTRACT ENCOUNTERS FROM TABLE
                    # ============================================================
                    track_call(f"/encounters/{current_username}")
                    await websocket.send_json({"type": "rate_status", **get_rate_status()})

                    encounters = []

                    try:
                        row_selector = 'tr[class*="group/row"], table tbody tr, [class*="played-with"] tr'
                        await page.wait_for_selector(row_selector, timeout=7000)
                        await page.mouse.wheel(0, random.randint(300, 600))
                        await page.wait_for_timeout(random.randint(800, 1500))
                        encounter_rows = await page.query_selector_all(row_selector)

                        for row in encounter_rows:
                            try:
                                cells = await row.query_selector_all("td")
                                if len(cells) < 2:
                                    continue

                                name_elem = await cells[0].query_selector("span.truncate, a, [class*='name']")
                                if not name_elem:
                                    name_elem = await row.query_selector("a[href*='/profile/'], span.truncate")
                                if not name_elem:
                                    continue

                                encounter_name = await name_elem.inner_text()
                                encounter_name = encounter_name.strip()
                                if not encounter_name or encounter_name == current_username:
                                    continue

                                # 0 Player | 1 Encountered | 2 Rank | 3 Win Rate | 4 KD | 5 Matches | 6 Last Match
                                encountered_count = 0
                                rank_points = 0
                                win_rate = 0.0
                                kd_ratio = 0.0
                                matches_played = 0
                                last_match = ""
                                try:
                                    if len(cells) >= 2:
                                        encountered_text = await cells[1].inner_text()
                                        digits = "".join(ch for ch in encountered_text if ch.isdigit())
                                        encountered_count = int(digits) if digits else 0

                                    if len(cells) >= 3:
                                        rank_text = await cells[2].inner_text()
                                        digits = "".join(ch for ch in rank_text if ch.isdigit())
                                        rank_points = int(digits) if digits else 0

                                    if len(cells) >= 4:
                                        win_text = (await cells[3].inner_text()).replace("%", "").strip()
                                        win_rate = float(win_text) if win_text else 0.0

                                    if len(cells) >= 5:
                                        kd_text = (await cells[4].inner_text()).strip()
                                        kd_ratio = float(kd_text) if kd_text else 0.0

                                    if len(cells) >= 6:
                                        matches_text = await cells[5].inner_text()
                                        digits = "".join(ch for ch in matches_text if ch.isdigit())
                                        matches_played = int(digits) if digits else 0

                                    if len(cells) >= 7:
                                        last_match = (await cells[6].inner_text()).strip()
                                except Exception:
                                    pass

                                if encountered_count <= 0:
                                    continue

                                encounters.append(
                                    {
                                        "name": encounter_name,
                                        "count": encountered_count,
                                        "rank_points": rank_points,
                                        "kd": kd_ratio,
                                        "win_pct": win_rate,
                                        "matches_played": matches_played,
                                        "last_match": last_match,
                                    }
                                )
                            except Exception:
                                continue
                    except PlaywrightTimeout:
                        pass
                    except Exception as e:
                        await websocket.send_json(
                            {
                                "type": "warning",
                                "message": f"Error extracting encounters: {str(e)}",
                            }
                        )

                    # ============================================================
                    # SEND ENCOUNTERS
                    # ============================================================
                    await websocket.send_json(
                        {
                            "type": "encounters_found",
                            "username": current_username,
                            "count": len(encounters),
                        }
                    )

                    for encounter in encounters:
                        next_name = encounter["name"]
                        next_distance = depth + 1
                        known_distance = best_distance.get(next_name, float("inf"))

                        if next_distance < known_distance:
                            best_distance[next_name] = next_distance
                        emit_distance = best_distance.get(next_name, next_distance)

                        previously_sent_depth = node_depth_sent.get(next_name, float("inf"))
                        if emit_distance < previously_sent_depth:
                            await websocket.send_json(
                                {
                                    "type": "node_discovered",
                                    "username": next_name,
                                    "depth": emit_distance,
                                    "stats": {
                                        "rank_points": encounter.get("rank_points", 0),
                                        "kd": encounter.get("kd", 0.0),
                                        "win_pct": encounter.get("win_pct", 0.0),
                                    },
                                }
                            )
                            node_depth_sent[next_name] = emit_distance

                        await websocket.send_json(
                            {
                                "type": "edge_discovered",
                                "from": current_username,
                                "to": next_name,
                                "match_count": encounter["count"],
                                "last_played": encounter.get("last_match", ""),
                            }
                        )

                        if next_distance <= max_depth and next_distance < known_distance and next_name not in scanned:
                            to_scan.append((next_name, next_distance))

                    scanned.add(current_username)

                    # ============================================================
                    # DELAY BEFORE NEXT PLAYER
                    # ============================================================
                    delay = random.uniform(4.0, 7.0)
                    await websocket.send_json(
                        {
                            "type": "delay",
                            "seconds": round(delay, 1),
                            "reason": "Moving to next player...",
                        }
                    )
                    await page.wait_for_timeout(int(delay * 1000))

                except Exception as e:
                    error_msg = f"{type(e).__name__}: {str(e)}"
                    await websocket.send_json(
                        {
                            "type": "error",
                            "username": current_username,
                            "message": error_msg,
                        }
                    )
                    print(f"Error scanning {current_username}:")
                    print(traceback.format_exc())

                    error_tracker["consecutive_failures"] += 1
                    error_tracker["total_failures"] += 1
                    error_tracker["last_error"] = error_msg
                    scanned.add(current_username)
                    continue

    except Exception as e:
        await websocket.send_json(
            {
                "type": "error",
                "message": f"Fatal scan error: {str(e)}",
            }
        )
        print("Fatal error in scan:")
        print(traceback.format_exc())
    finally:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass

        if error_tracker["total_failures"] > 0:
            await websocket.send_json(
                {
                    "type": "scan_summary",
                    "total_failures": error_tracker["total_failures"],
                    "last_error": error_tracker["last_error"],
                }
            )


async def scrape_match_history(
    websocket: WebSocket,
    username: str,
    max_matches: int = 10,
    debug_browser: bool = False,
    newest_only: bool = False,
    full_backfill: bool = False,
    allowed_match_types: list[str] | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """
    Scrape detailed match history for a player.
    """
    browser = None

    allowed_types_norm = {
        str(t or "").strip().lower() for t in (allowed_match_types or []) if str(t or "").strip()
    }
    if full_backfill and newest_only:
        newest_only = False

    def _normalize_match_type(raw_mode: object) -> str:
        text = str(raw_mode or "").strip().lower()
        if "unranked" in text:
            return "unranked"
        if "ranked" in text:
            return "ranked"
        if "quick" in text:
            return "quick"
        if "standard" in text:
            return "standard"
        if "event" in text:
            return "event"
        return "other"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not debug_browser)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        page = await context.new_page()

        try:
            url = f"https://r6.tracker.network/r6siege/profile/ubi/{username}/matches"
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(random.randint(800, 1200))
            await websocket.send_json(
                {
                    "type": "debug",
                    "message": f"On matches page, URL: {page.url}",
                }
            )
            # Navigate to Matches tab and wait for rendered match rows.
            try:
                matches_link = page.get_by_role("link", name="Matches", exact=True)
                await matches_link.scroll_into_view_if_needed()
                await page.wait_for_timeout(random.randint(500, 1000))
                await matches_link.click()
                await page.wait_for_timeout(random.randint(800, 1200))
            except Exception:
                # Already on matches tab or link not present in this layout.
                pass
            try:
                await page.wait_for_selector(".v3-match-row", timeout=10000, state="visible")
                await websocket.send_json(
                    {
                        "type": "debug",
                        "message": "Match rows loaded and visible",
                    }
                )
            except Exception as e:
                await websocket.send_json(
                    {
                        "type": "error",
                        "message": f"Match rows failed to load: {str(e)}",
                    }
                )
                return
            await page.wait_for_timeout(800)
            match_cards = await page.query_selector_all(".v3-match-row")
            await websocket.send_json(
                {
                    "type": "debug",
                    "message": f"Found {len(match_cards)} match cards",
                }
            )
            if len(match_cards) == 0:
                await websocket.send_json(
                    {
                        "type": "error",
                        "message": "No matches found (even after waiting)",
                    }
                )
                return

            def _unique_items(items):
                out = []
                seen = set()
                for item in items:
                    key = item.get("href") or item.get("text") or ""
                    if key and key in seen:
                        continue
                    seen.add(key)
                    out.append(item)
                return out

            async def _collect_match_targets() -> list:
                selectors = [
                    'a[href*="/matches/"]',
                    '[class*="match-card"] a',
                    '[class*="match-item"] a',
                    'tr a[href*="/matches/"]',
                ]
                targets = []
                for sel in selectors:
                    try:
                        await page.wait_for_selector(sel, timeout=4000)
                    except PlaywrightTimeout:
                        continue
                    nodes = await page.query_selector_all(sel)
                    for node in nodes:
                        try:
                            href = await node.get_attribute("href")
                            text = (await node.inner_text()).strip()
                            if href and "/matches/" in href:
                                targets.append({"href": href, "text": text})
                        except Exception:
                            continue
                    if targets:
                        break
                return _unique_items(targets)

            def _extract_match_id(text: str) -> str:
                if not text:
                    return ""
                m = re.search(r"/matches/([0-9a-fA-F-]{36})", text)
                if m:
                    return m.group(1)
                m = re.search(r"([0-9a-fA-F-]{36})", text)
                return m.group(1) if m else ""

            async def _extract_row_match_id_from_dom(row) -> str:
                try:
                    href = await row.get_attribute("href")
                    mid = _extract_match_id(href or "")
                    if mid:
                        return mid
                except Exception:
                    pass
                try:
                    html = await row.inner_html()
                    return _extract_match_id(html or "")
                except Exception:
                    return ""

            def _extract_operator_name(raw: object) -> str:
                if isinstance(raw, str):
                    return raw.strip()
                if isinstance(raw, dict):
                    for key in ("name", "operatorName", "operator", "label", "value", "slug"):
                        value = raw.get(key)
                        if isinstance(value, str) and value.strip():
                            return value.strip()
                return ""

            def _parse_rounds(round_data: dict) -> list:
                if not isinstance(round_data, dict):
                    return []
                players_raw = round_data.get("players", [])
                player_name_by_id = {}
                player_operator_by_id = {}
                if isinstance(players_raw, list):
                    for p in players_raw:
                        if not isinstance(p, dict):
                            continue
                        pid = p.get("id")
                        pname = p.get("nickname") or p.get("pseudonym") or p.get("name") or ""
                        if pid and pname:
                            player_name_by_id[str(pid)] = str(pname)
                        op_name = _extract_operator_name(
                            p.get("operator")
                            or p.get("operatorName")
                            or p.get("operator_name")
                            or p.get("operatorData")
                            or p.get("operator_data")
                        )
                        if pid and op_name:
                            player_operator_by_id[str(pid)] = op_name

                killfeed_raw = round_data.get("killfeed", [])
                killfeed_by_round = {}
                if isinstance(killfeed_raw, list):
                    for ev in killfeed_raw:
                        if not isinstance(ev, dict):
                            continue
                        rid = ev.get("roundId")
                        if rid is None:
                            continue
                        rid_key = str(rid)
                        attacker_id = ev.get("attackerId")
                        victim_id = ev.get("victimId")
                        attacker_operator = _extract_operator_name(
                            ev.get("attackerOperatorName")
                            or ev.get("attackerOperator")
                            or ev.get("killerOperatorName")
                            or ev.get("killerOperator")
                        )
                        victim_operator = _extract_operator_name(
                            ev.get("victimOperatorName")
                            or ev.get("victimOperator")
                        )
                        parsed_ev = {
                            "timestamp": ev.get("timestamp"),
                            "killerId": attacker_id,
                            "victimId": victim_id,
                            "killerName": player_name_by_id.get(str(attacker_id), attacker_id or "Unknown"),
                            "victimName": player_name_by_id.get(str(victim_id), victim_id or "Unknown"),
                            "killerOperator": attacker_operator or player_operator_by_id.get(str(attacker_id), ""),
                            "victimOperator": victim_operator or player_operator_by_id.get(str(victim_id), ""),
                        }
                        killfeed_by_round.setdefault(rid_key, []).append(parsed_ev)

                rounds_raw = (
                    round_data.get("rounds")
                    or round_data.get("data", {}).get("rounds")
                    or []
                )
                parsed = []
                for idx, rnd in enumerate(rounds_raw):
                    if not isinstance(rnd, dict):
                        continue
                    round_id = rnd.get("id", idx + 1)
                    round_num = idx + 1
                    try:
                        if isinstance(round_id, (int, float)):
                            round_num = int(round_id)
                        elif isinstance(round_id, str) and round_id.isdigit():
                            round_num = int(round_id)
                    except Exception:
                        pass
                    kill_events = (
                        killfeed_by_round.get(str(round_num))
                        or killfeed_by_round.get(str(round_id))
                        or rnd.get("killEvents")
                        or rnd.get("kills")
                        or []
                    )
                    round_players = rnd.get("players", [])
                    round_operator_by_id = {}
                    if isinstance(round_players, list):
                        for p in round_players:
                            if not isinstance(p, dict):
                                continue
                            pid = p.get("id") or p.get("playerId") or p.get("player_id")
                            op_name = _extract_operator_name(
                                p.get("operator")
                                or p.get("operatorName")
                                or p.get("operator_name")
                                or p.get("operatorData")
                                or p.get("operator_data")
                            )
                            if pid and op_name:
                                round_operator_by_id[str(pid)] = op_name
                    if isinstance(kill_events, list):
                        for ev in kill_events:
                            if not isinstance(ev, dict):
                                continue
                            if not ev.get("killerOperator"):
                                killer_id = ev.get("killerId") or ev.get("attackerId")
                                if killer_id:
                                    ev["killerOperator"] = round_operator_by_id.get(
                                        str(killer_id),
                                        player_operator_by_id.get(str(killer_id), ""),
                                    )
                            if not ev.get("victimOperator"):
                                victim_id = ev.get("victimId")
                                if victim_id:
                                    ev["victimOperator"] = round_operator_by_id.get(
                                        str(victim_id),
                                        player_operator_by_id.get(str(victim_id), ""),
                                    )
                    parsed.append(
                        {
                            "round_number": round_num,
                            "winner": (
                                rnd.get("winner")
                                or rnd.get("winningTeam")
                                or rnd.get("resultId")
                                or "unknown"
                            ),
                            "outcome": (
                                rnd.get("roundOutcome")
                                or rnd.get("outcome")
                                or rnd.get("outcomeId")
                                or rnd.get("resultId")
                                or "unknown"
                            ),
                            "kill_events": kill_events,
                            "players": round_players if isinstance(round_players, list) else [],
                        }
                    )
                return parsed

            def _enrich_round_operators_from_match_players(match_data: dict) -> None:
                rounds = match_data.get("rounds", [])
                players = match_data.get("players", [])
                if not isinstance(rounds, list) or not isinstance(players, list):
                    return

                operator_by_username = {}
                for player in players:
                    if not isinstance(player, dict):
                        continue
                    username = str(player.get("username") or "").strip().lower()
                    if not username:
                        continue
                    operators = player.get("operators")
                    if isinstance(operators, list):
                        for op in operators:
                            op_name = _extract_operator_name(op)
                            if op_name:
                                operator_by_username[username] = op_name
                                break

                if not operator_by_username:
                    return

                for rnd in rounds:
                    if not isinstance(rnd, dict):
                        continue
                    events = rnd.get("kill_events")
                    if not isinstance(events, list):
                        continue
                    for ev in events:
                        if not isinstance(ev, dict):
                            continue
                        if not ev.get("killerOperator"):
                            killer_name = str(ev.get("killerName") or "").strip().lower()
                            if killer_name and killer_name in operator_by_username:
                                ev["killerOperator"] = operator_by_username[killer_name]
                        if not ev.get("victimOperator"):
                            victim_name = str(ev.get("victimName") or "").strip().lower()
                            if victim_name and victim_name in operator_by_username:
                                ev["victimOperator"] = operator_by_username[victim_name]

            def _team_score(team: dict) -> int:
                if not isinstance(team, dict):
                    return 0
                direct_score = team.get("score")
                if isinstance(direct_score, (int, float)):
                    return int(direct_score)
                stats = team.get("stats", {})
                if not isinstance(stats, dict):
                    return 0
                for key in ("score", "roundsWon", "rounds_won"):
                    value = stats.get(key)
                    if isinstance(value, (int, float)):
                        return int(value)
                    if isinstance(value, dict):
                        inner = value.get("value")
                        if isinstance(inner, (int, float)):
                            return int(inner)
                return 0

            def _stat_value(segment: dict, key: str) -> int | None:
                stats = segment.get("stats", {}) if isinstance(segment, dict) else {}
                if not isinstance(stats, dict):
                    return None
                raw = stats.get(key)
                if isinstance(raw, (int, float)):
                    return int(raw)
                if isinstance(raw, dict):
                    value = raw.get("value")
                    if isinstance(value, (int, float)):
                        return int(value)
                return None

            def _user_round_score(summary_data: dict, target_username: str) -> tuple[int | None, int | None]:
                if not isinstance(summary_data, dict):
                    return (None, None)
                segments = summary_data.get("segments", [])
                if not isinstance(segments, list):
                    return (None, None)
                normalized = (target_username or "").strip().lower()
                if not normalized:
                    return (None, None)
                for seg in segments:
                    if not isinstance(seg, dict):
                        continue
                    metadata = seg.get("metadata", {})
                    if not isinstance(metadata, dict):
                        continue
                    handle = (
                        metadata.get("platformUserHandle")
                        or metadata.get("name")
                        or metadata.get("username")
                        or ""
                    )
                    if str(handle).strip().lower() != normalized:
                        continue
                    won = _stat_value(seg, "roundsWon")
                    lost = _stat_value(seg, "roundsLost")
                    return (won, lost)
                return (None, None)

            # r6.tracker.network no longer uses <a href="/matches/..."> links.
            # Match rows are click-handler divs with no href. We count the
            # .v3-match-row elements directly and drive everything by index.
            match_targets = await page.query_selector_all(".v3-match-row")
            if not match_targets:
                await websocket.send_json(
                    {
                        "type": "error",
                        "message": "No matches found on matches page (selector mismatch or blocked page).",
                    }
                )
                return

            matches_data = []
            existing_match_ids = set()
            fully_scraped_match_ids = set()
            try:
                existing_match_ids = db.get_existing_scraped_match_ids(username)
                fully_scraped_match_ids = db.get_fully_scraped_match_ids(username)
                await websocket.send_json(
                    {
                        "type": "debug",
                        "message": (
                            f"Loaded {len(existing_match_ids)} existing stored match IDs and "
                            f"{len(fully_scraped_match_ids)} fully scraped match IDs for {username}"
                        ),
                    }
                )
            except Exception as e:
                await websocket.send_json(
                    {
                        "type": "warning",
                        "message": f"Could not load existing stored matches: {str(e)}",
                    }
                )
            # Pre-filter unavailable/rollback rows before the loop.
            available_match_indexes = []
            for idx, row in enumerate(match_targets):
                cls = await row.get_attribute("class") or ""
                if "v3-match-row--unavailable" in cls:
                    continue
                available_match_indexes.append(idx)

            await websocket.send_json(
                {
                    "type": "debug",
                    "message": (
                        f"Found {len(match_targets)} rows, "
                        f"{len(available_match_indexes)} available after filtering"
                    ),
                }
            )

            if full_backfill:
                candidate_match_indexes = available_match_indexes
                checkpoint_mode_key = "full_backfill"
                checkpoint_filter_key = ",".join(sorted(allowed_types_norm)) if allowed_types_norm else "*"
                resume_skip_remaining = 0
                try:
                    checkpoint_seed = db.get_scrape_checkpoint_skip_count(
                        username,
                        checkpoint_mode_key,
                        checkpoint_filter_key,
                    )
                    if checkpoint_seed > 0:
                        resume_skip_remaining = checkpoint_seed
                    else:
                        resume_skip_remaining = db.count_fully_scraped_match_ids(username, allowed_types_norm)
                except Exception:
                    resume_skip_remaining = 0
                resume_skip_checkpoint = resume_skip_remaining
                await websocket.send_json(
                    {
                        "type": "debug",
                        "message": (
                            "Full backfill mode enabled: scanning all available rows and "
                            "loading until Load More is exhausted."
                        ),
                    }
                )
                if resume_skip_remaining > 0:
                    await websocket.send_json(
                        {
                            "type": "debug",
                            "message": (
                                "Full backfill resume optimization: skipping first "
                                f"{resume_skip_remaining} previously validated rows for this filter."
                            ),
                        }
                    )
            elif newest_only:
                candidate_match_indexes = available_match_indexes
                await websocket.send_json(
                    {
                        "type": "debug",
                        "message": (
                            "Newest-only mode enabled: scanning from newest to oldest and "
                            "stopping at first already-stored match ID."
                        ),
                    }
                )
            else:
                # Fast path: newest rows are usually already scraped.
                # Skip an initial window equal to known stored IDs to avoid needless clicks.
                skip_count = min(len(existing_match_ids), len(available_match_indexes))
                candidate_match_indexes = available_match_indexes[skip_count:]
                if not candidate_match_indexes:
                    candidate_match_indexes = available_match_indexes
                if skip_count > 0:
                    await websocket.send_json(
                        {
                            "type": "debug",
                            "message": (
                                f"Fast-skip enabled: skipping first {skip_count} rows based on "
                                f"{len(existing_match_ids)} stored IDs"
                            ),
                        }
                    )

            target_new_matches = max(1, int(max_matches))
            newly_captured = 0
            consecutive_dupes = 0
            rows_scanned = 0
            discovered_ids = set()
            matches_backfill = []
            seen_row_indexes = set()
            row_match_id_cache = {}

            async def _click_load_more_if_available(previous_count: int) -> bool:
                await _ensure_match_list_state()
                selectors = [
                    page.get_by_role("button", name=re.compile(r"load more", re.I)).first,
                    page.locator("button:has-text('Load More')").first,
                    page.locator(".v3-button:has-text('Load More')").first,
                ]
                for attempt in range(1, 4):
                    try:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    except Exception:
                        pass
                    await page.wait_for_timeout(random.randint(300, 700))

                    clicked = False
                    click_error = ""
                    for locator in selectors:
                        try:
                            if await locator.count() == 0:
                                continue
                            await locator.scroll_into_view_if_needed()
                            await page.wait_for_timeout(random.randint(120, 260))
                            await locator.click(timeout=3500)
                            clicked = True
                            break
                        except Exception as e:
                            click_error = str(e)
                            continue

                    if not clicked:
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    f"Load More attempt {attempt}/3: button not clickable"
                                    + (f" ({click_error})" if click_error else "")
                                ),
                            }
                        )
                        continue

                    await websocket.send_json(
                        {
                            "type": "debug",
                            "message": f"Clicked Load More (attempt {attempt}/3)",
                        }
                    )
                    for _ in range(24):
                        await page.wait_for_timeout(300)
                        try:
                            current_rows = await page.query_selector_all(".v3-match-row")
                        except Exception:
                            current_rows = []
                        if len(current_rows) > previous_count:
                            await websocket.send_json(
                                {
                                    "type": "debug",
                                    "message": (
                                        f"Load More succeeded: rows {previous_count} -> "
                                        f"{len(current_rows)}"
                                    ),
                                }
                            )
                            return True

                    await websocket.send_json(
                        {
                            "type": "debug",
                            "message": (
                                f"Load More attempt {attempt}/3 clicked but row count stayed at "
                                f"{previous_count}"
                            ),
                        }
                    )
                await websocket.send_json(
                    {
                        "type": "debug",
                        "message": "Load More unavailable or exhausted; no additional rows loaded",
                    }
                )
                return False

            async def _ensure_match_list_state() -> None:
                # If detail view navigated to a dedicated page, go back first.
                if page.url != url:
                    try:
                        await page.go_back(wait_until="domcontentloaded", timeout=20000)
                        await page.wait_for_timeout(random.randint(500, 900))
                    except Exception:
                        pass

                # If details are inline, close/dismiss them so row clicks and Load More are not blocked.
                close_selectors = [
                    page.get_by_role("button", name=re.compile(r"close|back", re.I)).first,
                    page.locator("button:has-text('Close')").first,
                    page.locator("button:has-text('Back')").first,
                    page.locator("[aria-label*='Close']").first,
                    page.locator("[class*='close']").first,
                ]
                for _ in range(2):
                    closed = False
                    for locator in close_selectors:
                        try:
                            if await locator.count() == 0:
                                continue
                            if not await locator.is_visible():
                                continue
                            await locator.click(timeout=2000)
                            await page.wait_for_timeout(random.randint(250, 450))
                            closed = True
                            break
                        except Exception:
                            continue
                    if closed:
                        continue
                    try:
                        await page.keyboard.press("Escape")
                        await page.wait_for_timeout(random.randint(220, 420))
                    except Exception:
                        pass

                # Re-anchor on match list surface.
                try:
                    matches_link = page.get_by_role("link", name="Matches", exact=True)
                    if await matches_link.count() > 0:
                        await matches_link.click(timeout=2000)
                        await page.wait_for_timeout(random.randint(350, 700))
                except Exception:
                    pass
                try:
                    await page.wait_for_selector(".v3-match-row", timeout=8000, state="visible")
                except Exception:
                    pass

            async def _probe_row_match_id(row_index: int) -> str:
                """
                Open a row briefly and capture only the match ID from API traffic.
                Used for chunk-boundary checks when IDs are not present in DOM.
                """
                await _ensure_match_list_state()
                match_elements = await page.query_selector_all(".v3-match-row")
                if row_index < 0 or row_index >= len(match_elements):
                    return ""

                captured_mid = ""

                async def _capture_probe_response(response):
                    nonlocal captured_mid
                    if captured_mid:
                        return
                    response_url = response.url
                    if "/api/v2/r6siege/standard/matches/" not in response_url:
                        return
                    m = re.search(r"/matches/([0-9a-fA-F-]{36})", response_url)
                    if m:
                        captured_mid = m.group(1)

                page.on("response", _capture_probe_response)
                try:
                    row = match_elements[row_index]
                    await row.scroll_into_view_if_needed()
                    await page.wait_for_timeout(random.randint(200, 420))
                    await row.evaluate("element => element.click()")
                    for _ in range(16):
                        if captured_mid:
                            break
                        await asyncio.sleep(0.35)
                except Exception:
                    pass
                finally:
                    try:
                        page.remove_listener("response", _capture_probe_response)
                    except Exception:
                        pass
                    await _ensure_match_list_state()

                return captured_mid

            await websocket.send_json(
                {
                    "type": "debug",
                    "message": (
                        f"Targeting up to {target_new_matches} new matches "
                        f"(mode={'full_backfill' if full_backfill else ('newest' if newest_only else 'standard')})"
                    ),
                }
            )

            cursor = 0
            chunk_size = 24
            if not full_backfill:
                resume_skip_remaining = 0
                resume_skip_checkpoint = 0
                checkpoint_mode_key = ""
                checkpoint_filter_key = ""
            while True:
                if stop_event and stop_event.is_set():
                    await websocket.send_json(
                        {
                            "type": "debug",
                            "message": "Graceful stop: current match completed, exiting before next match.",
                        }
                    )
                    break
                if not full_backfill and newly_captured >= target_new_matches:
                    break
                await _ensure_match_list_state()
                if cursor >= len(candidate_match_indexes):
                    previous_count = len(match_targets)
                    did_expand = await _click_load_more_if_available(previous_count)
                    if not did_expand:
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": "No more rows to scan; stopping match scrape loop.",
                            }
                        )
                        break
                    match_targets = await page.query_selector_all(".v3-match-row")
                    available_match_indexes = []
                    new_available_match_indexes = []
                    for idx, row in enumerate(match_targets):
                        cls = await row.get_attribute("class") or ""
                        if "v3-match-row--unavailable" in cls:
                            continue
                        available_match_indexes.append(idx)
                        if idx not in seen_row_indexes:
                            new_available_match_indexes.append(idx)
                    candidate_match_indexes = new_available_match_indexes
                    cursor = 0
                    continue
                if full_backfill and resume_skip_remaining > 0 and cursor < len(candidate_match_indexes):
                    skip_now = min(resume_skip_remaining, len(candidate_match_indexes) - cursor)
                    skipped_slice = candidate_match_indexes[cursor : cursor + skip_now]
                    if skipped_slice:
                        seen_row_indexes.update(skipped_slice)
                    cursor += skip_now
                    resume_skip_remaining -= skip_now
                    await websocket.send_json(
                        {
                            "type": "debug",
                            "message": (
                                f"Full backfill resume: fast-skipped {skip_now} already-complete rows "
                                f"(remaining skip={resume_skip_remaining})."
                            ),
                        }
                    )
                    continue

                # Startup fast path: click row 1 immediately, then boundary-check row 24
                # so users don't wait on probes before the first visible action.
                if newest_only and cursor == 1:
                    probe_rows = await page.query_selector_all(".v3-match-row")
                    card_start = 0
                    card_end = chunk_size - 1
                    if card_end < len(probe_rows):
                        first_id = row_match_id_cache.get(card_start, "")
                        if not first_id:
                            first_id = await _extract_row_match_id_from_dom(probe_rows[card_start])
                        last_id = await _extract_row_match_id_from_dom(probe_rows[card_end])
                        used_api_probe = False
                        if not last_id:
                            last_id = await _probe_row_match_id(card_end)
                            used_api_probe = used_api_probe or bool(last_id)
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    "Chunk boundary probe: "
                                    f"cards {card_start + 1}-{card_end + 1}, "
                                    f"first_id={'yes' if first_id else 'no'}, "
                                    f"last_id={'yes' if last_id else 'no'}, "
                                    f"source={'api' if used_api_probe else 'dom'}"
                                ),
                            }
                        )
                        if (
                            first_id
                            and last_id
                            and first_id in fully_scraped_match_ids
                            and last_id in fully_scraped_match_ids
                        ):
                            # Row 1 already processed; skip directly to next page boundary.
                            seen_row_indexes.update(range(card_start + 1, card_end + 1))
                            cursor = chunk_size
                            await websocket.send_json(
                                {
                                    "type": "debug",
                                    "message": (
                                        "Chunk skip optimization: first and 24th card are fully stored; "
                                        "jumping to Load More."
                                    ),
                                }
                            )
                            continue

                # Chunk skip is based on rendered cards (24 cards per page), not "available match" count.
                if newest_only and rows_scanned > 0 and (cursor % chunk_size == 0):
                    probe_rows = await page.query_selector_all(".v3-match-row")
                    card_start = cursor
                    card_end = cursor + chunk_size - 1
                    if card_end < len(probe_rows):
                        first_id = await _extract_row_match_id_from_dom(probe_rows[card_start])
                        last_id = await _extract_row_match_id_from_dom(probe_rows[card_end])
                        used_api_probe = False
                        if not first_id:
                            first_id = await _probe_row_match_id(card_start)
                            used_api_probe = used_api_probe or bool(first_id)
                        if not last_id:
                            last_id = await _probe_row_match_id(card_end)
                            used_api_probe = used_api_probe or bool(last_id)
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    "Chunk boundary probe: "
                                    f"cards {card_start + 1}-{card_end + 1}, "
                                    f"first_id={'yes' if first_id else 'no'}, "
                                    f"last_id={'yes' if last_id else 'no'}, "
                                    f"source={'api' if used_api_probe else 'dom'}"
                                ),
                            }
                        )
                        if (
                            first_id
                            and last_id
                            and first_id in fully_scraped_match_ids
                            and last_id in fully_scraped_match_ids
                        ):
                            seen_row_indexes.update(range(card_start, card_end + 1))
                            cursor += chunk_size
                            await websocket.send_json(
                                {
                                    "type": "debug",
                                    "message": (
                                        "Chunk skip optimization: 24 rendered cards in this block appear fully stored; "
                                        f"skipping cards {card_start + 1}-{card_end + 1}."
                                    ),
                                }
                            )
                            continue

                row_index = candidate_match_indexes[cursor]
                cursor += 1
                if row_index in seen_row_indexes:
                    continue
                seen_row_indexes.add(row_index)
                rows_scanned += 1
                opened_detail = False
                try:
                    await websocket.send_json(
                        {
                            "type": "scraping_match",
                            "match_number": rows_scanned,
                            "new_matches": len(matches_data),
                            "total": target_new_matches,
                        }
                    )

                    # Stay on the matches page between iterations; do not reload.
                    try:
                        await page.wait_for_selector(".v3-match-row", timeout=10000, state="visible")
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": "Match rows loaded and visible",
                            }
                        )
                    except Exception as e:
                        await websocket.send_json(
                            {
                                "type": "error",
                                "message": f"Match rows failed to load: {str(e)}",
                            }
                        )
                        continue
                    await page.wait_for_timeout(800)
                    match_cards = await page.query_selector_all(".v3-match-row")
                    await websocket.send_json(
                        {
                            "type": "debug",
                            "message": f"Found {len(match_cards)} match cards",
                        }
                    )
                    if len(match_cards) == 0:
                        await websocket.send_json(
                            {
                                "type": "error",
                                "message": "No matches found (even after waiting)",
                            }
                        )
                        continue
                    # Re-query rows each iteration (page reloads between matches)
                    match_elements = await page.query_selector_all(".v3-match-row")
                    if row_index >= len(match_elements):
                        did_expand = await _click_load_more_if_available(len(match_elements))
                        if did_expand:
                            match_elements = await page.query_selector_all(".v3-match-row")
                        if row_index >= len(match_elements):
                            await websocket.send_json(
                                {
                                    "type": "warning",
                                    "message": f"Match row index {row_index + 1} no longer available on page.",
                                }
                            )
                            continue

                    if full_backfill:
                        quick_row_match_id = row_match_id_cache.get(row_index, "")
                        if not quick_row_match_id:
                            try:
                                quick_row_match_id = await _extract_row_match_id_from_dom(match_elements[row_index])
                            except Exception:
                                quick_row_match_id = ""
                            if quick_row_match_id:
                                row_match_id_cache[row_index] = quick_row_match_id
                        if quick_row_match_id and quick_row_match_id in fully_scraped_match_ids:
                            if resume_skip_remaining == 0:
                                resume_skip_checkpoint += 1
                            await websocket.send_json(
                                {
                                    "type": "match_seen",
                                    "status": "skipped_complete",
                                    "match_data": {
                                        "match_id": quick_row_match_id,
                                        "map": "",
                                        "mode": "",
                                        "score_team_a": 0,
                                        "score_team_b": 0,
                                        "duration": "",
                                        "date": "",
                                    },
                                }
                            )
                            continue

                    # Match ID is unknown until the API fires — start empty
                    current_match_id = ""
                    captured_api = {}

                    async def _capture_response(response):
                        response_url = response.url
                        if "/api/v2/r6siege/standard/matches/" in response_url:
                            if "match_summary" in captured_api:
                                return
                            try:
                                captured_api["match_summary"] = await response.json()
                                # Extract match ID from URL now that we have it
                                m = re.search(r"/matches/([0-9a-fA-F-]{36})", response_url)
                                if m:
                                    captured_api["_match_id"] = m.group(1)
                                await websocket.send_json(
                                    {
                                        "type": "debug",
                                        "message": f"Captured match_summary API ({response.status})",
                                    }
                                )
                            except Exception as e:
                                await websocket.send_json(
                                    {
                                        "type": "warning",
                                        "message": f"Failed to parse match_summary API: {str(e)}",
                                    }
                                )

                    page.on("response", _capture_response)
                    try:
                        # Rows are click-handler divs — click by index directly
                        match_card = match_elements[row_index]

                        await match_card.scroll_into_view_if_needed()
                        await page.wait_for_timeout(500)
                        await match_card.evaluate("element => element.click()")
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": f"Clicked match row {rows_scanned}",
                            }
                        )
                        # Wait for the detail panel to open
                        try:
                            await page.wait_for_selector("text=/Team A|Team B/", timeout=7000)
                            opened_detail = True
                            # Grab match ID from URL if page navigated
                            current_match_id = _extract_match_id(page.url)
                        except Exception:
                            opened_detail = True  # Panel may be inline; proceed anyway

                        await page.wait_for_timeout(800)
                        # Only summary capture is required for this pipeline.
                        for _ in range(8):
                            if "match_summary" in captured_api:
                                break
                            await asyncio.sleep(0.25)
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    f"API capture status for match {rows_scanned}: "
                                    f"summary={'yes' if 'match_summary' in captured_api else 'no'}, "
                                    "rounds=skipped (summary-first mode)"
                                ),
                            }
                        )
                    except Exception as e:
                        await websocket.send_json(
                            {
                                "type": "warning",
                                "message": f"Error while opening/capturing match {rows_scanned}: {str(e)}",
                            }
                        )
                        continue
                    finally:
                        try:
                            page.remove_listener("response", _capture_response)
                        except Exception:
                            pass

                    match_data = {
                        "match_id": current_match_id,
                        "map": "",
                        "mode": "",
                        "score_team_a": 0,
                        "score_team_b": 0,
                        "duration": "",
                        "date": "",
                        "players": [],
                        "match_summary": captured_api.get("match_summary"),
                        "round_data": {},
                        "rounds": [],
                        "partial_capture": False,
                        "partial_reason": "",
                    }

                    summary_payload = match_data.get("match_summary") or {}
                    summary_data = summary_payload.get("data", {}) if isinstance(summary_payload, dict) else {}
                    metadata = {}
                    variant_match_ids = set()
                    if isinstance(summary_data, dict):
                        metadata = summary_data.get("metadata", {})
                        if isinstance(metadata, dict):
                            if not current_match_id:
                                current_match_id = captured_api.get("_match_id", "") or current_match_id
                            if not current_match_id:
                                try:
                                    variants = metadata.get("overwolfMatchVariants", [])
                                    if variants:
                                        current_match_id = variants[0].get("matchId", "")
                                except Exception:
                                    pass
                            try:
                                variants = metadata.get("overwolfMatchVariants", [])
                                for v in variants:
                                    mid = (v.get("matchId", "") if isinstance(v, dict) else "").strip()
                                    if mid:
                                        variant_match_ids.add(mid)
                            except Exception:
                                pass
                            match_data["match_id"] = current_match_id or match_data["match_id"]

                            if not match_data["map"]:
                                match_data["map"] = (
                                    metadata.get("sessionMapName")
                                    or metadata.get("mapName")
                                    or metadata.get("map")
                                    or match_data["map"]
                                )
                            if not match_data["mode"]:
                                match_data["mode"] = (
                                    metadata.get("sessionTypeName")
                                    or metadata.get("sessionGameModeName")
                                    or metadata.get("playlistName")
                                    or metadata.get("gamemode")
                                    or match_data["mode"]
                                )
                            if not match_data["date"]:
                                match_data["date"] = (
                                    metadata.get("timestamp")
                                    or metadata.get("date")
                                    or match_data["date"]
                                )

                        teams = summary_data.get("teams", [])
                        if isinstance(teams, list) and len(teams) >= 2:
                            if not match_data["score_team_a"]:
                                match_data["score_team_a"] = _team_score(teams[0])
                            if not match_data["score_team_b"]:
                                match_data["score_team_b"] = _team_score(teams[1])

                        user_won, user_lost = _user_round_score(summary_data, username)
                        if isinstance(user_won, int) and isinstance(user_lost, int):
                            match_data["score_team_a"] = user_won
                            match_data["score_team_b"] = user_lost

                    match_id = (match_data.get("match_id") or "").strip()
                    if match_id:
                        row_match_id_cache[row_index] = match_id

                    is_known_pre = bool(match_id and (match_id in existing_match_ids or match_id in discovered_ids))
                    is_complete_pre = bool(match_id and match_id in fully_scraped_match_ids)
                    if full_backfill and is_known_pre and is_complete_pre:
                        if resume_skip_remaining == 0:
                            resume_skip_checkpoint += 1
                        if match_id:
                            discovered_ids.add(match_id)
                        discovered_ids.update(variant_match_ids)
                        await websocket.send_json(
                            {
                                "type": "match_seen",
                                "status": "skipped_complete",
                                "match_data": match_data,
                            }
                        )
                        continue

                    summary_present = bool(match_data.get("match_summary"))
                    rounds_present = False

                    is_known = bool(match_id and (match_id in existing_match_ids or match_id in discovered_ids))
                    is_complete = bool(match_id and match_id in fully_scraped_match_ids)
                    if full_backfill and is_known and is_complete:
                        discovered_ids.update(variant_match_ids)
                        await websocket.send_json(
                            {
                                "type": "match_seen",
                                "status": "skipped_complete",
                                "match_data": match_data,
                            }
                        )
                        continue
                    if is_known and is_complete:
                        consecutive_dupes += 1
                        discovered_ids.update(variant_match_ids)
                        if summary_present or rounds_present:
                            matches_backfill.append(match_data)
                            await websocket.send_json(
                                {
                                    "type": "debug",
                                    "message": f"Queued duplicate {match_id} for metadata/round backfill.",
                                }
                            )
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    f"Skipping already-stored/seen fully-scraped match {match_id} "
                                    f"(dupe streak={consecutive_dupes})"
                                ),
                            }
                        )
                        if newest_only:
                            await websocket.send_json(
                                {
                                    "type": "debug",
                                    "message": (
                                        f"Newest-only: encountered fully-scraped stored match {match_id}; "
                                        "continuing scan for additional unseen rows."
                                    ),
                                }
                            )
                        if consecutive_dupes >= 5:
                            if newest_only:
                                await websocket.send_json(
                                    {
                                        "type": "debug",
                                        "message": (
                                            "5 consecutive already-stored matches in newest-only mode; "
                                            "boundary found, stopping."
                                        ),
                                    }
                                )
                                break
                            await websocket.send_json(
                                {
                                    "type": "debug",
                                    "message": "5 consecutive already-stored matches, continuing to next rows",
                                }
                            )
                            consecutive_dupes = 0
                        continue
                    if is_known and not is_complete:
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    f"Known match {match_id} has partial/missing stored data; "
                                    "re-scraping to fill gaps."
                                ),
                            }
                        )

                    consecutive_dupes = 0
                    if match_id:
                        discovered_ids.add(match_id)
                    discovered_ids.update(variant_match_ids)

                    try:
                        map_elem = await page.query_selector('[class*="map-name"]')
                        if map_elem:
                            match_data["map"] = (await map_elem.inner_text()).strip()
                    except Exception:
                        pass

                    mode_key = _normalize_match_type(match_data.get("mode"))
                    if (newest_only or full_backfill) and allowed_types_norm and mode_key not in allowed_types_norm:
                        await websocket.send_json(
                            {
                                "type": "match_seen",
                                "status": "filtered",
                                "match_data": match_data,
                            }
                        )
                        await websocket.send_json(
                            {
                                "type": "match_filtered",
                                "mode": match_data.get("mode") or "Unknown",
                                "match_id": match_id,
                            }
                        )
                        continue

                    try:
                        # Only use DOM score as fallback when API score parsing failed.
                        if not match_data["score_team_a"] and not match_data["score_team_b"]:
                            score_text = await page.query_selector("text=/[0-9] : [0-9]/")
                            if score_text:
                                scores = await score_text.inner_text()
                                parts = scores.split(":")
                                if len(parts) == 2:
                                    match_data["score_team_a"] = int(parts[0].strip())
                                    match_data["score_team_b"] = int(parts[1].strip())
                    except Exception:
                        pass

                    if not summary_present:
                        reasons = ["summary_missing"]
                        match_data["partial_capture"] = True
                        match_data["partial_reason"] = ", ".join(reasons)

                    try:
                        player_rows = await page.query_selector_all('tr[class*="group/row"]')
                        current_team = None

                        for row in player_rows:
                            try:
                                team_header = await row.query_selector("text=/Team [AB]/")
                                if team_header:
                                    team_text = await team_header.inner_text()
                                    current_team = "A" if "Team A" in team_text else "B"
                                    continue

                                cells = await row.query_selector_all("td")
                                if len(cells) < 8:
                                    continue

                                player_data = {
                                    "team": current_team,
                                    "username": "",
                                    "rank_points": 0,
                                    "kd": 0.0,
                                    "kills": 0,
                                    "deaths": 0,
                                    "assists": 0,
                                    "hs_percent": 0.0,
                                    "operators": [],
                                }

                                name_elem = await cells[0].query_selector("a, span")
                                if name_elem:
                                    player_data["username"] = (await name_elem.inner_text()).strip()

                                try:
                                    rp_text = await cells[1].inner_text()
                                    player_data["rank_points"] = int(rp_text.replace(",", "").strip())
                                except Exception:
                                    pass

                                try:
                                    kd_text = await cells[2].inner_text()
                                    player_data["kd"] = float(kd_text.strip())
                                except Exception:
                                    pass

                                try:
                                    k_text = await cells[3].inner_text()
                                    player_data["kills"] = int(k_text.strip())
                                except Exception:
                                    pass

                                try:
                                    d_text = await cells[4].inner_text()
                                    player_data["deaths"] = int(d_text.strip())
                                except Exception:
                                    pass

                                try:
                                    a_text = await cells[5].inner_text()
                                    player_data["assists"] = int(a_text.strip())
                                except Exception:
                                    pass

                                try:
                                    hs_text = await cells[6].inner_text()
                                    player_data["hs_percent"] = float(hs_text.replace("%", "").strip())
                                except Exception:
                                    pass

                                try:
                                    op_imgs = await cells[-1].query_selector_all("img")
                                    for img in op_imgs:
                                        op_name = await img.get_attribute("alt")
                                        if op_name:
                                            player_data["operators"].append(op_name)
                                except Exception:
                                    pass

                                match_data["players"].append(player_data)
                            except Exception:
                                continue
                    except Exception as e:
                        await websocket.send_json(
                            {
                                "type": "warning",
                                "message": f"Error extracting players: {str(e)}",
                            }
                        )

                    _enrich_round_operators_from_match_players(match_data)
                    matches_data.append(match_data)
                    newly_captured += 1
                    if match_id:
                        existing_match_ids.add(match_id)

                    await websocket.send_json(
                        {
                            "type": "match_scraped",
                            "match_data": match_data,
                        }
                    )
                    if match_data.get("partial_capture"):
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    f"Partial capture queued for unpack/recovery for match "
                                    f"{match_data.get('match_id') or rows_scanned}: {match_data.get('partial_reason')}"
                                ),
                            }
                        )

                except Exception as e:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "message": f"Error scraping match {rows_scanned}: {str(e)}",
                        }
                    )
                    continue
                finally:
                    if opened_detail:
                        try:
                            await _ensure_match_list_state()
                        except Exception as nav_err:
                            await websocket.send_json(
                                {
                                    "type": "warning",
                                    "message": f"Failed to reset matches list state: {str(nav_err)}",
                                }
                            )

            await websocket.send_json(
                {
                    "type": "match_scraping_complete",
                    "total_matches": len(matches_data),
                    "rows_scanned": rows_scanned,
                }
            )

            try:
                save_payload = list(matches_data)
                save_payload.extend(matches_backfill)
                db.save_scraped_match_cards(username, save_payload)
                unpack_stats = db.unpack_pending_scraped_match_cards(username=username, limit=5000)
                await websocket.send_json(
                    {
                        "type": "matches_saved",
                        "username": username,
                        "saved_matches": len(matches_data),
                        "backfilled_matches": len(matches_backfill),
                    }
                )
                await websocket.send_json(
                    {
                        "type": "matches_unpacked",
                        "username": username,
                        "stats": unpack_stats,
                    }
                )
                if full_backfill:
                    try:
                        db.set_scrape_checkpoint_skip_count(
                            username,
                            checkpoint_mode_key,
                            checkpoint_filter_key,
                            resume_skip_checkpoint,
                        )
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    "Full backfill checkpoint updated: "
                                    f"skip_count={resume_skip_checkpoint} "
                                    f"(filter={checkpoint_filter_key})"
                                ),
                            }
                        )
                    except Exception as checkpoint_err:
                        await websocket.send_json(
                            {
                                "type": "warning",
                                "message": f"Failed to persist full backfill checkpoint: {checkpoint_err}",
                            }
                        )
            except Exception as e:
                await websocket.send_json(
                    {
                        "type": "warning",
                        "message": f"Failed to save scraped matches: {str(e)}",
                    }
                )

        finally:
            if browser:
                await browser.close()


@app.websocket("/ws/scrape-matches")
async def scrape_matches(websocket: WebSocket) -> None:
    await websocket.accept()
    stop_event = asyncio.Event()
    control_task = None

    try:
        data = await websocket.receive_json()
        username = data.get("username")
        max_matches = data.get("max_matches", 10)
        debug_browser = bool(data.get("debug_browser", False))
        newest_only = bool(data.get("newest_only", False))
        full_backfill = bool(data.get("full_backfill", False))
        allowed_match_types = data.get("allowed_match_types", [])
        if not isinstance(allowed_match_types, list):
            allowed_match_types = []

        async def _control_loop() -> None:
            while True:
                msg = await websocket.receive_json()
                action = str((msg or {}).get("action") or "").strip().lower()
                if action == "stop":
                    stop_event.set()
                    await websocket.send_json(
                        {
                            "type": "stop_ack",
                            "message": "Stop requested. Finishing current match before stopping.",
                        }
                    )

        control_task = asyncio.create_task(_control_loop())
        await scrape_match_history(
            websocket,
            username,
            max_matches,
            debug_browser,
            newest_only=newest_only,
            full_backfill=full_backfill,
            allowed_match_types=allowed_match_types,
            stop_event=stop_event,
        )
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        await websocket.send_json(
            {
                "type": "error",
                "message": str(e),
            }
        )
    finally:
        if control_task:
            control_task.cancel()


if __name__ == "__main__":
    import uvicorn

    print("Starting JAKAL Web Server...")
    print("Open http://localhost:5000 in your browser")
    uvicorn.run(app, host="127.0.0.1", port=5000)
