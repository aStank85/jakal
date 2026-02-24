from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import asyncio
from collections import deque
import os
import random
import re
import sys
import time
import traceback

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api_client import TrackerAPIClient
from src.database import Database

app = FastAPI()
app.mount("/static", StaticFiles(directory="web/static"), name="static")
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _normalize_asset_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


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
        return HTMLResponse(content=f.read())


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
    safe_limit = max(1, min(limit, 200))
    try:
        matches = db.get_scraped_match_cards(username, safe_limit)
        return {"username": username, "matches": matches, "count": len(matches)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load saved matches: {str(e)}")


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
) -> None:
    """
    Scrape detailed match history for a player.
    """
    browser = None

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
            try:
                existing_match_ids = db.get_existing_scraped_match_ids(username)
                await websocket.send_json(
                    {
                        "type": "debug",
                        "message": f"Loaded {len(existing_match_ids)} existing stored match IDs for {username}",
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
                try:
                    row_text = await row.inner_text()
                    if "Quick Match" in row_text or "Event" in row_text:
                        continue
                except Exception:
                    pass
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

            target_new_matches = max_matches
            newly_captured = 0
            consecutive_dupes = 0
            discovered_ids = set()
            await websocket.send_json(
                {
                    "type": "debug",
                    "message": (
                        f"Targeting {target_new_matches} new matches "
                        f"(early-stop after 5 consecutive stored/duplicate IDs)"
                    ),
                }
            )

            for i, row_index in enumerate(candidate_match_indexes):
                if newly_captured >= target_new_matches:
                    break
                opened_detail = False
                try:
                    await websocket.send_json(
                        {
                            "type": "scraping_match",
                            "match_number": len(matches_data) + 1,
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
                        await websocket.send_json(
                            {
                                "type": "warning",
                                "message": f"Match row index {row_index + 1} no longer available on page.",
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
                        elif "/api/v1/r6siege/ow-ingest/match/get/" in response_url:
                            if "round_data" in captured_api:
                                return
                            try:
                                captured_api["round_data"] = await response.json()
                                await websocket.send_json(
                                    {
                                        "type": "debug",
                                        "message": f"Captured round_data API ({response.status})",
                                    }
                                )
                            except Exception as e:
                                await websocket.send_json(
                                    {
                                        "type": "warning",
                                        "message": f"Failed to parse round_data API: {str(e)}",
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
                                "message": f"Clicked match row {i + 1}",
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
                        # Allow internal tracker API requests to fire and be intercepted.
                        for _ in range(20):
                            if "match_summary" in captured_api and "round_data" in captured_api:
                                break
                            await asyncio.sleep(0.5)
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    f"API capture status for match {i + 1}: "
                                    f"summary={'yes' if 'match_summary' in captured_api else 'no'}, "
                                    f"rounds={'yes' if 'round_data' in captured_api else 'no'}"
                                ),
                            }
                        )
                    except Exception as e:
                        await websocket.send_json(
                            {
                                "type": "warning",
                                "message": f"Error while opening/capturing match {i + 1}: {str(e)}",
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
                        "round_data": captured_api.get("round_data"),
                        "rounds": _parse_rounds(captured_api.get("round_data", {})),
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
                    if match_id and (match_id in existing_match_ids or match_id in discovered_ids):
                        consecutive_dupes += 1
                        discovered_ids.update(variant_match_ids)
                        await websocket.send_json(
                            {
                                "type": "debug",
                                "message": (
                                    f"Skipping already-stored/seen match {match_id} "
                                    f"(dupe streak={consecutive_dupes})"
                                ),
                            }
                        )
                        if consecutive_dupes >= 5:
                            await websocket.send_json(
                                {
                                    "type": "debug",
                                    "message": "5 consecutive already-stored matches, stopping early",
                                }
                            )
                            break
                        continue

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

                    summary_present = bool(match_data.get("match_summary"))
                    rounds_present = bool(match_data.get("round_data"))
                    if summary_present and not rounds_present:
                        reasons = ["round_data_missing"]
                        if isinstance(metadata, dict) and metadata.get("extendedDataAvailable") is False:
                            reasons.append("extendedDataAvailable=false")
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
                                "type": "warning",
                                "message": (
                                    f"Partial capture for match {match_data.get('match_id') or i + 1}: "
                                    f"{match_data.get('partial_reason')}"
                                ),
                            }
                        )

                except Exception as e:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "message": f"Error scraping match {i + 1}: {str(e)}",
                        }
                    )
                    continue
                finally:
                    if opened_detail and page.url != url:
                        try:
                            await page.go_back(wait_until="domcontentloaded", timeout=20000)
                            await page.wait_for_timeout(random.randint(800, 1200))
                        except Exception as nav_err:
                            await websocket.send_json(
                                {
                                    "type": "warning",
                                    "message": f"Failed to navigate back to matches list: {str(nav_err)}",
                                }
                            )

            await websocket.send_json(
                {
                    "type": "match_scraping_complete",
                    "total_matches": len(matches_data),
                }
            )

            try:
                db.save_scraped_match_cards(username, matches_data)
                await websocket.send_json(
                    {
                        "type": "matches_saved",
                        "username": username,
                        "saved_matches": len(matches_data),
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

    try:
        data = await websocket.receive_json()
        username = data.get("username")
        max_matches = data.get("max_matches", 10)
        debug_browser = bool(data.get("debug_browser", False))
        await scrape_match_history(websocket, username, max_matches, debug_browser)
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        await websocket.send_json(
            {
                "type": "error",
                "message": str(e),
            }
        )


if __name__ == "__main__":
    import uvicorn

    print("Starting JAKAL Web Server...")
    print("Open http://localhost:5000 in your browser")
    uvicorn.run(app, host="127.0.0.1", port=5000)
