from collections import deque
import random
import re
import traceback

from fastapi import WebSocket, WebSocketDisconnect
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

error_tracker = None
track_call = None
get_rate_status = None


def configure_network_scan(*, error_tracker_dep, track_call_dep, get_rate_status_dep) -> None:
    global error_tracker, track_call, get_rate_status
    error_tracker = error_tracker_dep
    track_call = track_call_dep
    get_rate_status = get_rate_status_dep


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


def register_network_scan_routes(app) -> None:
    app.websocket("/ws/scan")(scan_network)
