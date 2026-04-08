"""
R6 Tracker - Match and Round Data Fetcher (Playwright)
=======================================================
Uses Playwright to drive a browser session on r6.tracker.network and intercept
the internal API calls fired when a match opens.
"""

import argparse
import asyncio
import json
from pathlib import Path
from urllib.parse import quote

from playwright.async_api import Page, Response, async_playwright


# Config
BASE_URL = "https://r6.tracker.network"
MATCH_LIST_URL = "{base}/r6siege/profile/ubi/{player}/matches?gamemode=pvp_ranked"

# API endpoints discovered from browser network traffic
MATCH_API_V2 = "https://api.tracker.gg/api/v2/r6siege/standard/matches/{match_id}"
ROUND_API_V1 = "https://api.tracker.gg/api/v1/r6siege/ow-ingest/match/get/{match_id}/{session_id}"

# Example IDs
EXAMPLE_MATCH_ID = "c0f92f5b-f82f-4b69-856c-b3e0ea590441"
EXAMPLE_SESSION_ID = "4845c941-217a-45a9-baa4-2787ac0d885d"


async def fetch_match_data(page: Page, match_id: str, player: str = "SaucedZyn") -> dict:
    """
    Navigate to match history, open one match, and intercept:
      - v2 match summary
      - v1 per-round detail
    """
    results: dict = {}

    async def handle_response(response: Response):
        url = response.url

        # Match summary endpoint (v2)
        if f"/matches/{match_id}" in url and "/v2/" in url:
            try:
                results["match_summary"] = await response.json()
                print(f"  [ok] match_summary captured (HTTP {response.status})")
            except Exception as exc:
                print(f"  [warn] Could not parse match_summary: {exc}")

        # Round detail endpoint (v1 ow-ingest)
        elif f"/match/get/{match_id}/" in url and "/v1/" in url:
            try:
                results["round_data"] = await response.json()
                print(f"  [ok] round_data captured   (HTTP {response.status})")
            except Exception as exc:
                print(f"  [warn] Could not parse round_data: {exc}")

    page.on("response", handle_response)

    encoded_player = quote(player, safe="")
    history_url = MATCH_LIST_URL.format(base=BASE_URL, player=encoded_player)
    print(f"\n  -> Navigating to match history: {history_url}")
    await page.goto(history_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(2500)

    match_link = page.locator(f'a[href*="{match_id}"]').first
    if await match_link.count() > 0:
        print("  -> Found target match row, clicking...")
        await match_link.click()
    else:
        print("  [warn] Match row not found by ID, clicking first available row...")
        fallback = page.locator(".v3-match-row, .match-row, [class*='match']:not(nav)").first
        await fallback.click()

    # Poll up to ~10s for both responses
    for _ in range(20):
        if "match_summary" in results and "round_data" in results:
            break
        await asyncio.sleep(0.5)

    if not results:
        print("  [warn] No API data captured; page structure may have changed.")

    page.remove_listener("response", handle_response)
    return results


async def fetch_player_matches(page: Page, player: str) -> list:
    """
    Intercept match-list API calls while loading a player's match history.
    Returns the raw match objects.
    """
    matches: list = []
    encoded_player = quote(player, safe="")

    async def handle_response(response: Response):
        url = response.url
        if "/v2/" in url and "matches" in url and encoded_player.lower() in url.lower():
            try:
                data = await response.json()
                raw = data.get("data", {}).get("matches", data.get("matches", []))
                matches.extend(raw)
                print(f"  [ok] Match list captured: {len(raw)} matches")
            except Exception as exc:
                print(f"  [warn] Could not parse match list: {exc}")

    page.on("response", handle_response)
    url = MATCH_LIST_URL.format(base=BASE_URL, player=encoded_player)
    print(f"\n  -> Loading profile: {url}")
    await page.goto(url, wait_until="networkidle")
    await page.wait_for_timeout(2000)
    page.remove_listener("response", handle_response)
    return matches


def parse_rounds(round_data: dict) -> list:
    """Normalize round_data JSON to a concise, consistent list."""
    rounds_raw = round_data.get("rounds") or round_data.get("data", {}).get("rounds") or []
    parsed = []

    for idx, rnd in enumerate(rounds_raw):
        parsed.append(
            {
                "round_number": idx + 1,
                "winner": rnd.get("winner") or rnd.get("winningTeam", "unknown"),
                "outcome": rnd.get("roundOutcome") or rnd.get("outcome", "unknown"),
                "kill_events": rnd.get("killEvents") or rnd.get("kills", []),
                "players": rnd.get("players", []),
            }
        )
    return parsed


def print_rounds(rounds: list) -> None:
    """Pretty-print round summaries."""
    print("\n" + "-" * 62)
    print(f"  ROUND BREAKDOWN ({len(rounds)} rounds total)")
    print("-" * 62)

    for rnd in rounds:
        kills = rnd["kill_events"]
        print(f"\n  Rnd {rnd['round_number']:>2} | Winner: {str(rnd['winner']):<12} | {rnd['outcome']}")
        for kill in kills:
            attacker = kill.get("attacker") or kill.get("killerName", "?")
            victim = kill.get("victim") or kill.get("victimName", "?")
            weapon = kill.get("weapon") or kill.get("weaponName", "unknown")
            print(f"      {attacker:<18} -> {victim:<18} [{weapon}]")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch R6 Tracker match and round data using Playwright")
    parser.add_argument("--match", type=str, help=f"Match UUID to fetch (example: {EXAMPLE_MATCH_ID})")
    parser.add_argument("--player", type=str, default="SaucedZyn", help="Ubisoft username (default: SaucedZyn)")
    parser.add_argument("--save", action="store_true", help="Write captured JSON to files")
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        default=True,
        help="Show browser window while running",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Max number of matches to process in --player mode (default: 5)",
    )
    args = parser.parse_args()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=args.headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = await context.new_page()

        if args.match:
            print(f"\n[MODE] Single match -> {args.match}")
            data = await fetch_match_data(page, args.match, player=args.player)

            if "round_data" in data:
                rounds = parse_rounds(data["round_data"])
                print_rounds(rounds)
            else:
                print("\n  No round data found.")

            if args.save and data:
                for key, value in data.items():
                    out = Path(f"{args.match}_{key}.json")
                    out.write_text(json.dumps(value, indent=2), encoding="utf-8")
                    print(f"\n  Saved -> {out}")
        else:
            print(f"\n[MODE] Player history -> {args.player}")
            matches = await fetch_player_matches(page, args.player)

            if not matches:
                print("  No matches found. Try --no-headless to debug.")
            else:
                print(f"\n  Processing up to {args.limit} matches...")
                for match in matches[: args.limit]:
                    match_id = match.get("id") or match.get("matchId", "")
                    map_name = (match.get("metadata") or {}).get("mapName", "Unknown Map")
                    if not match_id:
                        continue

                    print(f"\n  Match: {map_name} (ID: {match_id})")
                    data = await fetch_match_data(page, match_id, player=args.player)

                    if "round_data" in data:
                        rounds = parse_rounds(data["round_data"])
                        print_rounds(rounds)

                    if args.save and data:
                        out = Path(f"{match_id}_rounds.json")
                        out.write_text(json.dumps(data.get("round_data", {}), indent=2), encoding="utf-8")
                        print(f"\n  Saved -> {out}")

                    await asyncio.sleep(2)

        await browser.close()
        print("\n[Done]")


if __name__ == "__main__":
    asyncio.run(main())
