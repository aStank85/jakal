from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from src.parser import R6TrackerParser
from .drawer import scrape_drawer_text


class ScraperBlockedError(Exception):
    """Raised when Cloudflare blocks scraper access."""


class PlayerNotFoundError(Exception):
    """Raised when username does not exist on R6 Tracker."""


class R6Scraper:
    """Automated R6 Tracker data scraper using Playwright."""

    BASE_URL = "https://r6.tracker.network/r6siege/profile/ubi/{username}/{section}"
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    VIEWPORT = {"width": 1280, "height": 720}

    def __init__(self, headless: bool = True, slow_mo: int = 500):
        self.headless = headless
        self.slow_mo = slow_mo
        self._playwright = None
        self.browser = None
        self.context = None
        self.page = None

    # --- Main entry points ---

    def scrape_full_profile(self, username: str) -> Dict[str, Any]:
        """Scrape all available profile sections for a player."""
        errors: List[str] = []
        season_stats: Optional[Dict[str, Any]] = None
        map_stats: List[Dict[str, Any]] = []
        operator_stats: List[Dict[str, Any]] = []
        match_history: List[Dict[str, Any]] = []

        self._launch_browser()
        try:
            overview_url = self.BASE_URL.format(username=username, section="overview")
            self._navigate(self.page, overview_url, wait_ms=3000)
            self._dismiss_modals(self.page)

            try:
                season_stats = self._scrape_season_stats_from_page(self.page)
            except Exception as exc:
                errors.append(f"Season stats failed: {exc}")

            try:
                self._close_drawer(self.page)
            except Exception:
                pass

            try:
                self._click_nav_link(self.page, "Maps")
                self.page.wait_for_timeout(3000)
                self._dismiss_modals(self.page)
                ranked_button = self.page.get_by_role("button", name="Ranked", exact=True)
                if ranked_button.count() > 0:
                    ranked_button.first.click()
                    self.page.wait_for_timeout(2000)
                map_stats = self._parse_map_stats_html(self.page.content())
            except Exception as exc:
                errors.append(f"Map stats failed: {exc}")

            try:
                self._click_nav_link(self.page, "Operators")
                self.page.wait_for_timeout(3000)
                self._dismiss_modals(self.page)
                operator_stats = self._parse_operator_stats_html(self.page.content())
            except Exception as exc:
                errors.append(f"Operator stats failed: {exc}")

            # Match history is now sourced from Tracker API in sync flow.

            return {
                "username": username,
                "scraped_at": datetime.now().isoformat(),
                "season_stats": season_stats,
                "map_stats": map_stats,
                "operator_stats": operator_stats,
                "match_history": match_history,
                "errors": errors,
            }
        finally:
            self._close_browser()

    def scrape_season_stats(self, username: str) -> Dict[str, Any]:
        """Scrape season drawer stats in parser-compatible output format."""
        self._launch_browser()
        try:
            overview_url = self.BASE_URL.format(username=username, section="overview")
            self._navigate(self.page, overview_url, wait_ms=3000)
            self._dismiss_modals(self.page)
            return self._scrape_season_stats_from_page(self.page)
        finally:
            self._close_browser()

    def scrape_map_stats(self, username: str, filter_ranked: bool = True) -> List[Dict]:
        """Scrape map stats table from /maps."""
        self._launch_browser()
        try:
            overview_url = self.BASE_URL.format(username=username, section="overview")
            self._navigate(self.page, overview_url, wait_ms=3000)
            self._dismiss_modals(self.page)
            self._click_nav_link(self.page, "Maps")
            self.page.wait_for_timeout(3000)
            self._dismiss_modals(self.page)
            if filter_ranked:
                ranked_button = self.page.get_by_role("button", name="Ranked", exact=True)
                if ranked_button.count() > 0:
                    ranked_button.first.click()
                    self.page.wait_for_timeout(2000)
            return self._parse_map_stats_html(self.page.content())
        finally:
            self._close_browser()

    def scrape_operator_stats(self, username: str) -> List[Dict]:
        """Scrape operator stats table from /operators."""
        self._launch_browser()
        try:
            overview_url = self.BASE_URL.format(username=username, section="overview")
            self._navigate(self.page, overview_url, wait_ms=3000)
            self._dismiss_modals(self.page)
            self._click_nav_link(self.page, "Operators")
            self.page.wait_for_timeout(3000)
            self._dismiss_modals(self.page)
            return self._parse_operator_stats_html(self.page.content())
        finally:
            self._close_browser()

    def scrape_match_history(self, username: str) -> List[Dict]:
        """Deprecated: match history should be fetched via Tracker API client."""
        self._launch_browser()
        try:
            overview_url = self.BASE_URL.format(username=username, section="overview")
            self._navigate(self.page, overview_url, wait_ms=3000)
            self._dismiss_modals(self.page)
            self._click_nav_link(self.page, "Matches")
            self.page.wait_for_timeout(3000)
            self._dismiss_modals(self.page)
            self._load_match_history_rows(self.page, target_rows=40)
            return self._parse_match_history_html(self.page.content())
        finally:
            self._close_browser()

    def scrape_match_detail(self, username: str, match_index: int = 0) -> Dict:
        """Scrape expanded match detail scoreboard."""
        self._launch_browser()
        try:
            overview_url = self.BASE_URL.format(username=username, section="overview")
            self._navigate(self.page, overview_url, wait_ms=3000)
            self._dismiss_modals(self.page)
            self._click_nav_link(self.page, "Matches")
            self.page.wait_for_timeout(3000)
            self._dismiss_modals(self.page)
            self._load_match_history_rows(self.page, target_rows=max(40, match_index + 1))

            rows = self.page.locator('[class*="v3-match-row"]')
            if rows.count() == 0:
                raise PlayerNotFoundError(f"No matches found for '{username}'")
            if match_index >= rows.count():
                raise ValueError(f"Match index {match_index} out of range")

            rows.nth(match_index).click()
            self.page.wait_for_timeout(3000)
            return self._parse_match_detail_html(self.page.content(), username)
        finally:
            self._close_browser()

    # --- Internal helpers ---

    def _launch_browser(self) -> None:
        """Start Playwright browser with anti-detection headers."""
        if self.browser:
            return

        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise ImportError(
                "Playwright is not installed. Install with: pip install playwright; playwright install chromium"
            ) from exc

        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(headless=self.headless, slow_mo=self.slow_mo)
        self.context = self.browser.new_context(
            user_agent=self.USER_AGENT,
            viewport=self.VIEWPORT,
            locale="en-US",
        )
        self.page = self.context.new_page()

    def _close_browser(self) -> None:
        """Clean up browser resources."""
        if self.context:
            try:
                self.context.close()
            except Exception:
                pass
        if self.browser:
            try:
                self.browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                self._playwright.stop()
            except Exception:
                pass

        self._playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def _navigate(self, page, url: str, wait_ms: int = 3000) -> None:
        """Navigate to URL with proper waits and Cloudflare detection."""
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(wait_ms)
        title = page.title().lower()

        if "attention" in title:
            page.wait_for_timeout(10000)
            page.reload(wait_until="domcontentloaded")
            page.wait_for_timeout(wait_ms)
            if "attention" in page.title().lower():
                raise ScraperBlockedError("Cloudflare blocked this request")

        if "not found" in title or "404" in title:
            raise PlayerNotFoundError(f"Profile page not found at {url}")

    def _load_match_history_rows(self, page, target_rows: int = 40, max_attempts: int = 10) -> int:
        """Load additional match rows via scrolling/pagination where available."""
        last_count = -1
        stable_loops = 0

        for _ in range(max_attempts):
            count = page.locator(".v3-match-row").count()
            if count >= target_rows:
                return count

            if count == last_count:
                stable_loops += 1
            else:
                stable_loops = 0
            last_count = count

            if stable_loops >= 2:
                break

            try:
                page.mouse.wheel(0, 2200)
            except Exception:
                pass
            page.wait_for_timeout(1200)

            load_more_buttons = [
                page.get_by_role("button", name=re.compile(r"show more|load more|more", re.I)),
                page.locator("button:has-text('Show More')"),
                page.locator("button:has-text('Load More')"),
            ]
            for locator in load_more_buttons:
                try:
                    if locator.count() > 0:
                        locator.first.click(timeout=1200)
                        page.wait_for_timeout(1200)
                        break
                except Exception:
                    continue

        return page.locator(".v3-match-row").count()

    def _dismiss_modals(self, page) -> None:
        """Dismiss cookie/premium popups that block clicks."""
        close_candidates = [
            page.get_by_role("button", name=re.compile("accept|agree", re.I)),
            page.get_by_role("button", name=re.compile("close", re.I)),
            page.locator('[aria-label*="close" i]'),
            page.locator("button[class*='close']"),
        ]

        for locator in close_candidates:
            try:
                if locator.count() > 0:
                    locator.first.click(timeout=1200)
                    page.wait_for_timeout(500)
            except Exception:
                continue

    def _click_nav_link(self, page, label: str) -> None:
        """Click top-nav profile link with robust selector fallbacks."""
        exact = page.get_by_role("link", name=label, exact=True)
        if exact.count() > 0:
            exact.first.click()
            return

        case_sensitive = page.get_by_role("link", name=re.compile(rf"^{re.escape(label)}$"))
        if case_sensitive.count() > 0:
            case_sensitive.first.click()
            return

        fuzzy = page.get_by_role("link", name=re.compile(label, re.I))
        if fuzzy.count() > 0:
            fuzzy.first.click()
            return

        raise RuntimeError(f"Could not locate '{label}' tab")

    def _close_drawer(self, page) -> None:
        """Close expanded season drawer before tab navigation."""
        candidates = [
            page.locator(".size-6.cursor-pointer"),
            page.get_by_role("button", name=re.compile("close", re.I)),
            page.locator('[aria-label="Close"]'),
        ]
        for locator in candidates:
            try:
                if locator.count() > 0:
                    locator.first.click(timeout=1200)
                    page.wait_for_timeout(1000)
                    return
            except Exception:
                continue

    def _scrape_season_stats_from_page(self, page) -> Dict[str, Any]:
        """Use existing drawer parser path to preserve output schema exactly."""
        drawer_text = scrape_drawer_text(page, dump_raw=False)
        parser = R6TrackerParser()
        return parser.parse(drawer_text)

    def _parse_rp_string(self, rp_str: str) -> Tuple[int, int]:
        """Parse RP string like '3,053+27' into (3053, 27)."""
        compact = re.sub(r"\s+", "", rp_str or "")
        match = re.search(r"([\d,]+)([+-]\d+)", compact)
        if not match:
            return (self._parse_number(compact), 0)
        rp = self._parse_number(match.group(1))
        delta = int(match.group(2))
        return (rp, delta)

    def _parse_percent(self, pct_str: str) -> float:
        """Parse '51.9%' -> 51.9."""
        clean = (pct_str or "").replace("%", "").strip()
        if clean == "":
            return 0.0
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def _parse_number(self, num_str: str) -> int:
        """Parse comma-formatted integer strings."""
        clean = re.sub(r"[^\d-]", "", num_str or "")
        if clean in ("", "-"):
            return 0
        try:
            return int(clean)
        except ValueError:
            return 0

    def _parse_kd(self, kd_str: str) -> float:
        """Parse KD float string safely."""
        clean = re.sub(r"[^0-9.]", "", kd_str or "")
        if clean == "":
            return 0.0
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def _parse_map_row(self, row) -> Dict[str, Any]:
        cells = row.select("td")
        if len(cells) < 10:
            return {}
        vals = [c.get_text(" ", strip=True) for c in cells[:10]]
        return {
            "map_name": vals[0],
            "matches": self._parse_number(vals[1]),
            "win_pct": self._parse_percent(vals[2]),
            "wins": self._parse_number(vals[3]),
            "losses": self._parse_number(vals[4]),
            "kd": self._parse_kd(vals[5]),
            "atk_win_pct": self._parse_percent(vals[6]),
            "def_win_pct": self._parse_percent(vals[7]),
            "hs_pct": self._parse_percent(vals[8]),
            "esr": self._parse_kd(vals[9]),
        }

    def _parse_operator_row(self, row) -> Dict[str, Any]:
        cells = row.select("td")
        if len(cells) < 12:
            return {}
        vals = [c.get_text(" ", strip=True) for c in cells[:12]]
        return {
            "operator_name": vals[0],
            "rounds": self._parse_number(vals[1]),
            "win_pct": self._parse_percent(vals[2]),
            "kd": self._parse_kd(vals[3]),
            "hs_pct": self._parse_percent(vals[4]),
            "kills": self._parse_number(vals[5]),
            "deaths": self._parse_number(vals[6]),
            "wins": self._parse_number(vals[7]),
            "losses": self._parse_number(vals[8]),
            "assists": self._parse_number(vals[9]),
            "aces": self._parse_number(vals[10]),
            "teamkills": self._parse_number(vals[11]),
        }

    def _parse_match_row(self, row) -> Dict[str, Any]:
        text = row.get_text(" ", strip=True)
        classes = " ".join(row.get("class", []))

        result = "win" if "--win" in classes else "loss" if "--loss" in classes else ""
        had_ace = bool(re.search(r"\bAce\b", text, re.I))
        had_4k = bool(re.search(r"\b4K\b", text))
        had_3k = bool(re.search(r"\b3K\b", text))
        had_2k = bool(re.search(r"\b2K\b", text))

        time_ago = ""
        map_name = ""
        mode = ""
        score = ""

        top_match = re.search(
            r"^(?P<time>\S+\s+ago)\s+(?P<map>.+?)\s+(?P<mode>Ranked|Standard|Quick Match|Arcade|Event)\s+Score\s+",
            text,
        )
        if top_match:
            time_ago = top_match.group("time")
            map_name = top_match.group("map").strip()
            mode = top_match.group("mode")

        score_match = re.search(r"(\d+\s*:\s*\d+)", text)
        if score_match:
            score = score_match.group(1).replace(" ", "")

        rp_match = re.search(r"RP\s*([\d,]+\s*[+-]\s*\d+)", text)
        rp, rp_change = (0, 0)
        if rp_match:
            rp, rp_change = self._parse_rp_string(rp_match.group(1))

        kd_match = re.search(r"K/D\s*([0-9.]+)", text)
        kd = self._parse_kd(kd_match.group(1)) if kd_match else 0.0

        kda_match = re.search(r"K/D/A\s*([0-9]+\s*[0-9]+\s*[0-9]+)", text)
        kda = re.sub(r"\s+", "", kda_match.group(1)) if kda_match else ""

        hs_match = re.search(r"HS\s*%\s*([0-9.]+%)", text)
        hs_pct = self._parse_percent(hs_match.group(1)) if hs_match else 0.0

        return {
            "time_ago": time_ago,
            "map_name": map_name,
            "mode": mode,
            "score": score,
            "result": result,
            "rp": rp,
            "rp_change": rp_change,
            "kd": kd,
            "kda": kda,
            "hs_pct": hs_pct,
            "had_ace": had_ace,
            "had_4k": had_4k,
            "had_3k": had_3k,
            "had_2k": had_2k,
        }

    def _parse_match_detail_table(self, table) -> List[Dict[str, Any]]:
        players: List[Dict[str, Any]] = []
        rows = table.select("tbody tr") or table.select("tr")

        for row in rows:
            cells = row.select("td")
            if len(cells) < 11:
                continue

            username = cells[0].get_text(" ", strip=True)
            rp, rp_change = self._parse_rp_string(cells[1].get_text(" ", strip=True))
            operators = [img.get("alt", "").strip() for img in cells[10].select("img[alt]") if img.get("alt")]

            players.append({
                "username": username,
                "rp": rp,
                "rp_change": rp_change,
                "kd": self._parse_kd(cells[2].get_text(" ", strip=True)),
                "kills": self._parse_number(cells[3].get_text(" ", strip=True)),
                "deaths": self._parse_number(cells[4].get_text(" ", strip=True)),
                "assists": self._parse_number(cells[5].get_text(" ", strip=True)),
                "hs_pct": self._parse_percent(cells[6].get_text(" ", strip=True)),
                "first_kills": self._parse_number(cells[7].get_text(" ", strip=True)),
                "first_deaths": self._parse_number(cells[8].get_text(" ", strip=True)),
                "clutches": self._parse_number(cells[9].get_text(" ", strip=True)),
                "operators": operators,
            })

        return players

    def _parse_map_stats_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select('tr[class*="group/row"]')
        maps: List[Dict[str, Any]] = []
        for row in rows:
            parsed = self._parse_map_row(row)
            if parsed.get("map_name"):
                maps.append(parsed)
        return maps

    def _parse_operator_stats_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select('tr[class*="group/row"]')
        operators: List[Dict[str, Any]] = []
        for row in rows:
            parsed = self._parse_operator_row(row)
            if parsed.get("operator_name"):
                operators.append(parsed)
        return operators

    def _parse_match_history_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select(".v3-match-row")
        matches: List[Dict[str, Any]] = []
        for row in rows:
            parsed = self._parse_match_row(row)
            if parsed.get("map_name") or parsed.get("score"):
                matches.append(parsed)
        return matches

    def _parse_match_detail_html(self, html: str, username: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.select("table")
        if len(tables) < 2:
            return {"team_a": [], "team_b": [], "your_team": ""}

        team_a = self._parse_match_detail_table(tables[0])
        team_b = self._parse_match_detail_table(tables[1])

        your_team = ""
        lower = username.lower()
        if any(p["username"].lower() == lower for p in team_a):
            your_team = "A"
        elif any(p["username"].lower() == lower for p in team_b):
            your_team = "B"

        return {"team_a": team_a, "team_b": team_b, "your_team": your_team}
