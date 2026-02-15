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
    """Deprecated legacy scraper. Sync now uses Tracker API only."""

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
        raise RuntimeError("R6Scraper is deprecated. Use TrackerAPIClient endpoints instead.")

    def scrape_season_stats(self, username: str) -> Dict[str, Any]:
        raise RuntimeError("scrape_season_stats is deprecated. Use TrackerAPIClient.get_profile().")

    def scrape_map_stats(self, username: str, filter_ranked: bool = True) -> List[Dict]:
        raise RuntimeError("scrape_map_stats is deprecated. Use TrackerAPIClient.get_map_stats().")

    def scrape_operator_stats(self, username: str) -> List[Dict]:
        raise RuntimeError("scrape_operator_stats is deprecated. Use TrackerAPIClient.get_operator_stats().")

    def scrape_match_history(self, username: str) -> List[Dict]:
        raise RuntimeError("scrape_match_history is deprecated. Use TrackerAPIClient APIs.")

    def scrape_match_detail(self, username: str, match_index: int = 0) -> Dict:
        raise RuntimeError("scrape_match_detail is deprecated. Use TrackerAPIClient match detail APIs.")

    # --- Internal helpers ---

    def _launch_browser(self) -> None:
        raise RuntimeError("Playwright browser automation is removed from v0.5.1 sync flow.")

    def _close_browser(self) -> None:
        self._playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def _navigate(self, page, url: str, wait_ms: int = 3000) -> None:
        raise RuntimeError("Playwright navigation is removed from v0.5.1 sync flow.")

    def _load_match_history_rows(self, page, target_rows: int = 40, max_attempts: int = 10) -> int:
        raise RuntimeError("Playwright pagination is removed from v0.5.1 sync flow.")

    def _dismiss_modals(self, page) -> None:
        raise RuntimeError("Playwright modal handling is removed from v0.5.1 sync flow.")

    def _click_nav_link(self, page, label: str) -> None:
        raise RuntimeError("Playwright navigation is removed from v0.5.1 sync flow.")

    def _close_drawer(self, page) -> None:
        raise RuntimeError("Playwright drawer interactions are removed from v0.5.1 sync flow.")

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
